package taskmill

import (
	"cmp"
	"context"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
)

// Activity limits.
const (
	activityRetention = 24 * time.Hour // how long activity is kept
	tallyMaxAge       = time.Hour      // counts a worker could not write for this long are dropped
)

// ActivityParams selects the activity that [Client.Activity] returns.
type ActivityParams struct {
	// Queues limits the activity to these queues. Empty means all queues.
	Queues []string

	// Since is when the period starts. Default: an hour ago. Activity is
	// kept for 24 hours.
	Since time.Time

	// Step is how much time each point covers, in whole minutes. Default: a
	// minute. Maximum: a day.
	Step time.Duration
}

// Activity is what happened during one step of time.
type Activity struct {
	// At is when the step starts.
	At time.Time

	// Counts holds the most tasks counted in each state during the step,
	// except completed: the maintainer counts them every 10 seconds, with the
	// caps of [QueueInfo.Counts]. It is nil if no maintainer counted them, for
	// example while no worker ran.
	Counts map[State]int64

	// Completed and Failed are the attempts that succeeded and failed during
	// the step. Failed counts every failed attempt, whether its task is
	// retried or dead, including attempts whose workers lost their leases.
	// Workers write their counts every few seconds; a worker that crashes
	// loses its last few seconds of them.
	Completed, Failed int64
}

// Activity returns the activity of the selected queues, one point per step,
// oldest first. A step in which nothing was recorded has no point, so a gap
// between points means that no worker ran. The query is canceled after 5
// seconds, like [Client.List].
func (c *Client) Activity(ctx context.Context, params *ActivityParams) ([]Activity, error) {
	p, queues, err := activityParams(params)
	if err != nil {
		return nil, err
	}
	var out []Activity
	err = pgx.BeginFunc(ctx, c.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, "SET LOCAL statement_timeout = '"+listTimeout+"'"); err != nil {
			return err
		}
		r, err := tx.Query(ctx, c.sql.activity, p.Since, time.Now().Add(time.Minute), int64(p.Step/time.Second), queues)
		if err != nil {
			return err
		}
		defer r.Close()
		for r.Next() {
			a, err := scanActivity(r)
			if err != nil {
				return err
			}
			out = append(out, a)
		}
		return r.Err()
	})
	if err != nil {
		return nil, fmt.Errorf("taskmill: activity: %w", err)
	}
	return out, nil
}

// activityParams applies the defaults to params and checks them. It returns
// the queues as a slice that is never nil: NULL would match nothing.
func activityParams(params *ActivityParams) (ActivityParams, []string, error) {
	p := ActivityParams{}
	if params != nil {
		p = *params
	}
	switch {
	case p.Step == 0:
		p.Step = time.Minute
	case p.Step < 0 || p.Step%time.Minute != 0 || p.Step > 24*time.Hour:
		return p, nil, invalidArgument("step must be a whole number of minutes, up to a day")
	}
	if p.Since.IsZero() {
		p.Since = time.Now().Add(-time.Hour)
	}
	queues := make([]string, 0, len(p.Queues))
	for _, q := range p.Queues {
		if q == "" {
			return p, nil, invalidArgument("queue name must not be empty")
		}
		queues = append(queues, q)
	}
	return p, queues, nil
}

// scanActivity reads one row of the activity statement.
func scanActivity(r rows) (Activity, error) {
	var a Activity
	var counts [len(activityStates)]*int64
	dest := []any{&a.At}
	for i := range counts {
		dest = append(dest, &counts[i])
	}
	if err := r.Scan(append(dest, &a.Completed, &a.Failed)...); err != nil {
		return a, err
	}
	for i, n := range counts {
		if n == nil {
			continue
		}
		if a.Counts == nil {
			a.Counts = make(map[State]int64, len(activityStates))
		}
		a.Counts[activityStates[i]] = *n
	}
	return a, nil
}

// activityStates are the states whose counts the activity keeps, in the
// order of the activity statement's columns.
var activityStates = [...]State{
	StateScheduled, StatePending, StateAvailable, StateRunning, StateRetryable, StateDead, StateCanceled,
}

// tally counts the attempts that a worker ended, per minute and queue, until
// its next heartbeat adds them to the activity table. Counting in memory
// keeps the statements that record attempts free of a counter row that every
// worker would update.
type tally struct {
	mu     sync.Mutex
	counts map[tallyKey]tallyCount
}

type tallyKey struct {
	minute int64 // Unix time of the minute's start
	queue  string
}

type tallyCount struct {
	completed, failed int
}

func (t *tally) add(queue string, completed, failed int) {
	if completed == 0 && failed == 0 {
		return
	}
	key := tallyKey{time.Now().Truncate(time.Minute).Unix(), queue}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.counts == nil {
		t.counts = map[tallyKey]tallyCount{}
	}
	c, ok := t.counts[key]
	if !ok {
		// A worker that only runs tasks with RunTask never flushes: drop what
		// is too old to matter, so that the counts stay small.
		oldest := key.minute - int64(tallyMaxAge/time.Second)
		maps.DeleteFunc(t.counts, func(k tallyKey, _ tallyCount) bool { return k.minute < oldest })
	}
	c.completed += completed
	c.failed += failed
	t.counts[key] = c
}

// take removes and returns the counts.
func (t *tally) take() map[tallyKey]tallyCount {
	t.mu.Lock()
	defer t.mu.Unlock()
	counts := t.counts
	t.counts = nil
	return counts
}

// restore puts back counts that could not be written, except those too old
// to be worth keeping, so that a long outage cannot grow them without bound.
func (t *tally) restore(counts map[tallyKey]tallyCount) {
	oldest := time.Now().Add(-tallyMaxAge).Unix()
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.counts == nil {
		t.counts = map[tallyKey]tallyCount{}
	}
	for key, c := range counts {
		if key.minute < oldest {
			continue
		}
		now := t.counts[key]
		now.completed += c.completed
		now.failed += c.failed
		t.counts[key] = now
	}
}

// countCompleted adds completed tasks to the worker's tally.
func (w *Worker) countCompleted(done []completedTask) {
	for _, d := range done {
		w.tally.add(d.queue, 1, 0)
	}
}

// flushActivity adds the attempts counted since the last flush to the
// activity table. Counts it cannot write are kept for the next try.
func (w *Worker) flushActivity(ctx context.Context) {
	counts := w.tally.take()
	if len(counts) == 0 {
		return
	}
	keys := slices.SortedFunc(maps.Keys(counts), func(a, b tallyKey) int {
		return cmp.Or(cmp.Compare(a.minute, b.minute), cmp.Compare(a.queue, b.queue))
	})
	minutes := make([]time.Time, len(keys))
	queues := make([]string, len(keys))
	completed := make([]int32, len(keys))
	failed := make([]int32, len(keys))
	for i, k := range keys {
		c := counts[k]
		minutes[i], queues[i] = time.Unix(k.minute, 0), k.queue
		completed[i], failed[i] = int32(min(c.completed, 1<<31-1)), int32(min(c.failed, 1<<31-1)) //nolint:gosec // capped
	}
	if _, err := w.client.pool.Exec(ctx, w.client.sql.addActivity, minutes, queues, completed, failed); err != nil {
		w.tally.restore(counts)
		if ctx.Err() == nil {
			w.logger.LogAttrs(ctx, slog.LevelWarn, "record activity failed; trying again", slog.Any(logKeyError, err))
		}
	}
}
