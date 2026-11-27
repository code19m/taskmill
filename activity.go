package taskmill

import (
	"cmp"
	"context"
	"fmt"
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

// ActivityParams selects the activity that [Client.Activity] returns. A
// field outside its limits is an error that wraps [ErrInvalidArgument].
type ActivityParams struct {
	// Queues limits the activity to these queues. Empty means all queues.
	// Maximum: 100 names.
	Queues []string

	// Since is when the period starts, a time that PostgreSQL can store,
	// from 4713 BC to 294276 AD. Default: an hour ago. Activity is kept for
	// 24 hours, in points of one minute.
	Since time.Time

	// Step is how much time each point covers, in whole minutes. Default: a
	// minute. Maximum: a day. Steps are counted from midnight UTC on 1
	// January 2000, whatever the time zone: a step of an hour starts on the
	// hour in UTC, and a step of a day at midnight UTC.
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
	// Workers write their counts every 10 seconds, and add each once. A
	// worker that crashes, or that stops while it cannot write, loses the
	// counts that it has not written, and a worker drops the counts that it
	// could not write for 1 hour.
	Completed, Failed int64
}

// Activity returns the activity of the selected queues, one point per step,
// oldest first. params may be nil, for the last hour of every queue, in
// steps of a minute. A step in which nothing was recorded has no point, so a
// gap between points means that no worker ran. The query is canceled after 5
// seconds, like [Client.List].
func (c *Client) Activity(ctx context.Context, params *ActivityParams) ([]Activity, error) {
	p, queues, err := activityParams(params)
	if err != nil {
		return nil, err
	}

	var out []Activity
	args := []any{
		p.Since,                     // $1
		time.Now().Add(time.Minute), // $2
		int64(p.Step / time.Second), // $3
		queues,                      // $4
	}
	err = c.readAlone(ctx, activitySettings, c.sql.activity, args, func(r pgx.Rows) error {
		a, err := scanActivity(r)
		if err != nil {
			return err
		}
		out = append(out, a)
		return nil
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
	if err := checkTime("since", p.Since); err != nil {
		return p, nil, err
	}

	queues, err := checkFilter("queue name", p.Queues, maxNameLength)
	return p, queues, err
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
// it adds them to the activity table, every 10 seconds. Counting in memory
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

// countCompleted adds completed tasks to the worker's tally.
func (w *Worker) countCompleted(done []completedTask) {
	for _, d := range done {
		w.tally.add(d.queue, 1, 0)
	}
}

// activityLoop writes the attempts the worker counted, on a schedule of its
// own: a slow write never delays the check-ins that keep leases.
func (w *Worker) activityLoop(ctx context.Context) {
	tick := time.NewTicker(w.timing.activity)
	defer tick.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			flushCtx, cancel := context.WithTimeout(ctx, w.timing.activity)
			w.flushActivity(flushCtx)
			cancel()
		}
	}
}

// flushActivity adds the attempts counted since the last flush to the
// activity table. A write that fails is tried again, with the same counts,
// before any other: it may have committed all the same, as a commit that
// outlasts the deadline does when it waits for a synchronous standby. So
// each write has a number, one more than the worker's last, and adds its
// counts only if it raises the number that the worker's row of
// activity_writes holds: a write tried again adds them once. The number
// commits with the counts, so it holds after a failover too, and a write
// tried again while the first try is still under way waits for its end.
// At most one write is unwritten at a time, so a standby that lags does
// not pile up connections that wait for it, and the counts made meanwhile
// wait in the tally.
func (w *Worker) flushActivity(ctx context.Context) {
	if w.unwritten != nil && !w.writeActivity(ctx) {
		return
	}

	counts := w.tally.take()
	if len(counts) == 0 {
		return
	}
	w.writes++
	w.unwritten = &activityWrite{number: w.writes, counts: counts}
	w.writeActivity(ctx)
}

// activityWrite is a write of counts to the activity table.
type activityWrite struct {
	number int64 // the worker's writes so far, this one included
	counts map[tallyKey]tallyCount
}

// writeActivity writes w.unwritten, and reports whether it succeeded. It
// drops the counts too old to be worth keeping, so that a long outage
// cannot keep them without bound. A write left with none still runs: its
// outcome tells whether the database answers again, so that the failures
// logged end with it, also on an idle worker.
func (w *Worker) writeActivity(ctx context.Context) bool {
	oldest := time.Now().Add(-tallyMaxAge).Unix()
	maps.DeleteFunc(w.unwritten.counts, func(k tallyKey, _ tallyCount) bool { return k.minute < oldest })

	err := w.addActivity(ctx, w.unwritten)
	w.activityFailures.observe(ctx, err)
	if err != nil {
		return false
	}
	w.unwritten = nil
	return true
}

// addActivity adds the counts of write to the activity table, unless an
// earlier try of it committed.
func (w *Worker) addActivity(ctx context.Context, write *activityWrite) error {
	keys := slices.SortedFunc(maps.Keys(write.counts), func(a, b tallyKey) int {
		return cmp.Or(cmp.Compare(a.minute, b.minute), cmp.Compare(a.queue, b.queue))
	})

	minutes := make([]time.Time, len(keys))
	queues := make([]string, len(keys))
	completed := make([]int32, len(keys))
	failed := make([]int32, len(keys))
	for i, k := range keys {
		c := write.counts[k]
		minutes[i], queues[i] = time.Unix(k.minute, 0), k.queue
		completed[i], failed[i] = int32(min(c.completed, 1<<31-1)), int32(min(c.failed, 1<<31-1)) //nolint:gosec // capped
	}

	_, err := w.client.pool.Exec(ctx, w.client.sql.addActivity,
		minutes,      // $1
		queues,       // $2
		completed,    // $3
		failed,       // $4
		w.writer,     // $5
		write.number, // $6
	)
	return err
}
