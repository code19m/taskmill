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
	// Workers write their counts every 10 seconds; a worker that crashes
	// loses its last few seconds of them, and a worker drops the counts
	// that it could not write for 1 hour.
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
// activity table. Counts it cannot write are kept for the next try.
//
// A write that runs out of time may still commit: its commit can outlast
// the deadline, as when it waits for a synchronous standby, and the server
// finishes it although the worker no longer waits for the answer. So a
// write learns its transaction's ID before it commits, and when its commit
// fails, the next flush asks PostgreSQL what became of it, and writes
// nothing else until it knows: the counts are added again only if the
// write did not commit. At most one write is in doubt at a time, so a
// standby that lags does not pile up connections that wait for it.
func (w *Worker) flushActivity(ctx context.Context) {
	if w.unsure != nil && !w.settleUnsure(ctx) {
		return
	}

	counts := w.tally.take()
	if len(counts) == 0 {
		return
	}

	xid, err := w.addActivity(ctx, counts)
	w.activityFailures.observe(ctx, err)
	switch {
	case err == nil:
	case xid != 0: // the commit failed: the write may have committed all the same
		w.unsure = &unsureWrite{xid: xid, counts: counts}
	default: // nothing was committed
		w.tally.restore(counts)
	}
}

// unsureWrite is a write of counts to the activity table whose commit
// failed, so that it may or may not have committed.
type unsureWrite struct {
	xid    uint64 // its transaction's ID
	counts map[tallyKey]tallyCount
}

// settleUnsure asks PostgreSQL whether the transaction of w.unsure
// committed, and reports whether it knows now. If the transaction did not
// commit, it puts the counts back in the tally. A transaction that the
// server does not know yet, as after a failover to a standby that lacks it,
// did not commit there.
func (w *Worker) settleUnsure(ctx context.Context) bool {
	var status *string
	err := w.client.pool.QueryRow(ctx, xactStatusSQL, w.unsure.xid).Scan(&status)
	switch {
	case pgCode(err) == codeInvalidParameterValue: // the ID is in the future
	case err != nil:
		w.activityFailures.observe(ctx, err)
		return false
	case status == nil: // so old that the server no longer knows: cannot happen within tallyMaxAge
	case *status == "committed":
		w.unsure = nil
		return true
	case *status == "in progress": // its commit still waits, as for a standby
		return false
	}
	w.tally.restore(w.unsure.counts)
	w.unsure = nil
	return true
}

// addActivity adds counts to the activity table, in a transaction that
// returns its ID before it commits. It returns the ID if the commit
// failed, and 0 if the transaction failed before it.
func (w *Worker) addActivity(ctx context.Context, counts map[tallyKey]tallyCount) (xid uint64, err error) {
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

	conn, err := w.client.pool.Acquire(ctx)
	if err != nil {
		return 0, err
	}
	// A connection released inside the transaction is closed, and the
	// server rolls the transaction back: it never got a COMMIT.
	defer conn.Release()

	b := &pgx.Batch{}
	b.Queue("BEGIN")
	b.Queue(w.client.sql.addActivity,
		minutes,   // $1
		queues,    // $2
		completed, // $3
		failed,    // $4
	).QueryRow(func(r pgx.Row) error { return r.Scan(&xid) })
	if err := conn.SendBatch(ctx, b).Close(); err != nil {
		return 0, err
	}
	if _, err := conn.Exec(ctx, "COMMIT"); err != nil {
		return xid, err
	}
	return 0, nil
}
