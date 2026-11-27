package taskmill

import (
	"context"
	"sync"
	"time"
)

// outcomeRequest asks the recorder to record an outcome other than
// completion, and holds the result once done is closed.
type outcomeRequest struct {
	t    *liveAttempt
	o    outcome
	done chan struct{}

	ended attemptEnd
	held  bool // another transaction held the task's row: the attempt records its outcome alone
	err   error
}

// recorder records the outcomes other than completion of ungrouped attempts
// in batches, on record, until ctx ends, as the completer records
// completions: one statement records the outcomes that arrive within a
// moment of each other, so a worker whose handlers fail fast, as in an
// outage of what they call, costs the database little more than one whose
// handlers succeed. The statement skips the rows that other transactions
// hold, and those attempts then record their outcomes alone. Each attempt
// keeps its slot until its outcome is recorded, as before. Run stops the
// recorder once no attempt records any more.
func (w *Worker) recorder(ctx, record context.Context) {
	defer close(w.recorderDone)
	for {
		select {
		case r := <-w.outcomes:
			w.recordBatch(record, gather(w.outcomes, []*outcomeRequest{r}))
		case <-ctx.Done():
			return
		}
	}
}

// recordGathered records outcome o of t with the outcomes of other
// attempts, and reports held if another transaction held the task's row.
// It waits until ctx ends at most: the recorder records on Run's context,
// which outlives the deadline of a stopping worker's release of the tasks
// of a fetch that it could not start. The recorder may then still record o.
func (w *Worker) recordGathered(ctx context.Context, t *liveAttempt, o outcome) (ended attemptEnd, held bool, err error) {
	r := &outcomeRequest{t: t, o: o, done: make(chan struct{})}
	select {
	case w.outcomes <- r: // Run stops the recorder only after every outcome is recorded
	case <-ctx.Done():
		return ended, held, context.Cause(ctx)
	}
	select {
	case <-r.done:
		return r.ended, r.held, r.err
	case <-ctx.Done():
		return ended, held, context.Cause(ctx)
	}
}

// recordBatch records the outcomes of batch in one statement, and closes
// each request's done.
func (w *Worker) recordBatch(ctx context.Context, batch []*outcomeRequest) {
	ts := make([]*liveAttempt, len(batch))
	var rows outcomeRows
	for i, r := range batch {
		ts[i] = r.t
		rows.add(r.t, r.o)
	}

	var recorded map[attemptKey]recordedOutcome
	err := w.whileLeased(ctx, ts, func(ctx context.Context) error {
		conn, err := w.acquire(ctx)
		if err != nil {
			return err
		}
		defer conn.Release()
		recorded, err = recordOutcomes(ctx, pgxExecutor{q: conn}, w.client.sql.recordSkipping, &rows)
		return err
	})

	// Each statement on the pool commits as it ends: see ownQuerier.
	q := w.client.own(w.client.pool)
	for _, r := range batch {
		row, found := recorded[keyOf(r.t)]
		switch {
		case err != nil:
			r.err = err
		case found && !row.recorded:
			r.held = true
		default:
			r.ended, r.err = w.recorded(ctx, q, r.t, r.o, found, row)
		}
		close(r.done)
	}
}

// finishAll finishes the attempts ts, each with the outcome that outcomeOf
// returns for its index, at the same time, so that their outcomes go into
// one batch.
func (w *Worker) finishAll(record context.Context, ts []*liveAttempt, outcomeOf func(i int) outcome, start time.Time) {
	var finishing sync.WaitGroup
	for i, t := range ts {
		finishing.Go(func() { w.finish(record, t, outcomeOf(i), start) })
	}
	finishing.Wait()
}

// outcomeRows holds outcomes as the parallel arrays that recordSQL takes.
type outcomeRows struct {
	ids     []int64
	tokens  []int32
	kinds   []string
	delays  []int64   // in microseconds
	notes   []*string // errors and notes, as JSON objects
	handled []bool
}

// add adds outcome o of attempt t. A cancel is recorded as a release: only
// the task's row says whether its cancel was requested, and without a
// request, the task goes back to its queue, whatever the worker read before.
func (r *outcomeRows) add(t *liveAttempt, o outcome) {
	kind, delay := "release", int64(0)
	var note *string
	switch o.kind {
	default: // a release or a cancel
	case outcomeRetry, outcomeDead:
		kind = "dead"
		if o.kind == outcomeRetry {
			kind, delay = "retryable", o.delay.Microseconds()
		}
		s := errorJSON(o)
		note = &s
	case outcomeSnooze:
		kind, delay = "snooze", o.delay.Microseconds()
		if o.msg != "" { // a snooze without a message records no note
			s := errorJSON(o)
			note = &s
		}
	}

	r.ids = append(r.ids, t.att.id)
	r.tokens = append(r.tokens, t.att.token)
	r.kinds = append(r.kinds, kind)
	r.delays = append(r.delays, delay)
	r.notes = append(r.notes, note)
	r.handled = append(r.handled, t.reg != nil)
}

// recordedOutcome is a row of recordSQL or recordSkippingSQL.
type recordedOutcome struct {
	queue, group, state string
	recorded            bool // false: another transaction held the row, and recordSkippingSQL skipped it
}

// attemptKey identifies an attempt: its task, and the lease token that it
// holds. A batch can hold two attempts of one task: one that lost its
// lease, and the attempt that took the task after it.
type attemptKey struct {
	id    int64
	token int32
}

// keyOf returns the key of attempt t.
func keyOf(t *liveAttempt) attemptKey {
	return attemptKey{t.att.id, t.att.token}
}

// recordOutcomes runs sql, recordSQL or recordSkippingSQL, on rows, and
// returns the rows that it found, by attempt.
func recordOutcomes(ctx context.Context, q querier, sql string, rows *outcomeRows) (map[attemptKey]recordedOutcome, error) {
	r, err := q.query(ctx, sql, rows.ids, rows.tokens, rows.kinds, rows.delays, rows.notes, rows.handled)
	if err != nil {
		return nil, err
	}
	defer r.Close()

	found := make(map[attemptKey]recordedOutcome, len(rows.ids))
	for r.Next() {
		var key attemptKey
		var o recordedOutcome
		if err := r.Scan(&key.id, &key.token, &o.queue, &o.group, &o.state, &o.recorded); err != nil {
			return nil, err
		}
		found[key] = o
	}
	return found, r.Err()
}
