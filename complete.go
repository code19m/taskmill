package taskmill

import (
	"cmp"
	"context"
	"fmt"
	"log/slog"
	"slices"
	"time"
)

// completer records successful attempts in batches, on record, until ctx
// ends: it gathers the results that arrive within a moment of each other and
// completes them together, so a busy worker needs far fewer statements,
// while an idle one adds only that moment. It keeps completions whose rows,
// or the rows of their FIFO groups, other transactions hold, and tries them
// again shortly, so that it never waits for another transaction's lock while
// holding a connection, and the completions of a batch never wait for one
// such row.
func (w *Worker) completer(ctx, record context.Context) {
	defer close(w.completerDone)
	var blocked []*liveAttempt // completions whose rows other transactions held
	delay := blockedRetryDelay
	retryBlocked := time.NewTimer(delay)
	retryBlocked.Stop()

	for {
		batch := blocked
		select {
		case t := <-w.completions:
			batch = append(batch, t)
		case <-retryBlocked.C:
		case <-ctx.Done():
			// Record what finished before shutdown, then stop: every attempt
			// has returned, so nothing more arrives. Completions whose groups
			// are still held wait for the locks now: there is no later try.
			for len(w.completions) > 0 {
				batch = append(batch, <-w.completions)
			}
			w.completeWaiting(record, w.completeBatch(record, batch))
			return
		}

		// Retry blocked completions soon, then less often while they stay blocked.
		wasBlocked := len(blocked) > 0
		blocked = w.completeBatch(record, w.gather(batch))
		w.warnBlocked(record, blocked)
		switch {
		case len(blocked) == 0:
			delay = blockedRetryDelay
		case wasBlocked:
			delay = min(2*delay, blockedRetryMax)
		}
		if len(blocked) > 0 {
			retryBlocked.Reset(delay)
		}
	}
}

// warnBlocked warns once of each completion of blocked that other
// transactions have held up for longer than the lease: a transaction that
// holds a task's row, or its group's, for that long is likely stuck, and
// the completion waits for it, also past the lease.
func (w *Worker) warnBlocked(ctx context.Context, blocked []*liveAttempt) {
	now := time.Now()
	for _, t := range blocked {
		switch {
		case t.blockedSince.IsZero():
			t.blockedSince = now
		case !t.blockedWarned && now.Sub(t.blockedSince) > w.cfg.LeaseDuration:
			t.blockedWarned = true
			w.logger.LogAttrs(ctx, slog.LevelWarn, "completion waits for a row that another transaction holds",
				slog.Int64(logKeyTaskID, t.att.id), slog.String(logKeyKind, t.info.Kind),
				slog.Duration(logKeyDuration, now.Sub(t.blockedSince)))
		}
	}
}

// gather adds to batch the completions that are ready, then those that
// arrive within completionLinger, so that a worker whose attempts finish one
// at a time still records them in batches.
func (w *Worker) gather(batch []*liveAttempt) []*liveAttempt {
ready:
	for len(batch) < completionBatch {
		select {
		case t := <-w.completions:
			batch = append(batch, t)
		default:
			break ready
		}
	}

	linger := time.NewTimer(completionLinger)
	defer linger.Stop()
	for len(batch) < completionBatch {
		select {
		case t := <-w.completions:
			batch = append(batch, t)
		case <-linger.C:
			return batch
		}
	}
	return batch
}

// completeWaiting completes tasks in one transaction, in which it waits for
// the rows of their FIFO groups that other transactions hold. The completer
// ends with it, for the tasks whose groups were held until then, and RunTask
// completes its task with it. It returns the tasks that it completed, once
// that is committed.
func (w *Worker) completeWaiting(ctx context.Context, ts []*liveAttempt) []completedTask {
	if len(ts) == 0 {
		return nil
	}

	var done []completedTask
	err := w.whileLeased(ctx, ts, func(ctx context.Context) error {
		return w.client.inTx(ctx, func(q querier) error {
			var err error
			done, err = w.client.complete(ctx, w.logger, q, completionsOf(ts))
			return err
		})
	})
	if err != nil {
		done = nil // the transaction did not commit
	}

	w.afterComplete(ctx, ts, done, err)
	return done
}

// completeBatch completes a batch of successful attempts. Ungrouped tasks
// complete in one statement, which skips the rows that other transactions
// hold. Grouped tasks complete in one transaction, which first locks their
// groups' rows and then their own, skipping rows that other transactions
// hold. It returns the tasks whose rows or groups' rows were held, for
// example by a transaction that cancels a task or enqueues into a group, so
// that the completer tries them again later instead of waiting for those
// locks; they stay tracked, so their leases are renewed once the rows are
// free.
func (w *Worker) completeBatch(ctx context.Context, batch []*liveAttempt) (blocked []*liveAttempt) {
	var plain, grouped []*liveAttempt
	for _, t := range batch {
		if t.info.GroupKey == "" {
			plain = append(plain, t)
		} else {
			grouped = append(grouped, t)
		}
	}

	if len(plain) > 0 {
		var done []completedTask
		var held map[int64]bool
		err := w.whileLeased(ctx, plain, func(ctx context.Context) error {
			var err error
			done, held, err = w.client.completeSkipping(ctx, w.logger, pgxExecutor{q: w.client.pool}, completionsOf(plain))
			return err
		})
		if err == nil {
			blocked = slices.DeleteFunc(slices.Clone(plain), func(t *liveAttempt) bool { return !held[t.att.id] })
			plain = slices.DeleteFunc(plain, func(t *liveAttempt) bool { return held[t.att.id] })
		}
		w.afterComplete(ctx, plain, done, err)
	}

	if len(grouped) == 0 {
		return blocked
	}
	var done []completedTask
	var heldGroups []*liveAttempt
	err := w.whileLeased(ctx, grouped, func(ctx context.Context) error {
		return w.client.inTx(ctx, func(q querier) error {
			var err error
			heldGroups, done, err = w.completeLocked(ctx, q, grouped)
			return err
		})
	})
	if err != nil {
		w.afterComplete(ctx, grouped, nil, err)
		return blocked
	}

	w.afterComplete(ctx, slices.DeleteFunc(grouped, func(t *liveAttempt) bool { return slices.Contains(heldGroups, t) }), done, nil)
	return append(blocked, heldGroups...)
}

// completeLocked completes, in q's transaction, the grouped tasks whose
// group rows and task rows it can lock without waiting. It returns the
// others, which other transactions hold, and the tasks it completed. A task
// whose group has no row, which should not happen, completes all the same,
// and leaveGroups reports it.
func (w *Worker) completeLocked(ctx context.Context, q querier, ts []*liveAttempt) (blocked []*liveAttempt, done []completedTask, err error) {
	queues := make([]string, len(ts))
	groups := make([]string, len(ts))
	for i, t := range ts {
		queues[i], groups[i] = t.info.Queue, t.info.GroupKey
	}

	groupLocked, groupExists, err := lockedRows(ctx, q, w.client.sql.lockGroups, func(r rows) (groupID, bool, error) {
		var key groupID
		var locked bool
		err := r.Scan(&key.queue, &key.group, &locked)
		return key, locked, err
	}, queues, groups)
	if err != nil {
		return nil, nil, err
	}

	var mine []*liveAttempt
	for _, t := range ts {
		key := groupID{t.info.Queue, t.info.GroupKey}
		if groupExists[key] && !groupLocked[key] {
			blocked = append(blocked, t)
		} else {
			mine = append(mine, t)
		}
	}
	if len(mine) == 0 {
		return blocked, nil, nil
	}

	ids := make([]int64, len(mine))
	tokens := make([]int32, len(mine))
	for i, t := range mine {
		ids[i], tokens[i] = t.att.id, t.att.token
	}

	taskLocked, owned, err := lockedRows(ctx, q, w.client.sql.lockLeased, func(r rows) (int64, bool, error) {
		var id int64
		var locked bool
		err := r.Scan(&id, &locked)
		return id, locked, err
	}, ids, tokens)
	if err != nil {
		return nil, nil, err
	}

	// Tasks no longer owned lost their leases, and are left out; the
	// complete statement would skip them anyway.
	var ready []*liveAttempt
	for _, t := range mine {
		switch {
		case taskLocked[t.att.id]:
			ready = append(ready, t)
		case owned[t.att.id]:
			blocked = append(blocked, t)
		}
	}

	if len(ready) > 0 {
		if done, err = w.client.complete(ctx, w.logger, q, completionsOf(ready)); err != nil {
			return nil, nil, err
		}
	}
	return blocked, done, nil
}

// lockedRows runs a statement that returns rows of a key and whether that
// key's row was locked, and returns the keys locked and the keys seen.
func lockedRows[K comparable](ctx context.Context, q querier, sql string, scan func(rows) (K, bool, error), args ...any) (locked, seen map[K]bool, err error) {
	r, err := q.query(ctx, sql, args...)
	if err != nil {
		return nil, nil, err
	}
	defer r.Close()
	locked, seen = map[K]bool{}, map[K]bool{}

	for r.Next() {
		key, isLocked, err := scan(r)
		if err != nil {
			return nil, nil, err
		}
		seen[key] = true
		if isLocked {
			locked[key] = true
		}
	}
	return locked, seen, r.Err()
}

// afterComplete stops tracking completed attempts, counts the tasks that their
// committed completion finished, wakes the fetches that those tasks kept
// waiting, and logs a failure to record them.
func (w *Worker) afterComplete(ctx context.Context, ts []*liveAttempt, done []completedTask, err error) {
	for _, t := range ts {
		w.untrack(t)
	}

	if err == nil {
		w.countCompleted(done)
		w.madeRoom(done)
	}
	if err != nil {
		w.logger.LogAttrs(ctx, slog.LevelWarn, "complete tasks failed; they are rescued when their leases run out",
			slog.Int(logKeyTasks, len(ts)), slog.Any(logKeyError, err))
	}
}

// madeRoom wakes the fetch loops of the queues in which the completions
// done, now committed, let a task start: a queue with a MaxRunning limit
// runs fewer tasks than before, and the next task of a FIFO group took its
// head's place. The fetch that follows a freed slot cannot see either. A
// slot is free as soon as its handler returns, before the completer records
// the result, so that fetch still counts the task as running, and finds the
// group's next task pending. The signals of a batch of completions gather in
// the loop's buffer of one: a batch costs a fetch or two, not one per task.
//
// Only this worker wakes, so a group's tasks follow each other without the
// notification of each promotion, which the client's throttle holds back
// for up to notifyWindow, and no other worker fetches more often for it. A
// group whose last task completed costs one fetch that finds nothing.
func (w *Worker) madeRoom(done []completedTask) {
	for _, t := range done {
		// A task of RunTask may be of a queue that Run does not serve.
		if q := w.queues[t.queue]; q != nil && (t.group != "" || q.maxRunning.Load() > 0) {
			signal(q.freed)
		}
	}
}

// completionsOf returns the completions of ts.
func completionsOf(ts []*liveAttempt) []completion {
	cs := make([]completion, len(ts))
	for i, t := range ts {
		cs[i] = completion{id: t.att.id, token: t.att.token, discard: t.att.discard}
	}
	return cs
}

// completion identifies an attempt whose task succeeded.
type completion struct {
	id      int64
	token   int32
	discard bool // delete the task without keeping it in history
}

// completedTask is a task that a completion finished.
type completedTask struct {
	id    int64
	queue string
	group string
}

// complete records successful attempts: it moves their tasks to history, or
// deletes those whose kind keeps none, and advances their FIFO groups. It
// waits for the rows that other transactions hold. It returns the tasks it
// completed; an attempt that lost its lease is left out. q must be a
// transaction if any task belongs to a FIFO group. It logs to the logger of
// the worker that completes.
func (c *Client) complete(ctx context.Context, logger *slog.Logger, q querier, cs []completion) ([]completedTask, error) {
	fail := func(err error) error { return fmt.Errorf("taskmill: complete: %w", err) }
	done, _, err := c.completeTasks(ctx, logger, q, c.sql.complete, cs)
	if err != nil {
		return nil, fail(err)
	}

	var heads []groupLeave
	for _, t := range sortedByGroup(done) {
		heads = append(heads, groupLeave{id: t.id, queue: t.queue, group: t.group, head: true})
	}
	if err := c.leaveGroups(ctx, logger, q, heads); err != nil {
		return nil, fail(err)
	}
	return done, nil
}

// completeSkipping completes the attempts cs of tasks outside FIFO groups,
// as complete does, but skips the rows that other transactions hold. It
// returns the tasks it completed, and the IDs of the attempts whose tasks it
// skipped for that.
func (c *Client) completeSkipping(ctx context.Context, logger *slog.Logger, q querier, cs []completion) (done []completedTask, held map[int64]bool, err error) {
	done, held, err = c.completeTasks(ctx, logger, q, c.sql.completeSkipping, cs)
	if err != nil {
		return nil, nil, fmt.Errorf("taskmill: complete: %w", err)
	}
	return done, held, nil
}

// completeTasks runs sql, completeSQL or completeSkippingSQL, on the
// attempts cs, and returns the tasks that it completed, and those that it
// skipped because other transactions held them.
func (c *Client) completeTasks(ctx context.Context, logger *slog.Logger, q querier, sql string, cs []completion) (done []completedTask, held map[int64]bool, err error) {
	ids := make([]int64, len(cs))
	tokens := make([]int32, len(cs))
	discard := make([]int64, 0) // not nil: the statement compares with every element
	keep := time.Now().UnixNano() >= c.historyGapUntil.Load()
	for i, x := range cs {
		ids[i], tokens[i] = x.id, x.token
		if x.discard || !keep {
			discard = append(discard, x.id)
		}
	}

	r, err := q.query(ctx, sql, ids, tokens, discard)
	if err != nil {
		c.noteMissingHistory(ctx, logger, err)
		return nil, nil, err
	}
	defer r.Close()

	for r.Next() {
		var t completedTask
		var completed bool
		if err := r.Scan(&t.id, &t.queue, &t.group, &completed); err != nil {
			return nil, nil, err
		}
		if completed {
			done = append(done, t)
			continue
		}
		if held == nil {
			held = make(map[int64]bool)
		}
		held[t.id] = true
	}
	if err := r.Err(); err != nil {
		c.noteMissingHistory(ctx, logger, err)
		return nil, nil, err
	}
	return done, held, nil
}

// sortedByGroup returns the grouped tasks in the order of queue and group in
// which an enqueue takes the rows of several groups. It serves the one
// caller that waits for such rows, completeWaiting. Where the plan of the
// recount follows its arrays, as the nested loop over a large table does,
// the rows are then taken in the order of an enqueue, and the two cannot
// deadlock. No plan must follow them: see leaveGroups.
func sortedByGroup(ts []completedTask) []completedTask {
	var grouped []completedTask
	for _, t := range ts {
		if t.group != "" {
			grouped = append(grouped, t)
		}
	}

	slices.SortFunc(grouped, func(a, b completedTask) int {
		return cmp.Or(cmp.Compare(a.queue, b.queue), cmp.Compare(a.group, b.group))
	})
	return grouped
}
