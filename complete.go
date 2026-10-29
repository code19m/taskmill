package taskmill

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

// completer records successful attempts in batches: it gathers the results
// that arrive within a moment of each other and completes them together, so
// a busy worker needs far fewer statements, while an idle one adds only that
// moment. It keeps completions whose FIFO groups other transactions hold, and
// tries them again shortly, so that it never waits for another transaction's
// lock while holding a connection.
func (w *Worker) completer(ctx context.Context) {
	defer close(w.completerDone)
	record := context.WithoutCancel(ctx)
	var held []*tracked // completions whose rows other transactions held
	delay := heldRetryDelay
	retryHeld := time.NewTimer(delay)
	retryHeld.Stop()
	for {
		batch := held
		select {
		case t := <-w.completions:
			batch = append(batch, t)
		case <-retryHeld.C:
		case <-ctx.Done():
			// Record what finished before shutdown, then stop: every attempt
			// has returned, so nothing more arrives. Completions whose groups
			// are still held wait for the locks now: there is no later try.
			for len(w.completions) > 0 {
				batch = append(batch, <-w.completions)
			}
			w.completeWaiting(record, w.flush(record, batch))
			return
		}
		// Retry held completions soon, then less often while they stay held.
		wasHeld := len(held) > 0
		held = w.flush(record, w.gather(batch))
		switch {
		case len(held) == 0:
			delay = heldRetryDelay
		case wasHeld:
			delay = min(2*delay, heldRetryMax)
		}
		if len(held) > 0 {
			retryHeld.Reset(delay)
		}
	}
}

// gather adds to batch the completions that are ready, then those that
// arrive within completionLinger, so that a worker whose attempts finish one
// at a time still records them in batches.
func (w *Worker) gather(batch []*tracked) []*tracked {
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

// completeWaiting completes tasks whose groups other transactions held,
// waiting for those locks.
func (w *Worker) completeWaiting(ctx context.Context, ts []*tracked) {
	if len(ts) == 0 {
		return
	}
	var done []completedTask
	err := w.whileLeased(ctx, ts, func(ctx context.Context) error {
		return w.client.inTx(ctx, func(q querier) error {
			var err error
			done, err = w.client.complete(ctx, w.logger, q, completionsOf(ts))
			return err
		})
	})
	w.completed(ctx, ts, done, err)
}

// flush completes a batch of successful attempts. Ungrouped tasks complete
// in one statement. Grouped tasks complete in one transaction, which first
// locks their groups' rows, skipping rows that other transactions hold. It
// returns the tasks whose groups were held, for example by a transaction that
// enqueues into them, so that the completer tries them again later instead of
// waiting for those locks; they stay tracked, so their leases are renewed.
func (w *Worker) flush(ctx context.Context, batch []*tracked) (held []*tracked) {
	var plain, grouped []*tracked
	for _, t := range batch {
		if t.info.GroupKey == "" {
			plain = append(plain, t)
		} else {
			grouped = append(grouped, t)
		}
	}

	if len(plain) > 0 {
		var done []completedTask
		err := w.whileLeased(ctx, plain, func(ctx context.Context) error {
			var err error
			done, err = w.client.complete(ctx, w.logger, pgxQuerier{q: w.client.pool}, completionsOf(plain))
			return err
		})
		w.completed(ctx, plain, done, err)
	}
	if len(grouped) == 0 {
		return nil
	}
	var done []completedTask
	err := w.whileLeased(ctx, grouped, func(ctx context.Context) error {
		return w.client.inTx(ctx, func(q querier) error {
			var err error
			held, done, err = w.completeLocked(ctx, q, grouped)
			return err
		})
	})
	if err != nil {
		w.completed(ctx, grouped, nil, err)
		return nil
	}
	w.completed(ctx, slices.DeleteFunc(grouped, func(t *tracked) bool { return slices.Contains(held, t) }), done, nil)
	return held
}

// completeLocked completes, in q's transaction, the grouped tasks whose
// group rows and task rows it can lock without waiting. It returns the
// others, which other transactions hold, and the tasks it completed. A task
// whose group has no row, which should not happen, completes all the same.
func (w *Worker) completeLocked(ctx context.Context, q querier, ts []*tracked) (held []*tracked, done []completedTask, err error) {
	queues := make([]string, len(ts))
	groups := make([]string, len(ts))
	for i, t := range ts {
		queues[i], groups[i] = t.info.Queue, t.info.GroupKey
	}
	groupLocked, groupExists, err := lockedRows(ctx, q, w.client.sql.lockGroups, func(r rows) ([2]string, bool, error) {
		var key [2]string
		var locked bool
		err := r.Scan(&key[0], &key[1], &locked)
		return key, locked, err
	}, queues, groups)
	if err != nil {
		return nil, nil, err
	}
	var mine []*tracked
	for _, t := range ts {
		key := [2]string{t.info.Queue, t.info.GroupKey}
		if groupExists[key] && !groupLocked[key] {
			held = append(held, t)
		} else {
			mine = append(mine, t)
		}
	}
	if len(mine) == 0 {
		return held, nil, nil
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
	var ready []*tracked
	for _, t := range mine {
		switch {
		case taskLocked[t.att.id]:
			ready = append(ready, t)
		case owned[t.att.id]:
			held = append(held, t)
		}
	}
	if len(ready) > 0 {
		if done, err = w.client.complete(ctx, w.logger, q, completionsOf(ready)); err != nil {
			return nil, nil, err
		}
	}
	return held, done, nil
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

// completed stops tracking completed attempts, counts the tasks that their
// committed completion finished, and logs a failure to record them.
func (w *Worker) completed(ctx context.Context, ts []*tracked, done []completedTask, err error) {
	for _, t := range ts {
		w.untrack(t)
	}
	if err == nil {
		w.countCompleted(done)
	}
	if err != nil {
		w.logger.LogAttrs(ctx, slog.LevelWarn, "complete tasks failed; they are rescued when their leases expire",
			slog.Int(logKeyTasks, len(ts)), slog.Any(logKeyError, err))
	}
}

// whileLeased runs fn until it succeeds, waiting longer after each failed
// try, for as long as the leases of ts may still be held when it starts: by
// then the tasks are rescued, and a result recorded later would be refused.
// A database outage or failover shorter than a lease thus loses no results,
// while an error that persists ends the tries. Each try gets its own timeout.
func (w *Worker) whileLeased(ctx context.Context, ts []*tracked, fn func(context.Context) error) error {
	// The deadline is fixed now: the leases are still renewed while the
	// tries go on, which would otherwise never let them end.
	deadline := w.leaseEnd(ts)
	delay := recordRetryFirst
	for {
		tryCtx, cancel := context.WithTimeout(ctx, recordTimeout)
		err := fn(tryCtx)
		cancel()
		w.resultFailures.observe(ctx, err)
		if err == nil || ctx.Err() != nil || time.Now().Add(delay).After(deadline) {
			return err
		}
		select {
		case <-ctx.Done():
			return err
		case <-time.After(delay):
		}
		delay = min(2*delay, recordRetryMax)
	}
}

// leaseEnd returns when the first of the leases of ts may expire, as far as
// the worker knows.
func (w *Worker) leaseEnd(ts []*tracked) time.Time {
	var end time.Time
	for i, t := range ts {
		t.mu.Lock()
		e := t.renewedAt.Add(w.cfg.LeaseDuration)
		t.mu.Unlock()
		if i == 0 || e.Before(end) {
			end = e
		}
	}
	return end
}

// completionsOf returns the completions of ts.
func completionsOf(ts []*tracked) []completion {
	cs := make([]completion, len(ts))
	for i, t := range ts {
		cs[i] = completion{id: t.att.id, token: t.att.token, discard: t.att.discard}
	}
	return cs
}

// track starts tracking t, so that check-ins renew its lease.
func (w *Worker) track(t *tracked) {
	w.mu.Lock()
	w.attempts[t.att.id] = t
	w.mu.Unlock()
}

func (w *Worker) untrack(t *tracked) {
	w.mu.Lock()
	if w.attempts[t.att.id] == t {
		delete(w.attempts, t.att.id)
	}
	w.mu.Unlock()
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

// historyGap is how long completions skip history after one found no history
// partition for its day.
const historyGap = time.Minute

// complete records successful attempts: it moves their tasks to history, or
// deletes those whose kind keeps none, and advances their FIFO groups. It
// returns the tasks it completed; an attempt that lost its lease is left
// out. q must be a transaction if any task belongs to a FIFO group. It logs
// to the logger of the worker that completes.
func (c *Client) complete(ctx context.Context, logger *slog.Logger, q querier, cs []completion) ([]completedTask, error) {
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

	done, err := c.completeTasks(ctx, q, ids, tokens, discard)
	if err != nil {
		c.noteMissingHistory(ctx, logger, err)
		return nil, fmt.Errorf("taskmill: complete: %w", err)
	}

	var heads []groupLeave
	for _, t := range sortedByGroup(done) {
		heads = append(heads, groupLeave{queue: t.queue, group: t.group, tasks: 1, head: true})
	}
	if len(heads) > 0 {
		if err := c.leaveGroups(ctx, q, heads); err != nil {
			return nil, fmt.Errorf("taskmill: complete: %w", err)
		}
	}
	return done, nil
}

func (c *Client) completeTasks(ctx context.Context, q querier, ids []int64, tokens []int32, discard []int64) ([]completedTask, error) {
	r, err := q.query(ctx, c.sql.complete, ids, tokens, discard)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	var done []completedTask
	for r.Next() {
		var t completedTask
		if err := r.Scan(&t.id, &t.queue, &t.group); err != nil {
			return nil, err
		}
		done = append(done, t)
	}
	return done, r.Err()
}

// noteMissingHistory makes completions skip history for a while if err says
// that the history has no partition for today: the maintainer could not
// create it, typically because the worker's role does not own the table.
// Completions keep working, and a retry of the failed one succeeds. The
// history table has no CHECK constraint, so a check violation on it can only
// mean a missing partition; the message is not read, as it may be
// translated.
func (c *Client) noteMissingHistory(ctx context.Context, logger *slog.Logger, err error) {
	pgErr, ok := errors.AsType[*pgconn.PgError](err)
	if !ok || pgErr.Code != "23514" || pgErr.TableName != "task_history" || pgErr.SchemaName != c.schema { // check_violation
		return
	}
	c.historyGapUntil.Store(time.Now().Add(historyGap).UnixNano())
	logger.LogAttrs(ctx, slog.LevelError, "task history has no partition for today; completed tasks skip history",
		slog.Any(logKeyError, err))
}

// sortedByGroup returns the grouped tasks in a fixed order of queue and group,
// so that concurrent transactions lock group rows in the same order and cannot
// deadlock.
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
