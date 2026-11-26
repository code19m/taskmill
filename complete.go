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
// while an idle one adds only that moment. A batch never waits for a row
// that another transaction holds: the completer keeps those completions and
// tries them again shortly. A few completions whose FIFO groups' rows were
// held wait for those rows, each on a goroutine of its own, for
// groupLockTimeout at most: see waitForGroup. On the completer's goroutine,
// only regainKeys, once a batch has committed, waits for another
// transaction's lock while holding a connection, and for keyLockTimeout at
// most. At shutdown, completeWaiting waits for the rows still held, until
// Run stops recording results.
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
			// has returned, so nothing more arrives but the completions that
			// completeHeld gives back, each before it empties its place in
			// groupWait. Completions whose rows are still held wait for the
			// locks now: there is no later try.
			batch = drain(w.completions, batch)
			for range groupWaits {
				w.groupWait <- struct{}{}
			}
			for range groupWaits {
				<-w.groupWait
			}
			blocked, groupHeld := w.completeBatch(record, drain(w.completions, batch))
			w.completeWaiting(record, append(blocked, groupHeld...))
			return
		}

		// Retry blocked completions soon, then less often while they stay blocked.
		wasBlocked := len(blocked) > 0
		var groupHeld []*liveAttempt
		blocked, groupHeld = w.completeBatch(record, gather(w.completions, batch))
		blocked = append(blocked, w.waitForGroup(record, groupHeld)...)
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

// drain adds to batch the results that ch holds.
func drain[T any](ch <-chan T, batch []T) []T {
	for len(ch) > 0 {
		batch = append(batch, <-ch)
	}
	return batch
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
			w.counts.blocked.Add(1)
			w.logger.LogAttrs(ctx, slog.LevelWarn, "completion waits for a row that another transaction holds",
				slog.Int64(logKeyTaskID, t.att.id), slog.String(logKeyKind, t.info.Kind),
				slog.Duration(logKeyDuration, now.Sub(t.blockedSince)))
		}
	}
}

// gather adds to batch the results from ch that are ready, then those that
// arrive within completionLinger, up to completionBatch in all, so that a
// worker whose attempts finish one at a time still records them in batches.
func gather[T any](ch <-chan T, batch []T) []T {
ready:
	for len(batch) < completionBatch {
		select {
		case t := <-ch:
			batch = append(batch, t)
		default:
			break ready
		}
	}

	linger := time.NewTimer(completionLinger)
	defer linger.Stop()
	for len(batch) < completionBatch {
		select {
		case t := <-ch:
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
		return w.client.inTx(ctx, w.client.pool, func(q querier) error {
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
// hold. It returns the tasks whose rows were held, for example by a
// transaction that cancels a task, and the tasks whose groups' rows were
// held, for example by a transaction that enqueues into the group, so that
// the completer tries them again instead of waiting for those locks; they
// stay tracked, so their leases are renewed once the rows are free.
func (w *Worker) completeBatch(ctx context.Context, batch []*liveAttempt) (blocked, groupHeld []*liveAttempt) {
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
			conn, err := w.acquire(ctx)
			if err != nil {
				return err
			}
			defer conn.Release()
			done, held, err = w.client.completeSkipping(ctx, w.logger, pgxExecutor{q: conn}, completionsOf(plain))
			return err
		})
		if err == nil {
			blocked = slices.DeleteFunc(slices.Clone(plain), func(t *liveAttempt) bool { return !held[t.att.id] })
			plain = slices.DeleteFunc(plain, func(t *liveAttempt) bool { return held[t.att.id] })
		}
		w.afterComplete(ctx, plain, done, err)
	}

	if len(grouped) == 0 {
		return blocked, nil
	}
	var done []completedTask
	var taskHeld []*liveAttempt
	err := w.whileLeased(ctx, grouped, func(ctx context.Context) error {
		conn, err := w.acquire(ctx)
		if err != nil {
			return err
		}
		defer conn.Release()
		return w.client.inTx(ctx, conn, func(q querier) error {
			var err error
			groupHeld, taskHeld, done, err = w.completeLocked(ctx, q, w.client.sql.lockGroups, grouped)
			return err
		})
	})
	if err != nil {
		w.afterComplete(ctx, grouped, nil, err)
		return blocked, nil
	}

	held := make(map[*liveAttempt]bool, len(groupHeld)+len(taskHeld))
	for _, t := range slices.Concat(groupHeld, taskHeld) {
		held[t] = true
	}
	w.afterComplete(ctx, slices.DeleteFunc(grouped, func(t *liveAttempt) bool { return held[t] }), done, nil)
	return append(blocked, taskHeld...), groupHeld
}

// waitForGroup starts completeHeld for the completions of groupHeld, whose
// groups' rows other transactions held, that may wait for their groups'
// rows now, while fewer than groupWaits wait, and returns the others. A
// completion that waited before in vain, for its group's row or its own,
// waits only while no other does. So
// busy groups, whose holders hand their rows over within moments, get
// several waits at a time, while a transaction that holds the rows of many
// groups for long soon ties up one connection of the worker, which waits
// for one of them at a time.
func (w *Worker) waitForGroup(ctx context.Context, groupHeld []*liveAttempt) []*liveAttempt {
	now := time.Now()
	return slices.DeleteFunc(groupHeld, func(t *liveAttempt) bool {
		waitedBefore := !t.waitAfter.IsZero()
		if now.Before(t.waitAfter) || waitedBefore && len(w.groupWait) > 0 {
			return false
		}
		select {
		case w.groupWait <- struct{}{}:
			go w.completeHeld(ctx, t)
			return true
		default:
			return false
		}
	})
}

// groupLockTimeout bounds the wait of completeHeld for a FIFO group's row:
// long enough for a short transaction that enqueues into the group to
// commit, as keyLockTimeout is for a dedupe key's row, and far shorter than
// a lease, whose renewal skips the task's row while completeHeld holds it.
const groupLockTimeout = "100ms"

// completeHeld completes t, whose FIFO group's row other transactions held,
// in a transaction of its own that waits for that row, for groupLockTimeout
// at most, and then empties its place in w.groupWait. PostgreSQL hands a
// row to those who wait for it in turn. So a group into which short
// transactions enqueue one after another, holding its row most of the time,
// still completes its tasks as fast as they are enqueued, while tries that
// skip the held row rarely find it free, and its backlog would grow without
// bound: see "Completing a batch" in docs/design/schema-groups.md.
//
// The transaction waits for the group's row while it holds no other, and
// only then locks the task's row, skipping it if another transaction holds
// it, as completeLocked does for a batch: it writes nothing before it holds
// both, and never waits for a running task's row while it holds a group's.
// If the wait runs out, as it does for a transaction that holds the row for
// longer, or the task's own row is held, or the transaction fails,
// completeHeld gives t back to the completer, which tries it with its
// batches, skipping the rows while they are held, and lets it wait again a
// blockedRetryMax later at the soonest. The completer never waits for it.
func (w *Worker) completeHeld(ctx context.Context, t *liveAttempt) {
	defer func() { <-w.groupWait }()
	ts := []*liveAttempt{t}

	var groupHeld, taskHeld []*liveAttempt
	var done []completedTask
	err := func() error {
		ctx, cancel := context.WithTimeout(ctx, min(w.cfg.LeaseDuration/3, recordTimeout))
		defer cancel()
		conn, err := w.acquire(ctx)
		if err != nil {
			return err
		}
		defer conn.Release()
		return w.client.inTx(ctx, conn, func(q querier) error {
			// The transaction notifies nobody, so the timeout may last
			// until its commit: see liftLockTimeout.
			if _, err := q.exec(ctx, lockTimeoutSQL, groupLockTimeout); err != nil {
				return err
			}
			var err error
			groupHeld, taskHeld, done, err = w.completeLocked(ctx, q, w.client.sql.lockGroupWaiting, ts)
			return err
		})
	}()
	if err != nil || len(groupHeld)+len(taskHeld) > 0 {
		t.waitAfter = time.Now().Add(blockedRetryMax)
		w.completions <- t // before groupWait empties: see completer
		return
	}
	w.afterComplete(ctx, ts, done, nil)
}

// completeLocked completes, in q's transaction, the grouped tasks whose
// group rows lockGroups locks, and whose own rows it can lock without
// waiting. It locks them before it writes anything. It returns the others:
// those whose group rows other transactions hold, and those whose own rows
// they hold. It also returns the tasks it completed. A task whose group has
// no row, which should not happen, completes all the same, and leaveGroups
// reports it.
func (w *Worker) completeLocked(ctx context.Context, q querier, lockGroups string, ts []*liveAttempt) (groupHeld, taskHeld []*liveAttempt, done []completedTask, err error) {
	queues := make([]string, len(ts))
	groups := make([]string, len(ts))
	for i, t := range ts {
		queues[i], groups[i] = t.info.Queue, t.info.GroupKey
	}

	groupLocked, groupExists, err := lockedRows(ctx, q, lockGroups, func(r rows) (groupID, bool, error) {
		var key groupID
		var locked bool
		err := r.Scan(&key.queue, &key.group, &locked)
		return key, locked, err
	}, queues, groups)
	if err != nil {
		return nil, nil, nil, err
	}

	var mine []*liveAttempt
	for _, t := range ts {
		key := groupID{t.info.Queue, t.info.GroupKey}
		if groupExists[key] && !groupLocked[key] {
			groupHeld = append(groupHeld, t)
		} else {
			mine = append(mine, t)
		}
	}
	if len(mine) == 0 {
		return groupHeld, nil, nil, nil
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
		return nil, nil, nil, err
	}

	// Tasks no longer owned lost their leases, and are left out; the
	// complete statement would skip them anyway.
	var ready []*liveAttempt
	for _, t := range mine {
		switch {
		case taskLocked[t.att.id]:
			ready = append(ready, t)
		case owned[t.att.id]:
			taskHeld = append(taskHeld, t)
		}
	}

	if len(ready) > 0 {
		if done, err = w.client.complete(ctx, w.logger, q, completionsOf(ready)); err != nil {
			return nil, nil, nil, err
		}
	}
	return groupHeld, taskHeld, done, nil
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
// waiting, gives the tasks back the dedupe keys that their cancels freed, and
// logs a failure to record them.
func (w *Worker) afterComplete(ctx context.Context, ts []*liveAttempt, done []completedTask, err error) {
	for _, t := range ts {
		w.untrack(t)
	}

	if err == nil {
		w.counts.succeeded.Add(int64(len(done)))
		w.countCompleted(done)
		w.madeRoom(done)
		w.regainKeys(ctx, done)
	}
	if err != nil {
		ids := make([]int64, len(ts))
		for i, t := range ts {
			ids[i] = t.att.id
		}
		w.logger.LogAttrs(ctx, slog.LevelWarn, "complete tasks failed; they are rescued when their leases run out",
			slog.Int(logKeyTasks, len(ts)), taskIDs(ids), slog.Any(logKeyError, err))
	}
}

// keyLockTimeout bounds the wait of regainKeys for a dedupe key's row, as
// bulkSettings bound a bulk action's: long enough for an enqueue of the key
// to commit, and short enough that the completer barely pauses for a
// transaction that stays open.
const keyLockTimeout = "100ms"

// regainKeys gives the tasks of done, whose completion committed, the dedupe
// keys that their cancels freed while they ran, in a transaction of its own:
// see regainKeysSQL. A transaction that claims such a key makes the claim
// wait. The completion's statements must not wait for it: they hold the rows
// of up to completionBatch tasks and of their groups, which could deadlock
// with that transaction, and the completer records nothing else meanwhile.
// So the claim waits keyLockTimeout at most, and then gives up: the task
// stays without its key, and the worker logs it. The transaction notifies
// nobody, so the timeout may last until its commit: see liftLockTimeout.
func (w *Worker) regainKeys(ctx context.Context, done []completedTask) {
	n := 0
	for _, t := range done {
		if t.freed() {
			n++
		}
	}
	if n == 0 {
		return
	}

	ctx, cancel := context.WithTimeout(ctx, recordTimeout)
	defer cancel()
	err := w.client.inTx(ctx, w.client.pool, func(q querier) error {
		if _, err := q.exec(ctx, lockTimeoutSQL, keyLockTimeout); err != nil {
			return err
		}
		return w.client.regainKeys(ctx, q, done)
	})
	if err != nil {
		w.logger.LogAttrs(ctx, slog.LevelWarn, "completed tasks could not take back the dedupe keys that their cancels freed",
			slog.Int(logKeyTasks, n), slog.Any(logKeyError, err))
	}
}

// idsLogged is how many task IDs a line about many tasks names at most. Its
// attribute tasks says how many there are.
const idsLogged = 20

// taskIDs returns the attribute that names the first idsLogged of ids, so
// that an operator can find the tasks that a line is about, such as those
// that run twice.
func taskIDs(ids []int64) slog.Attr {
	return slog.Any(logKeyTaskIDs, ids[:min(len(ids), idsLogged)])
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
			signal(q.room)
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
	id         int64
	queue      string
	group      string
	freedKey   []byte     // the dedupe key that a cancel freed while the task ran
	freedUntil *time.Time // the end of that key's window
}

// freed reports whether a cancel freed a dedupe key of t while it ran.
func (t completedTask) freed() bool {
	return t.freedKey != nil && t.freedUntil != nil
}

// complete records successful attempts: it moves their tasks to history, or
// deletes those whose kind keeps none, and advances their FIFO groups. It
// waits for the rows that other transactions hold. It returns the tasks it
// completed; an attempt that lost its lease is left out. q must be a
// transaction if any task belongs to a FIFO group. It logs to the logger of
// the worker that completes.
func (c *Client) complete(ctx context.Context, logger *slog.Logger, q querier, cs []completion) ([]completedTask, error) {
	return c.completeThen(ctx, logger, q, cs, nil)
}

// completeThen is complete, with then run on the completed tasks after
// their rows and before their groups' rows: CompleteTx takes back the
// dedupe keys that their cancels freed there, since a key's row comes
// before a group's row.
func (c *Client) completeThen(ctx context.Context, logger *slog.Logger, q querier, cs []completion,
	then func([]completedTask) error,
) ([]completedTask, error) {
	fail := func(err error) error { return fmt.Errorf("taskmill: complete: %w", err) }
	done, _, err := c.completeTasks(ctx, logger, q, c.sql.complete, cs)
	if err != nil {
		return nil, fail(err)
	}
	if then != nil {
		if err := then(done); err != nil {
			return nil, fail(err)
		}
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
		if err := r.Scan(&t.id, &t.queue, &t.group, &completed, &t.freedKey, &t.freedUntil); err != nil {
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

// regainKeys gives the completed tasks of done the dedupe keys that their
// cancels freed while they ran, in q's transaction: see regainKeysSQL. It
// runs no statement if no cancel freed one.
func (c *Client) regainKeys(ctx context.Context, q querier, done []completedTask) error {
	var keys [][]byte
	var ids []int64
	var ends []time.Time
	for _, t := range done {
		if t.freed() {
			keys = append(keys, t.freedKey)
			ids = append(ids, t.id)
			ends = append(ends, *t.freedUntil)
		}
	}
	if len(ids) == 0 {
		return nil
	}

	_, err := q.exec(ctx, c.sql.regainKeys, keys, ids, ends)
	return err
}

// sortedByGroup returns the grouped tasks in the order of queue and group in
// which an enqueue takes the rows of several groups. It serves the one
// caller that waits for the rows of several groups, completeWaiting. Where
// the plan of the recount follows its arrays, as the nested loop over a
// large table does, the rows are then taken in the order of an enqueue, and
// the two cannot deadlock. No plan must follow them: see leaveGroups.
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
