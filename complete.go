package taskmill

import (
	"context"
	"log/slog"
	"slices"
	"time"
)

// completer records successful attempts in batches: it gathers the results
// that arrive within a moment of each other and completes them together, so
// a busy worker needs far fewer statements, while an idle one adds only that
// moment. It keeps completions whose FIFO groups other transactions hold, and
// tries them again shortly, so that it never waits for another transaction's
// lock while holding a connection.
func (w *Worker) completer(ctx context.Context) {
	defer close(w.completerUp)
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
// waiting for those locks, within one record timeout.
func (w *Worker) completeWaiting(ctx context.Context, ts []*tracked) {
	if len(ts) == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(ctx, recordTimeout)
	defer cancel()
	err := w.client.inTx(ctx, func(q querier) error {
		_, err := w.client.complete(ctx, q, completions(ts))
		return err
	})
	w.completed(ctx, ts, err)
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
		err := retry(ctx, func(ctx context.Context) error {
			_, err := w.client.complete(ctx, pgxQuerier{q: w.client.pool}, completions(plain))
			return err
		})
		w.completed(ctx, plain, err)
	}
	if len(grouped) == 0 {
		return nil
	}
	err := retry(ctx, func(ctx context.Context) error {
		return w.client.inTx(ctx, func(q querier) error {
			var err error
			held, err = w.completeLocked(ctx, q, grouped)
			return err
		})
	})
	if err != nil {
		w.completed(ctx, grouped, err)
		return nil
	}
	w.completed(ctx, slices.DeleteFunc(grouped, func(t *tracked) bool { return slices.Contains(held, t) }), nil)
	return held
}

// completeLocked completes, in q's transaction, the grouped tasks whose
// group rows and task rows it can lock without waiting, and returns the
// others, which other transactions hold. A task whose group has no row, which
// should not happen, completes all the same.
func (w *Worker) completeLocked(ctx context.Context, q querier, ts []*tracked) (held []*tracked, err error) {
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
		return nil, err
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
		return held, nil
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
		return nil, err
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
		if _, err := w.client.complete(ctx, q, completions(ready)); err != nil {
			return nil, err
		}
	}
	return held, nil
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

// completed stops tracking completed attempts, and logs a failure to record
// them.
func (w *Worker) completed(ctx context.Context, ts []*tracked, err error) {
	for _, t := range ts {
		w.untrack(t)
	}
	if err != nil {
		w.logger.LogAttrs(ctx, slog.LevelError, "complete tasks failed; they are rescued when their leases expire",
			slog.Int(logKeyTasks, len(ts)), slog.Any(logKeyError, err))
	}
}

// retry runs fn up to completeAttempts times, giving each try its own
// timeout, until it succeeds.
func retry(ctx context.Context, fn func(context.Context) error) error {
	var err error
	for try := range completeAttempts {
		tryCtx, cancel := context.WithTimeout(ctx, recordTimeout)
		err = fn(tryCtx)
		cancel()
		if err == nil || try == completeAttempts-1 {
			break
		}
		time.Sleep(time.Duration(try+1) * completeRetryDelay)
	}
	return err
}

// completions returns the completions of ts.
func completions(ts []*tracked) []completion {
	cs := make([]completion, len(ts))
	for i, t := range ts {
		cs[i] = completion{id: t.att.id, token: t.att.token}
	}
	return cs
}

func (w *Worker) untrack(t *tracked) {
	w.mu.Lock()
	if w.attempts[t.att.id] == t {
		delete(w.attempts, t.att.id)
	}
	w.mu.Unlock()
}
