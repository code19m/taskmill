package taskmill

import (
	"cmp"
	"context"
	"errors"
	"log/slog"
	"math/rand/v2"
	"slices"
	"time"

	"github.com/jackc/pgx/v5"
)

// fetchLoop fetches tasks for one queue whenever it has free capacity, until
// ctx is canceled.
func (w *Worker) fetchLoop(ctx context.Context, r *run, q *queueState) {
	fetches := newFailures(w, "fetch tasks", slog.String(logKeyQueue, q.name))

	// notified reports that a notification caused the last fetch, and held
	// that the queue's concurrency limit held tasks back.
	notified, held := false, false
	wait := func(pause bool) (ok bool) {
		ok, notified = waitForWork(ctx, q, w.pollWait(held), pause)
		return ok
	}

	for ctx.Err() == nil {
		free := cap(q.slots) - len(q.slots)
		if q.paused.Load() || free == 0 {
			if !wait(false) {
				return
			}
			continue
		}
		tasks, more, limited, err := w.fetch(ctx, q, free)
		held = limited
		fetches.observe(ctx, err)
		w.dispatch(ctx, r, q, tasks)
		if err != nil {
			if !wait(false) {
				return
			}
			continue
		}
		// A notification wakes every worker of the queue, and most find
		// nothing: those pause before acting on the next one.
		if !more && !wait(notified && len(tasks) == 0) {
			return
		}
	}
}

// pollWait returns how long an idle queue waits before it fetches again,
// unless a notification or a freed slot wakes it first. While notifications
// reach the worker, it polls every idlePoll, give or take a tenth so that
// workers started together poll apart: only for notifications that were
// never sent, such as the trailing one of a producer that crashed. Otherwise
// it polls every PollInterval, and so does a queue whose installation-wide
// limit held tasks back: no notification says when another worker's task of
// that queue ends.
func (w *Worker) pollWait(held bool) time.Duration {
	if held || !w.notifying.Load() {
		return w.cfg.PollInterval
	}
	d := max(w.timing.idlePoll, w.cfg.PollInterval)
	if spread := d / 5; spread > 0 {
		d += rand.N(spread) - spread/2 //nolint:gosec // spreads polls, no security role
	}
	return d
}

// waitForWork waits for a nudge, a freed slot, or the poll. It reports false
// if ctx ends first, and whether a nudge woke it. If pause is set, it first
// lets a moment pass, which gathers the nudges that arrive meanwhile into one
// fetch.
func waitForWork(ctx context.Context, q *queueState, poll time.Duration, pause bool) (ok, nudged bool) {
	if pause {
		select {
		case <-ctx.Done():
			return false, false
		case <-time.After(emptyFetchCooldown):
		}
	}
	timer := time.NewTimer(poll)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false, false
	case <-q.wake:
		return true, true
	case <-q.freed:
		return true, false
	case <-timer.C:
		return true, false
	}
}

// fetch leases up to slots tasks of q, one per free slot, then fills the
// batches of batch kinds among them with more tasks of those kinds, so that
// every fetched task starts at once. A queue with an installation-wide
// concurrency limit fetches only up to its remaining capacity. more reports
// that the first lease took all it asked for, so more tasks may be waiting,
// and held that the limit kept it from asking for as many as slots.
// If filling the batches fails outside a transaction, it returns the tasks
// leased before the error with the error: they must run.
func (w *Worker) fetch(ctx context.Context, q *queueState, slots int) (tasks []*tracked, more, held bool, err error) {
	// The statements run on a context that outlives ctx: once tasks are
	// leased, they must reach the worker even if it is stopping.
	dbCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), recordTimeout)
	defer cancel()

	// A queue with a concurrency limit fetches in one transaction, which
	// checks the capacity first. Other queues run each fetch statement in a
	// transaction of its own, sent in one round trip.
	lease, allowed := leaseFunc(w.leaseAlone), maxFetch
	var tx pgx.Tx
	if q.maxRunning.Load() > 0 {
		if tx, allowed, err = w.beginLimited(dbCtx, q); err != nil {
			return nil, false, false, err
		}
		defer func() { _ = tx.Rollback(dbCtx) }() // a no-op after commit
		lease = w.leaseIn(tx)
	}

	limit := min(slots, allowed)
	out, err := lease(dbCtx, q, w.client.sql.fetch, limit)
	if err != nil {
		return nil, false, false, err
	}
	more = limit > 0 && len(out) == limit
	held = allowed < slots && len(out) == limit
	extra, err := w.fillBatches(dbCtx, q, lease, out, allowed-len(out))
	if err != nil && tx != nil {
		return nil, false, false, err // rolled back: nothing was leased
	}
	out = append(out, extra...)
	if tx != nil {
		if err := tx.Commit(dbCtx); err != nil {
			return nil, false, false, err
		}
	}

	slices.SortFunc(out, func(a, b *tracked) int {
		return cmp.Or(
			cmp.Compare(b.info.Priority, a.info.Priority),
			a.info.RunAt.Compare(b.info.RunAt),
			cmp.Compare(a.info.ID, b.info.ID),
		)
	})
	return out, more, held, err
}

// leaseFunc runs a fetch statement for up to limit tasks of q, with extra
// arguments after the standard four, and returns the leased tasks.
type leaseFunc func(ctx context.Context, q *queueState, sql string, limit int, extra ...any) ([]*tracked, error)

// fillBatches leases more tasks of the batch kinds among fetched, up to room
// in all, to fill their batches. It returns what it leased before an error.
func (w *Worker) fillBatches(ctx context.Context, q *queueState, lease leaseFunc, fetched []*tracked, room int) ([]*tracked, error) {
	var extra []*tracked
	for kind, n := range batchCounts(fetched) {
		size := w.handlers[kind].opts.BatchSize
		fill := min((size-n%size)%size, room-len(extra))
		if fill <= 0 {
			continue
		}
		more, err := lease(ctx, q, w.client.sql.fetchKind, fill, kind, maxFetch)
		if err != nil {
			return extra, err
		}
		extra = append(extra, more...)
	}
	return extra, nil
}

// beginLimited begins the fetch transaction of a queue with a concurrency
// limit, and returns how many more of its tasks may run.
func (w *Worker) beginLimited(ctx context.Context, q *queueState) (pgx.Tx, int, error) { //nolint:ireturn // pgx represents a transaction as an interface
	tx, err := w.client.pool.Begin(ctx)
	if err != nil {
		return nil, 0, err
	}
	allowed := 0
	if _, err = tx.Exec(ctx, planSettings); err == nil {
		allowed, err = w.capacity(ctx, tx, q.name)
	}
	if err != nil {
		_ = tx.Rollback(ctx)
		return nil, 0, err
	}
	return tx, allowed, nil
}

// leaseIn returns a leaseFunc that runs fetch statements in tx.
func (w *Worker) leaseIn(tx pgx.Tx) leaseFunc {
	return func(ctx context.Context, q *queueState, sql string, limit int, extra ...any) ([]*tracked, error) {
		if limit <= 0 {
			return nil, nil
		}
		r, err := tx.Query(ctx, sql, w.leaseArgs(q, limit, extra)...)
		if err != nil {
			return nil, err
		}
		return w.scanAllLeased(r)
	}
}

// capacity locks a limited queue's row and returns how many more of its tasks
// may run. It counts running tasks in a statement of its own, after taking the
// lock, so that it sees what concurrent fetches committed.
func (w *Worker) capacity(ctx context.Context, tx pgx.Tx, queue string) (int, error) {
	var maxRunning *int
	if err := tx.QueryRow(ctx, w.client.sql.lockQueue, queue).Scan(&maxRunning); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return maxFetch, nil
		}
		return 0, err
	}
	if maxRunning == nil {
		return maxFetch, nil
	}
	var running int
	if err := tx.QueryRow(ctx, w.client.sql.countRunning, queue).Scan(&running); err != nil {
		return 0, err
	}
	return max(*maxRunning-running, 0), nil
}

// planSettings keep a fetch on its ordered index. A queue's table changes so
// fast that the planner's statistics are often far off: with too low an
// estimate of the available tasks, it may read all of them and sort them, on
// every fetch. Turning sorts and bitmap scans off for the fetch's transaction
// rules that out.
const planSettings = `SELECT set_config('enable_sort', 'off', true), set_config('enable_bitmapscan', 'off', true)`

// leaseAlone runs a fetch statement for up to limit tasks of q, with extra
// arguments after the standard four, in a transaction of its own with
// planSettings, all in one round trip. It returns the leased tasks.
func (w *Worker) leaseAlone(ctx context.Context, q *queueState, sql string, limit int, extra ...any) ([]*tracked, error) {
	if limit <= 0 {
		return nil, nil
	}
	b := &pgx.Batch{}
	b.Queue("BEGIN")
	b.Queue(planSettings)
	b.Queue(sql, w.leaseArgs(q, limit, extra)...)
	b.Queue("COMMIT")
	br := w.client.pool.SendBatch(ctx, b)
	defer func() { _ = br.Close() }()
	for range 2 {
		if _, err := br.Exec(); err != nil {
			return nil, err
		}
	}
	r, err := br.Query()
	if err != nil {
		return nil, err
	}
	out, err := w.scanAllLeased(r)
	if err != nil {
		return nil, err
	}
	// The tasks are leased once the transaction commits.
	if _, err := br.Exec(); err != nil {
		return nil, err
	}
	return out, nil
}

// leaseArgs returns a fetch statement's arguments.
func (w *Worker) leaseArgs(q *queueState, limit int, extra []any) []any {
	return append([]any{q.name, limit, w.cfg.LeaseDuration.Microseconds(), w.id}, extra...)
}

// scanAllLeased reads the tasks that a fetch statement leased.
func (w *Worker) scanAllLeased(r pgx.Rows) ([]*tracked, error) {
	defer r.Close()
	now := time.Now()
	var out []*tracked
	for r.Next() {
		t, err := w.scanLeased(r, now)
		if err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, r.Err()
}

// batchCounts counts the fetched tasks of each batch kind.
func batchCounts(ts []*tracked) map[string]int {
	var counts map[string]int
	for _, t := range ts {
		if t.reg != nil && t.reg.runBatch != nil {
			if counts == nil {
				counts = make(map[string]int)
			}
			counts[t.info.Kind]++
		}
	}
	return counts
}

// scanLeased reads one row of leasedColumns.
func (w *Worker) scanLeased(r pgx.Rows, now time.Time) (*tracked, error) {
	var (
		info                    TaskInfo
		priority, att, maxAtt   int16
		token                   int32
		metadata, payload, errs []byte
		attemptedAt             *time.Time
	)
	err := r.Scan(&info.ID, &info.Queue, &info.Kind, &priority, &att, &maxAtt, &token, &info.GroupKey,
		&metadata, &payload, &errs, &info.CreatedAt, &info.RunAt, &attemptedAt)
	if err != nil {
		return nil, err
	}
	info.State = StateRunning
	info.Priority = Priority(priority)
	info.Attempt = int(att)
	info.MaxAttempts = int(maxAtt)
	info.Payload = payload
	info.AttemptedAt = deref(attemptedAt)
	if err := decodeJSONColumns(&info, metadata, errs); err != nil {
		return nil, err
	}
	reg := w.handlers[info.Kind]
	return &tracked{
		att: &attempt{
			client: w.client, logger: w.logger, id: info.ID, token: token, discard: reg != nil && reg.opts.DiscardCompleted,
		},
		info:     &info,
		reg:      reg,
		lastBeat: now,
	}, nil
}

// dispatch starts the fetched tasks: each one alone, or in batches for batch
// handlers. Each start takes a slot of the queue's concurrency. Tasks that
// cannot start before the worker stops go back to the queue.
func (w *Worker) dispatch(ctx context.Context, r *run, q *queueState, tasks []*tracked) {
	for _, t := range tasks {
		w.track(t)
	}

	for _, unit := range w.units(tasks) {
		if !q.takeSlot(ctx) {
			for _, t := range unit {
				t.mu.Lock()
				t.state = stateRecording
				t.mu.Unlock()
				w.record(context.WithoutCancel(ctx), t, outcome{kind: outcomeRelease})
			}
			continue
		}
		r.inflight.Go(func() {
			defer q.freeSlot()
			if len(unit) == 1 && (unit[0].reg == nil || unit[0].reg.run != nil) {
				w.runOne(r.attemptCtx, unit[0])
			} else {
				w.runBatch(r.attemptCtx, unit)
			}
		})
	}
}

// takeSlot takes one of the queue's slots, and reports false if the worker
// is stopping instead.
func (q *queueState) takeSlot(ctx context.Context) bool {
	if ctx.Err() != nil {
		return false
	}
	select {
	case q.slots <- struct{}{}:
		return true
	case <-ctx.Done():
		return false
	}
}

// freeSlot gives a slot back and tells the fetch loop.
func (q *queueState) freeSlot() {
	<-q.slots
	select {
	case q.freed <- struct{}{}:
	default:
	}
}

// units splits fetched tasks into what runs together: each task alone, or
// tasks of a batch kind in batches of its batch size.
func (w *Worker) units(tasks []*tracked) [][]*tracked {
	var units [][]*tracked
	batches := map[string][]*tracked{}
	for _, t := range tasks {
		if t.reg != nil && t.reg.runBatch != nil {
			batches[t.info.Kind] = append(batches[t.info.Kind], t)
			continue
		}
		units = append(units, []*tracked{t})
	}
	for kind, ts := range batches {
		for chunk := range slices.Chunk(ts, w.handlers[kind].opts.BatchSize) {
			units = append(units, chunk)
		}
	}
	return units
}
