package taskmill

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"math/rand/v2"
	"slices"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

// fetchLoop fetches tasks for one queue whenever it has free capacity, until
// ctx is canceled.
func (w *Worker) fetchLoop(ctx context.Context, r *runState, q *queueState) {
	fetches := newIncidentLog(w, "fetch tasks", slog.String(logKeyQueue, q.name))

	// notified reports that a notification caused the last fetch, and
	// atLimit that the queue's MaxRunning limit held tasks back. Until
	// quietUntil, the loop leaves notifications waiting.
	notified, atLimit := false, false
	var quietUntil time.Time

	wait := func() (ok bool) {
		ok, notified = waitForWork(ctx, q, w.pollWait(atLimit), quietUntil)
		return ok
	}

	for ctx.Err() == nil {
		free := cap(q.slots) - len(q.slots)
		if q.paused.Load() || free == 0 {
			if !wait() {
				return
			}
			continue
		}

		var tasks []*liveAttempt
		var more bool
		var err error
		tasks, more, atLimit, err = w.fetch(ctx, q, free)
		fetches.observe(ctx, err)
		w.dispatch(ctx, r, q, tasks)

		// A notification wakes every worker of the queue, and most find
		// nothing: those let a moment pass before they act on the next one.
		if err == nil && notified && len(tasks) == 0 {
			quietUntil = time.Now().Add(w.timing.quiet)
		}

		if (err != nil || !more) && !wait() {
			return
		}
	}
}

// pollWait returns how long an idle queue waits before it fetches again,
// unless a notification or a freed slot wakes it first. While the
// maintainer's heartbeats arrive, it polls every idlePoll, plus or minus a
// tenth so that workers started together poll apart: the maintainer wakes
// each queue whose tasks wait in the same statement that sends the
// heartbeat, so this poll only bounds the harm of a fault in that. Otherwise
// it polls every PollInterval, and so does a queue whose installation-wide
// limit held tasks back: no notification says when another worker's task of
// that queue ends. The end of this worker's own task wakes the queue (see
// madeRoom).
func (w *Worker) pollWait(atLimit bool) time.Duration {
	if atLimit || !w.notifying.Load() {
		return w.cfg.PollInterval
	}
	d := max(w.timing.idlePoll, w.cfg.PollInterval)
	if spread := d / 5; spread > 0 {
		d += rand.N(spread) - spread/2 //nolint:gosec // spreads polls, no security role
	}
	return d
}

// waitForWork waits for a nudge, a freed slot, or the poll. It reports false
// if ctx ends first, and whether a nudge woke it. Until quietUntil, it
// leaves nudges waiting, which gathers those that arrive meanwhile into one
// fetch. A freed slot wakes it at any time: that is the worker's own news,
// not a hint, and it comes no more often than the worker's tasks end.
func waitForWork(ctx context.Context, q *queueState, poll time.Duration, quietUntil time.Time) (ok, nudged bool) {
	timer := time.NewTimer(poll)
	defer timer.Stop()

	if quiet := time.Until(quietUntil); quiet > 0 {
		select {
		case <-ctx.Done():
			return false, false
		case <-q.freed:
			return true, false
		case <-timer.C:
			return true, false
		case <-time.After(quiet):
		}
	}

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
// every fetched task starts right away. A queue with an installation-wide
// MaxRunning limit fetches only up to its remaining capacity. more reports
// that the first lease took all it asked for, so more tasks may be waiting,
// and atLimit that the limit kept it from asking for as many as slots, or
// that it gave up waiting for the row of a queue with a limit (see
// errQueueHeld).
// If filling the batches fails outside a transaction, it returns the tasks
// leased before the error with the error: they must run.
func (w *Worker) fetch(ctx context.Context, q *queueState, slots int) (tasks []*liveAttempt, more, atLimit bool, err error) {
	// A queue with a MaxRunning limit and a backlog is at its limit most of
	// the time: one statement, without a lock, then saves the transaction.
	limited := q.maxRunning.Load() > 0
	if limited {
		if atLimit, err = w.atLimit(ctx, q.name); err != nil || atLimit {
			return nil, false, atLimit, err
		}
	}

	// The statements run on a context that outlives ctx: once tasks are
	// leased, they must reach the worker even if it is stopping. But only by
	// cleanupTimeout, so that a database that does not answer holds the stop
	// up no longer than it holds the rest of it. Tasks that a statement
	// leased without an answer are rescued when their leases run out.
	dbCtx, cancel := outlive(ctx, recordTimeout, cleanupTimeout)
	defer cancel()

	if limited {
		tasks, more, atLimit, err = w.fetchLimited(dbCtx, q, slots)
		if errors.Is(err, errQueueHeld) {
			w.logger.LogAttrs(ctx, slog.LevelDebug, "fetch gave up waiting for the queue's row", slog.String(logKeyQueue, q.name))
			return nil, false, true, nil
		}
	} else {
		tasks, more, err = w.fetchFree(dbCtx, q, slots)
	}

	slices.SortFunc(tasks, func(a, b *liveAttempt) int {
		return cmp.Or(
			cmp.Compare(b.info.Priority, a.info.Priority),
			a.info.RunAt.Compare(b.info.RunAt),
			cmp.Compare(a.info.ID, b.info.ID),
		)
	})
	return tasks, more, atLimit, err
}

// fetchFree is fetch for a queue without a MaxRunning limit. Each fetch
// statement runs in a transaction of its own, sent in one round trip.
func (w *Worker) fetchFree(ctx context.Context, q *queueState, slots int) (tasks []*liveAttempt, more bool, err error) {
	limit := min(slots, maxFetch)
	out, err := w.leaseAlone(ctx, q, w.client.sql.fetch, limit)
	if err != nil {
		return nil, false, err
	}

	extra, err := w.fillBatches(ctx, q, w.leaseAlone, out, maxFetch-len(out))
	return append(out, extra...), len(out) == limit, err
}

// fetchLimited is fetch for a queue with a MaxRunning limit, in one
// transaction that locks the queue's row first, so that the fetches of all
// workers count each other's tasks. The statements up to the fetch, and
// the commit unless batches are to be filled, go in one round trip: the
// row is held for that one, not for four. So the fetch cannot wait for the
// answer of the count of the running tasks: it counts them itself, and the
// count's answer only tells the worker how far the limit held it back. It
// returns errQueueHeld if it gave up waiting for the row.
func (w *Worker) fetchLimited(ctx context.Context, q *queueState, slots int) (tasks []*liveAttempt, more, atLimit bool, err error) {
	conn, err := w.acquire(ctx)
	if err != nil {
		return nil, false, false, err
	}
	defer conn.Release()

	committed := !w.batchKinds
	b := &pgx.Batch{}
	b.Queue("BEGIN")
	b.Queue(limitedSettings)
	b.Queue(w.client.sql.lockQueue, q.name)
	b.Queue(w.client.sql.countRunning, q.name)
	b.Queue(w.client.sql.fetchLimited, w.leaseArgs(q, min(slots, maxFetch), nil)...)
	if committed {
		b.Queue("COMMIT")
	}

	allowed, out, err := w.readLimited(conn.SendBatch(ctx, b), committed)
	fetched := len(out) // by the first lease, before the batches fill
	if err == nil && !committed {
		var extra []*liveAttempt
		if extra, err = w.fillBatches(ctx, q, w.leaseIn(conn), out, allowed-len(out)); err == nil {
			out = append(out, extra...)
			_, err = conn.Exec(ctx, "COMMIT")
		}
	}
	if err != nil {
		// The transaction may still be open, after a failed statement: the
		// connection goes back to the pool without it. Tasks that a commit
		// leased all the same, as when the connection failed during the
		// commit, are rescued when their leases run out.
		_, _ = conn.Exec(ctx, "ROLLBACK")
		if pgCode(err) == codeLockNotAvailable {
			err = errQueueHeld
		}
		return nil, false, false, err
	}

	limit := min(slots, allowed)
	return out, limit > 0 && fetched == limit, allowed < slots && fetched == limit, nil
}

// readLimited reads the results of fetchLimited's batch, and closes it. It
// returns how many more tasks of the queue the limit allows, and the tasks
// that the fetch leased, which are leased once committed is true, or the
// worker commits later.
func (w *Worker) readLimited(br pgx.BatchResults, committed bool) (allowed int, out []*liveAttempt, err error) {
	defer func() { err = cmp.Or(err, br.Close()) }()
	for range 2 { // BEGIN and the settings
		if _, err := br.Exec(); err != nil {
			return 0, nil, err
		}
	}

	allowed = maxFetch
	var maxRunning *int
	switch err := br.QueryRow().Scan(&maxRunning); {
	case errors.Is(err, pgx.ErrNoRows): // no settings: no limit
	case err != nil:
		return 0, nil, err
	}

	var running int
	if err := br.QueryRow().Scan(&running); err != nil {
		return 0, nil, err
	}
	if maxRunning != nil {
		allowed = max(*maxRunning-running, 0)
	}

	r, err := br.Query()
	if err != nil {
		return 0, nil, err
	}
	if out, err = w.scanAllLeased(r); err != nil {
		return 0, nil, err
	}
	if committed {
		if _, err := br.Exec(); err != nil {
			return 0, nil, err
		}
	}
	return allowed, out, nil
}

// leaseFunc runs a fetch statement for up to limit tasks of q, with extra
// arguments after the standard four, and returns the leased tasks.
type leaseFunc func(ctx context.Context, q *queueState, sql string, limit int, extra ...any) ([]*liveAttempt, error)

// fillBatches leases more tasks of the batch kinds among fetched, up to room
// in all, to fill their batches. It returns what it leased before an error.
func (w *Worker) fillBatches(ctx context.Context, q *queueState, lease leaseFunc, fetched []*liveAttempt, room int) ([]*liveAttempt, error) {
	var extra []*liveAttempt
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

// atLimit reports whether a queue with a MaxRunning limit runs as many tasks
// as its limit allows. It asks without a lock, so its answer can only save a
// fetch its transaction, never let a task start: a queue that it finds below
// its limit is counted again under the lock before anything is leased. As it
// leases nothing, it ends with ctx, unlike the fetch's other statements.
func (w *Worker) atLimit(ctx context.Context, queue string) (full bool, err error) {
	ctx, cancel := context.WithTimeout(ctx, recordTimeout)
	defer cancel()
	err = w.client.pool.QueryRow(ctx, w.client.sql.atLimit, queue).Scan(&full)
	return full, err
}

// leaseIn returns a leaseFunc that runs fetch statements on q, in the
// transaction that is open on it.
func (w *Worker) leaseIn(q PgxQuerier) leaseFunc {
	return func(ctx context.Context, queue *queueState, sql string, limit int, extra ...any) ([]*liveAttempt, error) {
		if limit <= 0 {
			return nil, nil
		}
		r, err := q.Query(ctx, sql, w.leaseArgs(queue, limit, extra)...)
		if err != nil {
			return nil, err
		}
		return w.scanAllLeased(r)
	}
}

// errQueueHeld reports that the fetch of a queue with a MaxRunning limit
// gave up waiting for the queue's row. That is no failure of the fetch: the
// queue's fetches lock the row in turn, each for a moment, and with many
// workers the turns alone can take that long. The queue counts as held back
// by its limit, and is fetched again when it is next woken or polled.
var errQueueHeld = errors.New("taskmill: the queue's row is locked")

// leaseAlone runs a fetch statement for up to limit tasks of q, with extra
// arguments after the standard four, in a transaction of its own with
// planSettings, all in one round trip. It returns the leased tasks.
func (w *Worker) leaseAlone(ctx context.Context, q *queueState, sql string, limit int, extra ...any) ([]*liveAttempt, error) {
	if limit <= 0 {
		return nil, nil
	}

	b := &pgx.Batch{}
	b.Queue("BEGIN")
	b.Queue(planSettings)
	b.Queue(sql, w.leaseArgs(q, limit, extra)...)
	b.Queue("COMMIT")

	conn, err := w.acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Release()
	br := conn.SendBatch(ctx, b)
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
	args := []any{
		q.name,                             // $1
		limit,                              // $2
		w.cfg.LeaseDuration.Microseconds(), // $3
		w.id,                               // $4
	}
	return append(args, extra...)
}

// scanAllLeased reads the tasks that a fetch statement leased.
func (w *Worker) scanAllLeased(r pgx.Rows) ([]*liveAttempt, error) {
	defer r.Close()
	now := time.Now()
	var out []*liveAttempt
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
func batchCounts(ts []*liveAttempt) map[string]int {
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

// scanLeased reads one row of leasedColumns. A row that holds what taskmill
// never writes, as another database role or an UPDATE by hand can leave it,
// is no error: an error would lose every task that the statement leased
// with it. Such a task is tracked as invalid, without a handler, and runOne
// makes it dead, alone.
func (w *Worker) scanLeased(r pgx.Rows, now time.Time) (*liveAttempt, error) {
	var (
		info                    TaskInfo
		priority, att, maxAtt   int16
		token                   int32
		metadata, payload, errs []byte
		createdAt, runAt        pgtype.Timestamptz
		attemptedAt             *time.Time
	)

	err := r.Scan(&info.ID, &info.Queue, &info.Kind, &priority, &att, &maxAtt, &token, &info.GroupKey,
		&metadata, &payload, &errs, &createdAt, &runAt, &attemptedAt)
	if err != nil {
		return nil, err
	}

	info.State = StateRunning
	info.Priority = Priority(priority)
	info.Attempt = int(att)
	info.MaxAttempts = int(maxAtt)
	info.Payload = payload
	info.AttemptedAt = deref(attemptedAt)
	info.Errors = decodeErrors(errs)
	var created, due bool
	info.CreatedAt, created = finite(createdAt)
	info.RunAt, due = finite(runAt)

	var invalid error
	info.Metadata, invalid = decodeMetadata(metadata)
	if invalid == nil && (!created || !due) {
		invalid = errors.New("the time of creation or the run time is infinite")
	}
	if invalid == nil && token == math.MaxInt32 {
		// The fetch statements count no further, so another attempt would
		// have this token too, and could record this attempt's result.
		invalid = errors.New("the lease token is at its largest value, so the task cannot run again")
	}

	reg := w.handlers[info.Kind]
	if invalid != nil {
		invalid, reg = fmt.Errorf("invalid row: %w", invalid), nil
	}
	return &liveAttempt{
		att: &attempt{
			client: w.client, logger: w.logger, id: info.ID, token: token, discard: reg != nil && reg.opts.DiscardCompleted,
		},
		info:      &info,
		reg:       reg,
		invalid:   invalid,
		renewedAt: now,
	}, nil
}

// dispatch starts the fetched tasks: each one alone, or in batches for batch
// handlers. Each start takes a slot of the queue's concurrency. Tasks that
// cannot start before the worker stops go back to the queue.
func (w *Worker) dispatch(ctx context.Context, r *runState, q *queueState, tasks []*liveAttempt) {
	for _, t := range tasks {
		w.track(ctx, t)
	}

	units := w.units(tasks)
	for i, unit := range units {
		if !q.takeSlot(ctx) {
			// The worker is stopping, so the units after this one cannot
			// start either.
			unstarted := slices.Concat(units[i:]...)
			for _, t := range unstarted {
				t.mu.Lock()
				t.state = attemptRecording
				t.mu.Unlock()
			}
			releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cleanupTimeout)
			w.release(releaseCtx, unstarted)
			cancel()
			return
		}

		r.inflight.Go(func() {
			defer q.freeSlot()
			if len(unit) == 1 && (unit[0].reg == nil || unit[0].reg.run != nil) {
				w.runOne(r.attemptCtx, r.recordCtx, unit[0])
			} else {
				w.runBatch(r.attemptCtx, r.recordCtx, unit)
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
func (w *Worker) units(tasks []*liveAttempt) [][]*liveAttempt {
	var units [][]*liveAttempt
	batches := map[string][]*liveAttempt{}
	for _, t := range tasks {
		if t.reg != nil && t.reg.runBatch != nil {
			batches[t.info.Kind] = append(batches[t.info.Kind], t)
			continue
		}
		units = append(units, []*liveAttempt{t})
	}

	for kind, ts := range batches {
		for chunk := range slices.Chunk(ts, w.handlers[kind].opts.BatchSize) {
			units = append(units, chunk)
		}
	}
	return units
}
