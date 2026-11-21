package taskmill

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// This file holds what keeps the leases of a worker's attempts: the
// check-in that renews them every third of the lease, the attempts it
// tracks, and the tries to record a result while its lease may still be
// held.

// checkInLoop checks the worker in every third of the lease, and keeps its
// reserve ready, until ctx ends. The reserve has a time of its own, because
// a check-in may use up all of its time.
func (w *Worker) checkInLoop(ctx context.Context) {
	w.everyThirdOfLease(ctx, func(checkInCtx context.Context) {
		w.checkIn(checkInCtx)
		w.reserve.refresh(ctx)
	})
}

// everyThirdOfLease calls fn every third of the lease, each time with that
// third as its timeout, until ctx ends.
func (w *Worker) everyThirdOfLease(ctx context.Context, fn func(context.Context)) {
	interval := w.cfg.LeaseDuration / 3
	tick := time.NewTicker(interval)
	defer tick.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			fnCtx, cancel := context.WithTimeout(ctx, interval)
			fn(fnCtx)
			cancel()
		}
	}
}

// checkIn is the worker's periodic check-in: it renews the leases of every
// tracked attempt, cancels the attempts that lost theirs or whose tasks were
// canceled, and records in the worker's row that it is alive.
func (w *Worker) checkIn(ctx context.Context) {
	w.mu.Lock()
	ts := make([]*liveAttempt, 0, len(w.attempts))
	for _, t := range w.attempts {
		ts = append(ts, t)
	}
	w.mu.Unlock()

	// Leases first: on a slow database they need the check-in's time more
	// than the worker's own row, which only shows that it is alive.
	w.renewLeases(ctx, ts)

	var listed bool
	err := w.leaseStatement(ctx, func(ctx context.Context, conn *pgxpool.Conn) error {
		tag, err := conn.Exec(ctx, w.client.sql.checkInWorker, w.id)
		listed = tag.RowsAffected() > 0
		return err
	})
	if err == nil && !listed {
		// The maintainer removed this worker as stale, after a pause such as
		// the machine's sleep, while it kept working: it registers again.
		w.logger.LogAttrs(ctx, slog.LevelWarn, "worker was removed as stale; registering again")
		_, err = w.register(ctx)
	}
	w.checkInFailures.observe(ctx, err)
	if err == nil {
		now := time.Now()
		w.reached.Store(&now)
	}
}

// Health reports whether the worker runs and is in touch with the database:
// nil while Run runs and its last check-in succeeded within LeaseDuration,
// and otherwise an error that says why. A worker out of touch for that long
// can no longer keep its tasks' leases. It is also an error until Run has
// read the settings of the worker's queues, which it does when it starts:
// the worker fetches no task before that. And it is an error while handlers
// that did not return 5 seconds after their context ended, by their
// Timeout, a cancel, or a lost lease, hold every slot of the worker's
// queues: the worker can start no task until one of them returns, and
// [WorkerStats.SlotsStuck] counts them. Health reads only memory, so a
// readiness probe may call it often. It fails while the database is out of
// reach, which restarting the worker does not fix: do not base a liveness
// probe on it. The error wraps none of taskmill's error values.
//
// The [Kubernetes page] has a probe and manifests to copy.
//
// [Kubernetes page]: https://github.com/code19m/taskmill/blob/main/docs/production/kubernetes.md
func (w *Worker) Health() error {
	at := w.reached.Load()
	if at == nil {
		return errors.New("taskmill: the worker is not running")
	}
	if since := time.Since(*at); since > w.cfg.LeaseDuration {
		return fmt.Errorf("taskmill: the worker has not reached the database for %s", since.Round(time.Second))
	}
	if !w.settled.Load() {
		return errors.New("taskmill: the worker has not read the settings of its queues")
	}
	if s := w.Stats(); s.Slots > 0 && s.SlotsStuck == s.Slots {
		return fmt.Errorf("taskmill: all %d slots of the worker are held by handlers that did not return %s after their context ended",
			s.SlotsStuck, w.timing.shutdownGrace)
	}
	return nil
}

// renewLeases renews the leases of ts, and cancels the attempts that lost
// theirs or whose tasks were canceled.
func (w *Worker) renewLeases(ctx context.Context, ts []*liveAttempt) {
	if len(ts) == 0 {
		return
	}

	ids := make([]int64, len(ts))
	tokens := make([]int32, len(ts))
	for i, t := range ts {
		ids[i], tokens[i] = t.att.id, t.att.token
	}

	sent := time.Now()
	renewals, err := w.renew(ctx, ids, tokens)
	w.leaseFailures.observe(ctx, err)

	for _, t := range ts {
		r, owned := renewals[t.att.id]
		switch {
		case err == nil && !owned:
			w.loseLease(ctx, t)
			continue
		case r.renewed:
			t.mu.Lock()
			t.renewedAt = sent
			t.mu.Unlock()
		default:
			// The renewal failed, or the task's row was locked: until a
			// renewal succeeds, the attempt cannot know that it still owns
			// its task once its lease could have expired.
			t.mu.Lock()
			expired := time.Since(t.renewedAt) >= w.cfg.LeaseDuration
			t.mu.Unlock()
			if expired {
				w.loseLease(ctx, t)
				continue
			}
		}

		if r.cancelRequested {
			t.requestCancel()
		}
	}
}

// renewal is what renewing a lease learned about an attempt that owns its
// task.
type renewal struct {
	renewed         bool // false if another transaction locked the task's row
	cancelRequested bool
}

// renew extends the leases of the given attempts and returns those that still
// own their tasks.
func (w *Worker) renew(ctx context.Context, ids []int64, tokens []int32) (map[int64]renewal, error) {
	var renewals map[int64]renewal
	err := w.leaseStatement(ctx, func(ctx context.Context, conn *pgxpool.Conn) error {
		r, err := conn.Query(ctx, w.client.sql.renewLeases, ids, tokens, w.cfg.LeaseDuration.Microseconds())
		if err != nil {
			return err
		}
		defer r.Close()

		renewals = make(map[int64]renewal, len(ids))
		for r.Next() {
			var id int64
			var got renewal
			if err := r.Scan(&id, &got.renewed, &got.cancelRequested); err != nil {
				return err
			}
			// A renewed task appears twice: renewed, and owned.
			prev := renewals[id]
			renewals[id] = renewal{renewed: prev.renewed || got.renewed, cancelRequested: prev.cancelRequested || got.cancelRequested}
		}
		return r.Err()
	})
	if err != nil {
		return nil, err
	}
	return renewals, nil
}

// leaseStatement runs fn, a renewal or a check-in, on a connection of the
// pool. If the pool has none free within a thirtieth of the lease, as when
// handlers that share it hold every connection, fn runs on the worker's
// reserve instead: renewals that waited longer could let leases run out,
// and tasks would run twice.
func (w *Worker) leaseStatement(ctx context.Context, fn func(context.Context, *pgxpool.Conn) error) error {
	wait, cancel := context.WithTimeout(ctx, w.cfg.LeaseDuration/30)
	conn, err := w.acquire(wait)
	cancel()
	if err != nil && ctx.Err() == nil {
		return w.reserve.do(ctx, fn)
	}
	if err != nil {
		return err
	}
	defer conn.Release()
	return fn(ctx, conn)
}

// reserve is a connection of the pool that the worker keeps idle while Run
// runs, for the renewals and check-ins that find no connection of the pool
// free: see leaseStatement. Run keeps one only if the pool has the size
// that the docs ask for: a smaller pool cannot spare it. Without one, as
// outside Run, those statements wait for the pool, as the others do. Why:
// https://github.com/code19m/taskmill/blob/main/docs/design/schema-statements.md.
type reserve struct {
	w        *Worker
	turn     chan struct{} // full while a statement, keep, refresh, or release uses the fields below
	conn     *pgxpool.Conn // nil until taken, and once it broke
	taken    time.Time     // when conn was taken from the pool
	used     time.Time     // when conn last answered
	lifetime time.Duration // the pool's MaxConnLifetime: once older, conn is replaced
	kept     bool          // Run runs, with a pool of the size that the docs ask for
}

// keep starts keeping a reserve, and takes it now: Run calls it before any
// handler runs. It waits a third of the lease for a connection at most, so
// that Run starts also while others hold the whole pool. The next check-in
// then tries again.
func (r *reserve) keep(ctx context.Context) {
	r.turn <- struct{}{}
	defer func() { <-r.turn }()
	r.kept, r.lifetime = true, r.w.client.pool.Config().MaxConnLifetime

	ctx, cancel := context.WithTimeout(ctx, r.w.cfg.LeaseDuration/3)
	defer cancel()
	_ = r.take(ctx) // failed: a later check-in takes one
}

// release stops keeping the reserve, and gives it back to the pool.
func (r *reserve) release() {
	r.turn <- struct{}{}
	defer func() { <-r.turn }()
	r.kept = false
	r.drop()
}

// do runs fn on the reserve, once it is its turn, or, without a reserve, on
// a connection of the pool. If the reserve's connection broke, as when the
// server ended its session, do takes another and runs fn again, once.
func (r *reserve) do(ctx context.Context, fn func(context.Context, *pgxpool.Conn) error) error {
	select {
	case r.turn <- struct{}{}:
	case <-ctx.Done():
		return context.Cause(ctx)
	}
	if !r.kept {
		<-r.turn
		conn, err := r.w.acquire(ctx)
		if err != nil {
			return err
		}
		defer conn.Release()
		return fn(ctx, conn)
	}
	defer func() { <-r.turn }()

	err := r.run(ctx, fn)
	if err != nil && r.conn == nil && ctx.Err() == nil {
		err = r.run(ctx, fn)
	}
	return err
}

// run runs fn on the reserve's connection. It takes one if it has none, and
// pings it first if it was idle for more than a second, as the pool does
// with its own: a reserve is idle most of the time, and a server, a
// firewall, or a failover may have ended its session meanwhile. It drops a
// connection that fails either way.
func (r *reserve) run(ctx context.Context, fn func(context.Context, *pgxpool.Conn) error) error {
	if r.conn != nil && time.Since(r.used) > time.Second {
		if err := r.check(ctx); err != nil {
			return err
		}
	}
	if r.conn == nil {
		if err := r.take(ctx); err != nil {
			return err
		}
	}

	err := fn(ctx, r.conn)
	if c := r.conn.Conn(); err != nil && (c.IsClosed() || c.PgConn().IsBusy() || c.PgConn().TxStatus() != 'I') {
		r.drop()
	}
	if err == nil {
		r.used = time.Now()
	}
	return err
}

// refresh keeps the reserve ready while the pool has connections to spare,
// after each check-in: it takes one if it has none, and replaces one that
// is older than the pool's MaxConnLifetime, which the pool would have
// closed, or that does not answer a ping. It has a thirtieth of the lease. A
// connection that is only old stays until a new one comes.
func (r *reserve) refresh(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, r.w.cfg.LeaseDuration/30)
	defer cancel()
	select {
	case r.turn <- struct{}{}:
	case <-ctx.Done():
		return
	}
	defer func() { <-r.turn }()

	if !r.kept || ctx.Err() != nil {
		return
	}
	old := r.conn != nil && r.lifetime > 0 && time.Since(r.taken) >= r.lifetime
	if r.conn != nil && !old {
		if err := r.check(ctx); err != nil || r.conn != nil {
			return // out of time, or it answered
		}
	}

	conn, err := r.w.acquire(ctx)
	if err != nil {
		return
	}
	r.drop()
	r.conn, r.taken, r.used = conn, time.Now(), time.Now()
}

// check pings the reserve's connection, and drops it if it does not answer.
// It returns an error only if ctx ended first: that says nothing of the
// connection, which it keeps. One that the ended ping left closed goes the
// next time.
func (r *reserve) check(ctx context.Context) error {
	if r.conn.Conn().IsClosed() {
		r.drop()
		return nil
	}
	if err := r.conn.Ping(ctx); err != nil {
		if ctx.Err() != nil {
			return context.Cause(ctx)
		}
		r.drop()
		return nil
	}
	r.used = time.Now()
	return nil
}

// take takes the reserve's connection from the pool.
func (r *reserve) take(ctx context.Context) error {
	conn, err := r.w.acquire(ctx)
	if err != nil {
		return err
	}
	r.conn, r.taken, r.used = conn, time.Now(), time.Now()
	return nil
}

// drop gives the reserve's connection back to the pool, which closes it if
// it broke or is too old.
func (r *reserve) drop() {
	if r.conn != nil {
		r.conn.Release()
		r.conn = nil
	}
}

// acquire takes a connection of the pool for a statement of the worker. If
// every connection was in use, it counts a pool wait, and if it then waited
// for a while, it warns, at most once per failureReport: handlers that share
// the pool may hold all its connections, and the worker's fetches and
// records wait for them. It judges by what happened, not by the pool's size,
// because it cannot see which pool the handlers use. A wait for a new
// connection to open, or a short wait, is normal.
func (w *Worker) acquire(ctx context.Context) (*pgxpool.Conn, error) {
	s := w.client.pool.Stat()
	full := s.IdleConns() == 0 && s.TotalConns() >= s.MaxConns()

	if full {
		w.counts.poolWaits.Add(1)
	}
	start := time.Now()
	conn, err := w.client.pool.Acquire(ctx)
	waited := time.Since(start)

	if !full || waited < w.cfg.LeaseDuration/30 {
		return conn, err
	}
	now, last := time.Now().UnixNano(), w.poolWarned.Load()
	if now-last >= int64(w.timing.failureReport) && w.poolWarned.CompareAndSwap(last, now) {
		w.logger.LogAttrs(ctx, slog.LevelWarn,
			"the pool was full, so the worker waited for a connection; raise MaxConns, or give handlers a pool of their own",
			slog.Duration(logKeyDuration, waited), slog.Int(logKeyMaxConns, int(s.MaxConns())))
	}
	return conn, err
}

func (w *Worker) loseLease(ctx context.Context, t *liveAttempt) {
	t.mu.Lock()
	running := t.state == attemptRunning
	t.mu.Unlock()

	// An attempt that completed its task in its own transaction owns nothing
	// more: its row stays locked until that transaction commits.
	if !running || t.att.completedInTx.Load() || t.lost.Swap(true) {
		return
	}
	w.counts.leasesLost.Add(1)
	w.logger.LogAttrs(ctx, slog.LevelWarn, "attempt lost its lease; canceling it",
		slog.Int64(logKeyTaskID, t.att.id), slog.String(logKeyKind, t.info.Kind))
	t.cancelAttempt(ErrLeaseLost)
}

// requestCancel cancels the attempt because its task's cancellation was
// requested.
func (t *liveAttempt) requestCancel() {
	if !t.att.cancelRequested.Swap(true) {
		t.cancelAttempt(ErrTaskCanceled)
	}
}

// cancelAttempt cancels the attempt's context, if it has started. Batches
// keep running: a canceled or lost task's result is handled when it ends.
func (t *liveAttempt) cancelAttempt(cause error) {
	t.mu.Lock()
	cancel := t.cancel
	t.mu.Unlock()

	if cancel != nil {
		cancel(cause)
	}
}

// whileLeased runs fn until it succeeds, waiting longer after each failed
// try, for as long as the leases of ts may still be held when it starts: by
// then the tasks are rescued, and a result recorded later would be refused.
// A database outage or failover shorter than a lease thus loses no results,
// while an error that persists ends the tries. Each try gets its own timeout.
func (w *Worker) whileLeased(ctx context.Context, ts []*liveAttempt, fn func(context.Context) error) error {
	// The deadline is fixed now: the leases are still renewed while the
	// tries continue, which would otherwise never let them end.
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
func (w *Worker) leaseEnd(ts []*liveAttempt) time.Time {
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

// track starts tracking t, so that check-ins renew its lease.
//
// The worker may still run an older attempt of t's task: it was out of touch
// for longer than the lease, and the task was rescued and leased again
// before a check-in found out. t takes that attempt's place, so no check-in
// sees it again. The new lease proves that the older attempt lost its own,
// so track cancels its context with ErrLeaseLost, and its result is ignored.
func (w *Worker) track(ctx context.Context, t *liveAttempt) {
	w.mu.Lock()
	old := w.attempts[t.att.id]
	w.attempts[t.att.id] = t
	replaced := false
	if old != nil {
		// Under w.mu, so that a shutdown either still finds the older
		// attempt and abandons it, or finds it abandoned here.
		old.mu.Lock()
		if old.state == attemptRunning {
			old.state, replaced = attemptAbandoned, true
		}
		old.mu.Unlock()
	}
	w.mu.Unlock()

	if replaced && !old.lost.Swap(true) {
		w.counts.leasesLost.Add(1)
		w.logger.LogAttrs(ctx, slog.LevelWarn, "attempt lost its lease; canceling it",
			slog.Int64(logKeyTaskID, old.att.id), slog.String(logKeyKind, old.info.Kind))
		old.cancelAttempt(ErrLeaseLost)
	}
}

func (w *Worker) untrack(t *liveAttempt) {
	w.mu.Lock()
	if w.attempts[t.att.id] == t {
		delete(w.attempts, t.att.id)
	}
	w.mu.Unlock()
}
