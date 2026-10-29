package taskmill

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// heartbeatLoop renews the leases of every tracked attempt, and cancels the
// attempts that lost theirs or whose tasks were canceled.
func (w *Worker) heartbeatLoop(ctx context.Context) {
	w.everyBeat(ctx, w.heartbeat)
}

// everyBeat calls beat every third of the lease, each time with that third
// as its timeout, until ctx ends.
func (w *Worker) everyBeat(ctx context.Context, beat func(context.Context)) {
	interval := w.cfg.LeaseDuration / 3
	tick := time.NewTicker(interval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			beatCtx, cancel := context.WithTimeout(ctx, interval)
			beat(beatCtx)
			cancel()
		}
	}
}

func (w *Worker) heartbeat(ctx context.Context) {
	w.mu.Lock()
	ts := make([]*tracked, 0, len(w.attempts))
	for _, t := range w.attempts {
		ts = append(ts, t)
	}
	w.mu.Unlock()

	// Leases first: on a slow database they need the beat's time more than
	// the worker's own row, which only shows that it is alive.
	w.renewLeases(ctx, ts)
	tag, err := w.client.pool.Exec(ctx, w.client.sql.beatWorker, w.id)
	if err == nil && tag.RowsAffected() == 0 {
		// The maintainer removed this worker as stale, after a pause such as
		// the machine's sleep, while it kept working: it registers again.
		w.logger.LogAttrs(ctx, slog.LevelWarn, "worker was removed as stale; registering again")
		_, err = w.register(ctx)
	}
	w.beatFailures.observe(ctx, err)
	if err == nil {
		now := time.Now()
		w.reached.Store(&now)
	}
}

// Health reports whether the worker runs and is in touch with the database:
// nil while Run runs and its last heartbeat succeeded within LeaseDuration,
// and otherwise an error that says why. A worker out of touch for that long
// can no longer keep its tasks' leases. Health reads only memory, so a
// readiness probe may call it often. It fails while the database is out of
// reach, which restarting the worker does not fix: do not base a liveness
// probe on it.
func (w *Worker) Health() error {
	at := w.reached.Load()
	if at == nil {
		return errors.New("taskmill: the worker is not running")
	}
	if since := time.Since(*at); since > w.cfg.LeaseDuration {
		return fmt.Errorf("taskmill: the worker has not reached the database for %s", since.Round(time.Second))
	}
	return nil
}

// renewLeases renews the leases of ts, and cancels the attempts that lost
// theirs or whose tasks were canceled.
func (w *Worker) renewLeases(ctx context.Context, ts []*tracked) {
	if len(ts) == 0 {
		return
	}
	ids := make([]int64, len(ts))
	tokens := make([]int32, len(ts))
	for i, t := range ts {
		ids[i], tokens[i] = t.att.id, t.att.token
	}
	sent := time.Now()
	beats, err := w.renew(ctx, ids, tokens)
	w.leaseFailures.observe(ctx, err)

	for _, t := range ts {
		b, owned := beats[t.att.id]
		switch {
		case err == nil && !owned:
			w.loseLease(ctx, t)
			continue
		case b.renewed:
			t.mu.Lock()
			t.lastBeat = sent
			t.mu.Unlock()
		default:
			// The heartbeat failed, or the task's row was locked: without a
			// renewal, the attempt cannot know that it still owns its task
			// once its lease could have expired.
			t.mu.Lock()
			expired := time.Since(t.lastBeat) >= w.cfg.LeaseDuration
			t.mu.Unlock()
			if expired {
				w.loseLease(ctx, t)
				continue
			}
		}
		if b.cancelRequested {
			t.requestCancel()
		}
	}
}

// beat is what a heartbeat learned about an attempt that owns its task.
type beat struct {
	renewed         bool // false if another transaction locked the task's row
	cancelRequested bool
}

// renew extends the leases of the given attempts and returns those that still
// own their tasks.
func (w *Worker) renew(ctx context.Context, ids []int64, tokens []int32) (map[int64]beat, error) {
	conn, err := w.acquireForRenewal(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Release()
	r, err := conn.Query(ctx, w.client.sql.heartbeat, ids, tokens, w.cfg.LeaseDuration.Microseconds())
	if err != nil {
		return nil, err
	}
	defer r.Close()
	beats := make(map[int64]beat, len(ids))
	for r.Next() {
		var id int64
		var b beat
		if err := r.Scan(&id, &b.renewed, &b.cancelRequested); err != nil {
			return nil, err
		}
		// A renewed task appears twice: renewed, and owned.
		prev := beats[id]
		beats[id] = beat{renewed: prev.renewed || b.renewed, cancelRequested: prev.cancelRequested || b.cancelRequested}
	}
	return beats, r.Err()
}

// acquireForRenewal takes a connection of the pool to renew leases. If every
// connection was in use, and the renewal waited for one for at least a tenth
// of its time, it warns, at most once per failureReport: handlers that share
// the pool may hold all its connections, and if renewals keep waiting, leases
// run out and tasks run twice. It judges by what happened, not by the pool's
// size, because it cannot see which pool the handlers use. A wait for a new
// connection to open, or a short wait, is normal.
func (w *Worker) acquireForRenewal(ctx context.Context) (*pgxpool.Conn, error) {
	s := w.client.pool.Stat()
	full := s.IdleConns() == 0 && s.TotalConns() >= s.MaxConns()
	start := time.Now()
	conn, err := w.client.pool.Acquire(ctx)
	waited := time.Since(start)
	if !full || waited < w.cfg.LeaseDuration/30 { // a renewal has a third of the lease
		return conn, err
	}
	now, last := time.Now().UnixNano(), w.poolWarned.Load()
	if now-last >= int64(w.timing.failureReport) && w.poolWarned.CompareAndSwap(last, now) {
		w.logger.LogAttrs(ctx, slog.LevelWarn,
			"the pool was full, so renewing leases waited; raise MaxConns, or give handlers a pool of their own",
			slog.Duration(logKeyDuration, waited), slog.Int(logKeyMaxConns, int(s.MaxConns())))
	}
	return conn, err
}

func (w *Worker) loseLease(ctx context.Context, t *tracked) {
	t.mu.Lock()
	running := t.state == stateRunning
	t.mu.Unlock()
	// An attempt that completed its task in its own transaction owns nothing
	// more: its row stays locked until that transaction commits.
	if !running || t.att.completedInTx.Load() || t.lost.Swap(true) {
		return
	}
	w.logger.LogAttrs(ctx, slog.LevelWarn, "attempt lost its lease; canceling it",
		slog.Int64(logKeyTaskID, t.att.id), slog.String(logKeyKind, t.info.Kind))
	t.cancelAttempt(ErrLeaseLost)
}

// requestCancel cancels the attempt because its task's cancellation was
// requested.
func (t *tracked) requestCancel() {
	if !t.cancelRequested.Swap(true) {
		t.cancelAttempt(ErrTaskCanceled)
	}
}

// cancelAttempt cancels the attempt's context, if it has started. Batches
// keep running: a canceled or lost task's result is handled when it ends.
func (t *tracked) cancelAttempt(cause error) {
	t.mu.Lock()
	cancel := t.cancel
	t.mu.Unlock()
	if cancel != nil {
		cancel(cause)
	}
}
