package taskmill

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// Maintenance limits.
const (
	maintenanceBatch   = 1000                  // most tasks one maintenance statement changes
	minMaintainerSleep = 50 * time.Millisecond // shortest wait for the next due task: at most 20 promotions a second
	staleWorkerAge     = 5 * time.Minute       // a worker without heartbeats this long is removed
	detachTimeout      = 30 * time.Second      // bound on detaching one history partition

	// maintenanceLockTimeout bounds the wait for a row that an application
	// transaction holds, such as a FIFO group's.
	maintenanceLockTimeout = "1s"
)

// maintainer does the installation-wide work of one elected worker. Its main
// loop elects the leader, then rescues expired leases and enqueues schedules
// once per interval. Due tasks are promoted in a loop of their own, as they
// come due, and statistics and retention in another, so that slow work in
// one never delays the others.
type maintainer struct {
	w      *Worker
	c      *Client
	leader atomic.Bool

	lastElection time.Time // used by the main loop only

	// The failures of each kind of work, which it logs only now and then.
	election, promotion, rescues, schedules, pruning, stats, retention *failures
}

func newMaintainer(w *Worker) *maintainer {
	return &maintainer{
		w:         w,
		c:         w.client,
		election:  newFailures(w, "elect maintainer"),
		promotion: newFailures(w, "promote due tasks"),
		rescues:   newFailures(w, "rescue expired leases"),
		schedules: newFailures(w, "run schedules"),
		pruning:   newFailures(w, "delete removed schedules"),
		stats:     newFailures(w, "refresh statistics"),
		retention: newFailures(w, "apply retention"),
	}
}

func (m *maintainer) run(ctx context.Context) {
	var loops sync.WaitGroup
	loops.Go(func() { m.promoteLoop(ctx) })
	loops.Go(func() { m.housekeep(ctx) })
	defer loops.Wait()

	tick := time.NewTicker(m.w.timing.maintainer)
	defer tick.Stop()
	m.cycle(ctx)
	for {
		select {
		case <-ctx.Done():
			m.resign(context.WithoutCancel(ctx))
			return
		case <-tick.C:
			m.cycle(ctx)
		}
	}
}

// cycle renews or tries to take the leadership, and if this worker leads,
// rescues expired leases and enqueues due schedules.
func (m *maintainer) cycle(ctx context.Context) {
	if !m.leader.Load() && time.Since(m.lastElection) < m.w.timing.election {
		return
	}
	m.lastElection = time.Now()
	if !m.elect(ctx) {
		return
	}
	m.rescue(ctx)
	m.runSchedules(ctx)
}

// promoteLoop promotes due tasks while this worker leads, as they come due.
func (m *maintainer) promoteLoop(ctx context.Context) {
	wait := time.NewTimer(0)
	defer wait.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-wait.C:
		}
		next := m.w.timing.maintainer
		if m.leader.Load() {
			m.promote(ctx)
			next = m.untilNextDue(ctx)
		}
		wait.Reset(next)
	}
}

// housekeep refreshes statistics and applies retention while this worker
// leads.
func (m *maintainer) housekeep(ctx context.Context) {
	tick := time.NewTicker(m.w.timing.maintainer)
	defer tick.Stop()
	var lastStats, lastRetention time.Time
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
		if !m.leader.Load() {
			continue
		}
		if time.Since(lastStats) >= m.w.timing.stats {
			m.refreshStats(ctx)
			lastStats = time.Now()
		}
		if time.Since(lastRetention) >= m.w.timing.retention {
			m.retain(ctx)
			m.pruneSchedules(ctx)
			lastRetention = time.Now()
		}
	}
}

func (m *maintainer) elect(ctx context.Context) bool {
	var id string
	err := m.c.pool.QueryRow(ctx, m.c.sql.electLeader, m.w.id, m.w.timing.leaderLease.Microseconds()).Scan(&id)
	leader := err == nil
	if errors.Is(err, pgx.ErrNoRows) {
		err = nil // another worker leads
	}
	m.election.observe(ctx, err)
	switch was := m.leader.Swap(leader); {
	case leader && !was:
		m.w.logger.LogAttrs(ctx, slog.LevelInfo, "elected maintainer")
	case was && !leader:
		m.w.logger.LogAttrs(ctx, slog.LevelInfo, "no longer maintainer")
		// The work stops here: a later term starts without its incidents.
		for _, f := range []*failures{m.promotion, m.rescues, m.schedules, m.pruning, m.stats, m.retention} {
			f.reset()
		}
	}
	return leader
}

func (m *maintainer) resign(ctx context.Context) {
	if !m.leader.Swap(false) {
		return
	}
	ctx, cancel := context.WithTimeout(ctx, cleanupTimeout)
	defer cancel()
	if _, err := m.c.pool.Exec(ctx, m.c.sql.resignLeader, m.w.id); err != nil {
		m.w.logger.LogAttrs(ctx, slog.LevelWarn, "resign maintainer failed; another worker leads when the lease expires",
			slog.Any(logKeyError, err))
	}
}

// promote makes due scheduled and retryable tasks available. The statement
// also wakes the workers of their queues.
func (m *maintainer) promote(ctx context.Context) {
	for {
		var promoted int
		err := m.c.pool.QueryRow(ctx, m.c.sql.promoteDue, m.c.schema).Scan(&promoted)
		if err != nil || promoted < maintenanceBatch {
			m.promotion.observe(ctx, err)
			return
		}
	}
}

// leaseExpired is the error that a rescue records for the attempt it fails.
const leaseExpired = "the worker lost its lease: it crashed, stopped, or lost the database"

// rescue fails the attempts whose leases expired and updates their groups. It
// skips tasks whose groups other transactions hold; a later cycle rescues
// them. Each failed attempt counts in the activity, and is logged, once its
// batch commits.
func (m *maintainer) rescue(ctx context.Context) {
	var batch []swept
	committed := func() { m.rescued(ctx, batch) }
	err := m.sweep(ctx, committed, func(ctx context.Context, q querier) (int, error) {
		r, err := q.query(ctx, m.c.sql.rescue,
			defaultBackoffBase.Seconds(), defaultBackoffMax.Seconds(), leaseExpired)
		if err != nil {
			return 0, err
		}
		rows, err := collectSwept(r)
		if err != nil {
			return 0, err
		}
		batch = rows
		for _, x := range rows {
			s := State(x.state)
			if x.group == "" || !s.Finished() {
				continue
			}
			how := leftHead
			if s == StateDead {
				how = leftDeadHead
			}
			if err := m.c.leaveGroup(ctx, m.w.logger, q, x.queue, x.group, x.id, how); err != nil {
				return 0, err
			}
		}
		return len(rows), nil
	})
	m.rescues.observe(ctx, err)
}

// rescued counts and logs the failed attempts of a committed rescue batch.
func (m *maintainer) rescued(ctx context.Context, batch []swept) {
	if len(batch) > 0 {
		m.w.logger.LogAttrs(ctx, slog.LevelWarn, "rescued tasks whose workers lost their leases",
			slog.Int(logKeyTasks, len(batch)))
	}
	for _, x := range batch {
		m.w.tally.add(x.queue, 0, 1)
		if State(x.state) == StateDead {
			m.w.logger.LogAttrs(ctx, slog.LevelError, "attempt failed; the task is dead",
				slog.Int64(logKeyTaskID, x.id), slog.String(logKeyKind, x.kind),
				slog.Int(logKeyAttempt, x.attempt), slog.String(logKeyError, leaseExpired))
		}
	}
}

// sweep runs fn in transactions until a run handles less than a full batch,
// and calls committed after each transaction commits.
func (m *maintainer) sweep(ctx context.Context, committed func(), fn func(context.Context, querier) (int, error)) error {
	for {
		var n int
		err := m.c.inTx(ctx, func(q querier) error {
			var err error
			n, err = fn(ctx, q)
			return err
		})
		if err != nil {
			return err
		}
		committed()
		if n < maintenanceBatch {
			return nil
		}
	}
}

// swept is a task that a maintenance sweep changed.
type swept struct {
	id                        int64
	queue, group, state, kind string
	attempt                   int
}

// collectSwept reads the tasks that a sweep changed, in a fixed order of queue
// and group: updating their groups in that order cannot deadlock with
// completions, which use the same order.
func collectSwept(r rows) ([]swept, error) {
	defer r.Close()
	var out []swept
	for r.Next() {
		var x swept
		if err := r.Scan(&x.id, &x.queue, &x.group, &x.state, &x.kind, &x.attempt); err != nil {
			return nil, err
		}
		out = append(out, x)
	}
	slices.SortFunc(out, func(a, b swept) int {
		return cmp.Or(cmp.Compare(a.queue, b.queue), cmp.Compare(a.group, b.group), cmp.Compare(a.id, b.id))
	})
	return out, r.Err()
}

// retain applies retention: it keeps the next week's history partitions
// ready, drops expired ones, and deletes old dead and canceled tasks, expired
// dedupe keys, old activity, and workers that stopped without unregistering.
func (m *maintainer) retain(ctx context.Context) {
	m.retention.observe(ctx, m.applyRetention(ctx))
}

// applyRetention does the work of retain, and returns the errors of the
// steps that failed: one step's failure does not stop the others.
func (m *maintainer) applyRetention(ctx context.Context) error {
	var errs []error
	if err := ensureHistoryPartitions(ctx, m.c.pool, m.c.schema, time.Now(), historyDaysAhead); err != nil {
		if pgErr, ok := errors.AsType[*pgconn.PgError](err); ok && pgErr.Code == "42501" { // insufficient_privilege
			err = fmt.Errorf("%w; the role that runs workers must own the taskmill tables", err)
		}
		errs = append(errs, err)
	}

	var historySecs, deadSecs int64
	if err := m.c.pool.QueryRow(ctx, m.c.sql.retention).Scan(&historySecs, &deadSecs); err != nil {
		return errors.Join(append(errs, fmt.Errorf("read retention settings: %w", err))...)
	}
	if err := m.dropHistory(ctx, time.Duration(historySecs)*time.Second); err != nil {
		errs = append(errs, fmt.Errorf("drop expired history: %w", err))
	}
	if err := m.deleteInBatches(ctx, m.c.sql.deleteFinished, deadSecs); err != nil {
		errs = append(errs, fmt.Errorf("delete old dead and canceled tasks: %w", err))
	}
	if err := m.deleteInBatches(ctx, m.c.sql.deleteDedupeKeys); err != nil {
		errs = append(errs, fmt.Errorf("delete expired dedupe keys: %w", err))
	}
	if err := m.deleteInBatches(ctx, m.c.sql.deleteActivity, activityRetention.Seconds()); err != nil {
		errs = append(errs, fmt.Errorf("delete old activity: %w", err))
	}
	if _, err := m.c.pool.Exec(ctx, m.c.sql.deleteWorkers, staleWorkerAge.Seconds()); err != nil {
		errs = append(errs, fmt.Errorf("delete stale workers: %w", err))
	}
	return errors.Join(errs...)
}

// dropHistory detaches and drops the history partitions whose whole day is
// older than the retention period.
func (m *maintainer) dropHistory(ctx context.Context, retention time.Duration) error {
	var names []string
	pending := map[string]bool{}
	err := pgx.BeginFunc(ctx, m.c.pool, func(tx pgx.Tx) error {
		existing, err := historyPartitions(ctx, tx, m.c.schema)
		if err != nil {
			return err
		}
		for name := range existing {
			names = append(names, name)
		}
		r, err := tx.Query(ctx, m.c.sql.pendingDetach)
		if err != nil {
			return err
		}
		detaching, err := pgx.CollectRows(r, pgx.RowTo[string])
		if err != nil {
			return err
		}
		for _, name := range detaching {
			pending[name] = true
		}
		return nil
	})
	if err != nil {
		return err
	}
	slices.Sort(names)

	cutoff := time.Now().Add(-retention)
	for _, name := range names {
		day, err := time.Parse("20060102", strings.TrimPrefix(name, "task_history_"))
		if err != nil || !day.AddDate(0, 0, 1).Before(cutoff) {
			continue
		}
		if err := m.dropPartition(ctx, name, pending[name]); err != nil {
			return err
		}
		m.w.logger.LogAttrs(ctx, slog.LevelInfo, "dropped expired history", slog.String(logKeyPartition, name))
	}
	return nil
}

// dropPartition detaches and drops one history partition. DETACH ...
// CONCURRENTLY does not block completions, and must run outside a
// transaction. It waits for transactions that use the history, so it is
// bounded by a timeout; if it is interrupted, the partition stays pending
// detach, and a later run finalizes the detach instead.
func (m *maintainer) dropPartition(ctx context.Context, name string, pending bool) error {
	ctx, cancel := context.WithTimeout(ctx, detachTimeout)
	defer cancel()
	mode := "CONCURRENTLY"
	if pending {
		mode = "FINALIZE"
	}
	detach := fmt.Sprintf("ALTER TABLE %[1]s.task_history DETACH PARTITION %[1]s.%[2]s %[3]s", m.c.schema, name, mode)
	if _, err := m.c.pool.Exec(ctx, detach); err != nil {
		return fmt.Errorf("detach %s: %w", name, err)
	}
	if _, err := m.c.pool.Exec(ctx, fmt.Sprintf("DROP TABLE %s.%s", m.c.schema, name)); err != nil {
		return fmt.Errorf("drop %s: %w", name, err)
	}
	return nil
}

// deleteInBatches runs a delete statement that deletes up to one batch,
// until it deletes less than a full batch.
func (m *maintainer) deleteInBatches(ctx context.Context, sql string, args ...any) error {
	for {
		tag, err := m.c.pool.Exec(ctx, sql, args...)
		if err != nil || tag.RowsAffected() < maintenanceBatch {
			return err
		}
	}
}

// untilNextDue returns how long to sleep: until the next scheduled task is
// due, but at most one maintenance interval.
func (m *maintainer) untilNextDue(ctx context.Context) time.Duration {
	var secs *float64
	if err := m.c.pool.QueryRow(ctx, m.c.sql.nextDue).Scan(&secs); err != nil || secs == nil {
		return m.w.timing.maintainer
	}
	d := time.Duration(*secs * float64(time.Second))
	return min(max(d, minMaintainerSleep), m.w.timing.maintainer)
}

// refreshStats counts each queue's tasks per state, up to the caps, and
// stores the counts for [Client.Queues] and the current minute's activity.
func (m *maintainer) refreshStats(ctx context.Context) {
	m.stats.observe(ctx, m.countStats(ctx))
}

// countStats does the work of refreshStats.
func (m *maintainer) countStats(ctx context.Context) error {
	var queues, states []string
	var counts []int64
	var capped []bool
	var dueTotal int64 // scheduled and retryable tasks counted, in all queues
	r, err := m.c.pool.Query(ctx, m.c.sql.countStats, statsQueueCap, statsDueCap)
	if err != nil {
		return fmt.Errorf("count tasks: %w", err)
	}
	for r.Next() {
		var state, queue string
		var n int64
		if err := r.Scan(&state, &queue, &n); err != nil {
			r.Close()
			return fmt.Errorf("count tasks: %w", err)
		}
		if s := State(state); s == StateScheduled || s == StateRetryable {
			dueTotal += n
		}
		queues, states, counts = append(queues, queue), append(states, state), append(counts, n)
		capped = append(capped, n >= statsQueueCap)
	}
	r.Close()
	if err := r.Err(); err != nil {
		return fmt.Errorf("count tasks: %w", err)
	}
	// The scheduled and retryable counts share one cap: once it is hit,
	// any queue may have more of them than counted, even none counted.
	if dueTotal >= statsDueCap {
		for i := range capped {
			capped[i] = true
		}
	}

	err = pgx.BeginFunc(ctx, m.c.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, m.c.sql.clearStats); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, m.c.sql.storeStats, queues, states, counts, capped); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, m.c.sql.storeActivity)
		return err
	})
	if err != nil {
		return fmt.Errorf("store counts: %w", err)
	}
	return nil
}
