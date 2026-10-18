package taskmill

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// Maintainer timing.
const (
	maintainerInterval = time.Second      // how often the leader works
	leaderLease        = 10 * time.Second // how long a leader stays leader without renewing
	electionInterval   = 2 * time.Second  // how often other workers try to take over
	retentionInterval  = time.Minute
	staleWorkerAge     = 5 * time.Minute
	detachTimeout      = 30 * time.Second
	maintenanceBatch   = 1000
)

// maintainer does the cluster-wide work of one elected worker: it promotes
// due tasks, rescues expired leases, expires deadlines, enqueues schedules,
// and applies retention.
type maintainer struct {
	w             *Worker
	c             *Client
	leader        bool
	lastRetention time.Time
	lastStats     time.Time
}

func newMaintainer(w *Worker) *maintainer {
	return &maintainer{w: w, c: w.client}
}

func (m *maintainer) run(ctx context.Context) {
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			m.resign(context.WithoutCancel(ctx))
			return
		case <-timer.C:
		}
		timer.Reset(m.cycle(ctx))
	}
}

// cycle runs one round of maintenance if this worker is the leader, and
// returns how long to wait before the next.
func (m *maintainer) cycle(ctx context.Context) time.Duration {
	if !m.elect(ctx) {
		return electionInterval
	}
	m.promote(ctx)
	m.rescue(ctx)
	m.expireDeadlines(ctx)
	m.runSchedules(ctx)
	if time.Since(m.lastStats) >= statsInterval {
		m.refreshStats(ctx)
		m.lastStats = time.Now()
	}
	if time.Since(m.lastRetention) >= retentionInterval {
		m.retain(ctx)
		m.lastRetention = time.Now()
	}
	return m.untilNextDue(ctx)
}

func (m *maintainer) elect(ctx context.Context) bool {
	var id string
	err := m.c.pool.QueryRow(ctx, m.c.sql.electLeader, m.w.cfg.ID, leaderLease.Microseconds()).Scan(&id)
	leader := err == nil
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		m.log(ctx, slog.LevelWarn, "maintainer election", err)
	}
	if leader != m.leader {
		m.leader = leader
		if leader {
			m.w.logger.LogAttrs(ctx, slog.LevelInfo, "elected maintainer")
		}
	}
	return leader
}

func (m *maintainer) resign(ctx context.Context) {
	if !m.leader {
		return
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if _, err := m.c.pool.Exec(ctx, m.c.sql.resignLeader, m.w.cfg.ID); err != nil {
		m.log(ctx, slog.LevelWarn, "resign maintainer", err)
	}
	m.leader = false
}

// promote makes due scheduled and retryable tasks available, and wakes the
// workers of their queues.
func (m *maintainer) promote(ctx context.Context) {
	for {
		r, err := m.c.pool.Query(ctx, m.c.sql.promoteDue, maintenanceBatch)
		if err != nil {
			m.log(ctx, slog.LevelError, "promote due tasks", err)
			return
		}
		total := 0
		var queues []string
		for r.Next() {
			var queue string
			var n int
			if err := r.Scan(&queue, &n); err != nil {
				r.Close()
				m.log(ctx, slog.LevelError, "promote due tasks", err)
				return
			}
			total += n
			queues = append(queues, queue)
		}
		r.Close()
		if err := r.Err(); err != nil {
			m.log(ctx, slog.LevelError, "promote due tasks", err)
			return
		}
		for _, q := range queues {
			m.c.notifyAvailable(ctx, pgxQuerier{q: m.c.pool}, q)
		}
		if total < maintenanceBatch {
			return
		}
	}
}

// rescue fails the attempts whose leases expired and updates their groups.
func (m *maintainer) rescue(ctx context.Context) {
	m.sweep(ctx, "rescue expired leases", func(ctx context.Context, q querier) (int, error) {
		r, err := q.query(ctx, m.c.sql.rescue,
			defaultBackoffBase.Seconds(), defaultBackoffMax.Seconds(), maintenanceBatch)
		if err != nil {
			return 0, err
		}
		rows, err := collectSwept(r)
		if err != nil {
			return 0, err
		}
		for _, x := range rows {
			if x.group == "" || !State(x.state).Finished() {
				continue
			}
			if err := m.c.leaveGroup(ctx, q, x.queue, x.group, x.id, true, State(x.state) == StateDead); err != nil {
				return 0, err
			}
		}
		if len(rows) > 0 {
			m.w.logger.LogAttrs(ctx, slog.LevelWarn, "rescued tasks whose workers lost their leases",
				slog.Int(logKeyTasks, len(rows)))
		}
		return len(rows), nil
	})
}

// expireDeadlines makes waiting tasks dead once their deadline passed.
func (m *maintainer) expireDeadlines(ctx context.Context) {
	m.sweep(ctx, "expire deadlines", func(ctx context.Context, q querier) (int, error) {
		r, err := q.query(ctx, m.c.sql.expireDeadline, maintenanceBatch)
		if err != nil {
			return 0, err
		}
		rows, err := collectSwept(r)
		if err != nil {
			return 0, err
		}
		for _, x := range rows {
			if x.group == "" {
				continue
			}
			head := State(x.state) != StatePending
			if err := m.c.leaveGroup(ctx, q, x.queue, x.group, x.id, head, true); err != nil {
				return 0, err
			}
		}
		return len(rows), nil
	})
}

// sweep runs fn in transactions until a run handles less than a full batch.
func (m *maintainer) sweep(ctx context.Context, what string, fn func(context.Context, querier) (int, error)) {
	for {
		var n int
		err := m.c.inTx(ctx, func(q querier) error {
			var err error
			n, err = fn(ctx, q)
			return err
		})
		if err != nil {
			m.log(ctx, slog.LevelError, what, err)
			return
		}
		if n < maintenanceBatch {
			return
		}
	}
}

// swept is a task that a maintenance sweep changed.
type swept struct {
	id                  int64
	queue, group, state string
}

// collectSwept reads the tasks that a sweep changed, in a fixed order of queue
// and group: updating their groups in that order cannot deadlock with
// completions, which use the same order.
func collectSwept(r rows) ([]swept, error) {
	defer r.Close()
	var out []swept
	for r.Next() {
		var x swept
		if err := r.Scan(&x.id, &x.queue, &x.group, &x.state); err != nil {
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
// dedupe keys, and workers that stopped without unregistering.
func (m *maintainer) retain(ctx context.Context) {
	if err := ensureHistoryPartitions(ctx, m.c.pool, m.c.schema, time.Now(), historyDaysAhead); err != nil {
		m.log(ctx, slog.LevelError, "prepare history partitions", err)
	}

	var historySecs, deadSecs int64
	if err := m.c.pool.QueryRow(ctx, m.c.sql.retention).Scan(&historySecs, &deadSecs); err != nil {
		m.log(ctx, slog.LevelError, "read retention settings", err)
		return
	}
	if err := m.dropHistory(ctx, time.Duration(historySecs)*time.Second); err != nil {
		m.log(ctx, slog.LevelError, "drop expired history", err)
	}
	m.deleteInBatches(ctx, "delete old dead and canceled tasks", m.c.sql.deleteFinished, deadSecs)
	m.deleteInBatches(ctx, "delete expired dedupe keys", m.c.sql.deleteDedupeKeys)
	if _, err := m.c.pool.Exec(ctx, m.c.sql.deleteWorkers, staleWorkerAge.Seconds()); err != nil {
		m.log(ctx, slog.LevelWarn, "delete stale workers", err)
	}
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
		for _, name := range detaching {
			pending[name] = true
		}
		return err
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
		m.w.logger.LogAttrs(ctx, slog.LevelInfo, "dropped expired history", slog.String(logKeyPart, name))
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

// deleteInBatches runs a delete statement that takes extra arguments followed
// by a batch size, until it deletes less than a full batch.
func (m *maintainer) deleteInBatches(ctx context.Context, what, sql string, args ...any) {
	args = append(args, maintenanceBatch)
	for {
		tag, err := m.c.pool.Exec(ctx, sql, args...)
		if err != nil {
			m.log(ctx, slog.LevelError, what, err)
			return
		}
		if tag.RowsAffected() < maintenanceBatch {
			return
		}
	}
}

// untilNextDue returns how long to sleep: until the next scheduled task is
// due, but at most one maintenance interval.
func (m *maintainer) untilNextDue(ctx context.Context) time.Duration {
	var secs *float64
	if err := m.c.pool.QueryRow(ctx, m.c.sql.nextDue).Scan(&secs); err != nil || secs == nil {
		return maintainerInterval
	}
	d := time.Duration(*secs * float64(time.Second))
	return min(max(d, 10*time.Millisecond), maintainerInterval)
}

// log reports a failed maintenance step.
func (m *maintainer) log(ctx context.Context, level slog.Level, step string, err error) {
	if ctx.Err() != nil {
		return // shutting down
	}
	m.w.logger.LogAttrs(ctx, level, "maintenance step failed", slog.String(logKeyStep, step), slog.Any(logKeyError, err))
}
