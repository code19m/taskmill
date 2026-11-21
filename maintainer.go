package taskmill

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
)

// Maintenance limits.
const (
	maintenanceBatch   = 1000                  // most tasks that one statement of the maintainer, or one batch of a bulk action, changes
	minMaintainerSleep = 50 * time.Millisecond // shortest wait for the next due task: at most 20 promotions a second
	staleWorkerAge     = 5 * time.Minute       // a worker that has not checked in this long, and for two of its leases, is removed
)

// maintainer does the installation-wide work of one elected worker. Its main
// loop elects the leader, then rescues expired leases and enqueues schedules
// once per interval. Queues whose tasks wait are woken in a loop of their
// own, which also sends the heartbeat that workers count on, and due tasks
// are promoted in another, as they come due. Statistics, retention, and the
// comparison of the workers' schedules have a loop each too, so that slow
// work in one never delays the others.
type maintainer struct {
	w      *Worker
	c      *Client
	leader atomic.Bool
	term   chan struct{} // a term began: the wake loop sends the heartbeat right away

	// Used by the main loop only.
	lastElection time.Time
	lastTakeover time.Time // when a notification that the maintainer resigned last made this worker try to lead

	// The failures of each kind of work, which it logs only now and then.
	electionFailures, promotionFailures, wakeFailures, rescueFailures *incidentLog
	scheduleFailures, pruneFailures, compareFailures                  *incidentLog
	statsFailures, retentionFailures                                  *incidentLog

	ownHashes map[string]string // the hash of each of this worker's schedules, by name
	watch     scheduleWatch     // a difference between the workers' schedules and this maintainer's
}

func newMaintainer(w *Worker) *maintainer {
	return &maintainer{
		w:                 w,
		c:                 w.client,
		term:              make(chan struct{}, 1),
		electionFailures:  newIncidentLog(w, "elect maintainer"),
		promotionFailures: newIncidentLog(w, "promote due tasks"),
		wakeFailures:      newIncidentLog(w, "wake queues"),
		rescueFailures:    newIncidentLog(w, "rescue expired leases"),
		scheduleFailures:  newIncidentLog(w, "run schedules"),
		pruneFailures:     newIncidentLog(w, "delete removed schedules"),
		compareFailures:   newIncidentLog(w, "compare schedules"),
		statsFailures:     newIncidentLog(w, "refresh statistics"),
		retentionFailures: newIncidentLog(w, "apply retention"),
		ownHashes:         byName(w.scheduleNames, w.scheduleHashes),
	}
}

func (m *maintainer) run(ctx context.Context) {
	var loops sync.WaitGroup
	loops.Go(func() { m.wakeLoop(ctx) })
	loops.Go(func() { m.promoteLoop(ctx) })
	loops.Go(func() { m.every(ctx, m.w.timing.stats, m.refreshStats) })
	loops.Go(func() { m.every(ctx, m.w.timing.scheduleCheck, m.compareSchedules) })
	loops.Go(func() {
		m.every(ctx, m.w.timing.retention, func(ctx context.Context) {
			m.retain(ctx)
			m.pruneSchedules(ctx)
		})
	})
	defer loops.Wait()

	tick := time.NewTicker(m.w.timing.maintainer)
	defer tick.Stop()
	m.cycle(ctx)
	for {
		select {
		case <-ctx.Done():
			m.resign(context.WithoutCancel(ctx))
			return
		case <-m.w.resigned:
			m.takeOver(ctx)
		case <-tick.C:
			m.cycle(ctx)
		}
	}
}

// takeOver follows a notification that the maintainer resigned: this worker
// tries to lead now, not at its next election. The notification is only a
// hint, which any role in the database can send as often as it likes. So a
// worker that leads ignores it, and another worker follows it at most once
// per election interval: a flood adds at most one election try per interval
// to the one that the worker makes anyway.
func (m *maintainer) takeOver(ctx context.Context) {
	if m.leader.Load() || time.Since(m.lastTakeover) < m.w.timing.election {
		return
	}
	m.lastTakeover = time.Now()
	m.lastElection = time.Time{}
	m.cycle(ctx)
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

// wakeLoop wakes the queues whose tasks wait once per interval while this
// worker leads, and sends the heartbeat with it at the start of each term and
// every heartbeat interval.
func (m *maintainer) wakeLoop(ctx context.Context) {
	tick := time.NewTicker(m.w.timing.maintainer)
	defer tick.Stop()
	every := max(1, int(m.w.timing.heartbeat/m.w.timing.maintainer))
	ticks := every // since the last heartbeat went out

	for {
		select {
		case <-ctx.Done():
			return
		case <-m.term:
			ticks = every
		case <-tick.C:
			ticks++
		}

		if !m.leader.Load() {
			continue
		}
		heartbeat := ticks >= every
		if m.wake(ctx, heartbeat) && heartbeat {
			ticks = 0
		}
	}
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

// every calls work once per interval while this worker leads, until ctx
// ends. Statistics, retention, and the comparison of schedules each run in
// such a loop of their own: retention can take long, when a detach waits
// for a transaction that reads the history, or a backlog of old tasks takes
// many batches, and the statistics, which lag is read from, must stay fresh
// meanwhile. The comparison waits for neither.
func (m *maintainer) every(ctx context.Context, interval time.Duration, work func(context.Context)) {
	tick := time.NewTicker(m.w.timing.maintainer)
	defer tick.Stop()
	var last time.Time

	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}

		if m.leader.Load() && time.Since(last) >= interval {
			work(ctx)
			last = time.Now()
		}
	}
}

// bound returns the context of one statement or transaction of the
// maintainer's work, other than the election: ctx, ended after one leader
// lease. A connection that stops answering without being reset, as in a
// failover that drops packets, would hold the work for as long as the
// operating system waits for an answer, often 15 minutes, while the election
// keeps the maintainer's role on other connections. Bound, a stuck statement
// ends within the lease, and the work tries again on another connection. A
// statement that answers takes far less: a slow one has a bound of its own,
// such as the detach of a history partition.
func (m *maintainer) bound(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, m.w.timing.leaderLease)
}

// outlive returns a context that keeps ctx's values, but not its end: it ends
// after limit, or grace after ctx has ended, whichever comes first.
func outlive(ctx context.Context, limit, grace time.Duration) (context.Context, context.CancelFunc) {
	out, cancel := context.WithTimeout(context.WithoutCancel(ctx), limit)
	stop := context.AfterFunc(ctx, func() {
		select {
		case <-out.Done():
		case <-time.After(grace):
			cancel()
		}
	})
	return out, func() {
		stop()
		cancel()
	}
}

// elect takes or renews the maintainer's lease, and reports whether this
// worker leads.
//
// The election does not end with ctx, which ends when the worker stops. The
// worker then resigns, and it must know before that what became of its
// election. An election that it gave up could reach the database after the
// resignation, and give the lease to a worker that has stopped: nobody would
// lead until that lease ran out. So a worker that stops waits for the answer
// to the election that is under way, and starts no other.
//
// The wait is bounded. While the worker runs, an election has the lease's
// time: by then the lease that it would renew has run out. Once the worker
// is told to stop, it has cleanupTimeout left, so that a database that does
// not answer holds the stop up no longer than the resignation may. An
// election without an answer by then is given up. Should the database run it
// later still, after the resignation, its lease lasts until it runs out, as
// a crashed worker's does.
func (m *maintainer) elect(ctx context.Context) bool {
	if ctx.Err() != nil {
		return false
	}

	electCtx, cancel := outlive(ctx, m.w.timing.leaderLease, cleanupTimeout)
	defer cancel()

	var id string
	err := m.c.pool.QueryRow(electCtx, m.c.sql.electMaintainer, m.w.id, m.w.timing.leaderLease.Microseconds()).Scan(&id)
	if err != nil && ctx.Err() != nil {
		// The worker stops, and the statement may have committed all the
		// same. Whatever this worker knows, resign gives the lease up.
		return false
	}
	leader := err == nil
	if errors.Is(err, pgx.ErrNoRows) {
		err = nil // another worker leads
	}
	m.electionFailures.observe(ctx, err)
	was := m.leader.Load() // only the main loop changes it

	if leader && !was {
		// A term starts without the incidents of the last one, whose work
		// may have failed after it ended, and without the difference
		// between the schedules that it found: that one may have ended
		// meanwhile, and a new one must last its own time.
		for _, f := range []*incidentLog{
			m.promotionFailures, m.wakeFailures, m.rescueFailures, m.scheduleFailures, m.pruneFailures, m.compareFailures,
			m.statsFailures, m.retentionFailures,
		} {
			f.reset()
		}
		m.watch.start()
	}

	m.leader.Store(leader)
	switch {
	case leader && !was:
		m.w.logger.LogAttrs(ctx, slog.LevelInfo, "elected maintainer")
		select {
		case m.term <- struct{}{}:
		default:
		}
	case was && !leader:
		m.watch.end()
		m.w.logger.LogAttrs(ctx, slog.LevelInfo, "no longer maintainer")
	}
	return leader
}

// resign gives up the maintainer's lease when the worker stops, so that
// another worker takes over right away, not once the lease expires. It asks
// the database even if this worker does not know that it leads: an election
// whose answer was lost, or a renewal that failed, leaves a lease that only
// the database knows of. For a worker without a lease, the statement gives
// up nothing and notifies nobody. Either way it clears the schedules that
// the worker stored: it leaves the election, and while it drains its tasks,
// which can take long, its schedules must not count as a difference.
func (m *maintainer) resign(ctx context.Context) {
	led := m.leader.Swap(false)

	ctx, cancel := context.WithTimeout(ctx, cleanupTimeout)
	defer cancel()
	if _, err := m.c.pool.Exec(ctx, m.c.sql.resignMaintainer, m.w.id, m.c.schema); err != nil && led {
		m.w.logger.LogAttrs(ctx, slog.LevelWarn, "resign maintainer failed; another worker takes over when the lease runs out",
			slog.Any(logKeyError, err))
	}
}

// promote makes due scheduled and retryable tasks available. The statement
// also wakes the workers of their queues.
func (m *maintainer) promote(ctx context.Context) {
	for {
		var promoted int
		stmtCtx, cancel := m.bound(ctx)
		err := m.c.pool.QueryRow(stmtCtx, m.c.sql.promoteDue, m.c.schema).Scan(&promoted)
		cancel()
		if err != nil || promoted < maintenanceBatch {
			m.promotionFailures.observe(ctx, err)
			return
		}
	}
}

// wake notifies every queue that is not paused and whose next task waited
// longer than a maintenance interval, and sends the heartbeat if asked. It
// reports whether it worked. A task can become available without a
// notification that comes after it: the throttle's trailing notification can
// come before the transactions that it stands for commit, and a producer can
// crash before sending it. Idle queues poll rarely while heartbeats arrive,
// so this finds such tasks within two intervals, with one statement for the
// whole installation. A queue whose workers are all busy is woken too, which
// changes nothing; a queue whose tasks start right away is not.
func (m *maintainer) wake(ctx context.Context, heartbeat bool) bool {
	stmtCtx, cancel := m.bound(ctx)
	defer cancel()
	_, err := m.c.pool.Exec(stmtCtx, m.c.sql.wakeQueues, m.c.schema, m.w.timing.maintainer.Microseconds(), heartbeat)
	m.wakeFailures.observe(ctx, err)
	return err == nil
}

// leaseExpired is the error that a rescue records for the attempt it fails.
const leaseExpired = "the worker lost its lease: it crashed, stopped, or lost the database"

// rescue fails the attempts whose leases expired and updates their groups,
// one batch in a transaction, until a batch is not full. It skips tasks
// whose groups other transactions hold; a later cycle rescues them. Each
// failed attempt counts in the activity, and is logged, once its batch
// commits.
func (m *maintainer) rescue(ctx context.Context) {
	for {
		batch, err := m.rescueBatch(ctx)
		if err == nil {
			m.rescued(ctx, batch)
		}
		if err != nil || len(batch) < maintenanceBatch {
			m.rescueFailures.observe(ctx, err)
			return
		}
	}
}

// rescueBatch fails a batch of the attempts whose leases expired, in one
// transaction, and returns their tasks. A task that became dead halts its
// FIFO group, and one that became canceled leaves it.
func (m *maintainer) rescueBatch(ctx context.Context) (batch []rescuedTask, err error) {
	ctx, cancel := m.bound(ctx)
	defer cancel()
	err = m.c.inTx(ctx, func(q querier) error {
		r, err := q.query(ctx, m.c.sql.rescue,
			defaultBackoffBase.Seconds(), defaultBackoffMax.Seconds(), leaseExpired)
		if err != nil {
			return err
		}
		if batch, err = collectRescued(r); err != nil {
			return err
		}

		var left []groupLeave
		for _, x := range batch {
			switch s := State(x.state); {
			case x.group == "":
			case s == StateDead:
				if err := m.c.haltGroup(ctx, m.w.logger, q, x.queue, x.group, x.id); err != nil {
					return err
				}
			case s.Finished():
				left = append(left, groupLeave{id: x.id, queue: x.queue, group: x.group, head: true})
			}
		}
		return m.c.leaveGroups(ctx, m.w.logger, q, left)
	})
	return batch, err
}

// rescued counts and logs the failed attempts of a committed rescue batch.
func (m *maintainer) rescued(ctx context.Context, batch []rescuedTask) {
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

// rescuedTask is a task whose expired lease a rescue ended.
type rescuedTask struct {
	id                        int64
	queue, group, state, kind string
	attempt                   int
}

// collectRescued reads the tasks that a rescue changed. The rescue statement
// holds their groups' rows, so their order does not matter.
func collectRescued(r rows) ([]rescuedTask, error) {
	defer r.Close()
	var out []rescuedTask
	for r.Next() {
		var x rescuedTask
		if err := r.Scan(&x.id, &x.queue, &x.group, &x.state, &x.kind, &x.attempt); err != nil {
			return nil, err
		}
		out = append(out, x)
	}
	return out, r.Err()
}

// retain applies retention: it keeps the next week's history partitions
// ready, drops expired ones, and deletes old dead and canceled tasks, expired
// dedupe keys, old activity, and workers that stopped without unregistering.
func (m *maintainer) retain(ctx context.Context) {
	m.retentionFailures.observe(ctx, m.applyRetention(ctx))
}

// applyRetention does the work of retain, and returns the errors of the
// steps that failed: one step's failure does not stop the others. Each
// statement has the bound of one: see bound.
func (m *maintainer) applyRetention(ctx context.Context) error {
	var errs []error
	if err := m.ensureHistory(ctx); err != nil {
		errs = append(errs, err)
	}

	if err := m.dropHistory(ctx); err != nil {
		errs = append(errs, fmt.Errorf("drop expired history: %w", err))
	}
	if err := m.deleteInBatches(ctx, m.c.sql.deleteFinished); err != nil {
		errs = append(errs, fmt.Errorf("delete old dead and canceled tasks: %w", err))
	}
	if err := m.deleteInBatches(ctx, m.c.sql.deleteDedupeKeys); err != nil {
		errs = append(errs, fmt.Errorf("delete expired dedupe keys: %w", err))
	}
	if err := m.deleteInBatches(ctx, m.c.sql.deleteActivity, activityRetention.Seconds()); err != nil {
		errs = append(errs, fmt.Errorf("delete old activity: %w", err))
	}
	if err := m.deleteWorkers(ctx); err != nil {
		errs = append(errs, fmt.Errorf("delete stale workers: %w", err))
	}
	return errors.Join(errs...)
}

// ensureHistory creates the history partitions that the next week lacks.
func (m *maintainer) ensureHistory(ctx context.Context) error {
	ctx, cancel := m.bound(ctx)
	defer cancel()
	return ensureHistoryPartitions(ctx, m.c.pool, m.c.schema)
}

// deleteWorkers deletes the workers that stopped without unregistering.
func (m *maintainer) deleteWorkers(ctx context.Context) error {
	ctx, cancel := m.bound(ctx)
	defer cancel()
	_, err := m.c.pool.Exec(ctx, m.c.sql.deleteWorkers, staleWorkerAge.Seconds())
	return err
}

// deleteInBatches runs a delete statement that deletes up to one batch,
// until it deletes less than a full batch.
func (m *maintainer) deleteInBatches(ctx context.Context, sql string, args ...any) error {
	for {
		stmtCtx, cancel := m.bound(ctx)
		tag, err := m.c.pool.Exec(stmtCtx, sql, args...)
		cancel()
		if err != nil || tag.RowsAffected() < maintenanceBatch {
			return err
		}
	}
}

// untilNextDue returns how long to sleep: until the next scheduled task is
// due, but at most one maintenance interval.
func (m *maintainer) untilNextDue(ctx context.Context) time.Duration {
	ctx, cancel := m.bound(ctx)
	defer cancel()
	var secs *float64
	if err := m.c.pool.QueryRow(ctx, m.c.sql.nextDue).Scan(&secs); err != nil || secs == nil {
		return m.w.timing.maintainer
	}
	return sleepUntilDue(*secs, m.w.timing.maintainer)
}

// sleepUntilDue returns the sleep before a task that is due in secs seconds:
// at least minMaintainerSleep, and at most limit. It limits the seconds, not
// the Duration that they make. A run time can be further away than a Duration
// holds, about 292 years, and Go leaves open what such a number becomes: on
// amd64 the lowest Duration, which gave the shortest sleep, time after time.
func sleepUntilDue(secs float64, limit time.Duration) time.Duration {
	secs = min(max(secs, minMaintainerSleep.Seconds()), limit.Seconds())
	return time.Duration(secs * float64(time.Second))
}

// Limits of a statistics refresh.
const (
	statsQueueCap = 10_000  // most tasks counted per queue and state, and halted groups per queue
	statsDueCap   = 100_000 // most scheduled and retryable tasks counted in all
	statsRetryCap = 10_000  // most retryable tasks whose attempts are read in all, besides each queue's last
)

// refreshStats counts each queue's tasks per state, up to the caps, finds
// each count's oldest due task, its queue's halted groups, and the most
// failures of its queue's retryable tasks, and stores them for
// [Client.Queues] and the counts for the current minute's activity.
func (m *maintainer) refreshStats(ctx context.Context) {
	m.statsFailures.observe(ctx, m.recountStats(ctx))
}

// recountStats does the work of refreshStats, in one transaction with JIT
// off.
// The count only reads, so the transaction takes its locks at the end, when
// it stores the counts.
func (m *maintainer) recountStats(ctx context.Context) error {
	ctx, cancel := m.bound(ctx)
	defer cancel()
	return pgx.BeginFunc(ctx, m.c.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, statsSettings); err != nil {
			return fmt.Errorf("count tasks: %w", err)
		}
		stats, err := readStats(ctx, tx, m.c.sql.countStats)
		if err != nil {
			return fmt.Errorf("count tasks: %w", err)
		}

		if err := stats.store(ctx, tx, m.c.sql); err != nil {
			return fmt.Errorf("store counts: %w", err)
		}
		return nil
	})
}

// statsSnapshot holds one refresh's statistics as storeStats takes them,
// with one entry per queue and state.
type statsSnapshot struct {
	queues, states []string
	counts         []int64
	capped         []bool
	oldest         []*time.Time // the run time of the oldest due task of each count
	halted         []*int64     // with each dead count, the queue's halted groups
	failures       []*int32     // with each retryable count, the most failures of its tasks
	dueCapped      bool         // the scheduled and retryable tasks passed their shared cap
}

// readStats runs the statement sql that counts the tasks, and caps the
// counts.
func readStats(ctx context.Context, tx pgx.Tx, sql string) (*statsSnapshot, error) {
	var stats statsSnapshot
	var dueTotal int64 // scheduled and retryable tasks counted, in all queues

	// Each cap is read one past, so that a count that ends exactly at its cap
	// is not taken for one with more tasks left.
	r, err := tx.Query(ctx, sql, statsQueueCap+1, statsDueCap+1, statsRetryCap)
	if err != nil {
		return nil, err
	}
	defer r.Close()

	for r.Next() {
		var state, queue string
		var n int64
		var due *time.Time
		var h *int64
		var f *int32
		if err := r.Scan(&state, &queue, &n, &due, &h, &f); err != nil {
			return nil, err
		}

		if s := State(state); s == StateScheduled || s == StateRetryable {
			dueTotal += n
		}
		stats.queues, stats.states, stats.counts = append(stats.queues, queue), append(stats.states, state), append(stats.counts, min(n, statsQueueCap))
		stats.capped, stats.oldest = append(stats.capped, n > statsQueueCap), append(stats.oldest, due)
		stats.halted, stats.failures = append(stats.halted, h), append(stats.failures, f)
	}
	if err := r.Err(); err != nil {
		return nil, err
	}

	// The scheduled and retryable counts share one cap: once it is passed,
	// any queue may have more of them than counted, even none counted.
	stats.dueCapped = dueTotal > statsDueCap
	if stats.dueCapped {
		for i, s := range stats.states {
			if s := State(s); s == StateScheduled || s == StateRetryable {
				stats.capped[i] = true
			}
		}
	}
	return &stats, nil
}

// store replaces the stored statistics with s, and copies its counts into
// the current minute's activity. One refresh stores at a time, so that two
// maintainers whose terms overlap both store what they counted: see
// lockStats.
func (s *statsSnapshot) store(ctx context.Context, tx pgx.Tx, sql *statements) error {
	if _, err := tx.Exec(ctx, sql.lockStats); err != nil {
		return err
	}

	if _, err := tx.Exec(ctx, sql.clearStats); err != nil {
		return err
	}
	_, err := tx.Exec(ctx, sql.storeStats,
		s.queues,    // $1
		s.states,    // $2
		s.counts,    // $3
		s.capped,    // $4
		s.oldest,    // $5
		s.dueCapped, // $6
		s.halted,    // $7
		s.failures,  // $8
	)
	if err != nil {
		return err
	}

	_, err = tx.Exec(ctx, sql.storeActivity)
	return err
}
