package taskmill

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/jackc/pgx/v5"
)

// This file holds a queue's settings, which are the same for every worker:
// how an operator reads and changes them, and how each worker follows them.

// QueueSettings are a queue's installation-wide settings, stored in the
// database. Change them with [Client.UpdateQueue]. The zero value means no
// limit.
type QueueSettings struct {
	// MaxRunning caps the tasks of the queue running at the same time across
	// all workers of the installation. Each task of a batch counts. It must
	// not be negative, and is at most 2,147,483,647. Zero means no limit.
	MaxRunning int
}

// QueueInfo describes a queue: its settings, its workers, and its latest
// statistics, which the maintainer refreshes every 10 seconds. Read it with
// [Client.Queues].
//
// The [monitoring page] says which of these numbers to alert on, and at
// which values.
//
// [monitoring page]: https://github.com/code19m/taskmill/blob/main/docs/production/monitoring.md
type QueueInfo struct {
	Name     string
	Settings QueueSettings

	// Paused reports that the queue is paused: workers fetch none of its
	// tasks until [Client.ResumeQueue].
	Paused bool

	// Workers counts the workers registered to read the queue. A worker
	// that stopped without unregistering, such as after a crash, counts
	// until the maintainer removes it: 5 minutes after its last check-in,
	// or twice its LeaseDuration after it if that is longer.
	Workers int

	// Counts holds the number of tasks per state, except completed tasks.
	// Counting stops at 10,000 tasks per state, and at 100,000 scheduled
	// and retryable tasks across all queues; a count's Capped reports that
	// it stopped with more tasks left, so it is a lower bound. Once the
	// shared cap is passed, every queue's scheduled and retryable counts are
	// capped, even at zero.
	Counts map[State]TaskCount

	// RefreshedAt is when the maintainer last counted the queue: zero until
	// it first does. It stops moving when no maintainer runs.
	RefreshedAt time.Time

	// Lag is how long the queue's oldest due task has waited since its run
	// time, by the database's clock at the call: an available task, or a
	// scheduled or retryable one whose run time has passed. It grows when
	// workers fall behind or stop, the queue is paused, or due tasks are not
	// promoted, and keeps growing when no maintainer runs. Tasks pending
	// behind their FIFO group wait by design and do not count. Zero when no
	// task was due at the last count. The maintainer finds the oldest task
	// when it counts, so for up to one refresh, about 10 seconds, Lag may
	// be too high: a task that finished since still counts. Work can be
	// stuck while Lag stays near zero: HaltedGroups and MostFailures show it.
	// Lag stops at the longest [time.Duration], about 292 years.
	Lag time.Duration

	// HaltedGroups counts the queue's FIFO groups that a dead task halts.
	// Their later tasks wait as pending until an operator retries or
	// deletes the dead task, so they do not count in Lag. Counting stops at
	// 10,000.
	HaltedGroups int

	// MostFailures is the most times in a row that one task of the queue,
	// waiting to retry, has failed. It grows while a task keeps failing,
	// such as while a service that its handler calls is down, even though
	// Lag stays near zero: the task waits for its next attempt, not for a
	// worker. Zero when no task waits to retry. A task leaves it when it
	// completes, dies, or is canceled, and for as long as it runs its next
	// attempt.
	//
	// The maintainer reads the failures of the 10,000 retryable tasks in
	// all that retry last, and of each queue's task that retries last. A
	// task's wait before its retry grows with its failures, so the tasks
	// that failed most retry last. Once the waits reach their maximum, such
	// as 1 hour by default, they no longer tell the tasks apart: with more
	// than 10,000 tasks at that wait, MostFailures may be lower than the
	// most failures of any one task.
	MostFailures int
}

// UpdateQueue changes a queue's installation-wide settings for every worker,
// and creates the queue if needed. update gets a copy of the current
// settings and changes the ones it means to, so that settings it does not
// know of, such as those of a later version, keep their values.
//
// No lock is held while update runs. The settings are written back only if
// they did not change meanwhile; if they did, update runs again on the new
// settings, after a random pause of at most 50 milliseconds, up to 10 times
// before UpdateQueue gives up with an error that wraps [ErrConflict]. So
// concurrent updates never lose each other's changes, and update may run
// more than once: it must only change the settings it gets, with no I/O and
// no calls to the Client. Workers apply the change within 10 seconds, and
// right away if they receive notifications. A MaxRunning outside its limits
// is an error that wraps [ErrInvalidArgument].
//
//	err := client.UpdateQueue(ctx, "partner-api", func(s *taskmill.QueueSettings) { s.MaxRunning = 4 })
func (c *Client) UpdateQueue(ctx context.Context, name string, update func(*QueueSettings)) error {
	if err := checkName("queue", name, maxNameLength); err != nil {
		return err
	}
	if update == nil {
		return invalidArgument("update is nil")
	}
	op := fmt.Sprintf("update queue %q", name)
	fail := func(err error) error { return fmt.Errorf("taskmill: %s: %w", op, err) }
	return retryConflicts(ctx, op, func() (bool, error) {
		var limit *int
		if err := c.pool.QueryRow(ctx, c.sql.readQueue, name).Scan(&limit); err != nil {
			return false, fail(err)
		}
		var s QueueSettings
		if limit != nil {
			s.MaxRunning = *limit
		}
		update(&s)
		var next *int
		switch {
		case s.MaxRunning < 0:
			return false, invalidArgument("MaxRunning must not be negative")
		case s.MaxRunning > math.MaxInt32: // the column is an integer
			return false, invalidArgument("MaxRunning %d is more than the limit of %d", s.MaxRunning, math.MaxInt32)
		case s.MaxRunning > 0:
			next = &s.MaxRunning
		}
		var swapped int
		err := c.pool.QueryRow(ctx, c.sql.swapQueue,
			name,                 // $1
			limit,                // $2
			next,                 // $3
			c.schema,             // $4
			notifyQueue+"|"+name, // $5
		).Scan(&swapped)
		if err != nil {
			return false, fail(err)
		}
		return swapped == 1, nil
	})
}

// PauseQueue stops workers from fetching the queue's tasks. Tasks already
// running finish normally. Workers apply the pause within 10 seconds, and
// right away if they receive notifications. It never returns [ErrNotFound]:
// it creates the queue's settings if the queue has none.
func (c *Client) PauseQueue(ctx context.Context, name string) error {
	return c.setQueuePaused(ctx, name, true)
}

// ResumeQueue undoes [Client.PauseQueue]. Workers apply it as fast as a
// pause, and like PauseQueue it never returns [ErrNotFound].
func (c *Client) ResumeQueue(ctx context.Context, name string) error {
	return c.setQueuePaused(ctx, name, false)
}

// setQueuePaused pauses or resumes a queue, and tells the workers.
func (c *Client) setQueuePaused(ctx context.Context, name string, paused bool) error {
	if err := checkName("queue", name, maxNameLength); err != nil {
		return err
	}
	err := pgx.BeginFunc(ctx, c.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, c.sql.pauseQueue, name, paused); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, notifySQL, c.schema, notifyQueue+"|"+name)
		return err
	})
	if err != nil {
		return fmt.Errorf("taskmill: change queue %q: %w", name, err)
	}
	return nil
}

// Queues returns every known queue, ordered by name: those with settings,
// those that workers read, and those that hold tasks, as of the latest
// statistics.
func (c *Client) Queues(ctx context.Context) ([]QueueInfo, error) {
	fail := func(err error) error { return fmt.Errorf("taskmill: list queues: %w", err) }
	// The queues and their statistics, in one round trip.
	b := &pgx.Batch{}
	b.Queue(c.sql.listQueues)
	b.Queue(c.sql.queueStats)
	br := c.pool.SendBatch(ctx, b)
	defer func() { _ = br.Close() }()
	r, err := br.Query()
	if err != nil {
		return nil, fail(err)
	}
	var out []QueueInfo
	index := make(map[string]int)
	for r.Next() {
		var q QueueInfo
		if err := r.Scan(&q.Name, &q.Settings.MaxRunning, &q.Paused, &q.Workers); err != nil {
			r.Close()
			return nil, fail(err)
		}
		q.Counts = map[State]TaskCount{}
		index[q.Name] = len(out)
		out = append(out, q)
	}
	r.Close()
	if err := r.Err(); err != nil {
		return nil, fail(err)
	}

	r, err = br.Query()
	if err != nil {
		return nil, fail(err)
	}
	defer r.Close()
	for r.Next() {
		var name, state string
		var n int64
		var capped bool
		var at time.Time
		var lag int64
		var halted, failures int // each stored with one state only, and 0 with the others
		if err := r.Scan(&name, &state, &n, &capped, &at, &lag, &halted, &failures); err != nil {
			return nil, fail(err)
		}
		if i, ok := index[name]; ok {
			q := &out[i]
			q.Counts[State(state)] = TaskCount{N: n, Capped: capped}
			q.RefreshedAt = at
			q.Lag = max(q.Lag, fromMicros(lag))
			q.HaltedGroups = max(q.HaltedGroups, halted)
			q.MostFailures = max(q.MostFailures, failures)
		}
	}
	if err := r.Err(); err != nil {
		return nil, fail(err)
	}
	return out, nil
}

// settingsRetry is the first wait before the worker reads the settings of
// its queues again while it knows none.
const settingsRetry = 100 * time.Millisecond

// settingsLoop loads the installation-wide settings of the worker's queues
// when the worker starts, and again every settings interval. Until a load
// succeeds, the queues fetch nothing (see Run), so it tries again sooner:
// after settingsRetry, then waiting twice as long each time, up to the
// interval.
func (w *Worker) settingsLoop(ctx context.Context) {
	wait := settingsRetry
	for {
		w.refreshQueueSettings(ctx)
		if w.settled.Load() {
			wait = w.timing.settings
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}
		wait = min(2*wait, w.timing.settings)
	}
}

// refreshQueueSettings reads the settings of the worker's queues and applies
// them. If it cannot, the queues keep what they had. The read may take as
// long as the settings interval, after which the next one is due.
func (w *Worker) refreshQueueSettings(ctx context.Context) {
	if len(w.queues) == 0 {
		return
	}
	names := make([]string, 0, len(w.queues))
	for name := range w.queues {
		names = append(names, name)
	}
	readCtx, cancel := context.WithTimeout(ctx, w.timing.settings)
	settings, err := w.loadQueueSettings(readCtx, names)
	cancel()
	w.settingsFailures.observe(ctx, err)
	if err != nil {
		return
	}
	// Settled comes first: no queue fetches before the worker says that it
	// has read the settings.
	w.settled.Store(true)
	// A queue without a row has the default settings: running, no limit.
	for name, q := range w.queues {
		s := settings[name]
		q.maxRunning.Store(s.maxRunning)
		if q.paused.Swap(s.paused) && !s.paused {
			q.nudge()
		}
	}
}

// queueSetting is the part of a queue's settings that its worker applies.
type queueSetting struct {
	paused     bool
	maxRunning int32
}

func (w *Worker) loadQueueSettings(ctx context.Context, names []string) (map[string]queueSetting, error) {
	r, err := w.client.pool.Query(ctx, w.client.sql.queueSettings, names)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	settings := make(map[string]queueSetting, len(names))
	for r.Next() {
		var name string
		var s queueSetting
		if err := r.Scan(&name, &s.paused, &s.maxRunning); err != nil {
			return nil, err
		}
		settings[name] = s
	}
	return settings, r.Err()
}
