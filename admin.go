package taskmill

import (
	"context"
	"encoding/base64"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
)

// Limits for listing tasks and refreshing statistics.
const (
	defaultListLimit = 100
	maxListLimit     = 1000
	listTimeout      = "5s" // bound on one page's query, so inspection never loads the database
	statsCap         = 100_000
	statsInterval    = 10 * time.Second

	defaultHistoryRetention = 7 * 24 * time.Hour
	defaultDeadRetention    = 30 * 24 * time.Hour
)

// QueueSettings are a queue's cluster-wide settings, stored in the database.
// The zero value means: no concurrency limit, keep completed tasks in history,
// and let FIFO groups continue past dead tasks.
type QueueSettings struct {
	// MaxRunning caps the tasks of the queue running at once across all
	// workers. Zero means no limit.
	MaxRunning int

	// DiscardCompleted deletes completed tasks at once instead of keeping
	// them in history for the retention period.
	DiscardCompleted bool

	// HaltGroupOnDead stops a FIFO group when one of its tasks becomes dead,
	// until an operator retries or deletes that task.
	HaltGroupOnDead bool
}

// QueueInfo describes a queue: its settings, its workers, and its latest
// statistics, which the maintainer refreshes every few seconds.
type QueueInfo struct {
	Name     string
	Settings QueueSettings
	Paused   bool
	Workers  int // live workers that serve the queue

	// Counts holds the number of tasks per state, except completed tasks.
	// A count that reached the statistics cap of 100,000 per state is
	// reported as counted so far, and Capped is set.
	Counts      map[State]int64
	Capped      bool
	RefreshedAt time.Time // zero until the maintainer first counts the queue
}

// Settings are the installation-wide settings, stored in the database and
// applied by the maintainer. Zero fields mean their defaults.
type Settings struct {
	// HistoryRetention is how long completed tasks stay in history.
	// Default: 7 days.
	HistoryRetention time.Duration

	// DeadRetention is how long dead and canceled tasks are kept for
	// operators to inspect and retry. Default: 30 days.
	DeadRetention time.Duration
}

// WorkerInfo describes a worker process.
type WorkerInfo struct {
	ID          string
	Hostname    string
	Version     string
	Queues      []string
	StartedAt   time.Time
	HeartbeatAt time.Time
}

// ListParams filters and pages [Client.List].
type ListParams struct {
	// Queue limits the result to one queue. Empty means all queues.
	Queue string

	// States limits the result to tasks in these states. Empty means all.
	States []State

	// Kind limits the result to one kind. Empty means all kinds.
	Kind string

	// Limit is the page size. Default: 100. Maximum: 1000.
	Limit int

	// Cursor continues from a previous page's [ListResult.Next].
	Cursor string
}

// ListResult is one page of tasks, newest first.
type ListResult struct {
	Tasks []TaskInfo

	// Next is the cursor for the next page, or empty on the last page.
	Next string
}

// UpdateQueue changes a queue's settings for every worker in the cluster,
// creating the queue if it does not exist. Workers apply the change within
// seconds.
func (c *Client) UpdateQueue(ctx context.Context, name string, settings *QueueSettings) error {
	if err := checkName("queue", name, maxNameLength); err != nil {
		return err
	}
	if settings == nil {
		settings = &QueueSettings{}
	}
	if settings.MaxRunning < 0 {
		return invalidArgument("max running must not be negative")
	}
	var maxRunning *int
	if settings.MaxRunning > 0 {
		maxRunning = &settings.MaxRunning
	}
	return c.changeQueue(ctx, name, c.sql.updateQueue, maxRunning, !settings.DiscardCompleted, settings.HaltGroupOnDead)
}

// PauseQueue stops workers from fetching the queue's tasks. Tasks already
// running finish normally.
func (c *Client) PauseQueue(ctx context.Context, name string) error {
	return c.changeQueue(ctx, name, c.sql.pauseQueue, true)
}

// ResumeQueue undoes [Client.PauseQueue].
func (c *Client) ResumeQueue(ctx context.Context, name string) error {
	return c.changeQueue(ctx, name, c.sql.pauseQueue, false)
}

// changeQueue runs a statement that changes a queue's row, and tells the
// workers.
func (c *Client) changeQueue(ctx context.Context, name, sql string, args ...any) error {
	if err := checkName("queue", name, maxNameLength); err != nil {
		return err
	}
	err := pgx.BeginFunc(ctx, c.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, sql, append([]any{name}, args...)...); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, "SELECT pg_notify($1, $2)", c.schema, "q|"+name)
		return err
	})
	if err != nil {
		return fmt.Errorf("taskmill: change queue %q: %w", name, err)
	}
	return nil
}

// Queues returns every known queue: those with settings, those that workers
// serve, and those that hold tasks, as of the latest statistics.
func (c *Client) Queues(ctx context.Context) ([]QueueInfo, error) {
	r, err := c.pool.Query(ctx, c.sql.listQueues)
	if err != nil {
		return nil, fmt.Errorf("taskmill: list queues: %w", err)
	}
	var out []QueueInfo
	index := make(map[string]int)
	for r.Next() {
		var q QueueInfo
		if err := r.Scan(&q.Name, &q.Settings.MaxRunning, &q.Settings.DiscardCompleted,
			&q.Settings.HaltGroupOnDead, &q.Paused, &q.Workers); err != nil {
			r.Close()
			return nil, fmt.Errorf("taskmill: list queues: %w", err)
		}
		q.Counts = map[State]int64{}
		index[q.Name] = len(out)
		out = append(out, q)
	}
	r.Close()
	if err := r.Err(); err != nil {
		return nil, fmt.Errorf("taskmill: list queues: %w", err)
	}

	r, err = c.pool.Query(ctx, c.sql.queueStats)
	if err != nil {
		return nil, fmt.Errorf("taskmill: list queues: %w", err)
	}
	defer r.Close()
	for r.Next() {
		var name, state string
		var n int64
		var capped bool
		var at time.Time
		if err := r.Scan(&name, &state, &n, &capped, &at); err != nil {
			return nil, fmt.Errorf("taskmill: list queues: %w", err)
		}
		if i, ok := index[name]; ok {
			q := &out[i]
			q.Counts[State(state)] = n
			q.Capped = q.Capped || capped
			q.RefreshedAt = at
		}
	}
	if err := r.Err(); err != nil {
		return nil, fmt.Errorf("taskmill: list queues: %w", err)
	}
	return out, nil
}

// Settings returns the installation-wide settings.
func (c *Client) Settings(ctx context.Context) (*Settings, error) {
	var history, dead int64
	if err := c.pool.QueryRow(ctx, c.sql.retention).Scan(&history, &dead); err != nil {
		return nil, fmt.Errorf("taskmill: read settings: %w", err)
	}
	return &Settings{
		HistoryRetention: time.Duration(history) * time.Second,
		DeadRetention:    time.Duration(dead) * time.Second,
	}, nil
}

// UpdateSettings changes the installation-wide settings. Zero fields reset
// to their defaults.
func (c *Client) UpdateSettings(ctx context.Context, settings *Settings) error {
	if settings == nil {
		settings = &Settings{}
	}
	history := cmpDuration(settings.HistoryRetention, defaultHistoryRetention)
	dead := cmpDuration(settings.DeadRetention, defaultDeadRetention)
	if history < time.Hour || dead < time.Hour {
		return invalidArgument("retention periods must be at least an hour")
	}
	if _, err := c.pool.Exec(ctx, c.sql.updateSettings, history.Microseconds(), dead.Microseconds()); err != nil {
		return fmt.Errorf("taskmill: update settings: %w", err)
	}
	return nil
}

func cmpDuration(d, fallback time.Duration) time.Duration {
	if d == 0 {
		return fallback
	}
	return d
}

// Workers returns the registered worker processes. A worker that stopped
// without unregistering disappears after a few minutes without heartbeats.
func (c *Client) Workers(ctx context.Context) ([]WorkerInfo, error) {
	r, err := c.pool.Query(ctx, c.sql.listWorkers)
	if err != nil {
		return nil, fmt.Errorf("taskmill: list workers: %w", err)
	}
	defer r.Close()
	var out []WorkerInfo
	for r.Next() {
		var w WorkerInfo
		if err := r.Scan(&w.ID, &w.Hostname, &w.Version, &w.Queues, &w.StartedAt, &w.HeartbeatAt); err != nil {
			return nil, fmt.Errorf("taskmill: list workers: %w", err)
		}
		out = append(out, w)
	}
	if err := r.Err(); err != nil {
		return nil, fmt.Errorf("taskmill: list workers: %w", err)
	}
	return out, nil
}

// List returns tasks that match params, newest first, one page at a time.
// Each page's query is bounded in time, so listing never loads the database.
func (c *Client) List(ctx context.Context, params *ListParams) (*ListResult, error) {
	if params == nil {
		params = &ListParams{}
	}
	limit, before, err := listBounds(params)
	if err != nil {
		return nil, err
	}

	states := make([]string, 0, len(params.States))
	history := len(params.States) == 0
	for _, s := range params.States {
		if s == StateCompleted {
			history = true
			continue
		}
		states = append(states, string(s))
	}
	live := len(params.States) == 0 || len(states) > 0

	var out ListResult
	err = pgx.BeginFunc(ctx, c.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, "SET LOCAL statement_timeout = '"+listTimeout+"'"); err != nil {
			return err
		}
		r, err := tx.Query(ctx, c.sql.listTasks, params.Queue, states, params.Kind, before, limit+1, live, history)
		if err != nil {
			return err
		}
		defer r.Close()
		for r.Next() {
			info, err := scanTaskInfo(r)
			if err != nil {
				return err
			}
			out.Tasks = append(out.Tasks, *info)
		}
		return r.Err()
	})
	if err != nil {
		return nil, fmt.Errorf("taskmill: list tasks: %w", err)
	}
	if len(out.Tasks) > limit {
		out.Tasks = out.Tasks[:limit]
		last := out.Tasks[limit-1].ID
		out.Next = base64.RawURLEncoding.EncodeToString([]byte(strconv.FormatInt(last, 10)))
	}
	return &out, nil
}

// listBounds validates a page size and decodes a cursor into the ID that
// the page starts below.
func listBounds(p *ListParams) (limit int, before int64, err error) {
	limit = p.Limit
	switch {
	case limit == 0:
		limit = defaultListLimit
	case limit < 0 || limit > maxListLimit:
		return 0, 0, invalidArgument("limit must be between 1 and %d", maxListLimit)
	}
	if p.Cursor == "" {
		return limit, 1<<63 - 1, nil
	}
	raw, err := base64.RawURLEncoding.DecodeString(p.Cursor)
	if err != nil {
		return 0, 0, invalidArgument("invalid cursor")
	}
	if before, err = strconv.ParseInt(string(raw), 10, 64); err != nil {
		return 0, 0, invalidArgument("invalid cursor")
	}
	return limit, before, nil
}

// refreshStats counts each queue's tasks per state, up to a cap per state,
// and stores the counts for [Client.Queues].
func (m *maintainer) refreshStats(ctx context.Context) {
	type row struct {
		queue, state string
		n            int64
		capped       bool
	}
	var rows []row
	for _, s := range []State{StateScheduled, StatePending, StateAvailable, StateRunning, StateRetryable, StateDead, StateCanceled} {
		r, err := m.c.pool.Query(ctx, m.c.sql.countState, string(s), statsCap)
		if err != nil {
			m.log(ctx, slog.LevelError, "refresh statistics", err)
			return
		}
		var total int64
		var counted []row
		for r.Next() {
			x := row{state: string(s)}
			if err := r.Scan(&x.queue, &x.n); err != nil {
				r.Close()
				m.log(ctx, slog.LevelError, "refresh statistics", err)
				return
			}
			total += x.n
			counted = append(counted, x)
		}
		r.Close()
		if err := r.Err(); err != nil {
			m.log(ctx, slog.LevelError, "refresh statistics", err)
			return
		}
		for i := range counted {
			counted[i].capped = total >= statsCap
		}
		rows = append(rows, counted...)
	}

	queues := make([]string, len(rows))
	states := make([]string, len(rows))
	counts := make([]int64, len(rows))
	capped := make([]bool, len(rows))
	for i, x := range rows {
		queues[i], states[i], counts[i], capped[i] = x.queue, x.state, x.n, x.capped
	}
	err := pgx.BeginFunc(ctx, m.c.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, m.c.sql.clearStats); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, m.c.sql.storeStats, queues, states, counts, capped)
		return err
	})
	if err != nil {
		m.log(ctx, slog.LevelError, "store statistics", err)
	}
}
