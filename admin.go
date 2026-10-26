package taskmill

import (
	"cmp"
	"context"
	"encoding/base64"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
)

// Limits for listing tasks and refreshing statistics.
const (
	defaultListLimit = 100
	maxListLimit     = 1000
	listTimeout      = "5s"    // bound on one page's query, so inspection never loads the database
	statsQueueCap    = 10_000  // most tasks counted per queue and state
	statsDueCap      = 100_000 // most scheduled and retryable tasks counted in all

	defaultHistoryRetention = 7 * 24 * time.Hour
	defaultDeadRetention    = 30 * 24 * time.Hour
	minRetention            = time.Hour
)

// QueueSettings are a queue's installation-wide settings, stored in the
// database. The zero value means no concurrency limit.
type QueueSettings struct {
	// MaxRunning caps the tasks of the queue running at once across all
	// workers of the installation. Zero means no limit.
	MaxRunning int
}

// QueueInfo describes a queue: its settings, its workers, and its latest
// statistics, which the maintainer refreshes every 10 seconds.
type QueueInfo struct {
	Name     string
	Settings QueueSettings
	Paused   bool
	Workers  int // live workers that serve the queue

	// Counts holds the number of tasks per state, except completed tasks.
	// Counting stops at 10,000 tasks per state, and at 100,000 scheduled
	// and retryable tasks across all queues; Capped reports that a count
	// stopped early and is a lower bound.
	Counts map[State]int64
	Capped bool

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
	// read high: a task that finished since still counts.
	Lag time.Duration
}

// Settings are the installation-wide settings, stored in the database and
// applied by the maintainer. Zero fields mean their defaults.
type Settings struct {
	// HistoryRetention is how long completed tasks stay in history.
	// Minimum: 1 hour. Default: 7 days.
	HistoryRetention time.Duration

	// DeadRetention is how long dead and canceled tasks are kept for
	// operators to inspect and retry. Minimum: 1 hour. Default: 30 days.
	DeadRetention time.Duration
}

// WorkerInfo describes a worker process.
type WorkerInfo struct {
	ID          string
	Hostname    string
	Version     string // taskmill's version in the worker's binary
	Queues      []string
	Kinds       []string // the kinds it has handlers for
	StartedAt   time.Time
	HeartbeatAt time.Time
	Maintainer  bool // the worker is the elected maintainer
}

// ListParams filters and pages [Client.List].
type ListParams struct {
	// Filter selects the tasks. Its zero value selects every task.
	Filter TaskFilter

	// Limit is the page size. Default: 100. Maximum: 1,000.
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

// UpdateQueue sets a queue's installation-wide settings for every worker, and
// creates the queue if needed. Passing nil, or a zero MaxRunning, removes the
// limit. Workers apply the change within 10 seconds, and at once if they
// receive notifications.
func (c *Client) UpdateQueue(ctx context.Context, name string, settings *QueueSettings) error {
	if settings == nil {
		settings = &QueueSettings{}
	}
	if settings.MaxRunning < 0 {
		return invalidArgument("MaxRunning must not be negative")
	}
	var maxRunning *int
	if settings.MaxRunning > 0 {
		maxRunning = &settings.MaxRunning
	}
	return c.changeQueue(ctx, name, c.sql.updateQueue, maxRunning)
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
		_, err := tx.Exec(ctx, notifySQL, c.schema, notifyQueue+"|"+name)
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
		if err := r.Scan(&q.Name, &q.Settings.MaxRunning, &q.Paused, &q.Workers); err != nil {
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
		var lag int64
		if err := r.Scan(&name, &state, &n, &capped, &at, &lag); err != nil {
			return nil, fmt.Errorf("taskmill: list queues: %w", err)
		}
		if i, ok := index[name]; ok {
			q := &out[i]
			q.Counts[State(state)] = n
			q.Capped = q.Capped || capped
			q.RefreshedAt = at
			q.Lag = max(q.Lag, time.Duration(lag)*time.Microsecond)
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
	history := cmp.Or(settings.HistoryRetention, defaultHistoryRetention)
	dead := cmp.Or(settings.DeadRetention, defaultDeadRetention)
	if history < minRetention || dead < minRetention {
		return invalidArgument("retention periods must be at least %s", minRetention)
	}
	if _, err := c.pool.Exec(ctx, c.sql.updateSettings, history.Microseconds(), dead.Microseconds()); err != nil {
		return fmt.Errorf("taskmill: update settings: %w", err)
	}
	return nil
}

// Workers returns the registered worker processes. A worker that stopped
// without unregistering disappears after 5 minutes without heartbeats.
func (c *Client) Workers(ctx context.Context) ([]WorkerInfo, error) {
	r, err := c.pool.Query(ctx, c.sql.listWorkers)
	if err != nil {
		return nil, fmt.Errorf("taskmill: list workers: %w", err)
	}
	defer r.Close()
	var out []WorkerInfo
	for r.Next() {
		var w WorkerInfo
		if err := r.Scan(&w.ID, &w.Hostname, &w.Version, &w.Queues, &w.Kinds, &w.StartedAt, &w.HeartbeatAt, &w.Maintainer); err != nil {
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
// Each page's query is canceled after 5 seconds, so listing cannot overload
// the database; on a very large table, a filter that matches few tasks can
// reach that limit and return an error.
func (c *Client) List(ctx context.Context, params *ListParams) (*ListResult, error) {
	if params == nil {
		params = &ListParams{}
	}
	limit, before, err := listBounds(params)
	if err != nil {
		return nil, err
	}
	m, err := match(&params.Filter, allStates)
	if err != nil {
		return nil, err
	}

	var out ListResult
	err = pgx.BeginFunc(ctx, c.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, "SET LOCAL statement_timeout = '"+listTimeout+"'"); err != nil {
			return err
		}
		// Custom plans see which filters are empty, and so use the indexes
		// that serve the filters that are set.
		if _, err := tx.Exec(ctx, "SET LOCAL plan_cache_mode = force_custom_plan"); err != nil {
			return err
		}
		r, err := tx.Query(ctx, c.sql.listTasks,
			m.queues, m.states, m.kinds, before, limit+1, len(m.states) > 0, m.history, m.groups, m.after)
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

// nonEmpty checks that a filter's values are not empty, and returns them as a
// slice that is never nil: NULL would match nothing.
func nonEmpty(what string, values []string) ([]string, error) {
	out := make([]string, 0, len(values))
	for _, v := range values {
		if v == "" {
			return nil, invalidArgument("%s must not be empty", what)
		}
		out = append(out, v)
	}
	return out, nil
}

// GroupParams selects the FIFO groups that [Client.Groups] returns.
type GroupParams struct {
	// Queues limits the result to groups of these queues. Empty means all
	// queues.
	Queues []string

	// Search limits the result to groups whose keys contain it, ignoring
	// case. Empty means all groups.
	Search string

	// Limit is the most groups returned. Default: 100. Maximum: 1,000.
	Limit int
}

// GroupInfo is a FIFO group that has unfinished tasks.
type GroupInfo struct {
	Queue string
	Key   string
	Tasks int // unfinished tasks in the group

	// HaltedBy is the dead task that halts the group, or 0 if it runs.
	HaltedBy int64
}

// Groups returns the FIFO groups that have unfinished tasks, ordered by
// queue and key. Like [Client.List], the query is canceled after 5 seconds.
func (c *Client) Groups(ctx context.Context, params *GroupParams) ([]GroupInfo, error) {
	p := GroupParams{}
	if params != nil {
		p = *params
	}
	switch {
	case p.Limit == 0:
		p.Limit = defaultListLimit
	case p.Limit < 0 || p.Limit > maxListLimit:
		return nil, invalidArgument("limit must be between 1 and %d", maxListLimit)
	}
	queues, err := nonEmpty("queue name", p.Queues)
	if err != nil {
		return nil, err
	}
	var out []GroupInfo
	err = pgx.BeginFunc(ctx, c.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, "SET LOCAL statement_timeout = '"+listTimeout+"'"); err != nil {
			return err
		}
		r, err := tx.Query(ctx, c.sql.listGroups, queues, p.Search, p.Limit)
		if err != nil {
			return err
		}
		out, err = pgx.CollectRows(r, func(r pgx.CollectableRow) (GroupInfo, error) {
			var g GroupInfo
			err := r.Scan(&g.Queue, &g.Key, &g.Tasks, &g.HaltedBy)
			return g, err
		})
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("taskmill: list groups: %w", err)
	}
	return out, nil
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
		return limit, math.MaxInt64, nil
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
