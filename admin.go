package taskmill

import (
	"cmp"
	"context"
	"encoding/base64"
	"fmt"
	"math"
	"math/rand/v2"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// Limits for listing tasks and refreshing statistics.
const (
	defaultListLimit = 100
	maxListLimit     = 1000
	maxGroupQueues   = 100     // most queues named in one Groups call, each read up to a page
	listTimeout      = "5s"    // bound on one page's query, so inspection never loads the database
	statsQueueCap    = 10_000  // most tasks counted per queue and state, and halted groups per queue
	statsDueCap      = 100_000 // most scheduled and retryable tasks counted in all
	statsRetryCap    = 10_000  // most retryable tasks whose attempts are read in all, besides each queue's last

	defaultHistoryRetention = 7 * 24 * time.Hour
	defaultDeadRetention    = 30 * 24 * time.Hour
	minRetention            = time.Hour
)

// QueueSettings are a queue's installation-wide settings, stored in the
// database. Change them with [Client.UpdateQueue]. The zero value means no
// limit.
type QueueSettings struct {
	// MaxRunning caps the tasks of the queue running at the same time across
	// all workers of the installation. Each task of a batch counts. It must
	// not be negative. Zero means no limit.
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

// Settings are the installation-wide settings, stored in the database and
// applied by the maintainer. Read them with [Client.Settings], and change
// them with [Client.UpdateSettings]. Zero fields mean their defaults. A
// period shorter than its minimum is an error that wraps
// [ErrInvalidArgument]. A period is stored in whole microseconds, and the
// longest [time.Duration], about 292 years, keeps tasks for good.
type Settings struct {
	// HistoryRetention is how long completed tasks stay in history, at
	// least: history is dropped a whole UTC day at a time, so a task stays
	// up to a day longer. Minimum: 1 hour. Default: 7 days.
	HistoryRetention time.Duration

	// DeadRetention is how long dead and canceled tasks are kept for
	// operators to inspect and retry. A dead task that halts its FIFO group
	// is kept until it is retried or deleted. Minimum: 1 hour. Default: 30
	// days.
	DeadRetention time.Duration
}

// WorkerInfo describes a worker process.
type WorkerInfo struct {
	// ID names the worker: its host name, its process ID, and a random
	// suffix, joined by dashes.
	ID       string
	Hostname string
	Version  string // taskmill's version in the worker's binary
	Queues   []string
	Kinds    []string // the kinds it has handlers for

	// StartedAt is when the worker last registered: when its Run started,
	// or when it registered again after the maintainer removed it as stale.
	StartedAt   time.Time
	CheckedInAt time.Time // its last check-in, which it makes every third of its LeaseDuration
	Maintainer  bool      // the worker is the elected maintainer

	// Stale reports that the worker made no check-in for its whole
	// [WorkerConfig.LeaseDuration], by the database's clock. It stopped
	// without unregistering, or it cannot reach the database: the leases of
	// its tasks have run out, and its [Worker.Health] fails. A worker of a
	// release that does not store its LeaseDuration is stale after 30
	// seconds without a check-in.
	Stale bool
}

// ListParams filters and pages [Client.List]. A field outside its limits is
// an error that wraps [ErrInvalidArgument].
type ListParams struct {
	// Filter selects the tasks. Empty means every task.
	Filter TaskFilter

	// Limit is the page size. It must not be negative. Default: 100.
	// Maximum: 1,000.
	Limit int

	// Cursor continues from a previous page's [ListResult.Next]. Empty means
	// the first page.
	Cursor string

	// Summary leaves out what makes a task large, for a list that shows one
	// line per task: each [TaskInfo] comes with a nil Payload, a nil
	// Metadata, and only the newest entry of its Errors. The database does
	// not read or send what is left out, so a page costs about the same
	// whatever its tasks hold. [TaskInfo.ScheduledFor] reads the metadata,
	// so it returns the zero time for such a task. Default: false, which
	// returns every field, as [Client.Get] does.
	Summary bool
}

// ListResult is one page of tasks, newest first.
type ListResult struct {
	Tasks []TaskInfo

	// Next is the cursor for the next page, or empty on the last page.
	Next string
}

// An update of settings that loses to a concurrent update tries again, up
// to updateAttempts times. Before each new try it pauses a random time below
// a cap that doubles from minUpdatePause to maxUpdatePause, so that updates
// that collided spread out; the pauses add up to at most 0.26 seconds.
const (
	updateAttempts = 10
	minUpdatePause = 2 * time.Millisecond
	maxUpdatePause = 50 * time.Millisecond
)

// retryConflicts runs try until it saves the settings or fails, and returns
// an error that wraps [ErrConflict] if every try lost to a concurrent update.
// op names the update in errors.
func retryConflicts(ctx context.Context, op string, try func() (saved bool, err error)) error {
	pause := minUpdatePause
	for i := range updateAttempts {
		if i > 0 {
			wait := time.NewTimer(rand.N(pause)) //nolint:gosec // jitter needs no cryptographic randomness
			select {
			case <-ctx.Done():
				wait.Stop()
				return fmt.Errorf("taskmill: %s: %w", op, ctx.Err())
			case <-wait.C:
			}
			pause = min(2*pause, maxUpdatePause)
		}
		if saved, err := try(); saved || err != nil {
			return err
		}
	}
	return fmt.Errorf("%w: %s: the settings changed during each of %d tries", ErrConflict, op, updateAttempts)
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
// right away if they receive notifications. A negative MaxRunning is an
// error that wraps [ErrInvalidArgument].
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
	return retryConflicts(ctx, op, func() (bool, error) {
		var limit *int
		if err := c.pool.QueryRow(ctx, c.sql.readQueue, name).Scan(&limit); err != nil {
			return false, fmt.Errorf("taskmill: %s: %w", op, err)
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
		case s.MaxRunning > 0:
			next = &s.MaxRunning
		}
		var swapped int
		if err := c.pool.QueryRow(ctx, c.sql.swapQueue, name, limit, next, c.schema, notifyQueue+"|"+name).Scan(&swapped); err != nil {
			return false, fmt.Errorf("taskmill: %s: %w", op, err)
		}
		return swapped == 1, nil
	})
}

// PauseQueue stops workers from fetching the queue's tasks. Tasks already
// running finish normally. Workers apply the pause within 10 seconds, and
// right away if they receive notifications. It never returns [ErrNotFound]:
// it creates the queue's settings if the queue has none.
func (c *Client) PauseQueue(ctx context.Context, name string) error {
	return c.changeQueue(ctx, name, c.sql.pauseQueue, true)
}

// ResumeQueue undoes [Client.PauseQueue]. Workers apply it as fast as a
// pause, and like PauseQueue it never returns [ErrNotFound].
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

// Queues returns every known queue, ordered by name: those with settings,
// those that workers read, and those that hold tasks, as of the latest
// statistics.
func (c *Client) Queues(ctx context.Context) ([]QueueInfo, error) {
	// The queues and their statistics, in one round trip.
	b := &pgx.Batch{}
	b.Queue(c.sql.listQueues)
	b.Queue(c.sql.queueStats)
	br := c.pool.SendBatch(ctx, b)
	defer func() { _ = br.Close() }()
	r, err := br.Query()
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
		q.Counts = map[State]TaskCount{}
		index[q.Name] = len(out)
		out = append(out, q)
	}
	r.Close()
	if err := r.Err(); err != nil {
		return nil, fmt.Errorf("taskmill: list queues: %w", err)
	}

	r, err = br.Query()
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
		var halted, failures int // each stored with one state only, and 0 with the others
		if err := r.Scan(&name, &state, &n, &capped, &at, &lag, &halted, &failures); err != nil {
			return nil, fmt.Errorf("taskmill: list queues: %w", err)
		}
		if i, ok := index[name]; ok {
			q := &out[i]
			q.Counts[State(state)] = TaskCount{N: n, Capped: capped}
			q.RefreshedAt = at
			q.Lag = max(q.Lag, time.Duration(lag)*time.Microsecond)
			q.HaltedGroups = max(q.HaltedGroups, halted)
			q.MostFailures = max(q.MostFailures, failures)
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
	if err := c.pool.QueryRow(ctx, c.sql.readSettings).Scan(&history, &dead); err != nil {
		return nil, fmt.Errorf("taskmill: read settings: %w", err)
	}
	return &Settings{HistoryRetention: retention(history), DeadRetention: retention(dead)}, nil
}

// retention returns a retention period that the database stores as micros
// microseconds. A period beyond the longest duration, which only SQL can
// write, reads as the longest one.
func retention(micros int64) time.Duration {
	return time.Duration(min(micros, math.MaxInt64/int64(time.Microsecond))) * time.Microsecond
}

// UpdateSettings changes the installation-wide settings. update gets a copy
// of the current settings and changes the ones it means to, so that settings
// it does not know of, such as those of a later version, keep their values.
// A setting changed to zero goes back to its default. Like
// [Client.UpdateQueue], it holds no lock while update runs, and runs update
// again if the settings changed meanwhile, up to 10 times before it fails
// with [ErrConflict], so update must only change the settings it gets.
//
//	err := client.UpdateSettings(ctx, func(s *taskmill.Settings) { s.HistoryRetention = 48 * time.Hour })
func (c *Client) UpdateSettings(ctx context.Context, update func(*Settings)) error {
	if update == nil {
		return invalidArgument("update is nil")
	}
	return retryConflicts(ctx, "update settings", func() (bool, error) {
		var history, dead int64
		if err := c.pool.QueryRow(ctx, c.sql.readSettings).Scan(&history, &dead); err != nil {
			return false, fmt.Errorf("taskmill: update settings: %w", err)
		}
		s := Settings{HistoryRetention: retention(history), DeadRetention: retention(dead)}
		update(&s)
		s.HistoryRetention = cmp.Or(s.HistoryRetention, defaultHistoryRetention)
		s.DeadRetention = cmp.Or(s.DeadRetention, defaultDeadRetention)
		if s.HistoryRetention < minRetention || s.DeadRetention < minRetention {
			return false, invalidArgument("retention periods must be at least %s", minRetention)
		}
		tag, err := c.pool.Exec(ctx, c.sql.swapSettings, history, dead, s.HistoryRetention.Microseconds(), s.DeadRetention.Microseconds())
		if err != nil {
			return false, fmt.Errorf("taskmill: update settings: %w", err)
		}
		return tag.RowsAffected() == 1, nil
	})
}

// staleWithoutLease is how long a worker that stored no lease may make no
// check-in before it is stale: the workers of releases without the column.
const staleWithoutLease = 30 * time.Second

// Workers returns the registered worker processes, ordered by ID. A worker
// that stopped without unregistering is stale once it made no check-in for
// its LeaseDuration: see [WorkerInfo.Stale]. It disappears once it made
// none for 5 minutes, or for twice its LeaseDuration if that is longer: the
// maintainer removes it.
func (c *Client) Workers(ctx context.Context) ([]WorkerInfo, error) {
	r, err := c.pool.Query(ctx, c.sql.listWorkers, staleWithoutLease.Microseconds())
	if err != nil {
		return nil, fmt.Errorf("taskmill: list workers: %w", err)
	}
	defer r.Close()
	var out []WorkerInfo
	for r.Next() {
		var w WorkerInfo
		err := r.Scan(&w.ID, &w.Hostname, &w.Version, &w.Queues, &w.Kinds, &w.StartedAt, &w.CheckedInAt, &w.Maintainer, &w.Stale)
		if err != nil {
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
// params may be nil, for the first page of every task. A page holds every
// task in full, payloads included, unless [ListParams.Summary] is set.
//
// Each page's query is canceled after 5 seconds, so listing cannot load the
// database for longer: a list that needs more returns an error. A page
// reads about the tasks it returns where an index holds them newest first:
// with no filter, for the completed tasks of one queue or of all, and for
// the pending tasks of one queue's FIFO group. Other lists, such as the
// dead tasks of one queue or of all, are as fast when their tasks are among
// the newest, or when the database expects few of them. Otherwise they read
// the newer tasks of every queue and state first, which on a very large
// table can reach the limit.
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
	args := []any{m.queues, m.states, m.kinds, before, limit + 1, len(m.states) > 0, m.history, m.groups, params.Summary}
	err = c.readAlone(ctx, pageSettings, c.sql.listTasks, args, func(r pgx.Rows) error {
		info, err := scanTaskInfo(r)
		if err != nil {
			return err
		}
		out.Tasks = append(out.Tasks, *info)
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("taskmill: list tasks: %w", err)
	}
	if len(out.Tasks) > limit {
		out.Tasks = out.Tasks[:limit]
		out.Next = encodeCursor(out.Tasks[limit-1].ID)
	}
	return &out, nil
}

// pageSettings start the read of a page of tasks or of groups. They bound
// the read, so that inspection never loads the database for long. Custom
// plans see which filters are set, and so use the indexes that serve them:
// for a page of groups, the index of halted groups, if only those are asked
// for.
const pageSettings = `SELECT set_config('statement_timeout', '` + listTimeout + `', true), set_config('plan_cache_mode', 'force_custom_plan', true)`

// readAlone runs a read after settings, both in one round trip, and calls
// scan with each row of the result. The batch is one transaction without a
// BEGIN, so the settings end with it. And a read that fails, as one does
// that runs out of its time, leaves no open transaction on its connection:
// the pool would close such a connection, and open another for the next
// read.
func (c *Client) readAlone(ctx context.Context, settings, sql string, args []any, scan func(pgx.Rows) error) error {
	b := &pgx.Batch{}
	b.Queue(settings)
	b.Queue(sql, args...).Query(func(r pgx.Rows) error {
		for r.Next() {
			if err := scan(r); err != nil {
				return err
			}
		}
		return nil // Close returns an error of the rows.
	})
	return c.pool.SendBatch(ctx, b).Close()
}

// checkFilter checks that a filter's values are valid names, and returns them
// as a slice that is never nil: NULL would match nothing.
func checkFilter(what string, values []string, maxLen int) ([]string, error) {
	out := make([]string, 0, len(values))
	for _, v := range values {
		if err := checkName(what, v, maxLen); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, nil
}

// GroupParams selects and pages the FIFO groups that [Client.Groups]
// returns. A field outside its limits is an error that wraps
// [ErrInvalidArgument].
type GroupParams struct {
	// Queues limits the result to groups of these queues. Empty means all
	// queues. Maximum: 100 names.
	Queues []string

	// Search limits the result to groups whose keys contain it, ignoring
	// case. At most 256 characters. Empty means all groups.
	Search string

	// Halted limits the result to the groups that a dead task halts.
	Halted bool

	// Limit is the page size. It must not be negative. Default: 100.
	// Maximum: 1,000.
	Limit int

	// Cursor continues from a previous page's [GroupsResult.Next]. Empty
	// means the first page.
	Cursor string
}

// GroupsResult is one page of FIFO groups, ordered by queue and key.
type GroupsResult struct {
	Groups []GroupInfo

	// Next is the cursor for the next page, or empty on the last page.
	Next string
}

// GroupInfo describes a FIFO group that has unfinished tasks, or that a dead
// task halts.
type GroupInfo struct {
	Queue string
	Key   string

	// Tasks is how many tasks the group counts: its unfinished tasks, and
	// the dead task that halts it, if any. A group halted by its only task
	// counts 1.
	Tasks int

	// HaltedBy is the dead task that halts the group, or 0 if it runs.
	HaltedBy int64
}

// Groups returns the FIFO groups that have unfinished tasks or a dead task
// that halts them, one page at a time, ordered by queue and key. params may
// be nil, for the first page of every group. Each queue named in
// [GroupParams.Queues] reads up to a page, so it takes at most 100 names.
// Like [Client.List], each page's query is canceled after 5 seconds; a
// search that matches few of very many groups can reach that limit and
// return an error.
func (c *Client) Groups(ctx context.Context, params *GroupParams) (*GroupsResult, error) {
	p := GroupParams{}
	if params != nil {
		p = *params
	}
	sql, args, err := c.groupsQuery(&p)
	if err != nil {
		return nil, err
	}
	out := GroupsResult{Groups: []GroupInfo{}} // never nil, as callers have always got it
	err = c.readAlone(ctx, pageSettings, sql, args, func(r pgx.Rows) error {
		var g GroupInfo
		if err := r.Scan(&g.Queue, &g.Key, &g.Tasks, &g.HaltedBy); err != nil {
			return err
		}
		out.Groups = append(out.Groups, g)
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("taskmill: list groups: %w", err)
	}
	if len(out.Groups) > p.Limit {
		out.Groups = out.Groups[:p.Limit]
		last := out.Groups[p.Limit-1]
		out.Next = base64.RawURLEncoding.EncodeToString([]byte(last.Queue + "\x00" + last.Key))
	}
	return &out, nil
}

// groupsQuery checks p, sets its default limit, and returns the statement
// that reads its page, one group more than the limit, with its arguments.
func (c *Client) groupsQuery(p *GroupParams) (sql string, args []any, err error) {
	switch {
	case p.Limit == 0:
		p.Limit = defaultListLimit
	case p.Limit < 0 || p.Limit > maxListLimit:
		return "", nil, invalidArgument("limit must be between 1 and %d", maxListLimit)
	}
	if len(p.Queues) > maxGroupQueues {
		return "", nil, invalidArgument("at most %d queue names", maxGroupQueues)
	}
	queues, err := checkFilter("queue name", p.Queues, maxNameLength)
	if err != nil {
		return "", nil, err
	}
	if p.Search != "" {
		if err := checkName("search", p.Search, maxGroupKeyLength); err != nil {
			return "", nil, err
		}
	}
	var afterQueue, afterKey string
	if p.Cursor != "" {
		if afterQueue, afterKey, err = decodeGroupCursor(p.Cursor); err != nil {
			return "", nil, err
		}
	}
	args = []any{p.Search, p.Limit + 1, p.Halted, afterQueue, afterKey}
	if len(queues) == 0 {
		return c.sql.listGroups, args, nil
	}
	return c.sql.listQueueGroups, append(args, queues), nil
}

// decodeGroupCursor decodes the cursor of a page of groups into the queue
// and key of the group that the page starts after.
func decodeGroupCursor(cursor string) (queue, key string, err error) {
	raw, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return "", "", invalidArgument("invalid cursor")
	}
	queue, key, ok := strings.Cut(string(raw), "\x00")
	if !ok || checkName("queue", queue, maxNameLength) != nil || checkName("group key", key, maxGroupKeyLength) != nil {
		return "", "", invalidArgument("invalid cursor")
	}
	return queue, key, nil
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
	if before, err = decodeCursor(p.Cursor); err != nil {
		return 0, 0, invalidArgument("invalid cursor")
	}
	return limit, before, nil
}

// encodeCursor and decodeCursor turn a task ID into the opaque cursor of a
// list or a bulk action, and back.
func encodeCursor(id int64) string {
	return base64.RawURLEncoding.EncodeToString([]byte(strconv.FormatInt(id, 10)))
}

func decodeCursor(cursor string) (int64, error) {
	raw, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return 0, err
	}
	return strconv.ParseInt(string(raw), 10, 64)
}
