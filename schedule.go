package taskmill

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/code19m/taskmill/internal/cron"
)

// Schedule enqueues a task on a cron schedule, once per scheduled time across
// all workers. Define the same schedules on every worker that takes part in
// the maintainer election: the elected maintainer runs its own definitions.
// If it defines any, it also deletes the stored schedules that it does not
// define once they have not run or changed for a day.
//
// Runs that are missed while no maintainer runs, or while the schedule is
// paused, collapse into one run. The expression is read on the wall clock of
// Location, so daylight saving time behaves as in classic cron. The task's
// RunAt is the time it was scheduled for.
type Schedule struct {
	// Name identifies the schedule. Schedules are kept in the database by
	// name, so renaming one creates a new schedule.
	Name string

	// Cron is a five-field cron expression, such as "0 3 * * *", or one of
	// the macros @yearly, @annually, @monthly, @weekly, @daily, @midnight,
	// and @hourly.
	Cron string

	// Location is the time zone the expression is read in. Default: UTC.
	Location *time.Location

	// Args and Options describe the task to enqueue; Options may be nil.
	// The schedule sets the run time and the dedupe key itself, to enqueue
	// each run once.
	Args    Args
	Options *EnqueueOptions
}

// ScheduleInfo describes a schedule and its state.
type ScheduleInfo struct {
	Name      string
	Queue     string
	Kind      string
	Cron      string
	Location  string
	NextRunAt time.Time
	LastRunAt time.Time // zero if it never ran
	Paused    bool
}

// compiledSchedule is a validated schedule with its parsed expression.
type compiledSchedule struct {
	Schedule
	expr *cron.Schedule
	opts EnqueueOptions
}

func compileSchedules(ss []Schedule) ([]compiledSchedule, error) {
	out := make([]compiledSchedule, 0, len(ss))
	seen := make(map[string]bool, len(ss))
	for _, s := range ss {
		if err := checkName("schedule name", s.Name, maxNameLength); err != nil {
			return nil, err
		}
		if seen[s.Name] {
			return nil, invalidArgument("schedule %q defined twice", s.Name)
		}
		seen[s.Name] = true
		if s.Args == nil {
			return nil, invalidArgument("schedule %q has no args", s.Name)
		}
		if s.Location == nil {
			s.Location = time.UTC
		}
		expr, err := cron.Parse(s.Cron)
		if err == nil {
			_, err = expr.Next(time.Now().In(s.Location)) // fails if it never matches
		}
		if err != nil {
			return nil, invalidArgument("schedule %q: %v", s.Name, err)
		}
		var opts EnqueueOptions
		if d, ok := s.Args.(ArgsWithDefaults); ok {
			opts = d.EnqueueDefaults()
		}
		if s.Options != nil {
			opts = mergeOptions(opts, *s.Options)
		}
		opts.Queue = cmp.Or(opts.Queue, DefaultQueue)
		if err := checkScheduledTask(s.Args, opts); err != nil {
			return nil, fmt.Errorf("%w (schedule %q)", err, s.Name)
		}
		out = append(out, compiledSchedule{Schedule: s, expr: expr, opts: opts})
	}
	return out, nil
}

// checkScheduledTask validates the task that a schedule enqueues, so that a
// mistake fails NewWorker instead of every run.
func checkScheduledTask(args Args, opts EnqueueOptions) error {
	kind := args.Kind()
	if err := checkName("kind", kind, maxNameLength); err != nil {
		return err
	}
	t := resolvedTask{
		queue:       opts.Queue,
		kind:        kind,
		group:       opts.GroupKey,
		priority:    cmp.Or(opts.Priority, PriorityNormal),
		maxAttempts: cmp.Or(opts.MaxAttempts, defaultMaxAttempts),
	}
	if err := t.validate(); err != nil {
		return err
	}
	_, err := encodePayload(kind, args)
	return err
}

// scheduleState is a schedule's row in the database.
type scheduleState struct {
	queue, kind, cron, timezone string
	payload                     []byte
	nextRunAt                   time.Time
	paused                      bool
}

// runSchedules enqueues the due runs of the worker's schedules.
func (m *maintainer) runSchedules(ctx context.Context) {
	if len(m.w.schedules) == 0 {
		return
	}
	var now time.Time
	if err := m.c.pool.QueryRow(ctx, "SELECT now()").Scan(&now); err != nil {
		m.log(ctx, slog.LevelError, "read database time", err)
		return
	}
	states, err := m.loadSchedules(ctx)
	if err != nil {
		m.log(ctx, slog.LevelError, "load schedules", err)
		return
	}
	for i := range m.w.schedules {
		s := &m.w.schedules[i]
		if err := m.runSchedule(ctx, s, states[s.Name], now); err != nil {
			m.log(ctx, slog.LevelError, "run schedule "+s.Name, err)
		}
	}
}

func (m *maintainer) loadSchedules(ctx context.Context) (map[string]*scheduleState, error) {
	names := make([]string, len(m.w.schedules))
	for i, s := range m.w.schedules {
		names[i] = s.Name
	}
	r, err := m.c.pool.Query(ctx, m.c.sql.loadSchedules, names)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	states := make(map[string]*scheduleState, len(names))
	for r.Next() {
		var name string
		st := &scheduleState{}
		if err := r.Scan(&name, &st.queue, &st.kind, &st.cron, &st.timezone, &st.payload, &st.nextRunAt, &st.paused); err != nil {
			return nil, err
		}
		states[name] = st
	}
	return states, r.Err()
}

// runSchedule brings a schedule's row up to date with its definition, and
// enqueues its run if one is due.
func (m *maintainer) runSchedule(ctx context.Context, s *compiledSchedule, st *scheduleState, now time.Time) error {
	payload, err := json.Marshal(s.Args)
	if err != nil {
		return fmt.Errorf("encode args: %w", err)
	}
	tz := s.Location.String()
	changed := st == nil || st.cron != s.Cron || st.timezone != tz || st.queue != s.opts.Queue ||
		st.kind != s.Args.Kind() || !jsonEqual(st.payload, payload)
	if changed {
		next, err := s.expr.Next(now.In(s.Location))
		if err != nil {
			return err
		}
		resetNext := st == nil || st.cron != s.Cron || st.timezone != tz
		_, err = m.c.pool.Exec(ctx, m.c.sql.upsertSchedule,
			s.Name, s.opts.Queue, s.Args.Kind(), s.Cron, tz, string(payload), next, resetNext)
		return err
	}
	if st.paused || st.nextRunAt.After(now) {
		return nil
	}

	due := st.nextRunAt
	next, err := s.expr.Next(now.In(s.Location))
	if err != nil {
		return err
	}
	return pgx.BeginFunc(ctx, m.c.pool, func(tx pgx.Tx) error {
		// An enqueue into a FIFO group waits for the group's row. The election
		// runs in the same loop, so give up after a moment, and try again in
		// the next cycle.
		if _, err := tx.Exec(ctx, "SET LOCAL lock_timeout = '"+maintenanceLockTimeout+"'"); err != nil {
			return err
		}
		tag, err := tx.Exec(ctx, m.c.sql.advanceSchedule, s.Name, next, due)
		if err != nil || tag.RowsAffected() == 0 {
			return err // another maintainer ran it, or it was paused meanwhile
		}
		opts := s.opts
		opts.DedupeKey = "schedule:" + s.Name + ":" + strconv.FormatInt(due.Unix(), 10)
		opts.DedupeWindow = max(24*time.Hour, 2*next.Sub(due))
		opts.RunAt = due // in the past: the task is available at once
		_, err = m.c.enqueue(ctx, pgxQuerier{q: tx}, []EnqueueParams{{Args: s.Args, Options: &opts}})
		return err
	})
}

// pruneSchedules deletes the stored schedules that this maintainer does not
// define, once they have not run or changed for a day. A maintainer that
// defines none, such as a worker that only does maintenance, leaves them
// alone.
func (m *maintainer) pruneSchedules(ctx context.Context) {
	if len(m.w.schedules) == 0 {
		return
	}
	names := make([]string, len(m.w.schedules))
	for i, s := range m.w.schedules {
		names[i] = s.Name
	}
	if _, err := m.c.pool.Exec(ctx, m.c.sql.pruneSchedules, names); err != nil {
		m.log(ctx, slog.LevelError, "delete removed schedules", err)
	}
}

func jsonEqual(a, b []byte) bool {
	var x, y any
	if json.Unmarshal(a, &x) != nil || json.Unmarshal(b, &y) != nil {
		return false
	}
	ax, _ := json.Marshal(x)
	by, _ := json.Marshal(y)
	return string(ax) == string(by)
}

// Schedules returns every schedule stored in the database.
func (c *Client) Schedules(ctx context.Context) ([]ScheduleInfo, error) {
	r, err := c.pool.Query(ctx, c.sql.listSchedules)
	if err != nil {
		return nil, fmt.Errorf("taskmill: list schedules: %w", err)
	}
	defer r.Close()
	var out []ScheduleInfo
	for r.Next() {
		var s ScheduleInfo
		var last *time.Time
		if err := r.Scan(&s.Name, &s.Queue, &s.Kind, &s.Cron, &s.Location, &s.NextRunAt, &last, &s.Paused); err != nil {
			return nil, fmt.Errorf("taskmill: list schedules: %w", err)
		}
		s.LastRunAt = deref(last)
		out = append(out, s)
	}
	if err := r.Err(); err != nil {
		return nil, fmt.Errorf("taskmill: list schedules: %w", err)
	}
	return out, nil
}

// PauseSchedule stops a schedule from enqueueing runs until it is resumed.
// It returns [ErrNotFound] for an unknown schedule.
func (c *Client) PauseSchedule(ctx context.Context, name string) error {
	return c.setSchedulePaused(ctx, name, true)
}

// ResumeSchedule undoes [Client.PauseSchedule]. Runs missed while paused
// collapse into one run.
func (c *Client) ResumeSchedule(ctx context.Context, name string) error {
	return c.setSchedulePaused(ctx, name, false)
}

func (c *Client) setSchedulePaused(ctx context.Context, name string, paused bool) error {
	verb := "resume"
	if paused {
		verb = "pause"
	}
	tag, err := c.pool.Exec(ctx, c.sql.pauseSchedule, name, paused)
	if err != nil {
		return fmt.Errorf("taskmill: %s schedule %q: %w", verb, name, err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("%w: schedule %q", ErrNotFound, name)
	}
	return nil
}
