package taskmill

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"math/big"
	"slices"
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
// paused, collapse into one run, for the first time that was missed. Each
// run's task carries the schedule's name and the time the run is for in its
// metadata; see [MetadataSchedule]. In a handler, read that time with
// [TaskInfo.ScheduledFor]: the task's RunAt is that time only until the
// first retry or snooze moves it.
//
// The expression is read on the wall clock of Location, and each time on
// that clock that it names runs once. When clocks go forward, every time
// that they skip runs after the jump, as long after it as the time is into
// the skipped hour: 02:30 runs at 03:30. When clocks go back, a time in the
// repeated hour runs the first time the clock shows it, and not the second.
// So a schedule that runs several times an hour, such as "*/15 * * * *",
// makes no run during the second pass of that hour, where classic cron
// keeps it running.
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
	// each run once, so [NewWorker] rejects Options that set RunAt,
	// DedupeKey, or DedupeWindow.
	Args    Args
	Options *EnqueueOptions
}

// Metadata keys that taskmill sets itself on every task that a [Schedule]
// enqueues. Their values never change, whatever happens to the task, so a
// handler reads the same on every attempt, and the dashboard shows which
// schedule made a task. taskmill's values replace those that the schedule's
// Options or the kind's defaults give the same keys. Keys that start with
// "taskmill." are taskmill's own: do not set them on your tasks.
const (
	// MetadataSchedule holds the name of the schedule that enqueued the
	// task.
	MetadataSchedule = "taskmill.schedule"

	// MetadataScheduledFor holds the time that the run is for, in the
	// format of RFC 3339. Read it with [TaskInfo.ScheduledFor].
	MetadataScheduledFor = "taskmill.scheduled_for"
)

// ScheduledFor returns the time that the task's run is for, if a [Schedule]
// enqueued the task: the time of the timetable, or the time of the request
// for a run that [Client.RunSchedule] asked for. It is the same on every
// attempt, unlike RunAt, which a retry, a snooze, or a lost lease moves. The
// time is on the clock of the schedule's Location, so its day and hour are
// those that the schedule's expression names.
//
// It returns the zero time for a task that no schedule enqueued.
func (t *TaskInfo) ScheduledFor() time.Time {
	at, err := time.Parse(time.RFC3339Nano, t.Metadata[MetadataScheduledFor])
	if err != nil {
		return time.Time{}
	}
	return at
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
		c, err := compileSchedule(s)
		if err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, nil
}

// compileSchedule validates one schedule, and resolves the options of its
// task.
func compileSchedule(s Schedule) (compiledSchedule, error) {
	if s.Args == nil {
		return compiledSchedule{}, invalidArgument("schedule %q has no args", s.Name)
	}
	if o := s.Options; o != nil && (!o.RunAt.IsZero() || o.DedupeKey != "" || o.DedupeWindow != 0) {
		return compiledSchedule{}, invalidArgument(
			"schedule %q: Options set RunAt, DedupeKey, or DedupeWindow, which the schedule sets itself", s.Name,
		)
	}
	if s.Location == nil {
		s.Location = time.UTC
	}
	expr, err := cron.Parse(s.Cron)
	if err == nil {
		_, err = expr.Next(time.Now().In(s.Location)) // fails if it never matches
	}
	if err != nil {
		return compiledSchedule{}, invalidArgument("schedule %q: %v", s.Name, err)
	}
	var opts EnqueueOptions
	if d, ok := s.Args.(ArgsWithDefaults); ok {
		opts = d.EnqueueDefaults()
	}
	if s.Options != nil {
		opts = mergeOptions(opts, *s.Options)
	}
	opts.Queue = cmp.Or(opts.Queue, DefaultQueue)
	c := compiledSchedule{Schedule: s, expr: expr, opts: opts}
	// Check the task as a run enqueues it: with taskmill's own metadata, at
	// its longest.
	opts.Metadata = c.runMetadata(longestScheduledFor)
	if err := checkScheduledTask(s.Args, opts); err != nil {
		return compiledSchedule{}, fmt.Errorf("%w (schedule %q)", err, s.Name)
	}
	return c, nil
}

// longestScheduledFor takes as much room as any value of
// MetadataScheduledFor: the database keeps times to the microsecond.
const longestScheduledFor = "2006-01-02T15:04:05.999999+07:00"

// scheduledFor returns the value of MetadataScheduledFor for the run of s
// that is for the time at: at on the clock of the schedule's location.
func (s *compiledSchedule) scheduledFor(at time.Time) string {
	at = at.In(s.Location)
	if _, offset := at.Zone(); offset%60 != 0 {
		at = at.UTC() // RFC 3339 has no seconds in an offset, and would lose them
	}
	return at.Format(time.RFC3339Nano)
}

// runMetadata returns the metadata of a run of s: the metadata of the
// schedule's task, and over it taskmill's own keys.
func (s *compiledSchedule) runMetadata(scheduledFor string) map[string]string {
	meta := make(map[string]string, len(s.opts.Metadata)+2)
	maps.Copy(meta, s.opts.Metadata)
	meta[MetadataSchedule] = s.Name
	meta[MetadataScheduledFor] = scheduledFor
	return meta
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
	if _, err := encodeMetadata(opts.Metadata); err != nil {
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
	if len(m.w.schedules) > 0 {
		m.schedules.observe(ctx, m.enqueueDue(ctx))
	}
}

// enqueueDue does the work of runSchedules, and returns the errors of the
// schedules that failed: one schedule's failure does not stop the others.
func (m *maintainer) enqueueDue(ctx context.Context) error {
	var now time.Time
	if err := m.c.pool.QueryRow(ctx, "SELECT now()").Scan(&now); err != nil {
		return fmt.Errorf("read database time: %w", err)
	}
	states, err := m.loadSchedules(ctx)
	if err != nil {
		return fmt.Errorf("load schedules: %w", err)
	}
	var errs []error
	for i := range m.w.schedules {
		s := &m.w.schedules[i]
		if err := m.runSchedule(ctx, s, states[s.Name], now); err != nil {
			errs = append(errs, fmt.Errorf("schedule %s: %w", s.Name, err))
		}
	}
	return errors.Join(errs...)
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
		if err != nil || resetNext {
			return err // with a new next run time, no run is due
		}
		// Go on to the due run: a definition that differs in every pass,
		// such as args that encode the time, must not keep it from running.
	}
	if st.paused || st.nextRunAt.After(now) {
		return nil
	}

	// A regular run is due at a whole minute of the timetable. Any other
	// time is the time of a request, which Client.RunSchedule stored.
	due := st.nextRunAt
	requested := due.In(s.Location).Second() != 0 || due.Nanosecond() != 0
	// The regular runs missed since a regular run collapse into it, so the
	// next one comes after now. A requested run stands for no regular run,
	// so the next one comes after the request: if it is due already, the
	// next pass enqueues it.
	after := now
	if requested {
		after = due
	}
	next, err := s.expr.Next(after.In(s.Location))
	if err != nil {
		return err
	}
	// The key is the due time, in the unit that tells the two kinds of run
	// apart: a request may come in the second of a regular run.
	key := due.Unix()
	if requested {
		key = due.UnixMicro()
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
		opts.Metadata = s.runMetadata(s.scheduledFor(due))
		opts.DedupeKey = "schedule:" + s.Name + ":" + strconv.FormatInt(key, 10)
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
	_, err := m.c.pool.Exec(ctx, m.c.sql.pruneSchedules, names)
	m.pruning.observe(ctx, err)
}

// jsonEqual reports whether a and b hold the same JSON value, as PostgreSQL's
// jsonb compares them: numbers by their exact value, so that 0 equals -0 and
// 100 equals 1e2. jsonb stores numbers in its own form, so comparing their
// text, or their values as float64, would find a stored payload changed in
// every pass.
func jsonEqual(a, b []byte) bool {
	var x, y any
	return decodeJSON(a, &x) == nil && decodeJSON(b, &y) == nil && sameJSON(x, y)
}

// decodeJSON decodes JSON text into v, and keeps each number as its text.
func decodeJSON(text []byte, v *any) error {
	dec := json.NewDecoder(bytes.NewReader(text))
	dec.UseNumber()
	return dec.Decode(v)
}

// sameJSON reports whether two values from decodeJSON are equal.
func sameJSON(x, y any) bool {
	switch x := x.(type) {
	case json.Number:
		y, ok := y.(json.Number)
		return ok && (x == y || sameNumber(x, y))
	case []any:
		y, ok := y.([]any)
		return ok && slices.EqualFunc(x, y, sameJSON)
	case map[string]any:
		y, ok := y.(map[string]any)
		return ok && maps.EqualFunc(x, y, sameJSON)
	default: // a string, a bool, or null
		return x == y
	}
}

// sameNumber reports whether two JSON numbers have the same value. It is
// false for a number whose exponent is beyond what math/big reads, a million,
// which is far beyond what PostgreSQL stores.
func sameNumber(x, y json.Number) bool {
	p, okP := new(big.Rat).SetString(x.String())
	q, okQ := new(big.Rat).SetString(y.String())
	return okP && okQ && p.Cmp(q) == 0
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

// RunSchedule runs a schedule once now, in addition to its timetable. The
// maintainer enqueues the run within about a second, if it defines the
// schedule, with the schedule's options, and the run is for the time of the
// request: its RunAt at first, and its [TaskInfo.ScheduledFor]. The regular
// runs stay as they were. A request adds no run while a run of the schedule
// is due and waits for the maintainer, whether a regular run or an earlier
// request: that run is the one that runs now. It returns [ErrNotFound] for
// an unknown schedule, and [ErrInvalidState] for a paused one.
func (c *Client) RunSchedule(ctx context.Context, name string) error {
	if err := checkName("schedule name", name, maxNameLength); err != nil {
		return err
	}
	var paused bool
	if err := c.pool.QueryRow(ctx, c.sql.runSchedule, name).Scan(&paused); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("%w: schedule %q", ErrNotFound, name)
		}
		return fmt.Errorf("taskmill: run schedule %q: %w", name, err)
	}
	if paused {
		return fmt.Errorf("%w: cannot run schedule %q while it is paused; resume it first", ErrInvalidState, name)
	}
	return nil
}

func (c *Client) setSchedulePaused(ctx context.Context, name string, paused bool) error {
	if err := checkName("schedule name", name, maxNameLength); err != nil {
		return err
	}
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
