package taskmill

import (
	"bytes"
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"math/big"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/code19m/taskmill/internal/cron"
)

// Schedule enqueues a task on a cron schedule, once per scheduled time across
// all workers. Define the same schedules on every worker that takes part in
// the maintainer election: the elected maintainer runs its own definitions.
// It compares them with those of the other workers, by name and by
// definition, and logs an error when they keep differing for longer than a
// rolling deploy takes: the [schedules guide] says when. A definition is all
// that decides when a run happens and which task it enqueues, except the
// metadata, so args that hold a value of each process, such as the time
// that the worker started, count as a difference. If it defines any,
// it also deletes the stored schedules that it does not define, once no
// maintainer has defined them for about a day. Until then a stored schedule
// keeps its state, such as a pause: a rolling deploy or a rollback in which
// some workers do not define it loses nothing.
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
//
// Set a worker's schedules in [WorkerConfig.Schedules]. [NewWorker] reports
// a schedule that is not valid as an error that wraps [ErrInvalidArgument].
// The [schedules guide] shows a schedule from its timetable to its handler.
//
// [schedules guide]: https://github.com/code19m/taskmill/blob/main/docs/guides/schedules.md
type Schedule struct {
	// Name identifies the schedule, in 1 to 128 characters. Schedules are
	// kept in the database by name, so renaming one creates a new schedule.
	// Required.
	Name string

	// Cron is a cron expression of five fields separated by spaces, such as
	// "0 3 * * *": minute (0-59), hour (0-23), day of month (1-31), month
	// (1-12 or JAN-DEC), and day of week (0-7 or SUN-SAT, where 0 and 7 both
	// mean Sunday). A field is "*", a value, a range "a-b", or a list of
	// those separated by commas, and each of them may have a step "/n":
	// "*/15" in the first field means every 15 minutes. Names are read in
	// any letter case. If neither day field is "*", a day matches when
	// either field matches, as in classic cron.
	//
	// Cron can also be one of the macros @yearly, @annually, @monthly,
	// @weekly, @daily, @midnight, and @hourly. [NewWorker] rejects an
	// expression that matches no time in the next five years, such as
	// February 31. Required.
	Cron string

	// Location is the time zone the expression is read in. Default: UTC.
	// The time zones of the database and of the worker's process play no
	// part, except with time.Local: it is the zone of each worker's own
	// process, so workers in different zones read different timetables.
	Location *time.Location

	// Args is the task to enqueue, with the limits of any payload: see
	// [Args]. Required.
	Args Args

	// Options are the task's options. It may be nil. The schedule sets the
	// run time and the dedupe key itself, to enqueue each run once, so
	// [NewWorker] rejects Options that set RunAt, DedupeKey, or DedupeWindow.
	Options *EnqueueOptions
}

// Metadata keys that taskmill sets itself on every task that a [Schedule]
// enqueues. Their values never change, whatever happens to the task, so a
// handler reads the same on every attempt, and the dashboard shows which
// schedule made a task. taskmill sets its values over those that the
// schedule's Options, the kind's defaults, or an [EnqueueMiddleware] give
// the same keys.
//
// Keys that start with "taskmill." belong to taskmill, and later releases
// may add more of them: do not use the prefix for keys of your own. A test
// of a handler may set these two, so that its task stands for a scheduled
// run.
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
// attempt, unlike RunAt, which a retry, a snooze, or a lost lease moves.
//
// The time shows the clock of the schedule's Location, so its day and hour
// are those that the schedule's expression names. But it carries only the
// offset from UTC that this Location had at that moment, not the Location
// with its rules. So for arithmetic on the calendar that may cross a clock
// change, such as AddDate, first convert the time with In to the schedule's
// Location. For a Location whose offset is not a whole number of minutes,
// the time is in UTC.
//
// It returns the zero time for a task that no schedule enqueued, such as
// one enqueued by hand: a handler can fall back to RunAt then. It also
// returns the zero time for a task of a list in summary, which comes
// without its metadata: see [ListParams.Summary].
func (t *TaskInfo) ScheduledFor() time.Time {
	at, err := time.Parse(time.RFC3339Nano, t.Metadata[MetadataScheduledFor])
	if err != nil {
		return time.Time{}
	}
	return at
}

// ScheduleInfo describes a schedule and its state.
type ScheduleInfo struct {
	Name string

	// Queue is the queue of the task that the schedule enqueues.
	Queue string

	// Kind is the kind of that task.
	Kind string

	Cron string

	// Location is the name of the schedule's time zone, such as
	// "Europe/Berlin" or "UTC".
	Location string

	// NextRunAt is when the schedule's next run is due. A paused schedule
	// does not run then.
	NextRunAt time.Time
	LastRunAt time.Time // zero if it never ran
	Paused    bool
}

// compiledSchedule is a validated schedule with its parsed expression.
type compiledSchedule struct {
	Schedule
	expr *cron.Schedule
	opts EnqueueOptions
	hash string // of its definition: see definitionHash
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
	if noArgs(s.Args) {
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

	// Check the task as a run enqueues it, so that a mistake fails NewWorker
	// and not every run. A run sets its own run time and dedupe window over
	// those of the kind's defaults, so the check sets valid ones too. And a
	// run stores taskmill's own metadata, which is checked at its longest.
	opts.RunAt, opts.DedupeWindow = time.Now(), defaultDedupeWindow
	run, err := resolve(EnqueueParams{Args: s.Args, Options: &opts})
	if err == nil {
		run.reservedMeta = c.reservedMetadata(longestScheduledFor)
		_, err = encodeMetadata(run.metadata())
	}
	if err != nil {
		return compiledSchedule{}, fmt.Errorf("%w (schedule %q)", err, s.Name)
	}

	// A payload whose encoding changes from call to call, such as one that
	// holds the time, changes the stored row in every pass anyway. An
	// encoding that fails returns nothing, which equals no payload.
	again, _ := encodeJSON(s.Args)
	c.hash = definitionHash(&c, run, !jsonEqual(run.payload, again))
	return c, nil
}

// definitionHash returns a short hash of what decides when a run of schedule
// s happens, and which task it enqueues, which run holds resolved: the
// expression, the time zone, the queue, the kind, the priority, the most
// attempts, the FIFO group, and the payload, unless it varies. The payload
// counts as a JSON value, as jsonEqual compares it. A zone named Local is
// each process's own, so its offsets count too. A run's dedupe key and
// window follow from the schedule's name and timetable. The metadata is left
// out: it may hold values of each process, such as a trace's. A release that
// changes what the hash covers makes the schedules differ during its rolling
// deploy, which ends before the maintainer logs that.
func definitionHash(s *compiledSchedule, run *resolvedTask, varies bool) string {
	var b bytes.Buffer
	for _, field := range []string{
		s.Cron, zoneName(s.Location), run.queue, run.kind, strconv.Itoa(int(run.priority)), strconv.Itoa(run.maxAttempts), run.group,
	} {
		b.WriteString(strconv.Quote(field))
	}

	if !varies {
		var v any
		_ = decodeJSON(run.payload, &v) // never fails: resolve encoded the payload
		writeJSON(&b, v)
	}

	sum := sha256.Sum256(b.Bytes())
	return hex.EncodeToString(sum[:8])
}

// zoneName returns the name of loc, and for a zone named Local, which is the
// zone of each process, its offsets from UTC in January and in July, which
// tell most zones apart.
func zoneName(loc *time.Location) string {
	name := loc.String()
	if name != "Local" {
		return name
	}

	_, january := time.Date(2025, time.January, 15, 12, 0, 0, 0, time.UTC).In(loc).Zone()
	_, july := time.Date(2025, time.July, 15, 12, 0, 0, 0, time.UTC).In(loc).Zone()
	return name + " " + strconv.Itoa(january) + " " + strconv.Itoa(july)
}

// scheduleSums returns the names of schedules ss, sorted, and the hash of
// each one's definition, in the same order: what a worker stores of its
// schedules, so that the maintainer can compare them. Both are empty, not
// nil, for no schedules: the columns hold arrays.
func scheduleSums(ss []compiledSchedule) (names, hashes []string) {
	sorted := slices.SortedFunc(slices.Values(ss), func(a, b compiledSchedule) int { return cmp.Compare(a.Name, b.Name) })
	names, hashes = make([]string, len(sorted)), make([]string, len(sorted))
	for i, s := range sorted {
		names[i], hashes[i] = s.Name, s.hash
	}
	return names, hashes
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

// reservedMetadata returns taskmill's reserved metadata of a run of s, which
// is stored over the metadata of the schedule's task.
func (s *compiledSchedule) reservedMetadata(scheduledFor string) map[string]string {
	return map[string]string{MetadataSchedule: s.Name, MetadataScheduledFor: scheduledFor}
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
		m.scheduleFailures.observe(ctx, m.enqueueDue(ctx))
	}
}

// enqueueDue does the work of runSchedules, and returns the errors of the
// schedules that failed: one schedule's failure does not stop the others,
// except a statement that ran out of time. The database is slow or out of
// reach then, and each of the other schedules would wait as long: the pass
// stops, and the next one tries again.
func (m *maintainer) enqueueDue(ctx context.Context) error {
	readCtx, cancel := m.bound(ctx)
	defer cancel()
	var now time.Time
	if err := m.c.pool.QueryRow(readCtx, "SELECT now()").Scan(&now); err != nil {
		return fmt.Errorf("read database time: %w", err)
	}
	states, err := m.loadSchedules(readCtx)
	if err != nil {
		return fmt.Errorf("load schedules: %w", err)
	}

	var errs []error
	for i := range m.w.schedules {
		s := &m.w.schedules[i]
		if err := m.syncSchedule(ctx, s, states[s.Name], now); err != nil {
			errs = append(errs, fmt.Errorf("schedule %s: %w", s.Name, err))
			if errors.Is(err, context.DeadlineExceeded) {
				break
			}
		}
	}
	return errors.Join(errs...)
}

// loadSchedules reads the stored state of the worker's schedules, by name.
func (m *maintainer) loadSchedules(ctx context.Context) (map[string]*scheduleState, error) {
	r, err := m.c.pool.Query(ctx, m.c.sql.loadSchedules, m.w.scheduleNames)
	if err != nil {
		return nil, err
	}
	defer r.Close()

	states := make(map[string]*scheduleState, len(m.w.schedules))
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

// syncSchedule brings a schedule's row up to date with its definition, and
// enqueues its run if one is due.
//
// It runs code of the application: the encoding of the schedule's args, and
// the enqueue middleware. A panic there must not end the worker's process,
// and then the process of every maintainer elected after it. It fails the
// schedule's pass instead, as an error with the stack: the transaction of
// the run rolls back, so the run stays due, and the next pass tries again.
func (m *maintainer) syncSchedule(ctx context.Context, s *compiledSchedule, st *scheduleState, now time.Time) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = panicFailure(r)
		}
	}()

	ctx, cancel := m.bound(ctx)
	defer cancel()

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
			s.Name,          // $1
			s.opts.Queue,    // $2
			s.Args.Kind(),   // $3
			s.Cron,          // $4
			tz,              // $5
			string(payload), // $6
			next,            // $7
			resetNext,       // $8
		)
		if err != nil || resetNext {
			return err // with a new next run time, no run is due
		}
		// Continue to the due run: a definition that differs in every pass,
		// such as args that encode the time, must not keep it from running.
	}

	if st.paused || st.nextRunAt.After(now) {
		return nil
	}
	return m.enqueueRun(ctx, s, st.nextRunAt, now)
}

// enqueueRun enqueues the run of s that is due at due, and stores the time of
// the next regular run.
func (m *maintainer) enqueueRun(ctx context.Context, s *compiledSchedule, due, now time.Time) error {
	// A regular run is due at a whole minute of the timetable. Any other
	// time is the time of a request, which Client.RunSchedule stored.
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
		if _, err := tx.Exec(ctx, lockTimeoutSQL, m.w.timing.lockTimeout()); err != nil {
			return err
		}

		tag, err := tx.Exec(ctx, m.c.sql.advanceSchedule, s.Name, next, due)
		if err != nil || tag.RowsAffected() == 0 {
			return err // another maintainer ran it, or it was paused meanwhile
		}

		opts := s.opts
		opts.DedupeKey = "schedule:" + s.Name + ":" + strconv.FormatInt(key, 10)
		opts.DedupeWindow = max(24*time.Hour, 2*next.Sub(due))
		opts.RunAt = due // in the past: the task is available right away
		run, err := resolve(EnqueueParams{Args: s.Args, Options: &opts})
		if err != nil {
			return err
		}

		// The enqueue middleware sees taskmill's own metadata, and cannot
		// change it: a handler must find the run's time whatever it does.
		run.reservedMeta = s.reservedMetadata(s.scheduledFor(due))
		if _, err = m.c.storeThrough(ctx, pgxExecutor{q: tx}, false, []*resolvedTask{run}); err != nil {
			return err
		}

		// The timeout was for the group's row. At the commit, it would keep
		// the schedule from running for as long as notifying transactions
		// queue there for longer than that wait, while every other enqueue
		// only waits its turn.
		_, err = tx.Exec(ctx, liftLockTimeout)
		return err
	})
}

// pruneSchedules deletes the stored schedules that this maintainer does not
// define, once no maintainer has defined them for a day, and notes that it
// defines its own. A maintainer that defines none, such as a worker that
// only does maintenance, leaves them alone.
func (m *maintainer) pruneSchedules(ctx context.Context) {
	if len(m.w.schedules) == 0 {
		return
	}

	pruneCtx, cancel := m.bound(ctx)
	defer cancel()
	_, err := m.c.pool.Exec(pruneCtx, m.c.sql.pruneSchedules, m.w.scheduleNames)
	m.pruneFailures.observe(ctx, err)
}

// compareSchedules compares the schedules of the other live workers that
// take part in the maintainer election with this maintainer's, by name and
// by definition, and logs a difference that lasts: see scheduleWatch. It
// compares them also when this maintainer defines none, which is the worst
// case: then no schedule runs.
func (m *maintainer) compareSchedules(ctx context.Context) {
	term, ok := m.watch.current()
	if !ok {
		return
	}

	diff, err := m.otherSchedules(ctx)
	m.compareFailures.observe(ctx, err)
	if err == nil {
		m.watch.observe(ctx, m.w, diff, term)
	}
}

// otherSchedules returns how the schedules of up to 10 other live workers
// differ from this maintainer's.
func (m *maintainer) otherSchedules(ctx context.Context) (*scheduleDiff, error) {
	ctx, cancel := m.bound(ctx)
	defer cancel()
	r, err := m.c.pool.Query(ctx, m.c.sql.otherSchedules, m.w.scheduleNames, m.w.scheduleHashes)
	if err != nil {
		return nil, err
	}
	defer r.Close()

	d := &scheduleDiff{missingHere: map[string]bool{}, missingThere: map[string]bool{}, definedDifferently: map[string]bool{}}
	for r.Next() {
		var id string
		var names, hashes []string
		if err := r.Scan(&id, &names, &hashes); err != nil {
			return nil, err
		}
		d.add(id, m.ownHashes, byName(names, hashes))
	}
	return d, r.Err()
}

// byName returns the hash of each schedule by its name. The table holds a
// hash for each name.
func byName(names, hashes []string) map[string]string {
	out := make(map[string]string, len(names))
	for i, name := range names {
		out[name] = hashes[i]
	}
	return out
}

// scheduleDiff says how the schedules of other workers differ from the
// maintainer's.
type scheduleDiff struct {
	workers []string // the workers whose schedules differ; none if they all match

	// The schedules that the workers define and the maintainer does not,
	// those that it defines and some of them do not, and those that both
	// define differently.
	missingHere, missingThere, definedDifferently map[string]bool
}

// add adds the worker id, whose schedules are theirs, to d if they differ
// from ours, the maintainer's. Both map each schedule's name to its hash.
func (d *scheduleDiff) add(id string, ours, theirs map[string]string) {
	differs := false
	for name, hash := range theirs {
		switch h, ok := ours[name]; {
		case !ok:
			d.missingHere[name], differs = true, true
		case h != hash:
			d.definedDifferently[name], differs = true, true
		}
	}
	for name := range ours {
		if _, ok := theirs[name]; !ok {
			d.missingThere[name], differs = true, true
		}
	}

	if differs {
		d.workers = append(d.workers, id)
	}
}

// schedulesLogged is how many names of each list of schedules a line names
// at most. It says how many more there are after them.
const schedulesLogged = 20

// attrs returns the attributes of a line about d, which has lasted for
// lasted.
func (d *scheduleDiff) attrs(lasted time.Duration) []slog.Attr {
	sorted := func(set map[string]bool) []string {
		names := slices.AppendSeq(make([]string, 0, len(set)), maps.Keys(set)) // not nil: none logs as []
		slices.Sort(names)
		if more := len(names) - schedulesLogged; more > 0 {
			names = append(names[:schedulesLogged], "and "+strconv.Itoa(more)+" more")
		}
		return names
	}
	return []slog.Attr{
		slog.Any(logKeyWorkers, d.workers),
		slog.Any(logKeyMissingHere, sorted(d.missingHere)),
		slog.Any(logKeyMissingThere, sorted(d.missingThere)),
		slog.Any(logKeyDefinedDifferently, sorted(d.definedDifferently)),
		slog.Duration(logKeyDuration, lasted),
	}
}

// scheduleWatch follows a difference between the schedules of other workers
// and the maintainer's. A rolling deploy that changes a schedule makes one
// for a few minutes, so it logs nothing until the difference has lasted for
// timing.scheduleGrace: then an error, again at most once per
// timing.scheduleReport while it lasts, and a line when it ends. It starts
// over in each term of the maintainer, and counts only the comparisons of
// the term: one that a term began and that ended after it may report what
// changed between the terms, such as a difference that ended, which is the
// next term's to find.
type scheduleWatch struct {
	mu     sync.Mutex
	term   int       // counts the terms of the maintainer
	open   bool      // the term lasts
	since  time.Time // when the comparison first found the difference; zero while there is none
	logged time.Time // when it was last logged; zero if not yet
}

// current returns the term that a comparison which starts now belongs to,
// and false between terms.
func (s *scheduleWatch) current() (term int, ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.term, s.open
}

// observe takes the outcome of one comparison, which began in term. It
// drops the outcome if that term has ended.
func (s *scheduleWatch) observe(ctx context.Context, w *Worker, d *scheduleDiff, term int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.open || s.term != term {
		return
	}

	now := time.Now()
	switch {
	case len(d.workers) == 0:
		if !s.logged.IsZero() {
			w.logger.LogAttrs(ctx, slog.LevelInfo, "the schedules of other workers match the maintainer's again",
				slog.Duration(logKeyDuration, now.Sub(s.since)))
		}
		s.since, s.logged = time.Time{}, time.Time{}
	case s.since.IsZero():
		s.since = now
	case now.Sub(s.since) >= w.timing.scheduleGrace && now.Sub(s.logged) >= w.timing.scheduleReport:
		w.logger.LogAttrs(ctx, slog.LevelError, "the schedules of other workers differ from the maintainer's",
			d.attrs(now.Sub(s.since))...)
		s.logged = now
	}
}

// start begins a term without the difference of the last one, and without
// a line.
func (s *scheduleWatch) start() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.term++
	s.open, s.since, s.logged = true, time.Time{}, time.Time{}
}

// end ends the term: the comparisons under way count for nothing.
func (s *scheduleWatch) end() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.open = false
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

// writeJSON writes v, a value from decodeJSON, to b in one form for all the
// values that sameJSON finds equal: each number as its exact value, and the
// keys of each object in order.
func writeJSON(b *bytes.Buffer, v any) {
	switch v := v.(type) {
	case json.Number:
		if r, ok := new(big.Rat).SetString(v.String()); ok {
			b.WriteString("n" + r.Num().Text(16) + "/" + r.Denom().Text(16) + " ")
		} else {
			b.WriteString("t" + v.String() + " ") // a number that only its own text equals: see sameNumber
		}
	case []any:
		b.WriteString("[")
		for _, x := range v {
			writeJSON(b, x)
		}
		b.WriteString("]")
	case map[string]any:
		b.WriteString("{")
		for _, key := range slices.Sorted(maps.Keys(v)) {
			b.WriteString(strconv.Quote(key))
			writeJSON(b, v[key])
		}
		b.WriteString("}")
	case string:
		b.WriteString(strconv.Quote(v))
	case bool:
		b.WriteString(strconv.FormatBool(v) + " ")
	default: // null
		b.WriteString("null ")
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

// Schedules returns every schedule stored in the database, ordered by name.
func (c *Client) Schedules(ctx context.Context) ([]ScheduleInfo, error) {
	fail := func(err error) error { return fmt.Errorf("taskmill: list schedules: %w", err) }

	r, err := c.pool.Query(ctx, c.sql.listSchedules)
	if err != nil {
		return nil, fail(err)
	}
	defer r.Close()

	var out []ScheduleInfo
	for r.Next() {
		var s ScheduleInfo
		var last *time.Time
		if err := r.Scan(&s.Name, &s.Queue, &s.Kind, &s.Cron, &s.Location, &s.NextRunAt, &last, &s.Paused); err != nil {
			return nil, fail(err)
		}
		s.LastRunAt = deref(last)
		out = append(out, s)
	}
	if err := r.Err(); err != nil {
		return nil, fail(err)
	}
	return out, nil
}

// PauseSchedule stops a schedule from enqueueing runs until it is resumed.
// It returns [ErrNotFound] for an unknown schedule.
func (c *Client) PauseSchedule(ctx context.Context, name string) error {
	return c.setSchedulePaused(ctx, name, true)
}

// ResumeSchedule undoes [Client.PauseSchedule]. Runs missed while paused
// collapse into one run. It returns [ErrNotFound] for an unknown schedule.
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
