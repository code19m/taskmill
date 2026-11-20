package taskmill_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"math"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/code19m/taskmill"
	"github.com/code19m/taskmill/internal/testdb"
)

func TestScheduleEnqueuesEachRunOnce(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	schedules := []taskmill.Schedule{{Name: "digest", Cron: "@hourly", Args: sendEmail{To: "digest"}}}
	a := newWorker(t, e, &taskmill.WorkerConfig{Schedules: schedules})
	b := newWorker(t, e, &taskmill.WorkerConfig{Schedules: schedules})
	ctx := t.Context()
	tasks := func() int { return e.count(t, "SELECT count(*) FROM {s}.tasks") }

	// Each run is due at a different time, as cron runs are: runs are
	// deduplicated by their due time.
	makeDue := func(ago time.Duration) {
		e.exec(t, "UPDATE {s}.schedules SET next_run_at = date_trunc('second', now()) - $1 * interval '1 second'",
			ago.Seconds())
	}

	// The first pass stores the schedule, due at the next full hour.
	taskmill.RunSchedules(ctx, a)
	ss, err := e.client.Schedules(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(ss) != 1 || ss[0].Kind != "email.send" || ss[0].Queue != taskmill.DefaultQueue || ss[0].NextRunAt.UTC().Minute() != 0 {
		t.Fatalf("got %+v", ss)
	}
	if n := tasks(); n != 0 {
		t.Fatalf("a schedule that is not due enqueued %d tasks", n)
	}

	// Two maintainers whose terms overlap both find the run due: it is
	// enqueued once, to run at the time it was scheduled for.
	makeDue(time.Second)
	var slot time.Time
	if err := e.pool.QueryRow(ctx, replaceSchema("SELECT next_run_at FROM {s}.schedules", e.schema)).Scan(&slot); err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	for _, w := range []*taskmill.Worker{a, b} {
		wg.Go(func() { taskmill.RunSchedules(ctx, w) })
	}
	wg.Wait()

	if n := tasks(); n != 1 {
		t.Fatalf("one due run enqueued %d tasks", n)
	}
	page, err := e.client.List(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := page.Tasks[0].RunAt; !got.Equal(slot) {
		t.Fatalf("the task runs at %s, want its slot %s", got, slot)
	}

	// A paused schedule does not run; once resumed, its missed run does.
	if err := e.client.PauseSchedule(ctx, "digest"); err != nil {
		t.Fatal(err)
	}
	makeDue(time.Minute)
	taskmill.RunSchedules(ctx, a)
	if n := tasks(); n != 1 {
		t.Fatal("a paused schedule ran")
	}

	if err := e.client.ResumeSchedule(ctx, "digest"); err != nil {
		t.Fatal(err)
	}
	taskmill.RunSchedules(ctx, a)
	if n := tasks(); n != 2 {
		t.Fatal("a resumed schedule did not run")
	}

	if err := e.client.PauseSchedule(ctx, "unknown"); !errors.Is(err, taskmill.ErrNotFound) {
		t.Fatalf("unknown schedule: got %v", err)
	}
}

// adjust is a task whose payload holds a float.
type adjust struct {
	Delta float64 `json:"delta"`
}

func (adjust) Kind() string { return "test.adjust" }

// ticking is a task whose payload differs every time it is encoded.
type ticking struct {
	encoded *atomic.Int64
}

func (ticking) Kind() string { return "test.ticking" }

func (a ticking) MarshalJSON() ([]byte, error) {
	return fmt.Appendf(nil, `{"n":%d}`, a.encoded.Add(1)), nil
}

func TestScheduleRunsWhateverItsStoredPayloadLooksLike(t *testing.T) {
	t.Parallel()
	e := newEnv(t)

	// jsonb rewrites the numbers of the first two payloads: -0 becomes 0,
	// 1e2 becomes 100, and 1e400, which no float64 holds, is written in
	// full. The third payload really changes with every pass.
	w := newWorker(t, e, &taskmill.WorkerConfig{Schedules: []taskmill.Schedule{
		{Name: "negative-zero", Cron: "@hourly", Args: adjust{Delta: math.Copysign(0, -1)}},
		{Name: "exponents", Cron: "@hourly", Args: rawArgs{Body: json.RawMessage(`[1e2, 1.50e1, 1e400]`)}},
		{Name: "ticking", Cron: "@hourly", Args: ticking{encoded: new(atomic.Int64)}},
	}})

	ctx := t.Context()
	storedAt := func(name string) time.Time {
		var at time.Time
		err := e.pool.QueryRow(ctx, replaceSchema("SELECT updated_at FROM {s}.schedules WHERE name = $1", e.schema), name).Scan(&at)
		if err != nil {
			t.Fatal(err)
		}
		return at
	}

	// A definition that did not change is stored once, not in every pass.
	taskmill.RunSchedules(ctx, w)
	for _, name := range []string{"negative-zero", "exponents"} {
		first := storedAt(name)
		taskmill.RunSchedules(ctx, w)
		if again := storedAt(name); !again.Equal(first) {
			t.Errorf("schedule %s did not change, but the next pass stored it again", name)
		}
	}

	// Each schedule runs when it is due, also the one that changes in the
	// same pass.
	e.exec(t, "UPDATE {s}.schedules SET next_run_at = date_trunc('second', now()) - interval '1 second'")
	taskmill.RunSchedules(ctx, w)

	for _, kind := range []string{"test.adjust", "raw", "test.ticking"} {
		if n := e.count(t, "SELECT count(*) FROM {s}.tasks WHERE kind = $1", kind); n != 1 {
			t.Errorf("the due schedule of kind %s enqueued %d tasks, want 1", kind, n)
		}
	}
}

func TestRunScheduleNow(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	w := newWorker(t, e, &taskmill.WorkerConfig{
		Schedules: []taskmill.Schedule{{
			Name:    "yearly-report",
			Cron:    "@yearly",
			Args:    sendEmail{To: "report"},
			Options: &taskmill.EnqueueOptions{Priority: taskmill.PriorityHigh},
		}},
	})

	ctx := t.Context()
	taskmill.RunSchedules(ctx, w)
	before, err := e.client.Schedules(ctx)
	if err != nil {
		t.Fatal(err)
	}

	asked := time.Now()
	if err := e.client.RunSchedule(ctx, "yearly-report"); err != nil {
		t.Fatal(err)
	}
	taskmill.RunSchedules(ctx, w)

	page, err := e.client.List(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Tasks) != 1 {
		t.Fatalf("got %d tasks, want 1", len(page.Tasks))
	}

	// The run has the schedule's options, and runs at the time of the request.
	task := page.Tasks[0]
	if task.Priority != taskmill.PriorityHigh || task.RunAt.Sub(asked).Abs() > 5*time.Second {
		t.Fatalf("got priority %s and run time %s, want high and about %s", task.Priority, task.RunAt, asked)
	}

	// It is a run of the schedule, for the time of the request.
	if got := task.Metadata[taskmill.MetadataSchedule]; got != "yearly-report" || !task.ScheduledFor().Equal(task.RunAt) {
		t.Fatalf("the run is of schedule %q, for %s; want yearly-report, for %s", got, task.ScheduledFor(), task.RunAt)
	}

	// The timetable stays as it was.
	after, err := e.client.Schedules(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !after[0].NextRunAt.Equal(before[0].NextRunAt) || after[0].LastRunAt.IsZero() {
		t.Fatalf("next run %s, want %s; last run %s", after[0].NextRunAt, before[0].NextRunAt, after[0].LastRunAt)
	}

	if err := e.client.PauseSchedule(ctx, "yearly-report"); err != nil {
		t.Fatal(err)
	}
	if err := e.client.RunSchedule(ctx, "yearly-report"); !errors.Is(err, taskmill.ErrInvalidState) {
		t.Fatalf("paused schedule: got %v, want ErrInvalidState", err)
	}

	if err := e.client.RunSchedule(ctx, "unknown"); !errors.Is(err, taskmill.ErrNotFound) {
		t.Fatalf("unknown schedule: got %v, want ErrNotFound", err)
	}
}

// scheduleDueAt stores at as the time the env's schedule is due next.
// RunSchedule stores the time of the request there, so this also stands for
// a request made at that time.
func (e *env) scheduleDueAt(t *testing.T, at time.Time) {
	t.Helper()
	e.exec(t, "UPDATE {s}.schedules SET next_run_at = $1", at)
}

// runTimes returns the run times of the env's tasks, oldest task first.
func (e *env) runTimes(t *testing.T) []time.Time {
	t.Helper()
	page, err := e.client.List(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}

	var at []time.Time
	for _, task := range slices.Backward(page.Tasks) {
		at = append(at, task.RunAt)
	}
	return at
}

// sameTimes reports whether got holds the times in want, in order.
func sameTimes(got []time.Time, want ...time.Time) bool {
	return slices.EqualFunc(got, want, time.Time.Equal)
}

// newDailySchedule stores a schedule that runs every day at the database's
// current minute. It returns a worker that defines the schedule, and that
// minute of today: the time of a regular run, which has passed. No other
// regular run comes due while the test runs.
func newDailySchedule(t *testing.T, e *env) (*taskmill.Worker, time.Time) {
	t.Helper()
	var slot time.Time
	if err := e.pool.QueryRow(t.Context(), "SELECT date_trunc('minute', now())").Scan(&slot); err != nil {
		t.Fatal(err)
	}
	slot = slot.UTC()

	w := newWorker(t, e, &taskmill.WorkerConfig{Schedules: []taskmill.Schedule{
		{Name: "digest", Cron: fmt.Sprintf("%d %d * * *", slot.Minute(), slot.Hour()), Args: sendEmail{To: "digest"}},
	}})
	taskmill.RunSchedules(t.Context(), w)
	return w, slot
}

func TestRunScheduleJustAfterARegularRunAddsARun(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	w, slot := newDailySchedule(t, e)
	ctx := t.Context()

	// The maintainer enqueues today's regular run.
	e.scheduleDueAt(t, slot)
	taskmill.RunSchedules(ctx, w)

	// An operator asks for a run in the same second.
	request := slot.Add(time.Microsecond)
	e.scheduleDueAt(t, request)
	taskmill.RunSchedules(ctx, w)

	if got := e.runTimes(t); !sameTimes(got, slot, request) {
		t.Fatalf("got runs at %s, want the regular run at %s and the requested run at %s", got, slot, request)
	}
}

func TestRunScheduleJustBeforeARegularRunKeepsThatRun(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	w, slot := newDailySchedule(t, e)
	ctx := t.Context()

	// An operator asked for a run a moment before today's regular run, and
	// the maintainer passes by a moment after it.
	request := slot.Add(-300 * time.Millisecond)
	e.scheduleDueAt(t, request)
	taskmill.RunSchedules(ctx, w)
	taskmill.RunSchedules(ctx, w)

	if got := e.runTimes(t); !sameTimes(got, request, slot) {
		t.Fatalf("got runs at %s, want the requested run at %s and the regular run at %s", got, request, slot)
	}

	// The timetable goes on from there: the next regular run is tomorrow's.
	ss, err := e.client.Schedules(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if want := slot.AddDate(0, 0, 1); !ss[0].NextRunAt.Equal(want) {
		t.Fatalf("the next run is at %s, want %s", ss[0].NextRunAt, want)
	}
}

func TestScheduleCollapsesTheRegularRunsItMissed(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	w := newWorker(t, e, &taskmill.WorkerConfig{Schedules: []taskmill.Schedule{
		{Name: "digest", Cron: "* * * * *", Args: sendEmail{To: "digest"}},
	}})
	ctx := t.Context()
	taskmill.RunSchedules(ctx, w)

	// No maintainer ran for an hour, so 60 regular runs were missed.
	var missed time.Time
	if err := e.pool.QueryRow(ctx, "SELECT date_trunc('minute', now()) - interval '1 hour'").Scan(&missed); err != nil {
		t.Fatal(err)
	}
	e.scheduleDueAt(t, missed)
	taskmill.RunSchedules(ctx, w)

	if got := e.runTimes(t); !sameTimes(got, missed) {
		t.Fatalf("got runs at %s, want one run at %s, the first that was missed", got, missed)
	}

	ss, err := e.client.Schedules(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if next := ss[0].NextRunAt; !next.After(missed.Add(time.Hour)) {
		t.Fatalf("the next run is at %s, among the missed runs: it will run them one by one", next)
	}
}

func TestRunScheduleWhileARegularRunIsDueKeepsThatRun(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	w, slot := newDailySchedule(t, e)
	ctx := t.Context()

	// Today's regular run is due, and the maintainer has not passed by yet.
	// The request must not take its place: that run is the run.
	e.scheduleDueAt(t, slot)
	if err := e.client.RunSchedule(ctx, "digest"); err != nil {
		t.Fatal(err)
	}
	taskmill.RunSchedules(ctx, w)
	taskmill.RunSchedules(ctx, w)

	if got := e.runTimes(t); !sameTimes(got, slot) {
		t.Fatalf("got runs at %s, want the regular run at %s", got, slot)
	}
}

func TestRemovedSchedulesArePruned(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	w := newWorker(t, e, &taskmill.WorkerConfig{
		Schedules: []taskmill.Schedule{{Name: "kept", Cron: "@yearly", Args: sendEmail{}}},
	})
	ctx := t.Context()

	taskmill.RunSchedules(ctx, w)
	for _, name := range []string{"removed-today", "removed-long-ago"} {
		e.exec(t, `INSERT INTO {s}.schedules (name, queue, kind, cron, next_run_at) VALUES ($1, 'default', 'email.send', '@daily', now())`, name)
	}
	e.exec(t, "UPDATE {s}.schedules SET updated_at = now() - interval '2 days' WHERE name IN ('kept', 'removed-long-ago')")

	// A worker that defines no schedules, such as one that only does
	// maintenance, deletes none.
	taskmill.PruneSchedules(ctx, newWorker(t, e, &taskmill.WorkerConfig{}))
	if n := e.count(t, "SELECT count(*) FROM {s}.schedules"); n != 3 {
		t.Fatalf("a maintainer without schedules left %d of 3", n)
	}

	taskmill.PruneSchedules(ctx, w)
	ss, err := e.client.Schedules(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, s := range ss {
		names = append(names, s.Name)
	}
	if !slices.Equal(names, []string{"kept", "removed-today"}) {
		t.Fatalf("got schedules %v", names)
	}
}

// The day counts from when a maintainer last defined the schedule, not from
// the schedule's last run or change. So a schedule that ran and was paused
// long ago keeps both while a worker that does not define it leads, as in a
// rolling deploy, a canary, or a rollback.
func TestScheduleOutlivesAMaintainerThatDoesNotDefineIt(t *testing.T) {
	t.Parallel()
	e := newEnv(t)

	defining := func(name string) *taskmill.Worker {
		return newWorker(t, e, &taskmill.WorkerConfig{
			Schedules: []taskmill.Schedule{{Name: name, Cron: "@weekly", Args: sendEmail{}}},
		})
	}
	with, without := defining("weekly"), defining("other")

	ctx := t.Context()
	taskmill.RunSchedules(ctx, with)
	if err := e.client.PauseSchedule(ctx, "weekly"); err != nil {
		t.Fatal(err)
	}
	e.exec(t, "UPDATE {s}.schedules SET last_run_at = now() - interval '2 days', updated_at = now() - interval '2 days'")

	taskmill.PruneSchedules(ctx, with)
	taskmill.PruneSchedules(ctx, without)
	ss, err := e.client.Schedules(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(ss) != 1 || !ss[0].Paused || ss[0].LastRunAt.IsZero() {
		t.Fatalf("got schedules %+v, want the paused one with its last run", ss)
	}

	// The maintainer that defines it notes so once an hour, not in every
	// pass.
	e.exec(t, "UPDATE {s}.schedules SET updated_at = now() - interval '30 minutes'")
	taskmill.PruneSchedules(ctx, with)

	if n := e.count(t, "SELECT count(*) FROM {s}.schedules WHERE updated_at < now() - interval '29 minutes'"); n != 1 {
		t.Fatal("a pass within the hour wrote the schedule again")
	}
}

// A pass skips a schedule whose row another transaction holds: that one
// runs, pauses, or deletes it. So a pass waits for no row, and two
// maintainers that prune at the same time cannot deadlock.
func TestPruningSchedulesWaitsForNoRow(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	w := newWorker(t, e, &taskmill.WorkerConfig{
		Schedules: []taskmill.Schedule{{Name: "kept", Cron: "@yearly", Args: sendEmail{}}},
	})

	taskmill.RunSchedules(t.Context(), w)
	e.exec(t, `INSERT INTO {s}.schedules (name, queue, kind, cron, next_run_at) VALUES ('removed', 'default', 'email.send', '@daily', now())`)
	e.exec(t, "UPDATE {s}.schedules SET updated_at = now() - interval '2 days'")

	tx := e.begin(t)
	e.txExec(t, tx, "SELECT FROM {s}.schedules WHERE name = 'removed' FOR UPDATE")

	// A pass that waited for the row would not end while the test holds it.
	passed := make(chan struct{})
	go func() {
		defer close(passed)
		taskmill.PruneSchedules(t.Context(), w)
	}()
	recv(t, passed, "the pass to end while a schedule's row is held")

	if n := e.count(t, "SELECT count(*) FROM {s}.schedules WHERE updated_at > now() - interval '1 day'"); n != 1 {
		t.Fatal("the pass did not note the schedule that it defines")
	}
}

func TestScheduledRunKnowsItsTimeOnEveryAttempt(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ctx := t.Context()

	// The schedule runs every day at this minute, on the clock of a shop
	// that is 5 hours 30 minutes ahead of UTC.
	var slot time.Time
	if err := e.pool.QueryRow(ctx, "SELECT date_trunc('minute', now())").Scan(&slot); err != nil {
		t.Fatal(err)
	}
	slot = slot.In(time.FixedZone("shop", (5*60+30)*60))

	// The handler fails, then snoozes, then loses its lease, and then
	// succeeds. Each time, it notes what it knows about its run.
	type seen struct {
		attempt             int
		schedule            string
		scheduledFor, runAt time.Time
	}
	calls := make(chan seen, 4)
	leaseLost := make(chan struct{})
	var called atomic.Int32

	h := func(ctx context.Context, task *taskmill.Task[sendEmail]) error {
		calls <- seen{task.Attempt, task.Metadata[taskmill.MetadataSchedule], task.ScheduledFor(), task.RunAt}

		switch called.Add(1) {
		case 1:
			return errors.New("the database is down")
		case 2:
			return taskmill.Snooze(10 * time.Millisecond)
		case 3:
			select {
			case <-leaseLost:
			case <-ctx.Done():
			}
			return errors.New("too late: the task was rescued")
		}
		return nil
	}

	runWorker(t, e, &taskmill.WorkerConfig{
		Handlers: handlers(h, &taskmill.HandlerOptions{Retry: fastRetry}),
		Queues:   defaultQueue(1),
		Schedules: []taskmill.Schedule{{
			Name: "nightly-report", Cron: fmt.Sprintf("%d %d * * *", slot.Minute(), slot.Hour()), Location: slot.Location(),
			Args: sendEmail{To: "report"},
			// taskmill's own keys win over the same keys of the schedule.
			Options: &taskmill.EnqueueOptions{Metadata: map[string]string{taskmill.MetadataSchedule: "mine", "team": "billing"}},
		}},
	})

	waitFor(t, "the schedule to be stored", func() bool { return e.count(t, "SELECT count(*) FROM {s}.schedules") == 1 })
	e.scheduleDueAt(t, slot)

	var got []seen
	for range 3 {
		got = append(got, recv(t, calls, "an attempt"))
	}
	id := int64(e.count(t, "SELECT id FROM {s}.tasks"))

	// The worker of the third call is lost: the maintainer rescues the task,
	// and records why. The rescue stays on record, while the state that it
	// leaves lasts only until the next attempt. A lease that the worker
	// renews meanwhile is expired again, but only the third call's, which
	// is the task's second attempt.
	waitFor(t, "the rescue", func() bool {
		e.exec(t, "UPDATE {s}.tasks SET lease_expires_at = now() - interval '1 second' WHERE state = 'running' AND attempt = 2")
		return slices.ContainsFunc(e.get(t, id).Errors, func(a taskmill.AttemptError) bool { return a.Code == taskmill.CodeLeaseLost })
	})

	// A rescued task waits the default backoff, 2 seconds here, which says
	// nothing about its times: the test makes it due now.
	e.exec(t, "UPDATE {s}.tasks SET run_at = now() WHERE id = $1 AND state = 'retryable'", id)
	close(leaseLost)
	got = append(got, recv(t, calls, "the attempt after the rescue"))

	for i, call := range got {
		if want := []int{1, 2, 2, 3}[i]; call.attempt != want {
			t.Errorf("call %d is attempt %d, want %d", i+1, call.attempt, want)
		}
		// The time is on the schedule's clock, so its day and hour are those
		// that the expression names.
		if call.schedule != "nightly-report" || call.scheduledFor.Format(time.RFC3339) != slot.Format(time.RFC3339) {
			t.Errorf("call %d is a run of schedule %q for %s, want nightly-report for %s",
				i+1, call.schedule, call.scheduledFor.Format(time.RFC3339), slot.Format(time.RFC3339))
		}
	}

	// RunAt is the time the run is for on the first attempt only.
	if first, last := got[0].runAt, got[3].runAt; !first.Equal(slot) || !last.After(slot) {
		t.Errorf("the run time was %s on the first attempt and %s on the last, want %s and a later time", first, last, slot)
	}

	// An operator sees the same in the finished task.
	task := e.waitState(t, id, taskmill.StateCompleted)
	want := map[string]string{
		taskmill.MetadataSchedule:     "nightly-report",
		taskmill.MetadataScheduledFor: slot.Format(time.RFC3339),
		"team":                        "billing",
	}
	if !maps.Equal(task.Metadata, want) || !task.ScheduledFor().Equal(slot) {
		t.Errorf("the task has the metadata %v, for %s; want %v", task.Metadata, task.ScheduledFor(), want)
	}
}

func TestScheduledRunKeepsItsTimeWhateverEnqueueMiddlewareDoes(t *testing.T) {
	t.Parallel()
	// The first middleware notes the metadata that it gets, and writes over
	// taskmill's keys. The second replaces the whole map, as a middleware
	// that builds its own may do.
	var seen map[string]string
	overwrite := func(ctx context.Context, tasks []*taskmill.EnqueueTask, next func(context.Context) error) error {
		seen = maps.Clone(tasks[0].Metadata)
		tasks[0].Metadata[taskmill.MetadataSchedule] = "mine"
		tasks[0].Metadata[taskmill.MetadataScheduledFor] = "never"
		return next(ctx)
	}
	replace := func(ctx context.Context, tasks []*taskmill.EnqueueTask, next func(context.Context) error) error {
		tasks[0].Metadata = map[string]string{"trace": "abc"}
		return next(ctx)
	}

	e := newEnvWith(t, testdb.Pool(t), &taskmill.ClientConfig{EnqueueMiddleware: []taskmill.EnqueueMiddleware{overwrite, replace}})
	w, slot := newDailySchedule(t, e)
	e.scheduleDueAt(t, slot)
	taskmill.RunSchedules(t.Context(), w)

	page, err := e.client.List(t.Context(), nil)
	if err != nil || len(page.Tasks) != 1 {
		t.Fatalf("got %d tasks, %v; want the scheduled run", len(page.Tasks), err)
	}

	own := map[string]string{
		taskmill.MetadataSchedule:     "digest",
		taskmill.MetadataScheduledFor: slot.Format(time.RFC3339),
	}

	// The middleware sees taskmill's keys, but cannot change or drop them.
	if !maps.Equal(seen, own) {
		t.Errorf("the middleware got the metadata %v, want %v", seen, own)
	}
	stored := maps.Clone(own)
	stored["trace"] = "abc"
	if task := page.Tasks[0]; !maps.Equal(task.Metadata, stored) || !task.ScheduledFor().Equal(slot) {
		t.Errorf("the run has the metadata %v, for %s; want %v", task.Metadata, task.ScheduledFor(), stored)
	}
}

func TestEnqueuedTaskCanStandForAScheduledRun(t *testing.T) {
	t.Parallel()
	e := newEnv(t)

	// A test of a handler sets taskmill's keys itself: Enqueue keeps them.
	id := e.enqueue(t, sendEmail{To: "report"}, &taskmill.EnqueueOptions{Metadata: map[string]string{
		taskmill.MetadataSchedule:     "nightly-report",
		taskmill.MetadataScheduledFor: "2026-11-12T03:00:00+01:00",
	}})
	want := time.Date(2026, 11, 12, 2, 0, 0, 0, time.UTC)
	if got := e.get(t, id).ScheduledFor(); !got.Equal(want) || got.Hour() != 3 {
		t.Fatalf("the task is for %s, want 03:00 at %s", got, want)
	}
}

func TestNewWorkerCountsTheMetadataThatItAddsToAScheduledRun(t *testing.T) {
	t.Parallel()
	e := newEnv(t)

	// This metadata fits the limit of 64 KiB as it is, but not with the two
	// keys of a scheduled run: every run would fail.
	opts := &taskmill.EnqueueOptions{Metadata: map[string]string{"k": strings.Repeat("x", 64<<10-20)}}
	if _, err := e.client.Enqueue(t.Context(), sendEmail{}, opts); err != nil {
		t.Fatalf("the metadata alone: %v", err)
	}

	_, err := taskmill.NewWorker(e.client, &taskmill.WorkerConfig{
		Schedules: []taskmill.Schedule{{Name: "digest", Cron: "@daily", Args: sendEmail{}, Options: opts}},
	})
	if !errors.Is(err, taskmill.ErrInvalidArgument) {
		t.Fatalf("got %v, want ErrInvalidArgument", err)
	}
}

func TestScheduleDoesNotWaitForAHeldGroup(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	w := newWorker(t, e, &taskmill.WorkerConfig{Schedules: []taskmill.Schedule{
		{Name: "ledger", Cron: "@hourly", Args: sendEmail{}, Options: &taskmill.EnqueueOptions{GroupKey: "g"}},
	}})

	// The run gives up on the held group after 50 ms, not after a second.
	taskmill.SetTiming(w, taskmill.Timing{LockWait: 50 * time.Millisecond})

	ctx := t.Context()
	taskmill.RunSchedules(ctx, w)
	e.exec(t, "UPDATE {s}.schedules SET next_run_at = date_trunc('second', now()) - interval '1 second'")
	e.enqueue(t, sendEmail{}, &taskmill.EnqueueOptions{GroupKey: "g"})

	tx := e.begin(t)
	e.txExec(t, tx, "SELECT FROM {s}.groups WHERE group_key = 'g' FOR UPDATE")

	start := time.Now()
	taskmill.RunSchedules(ctx, w)

	if d := time.Since(start); d > 5*time.Second {
		t.Fatalf("the schedule waited %s for the group", d)
	}
	if n := e.count(t, "SELECT count(*) FROM {s}.tasks"); n != 1 {
		t.Fatalf("got %d tasks while the group was held, want 1", n)
	}

	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	taskmill.RunSchedules(ctx, w)
	if n := e.count(t, "SELECT count(*) FROM {s}.tasks"); n != 2 {
		t.Fatalf("got %d tasks after the group was free, want 2", n)
	}
}

func TestMaintainerRunsDueSchedules(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ran := make(chan struct{}, 1)
	runWorker(t, e, &taskmill.WorkerConfig{
		Handlers: handlers(func(context.Context, *taskmill.Task[sendEmail]) error {
			ran <- struct{}{}
			return nil
		}, nil),
		Queues:    defaultQueue(1),
		Schedules: []taskmill.Schedule{{Name: "digest", Cron: "@hourly", Args: sendEmail{To: "digest"}}},
	})

	waitFor(t, "the schedule to be stored", func() bool { return e.count(t, "SELECT count(*) FROM {s}.schedules") == 1 })
	e.exec(t, "UPDATE {s}.schedules SET next_run_at = date_trunc('second', now()) - interval '1 second'")

	select {
	case <-ran:
	case <-time.After(10 * time.Second):
		t.Fatal("the due schedule did not run")
	}
}
