package taskmill_test

import (
	"context"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/code19m/taskmill"
)

// These tests cover how the maintainer compares the schedules of the
// workers that can become the maintainer with its own, and logs a
// difference that lasts.

// compareTiming is testTiming with a comparison of the workers' schedules
// every 50 ms, which logs a difference once it has lasted 300 ms.
var compareTiming = func() taskmill.Timing {
	t := testTiming
	t.ScheduleCheck, t.ScheduleGrace = 50*time.Millisecond, 300*time.Millisecond
	return t
}()

// The two lines that the maintainer logs about the workers' schedules.
const (
	schedulesDiffer = "the schedules of other workers differ from the maintainer's"
	schedulesMatch  = "the schedules of other workers match the maintainer's again"
)

// digest is a schedule that the maintainers of these tests do not define.
var digest = []taskmill.Schedule{{Name: "digest", Cron: "@daily", Args: sendEmail{}}}

// runCompared runs a worker of cfg on e with timing, and returns its ID once
// it is registered, and a function that stops it.
func runCompared(t *testing.T, e *env, timing taskmill.Timing, cfg *taskmill.WorkerConfig) (id string, stop func()) {
	t.Helper()
	known := e.workers(t)

	w := newWorker(t, e, cfg)
	taskmill.SetTiming(w, timing)
	stop = startWorker(t, w)
	waitFor(t, "the worker to register", func() bool {
		for _, w := range e.workers(t) {
			if _, ok := known[w.ID]; !ok {
				id = w.ID
			}
		}
		return id != ""
	})
	return id, stop
}

// runMaintainer runs a worker of cfg as runCompared does, and waits until it
// is the maintainer.
func runMaintainer(t *testing.T, e *env, timing taskmill.Timing, cfg *taskmill.WorkerConfig) string {
	t.Helper()
	id, _ := runCompared(t, e, timing, cfg)
	waitFor(t, "the worker to lead", func() bool { return e.workers(t)[id].Maintainer })
	return id
}

// workers returns the registered workers, by ID.
func (e *env) workers(t *testing.T) map[string]taskmill.WorkerInfo {
	t.Helper()
	ws, err := e.client.Workers(t.Context())
	if err != nil {
		t.Fatal(err)
	}

	byID := make(map[string]taskmill.WorkerInfo, len(ws))
	for _, w := range ws {
		byID[w.ID] = w
	}
	return byID
}

// differLines returns the errors about the workers' schedules that logs
// recorded, in order.
func differLines(logs *logRecorder) []slog.Record {
	return slices.DeleteFunc(logs.all(), func(r slog.Record) bool { return r.Message != schedulesDiffer })
}

// lasted returns how long the difference of line r had lasted.
func lasted(t *testing.T, r slog.Record) time.Duration {
	t.Helper()
	d, err := time.ParseDuration(attr(r, "duration"))
	if err != nil {
		t.Fatalf("the line %q: %v", recordText(r), err)
	}
	return d
}

// sendEmailReordered is sendEmail with its fields in the other order.
type sendEmailReordered struct {
	UserID int64  `json:"user_id"`
	To     string `json:"to"`
}

func (sendEmailReordered) Kind() string { return "email.send" }

// Every worker that takes part in the maintainer election stores its
// schedules: their names, which Workers lists, and a hash of each one's
// definition. The maintainer compares them with its own, and once they have
// differed for a while it logs an error that names the workers and the
// schedules, and a line when they match again. A worker that takes no part,
// and one of a release that stores no schedules, are not compared.
func TestMaintainerLogsWorkersWhoseSchedulesDiffer(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	logs := &logRecorder{}
	ours := []taskmill.Schedule{
		{Name: "nightly", Cron: "0 3 * * *", Args: sendEmail{To: "ops", UserID: 1}},
		{Name: "carts", Cron: "@hourly", Args: sendEmail{}},
	}
	maintainer := runMaintainer(t, e.withLogger(t, slog.New(logs)), compareTiming, &taskmill.WorkerConfig{Schedules: ours})

	// The same schedules, listed in another order, with the payload's keys
	// in another order too.
	same, _ := runCompared(t, e, compareTiming, &taskmill.WorkerConfig{Schedules: []taskmill.Schedule{
		ours[1], {Name: "nightly", Cron: "0 3 * * *", Args: sendEmailReordered{UserID: 1, To: "ops"}},
	}})
	none, _ := runCompared(t, e, compareTiming, &taskmill.WorkerConfig{Schedules: ours[:1], NoMaintainer: true})
	old := e.oldWorker(t)
	other, stopOther := runCompared(t, e, compareTiming, &taskmill.WorkerConfig{Schedules: []taskmill.Schedule{
		{Name: "nightly", Cron: "0 4 * * *", Args: sendEmail{To: "ops", UserID: 1}},
		{Name: "digest", Cron: "@daily", Args: sendEmail{}},
	}})

	waitFor(t, "the error", func() bool { return len(differLines(logs)) == 1 })
	r := differLines(logs)[0]
	for key, want := range map[string][]string{
		"workers": {other}, "missing_here": {"digest"}, "missing_there": {"carts"}, "defined_differently": {"nightly"},
	} {
		if got := attr(r, key); got != fmt.Sprint(want) {
			t.Errorf("the error's %s is %s, want %v", key, got, want)
		}
	}
	if r.Level != slog.LevelError || lasted(t, r) < compareTiming.ScheduleGrace {
		t.Errorf("the error is %s, after %s; want an error after %s", r.Level, attr(r, "duration"), compareTiming.ScheduleGrace)
	}

	workers := e.workers(t)
	for id, want := range map[string][]string{
		maintainer: {"carts", "nightly"}, same: {"carts", "nightly"}, other: {"digest", "nightly"}, none: nil, old: nil,
	} {
		if got := workers[id].Schedules; !slices.Equal(got, want) || (got == nil) != (want == nil) {
			t.Errorf("the worker %s lists the schedules %#v, want %#v", id, got, want)
		}
	}

	stopOther()
	waitFor(t, "the line that they match again", func() bool { return logs.count(schedulesMatch) == 1 })
	if n := len(differLines(logs)); n != 1 {
		t.Errorf("the error was logged %d times within the hour, want once", n)
	}
}

// A maintainer that defines no schedules runs none: the worst case. It
// compares too, and names the schedules that the other workers define.
func TestMaintainerWithoutSchedulesLogsTheWorkersThatDefineSome(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	logs := &logRecorder{}
	maintainer := runMaintainer(t, e.withLogger(t, slog.New(logs)), compareTiming, &taskmill.WorkerConfig{})
	other, _ := runCompared(t, e, compareTiming, &taskmill.WorkerConfig{Schedules: digest})

	waitFor(t, "the error", func() bool { return len(differLines(logs)) == 1 })
	r := differLines(logs)[0]
	if got := recordText(r); got != schedulesDiffer+" workers=["+other+"] missing_here=[digest] missing_there=[] "+
		"defined_differently=[] duration="+attr(r, "duration") {
		t.Errorf("got %q", got)
	}

	// It stores its schedules as an empty list, not as none.
	if got := e.workers(t)[maintainer].Schedules; got == nil || len(got) != 0 {
		t.Errorf("the maintainer lists the schedules %#v, want an empty list", got)
	}
}

// A rolling deploy that changes a schedule runs both definitions for a few
// minutes. A difference that ends before the maintainer logs it leaves no
// line at all.
func TestMaintainerLogsNothingForADifferenceThatEndsSoon(t *testing.T) {
	t.Parallel()
	sent := &statementLog{}
	e := newTracedEnv(t, sent)
	logs := &logRecorder{}
	timing := compareTiming
	timing.ScheduleGrace = time.Hour
	runMaintainer(t, e.withLogger(t, slog.New(logs)), timing, &taskmill.WorkerConfig{})
	compared := func(what string) {
		sent.reset()
		waitFor(t, what, func() bool { return sent.count("schedule_hashes <>") >= 2 })
	}

	_, stop := runCompared(t, e, compareTiming, &taskmill.WorkerConfig{Schedules: digest})
	compared("the maintainer to compare the new worker")
	stop()
	compared("the maintainer to compare without it")

	for _, r := range logs.all() {
		if r.Message == schedulesDiffer || r.Message == schedulesMatch {
			t.Errorf("logged %q", recordText(r))
		}
	}
}

// While the schedules differ, the maintainer logs the error again once per
// report interval, with how long they have differed.
func TestMaintainerLogsALastingDifferenceAgain(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	logs := &logRecorder{}
	timing := compareTiming
	timing.ScheduleReport = 400 * time.Millisecond
	runMaintainer(t, e.withLogger(t, slog.New(logs)), timing, &taskmill.WorkerConfig{})
	runCompared(t, e, compareTiming, &taskmill.WorkerConfig{Schedules: digest})

	waitFor(t, "the error again", func() bool { return len(differLines(logs)) >= 2 })
	lines := differLines(logs)
	if gap := lasted(t, lines[1]) - lasted(t, lines[0]); gap < timing.ScheduleReport {
		t.Errorf("the error came again %s after the first, want at least %s", gap, timing.ScheduleReport)
	}
}

// A worker that stops leaves the election at once, but it checks in while
// it drains its tasks, which can take long: in a slow rolling deploy that
// changes a schedule, its schedules would count as a difference. So it
// clears them as it leaves.
func TestStoppingWorkerTakesItsSchedulesOutOfTheComparison(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	started, release := make(chan struct{}, 1), make(chan struct{})
	w := newWorker(t, e, &taskmill.WorkerConfig{
		Handlers: handlers(func(context.Context, *taskmill.Task[sendEmail]) error {
			started <- struct{}{}
			<-release
			return nil
		}, nil),
		Queues:    defaultQueue(1),
		Schedules: digest,
	})
	ctx, stop := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	finish := sync.OnceValue(func() error {
		stop()
		close(release)
		return <-done
	})
	t.Cleanup(func() { _ = finish() })

	e.enqueue(t, sendEmail{To: "slow"}, nil)
	recv(t, started, "the task to start")
	workers := slices.Collect(maps.Values(e.workers(t)))
	if len(workers) != 1 || !slices.Equal(workers[0].Schedules, []string{"digest"}) {
		t.Fatalf("the running worker lists the schedules %v", workers)
	}
	id := workers[0].ID

	stop()
	waitFor(t, "the draining worker to clear its schedules", func() bool {
		w, ok := e.workers(t)[id]
		return ok && w.Schedules == nil
	})
	if err := finish(); err != nil {
		t.Fatal(err)
	}
}

// Each term of a maintainer compares anew: a difference that it found in an
// earlier term counts from the new term's first comparison, and logs its
// error again, and one that ended between the terms logs no end.
func TestMaintainerComparesAnewInEachTerm(t *testing.T) {
	t.Parallel()
	sent := &statementLog{}
	e := newTracedEnv(t, sent)
	logs := &logRecorder{}
	maintainer := runMaintainer(t, e.withLogger(t, slog.New(logs)), compareTiming, &taskmill.WorkerConfig{})
	_, stopOther := runCompared(t, e, compareTiming, &taskmill.WorkerConfig{Schedules: digest})
	waitFor(t, "the error", func() bool { return len(differLines(logs)) == 1 })

	// newTerm ends the maintainer's term: a lease that is not its own, but
	// lasts, takes its place. Then the maintainer gets the lease back, which
	// starts a new term, and no other worker can take it meanwhile.
	terms := 1
	newTerm := func(meanwhile func()) (began time.Time) {
		t.Helper()
		began = time.Now()
		e.exec(t, "UPDATE {s}.leader SET worker_id = 'nobody', expires_at = now() + interval '3 seconds'")
		waitFor(t, "the term to end", func() bool { return logs.count("no longer maintainer") == terms })
		meanwhile()

		e.exec(t, "UPDATE {s}.leader SET worker_id = $1, expires_at = now() + interval '3 seconds'", maintainer)
		terms++
		waitFor(t, "a new term", func() bool { return logs.count("elected maintainer") == terms })
		return began
	}

	// The difference lasts across the terms: the new term counts it from its
	// own first comparison, and logs its error again.
	began := newTerm(func() {})
	waitFor(t, "the error of the new term", func() bool { return len(differLines(logs)) == 2 })
	if d := lasted(t, differLines(logs)[1]); d > time.Since(began) {
		t.Errorf("the new term's error says that the difference lasted %s, from before the term", d)
	}

	// The difference ends between the terms: the new term finds none, and
	// has no end to log.
	newTerm(stopOther)
	sent.reset()
	waitFor(t, "two comparisons in the new term", func() bool { return sent.count("schedule_hashes <>") >= 2 })
	if n := logs.count(schedulesMatch); n != 0 {
		t.Errorf("a new term logged the end of a difference that an earlier term found")
	}
}
