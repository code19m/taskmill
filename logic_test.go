package taskmill

// Tests of logic that needs no database.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"
	"testing/synctest"
	"time"
	"unicode/utf8"
)

func TestHasNUL(t *testing.T) {
	t.Parallel()
	for in, want := range map[string]bool{
		`"a\u0000b"`:    true,
		`"a\\u0000b"`:   false, // an escaped backslash, then text
		`"a\\\u0000"`:   true,  // an escaped backslash, then a NUL
		`"u0000"`:       false,
		`"\u00001"`:     true,
		`{"k":"plain"}`: false,
	} {
		if got := hasNUL([]byte(in)); got != want {
			t.Errorf("hasNUL(%s) = %v, want %v", in, got, want)
		}
	}
}

func TestJSONEqual(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		a, b string
		want bool
	}{
		// What jsonb does to a value leaves it equal.
		{`{"a": 1, "b": [true, null, "x"]}`, `{"b":[true,null,"x"],"a":1}`, true},
		{`-0`, `0`, true},
		{`[1e2, 1.50e1, 1E-2]`, `[100, 15.0, 0.01]`, true},
		{`1e400`, `1` + strings.Repeat("0", 400), true},
		{`"é"`, `"é"`, true},
		// A change is a change, however small.
		{`9007199254740993`, `9007199254740992`, false},
		{`1e400`, `1e401`, false},
		{`1`, `"1"`, false},
		{`[1, 2]`, `[2, 1]`, false},
		{`{"a": null}`, `{}`, false},
		{`{"a": 1}`, `{"b": 1}`, false},
		{`null`, `false`, false},
		{`{"a": [1]}`, `{"a": {"0": 1}}`, false},
		{`{`, `{`, false},
	} {
		if got := jsonEqual([]byte(tt.a), []byte(tt.b)); got != tt.want {
			t.Errorf("jsonEqual(%.20s, %.20s) = %v, want %v", tt.a, tt.b, got, tt.want)
		}
		if got := jsonEqual([]byte(tt.b), []byte(tt.a)); got != tt.want {
			t.Errorf("jsonEqual(%.20s, %.20s) = %v, want %v", tt.b, tt.a, got, tt.want)
		}
	}
}

func TestScheduledFor(t *testing.T) {
	t.Parallel()
	// A run's time comes back as it went into the metadata, to the
	// microsecond, on the clock of the schedule's location.
	at := time.Date(2026, 11, 12, 22, 30, 0, 123_456_000, time.UTC)
	for _, tt := range []struct {
		loc  *time.Location
		want string // the hour and minute that ScheduledFor shows
	}{
		{time.UTC, "22:30"},
		{time.FixedZone("ahead", (5*60+30)*60), "04:00"},
		{time.FixedZone("behind", -8*60*60), "14:30"},
		// RFC 3339 cannot write this offset, so the time stays in UTC.
		{time.FixedZone("seconds", 60*60+17), "22:30"},
	} {
		s := compiledSchedule{Schedule: Schedule{Name: "nightly", Location: tt.loc}}
		info := TaskInfo{Metadata: s.reservedMetadata(s.scheduledFor(at))}
		if got := info.ScheduledFor(); !got.Equal(at) || got.Format("15:04") != tt.want {
			t.Errorf("in %s: got %s, want %s at %s", tt.loc, got.Format(time.RFC3339Nano), at.Format(time.RFC3339Nano), tt.want)
		}
		if got := info.Metadata[MetadataSchedule]; got != "nightly" {
			t.Errorf("in %s: got the schedule %q, want nightly", tt.loc, got)
		}
	}

	// A task that no schedule enqueued has no such time.
	for _, meta := range []map[string]string{nil, {"trace": "abc"}, {MetadataScheduledFor: "tomorrow"}} {
		info := TaskInfo{Metadata: meta}
		if got := info.ScheduledFor(); !got.IsZero() {
			t.Errorf("metadata %v: got %s, want the zero time", meta, got)
		}
	}
}

// parkedArgs is a kind whose default run time and dedupe window no enqueue
// accepts.
type parkedArgs struct{}

func (parkedArgs) Kind() string { return "parked" }

func (parkedArgs) EnqueueDefaults() EnqueueOptions {
	return EnqueueOptions{RunAt: endTime, DedupeWindow: -time.Second}
}

func TestScheduleIsCheckedAsARunEnqueuesIt(t *testing.T) {
	t.Parallel()
	// A run sets its own run time and dedupe window over those of the kind's
	// defaults, so the schedule is valid whatever those are.
	if _, err := compileSchedules([]Schedule{{Name: "nightly", Cron: "@daily", Args: parkedArgs{}}}); err != nil {
		t.Fatalf("got %v, want a valid schedule", err)
	}
}

func TestExponentialBackoff(t *testing.T) {
	t.Parallel()
	b := ExponentialBackoff{}
	for _, tt := range []struct {
		attempt int
		want    time.Duration
	}{
		{1, time.Second},
		{2, 2 * time.Second},
		{10, 512 * time.Second},
		{13, time.Hour}, // 4096 s is capped
		{1000, time.Hour},
		{0, time.Hour}, // out of range: the maximum
	} {
		for range 20 {
			got := b.Backoff(tt.attempt)
			if lo, hi := tt.want*9/10, tt.want*11/10; got < lo || got > hi {
				t.Fatalf("Backoff(%d) = %s, want within 10%% of %s", tt.attempt, got, tt.want)
			}
		}
	}

	custom := ExponentialBackoff{Base: 10 * time.Millisecond, Max: 50 * time.Millisecond}
	if got := custom.Backoff(4); got < 45*time.Millisecond || got > 55*time.Millisecond {
		t.Fatalf("custom Backoff(4) = %s, want about 50ms", got)
	}
}

func TestErrorJSON(t *testing.T) {
	t.Parallel()
	long := strings.Repeat("é", errorMessageLimit) // two bytes each: the cut falls inside a rune
	raw := errorJSON(outcome{msg: "a\x00" + long, code: CodeTimeout})

	var got struct {
		Message string    `json:"message"`
		Code    ErrorCode `json:"code"`
	}
	if err := json.Unmarshal([]byte(raw), &got); err != nil {
		t.Fatal(err)
	}
	if got.Code != CodeTimeout || strings.ContainsRune(got.Message, 0) || !utf8.ValidString(got.Message) ||
		!strings.HasSuffix(got.Message, "…") || len(got.Message) > errorMessageLimit+len("…") {
		t.Fatalf("got code %q and a message of %d bytes ending %q", got.Code, len(got.Message), got.Message[len(got.Message)-8:])
	}

	if raw := errorJSON(outcome{}); raw != `{"message":"unknown error"}` {
		t.Fatalf("no error: got %s", raw)
	}
}

func TestMergeOptions(t *testing.T) {
	t.Parallel()
	base := EnqueueOptions{Queue: "q", Priority: PriorityLow, MaxAttempts: 3, Metadata: map[string]string{"a": "1"}}
	got := mergeOptions(base, EnqueueOptions{Priority: PriorityHigh, Metadata: map[string]string{"b": "2"}})

	if got.Queue != "q" || got.Priority != PriorityHigh || got.MaxAttempts != 3 || len(got.Metadata) != 2 {
		t.Fatalf("got %+v", got)
	}
	if len(base.Metadata) != 1 {
		t.Fatal("merging changed the base's metadata")
	}
}

func TestPermanentOfNil(t *testing.T) {
	t.Parallel()
	if err := Permanent(nil); err != nil {
		t.Fatalf("Permanent(nil) = %v, want nil", err)
	}
}

func TestPriorityString(t *testing.T) {
	t.Parallel()
	if got := PriorityCritical.String() + " " + Priority(9).String(); got != "critical Priority(9)" {
		t.Fatalf("got %q", got)
	}
}

// The rules of a middleware chain, which attempts and enqueues share. A
// broken rule loses a task: its attempt completes although the handler never
// ran, or its enqueue reports a task that nothing stored.
func TestMiddlewareChain(t *testing.T) {
	t.Parallel()
	errTwice, errSkipped, errCall := errors.New("twice"), errors.New("skipped"), errors.New("failed")
	type middleware func(ctx context.Context, trace *[]string, next func(context.Context) error) error

	// see keeps the rules. It records its start, and what its next returned.
	see := func(name string) middleware {
		return func(ctx context.Context, trace *[]string, next func(context.Context) error) error {
			*trace = append(*trace, name)
			err := next(ctx)
			*trace = append(*trace, fmt.Sprintf("%s got %v", name, err))
			return err
		}
	}
	skip := middleware(func(context.Context, *[]string, func(context.Context) error) error { return nil })
	hide := middleware(func(ctx context.Context, _ *[]string, next func(context.Context) error) error {
		_ = next(ctx)
		return nil
	})
	twice := middleware(func(ctx context.Context, _ *[]string, next func(context.Context) error) error {
		_ = next(ctx)
		return next(ctx)
	})

	for _, tt := range []struct {
		name    string
		chain   []middleware
		callErr error
		want    error
		trace   string
	}{
		{"no middleware", nil, errCall, errCall, "call"},
		{"the first middleware is the outermost", []middleware{see("a"), see("b")}, nil, nil, "a, b, call, b got <nil>, a got <nil>"},
		{"the error of call passes out", []middleware{see("a")}, errCall, errCall, "a, call, a got failed"},
		{"a middleware may hide the error of call", []middleware{hide}, errCall, nil, "call"},
		{"a second call of next fails, and call runs once", []middleware{see("a"), twice}, nil, errTwice, "a, call, a got twice"},
		{"the middleware outside sees a skip", []middleware{see("a"), skip}, nil, errSkipped, "a, a got skipped"},
		{"a skip that the middleware outside hides still fails", []middleware{hide, skip}, nil, errSkipped, ""},
	} {
		var trace []string
		err := through(t.Context(), tt.chain, &trace, func(context.Context) error {
			trace = append(trace, "call")
			return tt.callErr
		}, errTwice, errSkipped)
		if got := strings.Join(trace, ", "); !errors.Is(err, tt.want) || got != tt.trace {
			t.Errorf("%s: got %v after %q, want %v after %q", tt.name, err, got, tt.want, tt.trace)
		}
	}
}

type plainArgs struct{}

func (plainArgs) Kind() string { return "plain" }

type routedArgs struct{}

func (*routedArgs) Kind() string { return "routed" }

func (*routedArgs) EnqueueDefaults() EnqueueOptions { return EnqueueOptions{Queue: "routed"} }

func TestWorkerServesTheDefaultQueuesOfItsKinds(t *testing.T) {
	t.Parallel()
	var hs Handlers
	AddHandler(&hs, HandlerFunc[plainArgs](func(context.Context, *Task[plainArgs]) error { return nil }), nil)
	AddHandler(&hs, HandlerFunc[*routedArgs](func(context.Context, *Task[*routedArgs]) error { return nil }), nil)
	cfg := WorkerConfig{Handlers: &hs}

	if err := resolveWorkerConfig(&cfg); err != nil {
		t.Fatal(err)
	}
	want := map[string]QueueOptions{DefaultQueue: {Concurrency: 10}, "routed": {Concurrency: 10}}
	if len(cfg.Queues) != len(want) || cfg.Queues[DefaultQueue] != want[DefaultQueue] || cfg.Queues["routed"] != want["routed"] {
		t.Fatalf("got queues %v, want %v", cfg.Queues, want)
	}
}

// Tests shorten a worker's fixed intervals with SetTiming, so a changed
// default would pass them all. This test holds the defaults that
// docs/reference/defaults-and-limits.md and the doc comments name. One of
// them is a promise about stopping: a worker releases the tasks that ignore
// their context 5 seconds after DrainTimeout, so a process stops in time if
// its platform gives it DrainTimeout plus 10 seconds.
func TestDefaultTiming(t *testing.T) {
	t.Parallel()
	d := defaultTiming
	intervals := []struct {
		name      string
		got, want time.Duration
	}{
		{"maintainer", d.maintainer, time.Second},
		{"heartbeat", d.heartbeat, 5 * time.Second},
		{"election", d.election, 2 * time.Second},
		{"leaderLease", d.leaderLease, 10 * time.Second},
		{"retention", d.retention, time.Minute},
		{"stats", d.stats, 10 * time.Second},
		{"activity", d.activity, 10 * time.Second},
		{"settings", d.settings, 10 * time.Second},
		{"shutdownGrace", d.shutdownGrace, 5 * time.Second},
		{"noHandlerSnooze", d.noHandlerSnooze, time.Minute},
		{"noHandlerWait", d.noHandlerWait, time.Hour},
		{"reconnectMin", d.reconnectMin, time.Second},
		{"reconnectMax", d.reconnectMax, 30 * time.Second},
		{"failureReport", d.failureReport, time.Minute},
		{"idlePoll", d.idlePoll, time.Minute},
		{"quiet", d.quiet, 20 * time.Millisecond},
		{"listen", d.listen, 10 * time.Second},
		{"lockWait", d.lockWait, time.Second},
	}

	for _, tt := range intervals {
		if tt.got != tt.want {
			t.Errorf("the worker's %s interval is %s, want %s: the docs name it, and the tests that shorten it cannot see a change",
				tt.name, tt.got, tt.want)
		}
	}

	if fields := reflect.TypeFor[timing]().NumField(); fields != len(intervals) {
		t.Errorf("timing has %d intervals, and this test holds %d: add the new one", fields, len(intervals))
	}
}

// A task can be due further away than a Duration holds, and in a row that
// taskmill did not write, at no time at all. Converted first and limited
// after, as before, such a number gave the shortest sleep on amd64, so the
// maintainer looked for due tasks 20 times a second, for good.
func TestSleepUntilDueLimitsTheSecondsFirst(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		secs float64
		want time.Duration
	}{
		{0.25, 250 * time.Millisecond},
		{0.001, minMaintainerSleep},
		{-3600, minMaintainerSleep}, // a due task that the last promotion left, such as a locked one
		{math.Inf(-1), minMaintainerSleep},
		{30, time.Second},
		{1e13, time.Second},        // about 300,000 years: RunAt may be in the year 294276
		{math.Inf(1), time.Second}, // PostgreSQL 17 and later subtract infinite times
	} {
		if got := sleepUntilDue(tt.secs, time.Second); got != tt.want {
			t.Errorf("sleepUntilDue(%g) = %s, want %s", tt.secs, got, tt.want)
		}
	}
}

// An election does not end when its worker is told to stop, so that the
// worker knows the answer before it resigns. It has the lease's time while
// the worker runs, a minute here, and the clean-up's time, 5 seconds here,
// once the worker is told to stop.
func TestElectionOutlivesItsWorkersStopForABoundedTime(t *testing.T) {
	t.Parallel()
	type kept struct{}
	for _, tt := range []struct {
		name       string
		stopsAfter time.Duration // 0: the worker runs on
		wantEnd    time.Duration
	}{
		{"the worker runs on", 0, time.Minute},
		{"the worker stops", 10 * time.Second, 15 * time.Second},
		{"the worker stops just before the election's time is up", 58 * time.Second, time.Minute},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			// In the bubble, the clock moves only while the test waits, so
			// it measures the election's time exactly.
			synctest.Test(t, func(t *testing.T) {
				worker, stop := context.WithCancel(context.WithValue(t.Context(), kept{}, "a value"))
				defer stop()
				election, cancel := outlive(worker, time.Minute, 5*time.Second)
				defer cancel()

				start := time.Now()
				if tt.stopsAfter > 0 {
					time.AfterFunc(tt.stopsAfter, stop)
				}

				<-election.Done()
				if got := time.Since(start); got != tt.wantEnd {
					t.Errorf("the election ended after %s, want %s", got, tt.wantEnd)
				}
				if election.Value(kept{}) != "a value" {
					t.Error("the election lost its worker's values")
				}
			})
		})
	}
}

func TestWhileLeased(t *testing.T) {
	t.Parallel()
	// In the bubble, the clock moves only while whileLeased waits for its
	// next try, so the test measures its waits exactly.
	synctest.Test(t, func(t *testing.T) {
		w := &Worker{cfg: WorkerConfig{LeaseDuration: 400 * time.Millisecond}, logger: slog.New(slog.DiscardHandler)}
		w.resultFailures = newIncidentLog(w, "record results")
		ts := []*liveAttempt{{renewedAt: time.Now()}}

		calls := 0
		err := w.whileLeased(t.Context(), ts, func(context.Context) error {
			if calls++; calls < 3 {
				return errors.New("connection refused")
			}
			return nil
		})
		if err != nil || calls != 3 {
			t.Fatalf("got %v after %d tries, want success on the third", err, calls)
		}

		// Renewals keep extending the lease while the tries go on; an error
		// that persists still ends them. Tries that followed the renewals
		// would never end: the tenth succeeds, which fails the test.
		start, calls := time.Now(), 0
		ts = []*liveAttempt{{renewedAt: start}}
		err = w.whileLeased(t.Context(), ts, func(context.Context) error {
			if calls++; calls == 10 {
				return nil
			}
			ts[0].mu.Lock()
			ts[0].renewedAt = time.Now()
			ts[0].mu.Unlock()
			return errors.New("connection refused")
		})
		if err == nil || time.Since(start) > w.cfg.LeaseDuration || calls < 2 {
			t.Fatalf("got %v after %d tries in %s; it should give up once the lease may have expired",
				err, calls, time.Since(start))
		}
	})
}

func TestFailuresEndAnIncidentAfterAQuietInterval(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		every := time.Second
		f, l := newFailureLines(every)
		ctx, boom := t.Context(), errors.New("timeout")

		f.observe(ctx, boom)
		f.observe(ctx, nil)

		// A failure soon after the recovery belongs to the same incident, and
		// waits for the next line.
		f.observe(ctx, boom)
		f.observe(ctx, nil)

		// Much later, a failure reports that incident's recovery, then starts
		// another.
		time.Sleep(2 * every)
		f.observe(ctx, boom)

		want := []string{
			"WARN operation failed", "INFO operation recovered failures=1",
			"INFO operation recovered failures=2", "WARN operation failed",
		}
		if !slices.Equal(l.lines, want) {
			t.Errorf("got lines %q, want %q", l.lines, want)
		}
	})
}

func TestFailuresOfWorkThatRunsNowAndThen(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		every := time.Second
		for _, c := range []struct {
			pause time.Duration
			want  []string
		}{
			// A job that fails at every run, a little over an interval apart,
			// is one incident that escalates.
			{3 * every / 2, []string{"WARN operation failed", "ERROR operation still failing failures=2"}},
			// Failures two intervals apart, with no try between, are incidents
			// of their own.
			{3 * every, []string{"WARN operation failed", "WARN operation failed"}},
		} {
			f, l := newFailureLines(every)
			f.observe(t.Context(), errors.New("timeout"))
			time.Sleep(c.pause)
			f.observe(t.Context(), errors.New("timeout"))

			if !slices.Equal(l.lines, c.want) {
				t.Errorf("failures %s apart: got lines %q, want %q", c.pause, l.lines, c.want)
			}
		}
	})
}

func TestFailuresOfAFlappingOperationEscalateAndStayRare(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		every := time.Second
		f, l := newFailureLines(every)
		ctx, boom := t.Context(), errors.New("timeout")

		// The operation fails at every other try for ten report intervals,
		// then works again.
		elapsed, tries := 10*every, 0
		for ; tries < int(elapsed/(20*time.Millisecond)); tries++ {
			f.observe(ctx, boom)
			f.observe(ctx, nil)
			time.Sleep(20 * time.Millisecond)
		}
		time.Sleep(every)
		f.observe(ctx, nil)

		if limit := 2 * (1 + int(elapsed/every)); len(l.lines) > limit {
			t.Errorf("%d tries logged %d lines, want at most %d: %q", tries, len(l.lines), limit, l.lines)
		}

		// It is one incident: a warning, errors as it lasts, and a last line
		// that counts every failure.
		escalated := false
		for i, line := range l.lines {
			switch {
			case i == 0 && line != "WARN operation failed":
				t.Errorf("the first line is %q, want a warning", line)
			case i > 0 && strings.HasPrefix(line, "WARN"):
				t.Errorf("line %d warns again: %q", i, line)
			case strings.HasPrefix(line, "ERROR operation still failing"):
				escalated = true
			}
		}
		if !escalated {
			t.Errorf("failures for %s logged no error: %q", elapsed, l.lines)
		}
		if last, want := l.lines[len(l.lines)-1], "INFO operation recovered failures="+strconv.Itoa(tries); last != want {
			t.Errorf("the last line is %q, want %q", last, want)
		}
	})
}

// A notifier forgets a queue once its window has passed, so that a client
// that enqueues into ever new queues, such as one for each tenant, does not
// keep an entry for every queue it ever notified.
func TestNotifierForgetsQueuesAfterTheirWindow(t *testing.T) {
	t.Parallel()
	// In the bubble, the clock moves only when the test sleeps.
	synctest.Test(t, func(t *testing.T) {
		n := newNotifier(nil, "taskmill", slog.New(slog.DiscardHandler))
		const queues = 1000

		for _, window := range []string{"first", "second", "third"} {
			for i := range queues {
				if queue := window + "-" + strconv.Itoa(i); !n.allow(t.Context(), queue) {
					t.Fatalf("the first notification of queue %s was held back", queue)
				}
			}
			time.Sleep(notifyWindow)
		}

		n.mu.Lock()
		defer n.mu.Unlock()
		if kept := len(n.last); kept > 2*queues {
			t.Fatalf("after 3 windows of %d new queues each, the notifier keeps %d queues", queues, kept)
		}
	})
}

// newFailureLines returns the failures of an operation with a report
// interval of every, and the lines it logs. Its tests run in a synctest
// bubble, where the clock moves only when they sleep.
func newFailureLines(every time.Duration) (*incidentLog, *lineLog) {
	l := &lineLog{}
	w := &Worker{logger: slog.New(l), timing: timing{failureReport: every}}
	return newIncidentLog(w, "fetch tasks"), l
}

// lineLog is a slog handler that keeps each line's level, message, and
// failure count, for tests that log from one goroutine.
type lineLog struct {
	lines []string
}

func (*lineLog) Enabled(context.Context, slog.Level) bool { return true }

func (l *lineLog) Handle(_ context.Context, r slog.Record) error {
	line := r.Level.String() + " " + r.Message
	r.Attrs(func(a slog.Attr) bool {
		if a.Key == logKeyFailures {
			line += " failures=" + a.Value.String()
		}
		return true
	})

	l.lines = append(l.lines, line)
	return nil
}

func (l *lineLog) WithAttrs([]slog.Attr) slog.Handler { return l }

func (l *lineLog) WithGroup(string) slog.Handler { return l }
