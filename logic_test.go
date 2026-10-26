package taskmill

// Tests of logic that needs no database.

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"slices"
	"strconv"
	"strings"
	"testing"
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
	raw := errorJSON(outcome{err: errors.New("a\x00" + long), code: CodeTimeout})
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

func TestListBounds(t *testing.T) {
	t.Parallel()
	limit, before, err := listBounds(&ListParams{})
	if err != nil || limit != defaultListLimit || before <= 0 {
		t.Fatalf("defaults: got %d, %d, %v", limit, before, err)
	}
	for _, p := range []ListParams{{Limit: -1}, {Limit: maxListLimit + 1}, {Cursor: "%%%"}, {Cursor: "eA"}} {
		if _, _, err := listBounds(&p); !errors.Is(err, ErrInvalidArgument) {
			t.Errorf("%+v: got %v, want ErrInvalidArgument", p, err)
		}
	}
	if _, before, err := listBounds(&ListParams{Cursor: "NDI"}); err != nil || before != 42 { // "42"
		t.Fatalf("cursor: got %d, %v", before, err)
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

func TestWhileLeased(t *testing.T) {
	t.Parallel()
	w := &Worker{cfg: WorkerConfig{LeaseDuration: 400 * time.Millisecond}, logger: slog.New(slog.DiscardHandler)}
	w.resultFailures = newFailures(w, "record results")
	ts := []*tracked{{lastBeat: time.Now()}}

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

	// Heartbeats keep renewing the lease while the tries go on; an error
	// that persists still ends them.
	start, calls := time.Now(), 0
	ts = []*tracked{{lastBeat: start}}
	err = w.whileLeased(t.Context(), ts, func(context.Context) error {
		calls++
		ts[0].mu.Lock()
		ts[0].lastBeat = time.Now()
		ts[0].mu.Unlock()
		return errors.New("connection refused")
	})
	if err == nil || time.Since(start) > time.Second || calls < 2 {
		t.Fatalf("got %v after %d tries in %s; it should give up once the lease may have expired",
			err, calls, time.Since(start))
	}
}

func TestFailuresEndAnIncidentAfterAQuietInterval(t *testing.T) {
	t.Parallel()
	every := 100 * time.Millisecond
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
}

func TestFailuresOfWorkThatRunsNowAndThen(t *testing.T) {
	t.Parallel()
	every := 200 * time.Millisecond
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
}

func TestFailuresOfAFlappingOperationEscalateAndStayRare(t *testing.T) {
	t.Parallel()
	every := 50 * time.Millisecond
	f, l := newFailureLines(every)
	ctx, boom := t.Context(), errors.New("timeout")

	// The operation fails at every other try for ten report intervals, then
	// works again.
	start, tries := time.Now(), 0
	for ; time.Since(start) < 10*every; tries++ {
		f.observe(ctx, boom)
		f.observe(ctx, nil)
		time.Sleep(time.Millisecond)
	}
	elapsed := time.Since(start)
	time.Sleep(every)
	f.observe(ctx, nil)

	if limit := 2 * (1 + int(elapsed/every)); len(l.lines) > limit {
		t.Errorf("%d tries logged %d lines, want at most %d: %q", tries, len(l.lines), limit, l.lines)
	}
	// It is one incident: a warning, errors as it lasts, and a last line that
	// counts every failure.
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
}

// newFailureLines returns the failures of an operation with a report
// interval of every, and the lines it logs.
func newFailureLines(every time.Duration) (*failures, *lineLog) {
	l := &lineLog{}
	w := &Worker{logger: slog.New(l), timing: timing{failureReport: every}}
	return newFailures(w, "fetch tasks"), l
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
