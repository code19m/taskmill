package taskmill_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/code19m/taskmill"
)

func TestWorkerRetriesUntilSuccess(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	var calls atomic.Int32
	runWorker(t, e, &taskmill.WorkerConfig{
		Handlers: handlers(func(context.Context, *taskmill.Task[sendEmail]) error {
			if calls.Add(1) < 3 {
				return errors.New("smtp timeout")
			}
			return nil
		}, &taskmill.HandlerOptions{Retry: fastRetry}),
		Queues: defaultQueue(1),
	})

	id := e.enqueue(t, sendEmail{}, nil)
	info := e.waitState(t, id, taskmill.StateCompleted)
	if info.Attempt != 3 || len(info.Errors) != 2 || info.Errors[1].Message != "smtp timeout" || info.Errors[1].Attempt != 2 {
		t.Fatalf("got attempt %d, errors %+v", info.Attempt, info.Errors)
	}
}

func TestWorkerMakesTaskDeadAfterLastAttempt(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	runWorker(t, e, &taskmill.WorkerConfig{
		Handlers: handlers(func(context.Context, *taskmill.Task[sendEmail]) error {
			return errors.New("always fails")
		}, &taskmill.HandlerOptions{Retry: fastRetry}),
		Queues: defaultQueue(1),
	})

	id := e.enqueue(t, sendEmail{}, &taskmill.EnqueueOptions{MaxAttempts: 2})
	info := e.waitState(t, id, taskmill.StateDead)
	if info.Attempt != 2 || len(info.Errors) != 2 {
		t.Fatalf("got attempt %d, %d errors", info.Attempt, len(info.Errors))
	}
}

func TestWorkerFailureKinds(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	runWorker(t, e, &taskmill.WorkerConfig{
		Handlers: handlers(func(ctx context.Context, task *taskmill.Task[sendEmail]) error {
			switch task.Args.To {
			case "permanent":
				return taskmill.Permanent(errors.New("bad address"))
			case "panic":
				panic("boom")
			case "panic with a value that panics":
				panic(&stubbornError{})
			case "timeout":
				<-ctx.Done()
				return ctx.Err()
			}
			return nil
		}, &taskmill.HandlerOptions{Timeout: 100 * time.Millisecond, Retry: fastRetry}),
		Queues: defaultQueue(4),
	})

	tests := []struct {
		to       string
		attempts int
		code     taskmill.ErrorCode
		message  string // part of the first error's message
	}{
		{"permanent", 1, "", "bad address"},
		{"panic", 2, taskmill.CodePanic, "panic: boom"},
		{"panic with a value that panics", 2, taskmill.CodePanic, "stubbornError"},
		{"timeout", 2, taskmill.CodeTimeout, "context deadline exceeded"},
	}

	for _, tt := range tests {
		t.Run(tt.to, func(t *testing.T) {
			t.Parallel()
			id := e.enqueue(t, sendEmail{To: tt.to}, &taskmill.EnqueueOptions{MaxAttempts: 2})
			info := e.waitState(t, id, taskmill.StateDead)
			if info.Attempt != tt.attempts || len(info.Errors) != tt.attempts || info.Errors[0].Code != tt.code ||
				!strings.Contains(info.Errors[0].Message, tt.message) {
				t.Errorf("got attempt %d, errors %+v", info.Attempt, info.Errors)
			}
		})
	}
}

// stubbornError is an error whose Error method panics with the error itself,
// so that the panic's value panics again when it is formatted.
type stubbornError struct{}

func (e *stubbornError) Error() string { panic(e) }

// apiError is an error type as applications write them. A function that
// returns a nil *apiError as an error returns an error that is not nil, and
// whose Error method panics: a well-known Go mistake.
type apiError struct{ status int }

func (e *apiError) Error() string { return "status " + strconv.Itoa(e.status) }

// callAPI makes that mistake, except for the address "up".
func callAPI(to string) error {
	var failed *apiError
	if to == "up" {
		return nil
	}
	return failed
}

// apiBatch is a batch handler that calls the API for each task.
type apiBatch struct{}

func (apiBatch) HandleBatch(_ context.Context, tasks []*taskmill.Task[sendEmail]) []error {
	errs := make([]error, len(tasks))
	for i, task := range tasks {
		errs[i] = callAPI(task.Args.To)
	}
	return errs
}

// tableBackoff is a retry policy with a table of delays. It panics for an
// attempt beyond its table.
type tableBackoff []time.Duration

func (b tableBackoff) Backoff(attempt int) time.Duration { return b[attempt-1] }

// taskmill runs code of the application after a handler returned: the
// methods of the handler's error, and the retry policy. A panic there fails
// the attempt as a handler's panic does, and the worker goes on.
func TestPanicAfterTheHandlerReturnedFailsTheAttempt(t *testing.T) {
	t.Parallel()
	var batch taskmill.Handlers
	taskmill.AddBatchHandler[sendEmail](&batch, apiBatch{}, &taskmill.HandlerOptions{Retry: fastRetry})

	tests := map[string]struct {
		handlers *taskmill.Handlers
		where    string // the function that panics, as the recorded stack names it
	}{
		"the error's method": {
			handlers(func(_ context.Context, task *taskmill.Task[sendEmail]) error {
				return callAPI(task.Args.To)
			}, &taskmill.HandlerOptions{Retry: fastRetry}),
			"(*apiError).Error",
		},
		"the error's method, from a batch": {&batch, "(*apiError).Error"},
		"the error's method, with a value that panics again": {
			handlers(func(_ context.Context, task *taskmill.Task[sendEmail]) error {
				if task.Args.To == "up" {
					return nil
				}
				return &stubbornError{}
			}, &taskmill.HandlerOptions{Retry: fastRetry}),
			"(*stubbornError).Error",
		},
		"the retry policy": {
			handlers(func(_ context.Context, task *taskmill.Task[sendEmail]) error {
				if task.Args.To == "up" {
					return nil
				}
				return errors.New("the partner is down")
			}, &taskmill.HandlerOptions{Retry: tableBackoff{}}),
			"tableBackoff.Backoff",
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			e := newEnv(t)
			runWorker(t, e, &taskmill.WorkerConfig{Handlers: tt.handlers, Queues: defaultQueue(1)})

			// The first attempt is recorded as a panic, and the task is retried
			// after the default backoff of about a second.
			id := e.enqueue(t, sendEmail{}, &taskmill.EnqueueOptions{MaxAttempts: 2})
			info := e.waitState(t, id, taskmill.StateDead)
			first := info.Errors[0]
			if info.Attempt != 2 || first.Attempt != 1 || first.Code != taskmill.CodePanic || !strings.Contains(first.Message, tt.where) {
				t.Fatalf("got attempt %d with errors %+v, want a first error with the code panic that names %s",
					info.Attempt, info.Errors, tt.where)
			}

			e.waitState(t, e.enqueue(t, sendEmail{To: "up"}, nil), taskmill.StateCompleted)
		})
	}
}

// cardArgs is a payload whose decoding fails with an error that quotes it.
type cardArgs struct{ Card string }

func (cardArgs) Kind() string { return "card.charge" }

func (*cardArgs) UnmarshalJSON(b []byte) error { return fmt.Errorf("not a card: %s", b) }

// scoreArgs is a payload with a map, whose keys are values of the payload,
// and a list of structs.
type scoreArgs struct {
	Scores map[string]int `json:"scores"`
	Items  []struct {
		Qty int `json:"qty"`
	} `json:"items"`
}

func (scoreArgs) Kind() string { return "score.add" }

// A payload that does not decode makes its task dead right away. The error
// that says why is stored, logged, and set as a span's status, where a
// payload must not appear: it names the field and the types, not the value
// that encoding/json, or the application's UnmarshalJSON, quoted.
func TestWorkerRejectsUndecodablePayload(t *testing.T) {
	t.Parallel()
	for _, c := range []struct {
		name, kind, payload, says string
	}{
		{"wrong type", "email.send", `{"to": 4111111111111111}`, "the field to holds a JSON number, which does not fit string"},
		{"number out of range", "email.send", `{"user_id": 41111111111111111111}`, "the field user_id holds a JSON number, which does not fit int64"},
		{"rejected by the type", "card.charge", `{"Card": "4111111111111111"}`, "it does not decode into taskmill_test.cardArgs"},
		{"wrong type under a key", "score.add", `{"scores": {"4111@example.com": "x"}}`, "the field scores[key] holds a JSON string, which does not fit int"},
		{"wrong type in a list", "score.add", `{"items": [{"qty": "4111"}]}`, "the field items.qty holds a JSON string, which does not fit int"},
	} {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			logs := &logRecorder{}
			e := newEnv(t).withLogger(t, slog.New(logs))
			hs := handlers(func(context.Context, *taskmill.Task[sendEmail]) error { return nil }, nil)
			taskmill.AddHandler(hs, taskmill.HandlerFunc[cardArgs](func(context.Context, *taskmill.Task[cardArgs]) error { return nil }), nil)
			taskmill.AddHandler(hs, taskmill.HandlerFunc[scoreArgs](func(context.Context, *taskmill.Task[scoreArgs]) error { return nil }), nil)
			runWorker(t, e, &taskmill.WorkerConfig{Handlers: hs, Queues: defaultQueue(1)})

			e.exec(t, `INSERT INTO {s}.tasks (queue, kind, state, run_at, payload, max_attempts)
			           VALUES ('default', $1, 'available', now(), $2, 5)`, c.kind, c.payload)
			id := int64(e.count(t, "SELECT max(id) FROM {s}.tasks"))

			info := e.waitState(t, id, taskmill.StateDead)
			if info.Attempt != 1 || info.Errors[0].Code != taskmill.CodeInvalidPayload || !strings.Contains(info.Errors[0].Message, c.says) {
				t.Fatalf("got attempt %d, errors %+v; want one that says %q", info.Attempt, info.Errors, c.says)
			}
			for _, r := range logs.all() {
				if line := recordText(r); strings.Contains(line, "4111") {
					t.Errorf("a log line shows the payload: %s", line)
				}
			}
			if strings.Contains(info.Errors[0].Message, "4111") {
				t.Errorf("the error shows the payload: %s", info.Errors[0].Message)
			}
		})
	}
}

func TestWorkerSnoozeKeepsTheAttempt(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	var calls atomic.Int32
	runWorker(t, e, &taskmill.WorkerConfig{
		Handlers: handlers(func(context.Context, *taskmill.Task[sendEmail]) error {
			if calls.Add(1) == 1 {
				return taskmill.Snooze(0)
			}
			return nil
		}, nil),
		Queues: defaultQueue(1),
	})

	id := e.enqueue(t, sendEmail{}, nil)
	if info := e.waitState(t, id, taskmill.StateCompleted); info.Attempt != 1 {
		t.Fatalf("snoozed task used %d attempts, want 1", info.Attempt)
	}
}

func TestWorkerSnoozesForADay(t *testing.T) {
	t.Parallel()
	e := newEnv(t)

	// A day is more microseconds than a 32-bit integer holds.
	const delay = 24 * time.Hour
	runWorker(t, e, &taskmill.WorkerConfig{
		Handlers: handlers(func(context.Context, *taskmill.Task[sendEmail]) error { return taskmill.Snooze(delay) }, nil),
		Queues:   defaultQueue(1),
	})

	asked := time.Now()
	id := e.enqueue(t, sendEmail{}, nil)
	info := e.waitState(t, id, taskmill.StateScheduled)
	if wait := info.RunAt.Sub(asked); info.Attempt != 0 || (wait-delay).Abs() > 10*time.Minute {
		t.Fatalf("the task waits %s after %d attempts, want about %s after none", wait, info.Attempt, delay)
	}
}

func TestWorkerSnoozesUnknownKinds(t *testing.T) {
	t.Parallel()
	e := newEnv(t)

	var hs taskmill.Handlers
	taskmill.AddHandler(&hs, taskmill.HandlerFunc[report](func(context.Context, *taskmill.Task[report]) error { return nil }), nil)
	const timeout = 500 * time.Millisecond
	w := newWorker(t, e, &taskmill.WorkerConfig{Handlers: &hs, Queues: defaultQueue(1)})
	taskmill.SetTiming(w, taskmill.Timing{NoHandlerWait: timeout})
	startWorker(t, w)

	// A task created long ago, for example in a long FIFO group, still waits
	// the whole timeout from the first time a worker meets it.
	e.exec(t, `INSERT INTO {s}.tasks (queue, kind, state, run_at, payload, max_attempts, created_at)
	           VALUES ('default', 'old.kind', 'available', now(), '{}', 5, now() - interval '2 hours')`)
	id := int64(e.count(t, "SELECT max(id) FROM {s}.tasks"))

	info := e.waitState(t, id, taskmill.StateDead)
	if len(info.Errors) != 2 || info.Errors[0].Code != taskmill.CodeNoHandler || info.Errors[1].Code != taskmill.CodeUnknownKind {
		t.Fatalf("got errors %+v, want a note when snoozed and an error when dead", info.Errors)
	}
	if info.Errors[0].Attempt != 0 || info.Attempt != 1 {
		t.Fatalf("the note counts attempt %d and the task %d; snoozes use no attempt", info.Errors[0].Attempt, info.Attempt)
	}
	if waited := info.Errors[1].At.Sub(info.Errors[0].At); waited < timeout {
		t.Fatalf("gave up after %s, want at least %s", waited, timeout)
	}
}

// A rolling deploy that adds a kind can take longer than an hour. Workers of
// the old release with a longer NoHandlerWait keep its tasks waiting, where
// the default of an hour makes them dead.
func TestNoHandlerWaitIsTheWorkersOwn(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	var hs taskmill.Handlers
	taskmill.AddHandler(&hs, taskmill.HandlerFunc[report](func(context.Context, *taskmill.Task[report]) error { return nil }), nil)
	patient := newWorker(t, e, &taskmill.WorkerConfig{Handlers: &hs, NoHandlerWait: 3 * time.Hour})
	hasty := newWorker(t, e, &taskmill.WorkerConfig{Handlers: &hs})

	id := e.enqueue(t, sendEmail{}, nil)
	info, err := patient.RunTask(t.Context(), id)
	if err != nil || info.State != taskmill.StateScheduled || !strings.Contains(info.Errors[0].Message, "waiting up to 3h0m0s") {
		t.Fatalf("got %+v, %v, want a snooze with a note of a wait of 3 hours", info, err)
	}
	e.exec(t, `UPDATE {s}.tasks SET errors = jsonb_set(errors, '{0,at}', to_jsonb(now() - interval '2 hours')) WHERE id = $1`, id)

	if info, err = patient.RunTask(t.Context(), id); err != nil || info.State != taskmill.StateScheduled {
		t.Fatalf("after 2 hours of a wait of 3: got %+v, %v, want the task to wait on", info, err)
	}
	info, err = hasty.RunTask(t.Context(), id)
	if last := info.Errors[len(info.Errors)-1]; err != nil || info.State != taskmill.StateDead || last.Code != taskmill.CodeUnknownKind {
		t.Fatalf("after 2 hours of a wait of 1: got %+v, %v, want the task dead with unknown_kind", info, err)
	}
}

// A rolling deploy can leave many tasks of a new kind to the workers of the
// old release. Each worker warns once a minute per kind, with how many
// tasks began to wait, and logs each task at Debug.
func TestTasksWithoutAHandlerWarnOncePerKind(t *testing.T) {
	t.Parallel()
	logs := &logRecorder{}
	e := newEnv(t).withLogger(t, slog.New(logs))
	const n = 200
	for range n {
		e.enqueue(t, report{}, &taskmill.EnqueueOptions{Queue: taskmill.DefaultQueue})
	}
	runWorker(t, e, &taskmill.WorkerConfig{Handlers: noopHandlers(), Queues: defaultQueue(50)})
	waitFor(t, "every task to wait", func() bool {
		return e.count(t, "SELECT count(*) FROM {s}.tasks WHERE state = 'scheduled'") == n
	})

	warned := slices.DeleteFunc(logs.all(), func(r slog.Record) bool {
		return r.Message != "no handler for task kind; its tasks wait for a worker that has one"
	})
	if len(warned) != 1 || warned[0].Level != slog.LevelWarn || attr(warned[0], "kind") != "report.build" {
		t.Fatalf("got the warnings %v, want one for the kind", warned)
	}
	if got := logs.count("no handler for task kind; snoozing"); got != n {
		t.Fatalf("got %d lines for the tasks, want %d", got, n)
	}
}

// The wait for a handler ends when a worker that has one takes the task,
// however its attempt ends, and when an operator retries the task. A worker
// without a handler that meets the task later, after a rollback for example,
// starts a new wait: the note of the old wait must not make the task dead at
// once.
func TestWaitForAHandlerStartsAgain(t *testing.T) {
	t.Parallel()
	// Each case is what happens to the task after it began to wait.
	tests := map[string]func(t *testing.T, e *env, id int64){
		"a handler's attempt failed":      handledBy(errors.New("the partner is down"), taskmill.StateRetryable),
		"a handler snoozed the task":      handledBy(taskmill.Snooze(time.Hour), taskmill.StateScheduled),
		"a handler's worker gave it back": handledBy(nil, taskmill.StateAvailable),
		"an operator canceled and retried it": func(t *testing.T, e *env, id int64) {
			t.Helper()
			if err := errors.Join(e.client.Cancel(t.Context(), id), e.client.Retry(t.Context(), id)); err != nil {
				t.Fatal(err)
			}
		},
		"an operator canceled it and retried its kind": func(t *testing.T, e *env, id int64) {
			t.Helper()
			if err := e.client.Cancel(t.Context(), id); err != nil {
				t.Fatal(err)
			}
			res, err := e.client.RetryMatching(t.Context(), bulk(&taskmill.TaskFilter{Kinds: []string{"email.send"}}))
			if err != nil || res.Changed != 1 {
				t.Fatalf("retried %d tasks: %v", res.Changed, err)
			}
		},
	}

	for name, since := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			e := newEnv(t)

			var other taskmill.Handlers
			taskmill.AddHandler(&other, taskmill.HandlerFunc[report](func(context.Context, *taskmill.Task[report]) error { return nil }), nil)
			without := newWorker(t, e, &taskmill.WorkerConfig{Handlers: &other})
			id := e.enqueue(t, sendEmail{}, nil)

			// A rolling deploy: a worker of the old release meets the task, which
			// begins to wait for a handler. That was two hours ago.
			info, err := without.RunTask(t.Context(), id)
			if err != nil || info.State != taskmill.StateScheduled || info.Errors[0].Code != taskmill.CodeNoHandler {
				t.Fatalf("got %+v, %v, want a snooze with a note", info, err)
			}
			e.exec(t, `UPDATE {s}.tasks SET errors = jsonb_set(errors, '{0,at}', to_jsonb(now() - interval '2 hours'))
			           WHERE id = $1`, id)

			since(t, e, id)

			// A rollback: a worker without the handler meets the task again.
			if info, err = without.RunTask(t.Context(), id); err != nil {
				t.Fatal(err)
			}
			last := info.Errors[len(info.Errors)-1]
			if info.State != taskmill.StateScheduled || last.Code != taskmill.CodeNoHandler || time.Since(last.At) > time.Hour {
				t.Fatalf("the task is %s with errors %+v, want it to wait for a handler again, with a new note", info.State, info.Errors)
			}
		})
	}
}

// handledBy returns a step of TestWaitForAHandlerStartsAgain: a worker with a
// handler runs the task, which leaves it in the state want. The handler
// returns result, or, if result is nil, runs until RunTask's context ends.
func handledBy(result error, want taskmill.State) func(t *testing.T, e *env, id int64) {
	return func(t *testing.T, e *env, id int64) {
		t.Helper()
		runCtx, stopRun := context.WithCancel(t.Context())
		defer stopRun()

		with := newWorker(t, e, &taskmill.WorkerConfig{
			Handlers: handlers(func(ctx context.Context, _ *taskmill.Task[sendEmail]) error {
				if result != nil {
					return result
				}
				stopRun()
				<-ctx.Done()
				return ctx.Err()
			}, nil),
		})

		if info, err := with.RunTask(runCtx, id); err != nil || info.State != want {
			t.Fatalf("the worker with the handler left the task as %+v, %v, want %s", info, err, want)
		}
	}
}

// The task's row decides whether an attempt ends in a cancellation, whatever
// the worker believed while the handler ran.
func TestAttemptEndsCanceledOnlyIfTheTaskSaysSo(t *testing.T) {
	t.Parallel()
	e := newEnv(t)

	var calls atomic.Int32
	started := make(chan struct{}, 1)
	ended := make(chan struct{})
	proceed := make(chan struct{})
	runWorker(t, e, &taskmill.WorkerConfig{
		Handlers: handlers(func(ctx context.Context, _ *taskmill.Task[sendEmail]) error {
			if calls.Add(1) > 1 {
				return nil
			}
			started <- struct{}{}
			<-ctx.Done()
			close(ended)
			<-proceed
			return context.Cause(ctx)
		}, &taskmill.HandlerOptions{Timeout: time.Minute}),
		Queues: defaultQueue(1),
	})

	id := e.enqueue(t, sendEmail{}, nil)
	recv(t, started, "the handler to start")
	e.waitListening(t)

	if err := e.client.Cancel(t.Context(), id); err != nil {
		t.Fatal(err)
	}
	recv(t, ended, "the handler's context to end")

	// The request leaves the row before the attempt ends, here by hand.
	e.exec(t, "UPDATE {s}.tasks SET cancel_requested_at = NULL WHERE id = $1", id)
	close(proceed)
	e.waitState(t, id, taskmill.StateCompleted)
}

func TestHandlerContextSaysWhyItEnded(t *testing.T) {
	t.Parallel()
	e := newEnv(t)

	started := make(chan struct{}, 2)
	cause := make(chan error, 1)
	w := newWorker(t, e, &taskmill.WorkerConfig{
		Handlers: handlers(func(ctx context.Context, _ *taskmill.Task[sendEmail]) error {
			started <- struct{}{}
			<-ctx.Done()
			cause <- context.Cause(ctx)
			return ctx.Err()
		}, &taskmill.HandlerOptions{Timeout: 200 * time.Millisecond}),
		NoMaintainer: true,
	})

	// The attempt runs out of time.
	if _, err := w.RunTask(t.Context(), e.enqueue(t, sendEmail{}, nil)); err != nil {
		t.Fatal(err)
	}
	recv(t, started, "the handler to start")
	if err := recv(t, cause, "the timeout"); !errors.Is(err, taskmill.ErrAttemptTimeout) {
		t.Fatalf("got cause %v, want ErrAttemptTimeout", err)
	}

	// RunTask's context ends while the handler runs, as a worker's does when
	// it shuts down.
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	id := e.enqueue(t, sendEmail{}, nil)
	done := make(chan error, 1)
	go func() {
		_, err := w.RunTask(ctx, id)
		done <- err
	}()

	recv(t, started, "the handler to start")
	cancel()
	if err := recv(t, cause, "the shutdown"); !errors.Is(err, taskmill.ErrShutdown) {
		t.Fatalf("got cause %v, want ErrShutdown", err)
	}
	if err := recv(t, done, "RunTask to return"); err != nil {
		t.Fatal(err)
	}
}

func TestFIFOGroupHaltsOnDeadHead(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	var fixed atomic.Bool
	runWorker(t, e, &taskmill.WorkerConfig{
		Handlers: handlers(func(_ context.Context, task *taskmill.Task[sendEmail]) error {
			if task.Args.To == "first" && !fixed.Load() {
				return taskmill.Permanent(errors.New("bad"))
			}
			return nil
		}, nil),
		Queues: defaultQueue(4),
	})

	group := &taskmill.EnqueueOptions{GroupKey: "ledger-1"}
	first := e.enqueue(t, sendEmail{To: "first"}, group)
	second := e.enqueue(t, sendEmail{To: "second"}, group)

	e.waitState(t, first, taskmill.StateDead)
	// The halt commits with the dead state, so the next task stays pending.
	if n := e.count(t, "SELECT count(*) FROM {s}.groups WHERE halted_task_id = $1", first); n != 1 {
		t.Fatal("the dead head did not halt its group")
	}
	e.wantState(t, second, taskmill.StatePending)

	fixed.Store(true)
	if err := e.client.Retry(t.Context(), first); err != nil {
		t.Fatal(err)
	}
	e.waitState(t, first, taskmill.StateCompleted)
	e.waitState(t, second, taskmill.StateCompleted)
}

// A head that is canceled while it runs leaves its group when its worker
// records the end of the attempt, and the next task takes over.
func TestRecordedCancelOfAHeadPromotesTheNextTask(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	group := &taskmill.EnqueueOptions{GroupKey: "g"}
	head := e.enqueue(t, sendEmail{}, group)
	next := e.enqueue(t, sendEmail{}, group)

	w := newWorker(t, e, &taskmill.WorkerConfig{
		Handlers: handlers(func(ctx context.Context, task *taskmill.Task[sendEmail]) error {
			if err := e.client.Cancel(ctx, task.ID); err != nil {
				return err
			}
			return errors.New("stopped")
		}, nil),
	})

	if info, err := w.RunTask(t.Context(), head); err != nil || info.State != taskmill.StateCanceled {
		t.Fatalf("the head: %v, %+v, want it canceled", err, info)
	}
	e.wantState(t, next, taskmill.StateAvailable)
}

func TestMiddlewareWrapsAttemptsInOrder(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	var mu sync.Mutex
	var log []string

	mw := func(name string) taskmill.Middleware {
		return func(ctx context.Context, task *taskmill.TaskInfo, next func(context.Context) error) error {
			mu.Lock()
			log = append(log, name+" before "+task.Kind)
			mu.Unlock()

			err := next(ctx)
			mu.Lock()
			log = append(log, name+" after")
			mu.Unlock()
			return err
		}
	}

	done := make(chan struct{})
	runWorker(t, e, &taskmill.WorkerConfig{
		Handlers: handlers(func(context.Context, *taskmill.Task[sendEmail]) error {
			mu.Lock()
			log = append(log, "handler")
			mu.Unlock()
			close(done)
			return nil
		}, nil),
		Queues:     defaultQueue(1),
		Middleware: []taskmill.Middleware{mw("outer"), mw("inner")},
	})

	id := e.enqueue(t, sendEmail{}, nil)
	recv(t, done, "the worker to stop")
	e.waitState(t, id, taskmill.StateCompleted)

	mu.Lock()
	defer mu.Unlock()
	want := []string{"outer before email.send", "inner before email.send", "handler", "inner after", "outer after"}
	if !slices.Equal(log, want) {
		t.Fatalf("got %q, want %q", log, want)
	}
}

type batchPublisher struct {
	mu      sync.Mutex
	batches [][]int64
}

func (b *batchPublisher) HandleBatch(_ context.Context, tasks []*taskmill.Task[sendEmail]) []error {
	b.mu.Lock()
	defer b.mu.Unlock()
	ids := make([]int64, len(tasks))
	errs := make([]error, len(tasks))
	for i, task := range tasks {
		ids[i] = task.Args.UserID
		if task.Args.UserID == 3 && task.Attempt == 1 {
			errs[i] = errors.New("broker rejected message 3")
		}
	}

	b.batches = append(b.batches, ids)
	return errs
}

func TestBatchHandler(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	pub := &batchPublisher{}
	var hs taskmill.Handlers
	taskmill.AddBatchHandler[sendEmail](&hs, pub, &taskmill.HandlerOptions{BatchSize: 4, Retry: fastRetry})
	var wrapped atomic.Int32

	params := make([]taskmill.EnqueueParams, 10)
	for i := range params {
		params[i] = taskmill.EnqueueParams{Args: sendEmail{UserID: int64(i)}}
	}
	res, err := e.client.EnqueueMany(t.Context(), params)
	if err != nil {
		t.Fatal(err)
	}

	runWorker(t, e, &taskmill.WorkerConfig{
		Handlers: &hs,
		Queues:   defaultQueue(1),
		Middleware: []taskmill.Middleware{func(ctx context.Context, _ *taskmill.TaskInfo, next func(context.Context) error) error {
			wrapped.Add(1)
			return next(ctx)
		}},
	})

	for _, r := range res {
		e.waitState(t, r.ID, taskmill.StateCompleted)
	}

	pub.mu.Lock()
	defer pub.mu.Unlock()
	largest := 0
	for _, b := range pub.batches {
		largest = max(largest, len(b))
	}
	if largest < 2 || largest > 4 {
		t.Fatalf("largest batch has %d tasks, want 2 to 4", largest)
	}
	if w := wrapped.Load(); w != 11 {
		t.Fatalf("middleware wrapped %d attempts, want 11 (10 plus one retry)", w)
	}
}

// A cancel does not end a batch's context, but the batch handler sees it
// on the task, and can skip the task's work.
func TestBatchHandlerSeesACancelRequest(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	res, err := e.client.EnqueueMany(t.Context(), []taskmill.EnqueueParams{
		{Args: sendEmail{To: "kept"}}, {Args: sendEmail{To: "canceled"}},
	})
	if err != nil {
		t.Fatal(err)
	}

	started := make(chan struct{}, 1)
	var hs taskmill.Handlers
	taskmill.AddBatchHandler(&hs, taskmill.BatchHandlerFunc[sendEmail](func(ctx context.Context, tasks []*taskmill.Task[sendEmail]) []error {
		started <- struct{}{}
		errs := make([]error, len(tasks))
		for i, task := range tasks {
			for task.Args.To == "canceled" && !task.CancelRequested() && ctx.Err() == nil {
				time.Sleep(10 * time.Millisecond)
			}
			if task.CancelRequested() {
				errs[i] = taskmill.ErrTaskCanceled
			}
		}
		return errs
	}), nil)
	runWorker(t, e, &taskmill.WorkerConfig{Handlers: &hs, Queues: defaultQueue(1)})

	recv(t, started, "the batch to start")
	if err := e.client.Cancel(t.Context(), res[1].ID); err != nil {
		t.Fatal(err)
	}
	e.waitState(t, res[1].ID, taskmill.StateCanceled)
	e.waitState(t, res[0].ID, taskmill.StateCompleted)
}

func TestCancelRequestWinsOverOtherOutcomes(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name   string
		result func(ctx context.Context) error
	}{
		{"fail", func(context.Context) error { return errors.New("boom") }},
		{"snooze", func(context.Context) error { return taskmill.Snooze(time.Minute) }},
		{"release", func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() }},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			e := newEnv(t)
			started := make(chan struct{}, 1)
			proceed := make(chan struct{})
			stop := runWorker(t, e, &taskmill.WorkerConfig{
				Handlers: handlers(func(ctx context.Context, _ *taskmill.Task[sendEmail]) error {
					started <- struct{}{}
					<-proceed
					return tc.result(ctx)
				}, &taskmill.HandlerOptions{Retry: fastRetry}),
				Queues:       defaultQueue(1),
				DrainTimeout: 50 * time.Millisecond,
			})

			id := e.enqueue(t, sendEmail{}, &taskmill.EnqueueOptions{GroupKey: "g"})
			recv(t, started, "a handler to start")

			// The cancellation is requested, but its notification is missed.
			e.exec(t, "UPDATE {s}.tasks SET cancel_requested_at = now() WHERE id = $1", id)
			close(proceed)
			if tc.name == "release" {
				stop()
			}

			e.waitState(t, id, taskmill.StateCanceled)
			if n := e.count(t, "SELECT count(*) FROM {s}.groups"); n != 0 {
				t.Fatal("the canceled task did not leave its group")
			}
		})
	}
}

func TestWorkerRecordsErrorsWithNULBytes(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	runWorker(t, e, &taskmill.WorkerConfig{
		Handlers: handlers(func(context.Context, *taskmill.Task[sendEmail]) error {
			return errors.New("bad\x00byte")
		}, nil),
		Queues: defaultQueue(1),
	})

	id := e.enqueue(t, sendEmail{}, &taskmill.EnqueueOptions{MaxAttempts: 1})
	info := e.waitState(t, id, taskmill.StateDead)
	if len(info.Errors) != 1 || info.Errors[0].Message != "badbyte" {
		t.Fatalf("got errors %+v", info.Errors)
	}
}

// failingBatch fails or panics on its first batch.
type failingBatch struct {
	panics, appends bool
	calls           atomic.Int32
}

func (f *failingBatch) HandleBatch(_ context.Context, tasks []*taskmill.Task[sendEmail]) []error {
	if f.calls.Add(1) > 1 {
		return make([]error, len(tasks))
	}
	if f.panics {
		panic("broker client crashed")
	}
	if f.appends { // the slip of make and append, while the broker is down
		errs := make([]error, len(tasks))
		for range tasks {
			errs = append(errs, errors.New("broker down")) //nolint:makezero // the slip that the test is about
		}
		return errs
	}
	return nil // no result for any task
}

func TestBatchHandlerFailures(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, message   string
		code            taskmill.ErrorCode
		panics, appends bool
	}{
		{"panic", "panic: broker client crashed", taskmill.CodePanic, true, false},
		{"missing results", "the batch handler returned no result for this task", "", false, false},
		{"too many results", "the batch handler returned 4 results for 2 tasks", "", false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			e := newEnv(t)
			var hs taskmill.Handlers
			taskmill.AddBatchHandler[sendEmail](&hs, &failingBatch{panics: tc.panics, appends: tc.appends}, &taskmill.HandlerOptions{Retry: fastRetry})

			res, err := e.client.EnqueueMany(t.Context(), []taskmill.EnqueueParams{{Args: sendEmail{}}, {Args: sendEmail{}}})
			if err != nil {
				t.Fatal(err)
			}

			// With two free slots, the first fetch takes both tasks into one
			// batch.
			runWorker(t, e, &taskmill.WorkerConfig{Handlers: &hs, Queues: defaultQueue(2)})

			for _, r := range res {
				info := e.waitState(t, r.ID, taskmill.StateCompleted)
				if len(info.Errors) != 1 || info.Errors[0].Code != tc.code || !strings.HasPrefix(info.Errors[0].Message, tc.message) {
					t.Fatalf("got errors %+v", info.Errors)
				}
			}
		})
	}
}

func TestMiddlewareCannotRunAHandlerTwice(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	var runs atomic.Int32

	twice := taskmill.Middleware(func(ctx context.Context, _ *taskmill.TaskInfo, next func(context.Context) error) error {
		_ = next(ctx)
		return next(ctx)
	})

	w := newWorker(t, e, &taskmill.WorkerConfig{
		Handlers: handlers(func(context.Context, *taskmill.Task[sendEmail]) error {
			runs.Add(1)
			return nil
		}, nil),
		Middleware: []taskmill.Middleware{twice},
	})

	info, err := w.RunTask(t.Context(), e.enqueue(t, sendEmail{}, nil))
	if err != nil {
		t.Fatal(err)
	}
	if runs.Load() != 1 || info.State != taskmill.StateRetryable ||
		!strings.Contains(info.Errors[0].Message, "next twice") {
		t.Fatalf("the handler ran %d times; task %s with errors %+v", runs.Load(), info.State, info.Errors)
	}
}

func TestMiddlewareThatSkipsTheHandlerFailsTheAttempt(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	var runs atomic.Int32
	var hs taskmill.Handlers
	taskmill.AddHandler(&hs, taskmill.HandlerFunc[sendEmail](func(context.Context, *taskmill.Task[sendEmail]) error {
		runs.Add(1)
		return nil
	}), nil)
	taskmill.AddBatchHandler[report](&hs, batchFunc(func([]*taskmill.Task[report]) { runs.Add(1) }), nil)

	var failures atomic.Int32 // the failures that the outer middleware saw
	outer := taskmill.Middleware(func(ctx context.Context, _ *taskmill.TaskInfo, next func(context.Context) error) error {
		err := next(ctx)
		if err != nil {
			failures.Add(1)
		}
		return err
	})
	swallow := taskmill.Middleware(func(ctx context.Context, _ *taskmill.TaskInfo, next func(context.Context) error) error {
		_ = next(ctx)
		return nil
	})
	skip := taskmill.Middleware(func(context.Context, *taskmill.TaskInfo, func(context.Context) error) error { return nil })

	for _, c := range []struct {
		name  string
		chain []taskmill.Middleware
	}{
		{"seen by the middleware outside", []taskmill.Middleware{outer, skip}},
		{"hidden by the middleware outside", []taskmill.Middleware{swallow, skip}},
	} {
		w := newWorker(t, e, &taskmill.WorkerConfig{Handlers: &hs, Middleware: c.chain})
		for _, args := range []taskmill.Args{sendEmail{}, report{}} { // a single task, and a batch
			info, err := w.RunTask(t.Context(), e.enqueue(t, args, nil))
			if err != nil {
				t.Fatal(err)
			}
			if info.State != taskmill.StateRetryable || len(info.Errors) != 1 ||
				!strings.Contains(info.Errors[0].Message, "without running the handler") {
				t.Fatalf("%s, %s: got task %s with errors %+v, want a retry", c.name, args.Kind(), info.State, info.Errors)
			}
		}
	}

	if failures.Load() != 2 {
		t.Fatalf("the outer middleware saw %d failures, want 2", failures.Load())
	}
	if runs.Load() != 0 {
		t.Fatalf("the handlers ran %d times", runs.Load())
	}
}

func TestMiddlewareThatSkipsSomeTasksOfABatch(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	got := make(chan []string, 1)
	var hs taskmill.Handlers
	taskmill.AddBatchHandler[report](&hs, batchFunc(func(tasks []*taskmill.Task[report]) {
		names := make([]string, len(tasks))
		for i, task := range tasks {
			names[i] = task.Args.Name
		}
		got <- names
	}), &taskmill.HandlerOptions{Retry: taskmill.ExponentialBackoff{Base: time.Hour}})

	skip := taskmill.Middleware(func(ctx context.Context, task *taskmill.TaskInfo, next func(context.Context) error) error {
		if strings.Contains(string(task.Payload), "skip") {
			return nil
		}
		return next(ctx)
	})

	var ids []int64
	for _, name := range []string{"a", "skip-b", "c", "skip-d"} {
		ids = append(ids, e.enqueue(t, report{Name: name}, nil))
	}
	runWorker(t, e, &taskmill.WorkerConfig{Handlers: &hs, Middleware: []taskmill.Middleware{skip}})

	if names := recv(t, got, "the batch"); !slices.Equal(names, []string{"a", "c"}) {
		t.Fatalf("the batch handler got %q, want only the tasks not skipped", names)
	}

	for i, id := range ids {
		want := taskmill.StateCompleted
		if i%2 == 1 {
			want = taskmill.StateRetryable
		}
		e.waitState(t, id, want)
	}
}

// A handler that ignores its context would keep its task running for good:
// its timeout, a cancel, or a delete would change nothing. 5 seconds after
// its context ends (here 200 ms), the worker abandons its attempt as the
// cause says, and logs it, while the handler's goroutine keeps its slot
// until it returns: the next task waits for it, as Concurrency says. The
// handler's late result counts for nothing.
func TestHandlerThatIgnoresItsTimeoutIsAbandoned(t *testing.T) {
	t.Parallel()
	s := runStuckHandler(t, 300*time.Millisecond)
	info := s.e.waitState(t, s.stuck, taskmill.StateDead)
	if len(info.Errors) != 1 || info.Errors[0].Code != taskmill.CodeTimeout ||
		!strings.Contains(info.Errors[0].Message, "the handler did not return") {
		t.Errorf("got the errors %+v, want one with the code timeout that says the handler did not return", info.Errors)
	}
	s.check(t, taskmill.StateDead)
}

// So is a handler that ignores the cancel of its task: the task is
// canceled.
func TestHandlerThatIgnoresACancelIsAbandoned(t *testing.T) {
	t.Parallel()
	s := runStuckHandler(t, time.Hour)
	if err := s.e.client.Cancel(t.Context(), s.stuck); err != nil {
		t.Fatal(err)
	}
	s.e.waitState(t, s.stuck, taskmill.StateCanceled)
	s.check(t, taskmill.StateCanceled)
}

// In a queue with a MaxRunning limit, the worker does not abandon a handler
// that ignores its context: its task would run again while the handler
// still runs, beyond the limit, which protects what the handlers call. The
// task stays running until the handler returns, and the worker warns.
func TestHandlerThatIgnoresItsTimeoutInALimitedQueueKeepsItsTask(t *testing.T) {
	t.Parallel()
	logs := &logRecorder{}
	e := newEnv(t).withLogger(t, slog.New(logs))
	e.limitDefaultQueue(t, 1)
	var runs atomic.Int32
	unblock := make(chan struct{})
	runWorker(t, e, &taskmill.WorkerConfig{
		Handlers: handlers(func(context.Context, *taskmill.Task[sendEmail]) error {
			if runs.Add(1) == 1 {
				<-unblock // ignores its context
			}
			return nil
		}, &taskmill.HandlerOptions{Timeout: 300 * time.Millisecond}),
		Queues: defaultQueue(2),
	})

	id := e.enqueue(t, sendEmail{}, &taskmill.EnqueueOptions{MaxAttempts: 2})
	const kept = "handler ignored its context; its task keeps running because its queue has a MaxRunning limit"
	waitFor(t, "the warning", func() bool { return logs.count(kept) == 1 })
	if info := e.get(t, id); info.State != taskmill.StateRunning || info.Attempt != 1 || runs.Load() != 1 {
		t.Fatalf("the task is %s on attempt %d, after %d runs; want running on its first, alone", info.State, info.Attempt, runs.Load())
	}

	close(unblock)
	if info := e.waitState(t, id, taskmill.StateCompleted); info.Attempt != 1 || runs.Load() != 1 {
		t.Fatalf("completed on attempt %d after %d runs, want one", info.Attempt, runs.Load())
	}
	if n := logs.count("handler ignored its context; abandoning its attempt"); n != 0 {
		t.Errorf("the attempt of a limited queue was abandoned")
	}
}

// stuckHandler is a worker of one slot whose handler of a task ignores its
// context, and a task that waits behind it.
type stuckHandler struct {
	e           *env
	logs        *logRecorder
	stuck, next int64
	started     chan int64
	unblock     chan struct{}
}

// runStuckHandler runs a stuckHandler whose handlers have the timeout
// timeout, and waits until the stuck task's handler runs.
func runStuckHandler(t *testing.T, timeout time.Duration) *stuckHandler {
	t.Helper()
	s := &stuckHandler{logs: &logRecorder{}, started: make(chan int64, 2), unblock: make(chan struct{})}
	s.e = newEnv(t).withLogger(t, slog.New(s.logs))
	runWorker(t, s.e, &taskmill.WorkerConfig{
		Handlers: handlers(func(_ context.Context, task *taskmill.Task[sendEmail]) error {
			s.started <- task.ID
			if task.Args.To == "stuck" {
				<-s.unblock // ignores its context
			}
			return nil
		}, &taskmill.HandlerOptions{Timeout: timeout}),
		Queues: defaultQueue(1),
	})
	t.Cleanup(func() { close(s.unblock) })

	s.stuck = s.e.enqueue(t, sendEmail{To: "stuck"}, &taskmill.EnqueueOptions{MaxAttempts: 1})
	recv(t, s.started, "the handler to start")
	s.next = s.e.enqueue(t, sendEmail{}, nil)
	return s
}

// check checks, once the stuck task is in state, that the worker logged the
// abandoned attempt once, and that the next task started only once the
// stuck handler returned, and left the stuck task as it was.
func (s *stuckHandler) check(t *testing.T, state taskmill.State) {
	t.Helper()
	if n := s.logs.count("handler ignored its context; abandoning its attempt"); n != 1 {
		t.Errorf("the abandoned attempt was logged %d times, want once", n)
	}
	if n := s.e.count(t, "SELECT count(*) FROM {s}.tasks WHERE id = $1 AND state = 'available'", s.next); n != 1 {
		t.Errorf("the next task started while the abandoned handler kept its slot")
	}

	s.unblock <- struct{}{}
	if got := recv(t, s.started, "the next task to start"); got != s.next {
		t.Fatalf("task %d started, want %d", got, s.next)
	}
	s.e.waitState(t, s.next, taskmill.StateCompleted)
	s.e.wantState(t, s.stuck, state)
}

// A task keeps its last 20 errors: after 25 failed attempts, those of
// attempts 6 to 25.
func TestTaskKeepsItsLast20Errors(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	w := newWorker(t, e, &taskmill.WorkerConfig{
		Handlers: handlers(func(context.Context, *taskmill.Task[sendEmail]) error {
			return errors.New("the partner is down")
		}, nil),
		NoMaintainer: true,
	})

	id := e.enqueue(t, sendEmail{}, &taskmill.EnqueueOptions{MaxAttempts: 25})
	for range 25 {
		if _, err := w.RunTask(t.Context(), id); err != nil {
			t.Fatal(err)
		}
	}

	info := e.get(t, id)
	if info.State != taskmill.StateDead || len(info.Errors) != 20 {
		t.Fatalf("got %s with %d errors, want dead with 20", info.State, len(info.Errors))
	}
	for i, err := range info.Errors {
		if err.Attempt != 6+i {
			t.Fatalf("error %d is of attempt %d, want %d", i, err.Attempt, 6+i)
		}
	}
}

// Attempts that fail at about the same time record their failures together,
// in one statement for many, as completions do: workers whose handlers fail
// fast, as in an outage of what they call, then cost the database little
// more than when the handlers succeed.
func TestFailuresAtTheSameTimeShareStatements(t *testing.T) {
	t.Parallel()
	sent := &statementLog{}
	e := newTracedEnv(t, sent)
	const tasks = 64

	started, fail := make(chan struct{}, tasks), make(chan struct{})
	runWorker(t, e, &taskmill.WorkerConfig{
		Handlers: handlers(func(context.Context, *taskmill.Task[sendEmail]) error {
			started <- struct{}{}
			<-fail
			return errors.New("the API is down")
		}, nil),
		Queues:       defaultQueue(tasks),
		NoMaintainer: true,
	})

	params := make([]taskmill.EnqueueParams, tasks)
	for i := range params {
		params[i] = taskmill.EnqueueParams{Args: sendEmail{UserID: int64(i)}, Options: &taskmill.EnqueueOptions{MaxAttempts: 1}}
	}
	res, err := e.client.EnqueueMany(t.Context(), params)
	if err != nil {
		t.Fatal(err)
	}
	for range tasks {
		recv(t, started, "a handler to start")
	}

	sent.reset()
	close(fail)
	for _, r := range res {
		e.waitState(t, r.ID, taskmill.StateDead)
	}
	if n := sent.count("recorded AS"); n > tasks/8 {
		t.Errorf("%d failures at the same time took %d statements, want at most %d", tasks, n, tasks/8)
	}
}

// A batch handler's failures are recorded together, in one statement, or a
// few on a slow machine, as a stopping worker's releases of the tasks that
// it fetched and did not start are.
func TestABatchFailsInOneStatement(t *testing.T) {
	t.Parallel()
	sent := &statementLog{}
	e := newTracedEnv(t, sent)
	const tasks = 50

	params := make([]taskmill.EnqueueParams, tasks)
	for i := range params {
		params[i] = taskmill.EnqueueParams{Args: sendEmail{UserID: int64(i)}, Options: &taskmill.EnqueueOptions{MaxAttempts: 1}}
	}
	res, err := e.client.EnqueueMany(t.Context(), params)
	if err != nil {
		t.Fatal(err)
	}

	var hs taskmill.Handlers
	taskmill.AddBatchHandler(&hs, taskmill.BatchHandlerFunc[sendEmail](func(_ context.Context, ts []*taskmill.Task[sendEmail]) []error {
		errs := make([]error, len(ts))
		for i := range errs {
			errs[i] = errors.New("the broker is down")
		}
		return errs
	}), &taskmill.HandlerOptions{BatchSize: tasks})
	sent.reset()
	runWorker(t, e, &taskmill.WorkerConfig{Handlers: &hs, Queues: defaultQueue(1), NoMaintainer: true})

	for _, r := range res {
		e.waitState(t, r.ID, taskmill.StateDead)
	}
	if n := sent.count("recorded AS"); n > 3 {
		t.Errorf("a batch of %d failures took %d statements, want at most 3", tasks, n)
	}
}

// A failure whose task's row another transaction holds, as a CancelTx that
// has not committed does, waits for it alone: the failures recorded with it
// do not wait.
func TestHeldTaskDoesNotHoldUpOtherFailures(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	started, fail := make(chan struct{}, 10), make(chan struct{})
	runWorker(t, e, &taskmill.WorkerConfig{
		Handlers: handlers(func(context.Context, *taskmill.Task[sendEmail]) error {
			started <- struct{}{}
			<-fail
			return errors.New("fails")
		}, nil),
		Queues:       defaultQueue(10),
		NoMaintainer: true,
	})

	var ids []int64
	for range 5 {
		ids = append(ids, e.enqueue(t, sendEmail{}, &taskmill.EnqueueOptions{MaxAttempts: 1}))
	}
	for range ids {
		recv(t, started, "a handler to start")
	}

	held := ids[2]
	tx := e.begin(t)
	e.txExec(t, tx, "SELECT FROM {s}.tasks WHERE id = $1 FOR UPDATE", held)

	close(fail)
	for _, id := range ids {
		if id != held {
			e.waitState(t, id, taskmill.StateDead)
		}
	}
	e.wantState(t, held, taskmill.StateRunning)

	if err := tx.Commit(t.Context()); err != nil {
		t.Fatal(err)
	}
	if info := e.waitState(t, held, taskmill.StateDead); len(info.Errors) != 1 {
		t.Fatalf("the held task's failure was recorded with %d errors, want 1", len(info.Errors))
	}
}
