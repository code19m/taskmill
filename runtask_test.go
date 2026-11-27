package taskmill_test

import (
	"context"
	"errors"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/code19m/taskmill"
	"github.com/code19m/taskmill/internal/testdb"
)

// These tests cover Worker.RunTask, which runs one task now, in the calling
// goroutine.

// A handler that ends its goroutine with runtime.Goexit, as t.Fatal in a
// test does, skips the rest of RunTask. Its cleanup still runs: the task
// goes back to its queue, and nothing renews its lease any more. A batch
// handler runs in another goroutine than the middleware of its tasks: their
// next returns, and no goroutine stays behind.
func TestRunTaskOfAHandlerThatEndsItsGoroutine(t *testing.T) {
	t.Parallel()
	for _, batch := range []bool{false, true} {
		t.Run(map[bool]string{false: "handler", true: "batch handler"}[batch], func(t *testing.T) {
			t.Parallel()
			e := newEnv(t)
			var hs taskmill.Handlers
			var args taskmill.Args = sendEmail{}
			if batch {
				taskmill.AddBatchHandler[report](&hs, batchFunc(func([]*taskmill.Task[report]) { runtime.Goexit() }), nil)
				args = report{}
			} else {
				taskmill.AddHandler(&hs, taskmill.HandlerFunc[sendEmail](func(context.Context, *taskmill.Task[sendEmail]) error {
					runtime.Goexit()
					return nil
				}), nil)
			}
			nextReturned := make(chan error, 1)
			w := newWorker(t, e, &taskmill.WorkerConfig{
				Handlers: &hs, NoMaintainer: true, LeaseDuration: 300 * time.Millisecond,
				Middleware: []taskmill.Middleware{func(ctx context.Context, _ *taskmill.TaskInfo, next func(context.Context) error) error {
					err := next(ctx)
					nextReturned <- err
					return err
				}},
			})

			id := e.enqueue(t, args, nil)
			ended := make(chan struct{})
			go func() {
				defer close(ended)
				_, _ = w.RunTask(t.Context(), id)
			}()
			recv(t, ended, "the goroutine to end")
			if batch {
				if err := recv(t, nextReturned, "the next of the task's middleware to return"); err == nil {
					t.Error("the next of the task's middleware returned nil, want an error")
				}
			}

			if info := e.waitState(t, id, taskmill.StateAvailable); info.Attempt != 0 {
				t.Fatalf("released with attempt %d, want 0", info.Attempt)
			}
			// Longer than a third of the lease, at which a renewal would come.
			time.Sleep(200 * time.Millisecond)
			e.wantState(t, id, taskmill.StateAvailable)
		})
	}
}

// runTaskWorker returns a worker for the RunTask tests, a count of the batches
// its batch handler ran, and a channel that says when the handler of a
// "blocked" task waits for its context to end.
func runTaskWorker(t *testing.T, e *env) (*taskmill.Worker, *atomic.Int32, <-chan struct{}) {
	t.Helper()
	blocked := make(chan struct{}, 1)
	var hs taskmill.Handlers
	taskmill.AddHandler(&hs, taskmill.HandlerFunc[sendEmail](func(ctx context.Context, task *taskmill.Task[sendEmail]) error {
		switch task.Args.To {
		case "slow":
			// Longer than the lease, and than the renewal that would find
			// it ended.
			time.Sleep(3500 * time.Millisecond)
		case "blocked":
			blocked <- struct{}{}
			<-ctx.Done()
			return ctx.Err()
		}
		return nil
	}), nil)

	var batches atomic.Int32
	taskmill.AddBatchHandler[report](&hs, batchFunc(func([]*taskmill.Task[report]) { batches.Add(1) }), nil)

	// A lease long enough that a loaded machine does not miss renewals for
	// all of it: each has a third of it, which through a proxy that many
	// tests share was too little of one second.
	return newWorker(t, e, &taskmill.WorkerConfig{Handlers: &hs, LeaseDuration: 2 * time.Second}), &batches, blocked
}

func TestRunTask(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	w, batches, blocked := runTaskWorker(t, e)

	run := func(t *testing.T, ctx context.Context, id int64, want taskmill.State) *taskmill.TaskInfo {
		t.Helper()
		info, err := w.RunTask(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		if info.State != want {
			t.Fatalf("got %s, want %s", info.State, want)
		}
		return info
	}

	t.Run("future task", func(t *testing.T) {
		t.Parallel()
		id := e.enqueue(t, sendEmail{}, &taskmill.EnqueueOptions{RunAt: time.Now().Add(time.Hour)})
		run(t, t.Context(), id, taskmill.StateCompleted)
	})
	t.Run("longer than its lease", func(t *testing.T) {
		t.Parallel()
		id := e.enqueue(t, sendEmail{To: "slow"}, nil)
		if info := run(t, t.Context(), id, taskmill.StateCompleted); info.Attempt != 1 {
			t.Fatalf("completed on attempt %d; renewals did not hold the lease", info.Attempt)
		}
	})
	t.Run("canceled context", func(t *testing.T) {
		t.Parallel()
		id := e.enqueue(t, sendEmail{To: "blocked"}, nil)
		// The context ends once the handler waits for its end, and not after
		// a fixed time: a loaded machine can take longer to lease the task.
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		go func() {
			select {
			case <-blocked:
				cancel()
			case <-ctx.Done():
			}
		}()

		if info := run(t, ctx, id, taskmill.StateAvailable); info.Attempt != 0 {
			t.Fatalf("released with attempt %d, want 0", info.Attempt)
		}
	})
	t.Run("batch kind", func(t *testing.T) {
		t.Parallel()
		id := e.enqueue(t, report{}, nil)
		run(t, t.Context(), id, taskmill.StateCompleted)
		if batches.Load() != 1 {
			t.Fatalf("the batch handler ran %d times", batches.Load())
		}
	})
	t.Run("unknown kind", func(t *testing.T) {
		t.Parallel()
		e.exec(t, `INSERT INTO {s}.tasks (queue, kind, state, run_at, payload, max_attempts)
		           VALUES ('default', 'other.kind', 'available', now(), '{}', 5)`)
		id := int64(e.count(t, "SELECT max(id) FROM {s}.tasks WHERE kind = 'other.kind'"))

		if info := run(t, t.Context(), id, taskmill.StateScheduled); info.Errors[0].Code != taskmill.CodeNoHandler {
			t.Fatalf("got errors %+v", info.Errors)
		}
	})
}

func TestRunTaskOfAKindWithoutHistory(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	w := newWorker(t, e, &taskmill.WorkerConfig{
		Handlers: handlers(func(context.Context, *taskmill.Task[sendEmail]) error { return nil },
			&taskmill.HandlerOptions{DiscardCompleted: true}),
	})

	id := e.enqueue(t, sendEmail{}, nil)
	info, err := w.RunTask(t.Context(), id)
	if err != nil || info.State != taskmill.StateCompleted {
		t.Fatalf("got %v, %v", info, err)
	}
	if _, err := e.client.Get(t.Context(), id); !errors.Is(err, taskmill.ErrNotFound) {
		t.Fatalf("the completed task was kept: %v", err)
	}
}

func TestRunTaskRefuses(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	w, _, _ := runTaskWorker(t, e)
	completed := e.enqueue(t, sendEmail{}, nil)
	if _, err := w.RunTask(t.Context(), completed); err != nil {
		t.Fatal(err)
	}

	head := e.enqueue(t, sendEmail{}, &taskmill.EnqueueOptions{GroupKey: "g"})
	pending := e.enqueue(t, sendEmail{}, &taskmill.EnqueueOptions{GroupKey: "g"})
	running := e.enqueue(t, sendEmail{}, nil)
	e.exec(t, `UPDATE {s}.tasks SET state = 'running', attempt = 1, lease_token = 1,
	           lease_expires_at = now() + interval '30 seconds' WHERE id = $1`, running)

	for name, tc := range map[string]struct {
		id   int64
		want error
	}{
		"unknown":   {424242, taskmill.ErrNotFound},
		"completed": {completed, taskmill.ErrInvalidState},
		"pending":   {pending, taskmill.ErrInvalidState},
		"running":   {running, taskmill.ErrInvalidState},
	} {
		if _, err := w.RunTask(t.Context(), tc.id); !errors.Is(err, tc.want) {
			t.Errorf("%s: got %v, want %v", name, err, tc.want)
		}
	}

	// Leave the group consistent for the end-of-test check.
	for _, id := range []int64{pending, head} {
		if err := e.client.Delete(t.Context(), id); err != nil {
			t.Fatal(err)
		}
	}
}

// RunTask counts a lease from before it asks for the task, as Run does: the
// database starts the lease later. A task whose row took longer than the
// lease to arrive, as over a slow link, may have been rescued on its way,
// and run on another worker already. So RunTask renews the lease before
// any middleware or handler runs, as Run does after a slow fetch: it runs
// the task if it still holds it, and else nothing.
func TestRunTaskRenewsALeaseThatEndedOnItsWay(t *testing.T) {
	t.Parallel()
	for _, rescued := range []bool{false, true} {
		name := "kept"
		if rescued {
			name = "rescued"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			e := newEnvWith(t, slowPool(t, 1<<19, nil), nil) // 512 KiB a second from the database
			if rescued {
				direct := e.direct(t)
				runWorker(t, direct, &taskmill.WorkerConfig{}) // the maintainer, which rescues
				waitFor(t, "a maintainer", func() bool { return direct.count(t, "SELECT count(*) FROM {s}.leader") == 1 })
			}
			id := e.enqueue(t, report{Name: strings.Repeat("x", 1_000_000)}, nil) // about 2 seconds on the way

			var runs atomic.Int32
			var hs taskmill.Handlers
			taskmill.AddHandler(&hs, taskmill.HandlerFunc[report](func(context.Context, *taskmill.Task[report]) error { return nil }), nil)
			w := newWorker(t, e, &taskmill.WorkerConfig{
				Handlers: &hs,
				Middleware: []taskmill.Middleware{func(ctx context.Context, _ *taskmill.TaskInfo, next func(context.Context) error) error {
					runs.Add(1)
					return next(ctx)
				}},
				LeaseDuration: 500 * time.Millisecond,
				NoMaintainer:  true,
			})

			info, err := w.RunTask(t.Context(), id)
			if err != nil {
				t.Fatal(err)
			}
			switch n := runs.Load(); {
			case !rescued && (n != 1 || info.State != taskmill.StateCompleted):
				t.Errorf("the task ran %d times, and is %s; want once, and completed", n, info.State)
			case rescued && (n != 0 || len(info.Errors) != 1 || info.Errors[0].Code != taskmill.CodeLeaseLost):
				t.Errorf("the task ran %d times, with errors %+v; want none, and the rescue's", n, info.Errors)
			}
		})
	}
}

// RunTask tries a renewal that failed again soon, as Run does, also without
// Run: an outage that begins just before a renewal, and ends before the
// lease does, costs no task. The next renewal would come as the lease
// ends, too late.
func TestRunTaskOutlastsAnOutageShorterThanTheLease(t *testing.T) {
	t.Parallel()
	db := newProxy(t, testdb.URL(t))
	renewed := renewalTimes(make(chan time.Time, 100))
	e := newEnvWith(t, proxiedPool(t, db, 6, renewed), nil)

	const lease = 3 * time.Second
	started, ended, release := make(chan struct{}, 1), make(chan error, 1), make(chan struct{})
	w := newWorker(t, e, &taskmill.WorkerConfig{
		Handlers: handlers(func(ctx context.Context, _ *taskmill.Task[sendEmail]) error {
			started <- struct{}{}
			select {
			case <-ctx.Done():
				ended <- context.Cause(ctx)
			case <-release:
				ended <- nil
			}
			return nil
		}, nil),
		LeaseDuration: lease,
		NoMaintainer:  true,
	})

	id := e.enqueue(t, sendEmail{}, nil)
	go func() { _, _ = w.RunTask(t.Context(), id) }()
	recv(t, started, "the handler to start")
	last := recv(t, renewed, "a renewal")
	time.Sleep(time.Until(last.Add(lease/3 - 150*time.Millisecond)))
	db.cut(true)
	time.Sleep(4 * lease / 10)
	db.cut(false)

	// The handler runs on past the end of the lease that the last renewal
	// before the outage renewed.
	time.Sleep(time.Until(last.Add(lease + 300*time.Millisecond)))
	close(release)
	if err := recv(t, ended, "the handler to return"); err != nil {
		t.Errorf("the handler was stopped with %v, want it to run until it returned", err)
	}
	e.waitState(t, id, taskmill.StateCompleted)
}

// RunTask runs a task only if the statement that leases it succeeds. Here
// the statement returns the task and then fails, at the commit, by a
// deferred constraint of the application: the task is not leased, so no
// handler may run it.
func TestRunTaskRunsNoTaskWhoseLeaseFailed(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	var runs atomic.Int32

	w := newWorker(t, e, &taskmill.WorkerConfig{
		Handlers: handlers(func(context.Context, *taskmill.Task[sendEmail]) error {
			runs.Add(1)
			return nil
		}, nil),
	})

	id := e.enqueue(t, sendEmail{}, nil)
	e.exec(t, `
CREATE FUNCTION {s}.refuse_lease() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'the lease is refused at the commit';
END $$`)
	e.exec(t, `
CREATE CONSTRAINT TRIGGER refuse_lease AFTER UPDATE ON {s}.tasks
DEFERRABLE INITIALLY DEFERRED FOR EACH ROW WHEN (NEW.state = 'running') EXECUTE FUNCTION {s}.refuse_lease()`)

	if _, err := w.RunTask(t.Context(), id); err == nil || !strings.Contains(err.Error(), "the lease is refused at the commit") {
		t.Fatalf("RunTask returned %v, want the error of the statement that leases the task", err)
	}
	if n := runs.Load(); n != 0 {
		t.Fatalf("the handler ran %d times for a task that was not leased", n)
	}
	e.wantState(t, id, taskmill.StateAvailable)
}
