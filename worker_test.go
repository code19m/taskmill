package taskmill_test

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/code19m/taskmill"
	"github.com/code19m/taskmill/internal/testdb"
)

// fastRetry keeps retry tests quick.
var fastRetry = taskmill.ExponentialBackoff{Base: 10 * time.Millisecond, Max: 10 * time.Millisecond}

// testTiming shortens the worker's intervals, so that tests wait less for
// maintenance and elections.
var testTiming = taskmill.Timing{
	Maintainer:    100 * time.Millisecond,
	Election:      100 * time.Millisecond,
	LeaderLease:   2 * time.Second,
	Stats:         200 * time.Millisecond,
	ShutdownGrace: 200 * time.Millisecond,
	Reconnect:     100 * time.Millisecond,
}

// newWorker returns a worker with test timing that polls every 50 ms unless
// cfg says otherwise, and listens as testdb.ListenConfig says.
func newWorker(t *testing.T, e *env, cfg *taskmill.WorkerConfig) *taskmill.Worker {
	t.Helper()
	if cfg.PollInterval == 0 {
		cfg.PollInterval = 50 * time.Millisecond
	}
	if cfg.ListenConfig == nil {
		cfg.ListenConfig = testdb.ListenConfig(t)
	}
	w, err := taskmill.NewWorker(e.client, cfg)
	if err != nil {
		t.Fatalf("new worker: %v", err)
	}
	taskmill.SetTiming(w, testTiming)
	return w
}

// runWorker starts a worker and returns a function that stops it and waits
// for Run to return. The worker also stops when the test ends.
func runWorker(t *testing.T, e *env, cfg *taskmill.WorkerConfig) (stop func()) {
	t.Helper()
	return startWorker(t, newWorker(t, e, cfg))
}

// startWorker runs w as runWorker does.
func startWorker(t *testing.T, w *taskmill.Worker) (stop func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	stop = sync.OnceFunc(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("worker run: %v", err)
			}
		case <-time.After(60 * time.Second):
			t.Error("worker did not stop")
		}
	})
	t.Cleanup(stop)
	return stop
}

// handlers returns a handler set with one handler for sendEmail.
func handlers(fn func(ctx context.Context, task *taskmill.Task[sendEmail]) error, opts *taskmill.HandlerOptions) *taskmill.Handlers {
	var hs taskmill.Handlers
	taskmill.AddHandler(&hs, taskmill.HandlerFunc[sendEmail](fn), opts)
	return &hs
}

func defaultQueue(concurrency int) map[string]taskmill.QueueOptions {
	return map[string]taskmill.QueueOptions{taskmill.DefaultQueue: {Concurrency: concurrency}}
}

func TestWorkerCompletesTask(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	got := make(chan sendEmail, 1)
	runWorker(t, e, &taskmill.WorkerConfig{
		Handlers: handlers(func(_ context.Context, task *taskmill.Task[sendEmail]) error {
			got <- task.Args
			return nil
		}, nil),
		Queues: defaultQueue(1),
	})

	id := e.enqueue(t, sendEmail{To: "a@example.com", UserID: 1234567890123456789}, nil)
	if args := <-got; args.UserID != 1234567890123456789 || args.To != "a@example.com" {
		t.Fatalf("handler got %+v", args)
	}
	info := e.waitState(t, id, taskmill.StateCompleted)
	if info.Attempt != 1 || info.FinalizedAt.IsZero() {
		t.Fatalf("completed task: attempt %d, finalized %v", info.Attempt, info.FinalizedAt)
	}
}

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
	}{
		{"permanent", 1, ""},
		{"panic", 2, taskmill.CodePanic},
		{"timeout", 2, taskmill.CodeTimeout},
	}
	for _, tt := range tests {
		t.Run(tt.to, func(t *testing.T) {
			t.Parallel()
			id := e.enqueue(t, sendEmail{To: tt.to}, &taskmill.EnqueueOptions{MaxAttempts: 2})
			info := e.waitState(t, id, taskmill.StateDead)
			if info.Attempt != tt.attempts || len(info.Errors) != tt.attempts || info.Errors[0].Code != tt.code {
				t.Errorf("got attempt %d, errors %+v", info.Attempt, info.Errors)
			}
		})
	}
}

func TestWorkerRejectsUndecodablePayload(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	runWorker(t, e, &taskmill.WorkerConfig{
		Handlers: handlers(func(context.Context, *taskmill.Task[sendEmail]) error { return nil }, nil),
		Queues:   defaultQueue(1),
	})

	e.exec(t, `INSERT INTO {s}.tasks (queue, kind, state, run_at, payload, max_attempts)
	           VALUES ('default', 'email.send', 'available', now(), '{"user_id": "not a number"}', 5)`)
	id := int64(e.count(t, "SELECT max(id) FROM {s}.tasks"))
	info := e.waitState(t, id, taskmill.StateDead)
	if info.Attempt != 1 || info.Errors[0].Code != taskmill.CodeInvalidPayload {
		t.Fatalf("got attempt %d, errors %+v", info.Attempt, info.Errors)
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

func TestCompleteTxCommitsWithTheHandlersWrites(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.exec(t, "CREATE TABLE {s}.ledger (task_id bigint, attempt int, PRIMARY KEY (task_id, attempt))")

	staleResult := make(chan error, 1)
	runWorker(t, e, &taskmill.WorkerConfig{
		Handlers: handlers(func(ctx context.Context, task *taskmill.Task[sendEmail]) error {
			err := pgx.BeginFunc(ctx, e.pool, func(tx pgx.Tx) error {
				insert := replaceSchema("INSERT INTO {s}.ledger VALUES ($1, $2)", e.schema)
				if _, err := tx.Exec(ctx, insert, task.ID, task.Attempt); err != nil {
					return err
				}
				if task.Args.To == "stale" && task.Attempt == 1 {
					// Another attempt takes the task over before this one
					// completes it.
					bump := replaceSchema("UPDATE {s}.tasks SET lease_token = lease_token + 1 WHERE id = $1", e.schema)
					if _, err := tx.Exec(ctx, bump, task.ID); err != nil {
						return err
					}
				}
				return task.CompleteTx(ctx, taskmill.Pgx(tx))
			})
			if task.Args.To == "stale" && task.Attempt == 1 {
				staleResult <- err
			}
			return err
		}, &taskmill.HandlerOptions{Retry: fastRetry}),
		Queues: defaultQueue(1),
	})

	id := e.enqueue(t, sendEmail{To: "ok"}, nil)
	e.waitState(t, id, taskmill.StateCompleted)
	if n := e.count(t, "SELECT count(*) FROM {s}.ledger WHERE task_id = $1", id); n != 1 {
		t.Fatalf("ledger rows: got %d, want 1", n)
	}

	// The stale attempt's CompleteTx fails, which rolls back its writes; the
	// failed attempt is retried and completes.
	staleID := e.enqueue(t, sendEmail{To: "stale"}, nil)
	if err := <-staleResult; !errors.Is(err, taskmill.ErrLeaseLost) {
		t.Fatalf("stale CompleteTx: got %v, want ErrLeaseLost", err)
	}
	info := e.waitState(t, staleID, taskmill.StateCompleted)
	if info.Attempt != 2 {
		t.Fatalf("completed on attempt %d, want 2", info.Attempt)
	}
	if n := e.count(t, "SELECT count(*) FROM {s}.ledger WHERE task_id = $1 AND attempt = 1", staleID); n != 0 {
		t.Fatal("the stale attempt committed its writes")
	}
	if n := e.count(t, "SELECT count(*) FROM {s}.ledger WHERE task_id = $1", staleID); n != 1 {
		t.Fatalf("ledger rows: got %d, want 1", n)
	}
}

func TestWorkerCancelsRunningTask(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	started := make(chan struct{})
	runWorker(t, e, &taskmill.WorkerConfig{
		Handlers: handlers(func(ctx context.Context, _ *taskmill.Task[sendEmail]) error {
			close(started)
			<-ctx.Done()
			return context.Cause(ctx)
		}, &taskmill.HandlerOptions{Timeout: time.Minute}),
		Queues: defaultQueue(1),
	})

	id := e.enqueue(t, sendEmail{}, nil)
	recv(t, started, "a handler to start")
	e.waitListening(t) // a missed notification would wait for the next heartbeat
	if err := e.client.Cancel(t.Context(), id); err != nil {
		t.Fatal(err)
	}
	e.waitState(t, id, taskmill.StateCanceled)
}

func TestWorkerDrainsRunningTasksOnShutdown(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	started := make(chan struct{}, 1)
	stop := runWorker(t, e, &taskmill.WorkerConfig{
		Handlers: handlers(func(context.Context, *taskmill.Task[sendEmail]) error {
			started <- struct{}{}
			time.Sleep(300 * time.Millisecond)
			return nil
		}, nil),
		Queues: defaultQueue(1),
	})

	id := e.enqueue(t, sendEmail{}, nil)
	recv(t, started, "a handler to start")
	stop()
	e.wantState(t, id, taskmill.StateCompleted)
	if n := e.count(t, "SELECT count(*) FROM {s}.workers"); n != 0 {
		t.Fatalf("stopped worker is still registered")
	}
}

func TestWorkerReleasesTasksThatOutliveTheDrain(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	started := make(chan struct{}, 1)
	stop := runWorker(t, e, &taskmill.WorkerConfig{
		Handlers: handlers(func(ctx context.Context, _ *taskmill.Task[sendEmail]) error {
			started <- struct{}{}
			<-ctx.Done()
			return ctx.Err()
		}, &taskmill.HandlerOptions{Timeout: time.Minute}),
		Queues:       defaultQueue(1),
		DrainTimeout: 50 * time.Millisecond,
	})

	id := e.enqueue(t, sendEmail{}, nil)
	recv(t, started, "a handler to start")
	stop()
	info := e.get(t, id)
	if info.State != taskmill.StateAvailable || info.Attempt != 0 {
		t.Fatalf("released task: got %s with attempt %d, want available with 0", info.State, info.Attempt)
	}
}

func TestHeartbeatsKeepLongTasksLeased(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	var runs atomic.Int32
	runWorker(t, e, &taskmill.WorkerConfig{
		Handlers: handlers(func(context.Context, *taskmill.Task[sendEmail]) error {
			runs.Add(1)
			time.Sleep(3500 * time.Millisecond) // more than three lease durations
			return nil
		}, nil),
		Queues: defaultQueue(2),
		// Without heartbeats, the task is rescued and runs again. The lease is
		// long enough that a loaded machine does not miss heartbeats for all of
		// it.
		LeaseDuration: time.Second,
	})

	id := e.enqueue(t, sendEmail{}, nil)
	info := e.waitState(t, id, taskmill.StateCompleted)
	if runs.Load() != 1 || info.Attempt != 1 {
		t.Fatalf("task ran %d times over %d attempts; heartbeats did not hold the lease", runs.Load(), info.Attempt)
	}
}

func TestFIFOGroupRunsOneAtATimeInOrder(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	var mu sync.Mutex
	var order []int64
	var active atomic.Int32
	var overlap atomic.Bool
	runWorker(t, e, &taskmill.WorkerConfig{
		Handlers: handlers(func(_ context.Context, task *taskmill.Task[sendEmail]) error {
			if active.Add(1) > 1 {
				overlap.Store(true)
			}
			defer active.Add(-1)
			time.Sleep(5 * time.Millisecond)
			if task.Attempt == 1 && task.Args.UserID%4 == 0 {
				return errors.New("transient")
			}
			mu.Lock()
			order = append(order, task.Args.UserID)
			mu.Unlock()
			return nil
		}, &taskmill.HandlerOptions{Retry: fastRetry}),
		Queues: defaultQueue(8),
	})

	const n = 20
	params := make([]taskmill.EnqueueParams, n)
	for i := range params {
		params[i] = taskmill.EnqueueParams{Args: sendEmail{UserID: int64(i)}, Options: &taskmill.EnqueueOptions{GroupKey: "account-7"}}
	}
	res, err := e.client.EnqueueMany(t.Context(), params)
	if err != nil {
		t.Fatal(err)
	}
	e.waitState(t, res[n-1].ID, taskmill.StateCompleted)

	mu.Lock()
	defer mu.Unlock()
	if overlap.Load() {
		t.Fatal("two tasks of one group ran at the same time")
	}
	if !slices.IsSorted(order) || len(order) != n {
		t.Fatalf("group ran out of order: %v", order)
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

func TestWorkerConcurrency(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	var active, peak atomic.Int32
	runWorker(t, e, &taskmill.WorkerConfig{
		Handlers: handlers(func(context.Context, *taskmill.Task[sendEmail]) error {
			n := active.Add(1)
			defer active.Add(-1)
			storeMax(&peak, n)
			time.Sleep(50 * time.Millisecond)
			return nil
		}, nil),
		Queues: defaultQueue(2),
	})

	var last int64
	for range 8 {
		last = e.enqueue(t, sendEmail{}, nil)
	}
	e.waitState(t, last, taskmill.StateCompleted)
	if p := peak.Load(); p != 2 {
		t.Fatalf("peak concurrency %d, want 2", p)
	}
}

func TestQueueConcurrencyLimitAcrossWorkers(t *testing.T) {
	t.Parallel()
	e := newEnvWith(t, testdb.PoolSize(t, 16), nil)
	e.setDefaultQueue(t, &taskmill.QueueSettings{MaxRunning: 1})
	var active, peak atomic.Int32
	hs := handlers(func(context.Context, *taskmill.Task[sendEmail]) error {
		n := active.Add(1)
		defer active.Add(-1)
		storeMax(&peak, n)
		time.Sleep(30 * time.Millisecond)
		return nil
	}, nil)
	runWorker(t, e, &taskmill.WorkerConfig{Handlers: hs, Queues: defaultQueue(5)})
	runWorker(t, e, &taskmill.WorkerConfig{Handlers: hs, Queues: defaultQueue(5)})

	var last int64
	for range 10 {
		last = e.enqueue(t, sendEmail{}, nil)
	}
	e.waitState(t, last, taskmill.StateCompleted)
	if p := peak.Load(); p != 1 {
		t.Fatalf("peak concurrency %d across workers, want 1", p)
	}
}

func TestScheduledTaskRunsAfterItsRunTime(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	started := make(chan time.Time, 1)
	runWorker(t, e, &taskmill.WorkerConfig{
		Handlers: handlers(func(context.Context, *taskmill.Task[sendEmail]) error {
			started <- time.Now()
			return nil
		}, nil),
		Queues: defaultQueue(1),
	})

	runAt := time.Now().Add(500 * time.Millisecond)
	e.enqueue(t, sendEmail{}, &taskmill.EnqueueOptions{RunAt: runAt})
	select {
	case at := <-started:
		if at.Before(runAt) {
			t.Fatalf("task started %s before its run time", runAt.Sub(at))
		}
		// The target is 1.5 s (docs/benchmarks.md); the bound here
		// leaves room for a loaded test machine.
		if late := at.Sub(runAt); late > 5*time.Second {
			t.Fatalf("task started %s late", late)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("scheduled task did not run")
	}
}

func TestNotificationsWakeIdleWorkers(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	started := make(chan struct{}, 1)
	runWorker(t, e, &taskmill.WorkerConfig{
		Handlers: handlers(func(context.Context, *taskmill.Task[sendEmail]) error {
			started <- struct{}{}
			return nil
		}, nil),
		Queues:       defaultQueue(1),
		PollInterval: time.Minute,
	})
	e.waitListening(t)

	start := time.Now()
	e.enqueue(t, sendEmail{}, nil)
	select {
	case <-started:
		if d := time.Since(start); d > time.Second {
			t.Fatalf("pickup took %s", d)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("notification did not wake the worker")
	}
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

func TestNewWorkerRejectsInvalidConfig(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	noop := taskmill.HandlerFunc[sendEmail](func(context.Context, *taskmill.Task[sendEmail]) error { return nil })

	var dup taskmill.Handlers
	taskmill.AddHandler(&dup, noop, nil)
	taskmill.AddHandler(&dup, noop, nil)
	var noKind taskmill.Handlers
	taskmill.AddHandler(&noKind, taskmill.HandlerFunc[emptyKind](func(context.Context, *taskmill.Task[emptyKind]) error {
		return nil
	}), nil)

	tests := []struct {
		name string
		cfg  *taskmill.WorkerConfig
	}{
		{"kind that is empty", &taskmill.WorkerConfig{Handlers: &noKind}},
		{"duplicate kind", &taskmill.WorkerConfig{Handlers: &dup, Queues: defaultQueue(1)}},
		{"queues without handlers", &taskmill.WorkerConfig{Queues: defaultQueue(1)}},
		{"negative concurrency", &taskmill.WorkerConfig{Handlers: handlers(noop, nil), Queues: defaultQueue(-1)}},
		{"tiny lease", &taskmill.WorkerConfig{LeaseDuration: time.Millisecond}},
		{"negative drain", &taskmill.WorkerConfig{DrainTimeout: -time.Second}},
		{"schedule without args", &taskmill.WorkerConfig{Schedules: []taskmill.Schedule{{Name: "x", Cron: "* * * * *"}}}},
		{"schedule that never runs", &taskmill.WorkerConfig{Schedules: []taskmill.Schedule{
			{Name: "x", Cron: "0 0 31 2 *", Args: sendEmail{}},
		}}},
		{"schedule with an @every macro", &taskmill.WorkerConfig{Schedules: []taskmill.Schedule{
			{Name: "x", Cron: "@every 30s", Args: sendEmail{}},
		}}},
		{"schedule with an oversized payload", &taskmill.WorkerConfig{Schedules: []taskmill.Schedule{
			{Name: "x", Cron: "@daily", Args: sendEmail{To: strings.Repeat("x", 1<<20)}},
		}}},
		{"schedule with a bad queue", &taskmill.WorkerConfig{Schedules: []taskmill.Schedule{
			{Name: "x", Cron: "@daily", Args: sendEmail{}, Options: &taskmill.EnqueueOptions{Queue: "a\x00b"}},
		}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := taskmill.NewWorker(e.client, tt.cfg)
			if !errors.Is(err, taskmill.ErrInvalidArgument) {
				t.Fatalf("got %v, want ErrInvalidArgument", err)
			}
			if n := strings.Count(err.Error(), "taskmill:"); n != 1 {
				t.Errorf("%q names the package %d times", err, n)
			}
		})
	}
}

func TestRunTwiceFails(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	w, err := taskmill.NewWorker(e.client, &taskmill.WorkerConfig{NoMaintainer: true, ListenConfig: testdb.ListenConfig(t)})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	// Listening comes after registering: canceling as soon as the worker's
	// row shows could stop Run while it still waits for that insert's reply.
	e.waitListening(t)

	second, cancelSecond := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancelSecond()
	if err := w.Run(second); err == nil || second.Err() != nil {
		t.Fatalf("second Run: got %v, want an immediate error", err)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

// storeMax raises v to n if n is larger.
func storeMax(v *atomic.Int32, n int32) {
	for {
		p := v.Load()
		if n <= p || v.CompareAndSwap(p, n) {
			return
		}
	}
}

func TestHeartbeatSkipsLockedTasks(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	const lease = 1500 * time.Millisecond
	started := make(chan int64, 2)
	release := make(chan struct{})
	runWorker(t, e, &taskmill.WorkerConfig{
		Handlers: handlers(func(ctx context.Context, task *taskmill.Task[sendEmail]) error {
			started <- task.ID
			select {
			case <-release:
				return nil
			case <-ctx.Done():
				return context.Cause(ctx)
			}
		}, nil),
		Queues:        defaultQueue(2),
		LeaseDuration: lease,
	})
	locked := e.enqueue(t, sendEmail{To: "locked"}, nil)
	other := e.enqueue(t, sendEmail{To: "other"}, nil)
	recv(t, started, "a handler to start")
	recv(t, started, "a handler to start")

	// An application transaction holds the row lock of one running task, as
	// CancelTx does until it commits. The other task's lease is still renewed.
	tx := e.begin(t)
	var lockedAt time.Time
	err := tx.QueryRow(t.Context(), replaceSchema("SELECT clock_timestamp() FROM {s}.tasks WHERE id = $1 FOR UPDATE", e.schema),
		locked).Scan(&lockedAt)
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the other task's lease to be renewed", func() bool {
		return e.count(t, "SELECT count(*) FROM {s}.tasks WHERE id = $1 AND lease_expires_at > $2",
			other, lockedAt.Add(lease)) == 1
	})
	if err := tx.Rollback(t.Context()); err != nil {
		t.Fatal(err)
	}
	close(release)
	if info := e.waitState(t, other, taskmill.StateCompleted); info.Attempt != 1 {
		t.Fatalf("task ran %d times; it lost its lease", info.Attempt)
	}
}

// reportBatch completes every report in its batches.
type reportBatch struct{}

func (reportBatch) HandleBatch(_ context.Context, tasks []*taskmill.Task[report]) []error {
	return make([]error, len(tasks))
}

func TestBatchKindsDoNotWidenOtherQueuesFetches(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	started := make(chan struct{}, 5)
	release := make(chan struct{})
	var hs taskmill.Handlers
	taskmill.AddHandler(&hs, taskmill.HandlerFunc[sendEmail](func(context.Context, *taskmill.Task[sendEmail]) error {
		started <- struct{}{}
		<-release
		return nil
	}), nil)
	taskmill.AddBatchHandler[report](&hs, reportBatch{}, nil)
	for range 5 {
		e.enqueue(t, sendEmail{}, nil)
	}
	runWorker(t, e, &taskmill.WorkerConfig{
		Handlers: &hs,
		Queues:   map[string]taskmill.QueueOptions{taskmill.DefaultQueue: {Concurrency: 1}, "reports": {Concurrency: 1}},
	})

	recv(t, started, "a handler to start")
	n := e.count(t, "SELECT count(*) FROM {s}.tasks WHERE state = 'running'")
	close(release)
	if n != 1 {
		t.Fatalf("%d tasks leased for one slot", n)
	}
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

func TestFailureAfterRolledBackCompleteTxIsRecorded(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	runWorker(t, e, &taskmill.WorkerConfig{
		Handlers: handlers(func(ctx context.Context, task *taskmill.Task[sendEmail]) error {
			tx, err := e.pool.Begin(ctx)
			if err != nil {
				return err
			}
			if err := task.CompleteTx(ctx, taskmill.Pgx(tx)); err != nil {
				return err
			}
			_ = tx.Rollback(ctx)
			return errors.New("publish failed")
		}, nil),
		Queues: defaultQueue(1),
	})

	id := e.enqueue(t, sendEmail{}, &taskmill.EnqueueOptions{MaxAttempts: 1})
	info := e.waitState(t, id, taskmill.StateDead)
	if len(info.Errors) != 1 || info.Errors[0].Message != "publish failed" {
		t.Fatalf("got errors %+v", info.Errors)
	}
}

// pointerArgs is used through a pointer, with Kind on the value.
type pointerArgs struct {
	N int `json:"n"`
}

func (pointerArgs) Kind() string { return "pointer.args" }

func TestHandlerForPointerArgs(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	got := make(chan int, 1)
	var hs taskmill.Handlers
	taskmill.AddHandler(&hs, taskmill.HandlerFunc[*pointerArgs](func(_ context.Context, task *taskmill.Task[*pointerArgs]) error {
		got <- task.Args.N
		return nil
	}), nil)
	runWorker(t, e, &taskmill.WorkerConfig{Handlers: &hs, Queues: defaultQueue(1)})

	e.enqueue(t, &pointerArgs{N: 7}, nil)
	if n := recv(t, got, "the handler's result"); n != 7 {
		t.Fatalf("got args %d, want 7", n)
	}
}

func TestHandlerForInterfaceArgsIsRejected(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	var hs taskmill.Handlers
	taskmill.AddHandler(&hs, taskmill.HandlerFunc[taskmill.Args](func(context.Context, *taskmill.Task[taskmill.Args]) error {
		return nil
	}), nil)
	_, err := taskmill.NewWorker(e.client, &taskmill.WorkerConfig{Handlers: &hs, Queues: defaultQueue(1)})
	if !errors.Is(err, taskmill.ErrInvalidArgument) {
		t.Fatalf("got %v, want ErrInvalidArgument", err)
	}
}

func TestWorkerWithUnusableListenConfigPolls(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	runWorker(t, e, &taskmill.WorkerConfig{
		Handlers:     noopHandlers(),
		Queues:       defaultQueue(1),
		ListenConfig: &pgx.ConnConfig{}, // not created by pgx.ParseConfig
	})

	id := e.enqueue(t, sendEmail{}, nil)
	e.waitState(t, id, taskmill.StateCompleted)
}

func TestFailedSettingsRefreshKeepsQueuesPaused(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	if err := e.client.PauseQueue(t.Context(), taskmill.DefaultQueue); err != nil {
		t.Fatal(err)
	}
	runWorker(t, e, &taskmill.WorkerConfig{
		Handlers: noopHandlers(),
		Queues:   map[string]taskmill.QueueOptions{taskmill.DefaultQueue: {}, "canary": {}},
	})
	e.waitListening(t)

	// A settings refresh waits for a lock on the queues, and is canceled.
	tx := e.begin(t)
	e.txExec(t, tx, "LOCK TABLE {s}.queues IN ACCESS EXCLUSIVE MODE")
	e.exec(t, "SELECT pg_notify($1, 'q|default')", e.schema)
	pid := e.waitBlocked(t, "{s}.queues WHERE name = ANY")
	e.exec(t, "SELECT pg_cancel_backend($1)", pid)
	if err := tx.Rollback(t.Context()); err != nil {
		t.Fatal(err)
	}

	// The canary, enqueued after the paused task into a queue that is not
	// paused, shows that the worker is fetching.
	id := e.enqueue(t, sendEmail{}, nil)
	e.waitState(t, e.enqueue(t, sendEmail{}, &taskmill.EnqueueOptions{Queue: "canary"}), taskmill.StateCompleted)
	e.wantState(t, id, taskmill.StateAvailable)
}

func TestAttemptThatLostItsLeaseIsCanceledAndDiscarded(t *testing.T) {
	t.Parallel()
	t.Run("taken over", func(t *testing.T) {
		t.Parallel()
		testLostLease(t, func(e *env, id int64) (release func()) {
			e.exec(t, "UPDATE {s}.tasks SET lease_token = lease_token + 1 WHERE id = $1", id)
			return func() {}
		})
	})
	t.Run("row locked past the lease", func(t *testing.T) {
		t.Parallel()
		testLostLease(t, func(e *env, id int64) (release func()) {
			tx := e.begin(t)
			e.txExec(t, tx, "SELECT FROM {s}.tasks WHERE id = $1 FOR UPDATE", id)
			return func() { _ = tx.Rollback(context.Background()) }
		})
	})
}

// testLostLease makes a running attempt lose its lease with lose, and checks
// that the attempt is canceled and its success discarded.
func testLostLease(t *testing.T, lose func(e *env, id int64) (release func())) {
	t.Helper()
	e := newEnv(t)
	started := make(chan int64, 1)
	causes := make(chan error, 1)
	runWorker(t, e, &taskmill.WorkerConfig{
		Handlers: handlers(func(ctx context.Context, task *taskmill.Task[sendEmail]) error {
			if task.Attempt > 1 {
				return nil
			}
			started <- task.ID
			<-ctx.Done()
			causes <- context.Cause(ctx)
			return nil // a success that must be discarded
		}, nil),
		Queues:        defaultQueue(1),
		LeaseDuration: 300 * time.Millisecond,
	})
	id := e.enqueue(t, sendEmail{}, nil)
	recv(t, started, "a handler to start")

	release := lose(e, id)
	select {
	case cause := <-causes:
		if !errors.Is(cause, taskmill.ErrLeaseLost) {
			t.Fatalf("attempt canceled with %v, want ErrLeaseLost", cause)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("the attempt was not canceled")
	}
	release()

	// The first attempt's success was discarded: the task is rescued once its
	// lease expires, and completes on its second attempt.
	info := e.waitState(t, id, taskmill.StateCompleted)
	if info.Attempt != 2 || len(info.Errors) != 1 || info.Errors[0].Code != taskmill.CodeLeaseExpired {
		t.Fatalf("got attempt %d with errors %+v, want attempt 2 after a lost lease", info.Attempt, info.Errors)
	}
}

// failingBatch fails or panics on its first batch.
type failingBatch struct {
	panics bool
	calls  atomic.Int32
}

func (f *failingBatch) HandleBatch(_ context.Context, tasks []*taskmill.Task[sendEmail]) []error {
	if f.calls.Add(1) > 1 {
		return make([]error, len(tasks))
	}
	if f.panics {
		panic("broker client crashed")
	}
	return nil // no result for any task
}

func TestBatchHandlerFailures(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, message string
		code          taskmill.ErrorCode
		panics        bool
	}{
		{"panic", "panic: broker client crashed", taskmill.CodePanic, true},
		{"missing results", "the batch handler returned no result for this task", "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			e := newEnv(t)
			var hs taskmill.Handlers
			taskmill.AddBatchHandler[sendEmail](&hs, &failingBatch{panics: tc.panics}, &taskmill.HandlerOptions{Retry: fastRetry})
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

func TestHandlerThatIgnoresShutdownIsReleased(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	started := make(chan struct{}, 1)
	unblock := make(chan struct{})
	returned := make(chan struct{})
	stop := runWorker(t, e, &taskmill.WorkerConfig{
		Handlers: handlers(func(context.Context, *taskmill.Task[sendEmail]) error {
			defer close(returned)
			started <- struct{}{}
			<-unblock // ignores its context
			return nil
		}, nil),
		Queues:       defaultQueue(1),
		DrainTimeout: 50 * time.Millisecond,
	})
	id := e.enqueue(t, sendEmail{}, nil)
	recv(t, started, "a handler to start")
	stop()

	info := e.get(t, id)
	if info.State != taskmill.StateAvailable || info.Attempt != 0 {
		t.Fatalf("got %s with attempt %d, want available with 0", info.State, info.Attempt)
	}
	// The abandoned handler's late success changes nothing.
	close(unblock)
	recv(t, returned, "the handler to return")
	e.wantState(t, id, taskmill.StateAvailable)
}

func TestShutdownFitsTheGracePeriod(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	const drain = time.Second
	started, unblock := make(chan struct{}, 2), make(chan struct{})
	t.Cleanup(func() { close(unblock) })
	// The worker's own timing: the grace after the drain is a fixed 5 seconds.
	w, err := taskmill.NewWorker(e.client, &taskmill.WorkerConfig{
		Handlers: handlers(func(ctx context.Context, task *taskmill.Task[sendEmail]) error {
			started <- struct{}{}
			if task.Args.UserID == 1 {
				<-unblock // ignores its context
				return nil
			}
			<-ctx.Done()
			return ctx.Err()
		}, &taskmill.HandlerOptions{Timeout: time.Hour}),
		Queues:       defaultQueue(2),
		DrainTimeout: drain,
		NoMaintainer: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	ids := []int64{e.enqueue(t, sendEmail{UserID: 1}, nil), e.enqueue(t, sendEmail{UserID: 2}, nil)}
	recv(t, started, "the first task to start")
	recv(t, started, "the second task to start")

	// SIGTERM, through signal.NotifyContext. A process stops in time if its
	// termination grace period is DrainTimeout plus 10 seconds.
	stopping := time.Now()
	cancel()
	if err := recv(t, done, "Run to return"); err != nil {
		t.Fatal(err)
	}
	if took := time.Since(stopping); took < drain || took > drain+10*time.Second {
		t.Errorf("Run returned %s after its context ended, want between %s and %s", took, drain, drain+10*time.Second)
	}
	for _, id := range ids {
		if info := e.get(t, id); info.State != taskmill.StateAvailable || info.Attempt != 0 {
			t.Errorf("task %d is %s with attempt %d, want available with 0", id, info.State, info.Attempt)
		}
	}
	if n := e.count(t, "SELECT count(*) FROM {s}.workers"); n != 0 {
		t.Error("the stopped worker is still registered")
	}
}

func TestHealthFollowsTheDatabase(t *testing.T) {
	t.Parallel()
	db := newProxy(t, testdb.URL(t))
	cfg, err := pgxpool.ParseConfig(testdb.URL(t))
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.Host, cfg.ConnConfig.Port, cfg.ConnConfig.Fallbacks = db.host, db.port, nil
	pool, err := pgxpool.NewWithConfig(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	e := newEnvWith(t, pool, nil)
	const lease = 600 * time.Millisecond
	w := newWorker(t, e, &taskmill.WorkerConfig{LeaseDuration: lease})
	if w.Health() == nil {
		t.Fatal("a worker that does not run is healthy")
	}
	stop := startWorker(t, w)
	waitFor(t, "the worker to be healthy", func() bool { return w.Health() == nil })

	// Without the database, it turns unhealthy once it could have lost its
	// leases, and healthy again at its first heartbeat once it is back.
	cut := time.Now()
	db.cut(true)
	waitFor(t, "the worker to be unhealthy", func() bool { return w.Health() != nil })
	if d := time.Since(cut); d < 2*lease/3 {
		t.Errorf("the worker turned unhealthy %s after the database went away, before its lease of %s could end", d, lease)
	}
	db.cut(false)
	waitFor(t, "the worker to be healthy again", func() bool { return w.Health() == nil })

	stop()
	if w.Health() == nil {
		t.Error("a stopped worker is healthy")
	}
}

func TestListenerReconnects(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	started := make(chan struct{}, 1)
	runWorker(t, e, &taskmill.WorkerConfig{
		Handlers: handlers(func(context.Context, *taskmill.Task[sendEmail]) error {
			started <- struct{}{}
			return nil
		}, nil),
		Queues:       defaultQueue(1),
		PollInterval: time.Minute, // only a notification can wake the worker in time
	})
	e.waitListening(t)
	old := e.listenerPID(t)
	e.exec(t, "SELECT pg_terminate_backend($1)", old)
	waitFor(t, "the listener to reconnect", func() bool {
		pid := e.listenerPID(t)
		return pid != 0 && pid != old
	})

	e.enqueue(t, sendEmail{}, nil)
	select {
	case <-started:
	case <-time.After(10 * time.Second):
		t.Fatal("the reconnected listener did not wake the worker")
	}
}

func TestCompleteTxWithDatabaseSQL(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	db := openSQL(t)
	runWorker(t, e, &taskmill.WorkerConfig{
		Handlers: handlers(func(ctx context.Context, task *taskmill.Task[sendEmail]) error {
			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				return err
			}
			defer func() { _ = tx.Rollback() }()
			if err := task.CompleteTx(ctx, taskmill.SQL(tx)); err != nil {
				return err
			}
			return tx.Commit()
		}, nil),
		Queues: defaultQueue(1),
	})

	group := &taskmill.EnqueueOptions{GroupKey: "g"}
	first := e.enqueue(t, sendEmail{}, group)
	second := e.enqueue(t, sendEmail{}, group)
	e.waitState(t, first, taskmill.StateCompleted)
	e.waitState(t, second, taskmill.StateCompleted)
}

func TestCompleteTxThenMoreWorkKeepsTheAttempt(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	causes := make(chan error, 2)
	runWorker(t, e, &taskmill.WorkerConfig{
		Handlers: handlers(func(ctx context.Context, task *taskmill.Task[sendEmail]) error {
			err := pgx.BeginFunc(ctx, e.pool, func(tx pgx.Tx) error {
				if err := task.CompleteTx(ctx, taskmill.Pgx(tx)); err != nil {
					return err
				}
				// More work in the same transaction, for longer than the lease.
				time.Sleep(time.Second)
				return nil
			})
			causes <- context.Cause(ctx)
			return err
		}, nil),
		Queues:        defaultQueue(1),
		LeaseDuration: 300 * time.Millisecond,
	})

	id := e.enqueue(t, sendEmail{}, nil)
	if info := e.waitState(t, id, taskmill.StateCompleted); info.Attempt != 1 {
		t.Fatalf("completed on attempt %d, want 1", info.Attempt)
	}
	if cause := <-causes; cause != nil {
		t.Fatalf("the attempt was canceled: %v", cause)
	}
}

func TestLockedGroupDoesNotHoldUpOtherCompletions(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	release := make(chan struct{})
	started := make(chan struct{}, 10)
	runWorker(t, e, &taskmill.WorkerConfig{
		Handlers: handlers(func(context.Context, *taskmill.Task[sendEmail]) error {
			started <- struct{}{}
			<-release
			return nil
		}, nil),
		Queues: defaultQueue(10),
	})
	held := e.enqueue(t, sendEmail{}, &taskmill.EnqueueOptions{GroupKey: "held"})
	free := e.enqueue(t, sendEmail{}, &taskmill.EnqueueOptions{GroupKey: "free"})
	var plain []int64
	for range 3 {
		plain = append(plain, e.enqueue(t, sendEmail{}, nil))
	}
	for range 5 {
		recv(t, started, "a handler to start")
	}

	// A producer's transaction holds one group's row, as an open EnqueueTx
	// into that group does, while all five tasks complete together.
	tx := e.begin(t)
	e.txExec(t, tx, "SELECT FROM {s}.groups WHERE group_key = 'held' FOR UPDATE")
	close(release)
	for _, id := range append(plain, free) {
		e.waitState(t, id, taskmill.StateCompleted)
	}
	e.wantState(t, held, taskmill.StateRunning)
	if err := tx.Commit(t.Context()); err != nil {
		t.Fatal(err)
	}
	if info := e.waitState(t, held, taskmill.StateCompleted); info.Attempt != 1 {
		t.Fatalf("completed on attempt %d, want 1", info.Attempt)
	}
}

func TestBatchFillsWithoutLeasingOtherKinds(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	started := make(chan struct{}, 3)
	release := make(chan struct{})
	var mu sync.Mutex
	var sizes []int
	var hs taskmill.Handlers
	taskmill.AddHandler(&hs, taskmill.HandlerFunc[sendEmail](func(context.Context, *taskmill.Task[sendEmail]) error {
		started <- struct{}{}
		<-release
		return nil
	}), nil)
	taskmill.AddBatchHandler[report](&hs, batchFunc(func(tasks []*taskmill.Task[report]) {
		mu.Lock()
		defer mu.Unlock()
		sizes = append(sizes, len(tasks))
	}), &taskmill.HandlerOptions{BatchSize: 4})

	// Batch tasks first, then single tasks, all in one queue.
	reports := &taskmill.EnqueueOptions{Queue: taskmill.DefaultQueue, Priority: taskmill.PriorityNormal}
	for range 4 {
		e.enqueue(t, report{}, reports)
	}
	var last int64
	for range 3 {
		last = e.enqueue(t, sendEmail{}, nil)
	}
	runWorker(t, e, &taskmill.WorkerConfig{Handlers: &hs, Queues: defaultQueue(1)})

	// The first fetch takes one report and fills its batch with the other
	// three; the next takes one email, which then holds the only slot.
	recv(t, started, "a handler to start")
	n := e.count(t, "SELECT count(*) FROM {s}.tasks WHERE state = 'running' AND kind = 'email.send'")
	close(release)
	if n != 1 {
		t.Fatalf("%d emails leased for one slot", n)
	}
	e.waitState(t, last, taskmill.StateCompleted)
	mu.Lock()
	defer mu.Unlock()
	if !slices.Equal(sizes, []int{4}) {
		t.Fatalf("got batches of %v, want one batch of 4", sizes)
	}
}

// batchFunc is a batch handler for reports that completes them all.
type batchFunc func(tasks []*taskmill.Task[report])

func (f batchFunc) HandleBatch(_ context.Context, tasks []*taskmill.Task[report]) []error {
	f(tasks)
	return make([]error, len(tasks))
}

func TestHeldGroupsDoNotTieUpConnections(t *testing.T) {
	t.Parallel()
	// A small pool, as pgxpool's default is on a small machine.
	e := newEnvWith(t, testdb.PoolSize(t, 4), nil)
	const lease = 300 * time.Millisecond
	started := make(chan struct{}, 8)
	release := make(chan struct{})
	runWorker(t, e, &taskmill.WorkerConfig{
		Handlers: handlers(func(context.Context, *taskmill.Task[sendEmail]) error {
			started <- struct{}{}
			<-release
			return nil
		}, nil),
		Queues:        defaultQueue(8),
		LeaseDuration: lease,
	})
	var grouped []int64
	for i := range 6 {
		grouped = append(grouped, e.enqueue(t, sendEmail{}, &taskmill.EnqueueOptions{GroupKey: strconv.Itoa(i)}))
	}
	for range grouped {
		recv(t, started, "a handler to start")
	}

	// Another application's transaction holds every group's row, for several
	// lease durations, while the six tasks finish.
	tx, err := testdb.Pool(t).Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if _, err := tx.Exec(t.Context(), replaceSchema("SELECT FROM {s}.groups FOR UPDATE", e.schema)); err != nil {
		t.Fatal(err)
	}
	close(release)
	plain := e.enqueue(t, sendEmail{}, nil)
	e.waitState(t, plain, taskmill.StateCompleted)
	time.Sleep(4 * lease) // the held tasks' leases must survive, which needs free connections
	if err := tx.Commit(t.Context()); err != nil {
		t.Fatal(err)
	}

	for _, id := range grouped {
		if info := e.waitState(t, id, taskmill.StateCompleted); info.Attempt != 1 {
			t.Fatalf("task %d completed on attempt %d; it lost its lease", id, info.Attempt)
		}
	}
}

// runTaskWorker returns a worker for the RunTask tests, and a count of the
// batches its batch handler ran.
func runTaskWorker(t *testing.T, e *env) (*taskmill.Worker, *atomic.Int32) {
	t.Helper()
	var hs taskmill.Handlers
	taskmill.AddHandler(&hs, taskmill.HandlerFunc[sendEmail](func(ctx context.Context, task *taskmill.Task[sendEmail]) error {
		switch task.Args.To {
		case "slow":
			time.Sleep(3500 * time.Millisecond) // more than three leases
		case "blocked":
			<-ctx.Done()
			return ctx.Err()
		}
		return nil
	}), nil)
	var batches atomic.Int32
	taskmill.AddBatchHandler[report](&hs, batchFunc(func([]*taskmill.Task[report]) { batches.Add(1) }), nil)
	// A lease long enough that a loaded machine does not miss heartbeats for
	// all of it.
	return newWorker(t, e, &taskmill.WorkerConfig{Handlers: &hs, LeaseDuration: time.Second}), &batches
}

func TestRunTask(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	w, batches := runTaskWorker(t, e)
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
			t.Fatalf("completed on attempt %d; heartbeats did not hold the lease", info.Attempt)
		}
	})
	t.Run("canceled context", func(t *testing.T) {
		t.Parallel()
		id := e.enqueue(t, sendEmail{To: "blocked"}, nil)
		ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
		defer cancel()
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
	w, _ := runTaskWorker(t, e)
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

func TestFetchStaysOnItsIndexWithStaleStatistics(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	// The statistics see an empty queue; then a backlog arrives, as it does
	// faster than autovacuum analyzes the table.
	e.exec(t, "ANALYZE {s}.tasks")
	e.exec(t, `INSERT INTO {s}.tasks (queue, kind, state, run_at, payload, max_attempts)
	           SELECT 'default', 'email.send', 'available', now(), '{}', 1 FROM generate_series(1, 50000)`)

	plan, err := taskmill.ExplainFetch(t.Context(), e.client, taskmill.DefaultQueue, 32)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(plan, "Sort") || strings.Contains(plan, "Bitmap") || !strings.Contains(plan, "tasks_fetch_idx") {
		t.Fatalf("the fetch does not read its index in order:\n%s", plan)
	}
}

func TestLeasedTasksAreFoundByID(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	// The statistics see no running task, as they do after a quiet moment,
	// while tasks_running_idx may hold the entries of many finished tasks
	// that vacuum has not removed yet.
	e.exec(t, `INSERT INTO {s}.tasks (queue, kind, state, run_at, payload, max_attempts)
	           SELECT 'default', 'email.send', 'available', now(), '{}', 1 FROM generate_series(1, 50000)`)
	e.exec(t, "ANALYZE {s}.tasks")

	plans, err := taskmill.ExplainLeased(t.Context(), e.client)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(plans, "tasks_running_idx") || strings.Contains(plans, "Seq Scan on tasks") {
		t.Fatalf("leased tasks are not found by their IDs:\n%s", plans)
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

func TestCompletionWithoutHistoryPartition(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	logs := &logRecorder{}
	w := newWorker(t, e.withLogger(t, slog.New(logs)), &taskmill.WorkerConfig{Handlers: noopHandlers(), NoMaintainer: true})
	// The worker's role could not create today's partition.
	today := "task_history_" + time.Now().UTC().Format("20060102")
	e.exec(t, "ALTER TABLE {s}.task_history DETACH PARTITION {s}."+today)
	e.exec(t, "DROP TABLE {s}."+today)

	for range 2 {
		info, err := w.RunTask(t.Context(), e.enqueue(t, sendEmail{}, nil))
		if err != nil || info.State != taskmill.StateCompleted || info.Attempt != 1 {
			t.Fatalf("got %+v, %v; want completed on the first attempt", info, err)
		}
	}
	if n := e.count(t, "SELECT count(*) FROM {s}.tasks"); n != 0 {
		t.Fatalf("%d tasks left", n)
	}
	if logs.count("task history has no partition for today; completed tasks skip history") != 1 {
		t.Fatal("the missing partition was not logged once")
	}
}

func TestWorkerWarnsAboutUnservedKinds(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	logs := &logRecorder{}
	var hs taskmill.Handlers
	taskmill.AddHandler(&hs, taskmill.HandlerFunc[report](func(context.Context, *taskmill.Task[report]) error { return nil }), nil)
	runWorker(t, e.withLogger(t, slog.New(logs)), &taskmill.WorkerConfig{Handlers: &hs, Queues: defaultQueue(1)})
	waitFor(t, "a warning that the reports queue is not served", func() bool {
		return logs.count("the worker does not serve the default queue of these registered kinds") == 1
	})
}

func TestWorkerRemovedAsStaleRegistersAgain(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	logs := &logRecorder{}
	runWorker(t, e.withLogger(t, slog.New(logs)), &taskmill.WorkerConfig{
		Handlers: noopHandlers(), NoMaintainer: true, LeaseDuration: 300 * time.Millisecond,
	})
	e.waitListening(t)
	// What the maintainer does to a worker that missed its heartbeats for 5
	// minutes, such as one on a machine that slept, while it kept working.
	e.exec(t, "DELETE FROM {s}.workers")
	const logged = "worker was removed as stale; registering again"
	waitFor(t, "the worker to register again, and say so", func() bool {
		return logs.count(logged) > 0 && e.count(t, "SELECT count(*) FROM {s}.workers") == 1
	})
	if n := logs.count(logged); n != 1 {
		t.Fatalf("the new registration was logged %d times, want once", n)
	}
}
