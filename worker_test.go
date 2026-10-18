package taskmill_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/code19m/taskmill"
	"github.com/code19m/taskmill/internal/testdb"
)

// fastRetry keeps retry tests quick.
var fastRetry = taskmill.ExponentialBackoff{Base: 10 * time.Millisecond, Max: 10 * time.Millisecond}

// runWorker starts a worker and returns a function that stops it and waits
// for Run to return. The worker also stops when the test ends.
func runWorker(t *testing.T, e *env, cfg *taskmill.WorkerConfig) (stop func()) {
	t.Helper()
	if cfg.PollInterval == 0 {
		cfg.PollInterval = 50 * time.Millisecond
	}
	w, err := taskmill.NewWorker(e.client, cfg)
	if err != nil {
		t.Fatalf("new worker: %v", err)
	}
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

// waitState waits for a task to reach state.
func (e *env) waitState(t *testing.T, id int64, state taskmill.State) *taskmill.TaskInfo {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		info, err := e.client.Get(t.Context(), id)
		if err == nil && info.State == state {
			return info
		}
		if time.Now().After(deadline) {
			got := "error: " + fmt.Sprint(err)
			if info != nil {
				got = string(info.State)
			}
			t.Fatalf("task %d did not become %s; it is %s", id, state, got)
		}
		time.Sleep(20 * time.Millisecond)
	}
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
		code     string
	}{
		{"permanent", 1, ""},
		{"panic", 2, "panic"},
		{"timeout", 2, "timeout"},
	}
	for _, tt := range tests {
		id := e.enqueue(t, sendEmail{To: tt.to}, &taskmill.EnqueueOptions{MaxAttempts: 2})
		info := e.waitState(t, id, taskmill.StateDead)
		if info.Attempt != tt.attempts || len(info.Errors) != tt.attempts || info.Errors[0].Code != tt.code {
			t.Errorf("%s: got attempt %d, errors %+v", tt.to, info.Attempt, info.Errors)
		}
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
	if info.Attempt != 1 || info.Errors[0].Code != "invalid_payload" {
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
	runWorker(t, e, &taskmill.WorkerConfig{
		Handlers:           &hs,
		Queues:             defaultQueue(1),
		UnknownKindTimeout: time.Hour,
	})

	fresh := e.enqueue(t, sendEmail{}, nil)
	e.exec(t, `INSERT INTO {s}.tasks (queue, kind, state, run_at, payload, max_attempts, created_at)
	           VALUES ('default', 'old.kind', 'available', now(), '{}', 5, now() - interval '2 hours')`)
	old := int64(e.count(t, "SELECT max(id) FROM {s}.tasks"))

	if info := e.waitState(t, fresh, taskmill.StateScheduled); info.Attempt != 0 {
		t.Fatalf("snoozed unknown kind used %d attempts", info.Attempt)
	}
	if info := e.waitState(t, old, taskmill.StateDead); info.Errors[0].Code != "unknown_kind" {
		t.Fatalf("got errors %+v", info.Errors)
	}
}

func TestCompleteTxCommitsWithTheHandlersWrites(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.exec(t, "CREATE TABLE {s}.ledger (task_id bigint PRIMARY KEY)")

	var stale atomic.Bool
	runWorker(t, e, &taskmill.WorkerConfig{
		Handlers: handlers(func(ctx context.Context, task *taskmill.Task[sendEmail]) error {
			return pgx.BeginFunc(ctx, e.pool, func(tx pgx.Tx) error {
				if _, err := tx.Exec(ctx, replaceSchema("INSERT INTO {s}.ledger VALUES ($1)", e.schema), task.ID); err != nil {
					return err
				}
				if task.Args.To == "stale" && !stale.Swap(true) {
					// Another attempt took the task over.
					if _, err := tx.Exec(ctx, replaceSchema("UPDATE {s}.tasks SET lease_token = lease_token + 1 WHERE id = $1", e.schema), task.ID); err != nil {
						return err
					}
				}
				return task.CompleteTx(ctx, taskmill.Pgx(tx))
			})
		}, &taskmill.HandlerOptions{Retry: fastRetry}),
		Queues: defaultQueue(1),
	})

	id := e.enqueue(t, sendEmail{To: "ok"}, nil)
	e.waitState(t, id, taskmill.StateCompleted)
	if n := e.count(t, "SELECT count(*) FROM {s}.ledger WHERE task_id = $1", id); n != 1 {
		t.Fatalf("ledger rows: got %d, want 1", n)
	}

	// A stale attempt's CompleteTx fails with ErrLeaseLost, so its writes roll
	// back; the task is rescued later and runs again.
	staleID := e.enqueue(t, sendEmail{To: "stale"}, nil)
	deadline := time.Now().Add(5 * time.Second)
	for !stale.Load() && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if n := e.count(t, "SELECT count(*) FROM {s}.ledger WHERE task_id = $1", staleID); n != 0 {
		t.Fatalf("a lost lease committed %d ledger rows", n)
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
	<-started
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
	<-started
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
	<-started
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
			time.Sleep(3 * time.Second) // three lease durations
			return nil
		}, nil),
		Queues:        defaultQueue(2),
		LeaseDuration: time.Second, // without heartbeats, the task is rescued and runs again within 2 s
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
	e.exec(t, "INSERT INTO {s}.queues (name, halt_group_on_dead) VALUES ('default', true)")
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
	time.Sleep(300 * time.Millisecond) // give the worker a chance to misbehave
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
	e.exec(t, "INSERT INTO {s}.queues (name, max_running) VALUES ('default', 1)")
	var active, peak atomic.Int32
	hs := handlers(func(context.Context, *taskmill.Task[sendEmail]) error {
		n := active.Add(1)
		defer active.Add(-1)
		storeMax(&peak, n)
		time.Sleep(30 * time.Millisecond)
		return nil
	}, nil)
	runWorker(t, e, &taskmill.WorkerConfig{Handlers: hs, Queues: defaultQueue(5), ID: "w1"})
	runWorker(t, e, &taskmill.WorkerConfig{Handlers: hs, Queues: defaultQueue(5), ID: "w2"})

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
		// The target is 1.5 s (docs/design/performance.md); the bound here
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
	time.Sleep(300 * time.Millisecond) // let the worker connect and go idle

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
	<-done
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
	for _, b := range pub.batches {
		if len(b) > 4 {
			t.Fatalf("batch of %d exceeds the batch size", len(b))
		}
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

	tests := []struct {
		name string
		cfg  *taskmill.WorkerConfig
	}{
		{"duplicate kind", &taskmill.WorkerConfig{Handlers: &dup, Queues: defaultQueue(1)}},
		{"queues without handlers", &taskmill.WorkerConfig{Queues: defaultQueue(1)}},
		{"negative concurrency", &taskmill.WorkerConfig{Handlers: handlers(noop, nil), Queues: defaultQueue(-1)}},
		{"tiny lease", &taskmill.WorkerConfig{LeaseDuration: time.Millisecond}},
		{"negative drain", &taskmill.WorkerConfig{DrainTimeout: -time.Second}},
		{"schedule without args", &taskmill.WorkerConfig{Schedules: []taskmill.Schedule{{Name: "x", Cron: "* * * * *"}}}},
	}
	for _, tt := range tests {
		if _, err := taskmill.NewWorker(e.client, tt.cfg); !errors.Is(err, taskmill.ErrInvalidArgument) {
			t.Errorf("%s: got %v, want ErrInvalidArgument", tt.name, err)
		}
	}
}

func TestRunTwiceFails(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	w, err := taskmill.NewWorker(e.client, &taskmill.WorkerConfig{NoMaintainer: true})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	time.Sleep(200 * time.Millisecond)
	if err := w.Run(t.Context()); err == nil {
		t.Fatal("second Run succeeded")
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
	<-started
	<-started

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

	<-started
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
			<-started

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
	if n := <-got; n != 7 {
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
	runWorker(t, e, &taskmill.WorkerConfig{Handlers: noopHandlers(), Queues: defaultQueue(1)})
	waitFor(t, "the worker to listen", func() bool {
		return e.count(t, "SELECT count(*) FROM pg_stat_activity WHERE query = 'LISTEN ' || $1", e.schema) == 1
	})

	// A settings refresh waits for a lock on the queues, and is canceled.
	tx := e.begin(t)
	e.txExec(t, tx, "LOCK TABLE {s}.queues IN ACCESS EXCLUSIVE MODE")
	e.exec(t, "SELECT pg_notify($1, 'q|default')", e.schema)
	pid := e.waitBlocked(t, "{s}.queues WHERE name = ANY")
	e.exec(t, "SELECT pg_cancel_backend($1)", pid)
	if err := tx.Rollback(t.Context()); err != nil {
		t.Fatal(err)
	}

	id := e.enqueue(t, sendEmail{}, nil)
	time.Sleep(500 * time.Millisecond)
	e.wantState(t, id, taskmill.StateAvailable)
}
