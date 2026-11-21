package taskmill_test

import (
	"context"
	"errors"
	"log/slog"
	"runtime"
	"slices"
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
	if info.Attempt != 1 || info.FinishedAt.IsZero() {
		t.Fatalf("completed task: attempt %d, finalized %v", info.Attempt, info.FinishedAt)
	}
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

// A worker that stops says that it waits for running tasks only when it
// does. It counts the tasks whose handlers run: not a task whose handler
// returned, and whose result it has yet to record.
func TestStoppingWorkerSaysThatItWaitsOnlyWhenTasksRun(t *testing.T) {
	t.Parallel()
	const waiting = "worker stopping; waiting for running tasks"
	logs := &logRecorder{}
	e := newEnv(t).withLogger(t, slog.New(logs))
	started, release := make(chan struct{}, 3), make(chan struct{})

	w := newWorker(t, e, &taskmill.WorkerConfig{
		Handlers: handlers(func(_ context.Context, task *taskmill.Task[sendEmail]) error {
			started <- struct{}{}
			if task.GroupKey == "" {
				<-release
			}
			return nil
		}, nil),
		Queues:       defaultQueue(2),
		NoMaintainer: true,
	})

	// An idle worker stops without a word about waiting.
	stop := startWorker(t, w)
	waitFor(t, "the worker to start", func() bool { return logs.count("worker started") == 1 })
	stop()
	if n := logs.count(waiting); n != 0 {
		t.Fatalf("an idle worker said %d times that it waits for running tasks", n)
	}

	// It runs again, with three tasks. The handler of the first, a task of a
	// FIFO group, returns right away, and its result waits: a transaction
	// holds the group's row. The worker has two slots, so the third task
	// starts in the slot that the first gave back: once all three have
	// started, the first handler is through.
	e.enqueue(t, sendEmail{}, &taskmill.EnqueueOptions{GroupKey: "held"})
	e.enqueue(t, sendEmail{}, nil)
	e.enqueue(t, sendEmail{}, nil)

	tx := e.begin(t)
	e.txExec(t, tx, "SELECT FROM {s}.groups FOR UPDATE")
	stop = startWorker(t, w)
	for range 3 {
		recv(t, started, "a handler to start")
	}

	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		stop()
	}()

	waitFor(t, "the worker to say that it waits", func() bool { return logs.count(waiting) == 1 })
	close(release)
	if err := tx.Rollback(t.Context()); err != nil {
		t.Fatal(err)
	}
	recv(t, stopped, "the worker to stop")

	for _, r := range logs.all() {
		if r.Message == waiting && attr(r, "tasks") != "2" {
			t.Fatalf("the worker said that it waits for %s tasks, want the 2 whose handlers ran", attr(r, "tasks"))
		}
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

// fetchGate lets a test keep a fetch of the worker whose pool it traces in
// flight, as a slow database does, or a pool whose connections are all in
// use.
type fetchGate struct {
	hold atomic.Pointer[func()] // called once, before the next fetch is sent
}

// holdNext stops the next fetch before it is sent. It returns a channel that
// is closed when that fetch arrives, and a function that lets the fetch go.
func (g *fetchGate) holdNext() (held <-chan struct{}, release func()) {
	arrived, free := make(chan struct{}), make(chan struct{})
	hold := func() {
		close(arrived)
		<-free
	}

	g.hold.Store(&hold)
	return arrived, sync.OnceFunc(func() { close(free) })
}

func (*fetchGate) TraceQueryStart(ctx context.Context, _ *pgx.Conn, _ pgx.TraceQueryStartData) context.Context {
	return ctx
}

func (*fetchGate) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

// A queue without a limit sends each of its fetches as a batch.
func (g *fetchGate) TraceBatchStart(ctx context.Context, _ *pgx.Conn, d pgx.TraceBatchStartData) context.Context {
	fetch := slices.ContainsFunc(d.Batch.QueuedQueries, func(q *pgx.QueuedQuery) bool {
		return strings.Contains(q.SQL, "WITH picked AS")
	})
	if !fetch {
		return ctx
	}
	if hold := g.hold.Swap(nil); hold != nil {
		(*hold)()
	}
	return ctx
}

func (*fetchGate) TraceBatchQuery(context.Context, *pgx.Conn, pgx.TraceBatchQueryData) {}

func (*fetchGate) TraceBatchEnd(context.Context, *pgx.Conn, pgx.TraceBatchEndData) {}

// DrainTimeout counts from the moment the worker is told to stop, whatever
// its fetch loops do then. A fetch that is in flight can take long. When the
// handlers work on the worker's pool and hold every connection of it, the
// fetch waits for one of them to return, for up to 30 seconds: a clock that
// started only when the fetch has ended would start that much too late.
func TestDrainTimeoutCountsFromTheStop(t *testing.T) {
	t.Parallel()
	gate := &fetchGate{}
	e := newTracedEnv(t, gate)
	started, cause := make(chan struct{}, 1), make(chan error, 1)

	stop := runWorker(t, e, &taskmill.WorkerConfig{
		Handlers: handlers(func(ctx context.Context, _ *taskmill.Task[sendEmail]) error {
			started <- struct{}{}
			<-ctx.Done()
			cause <- context.Cause(ctx)
			return ctx.Err()
		}, nil),
		Queues:       defaultQueue(2), // one slot stays free, so the queue keeps fetching
		DrainTimeout: 50 * time.Millisecond,
		NoMaintainer: true,
	})

	id := e.enqueue(t, sendEmail{}, nil)
	recv(t, started, "the handler to start")

	// The worker is told to stop while a fetch is in flight, and the fetch
	// stays in flight until the handler has learned of the shutdown.
	held, release := gate.holdNext()
	defer release()
	recv(t, held, "the next fetch to start")
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		stop()
	}()

	if err := recv(t, cause, "the handler's context to end"); !errors.Is(err, taskmill.ErrShutdown) {
		t.Fatalf("the handler's context ended with %v, want ErrShutdown", err)
	}

	release()
	recv(t, stopped, "the worker to stop")

	if info := e.get(t, id); info.State != taskmill.StateAvailable || info.Attempt != 0 {
		t.Fatalf("released task: got %s with attempt %d, want available with 0", info.State, info.Attempt)
	}
}

// A fetch that is in flight when the worker is told to stop can still lease
// tasks. They cannot start any more, so the worker gives them back. A task
// that it only stopped tracking would stay running, with no one to run it,
// until its lease ran out.
func TestTasksFetchedWhileTheWorkerStopsGoBack(t *testing.T) {
	t.Parallel()
	gate := &fetchGate{}
	e := newTracedEnv(t, gate)

	w := newWorker(t, e, &taskmill.WorkerConfig{
		Handlers:     handlers(func(context.Context, *taskmill.Task[sendEmail]) error { return nil }, nil),
		Queues:       defaultQueue(1),
		NoMaintainer: true,
	})
	id := e.enqueue(t, sendEmail{}, nil)

	// The worker's first fetch reaches the database only after the worker
	// was told to stop.
	held, release := gate.holdNext()
	defer release()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()

	recv(t, held, "the worker's first fetch")
	cancel()
	release()

	if err := recv(t, done, "the worker to stop"); err != nil {
		t.Fatal(err)
	}

	// The lease token counts the times that the task was leased.
	if n := e.count(t, "SELECT lease_token FROM {s}.tasks WHERE id = $1", id); n != 1 {
		t.Fatalf("the task was leased %d times, want once, by the fetch that was in flight", n)
	}

	if info := e.get(t, id); info.State != taskmill.StateAvailable || info.Attempt != 0 {
		t.Fatalf("got %s with attempt %d, want available with 0", info.State, info.Attempt)
	}
}

// Run returns only when the results of its attempts are recorded. That
// holds also when the drain gives up: an attempt whose handler returned in
// time is not abandoned then, and its result may still be on its way to a
// slow database.
func TestRunWaitsForAResultThatIsBeingRecorded(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	started := make(chan struct{}, 1)

	stop := runWorker(t, e, &taskmill.WorkerConfig{
		Handlers: handlers(func(ctx context.Context, _ *taskmill.Task[sendEmail]) error {
			started <- struct{}{}
			<-ctx.Done()
			return ctx.Err()
		}, nil),
		Queues:       defaultQueue(1),
		DrainTimeout: 10 * time.Millisecond,
		NoMaintainer: true,
	})

	id := e.enqueue(t, sendEmail{}, nil)
	recv(t, started, "the handler to start")

	// The database takes longer to give the task back than the drain waits
	// for a handler that it canceled, which is 200 ms in tests.
	e.exec(t, `
CREATE FUNCTION {s}.slow_release() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    PERFORM pg_sleep(0.4);
    RETURN NEW;
END $$`)
	e.exec(t, `
CREATE TRIGGER slow_release BEFORE UPDATE ON {s}.tasks
FOR EACH ROW WHEN (NEW.state = 'available') EXECUTE FUNCTION {s}.slow_release()`)

	stop()
	if info := e.get(t, id); info.State != taskmill.StateAvailable || info.Attempt != 0 {
		t.Fatalf("Run returned before the task was given back: it is %s with attempt %d, want available with 0",
			info.State, info.Attempt)
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
		{"schedule with a run time", &taskmill.WorkerConfig{Schedules: []taskmill.Schedule{
			{Name: "x", Cron: "@daily", Args: sendEmail{}, Options: &taskmill.EnqueueOptions{RunAt: time.Now()}},
		}}},
		{"schedule with a dedupe key", &taskmill.WorkerConfig{Schedules: []taskmill.Schedule{
			{Name: "x", Cron: "@daily", Args: sendEmail{}, Options: &taskmill.EnqueueOptions{DedupeKey: "k"}},
		}}},
		{"schedule with a dedupe window", &taskmill.WorkerConfig{Schedules: []taskmill.Schedule{
			{Name: "x", Cron: "@daily", Args: sendEmail{}, Options: &taskmill.EnqueueOptions{DedupeWindow: time.Hour}},
		}}},
		{"schedule with oversized metadata", &taskmill.WorkerConfig{Schedules: []taskmill.Schedule{{
			Name: "x", Cron: "@daily", Args: sendEmail{},
			Options: &taskmill.EnqueueOptions{Metadata: map[string]string{"k": strings.Repeat("x", 64<<10)}},
		}}}},
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

func TestRunStoppedBeforeItStartsReturnsNil(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	w := newWorker(t, e, &taskmill.WorkerConfig{Handlers: noopHandlers()})
	ctx, cancel := context.WithCancel(t.Context())
	cancel() // as a SIGTERM during startup does

	if err := w.Run(ctx); err != nil {
		t.Fatalf("got %v, want nil", err)
	}
	if n := e.count(t, "SELECT count(*) FROM {s}.workers"); n != 0 {
		t.Fatalf("%d workers registered", n)
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
	if err := w.Run(second); !errors.Is(err, taskmill.ErrInvalidState) || second.Err() != nil {
		t.Fatalf("second Run: got %v, want an immediate ErrInvalidState", err)
	}

	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
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
	if n := e.count(t, "SELECT count(*) FROM {s}.workers"); n != 0 {
		t.Error("the stopped worker is still registered")
	}

	// The abandoned handler's late success changes nothing.
	close(unblock)
	recv(t, returned, "the handler to return")
	e.wantState(t, id, taskmill.StateAvailable)
}

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

// Run returns soon after the drain also when the database stops answering
// while it stops: the results that it cannot record, and the worker's row,
// share one deadline of 5 seconds, and the tasks are rescued once their
// leases run out.
func TestRunReturnsSoonWhenTheDatabaseStopsAnswering(t *testing.T) {
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

	started := make(chan struct{}, 5)
	release := make(chan struct{})
	const drain = time.Second
	w := newWorker(t, e, &taskmill.WorkerConfig{
		Handlers: handlers(func(context.Context, *taskmill.Task[sendEmail]) error {
			started <- struct{}{}
			<-release
			return nil
		}, nil),
		Queues:       defaultQueue(5),
		DrainTimeout: drain,
	})
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = w.Run(ctx)
	}()
	// The database answers again for the cleanups of the test.
	t.Cleanup(func() {
		cancel()
		<-done
		db.frozen.Store(false)
		db.cut(true)
		pool.Reset()
		db.cut(false)
	})

	for range 5 {
		e.enqueue(t, sendEmail{}, nil)
	}
	for range 5 {
		recv(t, started, "the handlers to start")
	}

	db.frozen.Store(true)
	close(release)
	stopped := time.Now()
	cancel()
	select {
	case <-done:
	case <-time.After(time.Minute):
		t.Fatal("Run did not return within a minute")
	}
	if d, limit := time.Since(stopped), drain+8*time.Second; d > limit {
		t.Fatalf("Run returned %s after its context ended, want at most %s", d.Round(time.Millisecond), limit)
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

func TestWorkerWarnsAboutUnservedKinds(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	logs := &logRecorder{}

	var hs taskmill.Handlers
	taskmill.AddHandler(&hs, taskmill.HandlerFunc[report](func(context.Context, *taskmill.Task[report]) error { return nil }), nil)

	runWorker(t, e.withLogger(t, slog.New(logs)), &taskmill.WorkerConfig{Handlers: &hs, Queues: defaultQueue(1)})
	waitFor(t, "a warning that the reports queue is not served", func() bool {
		return logs.count("the worker does not read the default queue of these registered kinds") == 1
	})
}
