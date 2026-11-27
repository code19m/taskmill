package taskmill_test

import (
	"context"
	"errors"
	"log/slog"
	"math"
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

// A DrainTimeout of the largest Duration, to wait as long as it takes, does
// not cut the recording of the results short: the bound of Run's last steps
// counts from the stop, and the sum must not overflow.
func TestTheLargestDrainTimeoutRecordsTheDrainedTasks(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	started := make(chan struct{}, 1)

	stop := runWorker(t, e, &taskmill.WorkerConfig{
		Handlers: handlers(func(context.Context, *taskmill.Task[sendEmail]) error {
			started <- struct{}{}
			time.Sleep(300 * time.Millisecond)
			return nil
		}, nil),
		Queues:       defaultQueue(1),
		DrainTimeout: math.MaxInt64,
	})

	id := e.enqueue(t, sendEmail{}, nil)
	recv(t, started, "a handler to start")

	stop() // the handler returns during the drain
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

// The worker gives tasks that it fetched while it stopped back within 5
// seconds, also when the database does not answer: the recorder, which
// records on Run's context, does not hold the release, and Run returns
// DrainTimeout plus 10 seconds after its context ended at most.
func TestTasksFetchedWhileTheWorkerStopsGoBackInTime(t *testing.T) {
	t.Parallel()
	gate := &fetchGate{}
	e := newTracedEnv(t, gate)

	w := newWorker(t, e, &taskmill.WorkerConfig{
		Handlers:     handlers(func(context.Context, *taskmill.Task[sendEmail]) error { return nil }, nil),
		Queues:       defaultQueue(1),
		DrainTimeout: 50 * time.Millisecond,
		NoMaintainer: true,
	})
	e.enqueue(t, sendEmail{}, nil)

	e.exec(t, `
CREATE FUNCTION {s}.slow_release() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    PERFORM pg_sleep(60);
    RETURN NEW;
END $$`)
	e.exec(t, `
CREATE TRIGGER slow_release BEFORE UPDATE ON {s}.tasks
FOR EACH ROW WHEN (NEW.state = 'available') EXECUTE FUNCTION {s}.slow_release()`)
	t.Cleanup(func() { // ends a release that the worker gave up on, so that the schema can be dropped
		_, _ = e.pool.Exec(context.Background(), `SELECT pg_cancel_backend(pid) FROM pg_stat_activity
WHERE pid <> pg_backend_pid() AND strpos(query, $1) > 0`, e.schema+".tasks")
	})

	held, release := gate.holdNext()
	defer release()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()

	recv(t, held, "the worker's first fetch")
	cancel()
	stopped := time.Now()
	release()

	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Minute):
		t.Fatal("Run did not return within a minute")
	}
	if took := time.Since(stopped); took > 10*time.Second {
		t.Fatalf("Run returned %v after its context ended, want DrainTimeout plus 10 seconds at most", took)
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
		{"wait for a handler below a minute", &taskmill.WorkerConfig{NoHandlerWait: 59 * time.Second}},
		{"wait for a handler above a week", &taskmill.WorkerConfig{NoHandlerWait: 7*24*time.Hour + time.Second}},
		{"nil middleware", &taskmill.WorkerConfig{Middleware: []taskmill.Middleware{nil}}},
		{"schedule without args", &taskmill.WorkerConfig{Schedules: []taskmill.Schedule{{Name: "x", Cron: "* * * * *"}}}},
		{"schedule with nil pointer args", &taskmill.WorkerConfig{Schedules: []taskmill.Schedule{
			{Name: "x", Cron: "@daily", Args: (*report)(nil)},
		}}},
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

// Run may be called again after it returned: each run registers the worker,
// handles tasks, and is healthy while it runs, and the counts of Stats go on
// from the run before.
func TestRunAgainAfterItReturned(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ran := make(chan int64, 1)
	w := newWorker(t, e, &taskmill.WorkerConfig{
		Handlers: handlers(func(_ context.Context, task *taskmill.Task[sendEmail]) error {
			ran <- task.ID
			return nil
		}, nil),
		Queues: defaultQueue(1),
	})

	for run := int64(1); run <= 2; run++ {
		stop := startWorker(t, w)
		id := e.enqueue(t, sendEmail{}, nil)
		if got := recv(t, ran, "the task to run"); got != id {
			t.Fatalf("run %d: task %d ran, want %d", run, got, id)
		}
		e.waitState(t, id, taskmill.StateCompleted)
		waitFor(t, "the worker to be healthy", func() bool { return w.Health() == nil })
		if n := e.count(t, "SELECT count(*) FROM {s}.workers"); n != 1 {
			t.Fatalf("run %d: %d workers registered, want 1", run, n)
		}

		stop()
		if err := w.Health(); err == nil {
			t.Fatalf("run %d: a stopped worker is healthy", run)
		}
		if n := e.count(t, "SELECT count(*) FROM {s}.workers"); n != 0 {
			t.Fatalf("run %d: the stopped worker is still registered", run)
		}
		if s := w.Stats(); s.AttemptsStarted != run || s.AttemptsSucceeded != run {
			t.Fatalf("after run %d: %d attempts started and %d succeeded, want %d of each",
				run, s.AttemptsStarted, s.AttemptsSucceeded, run)
		}
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

func TestWorkerWarnsOfAPoolSmallerThanItsConcurrencyPlus5(t *testing.T) {
	t.Parallel()
	const warning = "the pool has fewer connections than the worker's total Concurrency plus 5; " +
		"see https://github.com/code19m/taskmill/blob/main/docs/production/database.md#size-the-connection-pool"
	queues := map[string]taskmill.QueueOptions{"a": {Concurrency: 4}, "b": {Concurrency: 6}}

	for _, conns := range []int32{14, 15} {
		logs := &logRecorder{}
		e := newEnvWith(t, testdb.PoolSize(t, conns), &taskmill.ClientConfig{Logger: slog.New(logs)})
		stop := runWorker(t, e, &taskmill.WorkerConfig{Handlers: noopHandlers(), Queues: queues})
		waitFor(t, "the worker to start", func() bool { return logs.count("worker started") == 1 })
		stop()

		var warned []slog.Record
		for _, r := range logs.all() {
			if r.Message == warning {
				warned = append(warned, r)
			}
		}
		switch {
		case conns == 15 && len(warned) > 0:
			t.Errorf("a pool of 15 connections for a concurrency of 10: got %s, want no warning", recordText(warned[0]))
		case conns == 14 && (len(warned) != 1 || warned[0].Level != slog.LevelWarn ||
			attr(warned[0], "max_conns") != "14" || attr(warned[0], "concurrency") != "10"):
			t.Errorf("a pool of 14 connections for a concurrency of 10: got %d warnings, want one with max_conns=14 concurrency=10", len(warned))
		}
	}
}
