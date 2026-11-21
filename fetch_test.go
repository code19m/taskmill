package taskmill_test

import (
	"context"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/code19m/taskmill"
	"github.com/code19m/taskmill/internal/testdb"
)

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
	e.limitDefaultQueue(t, 1)

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

// Many workers, each with more slots than the limit, fetch from one queue
// with a MaxRunning limit at the same time: no more tasks than the limit
// ever run, by the handlers' count and by the database's.
func TestManyWorkersKeepTheLimitExact(t *testing.T) {
	t.Parallel()
	e := newEnvWith(t, testdb.PoolSize(t, 32), nil)
	const limit = 4
	e.limitDefaultQueue(t, limit)

	var active, peak atomic.Int32
	hs := handlers(func(context.Context, *taskmill.Task[sendEmail]) error {
		storeMax(&peak, active.Add(1))
		defer active.Add(-1)
		time.Sleep(5 * time.Millisecond)
		return nil
	}, nil)
	for range 6 {
		runWorker(t, e, &taskmill.WorkerConfig{Handlers: hs, Queues: defaultQueue(8), NoMaintainer: true})
	}

	params := make([]taskmill.EnqueueParams, 300)
	for i := range params {
		params[i] = taskmill.EnqueueParams{Args: sendEmail{UserID: int64(i)}}
	}
	if _, err := e.client.EnqueueMany(t.Context(), params); err != nil {
		t.Fatal(err)
	}

	var most int
	waitFor(t, "every task to complete", func() bool {
		most = max(most, e.count(t, "SELECT count(*) FROM {s}.tasks WHERE state = 'running'"))
		return e.count(t, "SELECT count(*) FROM {s}.tasks") == 0
	})
	if p := peak.Load(); p > limit || most > limit {
		t.Fatalf("up to %d handlers ran, and the database counted up to %d running tasks; want at most %d", p, most, limit)
	}
}

// batchLog keeps the statements of each batch that a pool's connections
// send.
type batchLog struct {
	mu      sync.Mutex
	open    map[*pgx.Conn][]string
	batches [][]string
}

func (*batchLog) TraceQueryStart(ctx context.Context, _ *pgx.Conn, _ pgx.TraceQueryStartData) context.Context {
	return ctx
}

func (*batchLog) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

func (b *batchLog) TraceBatchStart(ctx context.Context, conn *pgx.Conn, _ pgx.TraceBatchStartData) context.Context {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.open == nil {
		b.open = map[*pgx.Conn][]string{}
	}
	b.open[conn] = nil
	return ctx
}

func (b *batchLog) TraceBatchQuery(_ context.Context, conn *pgx.Conn, d pgx.TraceBatchQueryData) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.open[conn] = append(b.open[conn], d.SQL)
}

func (b *batchLog) TraceBatchEnd(_ context.Context, conn *pgx.Conn, _ pgx.TraceBatchEndData) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.batches = append(b.batches, b.open[conn])
	delete(b.open, conn)
}

// holding reports whether a batch held the row of a limited queue from its
// lock to its commit.
func (b *batchLog) holding() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return slices.ContainsFunc(b.batches, func(sqls []string) bool {
		return slices.ContainsFunc(sqls, func(s string) bool { return strings.Contains(s, queueLock) }) && slices.Contains(sqls, "COMMIT")
	})
}

// A fetch from a queue with a MaxRunning limit sends the statements from the
// lock of the queue's row to the commit in one round trip, so that the row
// is held while the database works, and not while the network carries
// answers: the queue's fetches take the row in turn, and each held it for
// four round trips, so a slow network bounded the queue's throughput.
func TestLimitedFetchHoldsTheQueueRowForOneRoundTrip(t *testing.T) {
	t.Parallel()
	sent := &batchLog{}
	e := newTracedEnv(t, sent)
	e.limitDefaultQueue(t, 5)
	runWorker(t, e, &taskmill.WorkerConfig{Handlers: noopHandlers(), Queues: defaultQueue(1), NoMaintainer: true})

	e.waitState(t, e.enqueue(t, sendEmail{}, nil), taskmill.StateCompleted)
	if !sent.holding() {
		t.Fatal("no batch took the queue's row and committed: the fetch held the row across round trips")
	}
}

// A fetch that takes a task for each free slot says that more may wait,
// also when it fills a batch with more tasks after that, and also from a
// queue with a limit that leaves room: the worker then fetches again at
// once, without waiting for a poll.
func TestFetchThatFillsABatchSaysThatMoreMayWait(t *testing.T) {
	t.Parallel()
	for _, limit := range []int32{0, 100} {
		e := newEnv(t)
		if limit > 0 {
			e.limitDefaultQueue(t, int(limit))
		}
		var hs taskmill.Handlers
		taskmill.AddBatchHandler[report](&hs, batchFunc(func([]*taskmill.Task[report]) {}), &taskmill.HandlerOptions{BatchSize: 10})
		w := newWorker(t, e, &taskmill.WorkerConfig{Handlers: &hs, Queues: defaultQueue(4), NoMaintainer: true})
		for range 30 {
			e.enqueue(t, report{}, &taskmill.EnqueueOptions{Queue: taskmill.DefaultQueue})
		}

		n, more, atLimit, err := taskmill.Fetch(t.Context(), w, taskmill.DefaultQueue, 4, limit)
		if err != nil || n != 10 || !more || atLimit {
			t.Errorf("limit %d: took %d tasks, more %v, at the limit %v, %v; want a batch of 10, and more",
				limit, n, more, atLimit, err)
		}
	}
}

// A fetch takes at most 1,000 tasks. A queue whose limit allows more, with
// more slots free, has more tasks to fetch once it took 1,000: they must not
// wait for the next poll.
func TestFetchAtItsMostSaysThatMoreMayWait(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.limitDefaultQueue(t, 5000)
	w := newWorker(t, e, &taskmill.WorkerConfig{Handlers: noopHandlers(), Queues: defaultQueue(2000), NoMaintainer: true})
	params := make([]taskmill.EnqueueParams, 1100)
	for i := range params {
		params[i] = taskmill.EnqueueParams{Args: sendEmail{}}
	}
	if _, err := e.client.EnqueueMany(t.Context(), params); err != nil {
		t.Fatal(err)
	}

	n, more, atLimit, err := taskmill.Fetch(t.Context(), w, taskmill.DefaultQueue, 2000, 5000)
	if err != nil || n != 1000 || !more || atLimit {
		t.Errorf("took %d tasks, more %v, at the limit %v, %v; want 1,000, and more", n, more, atLimit, err)
	}
}

// A wait for a row before a task is taken must not be taken off the task's
// lease: the task would start with a lease that is almost over, and be
// rescued while it runs.
func TestLeaseCountsFromWhenTheTaskIsTaken(t *testing.T) {
	t.Parallel()
	const lease, wait = time.Second, 700 * time.Millisecond

	// measuring returns a handler that sends how much of its lease an attempt
	// has left when it starts.
	measuring := func(e *env, left chan<- time.Duration) *taskmill.Handlers {
		query := replaceSchema("SELECT extract(epoch FROM lease_expires_at - clock_timestamp()) FROM {s}.tasks WHERE id = $1", e.schema)
		return handlers(func(ctx context.Context, task *taskmill.Task[sendEmail]) error {
			var secs float64
			if err := e.pool.QueryRow(ctx, query, task.ID).Scan(&secs); err != nil {
				return err
			}
			left <- time.Duration(secs * float64(time.Second))
			return nil
		}, nil)
	}

	// holdThenFree keeps tx's row lock for wait once a statement of the
	// worker waits for it, then frees it, and checks the lease that the
	// handler finds.
	holdThenFree := func(t *testing.T, e *env, tx pgx.Tx, blocked string, left <-chan time.Duration) {
		t.Helper()
		e.waitBlocked(t, blocked)
		time.Sleep(wait)
		if err := tx.Rollback(t.Context()); err != nil {
			t.Fatal(err)
		}

		if d := recv(t, left, "the handler to start"); d < lease/2 {
			t.Fatalf("the handler started with %s of its %s lease left: the wait for the row was taken off the lease", d, lease)
		}
	}

	t.Run("fetch from a queue with a limit", func(t *testing.T) {
		t.Parallel()
		e := newEnv(t)
		e.limitDefaultQueue(t, 5)
		left := make(chan time.Duration, 1)

		w := newWorker(t, e, &taskmill.WorkerConfig{
			Handlers: measuring(e, left), Queues: defaultQueue(1), LeaseDuration: lease,
			NoMaintainer: true, // nothing rescues: the test only measures the lease
		})

		// Another fetch of the queue holds its row, as a worker does that
		// stalls in the middle of its fetch.
		tx := e.begin(t)
		e.txExec(t, tx, "SELECT FROM {s}.queues WHERE name = 'default' FOR UPDATE")

		e.enqueue(t, sendEmail{}, nil)
		startWorker(t, w)
		holdThenFree(t, e, tx, "SELECT max_running FROM {s}.queues", left)
	})

	t.Run("RunTask", func(t *testing.T) {
		t.Parallel()
		e := newEnv(t)
		left := make(chan time.Duration, 1)
		w := newWorker(t, e, &taskmill.WorkerConfig{Handlers: measuring(e, left), LeaseDuration: lease})
		id := e.enqueue(t, sendEmail{}, nil)

		// Another transaction holds the task's row, as CancelTx does until
		// its caller commits or rolls back.
		tx := e.begin(t)
		e.txExec(t, tx, "SELECT FROM {s}.tasks WHERE id = $1 FOR UPDATE", id)

		done := make(chan error, 1)
		go func() {
			_, err := w.RunTask(t.Context(), id)
			done <- err
		}()

		holdThenFree(t, e, tx, "{s}.tasks", left)
		if err := recv(t, done, "RunTask to return"); err != nil {
			t.Fatal(err)
		}
	})
}

// queueLock is part of the statement that locks the row of a queue with a
// limit, for a statementLog to count.
const queueLock = "queues WHERE name = $1 FOR UPDATE"

// A fetch from a queue with a limit waits only briefly for the queue's row.
// While another transaction holds the row, the fetch gives up, and the worker
// fetches again later. Giving up is no failure to report: the queue's fetches
// hold the row in turn, so with many workers a fetch can wait as long while
// nothing is stuck.
func TestLimitedFetchGivesUpOnAHeldQueueRow(t *testing.T) {
	t.Parallel()
	sent, logs := &statementLog{}, &logRecorder{}
	e := newTracedEnv(t, sent).withLogger(t, slog.New(logs))
	e.limitDefaultQueue(t, 5)

	ran := make(chan struct{}, 1)
	w := newWorker(t, e, &taskmill.WorkerConfig{
		Handlers: handlers(func(context.Context, *taskmill.Task[sendEmail]) error {
			ran <- struct{}{}
			return nil
		}, nil),
		Queues:       defaultQueue(1),
		NoMaintainer: true,
	})

	tx := e.begin(t)
	e.txExec(t, tx, "SELECT FROM {s}.queues WHERE name = 'default' FOR UPDATE")
	e.enqueue(t, sendEmail{}, nil)
	startWorker(t, w)

	waitFor(t, "the fetch to give up on the held row, and to try again", func() bool {
		return sent.count(queueLock) >= 2
	})
	if lines := operationLines(logs.all())["fetch tasks"]; len(lines) > 0 {
		t.Fatalf("the fetch that gave up was reported as a failed operation: %s", recordText(lines[0]))
	}
	select {
	case <-ran:
		t.Fatal("the task ran while another transaction held its queue's row")
	default:
	}

	if err := tx.Rollback(t.Context()); err != nil {
		t.Fatal(err)
	}
	recv(t, ran, "the task to run once the row is free")
}

// A queue with a limit and a backlog is at its limit most of the time. A
// worker with free slots then only counts the queue's running tasks: it locks
// the queue's row, in a transaction, only once a task may start.
func TestFetchDoesNotLockAQueueAtItsLimit(t *testing.T) {
	t.Parallel()
	sent := &statementLog{}
	e := newTracedEnv(t, sent)
	e.limitDefaultQueue(t, 1)

	started, release := make(chan struct{}, 2), make(chan struct{})
	var active, peak atomic.Int32
	runWorker(t, e, &taskmill.WorkerConfig{
		Handlers: handlers(func(context.Context, *taskmill.Task[sendEmail]) error {
			storeMax(&peak, active.Add(1))
			defer active.Add(-1)
			started <- struct{}{}
			<-release
			return nil
		}, nil),
		Queues:       defaultQueue(2),
		NoMaintainer: true,
	})

	t.Cleanup(func() { close(release) })
	e.enqueue(t, sendEmail{}, nil)
	waiting := e.enqueue(t, sendEmail{}, nil)
	recv(t, started, "the first task to start")

	// One task runs, one waits behind the limit, and the worker has a free
	// slot: it polls.
	sent.reset()
	waitFor(t, "the worker to count the queue's running tasks five times", func() bool {
		return sent.count("SELECT", "count(*)", "state = 'running'") >= 5
	})
	if n := sent.count(queueLock); n != 0 {
		t.Fatalf("the worker locked the queue's row %d times while the queue was at its limit", n)
	}

	// Once the running task ends, the fetch locks the row and takes the next.
	release <- struct{}{}
	recv(t, started, "the second task to start")
	if n := sent.count(queueLock); n == 0 {
		t.Error("the second task started without a lock on the queue's row: nothing serialized its fetch")
	}

	if p := peak.Load(); p != 1 {
		t.Errorf("peak concurrency %d, want the limit of 1", p)
	}
	e.wantState(t, waiting, taskmill.StateRunning)
}

// A task that ends in a queue at its limit makes room for one more. Its
// worker fetches when the result is recorded, not only when the slot is
// free: a fetch that comes before the result still counts the task as
// running, finds no room, and leaves the queue below its limit until the
// next poll. Here no poll comes, and each start counts the running tasks.
func TestLimitedQueueStartsATaskWhenOneEnds(t *testing.T) {
	t.Parallel()
	const limit, tasks = 2, 8
	sent := &statementLog{}
	e := newTracedEnv(t, sent)
	e.limitDefaultQueue(t, limit)

	for range tasks {
		e.enqueue(t, sendEmail{}, nil)
	}

	running := replaceSchema("SELECT count(*) FILTER (WHERE state = 'running') FROM {s}.tasks", e.schema)
	started, release := make(chan int, tasks), make(chan struct{})
	runWorker(t, e, &taskmill.WorkerConfig{
		Handlers: handlers(func(ctx context.Context, _ *taskmill.Task[sendEmail]) error {
			var n int
			if err := e.pool.QueryRow(ctx, running).Scan(&n); err != nil {
				return err
			}
			started <- n
			<-release
			return nil
		}, nil),
		Queues:       defaultQueue(2 * limit),
		PollInterval: time.Hour,
		NoMaintainer: true, // nothing wakes the queue either
	})
	t.Cleanup(func() { close(release) })

	// The listener's start wakes every queue once: that must not stand in
	// for the fetch that the test waits for.
	e.waitListening(t)

	for i := range tasks {
		if i >= limit {
			release <- struct{}{}
		}
		if n := recv(t, started, "a task to start in the room that the last one left"); n != limit {
			t.Fatalf("task %d started with %d tasks running, want the limit of %d", i+1, n, limit)
		}
	}

	// A fetch that leases locks the queue's row once. More locks than
	// tasks would mean fetches that found no room.
	if n := sent.count(queueLock); n > tasks {
		t.Errorf("the worker locked the queue's row %d times to start %d tasks", n, tasks)
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
		// The target is 1.5 s (docs/design/benchmarks.md); the bound here
		// leaves room for a loaded test machine.
		if late := at.Sub(runAt); late > 5*time.Second {
			t.Fatalf("task started %s late", late)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("scheduled task did not run")
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

// Filling a batch reads no more of the queue than the tasks it takes, where
// they are the queue's next ones, as an outbox's are.
func TestBatchFillReadsOnlyTheTasksItTakes(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.exec(t, `INSERT INTO {s}.tasks (queue, kind, state, run_at, payload, max_attempts)
	           SELECT 'default', 'report.build', 'available', now(), '{}', 1 FROM generate_series(1, 2000)`)
	e.exec(t, "ANALYZE {s}.tasks")

	const fill = 90
	n, err := taskmill.FillRowsRead(t.Context(), e.client, taskmill.DefaultQueue, "report.build", fill)
	if err != nil {
		t.Fatal(err)
	}
	// It reads each task as a candidate, to lock it, and to lease it, and
	// returns it.
	if n > 4*fill {
		t.Fatalf("filling a batch with %d tasks read %d rows, want at most %d", fill, n, 4*fill)
	}
}

// A batch fills with the next tasks of its kind in the queue's order: by
// priority, and then by age.
func TestBatchFillsInTheQueuesOrder(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	var mu sync.Mutex
	var batches [][]int64
	var hs taskmill.Handlers
	taskmill.AddBatchHandler[report](&hs, batchFunc(func(tasks []*taskmill.Task[report]) {
		mu.Lock()
		defer mu.Unlock()
		ids := make([]int64, len(tasks))
		for i, task := range tasks {
			ids[i] = task.ID
		}
		batches = append(batches, ids)
	}), &taskmill.HandlerOptions{BatchSize: 4})

	// Three reports of low priority, then three of high priority.
	var ids []int64
	for _, p := range []taskmill.Priority{taskmill.PriorityLow, taskmill.PriorityHigh} {
		for range 3 {
			ids = append(ids, e.enqueue(t, report{}, &taskmill.EnqueueOptions{Queue: taskmill.DefaultQueue, Priority: p}))
		}
	}

	// The one slot's fetch takes the first report of high priority, and
	// fills its batch with the two others and the oldest of low priority.
	runWorker(t, e, &taskmill.WorkerConfig{Handlers: &hs, Queues: defaultQueue(1)})

	for _, id := range ids {
		e.waitState(t, id, taskmill.StateCompleted)
	}
	mu.Lock()
	defer mu.Unlock()
	if want := [][]int64{{ids[3], ids[4], ids[5], ids[0]}, {ids[1], ids[2]}}; !slices.EqualFunc(batches, want, slices.Equal[[]int64]) {
		t.Fatalf("got the batches %v, want %v", batches, want)
	}
}

// batchFunc is a batch handler for reports that completes them all.
type batchFunc func(tasks []*taskmill.Task[report])

func (f batchFunc) HandleBatch(_ context.Context, tasks []*taskmill.Task[report]) []error {
	f(tasks)
	return make([]error, len(tasks))
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
