package taskmill_test

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/code19m/taskmill"
	"github.com/code19m/taskmill/internal/testdb"
)

// A worker checks in every third of its LeaseDuration: with a lease of 10
// minutes, it is silent for more than 3 minutes while it is healthy. So how
// long a worker may be silent before it counts as stale, or as gone, follows
// the lease that it stored when it registered.

// longLease starts a worker with a lease of 10 minutes, which makes no
// check-in while the test runs, and returns its ID once it is registered.
func longLease(t *testing.T, e *env) string {
	t.Helper()
	runWorker(t, e, &taskmill.WorkerConfig{LeaseDuration: 10 * time.Minute, NoMaintainer: true})

	var id string
	waitFor(t, "the worker to register", func() bool {
		workers, err := e.client.Workers(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if len(workers) == 1 {
			id = workers[0].ID
		}
		return id != ""
	})
	return id
}

// silent sets the last check-in of worker id to ago before now, as if it had
// made none since.
func (e *env) silent(t *testing.T, id, ago string) {
	t.Helper()
	e.exec(t, "UPDATE {s}.workers SET heartbeat_at = now() - interval '"+ago+"' WHERE id = $1", id)
}

// oldWorker registers a worker as the previous release does, which stores no
// lease.
func (e *env) oldWorker(t *testing.T) string {
	t.Helper()
	e.exec(t, "INSERT INTO {s}.workers (id, queues, hostname, version) VALUES ('old', '{}', 'host', 'v0.6.0')")
	return "old"
}

func TestWorkerIsStaleAfterItsOwnLease(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	long, old := longLease(t, e), e.oldWorker(t)

	for _, tt := range []struct {
		id, ago string
		stale   bool
	}{
		{long, "5 minutes", false}, // one check-in was late: its tasks are still leased
		{long, "11 minutes", true},
		{old, "20 seconds", false}, // a release that stores no lease: 30 seconds, as before
		{old, "40 seconds", true},
	} {
		e.silent(t, tt.id, tt.ago)
		workers, err := e.client.Workers(t.Context())
		if err != nil {
			t.Fatal(err)
		}

		for _, w := range workers {
			if w.ID == tt.id && w.Stale != tt.stale {
				t.Errorf("the worker %s, silent for %s: stale is %v, want %v", tt.id, tt.ago, w.Stale, tt.stale)
			}
		}
	}
}

func TestMaintainerRemovesAWorkerByItsOwnLease(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	long, old := longLease(t, e), e.oldWorker(t)
	maintainer := newWorker(t, e, &taskmill.WorkerConfig{})
	listed := func(id string) bool {
		return e.count(t, "SELECT count(*) FROM {s}.workers WHERE id = $1", id) == 1
	}

	// Two check-ins of the long lease are missing, and its tasks are still
	// leased. The other worker has been silent for 5 minutes, as before.
	e.silent(t, long, "7 minutes")
	e.silent(t, old, "7 minutes")
	taskmill.RunRetention(t.Context(), maintainer)

	if !listed(long) {
		t.Fatal("the maintainer removed a worker that was silent for less than its lease")
	}
	if listed(old) {
		t.Fatal("the maintainer kept a worker of the previous release that was silent for 7 minutes")
	}

	e.silent(t, long, "21 minutes")
	taskmill.RunRetention(t.Context(), maintainer)
	if listed(long) {
		t.Fatal("the maintainer kept a worker that was silent for more than twice its lease")
	}
}

// The workers table accepts a lease of up to 300 years, which is more than
// any worker has. The statements that move a time back by a lease, or by two,
// must work with the longest one: a row that fails them fails the list of
// the workers, and the removal of the others.
func TestWorkerWithTheLongestLeaseDoesNotFailTheOthers(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	old := e.oldWorker(t)
	e.exec(t, `INSERT INTO {s}.workers (id, queues, hostname, version, lease_duration)
	           VALUES ('longest', '{}', 'host', 'v', interval '300 years')`)

	e.exec(t, "UPDATE {s}.workers SET heartbeat_at = now() - interval '7 minutes'")
	workers, err := e.client.Workers(t.Context())
	if err != nil || len(workers) != 2 {
		t.Fatalf("list the workers: %d of 2, %v", len(workers), err)
	}

	for _, w := range workers {
		if w.Stale != (w.ID == old) {
			t.Errorf("the worker %s, silent for 7 minutes: stale is %v", w.ID, w.Stale)
		}
	}

	taskmill.RunRetention(t.Context(), newWorker(t, e, &taskmill.WorkerConfig{}))
	if n := e.count(t, "SELECT count(*) FROM {s}.workers WHERE id = $1", old); n != 0 {
		t.Errorf("the maintainer kept the worker %q, silent for 7 minutes, beside one with the longest lease", old)
	}
}

func TestCheckInsKeepLongTasksLeased(t *testing.T) {
	t.Parallel()
	sent := &statementLog{}
	e := newTracedEnv(t, sent)

	var runs atomic.Int32
	started, release := make(chan struct{}, 2), make(chan struct{})
	runWorker(t, e, &taskmill.WorkerConfig{
		Handlers: handlers(func(context.Context, *taskmill.Task[sendEmail]) error {
			runs.Add(1)
			started <- struct{}{}
			<-release
			return nil
		}, nil),
		Queues: defaultQueue(2),
		// Without check-ins, the task is rescued and runs again. The lease is
		// long enough that a loaded machine does not miss check-ins for all of
		// it.
		LeaseDuration: time.Second,
	})

	// The task runs until its lease, as the fetch set it, has ended, and the
	// maintainer has then looked twice for leases to rescue.
	id := e.enqueue(t, sendEmail{}, nil)
	recv(t, started, "the task to start")

	var ends time.Time
	if err := e.pool.QueryRow(t.Context(), replaceSchema("SELECT lease_expires_at FROM {s}.tasks WHERE id = $1", e.schema), id).Scan(&ends); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the task's first lease to end", func() bool { return e.count(t, "SELECT count(*) WHERE now() > $1", ends) == 1 })

	sent.reset()
	waitFor(t, "the maintainer to look for expired leases twice", func() bool {
		return sent.count("t.lease_expires_at < now()") >= 2
	})

	close(release)
	info := e.waitState(t, id, taskmill.StateCompleted)
	if runs.Load() != 1 || info.Attempt != 1 {
		t.Fatalf("task ran %d times over %d attempts; check-ins did not hold the lease", runs.Load(), info.Attempt)
	}
}

func TestLeaseRenewalSkipsLockedTasks(t *testing.T) {
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

// connHolders returns handlers that hold a connection of pool each, as
// holdConn does.
func connHolders(pool *pgxpool.Pool, started chan<- struct{}, release <-chan struct{}) *taskmill.Handlers {
	return handlers(holdConn(pool, started, release), nil)
}

// holdConn returns a handler that takes a connection from pool, and holds it
// until release is closed or its context ends. It says so on started while
// that has room: a retried task never blocks there.
func holdConn(pool *pgxpool.Pool, started chan<- struct{}, release <-chan struct{}) func(context.Context, *taskmill.Task[sendEmail]) error {
	return func(ctx context.Context, _ *taskmill.Task[sendEmail]) error {
		conn, err := pool.Acquire(ctx)
		if err != nil {
			return err
		}
		defer conn.Release()

		select {
		case started <- struct{}{}:
		default:
		}

		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return context.Cause(ctx)
		}
	}
}

func TestLeasesAndCompletionsDoNotWaitForHandlersThatHoldThePool(t *testing.T) {
	t.Parallel()
	pool := testdb.PoolSize(t, 3)
	logs := &logRecorder{}
	e := newEnvWith(t, pool, &taskmill.ClientConfig{Logger: slog.New(logs)})
	watch := *e
	watch.pool = testdb.Pool(t) // the test's own reads: the worker's pool is full

	// Three handlers take a connection each and hold it for longer than a
	// lease, as handlers that share the pool with the worker can. Two others
	// return once the pool is full, so that their completions find no free
	// connection either.
	const lease = 600 * time.Millisecond
	holding, full, release := make(chan struct{}, 3), make(chan struct{}), make(chan struct{})
	hold := holdConn(pool, holding, release)
	runs := map[int64]*atomic.Int32{}
	var ids []int64
	for i := range 5 {
		id := e.enqueue(t, sendEmail{UserID: int64(i)}, nil)
		ids, runs[id] = append(ids, id), &atomic.Int32{}
	}
	w := newWorker(t, e, &taskmill.WorkerConfig{
		Handlers: handlers(func(ctx context.Context, task *taskmill.Task[sendEmail]) error {
			runs[task.ID].Add(1)
			if task.Args.UserID < 3 {
				return hold(ctx, task)
			}
			<-full
			return nil
		}, nil),
		Queues:        defaultQueue(5),
		LeaseDuration: lease,
		NoMaintainer:  true,
	})
	startWorker(t, w)

	// The worker keeps a connection of its own, so two handlers hold the
	// rest, and the third waits for one.
	recv(t, holding, "a handler to take a connection")
	recv(t, holding, "a handler to take a connection")
	var since time.Time
	if err := watch.pool.QueryRow(t.Context(), "SELECT now()").Scan(&since); err != nil {
		t.Fatal(err)
	}
	close(full)

	waitFor(t, "the tasks that returned to complete", func() bool {
		return watch.count(t, "SELECT count(*) FROM {s}.task_history WHERE id = ANY($1)", ids[3:]) == 2
	})
	waitFor(t, "the leases of the running tasks to be renewed for a while", func() bool {
		return watch.count(t, "SELECT count(*) FROM {s}.tasks WHERE id = ANY($1) AND lease_expires_at > $2",
			ids[:3], since.Add(3*lease)) == 3
	})
	// The two free slots fetch, and wait for the full pool.
	waitFor(t, "a fetch to wait for the pool", func() bool { return w.Stats().PoolWaits > 0 })

	close(release)
	waitFor(t, "every task to complete", func() bool {
		return watch.count(t, "SELECT count(*) FROM {s}.task_history WHERE id = ANY($1)", ids) == len(ids)
	})
	const poolWarning = "the pool was full, so the worker waited for a connection; raise MaxConns, or give handlers a pool of their own"
	waitFor(t, "the warning that the fetch waited", func() bool { return logs.count(poolWarning) > 0 })
	for _, r := range logs.all() {
		if r.Message == poolWarning && (r.Level != slog.LevelWarn || attr(r, "max_conns") != "3") {
			t.Errorf("got %s, want a warning with max_conns=3", recordText(r))
		}
	}
	for id, n := range runs {
		if n.Load() != 1 {
			t.Errorf("task %d ran %d times, want once", id, n.Load())
		}
	}
	if n := logs.count("attempt lost its lease; canceling it"); n > 0 {
		t.Errorf("%d attempts lost their leases", n)
	}
}

func TestWorkerDoesNotWarnWhenHandlersHaveTheirOwnPool(t *testing.T) {
	t.Parallel()
	// The handlers hold every connection of their own pool.
	logs := &logRecorder{}
	e := newEnvWith(t, testdb.PoolSize(t, 25), &taskmill.ClientConfig{Logger: slog.New(logs)})
	own := testdb.PoolSize(t, 4)
	const lease = 600 * time.Millisecond
	started, release := make(chan struct{}, 4), make(chan struct{})
	runWorker(t, e, &taskmill.WorkerConfig{
		Handlers:      connHolders(own, started, release),
		Queues:        defaultQueue(20),
		LeaseDuration: lease,
	})

	ids := make([]int64, 4)
	for i := range ids {
		ids[i] = e.enqueue(t, sendEmail{UserID: int64(i)}, nil)
	}
	for range ids {
		recv(t, started, "a handler to take a connection")
	}

	var now time.Time
	if err := e.pool.QueryRow(t.Context(), "SELECT clock_timestamp()").Scan(&now); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the leases to be renewed for a while", func() bool {
		return e.count(t, "SELECT count(*) FROM {s}.tasks WHERE id = ANY($1) AND lease_expires_at > $2",
			ids, now.Add(2*lease)) == len(ids)
	})

	close(release)
	for _, id := range ids {
		e.waitState(t, id, taskmill.StateCompleted)
	}

	for _, r := range logs.all() {
		if r.Level >= slog.LevelWarn && strings.Contains(r.Message, "pool") {
			t.Errorf("got %s, want no warning about the pool", recordText(r))
		}
	}
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
	w := newWorker(t, e, &taskmill.WorkerConfig{
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
	startWorker(t, w)

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
	if info.Attempt != 2 || len(info.Errors) != 1 || info.Errors[0].Code != taskmill.CodeLeaseLost {
		t.Fatalf("got attempt %d with errors %+v, want attempt 2 after a lost lease", info.Attempt, info.Errors)
	}
	if n := w.Stats().LeasesLost; n != 1 {
		t.Errorf("the worker counted %d lost leases, want 1", n)
	}
}

// A worker can fetch a task again while its own older attempt of that task
// still runs: it was cut off for longer than the lease, another worker's
// maintainer rescued the task, and its fetch came before its check-in. No
// check-in sees the older attempt after that, so the worker cancels it when
// it fetches the task.
func TestFetchingATaskAgainCancelsItsOlderAttempt(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	first := make(chan struct{})
	cause := make(chan error, 1)
	runWorker(t, e, &taskmill.WorkerConfig{
		Handlers: handlers(func(ctx context.Context, task *taskmill.Task[sendEmail]) error {
			if task.Attempt > 1 {
				return nil
			}
			close(first)
			<-ctx.Done()
			cause <- context.Cause(ctx)
			return context.Cause(ctx)
		}, nil),
		Queues:        defaultQueue(2),
		LeaseDuration: time.Minute, // no check-in comes during the test
		NoMaintainer:  true,
	})

	id := e.enqueue(t, sendEmail{}, nil)
	recv(t, first, "the first attempt to start")

	// What the rescue and the promotion do: the task is available again. The
	// worker fetches it at its next poll, and completes it.
	e.exec(t, `UPDATE {s}.tasks SET state = 'available', lease_expires_at = NULL WHERE id = $1`, id)
	e.waitState(t, id, taskmill.StateCompleted)

	if err := recv(t, cause, "the first attempt to be canceled"); !errors.Is(err, taskmill.ErrLeaseLost) {
		t.Fatalf("the first attempt was canceled with %v, want ErrLeaseLost", err)
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
	// leases, and healthy again at its first check-in once it is back.
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

// A handler that ignores its context keeps its slot after the worker
// abandons its attempt. Once such handlers hold every slot, the worker can
// start no task, and Health says so. One free slot is enough to be healthy.
func TestHealthFailsWhileStuckHandlersHoldEverySlot(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	started, release := make(chan struct{}, 2), make(chan struct{})
	w := newWorker(t, e, &taskmill.WorkerConfig{
		Handlers: handlers(func(context.Context, *taskmill.Task[sendEmail]) error {
			started <- struct{}{}
			<-release // ignores its context
			return nil
		}, &taskmill.HandlerOptions{Timeout: 100 * time.Millisecond}),
		Queues: defaultQueue(2),
	})
	startWorker(t, w)
	unblock := sync.OnceFunc(func() { close(release) })
	t.Cleanup(unblock) // before the worker stops

	once := &taskmill.EnqueueOptions{MaxAttempts: 1}
	e.enqueue(t, sendEmail{}, once)
	recv(t, started, "the first handler to start")
	waitFor(t, "a stuck slot", func() bool { return w.Stats().SlotsStuck == 1 })
	if err := w.Health(); err != nil {
		t.Fatalf("with one slot of two stuck: %v, want healthy", err)
	}

	e.enqueue(t, sendEmail{}, once)
	recv(t, started, "the second handler to start")
	waitFor(t, "the worker to be unhealthy", func() bool { return w.Health() != nil })
	if err := w.Health(); !strings.Contains(err.Error(), "all 2 slots") {
		t.Errorf("got %q, want an error that says that all 2 slots are stuck", err)
	}
	if s := w.Stats(); s.HandlersAbandoned != 2 || s.Slots != 2 || s.SlotsBusy != 2 || s.SlotsStuck != 2 {
		t.Errorf("got %+v, want 2 abandoned handlers that hold both slots", s)
	}

	unblock()
	waitFor(t, "the worker to be healthy again", func() bool { return w.Health() == nil })
	waitFor(t, "the slots to be free", func() bool { s := w.Stats(); return s.SlotsBusy == 0 && s.SlotsStuck == 0 })
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

// The statements that find a batch of tasks by their attempts look each
// attempt's task up once. Their work grows with the number of attempts, not
// with its square, also with the generic plan that a worker's connection
// keeps for them.
func TestLeasedTasksCostOneLookupPerAttempt(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	const attempts = 2000

	e.exec(t, `INSERT INTO {s}.tasks (queue, kind, state, run_at, payload, max_attempts)
	           SELECT 'default', 'email.send', 'available', now(), '{}', 1 FROM generate_series(1, 50000)`)

	// The running tasks were all asked to stop, as after a bulk cancel.
	e.exec(t, `INSERT INTO {s}.tasks (queue, kind, state, run_at, payload, max_attempts, attempt, lease_token,
	                                 lease_expires_at, attempted_at, attempted_by, cancel_requested_at)
	           SELECT 'default', 'email.send', 'running', now(), '{}', 1, 1, 1, now() + interval '1 minute', now(), 'w', now()
	           FROM generate_series(1, $1)`, attempts)
	e.exec(t, "ANALYZE {s}.tasks")

	var ids []int64
	var tokens []int32
	running := replaceSchema("SELECT array_agg(id), array_agg(lease_token) FROM {s}.tasks WHERE state = 'running'", e.schema)
	if err := e.pool.QueryRow(t.Context(), running).Scan(&ids, &tokens); err != nil {
		t.Fatal(err)
	}

	n, err := taskmill.LeasedRowsRead(t.Context(), e.client, ids, tokens)
	if err != nil {
		t.Fatal(err)
	}

	// The lock reads each task twice, the renewal three times and returns
	// it, the read of cancel requests once, and each of the two completions
	// reads it to lock it and to delete it, and returns it. The completion
	// that skips held rows looks up only the tasks that it skipped again.
	if n > 13*attempts {
		t.Fatalf("%d attempts read and compared %d rows, want at most %d", attempts, n, 13*attempts)
	}
}

// The statements that find tasks by their attempts find a task only for an
// attempt with its ID and its lease token, and only while it runs: those of
// a batch of attempts, and those that end one attempt. Without the lease
// token, an attempt that lost its task could fail, snooze, or release the
// task under the attempt that runs it now.
func TestAttemptsFindOnlyTheTasksTheyOwn(t *testing.T) {
	t.Parallel()
	e := newEnv(t)

	task := func(state string, token int) int64 {
		var lease any
		if state == "running" {
			lease = time.Now().Add(time.Minute)
		}
		return int64(e.count(t, `INSERT INTO {s}.tasks (queue, kind, state, run_at, payload, max_attempts, attempt, lease_token,
		                                               lease_expires_at, attempted_at, attempted_by)
		                         VALUES ('default', 'email.send', $1, now(), '{}', 5, 1, $2, $3, now(), 'w')
		                         RETURNING id`, state, token, lease))
	}

	owned, alsoOwned := task("running", 3), task("running", 7)
	takenOver := task("running", 5)   // its attempt holds token 4
	released := task("available", 2)  // its attempt holds its token, but the task no longer runs
	swapped := task("running", 1)     // its attempt holds the token of the next task,
	swappedBack := task("running", 2) // and that task's attempt holds this one's
	gone := swappedBack + 1           // no such task

	// Someone asked to cancel one task of each sort.
	e.exec(t, "UPDATE {s}.tasks SET cancel_requested_at = now() WHERE id = ANY($1)",
		[]int64{alsoOwned, takenOver, released, swapped})

	found, err := taskmill.LeasedFound(t.Context(), e.client,
		[]int64{owned, takenOver, released, swapped, swappedBack, gone, alsoOwned, owned},
		[]int32{3, 4, 2, 2, 1, 1, 7, 3})
	if err != nil {
		t.Fatal(err)
	}

	// The lock and the renewal return each task they find twice: as locked
	// or renewed, and as owned. The others return it once. An attempt
	// listed twice changes none of this. The failure, the snooze, and the
	// release run once for each attempt of the list, so they find the task
	// of the attempt that is listed twice both times.
	twice := []int64{owned, owned, alsoOwned, alsoOwned}
	each := []int64{owned, owned, alsoOwned}
	statements := []string{
		"the lock", "the renewal", "the read of cancel requests", "the completion that skips held rows", "the completion",
		"the failure", "the snooze", "the release",
	}
	for i, want := range [][]int64{twice, twice, {alsoOwned}, {owned, alsoOwned}, {owned, alsoOwned}, each, each, each} {
		if !slices.Equal(found[i], want) {
			t.Errorf("%s found the tasks %v, want %v", statements[i], found[i], want)
		}
	}

	for id, state := range map[int64]taskmill.State{
		owned: taskmill.StateCompleted, alsoOwned: taskmill.StateCompleted,
		takenOver: taskmill.StateRunning, released: taskmill.StateAvailable,
		swapped: taskmill.StateRunning, swappedBack: taskmill.StateRunning,
	} {
		e.wantState(t, id, state)
	}
}

func TestWorkerRemovedAsStaleRegistersAgain(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	logs := &logRecorder{}

	// The check-in that finds the row gone registers again within its third
	// of the lease. If that runs out, the next check-in says so again: a
	// third of 300 ms was too short on a busy machine.
	runWorker(t, e.withLogger(t, slog.New(logs)), &taskmill.WorkerConfig{
		Handlers: noopHandlers(), NoMaintainer: true, LeaseDuration: time.Second,
	})
	e.waitListening(t)

	// What the maintainer does to a worker that has not checked in for 5
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
