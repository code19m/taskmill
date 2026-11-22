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

	"github.com/jackc/pgx/v5"

	"github.com/code19m/taskmill"
	"github.com/code19m/taskmill/internal/testdb"
)

func TestDiscardCompletedSkipsHistory(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	done := make(chan struct{}, 1)
	runWorker(t, e, &taskmill.WorkerConfig{
		Handlers: handlers(func(context.Context, *taskmill.Task[sendEmail]) error {
			done <- struct{}{}
			return nil
		}, &taskmill.HandlerOptions{DiscardCompleted: true}),
	})

	id := e.enqueue(t, sendEmail{}, nil)
	recv(t, done, "the handler to run")
	waitFor(t, "the completed task to be deleted", func() bool {
		_, err := e.client.Get(t.Context(), id)
		return errors.Is(err, taskmill.ErrNotFound)
	})

	if n := e.count(t, "SELECT count(*) FROM {s}.task_history"); n != 0 {
		t.Fatalf("history holds %d tasks", n)
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
	sent := &statementLog{}
	e := newTracedEnv(t, sent)

	causes := make(chan error, 2)
	completed, release := make(chan struct{}, 2), make(chan struct{})
	defer close(release)
	runWorker(t, e, &taskmill.WorkerConfig{
		Handlers: handlers(func(ctx context.Context, task *taskmill.Task[sendEmail]) error {
			err := pgx.BeginFunc(ctx, e.pool, func(tx pgx.Tx) error {
				if err := task.CompleteTx(ctx, taskmill.Pgx(tx)); err != nil {
					return err
				}

				// More work in the same transaction, for longer than the lease.
				completed <- struct{}{}
				<-release
				return nil
			})
			causes <- context.Cause(ctx)
			return err
		}, nil),
		Queues: defaultQueue(1),
		// Until the handler completes the task, only check-ins keep the
		// lease. It is long enough that a loaded machine does not miss them
		// for all of it.
		LeaseDuration: time.Second,
	})

	// The transaction stays open until the lease has ended, and the worker
	// and the maintainer have then each looked twice for such leases. The
	// transaction holds the task's row, so no check-in renews the lease, and
	// other sessions see the task as it was before.
	id := e.enqueue(t, sendEmail{}, nil)
	recv(t, completed, "the handler to complete the task in its transaction")

	var ends time.Time
	if err := e.pool.QueryRow(t.Context(), replaceSchema("SELECT lease_expires_at FROM {s}.tasks WHERE id = $1", e.schema), id).Scan(&ends); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the task's lease to end", func() bool { return e.count(t, "SELECT count(*) WHERE now() > $1", ends) == 1 })

	sent.reset()
	waitFor(t, "the worker and the maintainer to look for ended leases twice", func() bool {
		return sent.count("renewed AS") >= 2 && sent.count("t.lease_expires_at < now()") >= 2
	})

	release <- struct{}{}
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

// A task's row that another transaction holds, as an open CancelTx does,
// keeps only that task's completion waiting, not those of the tasks that
// complete with it: their statement skips the row, and the worker tries it
// again once the row is free. If the row stays held for longer than the
// lease, the worker warns once. (No maintainer runs here: its rescue would
// take the task whose lease ran out, once the row is free.)
func TestHeldTaskDoesNotHoldUpOtherCompletions(t *testing.T) {
	t.Parallel()
	logs := &logRecorder{}
	e := newEnv(t).withLogger(t, slog.New(logs))
	release := make(chan struct{})
	started := make(chan struct{}, 10)
	w := newWorker(t, e, &taskmill.WorkerConfig{
		Handlers: handlers(func(context.Context, *taskmill.Task[sendEmail]) error {
			started <- struct{}{}
			<-release
			return nil
		}, nil),
		Queues:        defaultQueue(10),
		LeaseDuration: time.Second,
		NoMaintainer:  true,
	})
	startWorker(t, w)

	var ids []int64
	for range 5 {
		ids = append(ids, e.enqueue(t, sendEmail{}, nil))
	}
	for range ids {
		recv(t, started, "a handler to start")
	}

	held := ids[2]
	tx := e.begin(t)
	e.txExec(t, tx, "SELECT FROM {s}.tasks WHERE id = $1 FOR UPDATE", held)

	close(release)
	for _, id := range ids {
		if id != held {
			e.waitState(t, id, taskmill.StateCompleted)
		}
	}
	e.wantState(t, held, taskmill.StateRunning)

	const waits = "completion waits for a row that another transaction holds"
	waitFor(t, "the warning", func() bool { return logs.count(waits) > 0 })
	if err := tx.Commit(t.Context()); err != nil {
		t.Fatal(err)
	}
	if info := e.waitState(t, held, taskmill.StateCompleted); info.Attempt != 1 {
		t.Fatalf("completed on attempt %d, want 1", info.Attempt)
	}
	if n := logs.count(waits); n != 1 {
		t.Errorf("the held completion was logged %d times, want once", n)
	}
	if n := w.Stats().CompletionsBlocked; n != 1 {
		t.Errorf("the worker counted %d blocked completions, want 1", n)
	}
}

// The failure of a grouped task's last attempt halts its group, so it waits
// for the group's row, which a transaction that enqueues into the group
// holds. It waits without holding the task's row for long, which the
// renewal of the lease would skip: the lease stays renewed however long the
// group is held, and the failure is recorded once the group is free.
func TestFailureOfAGroupedTaskWaitsForItsGroupWithItsLease(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	const lease = time.Second
	failed := make(chan struct{}, 1)
	w := newWorker(t, e, &taskmill.WorkerConfig{
		Handlers: handlers(func(context.Context, *taskmill.Task[sendEmail]) error {
			failed <- struct{}{}
			return errors.New("the API refused")
		}, nil),
		Queues:        defaultQueue(1),
		LeaseDuration: lease,
	})
	timing := testTiming
	timing.LockWait = 50 * time.Millisecond
	taskmill.SetTiming(w, timing)

	id := e.enqueue(t, sendEmail{}, &taskmill.EnqueueOptions{GroupKey: "g", MaxAttempts: 1})
	tx := e.begin(t)
	e.txExec(t, tx, "SELECT FROM {s}.groups WHERE group_key = 'g' FOR UPDATE")
	startWorker(t, w)
	recv(t, failed, "the attempt to fail")

	var since time.Time
	if err := e.pool.QueryRow(t.Context(), "SELECT now()").Scan(&since); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the lease to be renewed for longer than a lease while the failure waits", func() bool {
		return e.count(t, "SELECT count(*) FROM {s}.tasks WHERE id = $1 AND lease_expires_at > $2", id, since.Add(3*lease)) == 1
	})
	if err := tx.Commit(t.Context()); err != nil {
		t.Fatal(err)
	}

	info := e.waitState(t, id, taskmill.StateDead)
	if last := info.Errors[len(info.Errors)-1]; last.Message != "the API refused" {
		t.Fatalf("the task's last error is %q, want the handler's", last.Message)
	}
}

// A completion that the database takes long for, longer than a third of
// the lease, still goes through: the task runs once. Renewals are as slow,
// so they fail, and the completion has the lease that the task began with.
func TestSlowCompletionCompletesTheTaskOnce(t *testing.T) {
	t.Parallel()
	e := newTracedEnv(t, &slowStatement{fragment: "SELECT DISTINCT id, lease_token FROM unnest", delay: 1500 * time.Millisecond})
	var runs atomic.Int32
	runWorker(t, e, &taskmill.WorkerConfig{
		Handlers: handlers(func(context.Context, *taskmill.Task[sendEmail]) error {
			runs.Add(1)
			return nil
		}, nil),
		Queues:        defaultQueue(1),
		LeaseDuration: 3 * time.Second,
	})

	id := e.enqueue(t, sendEmail{}, nil)
	if info := e.waitState(t, id, taskmill.StateCompleted); info.Attempt != 1 || runs.Load() != 1 {
		t.Fatalf("completed on attempt %d after %d runs, want one", info.Attempt, runs.Load())
	}
}

// A completion that the worker cannot record names its tasks in the log:
// they run again, and their work may need to be checked.
func TestFailedCompletionNamesItsTasks(t *testing.T) {
	t.Parallel()
	db := newProxy(t, testdb.URL(t))
	logs := &logRecorder{}
	e := newEnvWith(t, proxiedPool(t, db, 6, nil), &taskmill.ClientConfig{Logger: slog.New(logs)})
	started, release := make(chan struct{}), make(chan struct{})
	startWorker(t, newWorker(t, e, &taskmill.WorkerConfig{
		Handlers: handlers(func(context.Context, *taskmill.Task[sendEmail]) error {
			close(started)
			<-release
			return nil
		}, nil),
		Queues:        defaultQueue(1),
		LeaseDuration: 600 * time.Millisecond,
		NoMaintainer:  true,
	}))
	t.Cleanup(func() { db.cut(false) }) // so that the worker stops soon

	id := e.enqueue(t, sendEmail{}, nil)
	<-started
	db.cut(true)
	close(release)

	const failed = "complete tasks failed; they are rescued when their leases run out"
	waitFor(t, "the completion to be given up", func() bool { return logs.count(failed) > 0 })
	for _, r := range logs.all() {
		if r.Message == failed && attr(r, "task_ids") != fmt.Sprint([]int64{id}) {
			t.Errorf("got %s, want task_ids=[%d]", recordText(r), id)
		}
	}
}

// slowStatement makes each statement that contains fragment take delay
// longer, as a busy database would.
type slowStatement struct {
	fragment string
	delay    time.Duration
}

func (s *slowStatement) TraceQueryStart(ctx context.Context, _ *pgx.Conn, d pgx.TraceQueryStartData) context.Context {
	if strings.Contains(d.SQL, s.fragment) {
		time.Sleep(s.delay)
	}
	return ctx
}

func (*slowStatement) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

func TestHeldGroupsDoNotTieUpConnections(t *testing.T) {
	t.Parallel()
	// A small pool, as pgxpool's default is on a small machine.
	e := newEnvWith(t, testdb.PoolSize(t, 4), nil)

	// A check-in has a third of the lease to renew it, and shares this pool
	// of 4 with the maintainer, which the test timing makes busy. On a busy
	// machine, the wait for a connection and the statement can each take
	// hundreds of milliseconds, so shorter leases ran out even with no group
	// held.
	const lease = 2 * time.Second

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

	// Another application's transaction holds every group's row while the
	// six tasks finish, and until the worker has renewed their leases.
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

	// The held tasks' leases must survive, which needs free connections:
	// the worker renews all six while their completions wait.
	var before time.Time
	if err := e.pool.QueryRow(t.Context(), replaceSchema("SELECT max(lease_expires_at) FROM {s}.tasks", e.schema)).Scan(&before); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the worker to renew the leases of the six held tasks", func() bool {
		return e.count(t, "SELECT count(*) FROM {s}.tasks WHERE lease_expires_at > $1", before) == len(grouped)
	})

	if n := e.count(t, `SELECT count(*) FROM pg_stat_activity WHERE wait_event_type = 'Lock' AND strpos(query, $1) > 0`,
		e.schema+".groups"); n != 0 {
		t.Fatalf("%d statements wait for the held groups, each on a connection of the pool", n)
	}

	if err := tx.Commit(t.Context()); err != nil {
		t.Fatal(err)
	}

	for _, id := range grouped {
		if info := e.waitState(t, id, taskmill.StateCompleted); info.Attempt != 1 {
			t.Fatalf("task %d completed on attempt %d; it lost its lease", id, info.Attempt)
		}
	}
}

// A completion waits for the row of a task that another transaction holds.
// It locks the rows in the order of their IDs before it deletes any, so that
// it cannot deadlock with a transaction that takes tasks in that order too,
// such as one that cancels several of them. That holds whatever the order
// in which the attempts ended, wherever the rows lie in the table, and
// however the planner joins the attempts to the tasks.
func TestCompletionLocksTasksInTheOrderOfTheirIDs(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ctx := t.Context()

	e.exec(t, `INSERT INTO {s}.tasks (queue, kind, state, run_at, payload, max_attempts, attempt, lease_token,
	                                 lease_expires_at, attempted_at, attempted_by)
	           SELECT 'default', 'email.send', 'running', now(), '{}', 5, 1, 1, now() + interval '1 minute', now(), 'w'
	           FROM generate_series(1, 20000)`)

	// The lease of the older task was renewed, which moved its row behind
	// those of the tasks after it: the table's order is not that of the IDs.
	const older, newer = 3, 9
	e.exec(t, "UPDATE {s}.tasks SET lease_expires_at = lease_expires_at + interval '1 minute' WHERE id = $1", older)
	e.exec(t, "ANALYZE {s}.tasks")
	if e.count(t, "SELECT count(*) FROM {s}.tasks o, {s}.tasks n WHERE o.id = $1 AND n.id = $2 AND o.ctid > n.ctid", older, newer) != 1 {
		t.Fatal("the row of the older task does not lie behind the newer one's")
	}

	// The attempts ended newest first.
	attempts := func(n int) []int64 {
		ids := make([]int64, n)
		for i := range ids {
			ids[i] = int64(older + n - 1 - i)
		}
		return ids
	}

	type plan struct {
		settings string
		attempts int
	}

	// Each plan that the server may choose. With 200 attempts and the
	// planner's own choice, it can join the attempts to the tasks by reading
	// the whole table. Fewer attempts get a custom and a generic plan under
	// every set of join methods but none (nested loop, hash, merge).
	plans := []plan{{"SELECT set_config('plan_cache_mode', 'auto', true)", 200}}
	for _, mode := range []string{"force_custom_plan", "force_generic_plan"} {
		for _, on := range [][3]bool{
			{true, true, true},
			{false, true, true},
			{true, false, true},
			{true, true, false},
			{true, false, false},
			{false, true, false},
			{false, false, true},
		} {
			plans = append(plans, plan{fmt.Sprintf("SELECT set_config('plan_cache_mode', '%s', true), set_config('enable_nestloop', '%t', true), "+
				"set_config('enable_hashjoin', '%t', true), set_config('enable_mergejoin', '%t', true)", mode, on[0], on[1], on[2]), 7})
		}
	}

	for _, p := range plans {
		// A transaction cancels the older task, and so holds its row. The
		// completion then waits for that row and holds no other yet, so
		// the transaction gets the newer task too.
		tx := e.begin(t)
		if err := e.client.CancelTx(ctx, taskmill.Pgx(tx), older); err != nil {
			t.Fatal(err)
		}

		n, err := completeWhileHeld(t, e, p.settings, attempts(p.attempts), func() error {
			if err := e.client.CancelTx(ctx, taskmill.Pgx(tx), newer); err != nil {
				_ = tx.Rollback(ctx)
				return err
			}
			return tx.Rollback(ctx)
		})
		if err != nil || n != p.attempts {
			t.Errorf("%s: the completion finished %d of %d tasks, %v", p.settings, n, p.attempts, err)
		}
	}

	// Another worker took the older task while the completion waited for its
	// row: the completion leaves that task.
	tx := e.begin(t)
	e.txExec(t, tx, "UPDATE {s}.tasks SET lease_token = lease_token + 1 WHERE id = $1", older)

	n, err := completeWhileHeld(t, e, plans[0].settings, attempts(7), func() error { return tx.Commit(ctx) })
	if err != nil || n != 6 {
		t.Errorf("the completion finished %d tasks, %v; want the 6 that the attempts still own", n, err)
	}
}

// completeWhileHeld completes the tasks ids, planned with settings, while a
// transaction holds the row of one of them. Once the completion waits for
// that row, it calls release, which ends the transaction. It returns how
// many tasks the completion finished.
func completeWhileHeld(t *testing.T, e *env, settings string, ids []int64, release func() error) (int, error) {
	t.Helper()
	type result struct {
		tasks int
		err   error
	}
	done := make(chan result, 1)
	go func() {
		n, err := taskmill.Complete(t.Context(), e.client, settings, ids)
		done <- result{n, err}
	}()

	e.waitBlocked(t, "INSERT INTO {s}.task_history")
	if err := release(); err != nil {
		t.Errorf("%s, %d attempts: the transaction that held a task failed: %v", settings, len(ids), err)
	}

	got := <-done
	return got.tasks, got.err
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
