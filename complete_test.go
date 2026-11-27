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

// A group into which short transactions enqueue one after another, each
// holding the group's row for a few milliseconds, completes its tasks as fast
// as they are enqueued: a completion that finds the row held waits for it,
// and PostgreSQL hands the row over between two of the transactions. Tries
// that skip the held row rarely find it free: without the wait, 2 to 4 of
// these 100 tasks completed while they were enqueued. With it, 1 to 4 were
// left, and up to 42 on a busy machine.
func TestBusyGroupKeepsUpWithItsProducer(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	runWorker(t, e, &taskmill.WorkerConfig{
		Handlers:     handlers(func(context.Context, *taskmill.Task[sendEmail]) error { return nil }, nil),
		Queues:       defaultQueue(4),
		NoMaintainer: true,
	})

	const tasks, every, hold = 100, 14 * time.Millisecond, 5 * time.Millisecond
	next := time.Now()
	for range tasks {
		time.Sleep(time.Until(next))
		next = next.Add(every)
		tx := e.begin(t)
		if _, err := e.client.EnqueueTx(t.Context(), taskmill.Pgx(tx), sendEmail{}, &taskmill.EnqueueOptions{GroupKey: "busy"}); err != nil {
			t.Fatal(err)
		}
		time.Sleep(hold)
		if err := tx.Commit(t.Context()); err != nil {
			t.Fatal(err)
		}
	}

	const busy = "SELECT count(*) FROM {s}.tasks WHERE group_key = 'busy'"
	if left := e.count(t, busy); left > tasks/2 {
		t.Errorf("%d of %d tasks were left when the producer ended, want at most %d", left, tasks, tasks/2)
	}
	waitFor(t, "the group to finish", func() bool { return e.count(t, busy) == 0 })
}

// A completion whose task's row another transaction holds, as an open
// CancelTx does, in a group that short transactions keep holding, waits for
// the group's row again a second later at the soonest: it does not wait
// once for each of those transactions.
func TestHeldTaskOfABusyGroupWaitsForItsGroupInTurn(t *testing.T) {
	t.Parallel()
	log := &statementLog{}
	e := newTracedEnv(t, log)
	started, release := make(chan int64, 1), make(chan struct{})
	runWorker(t, e, &taskmill.WorkerConfig{
		Handlers: handlers(func(_ context.Context, task *taskmill.Task[sendEmail]) error {
			if task.Args.UserID == 1 {
				started <- task.ID
				<-release
			}
			return nil
		}, nil),
		Queues:       defaultQueue(4),
		NoMaintainer: true,
	})

	id := e.enqueue(t, sendEmail{UserID: 1}, &taskmill.EnqueueOptions{GroupKey: "busy"})
	recv(t, started, "the head to start")
	cancelTx := e.begin(t)
	if err := e.client.CancelTx(t.Context(), taskmill.Pgx(cancelTx), id); err != nil {
		t.Fatal(err)
	}

	// A producer holds the group's row for 11 of every 14 ms.
	stop, stopped := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(stopped)
		for next := time.Now(); ; next = next.Add(14 * time.Millisecond) {
			select {
			case <-stop:
				return
			case <-time.After(time.Until(next)):
			}
			tx := e.begin(t)
			if _, err := e.client.EnqueueTx(t.Context(), taskmill.Pgx(tx), sendEmail{}, &taskmill.EnqueueOptions{GroupKey: "busy"}); err != nil {
				t.Error(err)
				return
			}
			time.Sleep(11 * time.Millisecond)
			if err := tx.Commit(t.Context()); err != nil {
				t.Error(err)
				return
			}
		}
	}()

	close(release)
	time.Sleep(200 * time.Millisecond) // the completion reaches the completer
	log.reset()
	time.Sleep(time.Second)
	waits := log.count("set_config('lock_timeout'") // each wait for the group's row sets one
	close(stop)
	<-stopped
	if err := cancelTx.Commit(t.Context()); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the head to finish", func() bool { return e.get(t, id).State.Finished() })

	if waits > 3 {
		t.Errorf("the completion waited for its group's row %d times in a second, want a second apart", waits)
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

// A task that completes after its cancel was requested takes back the dedupe
// key that the cancel freed, but a transaction that claimed the key meanwhile
// holds up neither its completion nor those of the tasks that complete with
// it: the worker takes the key back once the completion committed, and gives
// up after a moment. If that transaction then rolls back, the task stays
// without its key.
func TestAClaimedKeyDoesNotHoldUpCompletions(t *testing.T) {
	t.Parallel()
	logs := &logRecorder{}
	e := newEnv(t).withLogger(t, slog.New(logs))
	started := make(chan struct{}, 2)
	release := make(chan struct{})
	runWorker(t, e, &taskmill.WorkerConfig{
		Handlers: handlers(func(context.Context, *taskmill.Task[sendEmail]) error {
			started <- struct{}{}
			<-release
			return nil
		}, nil),
		Queues: defaultQueue(2),
	})

	key := &taskmill.EnqueueOptions{DedupeKey: "order-1"}
	keyed := e.keyed(t, key).ID
	plain := e.enqueue(t, sendEmail{}, nil)
	recv(t, started, "a handler to start")
	recv(t, started, "a handler to start")
	e.cancel(t, keyed)

	tx := e.begin(t)
	if _, err := e.client.EnqueueTx(t.Context(), taskmill.Pgx(tx), sendEmail{}, key); err != nil {
		t.Fatal(err)
	}
	close(release)
	e.waitState(t, plain, taskmill.StateCompleted)
	e.waitState(t, keyed, taskmill.StateCompleted)
	const gaveUp = "completed tasks could not take back the dedupe keys that their cancels freed"
	waitFor(t, "the warning", func() bool { return logs.count(gaveUp) > 0 })

	if err := tx.Rollback(t.Context()); err != nil {
		t.Fatal(err)
	}
	if res := e.keyed(t, key); res.Duplicate {
		t.Fatalf("the key went back to task %d, whose claim gave up", res.ID)
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

	if err := tx.Commit(t.Context()); err != nil {
		t.Fatal(err)
	}

	for _, id := range grouped {
		if info := e.waitState(t, id, taskmill.StateCompleted); info.Attempt != 1 {
			t.Fatalf("task %d completed on attempt %d; it lost its lease", id, info.Attempt)
		}
	}
}

// Completions whose FIFO groups' rows another transaction holds wait for
// them, up to 4 at a time, each in a transaction that writes nothing before
// it holds the group's row. Once each of them waited in vain, one at a time
// waits again, so that a long transaction soon ties up one connection.
func TestHeldGroupsAreWaitedForFewAtATime(t *testing.T) {
	t.Parallel()
	const groups = 8
	waits := &groupWaitLog{groups: groups, waited: map[string]bool{}}
	e := newTracedEnv(t, waits)

	started := make(chan struct{}, groups)
	release := make(chan struct{})
	runWorker(t, e, &taskmill.WorkerConfig{
		Handlers: handlers(func(context.Context, *taskmill.Task[sendEmail]) error {
			started <- struct{}{}
			<-release
			return nil
		}, nil),
		Queues:       defaultQueue(groups),
		NoMaintainer: true,
	})
	var ids []int64
	for i := range groups {
		ids = append(ids, e.enqueue(t, sendEmail{}, &taskmill.EnqueueOptions{GroupKey: strconv.Itoa(i)}))
	}
	for range groups {
		recv(t, started, "a handler to start")
	}

	tx, err := testdb.Pool(t).Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if _, err := tx.Exec(t.Context(), replaceSchema("SELECT FROM {s}.groups FOR UPDATE", e.schema)); err != nil {
		t.Fatal(err)
	}
	// A sequence counts the rows written to history, also by a transaction
	// that rolls back.
	e.exec(t, `CREATE SEQUENCE {s}.history_writes`)
	e.exec(t, `CREATE FUNCTION {s}.count_history_write() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    PERFORM nextval('{s}.history_writes');
    RETURN NEW;
END $$`)
	e.exec(t, `CREATE TRIGGER count_history_write BEFORE INSERT ON {s}.task_history FOR EACH ROW EXECUTE FUNCTION {s}.count_history_write()`)
	close(release)
	waitFor(t, "every completion to wait for its group, and three to wait again", func() bool {
		waits.mu.Lock()
		defer waits.mu.Unlock()
		return waits.again >= 3
	})

	waits.mu.Lock()
	first, mostAgain := waits.first, waits.mostAgain
	waits.mu.Unlock()
	history := e.count(t, `SELECT count(*) FROM {s}.history_writes WHERE is_called`)
	if first < 2 || first > 4 {
		t.Errorf("%d completions waited for their groups at a time at first, want 2 to 4", first)
	}
	if mostAgain > 1 {
		t.Errorf("%d completions waited again at a time, want 1", mostAgain)
	}
	if history > 0 {
		t.Error("a completion wrote its task to history while its group was held")
	}

	if err := tx.Commit(t.Context()); err != nil {
		t.Fatal(err)
	}
	for _, id := range ids {
		e.waitState(t, id, taskmill.StateCompleted)
	}
}

// A worker completes the grouped tasks of a batch in one transaction of two
// round trips: the first locks the groups' rows and completes the tasks,
// the second recounts and settles the groups and commits. So it holds the
// groups' rows for one round trip.
func TestGroupedCompletionsTakeTwoRoundTrips(t *testing.T) {
	t.Parallel()
	trips := &groupedTrips{}
	e := newTracedEnv(t, trips)

	// The tasks are enqueued before the worker starts: a completion whose
	// group's row an enqueue holds waits for it, in round trips of their own.
	var ids []int64
	for i := range 12 {
		ids = append(ids, e.enqueue(t, sendEmail{}, &taskmill.EnqueueOptions{GroupKey: strconv.Itoa(i % 4)}))
	}
	runWorker(t, e, &taskmill.WorkerConfig{Handlers: noopHandlers(), Queues: defaultQueue(4)})
	for _, id := range ids {
		e.waitState(t, id, taskmill.StateCompleted)
	}

	trips.mu.Lock()
	defer trips.mu.Unlock()
	if len(trips.other) > 0 {
		t.Fatalf("statements of a grouped completion went in other round trips: %q", trips.other)
	}
	if trips.first == 0 || trips.second != trips.first {
		t.Fatalf("%d round trips locked and completed, and %d settled and committed, want as many, and some", trips.first, trips.second)
	}
}

// groupedTrips counts the round trips of grouped completions, and keeps the
// statements of one that went in a round trip of another shape.
type groupedTrips struct {
	mu            sync.Mutex
	first, second int        // round trips that lock the groups and complete, and that settle them and commit
	other         [][]string // the statements of round trips of another shape
}

// groupedStep names the statements of a grouped completion, and returns ""
// for the others, except BEGIN and COMMIT.
func groupedStep(sql string) string {
	switch {
	case sql == "BEGIN" || sql == "COMMIT":
		return sql
	case strings.Contains(sql, "FOR UPDATE OF g SKIP LOCKED"):
		return "lock"
	case strings.Contains(sql, "task_history") && strings.Contains(sql, "FOR UPDATE SKIP LOCKED"): // a completion that locks groups
		return "complete"
	case strings.Contains(sql, "SET tasks = greatest(g.tasks + u.n, 0)"):
		return "recount"
	case strings.Contains(sql, "WITH emptied AS"):
		return "settle"
	}
	return ""
}

func (g *groupedTrips) TraceQueryStart(ctx context.Context, _ *pgx.Conn, d pgx.TraceQueryStartData) context.Context {
	if step := groupedStep(d.SQL); step != "" && step != "BEGIN" && step != "COMMIT" {
		g.mu.Lock()
		defer g.mu.Unlock()
		g.other = append(g.other, []string{step})
	}
	return ctx
}

func (*groupedTrips) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

func (g *groupedTrips) TraceBatchStart(ctx context.Context, _ *pgx.Conn, d pgx.TraceBatchStartData) context.Context {
	var steps []string
	grouped := false
	for _, q := range d.Batch.QueuedQueries {
		step := groupedStep(q.SQL)
		steps = append(steps, step)
		grouped = grouped || step != "" && step != "BEGIN" && step != "COMMIT"
	}
	if !grouped {
		return ctx
	}

	g.mu.Lock()
	defer g.mu.Unlock()
	switch strings.Join(steps, ",") {
	case "BEGIN,lock,complete":
		g.first++
	case "recount,settle,COMMIT":
		g.second++
	default:
		g.other = append(g.other, steps)
	}
	return ctx
}

func (*groupedTrips) TraceBatchQuery(context.Context, *pgx.Conn, pgx.TraceBatchQueryData) {}

func (*groupedTrips) TraceBatchEnd(context.Context, *pgx.Conn, pgx.TraceBatchEndData) {}

// groupWaitLog follows the round trips in which completions wait for their
// FIFO groups' rows.
type groupWaitLog struct {
	groups int // the groups whose rows are held

	mu        sync.Mutex
	running   int             // waits under way
	waited    map[string]bool // the groups whose completion waited, by the wait's arguments
	first     int             // most waits at a time until every group's completion waited
	again     int             // waits that began after that
	mostAgain int             // most waits at a time among them, and those under way
}

type groupWaitKey struct{}

func (*groupWaitLog) TraceQueryStart(ctx context.Context, _ *pgx.Conn, _ pgx.TraceQueryStartData) context.Context {
	return ctx
}

func (*groupWaitLog) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

func (l *groupWaitLog) TraceBatchStart(ctx context.Context, _ *pgx.Conn, d pgx.TraceBatchStartData) context.Context {
	i := slices.IndexFunc(d.Batch.QueuedQueries, func(q *pgx.QueuedQuery) bool {
		return strings.Contains(q.SQL, "FOR UPDATE OF g") && !strings.Contains(q.SQL, "SKIP LOCKED")
	})
	if i < 0 {
		return ctx
	}

	l.mu.Lock()
	defer l.mu.Unlock()
	l.running++
	if len(l.waited) < l.groups {
		l.first = max(l.first, l.running)
	} else {
		l.again++
		l.mostAgain = max(l.mostAgain, l.running)
	}
	return context.WithValue(ctx, groupWaitKey{}, fmt.Sprint(d.Batch.QueuedQueries[i].Arguments...))
}

func (*groupWaitLog) TraceBatchQuery(context.Context, *pgx.Conn, pgx.TraceBatchQueryData) {}

func (l *groupWaitLog) TraceBatchEnd(ctx context.Context, _ *pgx.Conn, _ pgx.TraceBatchEndData) {
	if group, ok := ctx.Value(groupWaitKey{}).(string); ok {
		l.mu.Lock()
		defer l.mu.Unlock()
		l.running--
		l.waited[group] = true
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

// completeTxAfterCancel runs a worker whose batch handler completes the task
// "late" with CompleteTx, in a transaction of its own, once proceed is
// closed, and sends CompleteTx's error to result. A cancel does not end a
// batch handler's context, so the task completes after its cancel, and
// CompleteTx takes back the dedupe key that the cancel freed.
func completeTxAfterCancel(t *testing.T, e *env) (started chan int64, proceed chan struct{}, result chan error) {
	t.Helper()
	started, proceed, result = make(chan int64, 1), make(chan struct{}), make(chan error, 1)
	var hs taskmill.Handlers
	taskmill.AddBatchHandler(&hs, taskmill.BatchHandlerFunc[sendEmail](func(ctx context.Context, tasks []*taskmill.Task[sendEmail]) []error {
		errs := make([]error, len(tasks))
		for i, task := range tasks {
			if task.Args.To != "late" || task.Attempt > 1 {
				continue
			}
			started <- task.ID
			<-proceed
			errs[i] = pgx.BeginFunc(ctx, e.pool, func(tx pgx.Tx) error {
				return task.CompleteTx(ctx, taskmill.Pgx(tx))
			})
			result <- errs[i]
		}
		return errs
	}), &taskmill.HandlerOptions{BatchSize: 1})
	runWorker(t, e, &taskmill.WorkerConfig{Handlers: &hs, Queues: defaultQueue(1)})
	return started, proceed, result
}

// enqueueLate enqueues the task that completeTxAfterCancel's handler
// completes, with the dedupe key "k" in the group "g".
func (e *env) enqueueLate(t *testing.T) taskmill.EnqueueResult {
	t.Helper()
	res, err := e.client.Enqueue(t.Context(), sendEmail{To: "late"}, &taskmill.EnqueueOptions{DedupeKey: "k", GroupKey: "g"})
	if err != nil {
		t.Fatal(err)
	}
	return res
}

// CompleteTx takes back the key that a cancel freed before it takes its
// group's row, the order of an enqueue and of Retry. While it waits for a
// transaction that claims the key, it holds no group, so that transaction
// can continue into the task's group. In the other order, the two would
// wait for each other, and PostgreSQL would end one of them.
func TestCompleteTxTakesItsKeyBeforeItsGroup(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ctx := t.Context()
	started, proceed, result := completeTxAfterCancel(t, e)

	x := e.enqueueLate(t)
	recv(t, started, "the task to start")
	e.cancel(t, x.ID) // frees k while the task runs on

	tx := e.begin(t)
	if res, err := e.client.EnqueueTx(ctx, taskmill.Pgx(tx), sendEmail{}, &taskmill.EnqueueOptions{DedupeKey: "k"}); err != nil || res.Duplicate {
		t.Fatalf("claim of the freed key: %+v, %v", res, err)
	}
	close(proceed)
	e.waitBlocked(t, "INSERT INTO {s}.dedupe_keys")

	if _, err := e.client.EnqueueTx(ctx, taskmill.Pgx(tx), sendEmail{}, &taskmill.EnqueueOptions{GroupKey: "g"}); err != nil {
		t.Fatalf("enqueue into the group while CompleteTx waits for the key: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := recv(t, result, "CompleteTx"); err != nil {
		t.Fatalf("CompleteTx: %v", err)
	}
	e.waitState(t, x.ID, taskmill.StateCompleted)
	e.checkGroups(t)
}

// The replacement of a canceled task, enqueued with the task's key and group
// in one statement, takes the key, then the group. CompleteTx of the
// canceled task takes them in the same order, so while a third transaction
// holds the group, the replacement waits for CompleteTx, and finds the task
// that CompleteTx gave the key back to.
func TestCompleteTxAndAnEnqueueOfItsKeyAndGroup(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ctx := t.Context()
	started, proceed, result := completeTxAfterCancel(t, e)

	x := e.enqueueLate(t)
	recv(t, started, "the task to start")
	e.cancel(t, x.ID)

	holder := e.begin(t) // another producer's open transaction in the group
	if _, err := e.client.EnqueueTx(ctx, taskmill.Pgx(holder), sendEmail{}, &taskmill.EnqueueOptions{GroupKey: "g"}); err != nil {
		t.Fatal(err)
	}
	close(proceed)
	e.waitBlocked(t, "SET tasks = greatest(g.tasks + u.n, 0)") // CompleteTx holds k, and waits for g

	replaced := make(chan taskmill.EnqueueResult, 1)
	replace := make(chan error, 1)
	go func() {
		res, err := e.client.Enqueue(ctx, sendEmail{To: "replacement"}, &taskmill.EnqueueOptions{DedupeKey: "k", GroupKey: "g"})
		replaced <- res
		replace <- err
	}()
	e.waitBlockedCount(t, 2) // the replacement waits for k
	if err := holder.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	if err := recv(t, result, "CompleteTx"); err != nil {
		t.Fatalf("CompleteTx: %v", err)
	}
	if err := recv(t, replace, "the replacement's enqueue"); err != nil {
		t.Fatalf("replacement: %v", err)
	}
	if res := recv(t, replaced, "the replacement's result"); !res.Duplicate || res.ID != x.ID {
		t.Fatalf("replacement: got %+v, want a duplicate of task %d, which took its key back", res, x.ID)
	}
	e.checkGroups(t)
}

// CompleteTx leaves alone a key that another task holds: a claim of it
// would lock the key's row, and a CancelTx of that task, while it waits
// pending in the group, takes the group's row and then the key's. The two
// would wait for each other.
func TestCompleteTxSkipsAKeyThatAnotherTaskHolds(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ctx := t.Context()
	started, proceed, result := completeTxAfterCancel(t, e)

	x := e.enqueueLate(t)
	recv(t, started, "the task to start")
	e.cancel(t, x.ID)
	z := e.keyed(t, &taskmill.EnqueueOptions{DedupeKey: "k", GroupKey: "g"})
	if z.Duplicate {
		t.Fatalf("the replacement is a duplicate of task %d", z.ID)
	}
	e.wantState(t, z.ID, taskmill.StatePending) // behind x, with k

	holder := e.begin(t)
	if _, err := e.client.EnqueueTx(ctx, taskmill.Pgx(holder), sendEmail{}, &taskmill.EnqueueOptions{GroupKey: "g"}); err != nil {
		t.Fatal(err)
	}
	canceler := e.begin(t)
	canceled := make(chan error, 1)
	go func() { canceled <- e.client.CancelTx(ctx, taskmill.Pgx(canceler), z.ID) }()
	e.waitBlockedCount(t, 1) // CancelTx waits for g
	close(proceed)
	e.waitBlockedCount(t, 2) // CompleteTx waits for g too
	if err := holder.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	if err := recv(t, canceled, "CancelTx"); err != nil {
		t.Fatalf("CancelTx: %v", err)
	}
	if err := canceler.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := recv(t, result, "CompleteTx"); err != nil {
		t.Fatalf("CompleteTx: %v", err)
	}
	e.waitState(t, x.ID, taskmill.StateCompleted)
	e.wantState(t, z.ID, taskmill.StateCanceled)
	e.checkGroups(t)
}
