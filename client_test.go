package taskmill_test

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/code19m/taskmill"
	"github.com/code19m/taskmill/internal/testdb"
)

func TestGetUnknownTask(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	if _, err := e.client.Get(t.Context(), 424242); !errors.Is(err, taskmill.ErrNotFound) {
		t.Fatalf("got %v, want ErrNotFound", err)
	}
}

func TestCancelWaitingTask(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	id := e.enqueue(t, sendEmail{}, nil)

	if err := e.client.Cancel(t.Context(), id); err != nil {
		t.Fatal(err)
	}
	info := e.get(t, id)
	if info.State != taskmill.StateCanceled || info.FinalizedAt.IsZero() {
		t.Fatalf("got %s, finalized at %v", info.State, info.FinalizedAt)
	}

	if err := e.client.Cancel(t.Context(), id); !errors.Is(err, taskmill.ErrInvalidState) {
		t.Fatalf("second cancel: got %v, want ErrInvalidState", err)
	}
	if err := e.client.Cancel(t.Context(), 424242); !errors.Is(err, taskmill.ErrNotFound) {
		t.Fatalf("unknown task: got %v, want ErrNotFound", err)
	}
}

func TestCancelRunningTaskFlagsIt(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	id := e.enqueue(t, sendEmail{}, nil)
	e.exec(t, `UPDATE {s}.tasks SET state = 'running', attempt = 1, lease_token = 1,
	           lease_expires_at = now() + interval '30 seconds', attempted_at = now(), attempted_by = 'w'
	           WHERE id = $1`, id)

	if err := e.client.Cancel(t.Context(), id); err != nil {
		t.Fatal(err)
	}
	e.wantState(t, id, taskmill.StateRunning)
	if n := e.count(t, "SELECT count(*) FROM {s}.tasks WHERE id = $1 AND cancel_requested_at IS NOT NULL", id); n != 1 {
		t.Fatal("running task was not flagged for cancellation")
	}
}

// cancelBatch is a batch handler that waits until released, and then fails
// the tasks of user 1.
type cancelBatch struct {
	started chan int
	release chan struct{}
}

func (b *cancelBatch) HandleBatch(_ context.Context, tasks []*taskmill.Task[sendEmail]) []error {
	b.started <- len(tasks)
	<-b.release
	errs := make([]error, len(tasks))
	for i, t := range tasks {
		if t.Args.UserID == 1 {
			errs[i] = errors.New("smtp refused")
		}
	}
	return errs
}

// Canceling a task of a batch does not stop the batch, which cannot see the
// request: the task ends as the batch reports it, completed if the batch did
// its work, and canceled if not.
func TestCancelingATaskOfABatch(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	b := &cancelBatch{started: make(chan int, 1), release: make(chan struct{})}
	var hs taskmill.Handlers
	taskmill.AddBatchHandler[sendEmail](&hs, b, &taskmill.HandlerOptions{Retry: fastRetry})
	res, err := e.client.EnqueueMany(t.Context(), []taskmill.EnqueueParams{{Args: sendEmail{UserID: 0}}, {Args: sendEmail{UserID: 1}}})
	if err != nil {
		t.Fatal(err)
	}
	// With two free slots, the first fetch takes both tasks into one batch.
	runWorker(t, e, &taskmill.WorkerConfig{Handlers: &hs, Queues: defaultQueue(2)})
	if n := recv(t, b.started, "the batch to start"); n != 2 {
		t.Fatalf("the batch has %d tasks, want 2", n)
	}
	for _, r := range res {
		if err := e.client.Cancel(t.Context(), r.ID); err != nil {
			t.Fatal(err)
		}
	}
	close(b.release)

	e.waitState(t, res[0].ID, taskmill.StateCompleted)
	e.waitState(t, res[1].ID, taskmill.StateCanceled)
}

func TestCancelTxNeedsATransaction(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	id := e.enqueue(t, sendEmail{}, nil)

	if err := e.client.CancelTx(t.Context(), taskmill.Pgx(e.pool), id); !errors.Is(err, taskmill.ErrInvalidArgument) {
		t.Fatalf("pool instead of transaction: got %v, want ErrInvalidArgument", err)
	}
	if err := e.client.CancelTx(t.Context(), taskmill.Pgx(nil), id); !errors.Is(err, taskmill.ErrInvalidArgument) {
		t.Fatalf("nil executor: got %v, want ErrInvalidArgument", err)
	}

	err := pgx.BeginFunc(t.Context(), e.pool, func(tx pgx.Tx) error {
		return e.client.CancelTx(t.Context(), taskmill.Pgx(tx), id)
	})
	if err != nil {
		t.Fatal(err)
	}
	e.wantState(t, id, taskmill.StateCanceled)
}

func TestCancelingAGroupHeadPromotesTheNextTask(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	group := &taskmill.EnqueueOptions{GroupKey: "g"}
	head := e.enqueue(t, sendEmail{To: "1"}, group)
	second := e.enqueue(t, sendEmail{To: "2"}, group)
	third := e.enqueue(t, sendEmail{To: "3"}, group)

	// Canceling a pending task only shrinks the group.
	if err := e.client.Cancel(t.Context(), third); err != nil {
		t.Fatal(err)
	}
	e.wantState(t, second, taskmill.StatePending)

	// Canceling the head promotes the next task.
	if err := e.client.Cancel(t.Context(), head); err != nil {
		t.Fatal(err)
	}
	e.wantState(t, second, taskmill.StateAvailable)

	// Canceling the last task removes the group.
	if err := e.client.Cancel(t.Context(), second); err != nil {
		t.Fatal(err)
	}
	if n := e.count(t, "SELECT count(*) FROM {s}.groups"); n != 0 {
		t.Fatalf("empty group was not removed")
	}
}

func TestRetryDeadTask(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	id := e.enqueue(t, sendEmail{}, &taskmill.EnqueueOptions{MaxAttempts: 2})
	e.exec(t, `UPDATE {s}.tasks SET state = 'dead', attempt = 2, finalized_at = now(),
	           errors = '[{"attempt": 2, "at": "2026-01-01T00:00:00Z", "message": "boom"}]' WHERE id = $1`, id)

	if err := e.client.Retry(t.Context(), id); err != nil {
		t.Fatal(err)
	}
	info := e.get(t, id)
	if info.State != taskmill.StateAvailable || info.Attempt != 0 || !info.FinalizedAt.IsZero() {
		t.Fatalf("got %s, attempt %d, finalized %v", info.State, info.Attempt, info.FinalizedAt)
	}
	if len(info.Errors) != 1 || info.Errors[0].Message != "boom" {
		t.Fatalf("error history lost: %+v", info.Errors)
	}

	if err := e.client.Retry(t.Context(), id); !errors.Is(err, taskmill.ErrInvalidState) {
		t.Fatalf("retrying an available task: got %v, want ErrInvalidState", err)
	}
}

func TestRetryGroupedTaskWaitsBehindTheGroup(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	group := &taskmill.EnqueueOptions{GroupKey: "g"}
	first := e.enqueue(t, sendEmail{To: "1"}, group)
	if err := e.client.Cancel(t.Context(), first); err != nil {
		t.Fatal(err)
	}
	second := e.enqueue(t, sendEmail{To: "2"}, group)
	e.wantState(t, second, taskmill.StateAvailable)

	if err := e.client.Retry(t.Context(), first); err != nil {
		t.Fatal(err)
	}
	e.wantState(t, first, taskmill.StatePending)
	if n := e.count(t, "SELECT tasks FROM {s}.groups WHERE group_key = 'g'"); n != 2 {
		t.Fatalf("group count: got %d, want 2", n)
	}
}

func TestRetryResumesAHaltedGroup(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	group := &taskmill.EnqueueOptions{GroupKey: "ledger"}
	head := e.enqueue(t, sendEmail{To: "1"}, group)
	next := e.enqueue(t, sendEmail{To: "2"}, group)

	// The head dies and halts the group, staying counted.
	e.exec(t, `UPDATE {s}.tasks SET state = 'dead', finalized_at = now() WHERE id = $1`, head)
	e.exec(t, `UPDATE {s}.groups SET halted_task_id = $1, halted_at = now() WHERE group_key = 'ledger'`, head)

	if err := e.client.Retry(t.Context(), head); err != nil {
		t.Fatal(err)
	}
	e.wantState(t, head, taskmill.StateAvailable)
	e.wantState(t, next, taskmill.StatePending)
	if n := e.count(t, "SELECT count(*) FROM {s}.groups WHERE halted_at IS NULL AND tasks = 2"); n != 1 {
		t.Fatal("group still halted or miscounted")
	}
}

func TestDelete(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	group := &taskmill.EnqueueOptions{GroupKey: "g"}
	head := e.enqueue(t, sendEmail{To: "1"}, group)
	next := e.enqueue(t, sendEmail{To: "2"}, group)

	if err := e.client.Delete(t.Context(), head); err != nil {
		t.Fatal(err)
	}
	if _, err := e.client.Get(t.Context(), head); !errors.Is(err, taskmill.ErrNotFound) {
		t.Fatalf("deleted task: got %v, want ErrNotFound", err)
	}
	e.wantState(t, next, taskmill.StateAvailable)

	e.exec(t, `UPDATE {s}.tasks SET state = 'running', attempt = 1, lease_token = 1,
	           lease_expires_at = now() + interval '30 seconds' WHERE id = $1`, next)
	if err := e.client.Delete(t.Context(), next); !errors.Is(err, taskmill.ErrInvalidState) {
		t.Fatalf("deleting a running task: got %v, want ErrInvalidState", err)
	}
	if err := e.client.Delete(t.Context(), 424242); !errors.Is(err, taskmill.ErrNotFound) {
		t.Fatalf("unknown task: got %v, want ErrNotFound", err)
	}
}

func TestCancelAndDeleteFreeTheDedupeKey(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ctx := t.Context()
	opts := &taskmill.EnqueueOptions{DedupeKey: "order-42"}
	enqueue := func() taskmill.EnqueueResult {
		t.Helper()
		res, err := e.client.Enqueue(ctx, sendEmail{}, opts)
		if err != nil {
			t.Fatal(err)
		}
		return res
	}

	first := enqueue()
	if dup := enqueue(); !dup.Duplicate || dup.ID != first.ID {
		t.Fatalf("got %+v, want a duplicate of %d", dup, first.ID)
	}
	if err := e.client.Cancel(ctx, first.ID); err != nil {
		t.Fatal(err)
	}
	second := enqueue()
	if second.Duplicate {
		t.Fatalf("the canceled task still holds its key: %+v", second)
	}
	if err := e.client.Delete(ctx, second.ID); err != nil {
		t.Fatal(err)
	}
	if third := enqueue(); third.Duplicate {
		t.Fatalf("the deleted task still holds its key: %+v", third)
	}
}

func TestRetryAndDeleteSeeTheLatestGroupHalt(t *testing.T) {
	t.Parallel()
	for _, op := range []string{"retry", "delete"} {
		t.Run(op, func(t *testing.T) {
			t.Parallel()
			e := newEnv(t)
			group := &taskmill.EnqueueOptions{GroupKey: "g"}
			head := e.enqueue(t, sendEmail{To: "1"}, group)
			next := e.enqueue(t, sendEmail{To: "2"}, group)
			e.exec(t, `UPDATE {s}.tasks SET state = 'dead', finalized_at = now() WHERE id = $1`, head)
			e.exec(t, `UPDATE {s}.groups SET halted_task_id = $1, halted_at = now() WHERE group_key = 'g'`, head)

			// While the operation waits for the head's lock, another transaction
			// ends the halt: the head leaves its group and the next task takes
			// over.
			tx := e.begin(t)
			e.txExec(t, tx, "SELECT FROM {s}.tasks WHERE id = $1 FOR UPDATE", head)
			e.txExec(t, tx, `UPDATE {s}.groups SET halted_task_id = NULL, halted_at = NULL, tasks = tasks - 1
			                 WHERE group_key = 'g'`)
			e.txExec(t, tx, "UPDATE {s}.tasks SET state = 'available' WHERE id = $1", next)

			done := make(chan error, 1)
			go func() {
				if op == "retry" {
					done <- e.client.Retry(t.Context(), head)
				} else {
					done <- e.client.Delete(t.Context(), head)
				}
			}()
			e.waitBlocked(t, "{s}.tasks")
			if err := tx.Commit(t.Context()); err != nil {
				t.Fatal(err)
			}
			if err := <-done; err != nil {
				t.Fatal(err)
			}

			want := 1 // the next task
			if op == "retry" {
				want = 2 // the head rejoins, behind the next task
				e.wantState(t, head, taskmill.StatePending)
			}
			if n := e.count(t, "SELECT coalesce(sum(tasks), 0) FROM {s}.groups"); n != want {
				t.Fatalf("group counts %d tasks, want %d", n, want)
			}
		})
	}
}

// waitBlockedCount waits until n statements on the env's schema wait for
// locks.
func (e *env) waitBlockedCount(t *testing.T, n int) {
	t.Helper()
	waitFor(t, "statements to wait for locks", func() bool {
		return e.count(t, `SELECT count(*) FROM pg_stat_activity
			WHERE wait_event_type = 'Lock' AND strpos(query, $1) > 0`, e.schema) == n
	})
}

// removals are the actions that take a task out of its FIFO group, each with
// what a Cancel of the same task finds after it.
var removals = []struct {
	name  string
	act   func(c *taskmill.Client, ctx context.Context, id int64) error
	after error
}{
	{"cancel", (*taskmill.Client).Cancel, taskmill.ErrInvalidState},
	{"delete", (*taskmill.Client).Delete, taskmill.ErrNotFound},
}

// endWithASecondCancel goes on from an action on the pending task id that
// got the task's row late, and now waits for the task's dedupe key, which
// keyTx holds. A second operator cancels the task, which takes the group's
// row first: if the action holds the task's row alone, the two wait for each
// other once the key is free. Both must end: the action with no error, and
// the second cancel with after.
func (e *env) endWithASecondCancel(t *testing.T, id int64, keyTx pgx.Tx, acted <-chan error, after error) {
	t.Helper()
	e.waitBlocked(t, "DELETE FROM {s}.dedupe_keys")
	again := make(chan error, 1)
	go func() { again <- e.client.Cancel(t.Context(), id) }()
	e.waitBlockedCount(t, 2)

	if err := keyTx.Commit(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := recv(t, acted, "the action"); err != nil {
		t.Fatalf("the action: %v", err)
	}
	if err := recv(t, again, "the second cancel"); !errors.Is(err, after) {
		t.Fatalf("the second cancel: got %v, want %v", err, after)
	}
}

// A Retry makes a canceled task pending while a Cancel or a Delete of the
// same task waits for the task's row. The waiting action read the task as
// canceled, so it did not lock the group's row first, as it must for a
// pending task. If it goes on with the task's row alone, a second Cancel,
// which takes the group's row first, closes a cycle with it.
func TestCancelAndDeleteOfATaskRetriedMeanwhileDoNotDeadlock(t *testing.T) {
	t.Parallel()
	for _, tt := range removals {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			e := newEnv(t)
			ctx := t.Context()
			group := &taskmill.EnqueueOptions{GroupKey: "g"}
			e.enqueue(t, sendEmail{To: "head"}, group)
			id := e.enqueue(t, sendEmail{To: "next"}, group)
			if err := e.client.Cancel(ctx, id); err != nil {
				t.Fatal(err)
			}
			// A dedupe key of the task, whose lock holds the action later.
			e.exec(t, "INSERT INTO {s}.dedupe_keys (key, task_id, expires_at) VALUES (sha256('k'), $1, now() + interval '1 hour')", id)

			// The Retry locks the task, and waits for the group's row.
			groupTx := e.begin(t)
			e.txExec(t, groupTx, "SELECT FROM {s}.groups FOR UPDATE")
			retried := make(chan error, 1)
			go func() { retried <- e.client.Retry(ctx, id) }()
			e.waitBlocked(t, "INSERT INTO {s}.groups")
			// The action reads the task as canceled, and waits for its row.
			keyTx := e.begin(t)
			e.txExec(t, keyTx, "SELECT FROM {s}.dedupe_keys FOR UPDATE")
			acted := make(chan error, 1)
			go func() { acted <- tt.act(e.client, ctx, id) }()
			e.waitBlocked(t, "FROM {s}.tasks WHERE id = $1")

			// The Retry ends: the task is pending. The action gets the task's
			// row, and stops at the dedupe key.
			if err := groupTx.Commit(ctx); err != nil {
				t.Fatal(err)
			}
			if err := recv(t, retried, "the retry"); err != nil {
				t.Fatalf("retry: %v", err)
			}
			e.endWithASecondCancel(t, id, keyTx, acted, tt.after)
		})
	}
}

// An enqueue commits a pending task while a Cancel or a Delete of the same
// ID is between two of its statements: the first did not see the task, and
// the next locks the task's row. If the action took a task that it did not
// see for one without a FIFO group, it now holds a pending task's row
// without the group's row.
func TestCancelAndDeleteOfATaskEnqueuedMeanwhileDoNotDeadlock(t *testing.T) {
	t.Parallel()
	for _, tt := range removals {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			e := newEnv(t)
			ctx := t.Context()
			opts := &taskmill.EnqueueOptions{GroupKey: "g", DedupeKey: "k"}
			e.enqueue(t, sendEmail{To: "head"}, &taskmill.EnqueueOptions{GroupKey: "g"})
			// The task is enqueued behind the head, in a transaction that
			// stays open.
			enqueueTx := e.begin(t)
			res, err := e.client.EnqueueTx(ctx, taskmill.Pgx(enqueueTx), sendEmail{To: "next"}, opts)
			if err != nil {
				t.Fatal(err)
			}
			// A lock on the table waits for that transaction. Until it is
			// granted and released, whoever comes to lock a row of the table
			// waits behind it, but a plain read goes through.
			tableTx := e.begin(t)
			tableLocked := make(chan error, 1)
			go func() {
				_, err := tableTx.Exec(ctx, replaceSchema("LOCK TABLE {s}.tasks IN EXCLUSIVE MODE", e.schema))
				tableLocked <- err
			}()
			e.waitBlocked(t, "LOCK TABLE {s}.tasks")
			// The action reads no such task, and waits to lock its row.
			acted := make(chan error, 1)
			go func() { acted <- tt.act(e.client, ctx, res.ID) }()
			e.waitBlocked(t, "FROM {s}.tasks WHERE id = $1")

			// The enqueue commits: the task is there, and pending. The
			// action gets its row once the table is free, and stops at the
			// dedupe key.
			if err := enqueueTx.Commit(ctx); err != nil {
				t.Fatal(err)
			}
			if err := recv(t, tableLocked, "the lock on the table"); err != nil {
				t.Fatal(err)
			}
			keyTx := e.begin(t)
			e.txExec(t, keyTx, "SELECT FROM {s}.dedupe_keys FOR UPDATE")
			if err := tableTx.Rollback(ctx); err != nil {
				t.Fatal(err)
			}
			e.endWithASecondCancel(t, res.ID, keyTx, acted, tt.after)
		})
	}
}

// A Delete reads a task as pending and waits for the group's row, which a
// Cancel of the same task holds. Once the Cancel ends, the task is no longer
// pending, and a Retry locks it and waits for the group's row. If the Delete
// then waits for the task's row with the group's row in its hands, the two
// wait for each other.
func TestDeleteOfATaskCanceledMeanwhileDoesNotDeadlockWithItsRetry(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ctx := t.Context()
	e.enqueue(t, sendEmail{To: "head"}, &taskmill.EnqueueOptions{GroupKey: "g"})
	id := e.enqueue(t, sendEmail{To: "next"}, &taskmill.EnqueueOptions{GroupKey: "g", DedupeKey: "k"})

	// The Cancel holds the group's row and the task's, and stops at the
	// task's dedupe key.
	keyTx := e.begin(t)
	e.txExec(t, keyTx, "SELECT FROM {s}.dedupe_keys FOR UPDATE")
	canceled := make(chan error, 1)
	go func() { canceled <- e.client.Cancel(ctx, id) }()
	e.waitBlocked(t, "DELETE FROM {s}.dedupe_keys")
	// The Delete waits for the group's row, and the Retry for the task's.
	deleted := make(chan error, 1)
	go func() { deleted <- e.client.Delete(ctx, id) }()
	e.waitBlockedCount(t, 2)
	retried := make(chan error, 1)
	go func() { retried <- e.client.Retry(ctx, id) }()
	e.waitBlockedCount(t, 3)

	if err := keyTx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	for what, done := range map[string]chan error{"cancel": canceled, "retry": retried, "delete": deleted} {
		if err := recv(t, done, "the "+what); err != nil {
			t.Fatalf("%s: %v", what, err)
		}
	}
	if _, err := e.client.Get(ctx, id); !errors.Is(err, taskmill.ErrNotFound) {
		t.Fatalf("the deleted task: got %v, want ErrNotFound", err)
	}
}

func TestOperationsOnCompletedTasks(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	stop := runWorker(t, e, &taskmill.WorkerConfig{Handlers: noopHandlers(), Queues: defaultQueue(1)})
	id := e.enqueue(t, sendEmail{}, nil)
	e.waitState(t, id, taskmill.StateCompleted)
	stop()

	if err := e.client.Cancel(t.Context(), id); !errors.Is(err, taskmill.ErrInvalidState) {
		t.Fatalf("cancel: got %v, want ErrInvalidState", err)
	}
	if err := e.client.Retry(t.Context(), id); !errors.Is(err, taskmill.ErrInvalidState) {
		t.Fatalf("retry: got %v, want ErrInvalidState", err)
	}
	if err := e.client.Delete(t.Context(), id); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, err := e.client.Get(t.Context(), id); !errors.Is(err, taskmill.ErrNotFound) {
		t.Fatalf("deleted task: got %v, want ErrNotFound", err)
	}
}

func TestTxMethodsWithDatabaseSQL(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	db := openSQL(t)
	ctx := t.Context()
	group := &taskmill.EnqueueOptions{GroupKey: "g"}
	head := e.enqueue(t, sendEmail{To: "1"}, group)
	next := e.enqueue(t, sendEmail{To: "2"}, group)
	inTx := func(fn func(tx *sql.Tx) error) {
		t.Helper()
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := fn(tx); err != nil {
			_ = tx.Rollback()
			t.Fatal(err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
	}

	// Each of these runs several statements, which must commit together.
	inTx(func(tx *sql.Tx) error { return e.client.CancelTx(ctx, taskmill.SQL(tx), head) })
	e.wantState(t, head, taskmill.StateCanceled)
	e.wantState(t, next, taskmill.StateAvailable)

	if err := e.client.Retry(ctx, head); err != nil {
		t.Fatal(err)
	}
	e.wantState(t, head, taskmill.StatePending)

	inTx(func(tx *sql.Tx) error { return e.client.CancelTx(ctx, taskmill.SQL(tx), next) })
	e.wantState(t, head, taskmill.StateAvailable)

	// A pending task's group is locked before the task, under a savepoint.
	last := e.enqueue(t, sendEmail{To: "3"}, group)
	inTx(func(tx *sql.Tx) error { return e.client.CancelTx(ctx, taskmill.SQL(tx), last) })
	e.wantState(t, last, taskmill.StateCanceled)

	if err := e.client.CancelTx(ctx, taskmill.SQL(db), head); !errors.Is(err, taskmill.ErrInvalidArgument) {
		t.Fatalf("a database instead of a transaction: got %v, want ErrInvalidArgument", err)
	}
}

// wrappedTx is a transaction type that embeds *sql.Tx, as sqlx's does.
type wrappedTx struct {
	*sql.Tx
}

func TestTxMethodsAcceptWrappedSQLTransactions(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	db := openSQL(t)
	id := e.enqueue(t, sendEmail{}, &taskmill.EnqueueOptions{GroupKey: "g"})
	tx, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := e.client.CancelTx(t.Context(), taskmill.SQL(wrappedTx{tx}), id); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	e.wantState(t, id, taskmill.StateCanceled)
}

func TestCancelWhileTheWorkerRecordsAResult(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	id := e.enqueue(t, sendEmail{}, nil)
	e.exec(t, `UPDATE {s}.tasks SET state = 'running', attempt = 1, lease_token = 1,
	           lease_expires_at = now() + interval '30 seconds', attempted_at = now(), attempted_by = 'w'
	           WHERE id = $1`, id)

	// The worker records a failure while Cancel waits for the task's lock.
	tx := e.begin(t)
	e.txExec(t, tx, `UPDATE {s}.tasks SET state = 'retryable', lease_expires_at = NULL,
	                 run_at = now() + interval '1 minute' WHERE id = $1`, id)
	done := make(chan error, 1)
	go func() { done <- e.client.Cancel(t.Context(), id) }()
	e.waitBlocked(t, "FROM {s}.tasks WHERE id = $1 AND")
	if err := tx.Commit(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatalf("cancel: %v", err)
	}
	e.wantState(t, id, taskmill.StateCanceled)
}

func TestNewClientRequiresReadCommitted(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	cfg, err := pgxpool.ParseConfig(testdb.DirectURL(t))
	if err != nil {
		t.Fatal(err)
	}
	cfg.MaxConns = 1
	cfg.ConnConfig.RuntimeParams["default_transaction_isolation"] = "serializable"
	pool, err := pgxpool.NewWithConfig(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	_, err = taskmill.NewClient(t.Context(), pool, &taskmill.ClientConfig{Schema: e.schema})
	if !errors.Is(err, taskmill.ErrInvalidArgument) || !strings.Contains(err.Error(), "read committed") {
		t.Fatalf("got %v, want ErrInvalidArgument about the isolation level", err)
	}
}
