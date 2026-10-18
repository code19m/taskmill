package taskmill_test

import (
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/code19m/taskmill"
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

func TestCancelTxNeedsATransaction(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	id := e.enqueue(t, sendEmail{}, nil)

	if err := e.client.CancelTx(t.Context(), taskmill.Pgx(e.pool), id); !errors.Is(err, taskmill.ErrInvalidArgument) {
		t.Fatalf("pool instead of transaction: got %v, want ErrInvalidArgument", err)
	}
	if err := e.client.CancelTx(t.Context(), taskmill.PgxExecutor{}, id); !errors.Is(err, taskmill.ErrInvalidArgument) {
		t.Fatalf("zero executor: got %v, want ErrInvalidArgument", err)
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
	e.exec(t, "INSERT INTO {s}.queues (name, halt_group_on_dead) VALUES ('default', true)")
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
