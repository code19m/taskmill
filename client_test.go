package taskmill_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"maps"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
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

// keyed enqueues a task with a dedupe key, and returns the result.
func (e *env) keyed(t *testing.T, opts *taskmill.EnqueueOptions) taskmill.EnqueueResult {
	t.Helper()
	res, err := e.client.Enqueue(t.Context(), sendEmail{}, opts)
	if err != nil {
		t.Fatal(err)
	}
	return res
}

// keyCancels are the ways to cancel a task, which all free its dedupe key.
var keyCancels = map[string]func(t *testing.T, e *env, id int64){
	"Cancel": func(t *testing.T, e *env, id int64) {
		t.Helper()
		e.cancel(t, id)
	},
	"CancelTx": func(t *testing.T, e *env, id int64) {
		t.Helper()
		tx := e.begin(t)
		if err := e.client.CancelTx(t.Context(), taskmill.Pgx(tx), id); err != nil {
			t.Fatal(err)
		}
		if err := tx.Commit(t.Context()); err != nil {
			t.Fatal(err)
		}
	},
	"CancelMatching": func(t *testing.T, e *env, _ int64) {
		t.Helper()
		if _, err := e.client.CancelMatching(t.Context(), bulk(&taskmill.TaskFilter{Kinds: []string{"email.send"}})); err != nil {
			t.Fatal(err)
		}
	},
	// A running task gives its key away when it is asked to stop, and
	// becomes canceled when its attempt ends.
	"Cancel while it runs": func(t *testing.T, e *env, id int64) {
		t.Helper()
		e.lease(t, id)
		e.cancel(t, id)
		e.exec(t, `UPDATE {s}.tasks SET state = 'canceled', finalized_at = now(), lease_expires_at = NULL WHERE id = $1`, id)
	},
}

// keyRetries are the ways to retry a canceled task, which all take its
// dedupe key back.
var keyRetries = map[string]func(t *testing.T, e *env, id int64){
	"Retry": func(t *testing.T, e *env, id int64) {
		t.Helper()
		if err := e.client.Retry(t.Context(), id); err != nil {
			t.Fatal(err)
		}
	},
	"RetryMatching": func(t *testing.T, e *env, _ int64) {
		t.Helper()
		canceled := &taskmill.TaskFilter{States: []taskmill.State{taskmill.StateCanceled}}
		if res, err := e.client.RetryMatching(t.Context(), bulk(canceled)); err != nil || res.Changed != 1 || res.Conflicts != 0 {
			t.Fatalf("retried %+v, %v; want 1 task", res, err)
		}
	},
}

// A cancel frees the task's dedupe key, and a retry takes it back, however
// the task was canceled and retried: one key never stands for two unfinished
// tasks while its window lasts.
func TestRetryTakesTheDedupeKeyBack(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	for cancelName, cancel := range keyCancels {
		for retryName, retry := range keyRetries {
			cancelThenRetry(t, e, cancelName+" then "+retryName, cancel, retry)
		}
	}
}

// cancelThenRetry cancels a task with a dedupe key and retries it. The key
// must be free in between, and the task's again afterwards. The task shows
// nothing of the key, canceled or retried: its metadata is its own. In the
// end it deletes the task, and so leaves e as it found it.
func cancelThenRetry(t *testing.T, e *env, name string, cancel, retry func(t *testing.T, e *env, id int64)) {
	t.Helper()
	opts := &taskmill.EnqueueOptions{DedupeKey: "order-42", Metadata: map[string]string{"tenant": "7"}}
	first := e.keyed(t, opts)
	cancel(t, e, first.ID)
	other := e.keyed(t, opts)
	if other.Duplicate {
		t.Fatalf("%s: the canceled task still holds its key: %+v", name, other)
	}
	if got := e.get(t, first.ID).Metadata; !maps.Equal(got, opts.Metadata) {
		t.Fatalf("%s: metadata of the canceled task: got %v, want %v", name, got, opts.Metadata)
	}
	if err := e.client.Delete(t.Context(), other.ID); err != nil {
		t.Fatal(err)
	}

	retry(t, e, first.ID)
	e.wantState(t, first.ID, taskmill.StateAvailable)
	if dup := e.keyed(t, opts); !dup.Duplicate || dup.ID != first.ID {
		t.Fatalf("%s: after the retry: got %+v, want a duplicate of task %d", name, dup, first.ID)
	}
	if got := e.get(t, first.ID).Metadata; !maps.Equal(got, opts.Metadata) {
		t.Fatalf("%s: metadata after the retry: got %v, want %v", name, got, opts.Metadata)
	}
	if err := e.client.Delete(t.Context(), first.ID); err != nil {
		t.Fatal(err)
	}
}

// wantRefused retries a canceled task whose dedupe key another task holds:
// the retry must fail with ErrConflict, name the holder, and change nothing.
func (e *env) wantRefused(t *testing.T, what string, id, holder int64) {
	t.Helper()
	err := e.client.Retry(t.Context(), id)
	if !errors.Is(err, taskmill.ErrConflict) || !strings.Contains(err.Error(), fmt.Sprintf("task %d holds", holder)) {
		t.Fatalf("%s: got %v, want ErrConflict that names task %d", what, err, holder)
	}
	e.wantState(t, id, taskmill.StateCanceled)
}

// While another task holds the key, a retry would make two tasks stand for
// the same work. It is refused with ErrConflict, and changes nothing.
func TestRetryIsRefusedWhileAnotherTaskHoldsTheDedupeKey(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ctx := t.Context()
	opts := &taskmill.EnqueueOptions{DedupeKey: "remind-42", GroupKey: "customer-7"}
	first := e.keyed(t, opts)
	// One transaction replaces the task, as the deduplication guide shows.
	tx := e.begin(t)
	if err := e.client.CancelTx(ctx, taskmill.Pgx(tx), first.ID); err != nil {
		t.Fatal(err)
	}
	second, err := e.client.EnqueueTx(ctx, taskmill.Pgx(tx), sendEmail{}, opts)
	if err != nil || second.Duplicate {
		t.Fatalf("enqueue after the cancel: %+v, %v; want a new task", second, err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	e.wantRefused(t, "while the new task waits", first.ID, second.ID)
	if dup := e.keyed(t, opts); !dup.Duplicate || dup.ID != second.ID {
		t.Fatalf("after the refused retry: got %+v, want a duplicate of task %d", dup, second.ID)
	}
	if n := e.count(t, "SELECT tasks FROM {s}.groups WHERE group_key = 'customer-7'"); n != 1 {
		t.Fatalf("the group counts %d tasks after the refused retry, want 1", n)
	}
	// A dead task keeps its key until the window ends, so it still holds it.
	e.die(t, second.ID)
	e.wantRefused(t, "while the new task is dead", first.ID, second.ID)

	// Once the holder gives the key away, the retry takes it, and the other
	// task is the one that cannot come back.
	if err := e.client.Retry(ctx, second.ID); err != nil {
		t.Fatal(err)
	}
	e.cancel(t, second.ID)
	if err := e.client.Retry(ctx, first.ID); err != nil {
		t.Fatalf("retry once the key is free: %v", err)
	}
	e.wantRefused(t, "the task that gave the key away", second.ID, first.ID)
	if dup := e.keyed(t, opts); !dup.Duplicate || dup.ID != first.ID {
		t.Fatalf("after the retry: got %+v, want a duplicate of task %d", dup, first.ID)
	}
}

// A key belongs to its task until its window ends, and a cancel does not
// change when that is. A task retried after its window takes no key: like any
// task that outlives its window, it can run beside a newer task of the key.
func TestRetryAfterTheWindowTakesNoKey(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	opts := &taskmill.EnqueueOptions{DedupeKey: "report-monday"}
	first := e.keyed(t, opts)
	e.cancel(t, first.ID)
	second := e.keyed(t, opts)
	if second.Duplicate {
		t.Fatalf("the canceled task still holds its key: %+v", second)
	}
	// The first task's window ends.
	e.exec(t, "UPDATE {s}.tasks SET freed_key_expires_at = now() - interval '1 second' WHERE id = $1", first.ID)

	if err := e.client.Retry(t.Context(), first.ID); err != nil {
		t.Fatalf("retry after the window: %v", err)
	}
	e.wantState(t, first.ID, taskmill.StateAvailable)
	if dup := e.keyed(t, opts); !dup.Duplicate || dup.ID != second.ID {
		t.Fatalf("after the retry: got %+v, want a duplicate of task %d, which holds the key", dup, second.ID)
	}
}

// A retry takes its key back before it takes its group's row, the order of
// an enqueue. While it waits for a transaction that claims the key, it holds
// no group, so that transaction can continue into the task's group. In the
// other order, the two would wait for each other: PostgreSQL would end the
// retry after a second, and the retry would run again. So the retry must
// have run once.
func TestRetryTakesItsKeyBeforeItsGroup(t *testing.T) {
	t.Parallel()
	sent := &statementLog{}
	e := newTracedEnv(t, sent)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	id := e.keyed(t, &taskmill.EnqueueOptions{DedupeKey: "k", GroupKey: "g"}).ID
	e.cancel(t, id)
	tx := e.begin(t)
	if _, err := e.client.EnqueueTx(ctx, taskmill.Pgx(tx), sendEmail{}, &taskmill.EnqueueOptions{DedupeKey: "k"}); err != nil {
		t.Fatal(err)
	}
	sent.reset()
	retried := make(chan error, 1)
	go func() { retried <- e.client.Retry(ctx, id) }()
	e.waitBlocked(t, "INSERT INTO {s}.dedupe_keys")
	if _, err := e.client.EnqueueTx(ctx, taskmill.Pgx(tx), sendEmail{}, &taskmill.EnqueueOptions{GroupKey: "g"}); err != nil {
		t.Fatalf("enqueue into the group while the retry waits for the key: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	// The transaction's task holds the key now.
	if err := recv(t, retried, "the retry"); !errors.Is(err, taskmill.ErrConflict) {
		t.Fatalf("retry: got %v, want ErrConflict", err)
	}
	e.wantState(t, id, taskmill.StateCanceled)
	if n := sent.count("(NOT $2 OR state = 'pending') FOR UPDATE"); n != 1 {
		t.Errorf("the retry locked its task %d times, want 1: it ran again, as after a deadlock", n)
	}
}

// What taskmill stores of a freed key is in columns that only a trusted
// role writes. A value there that is no key, as a write by hand may leave,
// counts as none: a retry, of one task or of many, never fails on it.
func TestRetryIgnoresAStoredKeyThatIsNotOne(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ctx := t.Context()
	var ids []int64
	for _, stored := range []string{
		`freed_key = '\x01', freed_key_expires_at = 'infinity'`,
		`freed_key = NULL, freed_key_expires_at = 'infinity'`,
		`freed_key = sha256('k'), freed_key_expires_at = NULL`,
		`freed_key = sha256('k'), freed_key_expires_at = '-infinity'`,
	} {
		id := e.enqueue(t, sendEmail{}, nil)
		e.cancel(t, id)
		e.exec(t, "UPDATE {s}.tasks SET "+stored+" WHERE id = $1", id)
		ids = append(ids, id)
	}
	if err := e.client.Retry(ctx, ids[0]); err != nil {
		t.Fatalf("retry: %v", err)
	}
	res, err := e.client.RetryMatching(ctx, bulk(&taskmill.TaskFilter{States: []taskmill.State{taskmill.StateCanceled}}))
	if err != nil || res.Changed != int64(len(ids)-1) || res.Conflicts != 0 {
		t.Fatalf("retried %+v, %v; want the other %d tasks", res, err, len(ids)-1)
	}
	if n := e.count(t, "SELECT count(*) FROM {s}.dedupe_keys"); n != 0 {
		t.Fatalf("%d dedupe keys were claimed from values that are no keys", n)
	}
	// A key whose window never ends is a key: the task takes it back.
	forever := e.keyed(t, &taskmill.EnqueueOptions{DedupeKey: "forever"})
	e.exec(t, "UPDATE {s}.dedupe_keys SET expires_at = 'infinity'")
	e.cancel(t, forever.ID)
	if err := e.client.Retry(ctx, forever.ID); err != nil {
		t.Fatalf("retry of a task with a key that never expires: %v", err)
	}
	if dup := e.keyed(t, &taskmill.EnqueueOptions{DedupeKey: "forever"}); !dup.Duplicate || dup.ID != forever.ID {
		t.Fatalf("after the retry: got %+v, want a duplicate of task %d", dup, forever.ID)
	}
	// And a retry that it refuses says so without the end of its window.
	e.cancel(t, forever.ID)
	second := e.keyed(t, &taskmill.EnqueueOptions{DedupeKey: "forever"})
	e.exec(t, "UPDATE {s}.dedupe_keys SET expires_at = 'infinity'")
	e.wantRefused(t, "a key that never expires", forever.ID, second.ID)
}

// A completed task keeps its key until the window ends, also when the task
// itself is gone: its kind keeps no history, or the history's retention
// removed it. A retry that needs the key is refused until then, and its
// error names no task: a caller could find none to cancel or delete.
func TestRetryIsRefusedByTheKeyOfATaskThatIsGone(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ctx := t.Context()
	opts := &taskmill.EnqueueOptions{DedupeKey: "order-42"}
	first := e.keyed(t, opts)
	e.cancel(t, first.ID)
	holder := e.keyed(t, opts)
	// The holder completes, and nothing of it is kept but its key.
	e.exec(t, "DELETE FROM {s}.tasks WHERE id = $1", holder.ID)

	err := e.client.Retry(ctx, first.ID)
	if !errors.Is(err, taskmill.ErrConflict) || !strings.Contains(err.Error(), "a task that no longer exists holds its dedupe key until 2") ||
		strings.Contains(err.Error(), fmt.Sprintf("task %d", holder.ID)) {
		t.Fatalf("got %v, want ErrConflict that names no task, and the end of the key's window", err)
	}
	if err := e.client.Delete(ctx, holder.ID); !errors.Is(err, taskmill.ErrNotFound) {
		t.Fatalf("delete of the task that is gone: got %v, want ErrNotFound", err)
	}
	e.wantState(t, first.ID, taskmill.StateCanceled)

	// Once the window of the gone task ends, the retry takes the key.
	e.exec(t, "UPDATE {s}.dedupe_keys SET expires_at = now() - interval '1 second'")
	if err := e.client.Retry(ctx, first.ID); err != nil {
		t.Fatalf("retry after the window of the task that is gone: %v", err)
	}
	if dup := e.keyed(t, opts); !dup.Duplicate || dup.ID != first.ID {
		t.Fatalf("after the retry: got %+v, want a duplicate of task %d", dup, first.ID)
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

	// A pending task's group is locked before the task.
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

// PostgreSQL ends one transaction of a deadlock, and it may be a Cancel, a
// Delete, or a Retry that took its locks in the right order: here, another
// transaction holds the group's row and then asks for the task's row. The
// three own their transactions, so they run again.
func TestCancelDeleteAndRetryRunAgainAfterADeadlock(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name string
		act  func(c *taskmill.Client, ctx context.Context, id int64) error
		want taskmill.State // of the task in the end; empty if it is gone
	}{
		{"cancel", (*taskmill.Client).Cancel, taskmill.StateCanceled},
		{"delete", (*taskmill.Client).Delete, ""},
		{"retry", (*taskmill.Client).Retry, taskmill.StatePending},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			e := newEnv(t)
			ctx := t.Context()
			group := &taskmill.EnqueueOptions{GroupKey: "g"}
			id := e.enqueue(t, sendEmail{To: "head"}, group)
			if tt.name == "retry" {
				// A canceled task behind the group's head.
				id = e.enqueue(t, sendEmail{To: "next"}, group)
				if err := e.client.Cancel(ctx, id); err != nil {
					t.Fatal(err)
				}
			}

			// The other transaction holds the group's row, and is in no hurry
			// to look for a deadlock: PostgreSQL will end the action's.
			other := e.begin(t)
			e.txExec(t, other, "SET LOCAL deadlock_timeout = '1min'")
			e.txExec(t, other, "SELECT FROM {s}.groups FOR UPDATE")
			// The action locks the task's row, and waits for the group's.
			acted := make(chan error, 1)
			go func() { acted <- tt.act(e.client, ctx, id) }()
			e.waitBlockedCount(t, 1)
			// The other transaction gets the task's row once the action's
			// transaction has ended, after a second.
			e.txExec(t, other, "SELECT FROM {s}.tasks WHERE id = $1 FOR UPDATE", id)
			if err := other.Rollback(ctx); err != nil {
				t.Fatal(err)
			}

			if err := recv(t, acted, "the action"); err != nil {
				t.Fatalf("%s: %v", tt.name, err)
			}
			if tt.want != "" {
				e.wantState(t, id, tt.want)
			} else if _, err := e.client.Get(ctx, id); !errors.Is(err, taskmill.ErrNotFound) {
				t.Fatalf("the deleted task: got %v, want ErrNotFound", err)
			}
		})
	}
}

// spyTx is a transaction that notes the statements that run on it.
type spyTx struct {
	pgx.Tx
	statements *[]string
}

func (s spyTx) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	*s.statements = append(*s.statements, sql)
	return s.Tx.Exec(ctx, sql, args...)
}

func (s spyTx) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) { //nolint:ireturn // pgx's rows type is an interface
	*s.statements = append(*s.statements, sql)
	return s.Tx.Query(ctx, sql, args...)
}

func (s spyTx) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row { //nolint:ireturn // pgx's row type is an interface
	*s.statements = append(*s.statements, sql)
	return s.Tx.QueryRow(ctx, sql, args...)
}

// CancelTx runs in the caller's transaction, where it must set no savepoint.
// Each savepoint that writes is a subtransaction, and a transaction with many
// of them slows every session of the database down while it stays open.
func TestCancelTxSetsNoSavepoint(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ctx := t.Context()
	group := &taskmill.EnqueueOptions{GroupKey: "g"}
	head := e.enqueue(t, sendEmail{To: "head"}, group)
	next := e.enqueue(t, sendEmail{To: "next"}, group)

	// A pending task and a head take their locks in different orders.
	var statements []string
	tx := e.begin(t)
	for _, id := range []int64{next, head} {
		if err := e.client.CancelTx(ctx, taskmill.Pgx(spyTx{Tx: tx, statements: &statements}), id); err != nil {
			t.Fatal(err)
		}
	}
	if len(statements) == 0 {
		t.Fatal("CancelTx ran no statement on the caller's transaction")
	}
	for _, statement := range statements {
		if strings.Contains(strings.ToUpper(statement), "SAVEPOINT") {
			t.Errorf("CancelTx ran %q in the caller's transaction", statement)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	e.wantState(t, head, taskmill.StateCanceled)
	e.wantState(t, next, taskmill.StateCanceled)
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
