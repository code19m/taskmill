package taskmill_test

import (
	"context"
	"errors"
	"maps"
	"testing"
	"time"

	"github.com/code19m/taskmill"
)

// die makes a task dead, as a worker does when its last attempt fails; a
// group's head halts its group.
func (e *env) die(t *testing.T, id int64) {
	t.Helper()
	e.exec(t, `UPDATE {s}.tasks SET state = 'dead', attempt = max_attempts, finalized_at = now() WHERE id = $1`, id)
	e.exec(t, `UPDATE {s}.groups g SET halted_task_id = t.id, halted_at = now()
	           FROM {s}.tasks t WHERE t.id = $1 AND g.queue = t.queue AND g.group_key = t.group_key`, id)
}

// lease makes a task running, as a fetch does.
func (e *env) lease(t *testing.T, id int64) {
	t.Helper()
	e.exec(t, `UPDATE {s}.tasks SET state = 'running', attempt = 1, lease_token = 1,
	           lease_expires_at = now() + interval '30 seconds', attempted_at = now(), attempted_by = 'w'
	           WHERE id = $1`, id)
}

// cancel cancels a task and fails the test on error.
func (e *env) cancel(t *testing.T, id int64) {
	t.Helper()
	if err := e.client.Cancel(t.Context(), id); err != nil {
		t.Fatalf("cancel task %d: %v", id, err)
	}
}

// completeIn runs a worker on queue until a task enqueued there completes.
func (e *env) completeIn(t *testing.T, queue string) int64 {
	t.Helper()
	stop := runWorker(t, e, &taskmill.WorkerConfig{
		Handlers: noopHandlers(), Queues: map[string]taskmill.QueueOptions{queue: {Concurrency: 1}},
	})
	defer stop()
	id := e.enqueue(t, sendEmail{}, &taskmill.EnqueueOptions{Queue: queue})
	e.waitState(t, id, taskmill.StateCompleted)
	return id
}

func wantStates(t *testing.T, e *env, want map[int64]taskmill.State) {
	t.Helper()
	for id, state := range want {
		e.wantState(t, id, state)
	}
}

func TestRetryMatching(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ctx := t.Context()
	dead := e.enqueue(t, sendEmail{To: "dead"}, nil)
	e.die(t, dead)
	canceled := e.enqueue(t, sendEmail{To: "canceled"}, nil)
	e.cancel(t, canceled)
	waiting := e.enqueue(t, sendEmail{To: "waiting"}, nil)
	otherKind := e.enqueue(t, report{}, nil)
	e.die(t, otherKind)

	// A dead head halts its group: retrying it resumes the group.
	halting := e.enqueue(t, sendEmail{}, &taskmill.EnqueueOptions{GroupKey: "halted"})
	behind := e.enqueue(t, sendEmail{}, &taskmill.EnqueueOptions{GroupKey: "halted"})
	e.die(t, halting)
	// Of a group with no unfinished tasks, the oldest becomes the head.
	oldest := e.enqueue(t, sendEmail{}, &taskmill.EnqueueOptions{GroupKey: "drained"})
	newest := e.enqueue(t, sendEmail{}, &taskmill.EnqueueOptions{GroupKey: "drained"})
	e.cancel(t, newest)
	e.cancel(t, oldest)
	// In a group with a head, a retried task waits behind it.
	head := e.enqueue(t, sendEmail{}, &taskmill.EnqueueOptions{GroupKey: "busy"})
	waits := e.enqueue(t, sendEmail{}, &taskmill.EnqueueOptions{GroupKey: "busy"})
	e.cancel(t, waits)

	n, err := e.client.RetryMatching(ctx, &taskmill.TaskFilter{Kinds: []string{"email.send"}})
	if err != nil || n != 6 {
		t.Fatalf("retried %d tasks, %v; want 6", n, err)
	}
	wantStates(t, e, map[int64]taskmill.State{
		dead: taskmill.StateAvailable, canceled: taskmill.StateAvailable, waiting: taskmill.StateAvailable,
		otherKind: taskmill.StateDead,
		halting:   taskmill.StateAvailable, behind: taskmill.StatePending,
		oldest: taskmill.StateAvailable, newest: taskmill.StatePending,
		head: taskmill.StateAvailable, waits: taskmill.StatePending,
	})
	if info := e.get(t, dead); info.Attempt != 0 || !info.FinalizedAt.IsZero() {
		t.Fatalf("retried task has attempt %d, finalized at %v", info.Attempt, info.FinalizedAt)
	}
	if n, err := e.client.RetryMatching(ctx, &taskmill.TaskFilter{Kinds: []string{"email.send"}}); err != nil || n != 0 {
		t.Fatalf("second retry: %d tasks, %v; want none", n, err)
	}
}

func TestCancelMatching(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ctx := t.Context()
	mail := func(group string) *taskmill.EnqueueOptions {
		return &taskmill.EnqueueOptions{Queue: "mail", GroupKey: group}
	}
	keyed := e.enqueue(t, sendEmail{}, &taskmill.EnqueueOptions{Queue: "mail", DedupeKey: "k"})
	running := e.enqueue(t, sendEmail{}, mail(""))
	e.lease(t, running)
	head := e.enqueue(t, sendEmail{}, mail("g"))
	second := e.enqueue(t, sendEmail{}, mail("g"))
	third := e.enqueue(t, sendEmail{}, mail("g"))
	halting := e.enqueue(t, sendEmail{}, mail("h"))
	halted := e.enqueue(t, sendEmail{}, mail("h"))
	e.die(t, halting)
	elsewhere := e.enqueue(t, sendEmail{}, nil)

	// Canceling a head promotes the next task of its group.
	n, err := e.client.CancelMatching(ctx, &taskmill.TaskFilter{Queues: []string{"mail"}, States: []taskmill.State{taskmill.StateAvailable}})
	if err != nil || n != 2 {
		t.Fatalf("canceled %d available tasks, %v; want 2", n, err)
	}
	wantStates(t, e, map[int64]taskmill.State{
		keyed: taskmill.StateCanceled, head: taskmill.StateCanceled, second: taskmill.StateAvailable, third: taskmill.StatePending,
	})

	n, err = e.client.CancelMatching(ctx, &taskmill.TaskFilter{Queues: []string{"mail"}})
	if err != nil || n != 4 {
		t.Fatalf("canceled %d tasks, %v; want 4", n, err)
	}
	wantStates(t, e, map[int64]taskmill.State{
		running: taskmill.StateRunning, second: taskmill.StateCanceled, third: taskmill.StateCanceled,
		halting: taskmill.StateDead, halted: taskmill.StateCanceled, elsewhere: taskmill.StateAvailable,
	})
	if n := e.count(t, "SELECT count(*) FROM {s}.tasks WHERE id = $1 AND cancel_requested_at IS NOT NULL", running); n != 1 {
		t.Fatal("running task was not flagged for cancellation")
	}
	if n := e.count(t, "SELECT count(*) FROM {s}.groups WHERE group_key = 'h' AND halted_task_id = $1 AND tasks = 1", halting); n != 1 {
		t.Fatal("the halted group lost its halt or its count")
	}
	if res, err := e.client.Enqueue(ctx, sendEmail{}, &taskmill.EnqueueOptions{Queue: "mail", DedupeKey: "k"}); err != nil || res.Duplicate {
		t.Fatalf("the dedupe key was not released: %+v, %v", res, err)
	}
	// A running task asked to stop is not counted again.
	if n, err := e.client.CancelMatching(ctx, &taskmill.TaskFilter{States: []taskmill.State{taskmill.StateRunning}}); err != nil || n != 0 {
		t.Fatalf("second cancel: %d tasks, %v; want none", n, err)
	}
}

func TestDeleteMatching(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ctx := t.Context()
	completed := e.completeIn(t, "mail")
	waiting := e.enqueue(t, sendEmail{}, &taskmill.EnqueueOptions{Queue: "mail", DedupeKey: "k"})
	running := e.enqueue(t, sendEmail{}, &taskmill.EnqueueOptions{Queue: "mail"})
	e.lease(t, running)
	canceled := e.enqueue(t, sendEmail{}, &taskmill.EnqueueOptions{Queue: "mail"})
	e.cancel(t, canceled)
	halting := e.enqueue(t, sendEmail{}, &taskmill.EnqueueOptions{Queue: "mail", GroupKey: "g"})
	next := e.enqueue(t, sendEmail{}, &taskmill.EnqueueOptions{Queue: "mail", GroupKey: "g"})
	e.die(t, halting)

	// Deleting the dead head that halts its group resumes the group.
	n, err := e.client.DeleteMatching(ctx, &taskmill.TaskFilter{States: []taskmill.State{taskmill.StateDead}})
	if err != nil || n != 1 {
		t.Fatalf("deleted %d dead tasks, %v; want 1", n, err)
	}
	e.wantState(t, next, taskmill.StateAvailable)

	n, err = e.client.DeleteMatching(ctx, &taskmill.TaskFilter{Queues: []string{"mail"}})
	if err != nil || n != 4 {
		t.Fatalf("deleted %d tasks, %v; want 4", n, err)
	}
	for _, id := range []int64{completed, waiting, canceled, halting, next} {
		if _, err := e.client.Get(ctx, id); !errors.Is(err, taskmill.ErrNotFound) {
			t.Fatalf("task %d: got %v, want ErrNotFound", id, err)
		}
	}
	e.wantState(t, running, taskmill.StateRunning)
	if n := e.count(t, "SELECT count(*) FROM {s}.groups"); n != 0 {
		t.Fatal("the emptied group was not removed")
	}
	if res, err := e.client.Enqueue(ctx, sendEmail{}, &taskmill.EnqueueOptions{Queue: "mail", DedupeKey: "k"}); err != nil || res.Duplicate {
		t.Fatalf("the dedupe key was not released: %+v, %v", res, err)
	}
}

// Bulk actions go on batch after batch until no task is left, and keep FIFO
// groups consistent across batches.
func TestBulkActionsWorkInBatches(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ctx := t.Context()
	params := make([]taskmill.EnqueueParams, 2500)
	for i := range params {
		params[i].Args = sendEmail{}
		if i%5 < 3 {
			params[i].Options = &taskmill.EnqueueOptions{GroupKey: "big"}
		}
	}
	if _, err := e.client.EnqueueMany(ctx, params); err != nil {
		t.Fatal(err)
	}
	all := &taskmill.TaskFilter{Queues: []string{taskmill.DefaultQueue}}
	for _, step := range []struct {
		name string
		do   func(context.Context, *taskmill.TaskFilter) (int64, error)
		want map[string]int
	}{
		{"cancel", e.client.CancelMatching, map[string]int{"canceled": 2500}},
		{"retry", e.client.RetryMatching, map[string]int{"available": 1001, "pending": 1499}},
		{"delete", e.client.DeleteMatching, map[string]int{}},
	} {
		n, err := step.do(ctx, all)
		if err != nil || n != 2500 {
			t.Fatalf("%s: %d tasks, %v; want 2500", step.name, n, err)
		}
		got := map[string]int{}
		r, err := e.pool.Query(ctx, replaceSchema("SELECT state::text, count(*) FROM {s}.tasks GROUP BY state", e.schema))
		if err != nil {
			t.Fatal(err)
		}
		for r.Next() {
			var state string
			var n int
			if err := r.Scan(&state, &n); err != nil {
				t.Fatal(err)
			}
			got[state] = n
		}
		r.Close()
		if !maps.Equal(got, step.want) {
			t.Fatalf("after %s: %v, want %v", step.name, got, step.want)
		}
		e.checkGroups(t)
	}
}

// A bulk action never waits for another transaction: it skips the tasks and
// the FIFO groups that others hold, and a later run takes them.
func TestBulkActionsSkipHeldRows(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	free := e.enqueue(t, sendEmail{}, nil)
	held := e.enqueue(t, sendEmail{}, nil)
	head := e.enqueue(t, sendEmail{}, &taskmill.EnqueueOptions{GroupKey: "g"})
	grouped := e.enqueue(t, sendEmail{}, &taskmill.EnqueueOptions{GroupKey: "g"})
	for _, id := range []int64{free, held, grouped} {
		e.cancel(t, id)
	}
	tx := e.begin(t)
	e.txExec(t, tx, "SELECT FROM {s}.tasks WHERE id = $1 FOR UPDATE", held)
	// An open enqueue into the group holds the group's row.
	if _, err := e.client.EnqueueTx(t.Context(), taskmill.Pgx(tx), sendEmail{}, &taskmill.EnqueueOptions{GroupKey: "g"}); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	canceled := &taskmill.TaskFilter{States: []taskmill.State{taskmill.StateCanceled}}
	if n, err := e.client.RetryMatching(ctx, canceled); err != nil || n != 1 {
		t.Fatalf("retried %d tasks, %v; want only the one not held", n, err)
	}
	wantStates(t, e, map[int64]taskmill.State{
		free: taskmill.StateAvailable, held: taskmill.StateCanceled, head: taskmill.StateAvailable, grouped: taskmill.StateCanceled,
	})

	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if n, err := e.client.RetryMatching(ctx, canceled); err != nil || n != 2 {
		t.Fatalf("retried %d tasks once released, %v; want 2", n, err)
	}
	wantStates(t, e, map[int64]taskmill.State{held: taskmill.StateAvailable, grouped: taskmill.StatePending})
}

// A bulk action leaves alone the tasks that came after it started, so that it
// ends while producers enqueue, and retries a task that fails again at most
// once.
func TestBulkActionsLeaveLaterTasks(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	later := e.enqueue(t, sendEmail{}, nil)
	e.exec(t, `UPDATE {s}.tasks SET created_at = now() + interval '1 hour' WHERE id = $1`, later)
	if n, err := e.client.CancelMatching(t.Context(), &taskmill.TaskFilter{Queues: []string{taskmill.DefaultQueue}}); err != nil || n != 0 {
		t.Fatalf("canceled %d tasks, %v; want none", n, err)
	}
	e.die(t, later)
	e.exec(t, `UPDATE {s}.tasks SET finalized_at = now() + interval '1 hour' WHERE id = $1`, later)
	if n, err := e.client.RetryMatching(t.Context(), &taskmill.TaskFilter{Queues: []string{taskmill.DefaultQueue}}); err != nil || n != 0 {
		t.Fatalf("retried %d tasks, %v; want none", n, err)
	}
}

func TestBulkActionsCheckTheFilter(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	actions := map[string]func(context.Context, *taskmill.TaskFilter) (int64, error){
		"retry": e.client.RetryMatching, "cancel": e.client.CancelMatching, "delete": e.client.DeleteMatching,
	}
	for _, filter := range []*taskmill.TaskFilter{
		nil,
		{},
		{States: []taskmill.State{"lost"}},
		{Queues: []string{""}},
	} {
		for name, do := range actions {
			if _, err := do(t.Context(), filter); !errors.Is(err, taskmill.ErrInvalidArgument) {
				t.Errorf("%s with filter %+v: got %v, want ErrInvalidArgument", name, filter, err)
			}
		}
	}
	if _, err := e.client.Count(t.Context(), &taskmill.TaskFilter{States: []taskmill.State{"lost"}}); !errors.Is(err, taskmill.ErrInvalidArgument) {
		t.Errorf("count with an unknown state: got %v, want ErrInvalidArgument", err)
	}
}

func TestCount(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ctx := t.Context()
	e.completeIn(t, "done")
	e.enqueue(t, sendEmail{}, nil)
	e.enqueue(t, sendEmail{}, nil)
	e.die(t, e.enqueue(t, sendEmail{}, nil))
	e.enqueue(t, report{}, nil)

	got, err := e.client.Count(ctx, nil)
	want := map[taskmill.State]int64{
		taskmill.StateScheduled: 0, taskmill.StatePending: 0, taskmill.StateAvailable: 3, taskmill.StateRunning: 0,
		taskmill.StateRetryable: 0, taskmill.StateCompleted: 1, taskmill.StateDead: 1, taskmill.StateCanceled: 0,
	}
	if err != nil || !maps.Equal(got, want) {
		t.Fatalf("all tasks: %v, %v; want %v", got, err, want)
	}
	got, err = e.client.Count(ctx, &taskmill.TaskFilter{
		Kinds: []string{"email.send"}, States: []taskmill.State{taskmill.StateAvailable, taskmill.StateCompleted},
	})
	want = map[taskmill.State]int64{taskmill.StateAvailable: 2, taskmill.StateCompleted: 1}
	if err != nil || !maps.Equal(got, want) {
		t.Fatalf("filtered: %v, %v; want %v", got, err, want)
	}
}
