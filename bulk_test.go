package taskmill_test

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"reflect"
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
		if got := e.states(t); !maps.Equal(got, step.want) {
			t.Fatalf("after %s: %v, want %v", step.name, got, step.want)
		}
		e.checkGroups(t)
		if step.name == "retry" {
			e.wantHead(t, "big", e.count(t, "SELECT min(id) FROM {s}.tasks WHERE group_key = 'big'"))
		}
	}
}

// states counts the tasks in each state.
func (e *env) states(t *testing.T) map[string]int {
	t.Helper()
	got := map[string]int{}
	r, err := e.pool.Query(t.Context(), replaceSchema("SELECT state::text, count(*) FROM {s}.tasks GROUP BY state", e.schema))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	for r.Next() {
		var state string
		var n int
		if err := r.Scan(&state, &n); err != nil {
			t.Fatal(err)
		}
		got[state] = n
	}
	return got
}

// wantHead fails the test unless task id is the head of FIFO group.
func (e *env) wantHead(t *testing.T, group string, id int) {
	t.Helper()
	if head := e.count(t, `SELECT coalesce(min(id), 0) FROM {s}.tasks WHERE group_key = $1 AND state <> 'pending'
	                       AND state NOT IN ('dead', 'canceled')`, group); head != id {
		t.Fatalf("group %s has head %d, want %d", group, head, id)
	}
}

// A retry larger than a batch keeps a FIFO group in order, however the
// group's tasks lie in the table, and a held task leaves the order of the
// others alone.
func TestRetryKeepsTheOrderOfGroups(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ctx := t.Context()
	filler := make([]taskmill.EnqueueParams, 1500)
	for i := range filler {
		filler[i].Args = report{}
	}
	if _, err := e.client.EnqueueMany(ctx, filler); err != nil {
		t.Fatal(err)
	}
	group := make([]taskmill.EnqueueParams, 1500)
	for i := range group {
		group[i] = taskmill.EnqueueParams{Args: sendEmail{}, Options: &taskmill.EnqueueOptions{GroupKey: "g"}}
	}
	res, err := e.client.EnqueueMany(ctx, group)
	if err != nil {
		t.Fatal(err)
	}
	// Free the first pages, and cancel the newest tasks first, so that their
	// rows move there, before the older ones.
	if _, err := e.client.DeleteMatching(ctx, &taskmill.TaskFilter{Kinds: []string{"report"}}); err != nil {
		t.Fatal(err)
	}
	e.exec(t, "VACUUM {s}.tasks")
	for i := len(res) - 1; i >= 500; i-- {
		e.cancel(t, res[i].ID)
	}
	if _, err := e.client.CancelMatching(ctx, &taskmill.TaskFilter{GroupKeys: []string{"g"}}); err != nil {
		t.Fatal(err)
	}
	oldest, second := res[0].ID, res[1].ID

	tx := e.begin(t)
	e.txExec(t, tx, "SELECT FROM {s}.tasks WHERE id = $1 FOR UPDATE", oldest)
	n, err := e.client.RetryMatching(ctx, &taskmill.TaskFilter{GroupKeys: []string{"g"}})
	if err != nil || n != 1499 {
		t.Fatalf("retried %d tasks, %v; want all but the held one", n, err)
	}
	e.wantHead(t, "g", int(second))
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if n, err := e.client.RetryMatching(ctx, &taskmill.TaskFilter{GroupKeys: []string{"g"}}); err != nil || n != 1 {
		t.Fatalf("retried %d tasks once released, %v; want 1", n, err)
	}
	e.wantState(t, oldest, taskmill.StatePending)
	e.wantHead(t, "g", int(second))
}

// A bulk action never waits for another transaction while it holds rows: it
// skips the tasks, the FIFO groups, and the dedupe keys that others hold, and
// a later run takes them.
func TestBulkActionsSkipHeldRows(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second) // a wait would end it
	defer cancel()
	free := e.enqueue(t, sendEmail{}, nil)
	held := e.enqueue(t, sendEmail{}, nil)
	head := e.enqueue(t, sendEmail{}, &taskmill.EnqueueOptions{GroupKey: "g"})
	grouped := e.enqueue(t, sendEmail{}, &taskmill.EnqueueOptions{GroupKey: "g"})
	keyed := e.enqueue(t, sendEmail{}, &taskmill.EnqueueOptions{DedupeKey: "k"})
	for _, id := range []int64{free, held, grouped} {
		e.cancel(t, id)
	}
	tx := e.begin(t)
	e.txExec(t, tx, "SELECT FROM {s}.tasks WHERE id = $1 FOR UPDATE", held)
	// An open enqueue into the group holds the group's row, and a duplicate
	// one holds the dedupe key.
	for _, opts := range []*taskmill.EnqueueOptions{{GroupKey: "g"}, {DedupeKey: "k"}} {
		if _, err := e.client.EnqueueTx(ctx, taskmill.Pgx(tx), sendEmail{}, opts); err != nil {
			t.Fatal(err)
		}
	}

	if n, err := e.client.RetryMatching(ctx, &taskmill.TaskFilter{States: []taskmill.State{taskmill.StateCanceled}}); err != nil || n != 1 {
		t.Fatalf("retried %d tasks, %v; want only the one not held", n, err)
	}
	if n, err := e.client.CancelMatching(ctx, &taskmill.TaskFilter{States: []taskmill.State{taskmill.StateAvailable}}); err != nil || n != 1 {
		t.Fatalf("canceled %d tasks, %v; want only the retried one", n, err)
	}
	wantStates(t, e, map[int64]taskmill.State{
		free: taskmill.StateCanceled, held: taskmill.StateCanceled, head: taskmill.StateAvailable,
		grouped: taskmill.StateCanceled, keyed: taskmill.StateAvailable,
	})
	// The open transaction goes on: nothing of it waits for the bulk actions.
	if _, err := e.client.EnqueueTx(ctx, taskmill.Pgx(tx), sendEmail{}, &taskmill.EnqueueOptions{GroupKey: "g"}); err != nil {
		t.Fatal(err)
	}

	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if n, err := e.client.RetryMatching(ctx, &taskmill.TaskFilter{States: []taskmill.State{taskmill.StateCanceled}}); err != nil || n != 3 {
		t.Fatalf("retried %d tasks once released, %v; want 3", n, err)
	}
	if n, err := e.client.CancelMatching(ctx, &taskmill.TaskFilter{Kinds: []string{"email.send"}}); err != nil || n != 5 {
		t.Fatalf("canceled %d tasks once released, %v; want 5", n, err)
	}
}

// A retry that must create a group's row, while an open transaction creates
// it too, waits for it only briefly, as an enqueue into the group would, and
// leaves that group's tasks for a later run. It never deadlocks with the
// transaction.
func TestRetryWaitsBrieflyForANewGroup(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second) // a long wait would end it
	defer cancel()
	// Group "a" has a head, and a canceled task to retry; group "b" has no
	// unfinished task, so no row.
	head := e.enqueue(t, sendEmail{}, &taskmill.EnqueueOptions{GroupKey: "a"})
	inA := e.enqueue(t, sendEmail{}, &taskmill.EnqueueOptions{GroupKey: "a"})
	inB := e.enqueue(t, sendEmail{}, &taskmill.EnqueueOptions{GroupKey: "b"})
	e.cancel(t, inA)
	e.cancel(t, inB)
	tx := e.begin(t)
	if _, err := e.client.EnqueueTx(ctx, taskmill.Pgx(tx), sendEmail{}, &taskmill.EnqueueOptions{GroupKey: "b"}); err != nil {
		t.Fatal(err)
	}

	start := time.Now()
	n, err := e.client.RetryMatching(ctx, &taskmill.TaskFilter{States: []taskmill.State{taskmill.StateCanceled}})
	if err != nil || n != 1 || time.Since(start) > 2*time.Second {
		t.Fatalf("retried %d tasks in %v, %v; want the one of group a, at once", n, time.Since(start), err)
	}
	wantStates(t, e, map[int64]taskmill.State{head: taskmill.StateAvailable, inA: taskmill.StatePending, inB: taskmill.StateCanceled})
	// The transaction goes on into the group that the retry held.
	if _, err := e.client.EnqueueTx(ctx, taskmill.Pgx(tx), sendEmail{}, &taskmill.EnqueueOptions{GroupKey: "a"}); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if n, err := e.client.RetryMatching(ctx, &taskmill.TaskFilter{States: []taskmill.State{taskmill.StateCanceled}}); err != nil || n != 1 {
		t.Fatalf("retried %d tasks once released, %v; want 1", n, err)
	}
	e.wantState(t, inB, taskmill.StateAvailable)
}

// Each batch reads about its own tasks only, however many the action went
// through before, so that the work of a bulk action grows with the tasks it
// changes, not with their square.
func TestBulkBatchesReadTheirTasksOnly(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ctx := t.Context()
	e.exec(t, `INSERT INTO {s}.tasks (queue, kind, state, run_at, payload, max_attempts, attempt, finalized_at)
	           SELECT 'q' || (i % 2), 'k' || (i % 7), CASE WHEN i % 3 = 0 THEN 'available' ELSE 'canceled' END::{s}.task_state,
	                  now(), '{}', 3, 0, CASE WHEN i % 3 = 0 THEN NULL ELSE now() END
	           FROM generate_series(1, 150000) i`)
	// The first 140,000 went through earlier batches: none of the actions
	// matches them any more.
	e.exec(t, `UPDATE {s}.tasks SET state = 'running', attempt = 1, lease_expires_at = now() + interval '1 hour',
	           finalized_at = NULL, cancel_requested_at = now() WHERE id <= 140000`)
	e.exec(t, `INSERT INTO {s}.task_history (id, finalized_at, queue, created_at, kind, run_at, attempted_by, attempted_at,
	                                         priority, attempt, max_attempts, payload, metadata, errors)
	           SELECT 1000000 + i, now(), CASE WHEN i > 90000 THEN 'q' || (i % 2) ELSE 'q0' END, now(), 'k', now(), 'w', now(),
	                  2, 1, 3, '{}', '{}', '[]'
	           FROM generate_series(1, 100000) i`)
	e.exec(t, "ANALYZE {s}.tasks")
	e.exec(t, "ANALYZE {s}.task_history")
	for _, c := range []struct {
		action string
		filter *taskmill.TaskFilter
		after  int64
	}{
		{"retry", &taskmill.TaskFilter{Queues: []string{"q1"}}, 140000},
		{"cancel", &taskmill.TaskFilter{Queues: []string{"q1"}}, 140000},
		{"delete", &taskmill.TaskFilter{Queues: []string{"q1"}}, 140000},
		{"delete", &taskmill.TaskFilter{Kinds: []string{"k3"}}, 140000},
		{"history", &taskmill.TaskFilter{Queues: []string{"q1"}}, 1090000},
	} {
		n, err := taskmill.BulkRowsRead(ctx, e.client, c.action, c.filter, c.after)
		if err != nil {
			t.Fatal(err)
		}
		// 10,000 rows follow the position, and 90,000 or more precede it.
		if n > 15000 {
			t.Errorf("%s %+v after %d: a batch read %d rows", c.action, *c.filter, c.after, n)
		}
	}
	e.rawGroups = true
}

// A bulk action that ctx ends midway leaves the tasks and their FIFO groups
// consistent, and says exactly how many tasks it changed.
func TestBulkActionsStopMidway(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	params := make([]taskmill.EnqueueParams, 5000)
	for i := range params {
		params[i] = taskmill.EnqueueParams{Args: sendEmail{}, Options: &taskmill.EnqueueOptions{GroupKey: fmt.Sprint("g", i%10)}}
	}
	for range 6 {
		if _, err := e.client.EnqueueMany(t.Context(), params); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	all := &taskmill.TaskFilter{Queues: []string{taskmill.DefaultQueue}}
	n, err := e.client.CancelMatching(ctx, all)
	if !errors.Is(err, context.DeadlineExceeded) || n >= 30000 {
		t.Fatalf("canceled %d tasks, %v; want it to stop midway", n, err)
	}
	if canceled := e.states(t)["canceled"]; canceled != int(n) {
		t.Fatalf("said it canceled %d tasks, but %d are", n, canceled)
	}
	e.checkGroups(t)
	rest, err := e.client.CancelMatching(t.Context(), all)
	if err != nil || n+rest != 30000 {
		t.Fatalf("canceled %d, then %d tasks, %v; want 30,000 in all", n, rest, err)
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
	running := e.enqueue(t, sendEmail{}, nil)
	e.lease(t, running)
	stopping := e.enqueue(t, sendEmail{}, nil)
	e.lease(t, stopping)
	e.cancel(t, stopping)

	got, err := e.client.Count(ctx, nil)
	want := &taskmill.TaskCounts{
		States: map[taskmill.State]int64{
			taskmill.StateScheduled: 0, taskmill.StatePending: 0, taskmill.StateAvailable: 3, taskmill.StateRunning: 2,
			taskmill.StateRetryable: 0, taskmill.StateCompleted: 1, taskmill.StateDead: 1, taskmill.StateCanceled: 0,
		},
		// Cancel leaves out the running task already asked to stop.
		Retry: 1, Cancel: 4, Delete: 5,
	}
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("all tasks: %+v, %v; want %+v", got, err, want)
	}
	got, err = e.client.Count(ctx, &taskmill.TaskFilter{
		Kinds: []string{"email.send"}, States: []taskmill.State{taskmill.StateAvailable, taskmill.StateCompleted},
	})
	want = &taskmill.TaskCounts{
		States: map[taskmill.State]int64{taskmill.StateAvailable: 2, taskmill.StateCompleted: 1}, Cancel: 2, Delete: 3,
	}
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("filtered: %+v, %v; want %+v", got, err, want)
	}

	e.exec(t, `INSERT INTO {s}.tasks (queue, kind, state, run_at, payload, max_attempts)
	           SELECT 'big', 'k', 'available', now(), '{}', 3 FROM generate_series(1, 10001)`)
	got, err = e.client.Count(ctx, &taskmill.TaskFilter{Queues: []string{"big"}})
	if err != nil || !got.Capped || got.States[taskmill.StateAvailable] != 10000 || got.Cancel != 10000 {
		t.Fatalf("over the cap: %+v, %v; want 10,000, capped", got, err)
	}
}
