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

// bulk returns the params of a bulk action on the tasks that f selects.
func bulk(f *taskmill.TaskFilter) *taskmill.BulkParams {
	return &taskmill.BulkParams{Filter: *f}
}

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

	res, err := e.client.RetryMatching(ctx, bulk(&taskmill.TaskFilter{Kinds: []string{"email.send"}}))
	if err != nil || res.Changed != 6 {
		t.Fatalf("retried %d tasks, %v; want 6", res.Changed, err)
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
	if res, err := e.client.RetryMatching(ctx, bulk(&taskmill.TaskFilter{Kinds: []string{"email.send"}})); err != nil || res.Changed != 0 {
		t.Fatalf("second retry: %d tasks, %v; want none", res.Changed, err)
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
	res, err := e.client.CancelMatching(ctx, bulk(&taskmill.TaskFilter{Queues: []string{"mail"}, States: []taskmill.State{taskmill.StateAvailable}}))
	if err != nil || res.Changed != 2 {
		t.Fatalf("canceled %d available tasks, %v; want 2", res.Changed, err)
	}
	wantStates(t, e, map[int64]taskmill.State{
		keyed: taskmill.StateCanceled, head: taskmill.StateCanceled, second: taskmill.StateAvailable, third: taskmill.StatePending,
	})

	res, err = e.client.CancelMatching(ctx, bulk(&taskmill.TaskFilter{Queues: []string{"mail"}}))
	if err != nil || res.Changed != 4 {
		t.Fatalf("canceled %d tasks, %v; want 4", res.Changed, err)
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
	if res, err := e.client.CancelMatching(ctx, bulk(&taskmill.TaskFilter{States: []taskmill.State{taskmill.StateRunning}})); err != nil || res.Changed != 0 {
		t.Fatalf("second cancel: %d tasks, %v; want none", res.Changed, err)
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
	res, err := e.client.DeleteMatching(ctx, bulk(&taskmill.TaskFilter{States: []taskmill.State{taskmill.StateDead}}))
	if err != nil || res.Changed != 1 {
		t.Fatalf("deleted %d dead tasks, %v; want 1", res.Changed, err)
	}
	e.wantState(t, next, taskmill.StateAvailable)

	res, err = e.client.DeleteMatching(ctx, bulk(&taskmill.TaskFilter{Queues: []string{"mail"}}))
	if err != nil || res.Changed != 4 {
		t.Fatalf("deleted %d tasks, %v; want 4", res.Changed, err)
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
		do   func(context.Context, *taskmill.BulkParams) (taskmill.BulkResult, error)
		want map[string]int
	}{
		{"cancel", e.client.CancelMatching, map[string]int{"canceled": 2500}},
		{"retry", e.client.RetryMatching, map[string]int{"available": 1001, "pending": 1499}},
		{"delete", e.client.DeleteMatching, map[string]int{}},
	} {
		res, err := step.do(ctx, bulk(all))
		if err != nil || res.Changed != 2500 {
			t.Fatalf("%s: %d tasks, %v; want 2500", step.name, res.Changed, err)
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
// group's tasks lie in the table. Once it leaves a task of a group, it
// leaves the group's later tasks too, so that it never retries a group out of
// order.
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
	if _, err := e.client.DeleteMatching(ctx, bulk(&taskmill.TaskFilter{Kinds: []string{"report"}})); err != nil {
		t.Fatal(err)
	}
	e.exec(t, "VACUUM {s}.tasks")
	for i := len(res) - 1; i >= 500; i-- {
		e.cancel(t, res[i].ID)
	}
	g := &taskmill.TaskFilter{GroupKeys: []string{"g"}}
	if _, err := e.client.CancelMatching(ctx, bulk(g)); err != nil {
		t.Fatal(err)
	}

	tx := e.begin(t)
	e.txExec(t, tx, "SELECT FROM {s}.tasks WHERE id = $1 FOR UPDATE", res[0].ID)
	got, err := e.client.RetryMatching(ctx, bulk(g))
	if err != nil || got.Changed != 0 || got.Skipped != 1500 {
		t.Fatalf("with the oldest task held: %+v, %v; want the whole group left", got, err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	got, err = e.client.RetryMatching(ctx, bulk(g))
	if err != nil || got.Changed != 1500 || got.Skipped != 0 {
		t.Fatalf("once released: %+v, %v; want all 1,500", got, err)
	}
	e.wantHead(t, "g", int(res[0].ID))
}

// A retry that cannot create a group's row, because an open transaction
// creates it too, leaves the whole group, in all its batches.
func TestRetryLeavesAGroupItCannotJoin(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	group := make([]taskmill.EnqueueParams, 1500)
	for i := range group {
		group[i] = taskmill.EnqueueParams{Args: sendEmail{}, Options: &taskmill.EnqueueOptions{GroupKey: "g"}}
	}
	res, err := e.client.EnqueueMany(ctx, group)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.client.CancelMatching(ctx, bulk(&taskmill.TaskFilter{GroupKeys: []string{"g"}})); err != nil {
		t.Fatal(err)
	}
	g := &taskmill.TaskFilter{GroupKeys: []string{"g"}, States: []taskmill.State{taskmill.StateCanceled}}
	tx := e.begin(t)
	head, err := e.client.EnqueueTx(ctx, taskmill.Pgx(tx), sendEmail{}, &taskmill.EnqueueOptions{GroupKey: "g"})
	if err != nil {
		t.Fatal(err)
	}
	got, err := e.client.RetryMatching(ctx, bulk(g))
	if err != nil || got.Changed != 0 || got.Skipped != 1500 {
		t.Fatalf("while the group's row is created: %+v, %v; want the whole group left", got, err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if got, err = e.client.RetryMatching(ctx, bulk(g)); err != nil || got.Changed != 1500 {
		t.Fatalf("once created: %+v, %v; want all 1,500", got, err)
	}
	e.wantHead(t, "g", int(head.ID))
	if oldest := e.count(t, "SELECT min(id) FROM {s}.tasks WHERE state = 'pending'"); oldest != int(res[0].ID) {
		t.Fatalf("task %d waits first, want the oldest, %d", oldest, res[0].ID)
	}
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

	if res, err := e.client.RetryMatching(ctx, bulk(&taskmill.TaskFilter{States: []taskmill.State{taskmill.StateCanceled}})); err != nil || res.Changed != 1 {
		t.Fatalf("retried %d tasks, %v; want only the one not held", res.Changed, err)
	}
	if res, err := e.client.CancelMatching(ctx, bulk(&taskmill.TaskFilter{States: []taskmill.State{taskmill.StateAvailable}})); err != nil || res.Changed != 1 {
		t.Fatalf("canceled %d tasks, %v; want only the retried one", res.Changed, err)
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
	if res, err := e.client.RetryMatching(ctx, bulk(&taskmill.TaskFilter{States: []taskmill.State{taskmill.StateCanceled}})); err != nil || res.Changed != 3 {
		t.Fatalf("retried %d tasks once released, %v; want 3", res.Changed, err)
	}
	if res, err := e.client.CancelMatching(ctx, bulk(&taskmill.TaskFilter{Kinds: []string{"email.send"}})); err != nil || res.Changed != 5 {
		t.Fatalf("canceled %d tasks once released, %v; want 5", res.Changed, err)
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
	res, err := e.client.RetryMatching(ctx, bulk(&taskmill.TaskFilter{States: []taskmill.State{taskmill.StateCanceled}}))
	if err != nil || res.Changed != 1 || res.Skipped != 1 || time.Since(start) > 2*time.Second {
		t.Fatalf("retried %+v in %v, %v; want the one of group a, at once, and group b left", res, time.Since(start), err)
	}
	wantStates(t, e, map[int64]taskmill.State{head: taskmill.StateAvailable, inA: taskmill.StatePending, inB: taskmill.StateCanceled})
	// The transaction goes on into the group that the retry held.
	if _, err := e.client.EnqueueTx(ctx, taskmill.Pgx(tx), sendEmail{}, &taskmill.EnqueueOptions{GroupKey: "a"}); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if res, err := e.client.RetryMatching(ctx, bulk(&taskmill.TaskFilter{States: []taskmill.State{taskmill.StateCanceled}})); err != nil || res.Changed != 1 {
		t.Fatalf("retried %d tasks once released, %v; want 1", res.Changed, err)
	}
	e.wantState(t, inB, taskmill.StateAvailable)
}

// A run whose context ends before it starts can go on all the same.
func TestBulkActionsGoOnFromAnyStop(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.enqueue(t, sendEmail{}, nil)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	all := taskmill.TaskFilter{Queues: []string{taskmill.DefaultQueue}}
	res, err := e.client.CancelMatching(ctx, &taskmill.BulkParams{Filter: all})
	if !errors.Is(err, context.Canceled) || res.Next == "" {
		t.Fatalf("canceled run: %+v, %v; want a cursor to go on", res, err)
	}
	if res, err = e.client.CancelMatching(t.Context(), &taskmill.BulkParams{Filter: all, Cursor: res.Next}); err != nil || res.Changed != 1 {
		t.Fatalf("going on: %+v, %v; want the task canceled", res, err)
	}
}

// A run that stops early goes on, in the next run, at the first task it
// skipped: a FIFO group that it left comes back in order.
func TestRetryGoesOnInGroupOrder(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	canceled := func(n int, group string) {
		e.exec(t, `INSERT INTO {s}.tasks (queue, run_at, kind, finalized_at, state, cancel_requested_at, payload, max_attempts, group_key)
		           SELECT 'default', now(), 'email.send', now(), 'canceled', now(), '{}', 3, nullif($1, '')
		           FROM generate_series(1, $2)`, group, n)
	}
	canceled(1, "g")
	oldest := int64(e.count(t, "SELECT max(id) FROM {s}.tasks"))
	canceled(30000, "")
	canceled(1500, "g")
	all := taskmill.TaskFilter{States: []taskmill.State{taskmill.StateCanceled}}

	tx := e.begin(t)
	e.txExec(t, tx, "SELECT FROM {s}.tasks WHERE id = $1 FOR UPDATE", oldest)
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond) // a click that runs out of time
	first, err := e.client.RetryMatching(ctx, &taskmill.BulkParams{Filter: all})
	cancel()
	if err == nil && first.Next != "" || err != nil && !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("first run: %+v, %v", first, err)
	}
	if err := tx.Rollback(t.Context()); err != nil {
		t.Fatal(err)
	}
	// The first run's statement goes on in the database until its
	// cancellation arrives, holding the rows it locked: wait for it, so that
	// the rest skips none of them.
	waitFor(t, "the canceled statement to end", func() bool {
		return e.count(t, `SELECT count(*) FROM pg_stat_activity
		                   WHERE pid <> pg_backend_pid() AND state <> 'idle' AND strpos(query, '{s}.tasks') > 0`) == 0
	})
	rest, err := e.client.RetryMatching(t.Context(), &taskmill.BulkParams{Filter: all, Cursor: first.Next})
	if err != nil || first.Changed+rest.Changed != 31501 || rest.Skipped != 0 || rest.Next != "" {
		t.Fatalf("then %+v after %+v, %v; want every task retried", rest, first, err)
	}
	e.wantHead(t, "g", int(oldest))
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
	// The action's context ends once its first batch committed, as a
	// request's does when it runs out of time.
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go func() {
		for ctx.Err() == nil {
			var n int
			_ = e.pool.QueryRow(ctx, replaceSchema("SELECT count(*) FROM {s}.tasks WHERE state = 'canceled'", e.schema)).Scan(&n)
			if n > 0 {
				cancel()
				return
			}
			time.Sleep(5 * time.Millisecond)
		}
	}()
	all := &taskmill.TaskFilter{Queues: []string{taskmill.DefaultQueue}}
	res, err := e.client.CancelMatching(ctx, bulk(all))
	if !errors.Is(err, context.Canceled) || res.Changed >= 30000 || res.Next == "" {
		t.Fatalf("canceled %+v, %v; want it to stop midway", res, err)
	}
	if canceled := e.states(t)["canceled"]; canceled != int(res.Changed) {
		t.Fatalf("said it canceled %d tasks, but %d are", res.Changed, canceled)
	}
	e.checkGroups(t)
	// It goes on where it stopped.
	rest, err := e.client.CancelMatching(t.Context(), &taskmill.BulkParams{Filter: *all, Cursor: res.Next})
	if err != nil || res.Changed+rest.Changed != 30000 || rest.Next != "" {
		t.Fatalf("canceled %d, then %d tasks, %v; want 30,000 in all", res.Changed, rest.Changed, err)
	}
}

func TestBulkActionsCheckTheFilter(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	actions := map[string]func(context.Context, *taskmill.BulkParams) (taskmill.BulkResult, error){
		"retry": e.client.RetryMatching, "cancel": e.client.CancelMatching, "delete": e.client.DeleteMatching,
	}
	dead := taskmill.TaskFilter{States: []taskmill.State{taskmill.StateDead}}
	for _, params := range []*taskmill.BulkParams{
		nil,
		{},
		{Filter: taskmill.TaskFilter{States: []taskmill.State{"lost"}}},
		{Filter: taskmill.TaskFilter{Queues: []string{""}}},
		{Filter: dead, Cursor: "%%%"},
		{Filter: dead, Cursor: "LTE"},                        // -1
		{Filter: dead, Cursor: "OTIyMzM3MjAzNjg1NDc3NTgwOA"}, // 2^63
	} {
		for name, do := range actions {
			if _, err := do(t.Context(), params); !errors.Is(err, taskmill.ErrInvalidArgument) {
				t.Errorf("%s with %+v: got %v, want ErrInvalidArgument", name, params, err)
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

	n := func(n int64) taskmill.TaskCount { return taskmill.TaskCount{N: n} }
	got, err := e.client.Count(ctx, nil)
	want := &taskmill.TaskCounts{
		States: map[taskmill.State]taskmill.TaskCount{
			taskmill.StateScheduled: n(0), taskmill.StatePending: n(0), taskmill.StateAvailable: n(3), taskmill.StateRunning: n(2),
			taskmill.StateRetryable: n(0), taskmill.StateCompleted: n(1), taskmill.StateDead: n(1), taskmill.StateCanceled: n(0),
		},
		// Cancel leaves out the running task already asked to stop.
		Retry: n(1), Cancel: n(4), Delete: n(5),
	}
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("all tasks: %+v, %v; want %+v", got, err, want)
	}
	got, err = e.client.Count(ctx, &taskmill.TaskFilter{
		Kinds: []string{"email.send"}, States: []taskmill.State{taskmill.StateAvailable, taskmill.StateCompleted},
	})
	want = &taskmill.TaskCounts{
		States: map[taskmill.State]taskmill.TaskCount{taskmill.StateAvailable: n(2), taskmill.StateCompleted: n(1)},
		Cancel: n(2), Delete: n(3),
	}
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("filtered: %+v, %v; want %+v", got, err, want)
	}

	// Each count says whether it stopped at the cap.
	e.exec(t, `INSERT INTO {s}.tasks (queue, kind, state, run_at, payload, max_attempts)
	           SELECT 'big', 'k', 'available', now(), '{}', 3 FROM generate_series(1, 10001)`)
	e.die(t, e.enqueue(t, sendEmail{}, &taskmill.EnqueueOptions{Queue: "big"}))
	got, err = e.client.Count(ctx, &taskmill.TaskFilter{Queues: []string{"big"}})
	capped := taskmill.TaskCount{N: 10000, Capped: true}
	if err != nil || got.States[taskmill.StateAvailable] != capped || got.Cancel != capped || got.Retry != n(1) ||
		got.Delete != (taskmill.TaskCount{N: 10001, Capped: true}) {
		t.Fatalf("over the cap: %+v, %v", got, err)
	}
}
