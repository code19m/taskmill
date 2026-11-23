package taskmill_test

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/code19m/taskmill"
	"github.com/code19m/taskmill/internal/testdb"
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
	if info := e.get(t, dead); info.Attempt != 0 || !info.FinishedAt.IsZero() {
		t.Fatalf("retried task has attempt %d, finalized at %v", info.Attempt, info.FinishedAt)
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

// A bulk action that takes only tasks from behind a group's head promotes
// nothing: the head is still there.
func TestBulkActionBehindAHeadPromotesNothing(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	group := &taskmill.EnqueueOptions{Queue: "mail", GroupKey: "g"}
	head := e.enqueue(t, sendEmail{}, group)
	behind := e.enqueue(t, report{}, group)
	last := e.enqueue(t, sendEmail{}, group)

	res, err := e.client.CancelMatching(t.Context(), bulk(&taskmill.TaskFilter{Kinds: []string{report{}.Kind()}}))
	if err != nil || res.Changed != 1 {
		t.Fatalf("canceled %d tasks, %v; want 1", res.Changed, err)
	}
	wantStates(t, e, map[int64]taskmill.State{
		head: taskmill.StateAvailable, behind: taskmill.StateCanceled, last: taskmill.StatePending,
	})
}

// A batch of DeleteMatching takes at most 1,000 tasks of both tables in
// all, as BulkResult.Changed promises: an action that stops early has
// changed at most a batch more than it counted. Here the action stops
// after its first batch, which it counts.
func TestDeleteMatchingTakesOneBatchOfBothTables(t *testing.T) {
	t.Parallel()
	first := &beforeStatement{fragment: ".task_history WHERE id = ANY($1::bigint[])"}
	e := newTracedEnv(t, first)

	// 1,500 dead tasks and 1,500 completed ones, their IDs interleaved.
	e.exec(t, `INSERT INTO {s}.tasks (id, queue, kind, state, run_at, payload, max_attempts, finalized_at)
	           OVERRIDING SYSTEM VALUE
	           SELECT 2 * i, 'default', 'email.send', 'dead', now(), '{}', 3, now() FROM generate_series(1, 1500) i`)
	e.exec(t, `INSERT INTO {s}.task_history (id, finalized_at, queue, created_at, kind, run_at, attempted_by, attempted_at,
	                                         priority, attempt, max_attempts, payload, metadata, errors)
	           SELECT 2 * i + 1, now(), 'default', now(), 'email.send', now(), 'w', now(), 2, 1, 3, '{}', '{}', '[]'
	           FROM generate_series(1, 1500) i`)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	stop := func() { cancel() }
	first.call.Store(&stop)

	res, err := e.client.DeleteMatching(ctx, bulk(&taskmill.TaskFilter{States: []taskmill.State{taskmill.StateDead, taskmill.StateCompleted}}))
	if !errors.Is(err, context.Canceled) || res.Next == "" {
		t.Fatalf("got %+v, %v; want the action to stop after its first batch", res, err)
	}
	if res.Changed == 0 || res.Changed > 1000 {
		t.Fatalf("the first batch changed %d tasks, want at most 1000", res.Changed)
	}
	if left := e.count(t, "SELECT (SELECT count(*) FROM {s}.tasks) + (SELECT count(*) FROM {s}.task_history)"); int64(left) != 3000-res.Changed {
		t.Fatalf("%d tasks are left, but the action counted %d of 3000 as deleted", left, res.Changed)
	}
}

// Count and the bulk actions take the ranges of times of ListParams, under
// its rules, and count and change only the tasks of the range: an operator
// retries what died in an incident, and not the older dead tasks of the same
// kind.
func TestCountAndBulkActionsTakeARangeOfTimes(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ctx := t.Context()

	// The range is from second 10 to second 20. Tasks 2, 3, and 4 died in it;
	// 1 died before it, and 5 at its end, outside it. Of the tasks that wait,
	// 7 was created in it, as was 12, which runs, and of the completed tasks,
	// 10. With no queue named, the statements find the queue q twice, of the
	// running tasks and of the others, and must read its tasks once.
	start := time.Date(2026, time.March, 14, 14, 0, 0, 0, time.UTC)
	e.exec(t, `INSERT INTO {s}.tasks (id, queue, kind, state, created_at, finalized_at, run_at, payload, max_attempts)
	           OVERRIDING SYSTEM VALUE
	           SELECT v.id, 'q', 'charge', v.state::{s}.task_state, $1::timestamptz + v.created * interval '1 second',
	                  $1::timestamptz + v.finished * interval '1 second', now(), '{}', 3
	           FROM (VALUES (1, 'dead', 0, 5), (2, 'dead', 0, 10), (3, 'canceled', 0, 15), (4, 'dead', 0, 19), (5, 'dead', 0, 20),
	                        (6, 'available', 5, NULL), (7, 'available', 10, NULL), (8, 'scheduled', 20, NULL)
	                ) AS v(id, state, created, finished)`, start)
	e.exec(t, `INSERT INTO {s}.task_history (id, finalized_at, queue, created_at, kind, run_at, attempted_by, attempted_at,
	                                         priority, attempt, max_attempts, payload, metadata, errors)
	           SELECT v.id, now(), 'q', $1::timestamptz + v.created * interval '1 second', 'charge', now(), 'w', now(),
	                  2, 1, 3, '{}', '{}', '[]'
	           FROM (VALUES (9, 5), (10, 15), (11, 25)) AS v(id, created)`, start)
	e.exec(t, `INSERT INTO {s}.tasks (id, queue, kind, state, created_at, run_at, payload, max_attempts, attempt,
	                                 lease_expires_at, attempted_at, attempted_by)
	           OVERRIDING SYSTEM VALUE
	           VALUES (12, 'q', 'charge', 'running', $1::timestamptz + interval '12 seconds', now(), '{}', 3, 1,
	                   now() + interval '1 hour', now(), 'w')`, start)
	since, before := start.Add(10*time.Second), start.Add(20*time.Second)
	finished := taskmill.TaskFilter{States: []taskmill.State{taskmill.StateDead, taskmill.StateCanceled}}
	charges := taskmill.TaskFilter{Kinds: []string{"charge"}}
	queue := taskmill.TaskFilter{Queues: []string{"q"}}

	counts, err := e.client.Count(ctx, &taskmill.CountParams{Filter: inRange(finished, taskmill.TaskFilter{FinishedSince: since, FinishedBefore: before})})
	if err != nil || counts.Retry != (taskmill.TaskCount{N: 3}) || counts.States[taskmill.StateDead].N != 2 {
		t.Fatalf("count of the tasks that finished in the range: %+v, %v; want 3 to retry, 2 of them dead", counts, err)
	}
	counts, err = e.client.Count(ctx, &taskmill.CountParams{Filter: inRange(charges, taskmill.TaskFilter{CreatedSince: since, CreatedBefore: before})})
	if err != nil || counts.Cancel.N != 2 || counts.Delete.N != 2 {
		t.Fatalf("count of the tasks created in the range: %+v, %v; want 2 to cancel and 2 to delete", counts, err)
	}

	res, err := e.client.RetryMatching(ctx, &taskmill.BulkParams{Filter: inRange(finished, taskmill.TaskFilter{FinishedSince: since, FinishedBefore: before})})
	if err != nil || res.Changed != 3 {
		t.Fatalf("retry of the tasks that finished in the range: %+v, %v; want 3", res, err)
	}
	res, err = e.client.CancelMatching(ctx, &taskmill.BulkParams{Filter: inRange(charges, taskmill.TaskFilter{CreatedSince: since, CreatedBefore: before})})
	if err != nil || res.Changed != 2 {
		t.Fatalf("cancel of the tasks created in the range: %+v, %v; want 2", res, err)
	}
	completed := taskmill.TaskFilter{States: []taskmill.State{taskmill.StateCompleted}}
	res, err = e.client.DeleteMatching(ctx, &taskmill.BulkParams{Filter: inRange(completed, taskmill.TaskFilter{CreatedSince: since, CreatedBefore: before})})
	if err != nil || res.Changed != 1 {
		t.Fatalf("delete of the completed tasks created in the range: %+v, %v; want 1", res, err)
	}
	wantStates(t, e, map[int64]taskmill.State{
		1: taskmill.StateDead, 2: taskmill.StateAvailable, 3: taskmill.StateAvailable, 4: taskmill.StateAvailable, 5: taskmill.StateDead,
		6: taskmill.StateAvailable, 7: taskmill.StateCanceled, 8: taskmill.StateScheduled, 9: taskmill.StateCompleted, 11: taskmill.StateCompleted,
		12: taskmill.StateRunning,
	})
	if n := e.count(t, "SELECT count(*) FROM {s}.tasks WHERE id = 12 AND cancel_requested_at IS NOT NULL"); n != 1 {
		t.Error("the running task created in the range was not asked to stop")
	}
	if _, err := e.client.Get(ctx, 10); !errors.Is(err, taskmill.ErrNotFound) {
		t.Fatalf("task 10: got %v, want ErrNotFound", err)
	}

	// The rules of ListParams: one of the two ranges, a range of finish times
	// only for a filter of only dead and canceled tasks, as Count takes it,
	// and bounds that are a range.
	for name, err := range map[string]error{
		"retry in both ranges":                            errOf(e.client.RetryMatching(ctx, &taskmill.BulkParams{Filter: inRange(finished, taskmill.TaskFilter{CreatedSince: since, FinishedSince: since})})),
		"retry by finish time, of every state":            errOf(e.client.RetryMatching(ctx, &taskmill.BulkParams{Filter: inRange(queue, taskmill.TaskFilter{FinishedSince: since})})),
		"cancel by finish time":                           errOf(e.client.CancelMatching(ctx, &taskmill.BulkParams{Filter: inRange(queue, taskmill.TaskFilter{FinishedSince: since})})),
		"delete by finish time, completed tasks too":      errOf(e.client.DeleteMatching(ctx, &taskmill.BulkParams{Filter: inRange(queue, taskmill.TaskFilter{FinishedBefore: before})})),
		"count by finish time, of every task":             errOf(e.client.Count(ctx, &taskmill.CountParams{Filter: taskmill.TaskFilter{FinishedSince: since}})),
		"count in a range that ends before it starts":     errOf(e.client.Count(ctx, &taskmill.CountParams{Filter: taskmill.TaskFilter{CreatedSince: before, CreatedBefore: since}})),
		"retry after a time that PostgreSQL cannot store": errOf(e.client.RetryMatching(ctx, &taskmill.BulkParams{Filter: inRange(finished, taskmill.TaskFilter{FinishedSince: lastTimestamp.AddDate(1, 0, 0)})})),
	} {
		if !errors.Is(err, taskmill.ErrInvalidArgument) {
			t.Errorf("%s: got %v, want ErrInvalidArgument", name, err)
		}
	}
}

// A range of times narrows what Count and the first batch of a bulk action
// read to about the tasks of the range, through the index that holds those
// times: the dead and canceled tasks by finish time, and the others,
// completed tasks included, by creation time. A batch reads each task of
// the range once to pick it, and those it picks once more to lock them. The
// indexes hold tasks by queue first, and PostgreSQL skips through an index's
// first column only from version 18: with no queue named, the statements
// find the queues first, so that every version reads each queue's part of
// the index. The rows read are rows of the tables: they leave out the index
// entries that a scan passes over.
func TestRangesOfCountAndBulkActionsReadOnlyTheirRows(t *testing.T) {
	t.Parallel()
	e := newEnv(t)

	// 60,000 dead tasks over 30 days, the newest last, a third of them of
	// the kind charge, 20,000 available tasks in 50 other queues, and 60,000
	// completed tasks in those queues, created over 30 days. The range is a
	// day 3 days back, which holds 2,000 dead tasks and 2,000 completed ones.
	e.exec(t, `INSERT INTO {s}.tasks (queue, kind, state, created_at, finalized_at, run_at, payload, max_attempts)
	           SELECT 'q', CASE WHEN i % 3 = 0 THEN 'charge' ELSE 'mail' END, 'dead',
	                  now() - i * interval '43200 milliseconds', now() - i * interval '43200 milliseconds', now(), '{}', 3
	           FROM generate_series(60000, 1, -1) i`)
	e.exec(t, `INSERT INTO {s}.tasks (queue, kind, state, run_at, payload, max_attempts)
	           SELECT 'other' || (i % 50), 'mail', 'available', now(), '{}', 3 FROM generate_series(1, 20000) i`)
	e.exec(t, `INSERT INTO {s}.task_history (id, finalized_at, queue, created_at, kind, run_at, attempted_by, attempted_at,
	                                         priority, attempt, max_attempts, payload, metadata, errors)
	           SELECT 200000 - i, now(), 'other' || (i % 50), now() - i * interval '43200 milliseconds', 'mail', now(), 'w', now(),
	                  2, 1, 3, '{}', '{}', '[]'
	           FROM generate_series(1, 60000) i`)
	e.exec(t, "ANALYZE {s}.tasks")
	e.exec(t, "ANALYZE {s}.task_history")
	since := time.Now().Add(-4 * 24 * time.Hour)
	before := since.Add(24 * time.Hour)
	dead := []taskmill.State{taskmill.StateDead}

	for _, c := range []struct {
		action string // the pick: of retry, cancel, delete, or of a delete in history
		params taskmill.BulkParams
		count  bool // the count of the filter holds about the 2,000 tasks too
	}{
		{"retry", taskmill.BulkParams{Filter: taskmill.TaskFilter{States: dead, Kinds: []string{"charge"}, FinishedSince: since, FinishedBefore: before}}, true},
		{"retry", taskmill.BulkParams{Filter: taskmill.TaskFilter{Queues: []string{"q"}, Kinds: []string{"charge"}, CreatedSince: since, CreatedBefore: before}}, true},
		{"retry", taskmill.BulkParams{Filter: taskmill.TaskFilter{States: dead, FinishedSince: since, FinishedBefore: before}}, true},
		{"retry", taskmill.BulkParams{Filter: taskmill.TaskFilter{States: dead, CreatedSince: since, CreatedBefore: before}}, true},
		{"delete", taskmill.BulkParams{Filter: taskmill.TaskFilter{Queues: []string{"q"}, States: dead, FinishedSince: since, FinishedBefore: before}}, true},
		{"delete", taskmill.BulkParams{Filter: taskmill.TaskFilter{Queues: []string{"q"}, CreatedSince: since, CreatedBefore: before}}, true},
		{"history", taskmill.BulkParams{Filter: taskmill.TaskFilter{States: []taskmill.State{taskmill.StateCompleted}, CreatedSince: since, CreatedBefore: before}}, true},
		{"cancel", taskmill.BulkParams{Filter: taskmill.TaskFilter{Queues: []string{"q"}, Kinds: []string{"mail"}, CreatedSince: since, CreatedBefore: before}}, true},
		// Its count also counts the 2,000 completed tasks of the range.
		{"cancel", taskmill.BulkParams{Filter: taskmill.TaskFilter{Kinds: []string{"mail"}, CreatedSince: since, CreatedBefore: before}}, false},
	} {
		n, err := taskmill.BulkRowsRead(t.Context(), e.client, c.action, &c.params, 0, false)
		if err != nil {
			t.Fatal(err)
		}
		if n > 4000 {
			t.Errorf("%s %+v: a batch read %d rows for the 2,000 tasks of the range", c.action, c.params, n)
		}
		if !c.count {
			continue
		}

		count := taskmill.CountParams{Filter: c.params.Filter}
		n, err = taskmill.CountRowsRead(t.Context(), e.client, &count)
		if err != nil {
			t.Fatal(err)
		}
		if n > 4000 {
			t.Errorf("count %+v: read %d rows for the 2,000 tasks of the range", count, n)
		}
	}
}

// A whole action with a range of times and no queue reads about the tasks of
// the range, also when the range ends in the past and spans several
// batches. For a range of creation times, whose tasks lie together, the
// first batch reads the range queue by queue, which skips the tasks before
// it, and so does the last batch of a range with an end, which stops with
// it. A range of finish times can hold tasks that lie far apart, as here:
// the first batch reads it queue by queue, keeps the IDs of its tasks, and
// picks the first of them, and the next batches pick the others. Each
// changed task is read about three times: to pick it, to lock it, and to
// change it, and once more after the first batch of a range of finish
// times. However the batches read, the action changes each task of the
// range once, and no other task.
//
// The bound is on the plan. On a busy machine a batch's statements can take
// longer than openTimeout, and the action then goes on in windows, which
// read the whole table by design (TestARangeBatchThatRunsOutOfTimeGoesOnInWindows):
// the bound holds only for an action that did not.
func TestRangeActionsReadAboutTheirTasks(t *testing.T) {
	t.Parallel()

	// 60,000 dead tasks in 10 queues, created over 30 days until 2 days ago,
	// the newest last, and 20,000 available tasks after them. Most died a
	// minute after they were created; every 30th, 2,000 in all, died in the
	// last day.
	setup := func(t *testing.T) *env {
		t.Helper()
		e := newEnv(t)
		e.exec(t, `INSERT INTO {s}.tasks (queue, kind, state, created_at, finalized_at, run_at, payload, max_attempts)
		           SELECT 'q' || (i % 10), 'email.send', 'dead', c,
		                  CASE WHEN i % 30 = 0 THEN now() - (i % 80000) * interval '1 second' ELSE c + interval '1 minute' END,
		                  now(), '{}', 3
		           FROM generate_series(60000, 1, -1) i,
		                LATERAL (SELECT now() - interval '2 days' - i * interval '43200 milliseconds' AS c) x`)
		e.exec(t, `INSERT INTO {s}.tasks (queue, kind, state, run_at, payload, max_attempts)
		           SELECT 'other' || (i % 10), 'email.send', 'available', now(), '{}', 3 FROM generate_series(1, 20000) i`)
		e.exec(t, "ANALYZE {s}.tasks")
		return e
	}
	dead := taskmill.TaskFilter{States: []taskmill.State{taskmill.StateDead}}
	day := 24 * time.Hour
	since := time.Now().Add(-11 * day)
	created := &taskmill.BulkParams{Filter: inRange(dead, taskmill.TaskFilter{CreatedSince: since, CreatedBefore: since.Add(day)})}

	for name, c := range map[string]struct {
		action  func(context.Context, *taskmill.Client) (taskmill.BulkResult, error)
		inRange string // the tasks of the range, before the action, from time $1
		most    int64  // rows of tasks that the action may read
	}{
		// 2,000 tasks created on a day 10 days back, with 36,000 after them.
		"retry created in the past": {func(ctx context.Context, c *taskmill.Client) (taskmill.BulkResult, error) {
			return c.RetryMatching(ctx, created)
		}, "created_at >= $1 AND created_at < $1 + interval '1 day'", 5 * 2000},
		"delete created in the past": {func(ctx context.Context, c *taskmill.Client) (taskmill.BulkResult, error) {
			return c.DeleteMatching(ctx, created)
		}, "created_at >= $1 AND created_at < $1 + interval '1 day'", 5 * 2000},
		// 2,000 tasks among all 80,000.
		"retry died far apart": {func(ctx context.Context, c *taskmill.Client) (taskmill.BulkResult, error) {
			return c.RetryMatching(ctx, &taskmill.BulkParams{Filter: inRange(dead, taskmill.TaskFilter{FinishedSince: since.Add(10 * day)})})
		}, "finalized_at >= $1::timestamptz + interval '10 days'", 5 * 2000},
		"delete died far apart": {func(ctx context.Context, c *taskmill.Client) (taskmill.BulkResult, error) {
			return c.DeleteMatching(ctx, &taskmill.BulkParams{Filter: inRange(dead, taskmill.TaskFilter{FinishedSince: since.Add(10 * day)})})
		}, "finalized_at >= $1::timestamptz + interval '10 days'", 5 * 2000},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			e := setup(t)
			want := idsOf(t, e, `SELECT array_agg(id) FROM {s}.tasks WHERE state = 'dead' AND `+c.inRange, since)

			var res taskmill.BulkResult
			sent := &bulkStatements{}
			n := rowsReadBy(t, e, sent, func(client *taskmill.Client) (err error) {
				res, err = c.action(t.Context(), client)
				return err
			})
			if res.Changed != 2000 || len(want) != 2000 {
				t.Fatalf("the action changed %d tasks, want the %d of the range", res.Changed, len(want))
			}
			left := e.count(t, "SELECT count(*) FROM {s}.tasks WHERE id = ANY($1) AND state = 'dead'", want)
			others := e.count(t, "SELECT count(*) FROM {s}.tasks WHERE NOT id = ANY($1) AND queue LIKE 'q%' AND state <> 'dead'", want)
			all := e.count(t, "SELECT count(*) FROM {s}.tasks WHERE NOT id = ANY($1)", want)
			if left != 0 || others != 0 || all != 78000 {
				t.Errorf("%d tasks of the range are left, %d others changed, and %d others are kept; want 0, 0, and 78,000", left, others, all)
			}
			switch statements := sent.reset(); {
			case strings.Contains(statements, "w"):
				t.Logf("a batch ran out of time on this busy machine, and the action went on in windows (%q), reading %d rows", statements, n)
			case n > c.most:
				t.Errorf("the action read %d rows of tasks, want at most %d; its batches read %q (q: by queue, p: in ID order, r and k: by kept IDs)", n, c.most, statements)
			}
		})
	}
}

// A first batch that reads a wide range queue by queue reads all of it, and
// may run out of the time of a batch without a window. The action then
// goes on in windows, in ID order, as it does after any such batch.
func TestARangeBatchThatRunsOutOfTimeGoesOnInWindows(t *testing.T) {
	t.Parallel()
	dead := taskmill.TaskFilter{States: []taskmill.State{taskmill.StateDead}}
	since := time.Now().Add(-2 * time.Hour)

	for name, c := range map[string]struct {
		params taskmill.BulkParams
		first  string // what the first batch sends: q a pick by queue, r one that reads a range of finish times
	}{
		"created":  {taskmill.BulkParams{Filter: inRange(dead, taskmill.TaskFilter{CreatedSince: since})}, "q"},
		"finished": {taskmill.BulkParams{Filter: inRange(dead, taskmill.TaskFilter{FinishedSince: since})}, "r"},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			sent := &bulkStatements{}
			e := newTracedEnv(t, sent)

			e.exec(t, `INSERT INTO {s}.tasks (queue, kind, state, created_at, finalized_at, run_at, payload, max_attempts)
			           SELECT 'q' || (i % 3), 'email.send', 'dead', now() - interval '1 hour', now() - interval '1 minute', now(), '{}', 3
			           FROM generate_series(1, 3000) i`)
			e.exec(t, "ANALYZE {s}.tasks")

			// Where a statement has a time limit, which only a batch without a
			// window sets, the retry of a task does not end before it.
			e.exec(t, `CREATE FUNCTION {s}.hold() RETURNS trigger LANGUAGE plpgsql AS '
			           BEGIN
			               IF current_setting(''statement_timeout'') <> ''0'' THEN PERFORM pg_sleep(600); END IF;
			               RETURN NEW;
			           END'`)
			e.exec(t, `CREATE TRIGGER hold_retry BEFORE UPDATE ON {s}.tasks FOR EACH ROW
			           WHEN (NEW.state = 'available') EXECUTE FUNCTION {s}.hold()`)

			got, err := e.client.RetryMatching(t.Context(), &c.params)
			if err != nil || got != (taskmill.BulkResult{Changed: 3000}) {
				t.Fatalf("retry: %+v, %v; want each of the 3,000 dead tasks retried once", got, err)
			}
			if statements := sent.reset(); !strings.HasPrefix(statements, c.first+"wp") {
				t.Errorf("the retry sent %q, want %q, and then windows", statements, c.first)
			}
		})
	}
}

// A range of finish times with more tasks than a read keeps goes on in ID
// order once the batches went through the IDs that it kept, where reading it
// again would cost more: here, the rest of the range lies among the next few
// thousand tasks. The action still changes each task of the range once, and
// no other task.
func TestADenseRangeOfFinishTimesGoesOnInIDOrder(t *testing.T) {
	t.Parallel()
	sent := &bulkStatements{}
	e := newTracedEnv(t, sent)

	// 51,000 tasks that died a minute ago, each after one that died a day ago.
	e.exec(t, `INSERT INTO {s}.tasks (queue, kind, state, created_at, finalized_at, run_at, payload, max_attempts)
	           SELECT 'q' || (i % 3), 'email.send', 'dead', now() - interval '2 days',
	                  now() - CASE WHEN i % 2 = 0 THEN interval '1 minute' ELSE interval '1 day' END, now(), '{}', 3
	           FROM generate_series(1, 102000) i`)
	e.exec(t, "ANALYZE {s}.tasks")

	dead := taskmill.TaskFilter{States: []taskmill.State{taskmill.StateDead}}
	got, err := e.client.RetryMatching(t.Context(), &taskmill.BulkParams{Filter: inRange(dead, taskmill.TaskFilter{FinishedSince: time.Now().Add(-time.Hour)})})
	if err != nil || got != (taskmill.BulkResult{Changed: 51000}) {
		t.Fatalf("retry: %+v, %v; want each of the 51,000 tasks of the range retried once", got, err)
	}
	left := e.count(t, "SELECT count(*) FROM {s}.tasks WHERE id % 2 = 0 AND state <> 'available'")
	others := e.count(t, "SELECT count(*) FROM {s}.tasks WHERE id % 2 = 1 AND state <> 'dead'")
	if left != 0 || others != 0 {
		t.Errorf("%d tasks of the range are left, and %d others changed; want 0 and 0", left, others)
	}
	statements := sent.reset()
	read, rest, _ := strings.Cut(statements, "a")
	switch {
	case strings.Contains(statements, "w"):
		t.Logf("a batch ran out of time on this busy machine, and the action went on in windows (%q)", statements)
	case read != "r"+strings.Repeat("k", 49) || rest == "" || strings.Trim(rest, "p") != "":
		t.Errorf("the retry sent %q, want a pick that reads the range, 49 picks of the IDs that it kept, the weighing of a read again, and then picks in ID order", statements)
	}
}

// A range of finish times with more tasks than a read keeps is read again
// once the batches went through the IDs that it kept, where that costs less
// than reading on in ID order. A read costs about the whole range, and the
// action weighs it once: here, the rest of the range lies among the next
// 420,000 tasks, where reading 51,000 tasks again pays off for up to 1,234
// tasks left. A weighing that runs out of its second goes on in ID order.
// The action still changes each task of the range once, and no other task.
func TestAWideRangeOfFinishTimesIsReadAgainWhereThatPaysOff(t *testing.T) {
	t.Parallel()
	for _, c := range []struct {
		name  string
		left  int    // tasks of the range among the 420,000 after the first 50,000
		held  bool   // another transaction holds the table while the action weighs
		again string // the pick after the weighing
	}{
		{"read again", 1000, false, "r"},
		{"in ID order", 2000, false, "p"},
		{"in ID order, as the weighing ran out of time", 1000, true, "p"},
	} {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			sent := &bulkStatements{}
			e := newTracedEnv(t, sent)
			if c.held {
				holdWhileWeighing(t, e, sent)
			}

			// 50,000 tasks that died a minute ago, then 420,000 tasks that
			// died a day ago, but for c.left of them, spread out.
			e.exec(t, fmt.Sprintf(`INSERT INTO {s}.tasks (queue, kind, state, created_at, finalized_at, run_at, payload, max_attempts)
			           SELECT 'q' || (i %% 3), 'email.send', 'dead', now() - interval '2 days',
			                  now() - CASE WHEN i <= 50000 OR (i - 50000) %% %d = 0 THEN interval '1 minute' ELSE interval '1 day' END, now(), '{}', 3
			           FROM generate_series(1, 470000) i`, 420000/c.left))
			e.exec(t, "ANALYZE {s}.tasks")
			inRange := fmt.Sprintf("id <= 50000 OR (id - 50000) %% %d = 0", 420000/c.left)

			dead := taskmill.TaskFilter{States: []taskmill.State{taskmill.StateDead}, FinishedSince: time.Now().Add(-time.Hour)}
			got, err := e.client.RetryMatching(t.Context(), &taskmill.BulkParams{Filter: dead})
			if want := (taskmill.BulkResult{Changed: int64(50000 + c.left)}); err != nil || got != want {
				t.Fatalf("retry: %+v, %v; want %+v, each task of the range retried once", got, err, want)
			}
			left := e.count(t, "SELECT count(*) FROM {s}.tasks WHERE ("+inRange+") AND state <> 'available'")
			others := e.count(t, "SELECT count(*) FROM {s}.tasks WHERE NOT ("+inRange+") AND state <> 'dead'")
			if left != 0 || others != 0 {
				t.Errorf("%d tasks of the range are left, and %d others changed; want 0 and 0", left, others)
			}
			switch statements, want := sent.reset(), "r"+strings.Repeat("k", 49)+"a"+c.again; {
			case strings.Contains(statements, "w"):
				t.Logf("a batch ran out of time on this busy machine, and the action went on in windows (%q)", statements)
			case !strings.HasPrefix(statements, want):
				t.Errorf("the retry sent %q, want it to start with %q", statements, want)
			}
		})
	}
}

// holdWhileWeighing locks the index of finish times, on a connection of its
// own, before the action weighs reading a range of finish times again, and
// lets it go before the next batch's transaction begins. The weighing plans
// its statement once its settings ran, and waits for the lock until it runs
// out of its time. A lock on the table would hold up the statement before
// its settings, when the connection prepares it.
func holdWhileWeighing(t *testing.T, e *env, sent *bulkStatements) {
	t.Helper()
	conn, err := pgx.Connect(t.Context(), testdb.DirectURL(t))
	if err != nil {
		t.Fatal(err)
	}
	var once sync.Once
	release := func() {
		once.Do(func() {
			if _, err := conn.Exec(context.Background(), "ROLLBACK"); err != nil {
				t.Error(err)
			}
		})
	}
	// t.Context is canceled before cleanups run.
	t.Cleanup(func() {
		release()
		_ = conn.Close(context.Background())
	})

	hold := func() {
		if _, err := conn.Exec(t.Context(), replaceSchema("BEGIN; REINDEX INDEX {s}.tasks_finished_idx", e.schema)); err != nil {
			t.Error(err)
		}
		sent.onBegin.Store(&release)
		// A weighing that waits on has no bound: it goes on, and the test fails.
		time.AfterFunc(10*time.Second, release)
	}
	sent.onWeigh.Store(&hold)
}

// idsOf returns the IDs that sql selects, read on a connection of its own
// that has ended when it returns. The database adds a connection's reads to
// the table's statistics only from time to time, or when it ends: a read on
// a connection that lives on could be counted in what rowsReadBy measures
// next, such as the scan of every task that finds a range's IDs.
func idsOf(t *testing.T, e *env, sql string, args ...any) []int64 {
	t.Helper()
	conn, err := pgx.Connect(t.Context(), testdb.DirectURL(t))
	if err != nil {
		t.Fatal(err)
	}
	pid := int64(conn.PgConn().PID())

	var ids []int64
	err = conn.QueryRow(t.Context(), replaceSchema(sql, e.schema), args...).Scan(&ids)
	_ = conn.Close(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the connection that read the IDs to end", func() bool {
		return e.count(t, "SELECT count(*) FROM pg_stat_activity WHERE pid = $1", pid) == 0
	})
	return ids
}

// rowsReadBy runs do with a client of its own, on a pool of its own that
// reaches the database directly and traces its statements with tracer, and
// returns how many rows of the tasks table the database read for it: the
// rows that its scans fetched, which the database counts once the pool's
// connections end.
func rowsReadBy(t *testing.T, e *env, tracer pgx.QueryTracer, do func(*taskmill.Client) error) int64 {
	t.Helper()
	cfg, err := pgxpool.ParseConfig(testdb.DirectURL(t))
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.Tracer = tracer
	app := e.schema + "_reads"
	cfg.ConnConfig.RuntimeParams["application_name"] = app
	pool, err := pgxpool.NewWithConfig(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	client, err := taskmill.NewClient(t.Context(), pool, &taskmill.ClientConfig{Schema: e.schema})
	if err != nil {
		t.Fatal(err)
	}

	const read = `SELECT idx_tup_fetch + seq_tup_read FROM pg_stat_user_tables WHERE schemaname = $1 AND relname = 'tasks'`
	before := e.count(t, read, e.schema)
	err = do(client)
	client.Close()
	pool.Close()
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the connections of the action to end", func() bool {
		return e.count(t, "SELECT count(*) FROM pg_stat_activity WHERE application_name = $1", app) == 0
	})
	return int64(e.count(t, read, e.schema) - before)
}

// errOf returns the error of a call that returns a value and an error.
func errOf[T any](_ T, err error) error { return err }

// History is split in a partition per day. A batch of a delete of completed
// tasks reads only the partitions from the first day on which its tasks
// finished to the last, when it locks them and when it deletes them:
// PostgreSQL 15 and 16 search the index of every partition that a statement
// reads once for each ID of the batch.
func TestDeleteMatchingReadsOnlyThePartitionsOfItsBatch(t *testing.T) {
	t.Parallel()
	// 300 completed tasks in each of the 7 days that have partitions, in the
	// order of their IDs: a batch of 1,000 holds the tasks of 4 days. If its
	// first task finished 5 days later, as a retried task can, the batch
	// spans 6 days.
	cases := map[string]struct {
		late int // days that the first task finished after the others of its day
		want int // the partitions from the batch's first finish day to its last
	}{
		"finish days follow the IDs": {0, 4},
		"one task finished later":    {5, 6},
	}

	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			e := newEnv(t)
			e.exec(t, `INSERT INTO {s}.task_history (id, finalized_at, queue, created_at, kind, run_at, attempted_by, attempted_at,
			                                         priority, attempt, max_attempts, payload, metadata, errors)
			           SELECT i, now() + ((i - 1) / 300 + CASE WHEN i = 1 THEN $1 ELSE 0 END) * interval '1 day', 'default', now(),
			                  'email.send', now(), 'w', now(), 2, 1, 3, '{}', '{}', '[]'
			           FROM generate_series(1, 2100) i`, c.late)
			e.exec(t, "ANALYZE {s}.task_history")

			picked, deleted, err := taskmill.HistoryPartitionsRead(t.Context(), e.client, &taskmill.TaskFilter{Kinds: []string{"email.send"}})
			if err != nil {
				t.Fatal(err)
			}
			if picked > c.want || deleted > c.want {
				t.Fatalf("the batch locked its tasks in %d partitions, and deleted them in %d; want at most %d", picked, deleted, c.want)
			}
		})
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

// A transaction that sent a notification waits at its commit for the lock
// that every notifying transaction of the server takes there. The lock
// timeout of a bulk action's batch, or of a schedule's run, is for its
// statements: at the commit it would fail a transaction whose work is done.
// That lock belongs to the whole server, so the test does not hold it: a
// deferred trigger runs inside each commit, and refuses one that still has a
// lock timeout.
func TestALockTimeoutEndsBeforeTheCommit(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ctx := t.Context()

	e.exec(t, `
CREATE FUNCTION {s}.commit_without_lock_timeout() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF current_setting('lock_timeout') <> '0' THEN
        RAISE EXCEPTION 'the commit has a lock timeout of %', current_setting('lock_timeout');
    END IF;
    RETURN NULL;
END $$`)
	e.exec(t, `
CREATE CONSTRAINT TRIGGER commit_without_lock_timeout AFTER INSERT OR UPDATE OR DELETE ON {s}.tasks
DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION {s}.commit_without_lock_timeout()`)

	e.enqueue(t, sendEmail{}, &taskmill.EnqueueOptions{GroupKey: "g"})
	e.enqueue(t, sendEmail{}, &taskmill.EnqueueOptions{GroupKey: "g"})

	all := bulk(&taskmill.TaskFilter{Queues: []string{taskmill.DefaultQueue}})
	for _, step := range []struct {
		name string
		do   func(context.Context, *taskmill.BulkParams) (taskmill.BulkResult, error)
	}{
		{"cancel", e.client.CancelMatching},
		{"retry", e.client.RetryMatching},
		{"delete", e.client.DeleteMatching},
	} {
		if res, err := step.do(ctx, all); err != nil || res.Changed != 2 {
			t.Fatalf("%s: %d tasks, %v; want 2", step.name, res.Changed, err)
		}
	}

	// The first pass stores the schedule, and the second enqueues its run.
	w := newWorker(t, e, &taskmill.WorkerConfig{Schedules: []taskmill.Schedule{{Name: "digest", Cron: "@hourly", Args: sendEmail{}}}})
	taskmill.RunSchedules(ctx, w)
	e.exec(t, "UPDATE {s}.schedules SET next_run_at = date_trunc('second', now()) - interval '1 second'")
	taskmill.RunSchedules(ctx, w)

	if n := e.count(t, "SELECT count(*) FROM {s}.tasks"); n != 1 {
		t.Errorf("the schedule's run enqueued %d tasks, want 1", n)
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
	g := taskmill.TaskFilter{GroupKeys: []string{"g"}}
	canceled := taskmill.TaskFilter{GroupKeys: []string{"g"}, States: []taskmill.State{taskmill.StateCanceled}}

	for name, params := range map[string]taskmill.BulkParams{
		"without a range":            {Filter: g},
		"in a range of finish times": {Filter: inRange(canceled, taskmill.TaskFilter{FinishedSince: time.Now().Add(-time.Hour)})},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			retryKeepsTheOrderOfGroups(t, &params)
		})
	}
}

// retryKeepsTheOrderOfGroups checks that retries with params, which select
// the canceled tasks of group g, keep the group in order.
func retryKeepsTheOrderOfGroups(t *testing.T, params *taskmill.BulkParams) {
	t.Helper()
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
	if _, err := e.client.CancelMatching(ctx, bulk(&taskmill.TaskFilter{GroupKeys: []string{"g"}})); err != nil {
		t.Fatal(err)
	}

	tx := e.begin(t)
	e.txExec(t, tx, "SELECT FROM {s}.tasks WHERE id = $1 FOR UPDATE", res[0].ID)

	got, err := e.client.RetryMatching(ctx, params)
	if err != nil || got.Changed != 0 || got.Skipped != 1500 {
		t.Fatalf("with the oldest task held: %+v, %v; want the whole group left", got, err)
	}

	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}

	got, err = e.client.RetryMatching(ctx, params)
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

	// An open enqueue into the group holds the group's row. The transaction
	// also holds the row of the dedupe key, as an enqueue does that claims a
	// key whose window has ended.
	if _, err := e.client.EnqueueTx(ctx, taskmill.Pgx(tx), sendEmail{}, &taskmill.EnqueueOptions{GroupKey: "g"}); err != nil {
		t.Fatal(err)
	}
	e.txExec(t, tx, "SELECT FROM {s}.dedupe_keys FOR UPDATE")

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

// A bulk retry gives canceled tasks their dedupe keys back, as Retry does. It
// leaves the tasks whose keys other tasks hold, and counts them apart from
// the tasks that it skipped: running the action again does not help them.
func TestRetryMatchingLeavesTasksWhoseDedupeKeyIsTaken(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ctx := t.Context()

	key := func(k, group string) *taskmill.EnqueueOptions {
		return &taskmill.EnqueueOptions{DedupeKey: k, GroupKey: group}
	}
	free := e.keyed(t, key("free", "")).ID
	e.cancel(t, free)
	plain := e.enqueue(t, sendEmail{}, nil)
	e.cancel(t, plain)
	taken := e.keyed(t, key("taken", "")).ID
	e.cancel(t, taken)
	holder := e.keyed(t, key("taken", "")).ID

	// Two canceled tasks gave the same key away: the older one gets it back.
	older := e.keyed(t, key("twice", "")).ID
	e.cancel(t, older)
	newer := e.keyed(t, key("twice", "")).ID
	e.cancel(t, newer)

	// A task that is left for its key does not keep its group's later tasks
	// from their retry: it is not waiting for another run.
	head := e.keyed(t, key("in-group", "g")).ID
	e.cancel(t, head)
	inGroup := e.keyed(t, key("in-group", "")).ID
	later := e.enqueue(t, sendEmail{}, &taskmill.EnqueueOptions{GroupKey: "g"})
	e.cancel(t, later)

	canceled := bulk(&taskmill.TaskFilter{States: []taskmill.State{taskmill.StateCanceled}})
	want := taskmill.BulkResult{Changed: 4, Conflicts: 3}
	if res, err := e.client.RetryMatching(ctx, canceled); err != nil || res != want {
		t.Fatalf("retried %+v, %v; want %+v", res, err, want)
	}
	wantStates(t, e, map[int64]taskmill.State{
		free: taskmill.StateAvailable, plain: taskmill.StateAvailable, older: taskmill.StateAvailable,
		later: taskmill.StateAvailable, taken: taskmill.StateCanceled, newer: taskmill.StateCanceled,
		head: taskmill.StateCanceled, holder: taskmill.StateAvailable, inGroup: taskmill.StateAvailable,
	})

	for k, id := range map[string]int64{"free": free, "taken": holder, "twice": older, "in-group": inGroup} {
		if dup := e.keyed(t, key(k, "")); !dup.Duplicate || dup.ID != id {
			t.Errorf("key %q: got %+v, want a duplicate of task %d", k, dup, id)
		}
	}

	// The next run leaves them again, until the keys are free.
	want = taskmill.BulkResult{Conflicts: 3}
	if res, err := e.client.RetryMatching(ctx, canceled); err != nil || res != want {
		t.Fatalf("second run: %+v, %v; want %+v", res, err, want)
	}

	for _, id := range []int64{holder, older, inGroup} {
		if err := e.client.Delete(ctx, id); err != nil {
			t.Fatal(err)
		}
	}

	want = taskmill.BulkResult{Changed: 3}
	if res, err := e.client.RetryMatching(ctx, canceled); err != nil || res != want {
		t.Fatalf("once the keys are free: %+v, %v; want %+v", res, err, want)
	}
}

// A retry that must take a dedupe key back, while an open transaction claims
// the same key, waits for it only briefly, as it does for a new group, and
// leaves the tasks that need keys for a later run. It never deadlocks with
// the transaction.
func TestRetryWaitsBrieflyForADedupeKey(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second) // a long wait would end it
	defer cancel()

	opts := &taskmill.EnqueueOptions{DedupeKey: "k"}
	keyed := e.keyed(t, opts).ID
	e.cancel(t, keyed)
	plain := e.enqueue(t, sendEmail{}, nil)
	e.cancel(t, plain)

	tx := e.begin(t)
	if _, err := e.client.EnqueueTx(ctx, taskmill.Pgx(tx), sendEmail{}, opts); err != nil {
		t.Fatal(err)
	}

	canceled := bulk(&taskmill.TaskFilter{States: []taskmill.State{taskmill.StateCanceled}})
	res, err := e.client.RetryMatching(ctx, canceled)
	if err != nil || res.Changed != 1 || res.Skipped != 1 || res.Conflicts != 0 {
		t.Fatalf("retried %+v, %v; want the task without a key, and the other left", res, err)
	}
	wantStates(t, e, map[int64]taskmill.State{plain: taskmill.StateAvailable, keyed: taskmill.StateCanceled})

	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}

	if res, err := e.client.RetryMatching(ctx, canceled); err != nil || res.Changed != 1 {
		t.Fatalf("retried %+v once released, %v; want 1 task", res, err)
	}
	if dup := e.keyed(t, opts); !dup.Duplicate || dup.ID != keyed {
		t.Fatalf("after the retry: got %+v, want a duplicate of task %d", dup, keyed)
	}
}

// A retry that leaves a task of a group leaves the group's later tasks, not
// the earlier ones. An earlier task that takes its dedupe key back is
// retried with it: no task is left canceled with a key.
func TestRetryTakesTheKeyOfATaskAheadOfOneItLeaves(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	opts := &taskmill.EnqueueOptions{DedupeKey: "k", GroupKey: "g"}
	ahead := e.keyed(t, opts).ID
	held := e.enqueue(t, sendEmail{}, &taskmill.EnqueueOptions{GroupKey: "g"})
	behind := e.enqueue(t, sendEmail{}, &taskmill.EnqueueOptions{GroupKey: "g"})

	for _, id := range []int64{behind, held, ahead} {
		e.cancel(t, id)
	}

	tx := e.begin(t)
	e.txExec(t, tx, "SELECT FROM {s}.tasks WHERE id = $1 FOR UPDATE", held)

	canceled := bulk(&taskmill.TaskFilter{States: []taskmill.State{taskmill.StateCanceled}})
	if res, err := e.client.RetryMatching(ctx, canceled); err != nil || res.Changed != 1 || res.Skipped != 2 {
		t.Fatalf("retried %+v, %v; want the task ahead retried, and the held task and the one behind it left", res, err)
	}
	wantStates(t, e, map[int64]taskmill.State{
		ahead: taskmill.StateAvailable, held: taskmill.StateCanceled, behind: taskmill.StateCanceled,
	})
	if dup := e.keyed(t, opts); !dup.Duplicate || dup.ID != ahead {
		t.Fatalf("after the retry: got %+v, want a duplicate of task %d", dup, ahead)
	}

	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
}

// A task that a retry leaves after all, because it cannot join its group
// now, stays canceled, so it must not keep the key that the retry took back
// for it: a canceled task holds no key.
func TestRetryLeavesNoKeyWithATaskItLeaves(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	opts := &taskmill.EnqueueOptions{DedupeKey: "k"}
	left := e.keyed(t, &taskmill.EnqueueOptions{DedupeKey: "k", GroupKey: "g"}).ID
	e.cancel(t, left)

	// An open enqueue into the group creates the group's row.
	tx := e.begin(t)
	if _, err := e.client.EnqueueTx(ctx, taskmill.Pgx(tx), sendEmail{}, &taskmill.EnqueueOptions{GroupKey: "g"}); err != nil {
		t.Fatal(err)
	}

	canceled := bulk(&taskmill.TaskFilter{States: []taskmill.State{taskmill.StateCanceled}})
	if res, err := e.client.RetryMatching(ctx, canceled); err != nil || res.Changed != 0 || res.Skipped != 1 {
		t.Fatalf("retried %+v, %v; want the task left", res, err)
	}
	e.wantState(t, left, taskmill.StateCanceled)
	other := e.keyed(t, opts)
	if other.Duplicate {
		t.Fatalf("the task that the retry left holds its key: %+v", other)
	}

	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}

	// The task still knows its key: its retry is refused while the new task
	// holds it.
	want := taskmill.BulkResult{Conflicts: 1}
	if res, err := e.client.RetryMatching(ctx, canceled); err != nil || res != want {
		t.Fatalf("while another task holds the key: %+v, %v; want %+v", res, err, want)
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

	res, err := e.client.RetryMatching(ctx, bulk(&taskmill.TaskFilter{States: []taskmill.State{taskmill.StateCanceled}}))
	if err != nil || res.Changed != 1 || res.Skipped != 1 {
		t.Fatalf("retried %+v, %v; want the one of group a, and group b left", res, err)
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

// A batch that began ends whole when the context ends meanwhile, so that it
// holds no rows once the action returned, and the result counts what it did.
func TestBulkBatchEndsWholeWhenTheContextEnds(t *testing.T) {
	t.Parallel()
	e := newEnv(t)

	// As above: the retry of group b's task waits, for up to its lock
	// timeout, for the transaction that creates the group's row.
	head := e.enqueue(t, sendEmail{}, &taskmill.EnqueueOptions{GroupKey: "a"})
	inA := e.enqueue(t, sendEmail{}, &taskmill.EnqueueOptions{GroupKey: "a"})
	inB := e.enqueue(t, sendEmail{}, &taskmill.EnqueueOptions{GroupKey: "b"})
	e.cancel(t, inA)
	e.cancel(t, inB)

	tx := e.begin(t)
	if _, err := e.client.EnqueueTx(t.Context(), taskmill.Pgx(tx), sendEmail{}, &taskmill.EnqueueOptions{GroupKey: "b"}); err != nil {
		t.Fatal(err)
	}

	waiting := func(ctx context.Context) bool {
		var waiting bool
		_ = e.pool.QueryRow(ctx, `SELECT EXISTS (SELECT FROM pg_stat_activity WHERE wait_event_type = 'Lock' AND strpos(query, $1) > 0)`,
			e.schema+".groups").Scan(&waiting)
		return waiting
	}

	// The context ends once the batch waits. A run whose wait ended before
	// that was seen goes again: it waits again for group b.
	var changed int64
	for range 20 {
		ctx, cancel := context.WithCancel(t.Context())
		go func() {
			for ctx.Err() == nil {
				if waiting(ctx) {
					cancel()
					return
				}
				time.Sleep(time.Millisecond)
			}
		}()

		res, err := e.client.RetryMatching(ctx, bulk(&taskmill.TaskFilter{States: []taskmill.State{taskmill.StateCanceled}}))
		ended := ctx.Err() != nil
		cancel()
		changed += res.Changed
		if !ended {
			continue
		}

		if err != nil || res.Skipped != 1 || changed != 1 || waiting(t.Context()) {
			t.Fatalf("retried %+v, %v, and %d tasks in all; want the batch to end whole after the context ended: group a retried, group b left", res, err, changed)
		}
		wantStates(t, e, map[int64]taskmill.State{head: taskmill.StateAvailable, inA: taskmill.StatePending, inB: taskmill.StateCanceled})
		return
	}
	t.Fatal("the context never ended while the batch waited")
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
// skipped: a FIFO group that it left comes back in order. A range of finish
// times keeps the order of IDs, also where the group's later tasks died
// first.
func TestRetryGoesOnInGroupOrder(t *testing.T) {
	t.Parallel()
	all := taskmill.TaskFilter{States: []taskmill.State{taskmill.StateCanceled}}

	for name, params := range map[string]taskmill.BulkParams{
		"without a range":            {Filter: all},
		"in a range of finish times": {Filter: inRange(all, taskmill.TaskFilter{FinishedSince: time.Now().Add(-time.Hour)})},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			sent := &bulkStatements{}
			e := newTracedEnv(t, sent)

			canceled := func(n int, group string, ago time.Duration) {
				e.exec(t, `INSERT INTO {s}.tasks (queue, run_at, kind, finalized_at, state, cancel_requested_at, payload, max_attempts, group_key)
				           SELECT 'default', now(), 'email.send', now() - $3::interval, 'canceled', now(), '{}', 3, nullif($1, '')
				           FROM generate_series(1, $2)`, group, n, ago)
			}

			// The oldest task of the group, two batches of other tasks, and the
			// rest of the group, which is more than a batch, and died first.
			canceled(1, "g", time.Minute)
			oldest := int64(e.count(t, "SELECT max(id) FROM {s}.tasks"))
			canceled(2000, "", 2*time.Minute)
			canceled(1500, "g", 3*time.Minute)

			// The first run meets the oldest task held, and leaves the group.
			// Its context ends when its second batch picks its tasks, as a
			// click that runs out of time: it stops after that batch, short of
			// the group's rest.
			tx := e.begin(t)
			e.txExec(t, tx, "SELECT FROM {s}.tasks WHERE id = $1 FOR UPDATE", oldest)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			sent.atBatch(2, cancel)
			first, err := e.client.RetryMatching(ctx, &params)
			if !errors.Is(err, context.Canceled) || first.Changed != 1999 || first.Next == "" {
				t.Fatalf("first run: %+v, %v; want it to stop after two batches, with a cursor", first, err)
			}

			if err := tx.Rollback(t.Context()); err != nil {
				t.Fatal(err)
			}

			// The first run's last batch ended before it returned, so the rest
			// skips none of its rows.
			params.Cursor = first.Next
			rest, err := e.client.RetryMatching(t.Context(), &params)
			if err != nil || first.Changed+rest.Changed != 3501 || rest.Skipped != 0 || rest.Next != "" {
				t.Fatalf("then %+v after %+v, %v; want every task retried", rest, first, err)
			}
			e.wantHead(t, "g", int(oldest))
		})
	}
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
	           FROM generate_series(1, 30000) i`)

	// The first 28,000 went through earlier batches: none of the actions
	// matches them any more.
	e.exec(t, `UPDATE {s}.tasks SET state = 'running', attempt = 1, lease_expires_at = now() + interval '1 hour',
	           finalized_at = NULL, cancel_requested_at = now() WHERE id <= 28000`)

	e.exec(t, `INSERT INTO {s}.task_history (id, finalized_at, queue, created_at, kind, run_at, attempted_by, attempted_at,
	                                         priority, attempt, max_attempts, payload, metadata, errors)
	           SELECT 1000000 + i, now(), CASE WHEN i > 90000 THEN 'q' || (i % 2) ELSE 'q0' END, now(), 'k', now(), 'w', now(),
	                  2, 1, 3, '{}', '{}', '[]'
	           FROM generate_series(1, 100000) i`)

	e.exec(t, "ANALYZE {s}.tasks")
	e.exec(t, "ANALYZE {s}.task_history")

	// A batch reads the rows after its position at most, and those it picks
	// once more to lock them: 2,000 tasks follow the position and 28,000
	// precede it, and 10,000 completed tasks follow and 90,000 precede. The
	// history is that large because with a fifth of it, the planner finds
	// another plan that reads little for a batch that ignores its position.
	for _, c := range []struct {
		action string
		filter *taskmill.TaskFilter
		after  int64
		most   int64
	}{
		{"retry", &taskmill.TaskFilter{Queues: []string{"q1"}}, 28000, 4000},
		{"cancel", &taskmill.TaskFilter{Queues: []string{"q1"}}, 28000, 4000},
		{"delete", &taskmill.TaskFilter{Queues: []string{"q1"}}, 28000, 4000},
		{"delete", &taskmill.TaskFilter{Kinds: []string{"k3"}}, 28000, 4000},
		{"history", &taskmill.TaskFilter{Queues: []string{"q1"}}, 1090000, 15000},
	} {
		n, err := taskmill.BulkRowsRead(ctx, e.client, c.action, bulk(c.filter), c.after, false)
		if err != nil {
			t.Fatal(err)
		}
		if n > c.most {
			t.Errorf("%s %+v after %d: a batch read %d rows, want at most %d", c.action, *c.filter, c.after, n, c.most)
		}
	}

	e.rawGroups = true
}

// bulkStatements keeps the statements of bulk actions that a pool's
// connections send, in their order: a p for the pick of a batch, a q for a
// pick that reads a range queue by queue, an r for one that reads a range of
// finish times and keeps its IDs, a k for a pick of those IDs, an a for the
// statement that weighs reading such a range again, and a w for the search
// for a window.
type bulkStatements struct {
	mu   sync.Mutex
	sent []byte
	// Each is called once: before the next transaction begins, before the
	// pick of the next batch, and before the next weighing of a read again.
	onBegin, onBatch, onWeigh atomic.Pointer[func()]
	// again is set while the action runs a batch again: its transaction was
	// rolled back, and its next pick is of the same batch.
	again atomic.Bool
}

func (s *bulkStatements) TraceQueryStart(ctx context.Context, _ *pgx.Conn, d pgx.TraceQueryStartData) context.Context {
	on := &s.onBegin
	switch {
	case strings.Contains(d.SQL, "candidates AS ("):
		mark := byte('p')
		switch {
		case strings.Contains(d.SQL, "count(*) OVER ()"):
			mark = 'q' // a pick that reads a range queue by queue
		case strings.Contains(d.SQL, "kept AS MATERIALIZED"):
			mark = 'r' // a pick that reads a range of finish times
		case strings.Contains(d.SQL, "= ANY($13::bigint[])"):
			mark = 'k' // a pick of kept IDs
		}
		s.add(mark)
		if s.again.Swap(false) {
			return ctx
		}
		on = &s.onBatch
	case d.SQL == "rollback":
		s.again.Store(true)
		return ctx
	case d.SQL != "begin":
		return ctx
	}

	if f := on.Swap(nil); f != nil {
		(*f)()
	}
	return ctx
}

func (*bulkStatements) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

func (s *bulkStatements) TraceBatchStart(ctx context.Context, _ *pgx.Conn, d pgx.TraceBatchStartData) context.Context {
	for _, q := range d.Batch.QueuedQueries {
		switch {
		case strings.Contains(q.SQL, "SELECT least("):
			s.add('w')
		case strings.Contains(q.SQL, "AS most"):
			s.add('a')
			if f := s.onWeigh.Swap(nil); f != nil {
				(*f)()
			}
		}
	}
	return ctx
}

func (*bulkStatements) TraceBatchQuery(context.Context, *pgx.Conn, pgx.TraceBatchQueryData) {}

func (s *bulkStatements) add(statement byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sent = append(s.sent, statement)
}

func (*bulkStatements) TraceBatchEnd(context.Context, *pgx.Conn, pgx.TraceBatchEndData) {}

// atBatch calls f once, when the nth batch from now picks its tasks. A batch
// that the action runs again counts once: on a slow database, a statement of
// a batch without a window may not end within its second, and the action
// then runs the batch again in a window, with a second pick.
func (s *bulkStatements) atBatch(n int, f func()) {
	var next func()
	next = func() {
		if n--; n > 0 {
			s.onBatch.Store(&next)
			return
		}
		f()
	}

	s.again.Store(false)
	s.onBatch.Store(&next)
}

// reset returns the statements since the last reset.
func (s *bulkStatements) reset() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	sent := string(s.sent)
	s.sent = nil
	return sent
}

// A batch has its time from when its transaction began: what the action
// waited for that, as for a connection of a busy pool, is not taken off it.
func TestBulkBatchTimeStartsWhenItsTransactionBegan(t *testing.T) {
	t.Parallel()
	sent := &bulkStatements{}
	e := newTracedEnv(t, sent)

	for range 10 {
		e.die(t, e.enqueue(t, sendEmail{}, nil))
	}

	// The batch's transaction begins only after more than a batch's time.
	wait := func() { time.Sleep(taskmill.BatchTimeout + 200*time.Millisecond) }
	sent.onBegin.Store(&wait)

	res, err := e.client.RetryMatching(t.Context(), bulk(&taskmill.TaskFilter{States: []taskmill.State{taskmill.StateDead}}))
	if err != nil || res != (taskmill.BulkResult{Changed: 10}) {
		t.Fatalf("retried %+v, %v; want 10 tasks retried", res, err)
	}
}

// A bulk action whose filter an index serves reads the tasks that match,
// however many other tasks the tables hold: a batch reads them through the
// index, and the action starts with a batch, not with a window.
func TestBulkActionsWithAnIndexReadOnlyTheirTasks(t *testing.T) {
	t.Parallel()
	sent := &bulkStatements{}
	e := newTracedEnv(t, sent)
	ctx := t.Context()

	// 30,000 tasks and as many completed ones. Every 500th of each is of
	// the queue q, 60 of each, and those not completed are dead.
	e.exec(t, `INSERT INTO {s}.tasks (queue, kind, state, run_at, payload, max_attempts, finalized_at)
	           SELECT CASE WHEN i % 500 = 0 THEN 'q' ELSE 'other' END, 'email.send',
	                  CASE WHEN i % 500 = 0 THEN 'dead' ELSE 'available' END::{s}.task_state, now(), '{}', 3,
	                  CASE WHEN i % 500 = 0 THEN now() END
	           FROM generate_series(1, 30000) i`)
	e.exec(t, `INSERT INTO {s}.task_history (id, finalized_at, queue, created_at, kind, run_at, attempted_by, attempted_at,
	                                         priority, attempt, max_attempts, payload, metadata, errors)
	           SELECT 1000000 + i, now(), CASE WHEN i % 500 = 0 THEN 'q' ELSE 'other' END, now(), 'email.send', now(), 'w', now(),
	                  2, 1, 3, '{}', '{}', '[]'
	           FROM generate_series(1, 30000) i`)

	e.exec(t, "ANALYZE {s}.tasks")
	e.exec(t, "ANALYZE {s}.task_history")

	q := []string{"q"}
	for _, c := range []struct {
		pick   string
		do     func(context.Context, *taskmill.BulkParams) (taskmill.BulkResult, error)
		filter taskmill.TaskFilter
	}{
		{"retry", e.client.RetryMatching, taskmill.TaskFilter{Queues: q}},
		{"history", e.client.DeleteMatching, taskmill.TaskFilter{Queues: q, States: []taskmill.State{taskmill.StateCompleted}}},
	} {
		// A batch reads each task that matches once to pick it, and once to
		// lock it.
		read, err := taskmill.BulkRowsRead(ctx, e.client, c.pick, bulk(&c.filter), 0, false)
		if err != nil {
			t.Fatal(err)
		}
		if read > 200 {
			t.Errorf("%s %+v: the batch read %d rows for 60 tasks", c.pick, c.filter, read)
		}

		sent.reset()
		res, err := c.do(ctx, &taskmill.BulkParams{Filter: c.filter})
		if err != nil || res != (taskmill.BulkResult{Changed: 60}) {
			t.Fatalf("%s %+v: %+v, %v; want 60 tasks changed", c.pick, c.filter, res, err)
		}
		// A window follows only a batch that a busy database did not end
		// in time.
		if got := sent.reset(); !strings.HasPrefix(got, "p") {
			t.Errorf("%s %+v: the action sent %q, want a batch before any window", c.pick, c.filter, got)
		}
	}
}

// A batch that cannot end in time, as when too many tasks lie before those
// that match, gives way: the action goes on in windows of 100,000 tasks, in
// which every batch moves its position, also if its context ended meanwhile.
// It still acts on each matching task once: where a batch ends with its
// window, and when a task matches again behind the action.
func TestBulkActionsGetPastASlowBatchInWindows(t *testing.T) {
	t.Parallel()
	sent := &bulkStatements{}
	e := newTracedEnv(t, sent)
	ctx := t.Context()

	// Tasks 1 to 250,000. The dead tasks of the queue q are every 1,000th
	// of the first 100,000, and every fifth from 120,005 to 240,000: 24,100
	// in all. So the first two windows each end with one of them, tasks
	// 100,000 and 200,000. Every 500th task is of a rare kind, 500 in all.
	e.exec(t, `INSERT INTO {s}.tasks (queue, kind, state, run_at, payload, max_attempts, finalized_at)
	           SELECT CASE WHEN dead THEN 'q' ELSE 'other' END, CASE WHEN i % 500 = 0 THEN 'rare' ELSE 'email.send' END,
	                  CASE WHEN dead THEN 'dead' ELSE 'available' END::{s}.task_state, now(), '{}', 3,
	                  CASE WHEN dead THEN now() END
	           FROM (SELECT i, i <= 100000 AND i % 1000 = 0 OR i > 120000 AND i <= 240000 AND i % 5 = 0 AS dead
	                 FROM generate_series(1, 250000) i) x`)

	// 120,000 completed tasks after them, 120 of them of the rare kind.
	e.exec(t, `INSERT INTO {s}.task_history (id, finalized_at, queue, created_at, kind, run_at, attempted_by, attempted_at,
	                                         priority, attempt, max_attempts, payload, metadata, errors)
	           SELECT 1000000 + i, now(), 'other', now(), CASE WHEN i % 1000 = 0 THEN 'rare' ELSE 'email.send' END, now(), 'w', now(),
	                  2, 1, 3, '{}', '{}', '[]'
	           FROM generate_series(1, 120000) i`)

	e.exec(t, "ANALYZE {s}.tasks")
	e.exec(t, "ANALYZE {s}.task_history")

	q := taskmill.TaskFilter{Queues: []string{"q"}}
	rare := taskmill.TaskFilter{Kinds: []string{"rare"}}

	// A batch in a window reads the window at most, and the tasks it picks
	// once more to lock them. Finding the window reads its own entries of
	// each table's index.
	for _, c := range []struct {
		action string
		filter taskmill.TaskFilter
	}{
		{"cancel", q},
		{"delete", q},
		{"delete", rare},
	} {
		n, err := taskmill.BulkRowsRead(ctx, e.client, c.action, bulk(&c.filter), 0, true)
		if err != nil {
			t.Fatal(err)
		}
		if n > 101_000 {
			t.Errorf("%s %+v: a batch in a window read %d rows, want at most a window of 100,000 and a batch", c.action, c.filter, n)
		}
	}

	for _, generic := range []bool{false, true} {
		n, err := taskmill.WindowRowsRead(ctx, e.client, 0, generic)
		if err != nil {
			t.Fatal(err)
		}
		if n > 200_000 {
			t.Errorf("generic plan %t: finding a window read %d rows, want at most 100,000 of each table", generic, n)
		}
	}

	// A retried task dies again at once, so it matches again. Where a
	// statement has a time limit, which only a batch without a window sets,
	// the statement that changes a task does not end before it.
	e.exec(t, `CREATE FUNCTION {s}.hold() RETURNS trigger LANGUAGE plpgsql AS '
	           BEGIN
	               IF current_setting(''statement_timeout'') <> ''0'' THEN PERFORM pg_sleep(600); END IF;
	               IF TG_OP = ''DELETE'' THEN RETURN OLD; END IF;
	               NEW.state := ''dead'';
	               NEW.finalized_at := now();
	               RETURN NEW;
	           END'`)
	e.exec(t, `CREATE TRIGGER hold_retry BEFORE UPDATE ON {s}.tasks FOR EACH ROW
	           WHEN (NEW.state = 'available') EXECUTE FUNCTION {s}.hold()`)
	e.exec(t, "CREATE TRIGGER hold_delete BEFORE DELETE ON {s}.tasks FOR EACH ROW EXECUTE FUNCTION {s}.hold()")

	// The retry goes through three windows: the first has 100 of the tasks,
	// the second ends with its 16th full batch, and the third ends at the
	// last task.
	sent.reset()
	got, err := e.client.RetryMatching(ctx, &taskmill.BulkParams{Filter: q})
	if err != nil || got != (taskmill.BulkResult{Changed: 24100}) {
		t.Fatalf("retry: %+v, %v; want each of the 24,100 dead tasks retried once", got, err)
	}
	if statements := sent.reset(); strings.Count(statements, "w") != 3 {
		t.Errorf("the retry sent %q, want 3 windows", statements)
	}

	// The context of a delete ends when its first batch picks its tasks.
	// The action still goes through one window, which has 200 of the 500
	// tasks and none of the 120 completed ones, and a second run does the
	// rest.
	stopped, stop := context.WithCancel(ctx)
	defer stop()
	sent.atBatch(1, stop)
	got, err = e.client.DeleteMatching(stopped, &taskmill.BulkParams{Filter: rare})
	if !errors.Is(err, context.Canceled) || got.Changed != 200 || got.Next == "" {
		t.Fatalf("delete: %+v, %v; want 200 tasks deleted in one window after the context ended", got, err)
	}
	if statements := sent.reset(); strings.Count(statements, "w") != 1 {
		t.Errorf("the delete sent %q, want 1 window", statements)
	}

	rest, err := e.client.DeleteMatching(ctx, &taskmill.BulkParams{Filter: rare, Cursor: got.Next})
	if err != nil || rest != (taskmill.BulkResult{Changed: 420}) {
		t.Fatalf("the rest of the delete: %+v, %v; want the other 300 tasks and the 120 completed ones deleted", rest, err)
	}

	left := e.count(t, `SELECT (SELECT count(*) FROM {s}.tasks WHERE kind = 'rare')
	                         + (SELECT count(*) FROM {s}.task_history WHERE kind = 'rare')`)
	all := e.count(t, "SELECT (SELECT count(*) FROM {s}.tasks) + (SELECT count(*) FROM {s}.task_history)")
	if left != 0 || all != 250000-500+120000-120 {
		t.Fatalf("%d tasks of the kind are left, of %d tasks; want none, and every other task kept", left, all)
	}
}

// A bulk action that ctx ends midway leaves the tasks and their FIFO groups
// consistent, and says exactly how many tasks it changed.
func TestBulkActionsStopMidway(t *testing.T) {
	t.Parallel()
	sent := &bulkStatements{}
	e := newTracedEnv(t, sent)

	// Three batches of tasks, in ten groups that each lie across all three.
	params := make([]taskmill.EnqueueParams, 3000)
	for i := range params {
		params[i] = taskmill.EnqueueParams{Args: sendEmail{}, Options: &taskmill.EnqueueOptions{GroupKey: fmt.Sprint("g", i%10)}}
	}
	if _, err := e.client.EnqueueMany(t.Context(), params); err != nil {
		t.Fatal(err)
	}

	// The action's context ends when its second batch picks its tasks, as a
	// request's does when it runs out of time: the action stops after that
	// batch.
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	sent.atBatch(2, cancel)
	all := &taskmill.TaskFilter{Queues: []string{taskmill.DefaultQueue}}
	res, err := e.client.CancelMatching(ctx, bulk(all))
	if !errors.Is(err, context.Canceled) || res.Changed == 0 || res.Changed >= 3000 || res.Next == "" {
		t.Fatalf("canceled %+v, %v; want it to stop midway", res, err)
	}
	if canceled := e.states(t)["canceled"]; canceled != int(res.Changed) {
		t.Fatalf("said it canceled %d tasks, but %d are", res.Changed, canceled)
	}
	e.checkGroups(t)

	// It goes on where it stopped.
	rest, err := e.client.CancelMatching(t.Context(), &taskmill.BulkParams{Filter: *all, Cursor: res.Next})
	if err != nil || res.Changed+rest.Changed != 3000 || rest.Next != "" {
		t.Fatalf("canceled %d, then %d tasks, %v; want 3,000 in all", res.Changed, rest.Changed, err)
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

	if _, err := e.client.Count(t.Context(), &taskmill.CountParams{Filter: taskmill.TaskFilter{States: []taskmill.State{"lost"}}}); !errors.Is(err, taskmill.ErrInvalidArgument) {
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
	want := &taskmill.CountResult{
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

	got, err = e.client.Count(ctx, &taskmill.CountParams{Filter: taskmill.TaskFilter{
		Kinds: []string{"email.send"}, States: []taskmill.State{taskmill.StateAvailable, taskmill.StateCompleted},
	}})
	want = &taskmill.CountResult{
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
	got, err = e.client.Count(ctx, &taskmill.CountParams{Filter: taskmill.TaskFilter{Queues: []string{"big"}}})
	capped := taskmill.TaskCount{N: 10000, Capped: true}
	if err != nil || got.States[taskmill.StateAvailable] != capped || got.Cancel != capped || got.Retry != n(1) ||
		got.Delete != (taskmill.TaskCount{N: 10001, Capped: true}) {
		t.Fatalf("over the cap: %+v, %v", got, err)
	}

	// Cancel leaves out the running tasks already asked to stop, here every
	// third of 10,001. Counting stopped at the cap, so Cancel is capped too,
	// though it counts fewer tasks than the cap.
	e.exec(t, `INSERT INTO {s}.tasks (queue, kind, state, run_at, payload, max_attempts, attempt, lease_expires_at, cancel_requested_at)
	           SELECT 'busy', 'k', 'running', now(), '{}', 3, 1, now() + interval '1 hour', CASE WHEN i % 3 = 0 THEN now() END
	           FROM generate_series(1, 10001) i`)
	got, err = e.client.Count(ctx, &taskmill.CountParams{Filter: taskmill.TaskFilter{Queues: []string{"busy"}}})
	if err != nil || got.States[taskmill.StateRunning] != capped || got.Cancel != (taskmill.TaskCount{N: 6668, Capped: true}) {
		t.Fatalf("running tasks over the cap, a third of them asked to stop: %+v, %v", got, err)
	}
}

// A count reads each state's tasks through that state's index, up to its
// cap: not the table, once for each state, to find the tasks of a state that
// lie together, as tasks of one age do.
func TestCountReadsEachStatesIndex(t *testing.T) {
	t.Parallel()
	e := newEnv(t)

	// Available tasks, and after them dead ones, each more than the cap. The
	// count turns scans of the table off for itself, so the table need not
	// be large for the planner to read the indexes: only so large that a scan
	// for the dead tasks, which lie behind 20,000 others, reads more.
	e.exec(t, `INSERT INTO {s}.tasks (queue, kind, state, run_at, payload, max_attempts, finalized_at)
	           SELECT 'default', 'email.send', CASE WHEN i > 20000 THEN 'dead' ELSE 'available' END::{s}.task_state,
	                  now(), '{}', 3, CASE WHEN i > 20000 THEN now() END
	           FROM generate_series(1, 35000) i`)
	e.exec(t, "ANALYZE {s}.tasks")

	for _, filter := range []taskmill.TaskFilter{{}, {Queues: []string{taskmill.DefaultQueue}}} {
		n, err := taskmill.CountRowsRead(t.Context(), e.client, &taskmill.CountParams{Filter: filter})
		if err != nil {
			t.Fatal(err)
		}
		// Two states have tasks, and each count stops one task past 10,000.
		if n > 2*10_001 {
			t.Errorf("filter %+v: the count read %d rows, want at most %d", filter, n, 2*10_001)
		}
	}
}
