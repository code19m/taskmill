package taskmill_test

import (
	"context"
	"errors"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/code19m/taskmill"
)

// The tests in this file write rows that taskmill never writes, as a role
// with INSERT on tasks can, or an UPDATE by hand. Such a row must fail
// alone: the tasks fetched, counted, or rescued with it are not its business.

// insertTask writes an available task of the default queue by hand, first
// in line, changes it with the SET clause set, and returns its ID.
func (e *env) insertTask(t *testing.T, set string) int64 {
	t.Helper()
	var id int64
	err := e.pool.QueryRow(t.Context(), replaceSchema(`
INSERT INTO {s}.tasks (queue, kind, state, priority, run_at, payload, max_attempts)
VALUES ('default', 'email.send', 'available', 4, now(), '{}', 25) RETURNING id`, e.schema)).Scan(&id)
	if err != nil {
		t.Fatal(err)
	}
	e.exec(t, "UPDATE {s}.tasks SET "+set+" WHERE id = $1", id)
	return id
}

func TestMalformedTaskFailsAlone(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct{ name, set string }{
		{"metadata that is not an object", `metadata = '[]'`},
		{"metadata that is not an object of strings", `metadata = '{"tenant": 7}'`},
		{"a lease token at its largest value", `lease_token = 2147483647`},
		{"a time of creation that is infinite", `created_at = 'infinity'`},
		{"a run time that is infinite", `run_at = '-infinity'`},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			failsAlone(t, tt.set, false)
		})
		t.Run(tt.name+", in a queue with MaxRunning", func(t *testing.T) {
			t.Parallel()
			failsAlone(t, tt.set, true)
		})
	}
}

// failsAlone runs a worker on a queue whose first task has a row that set
// made malformed. The tasks fetched with it must run once, without an error.
// The malformed task must become dead with the code invalid_row, and no code
// of the application may see it: not its handler, and not a middleware,
// which could turn the failure into another one.
func failsAlone(t *testing.T, set string, limited bool) {
	t.Helper()
	e := newEnv(t)
	bad := e.insertTask(t, set)
	if limited {
		e.limitDefaultQueue(t, 8)
	}
	var ids []int64
	for range 5 {
		ids = append(ids, e.enqueue(t, sendEmail{}, nil))
	}
	var sawBad atomic.Bool
	see := func(id int64) {
		if id == bad {
			sawBad.Store(true)
		}
	}
	runWorker(t, e, &taskmill.WorkerConfig{
		Handlers: handlers(func(_ context.Context, task *taskmill.Task[sendEmail]) error {
			see(task.ID)
			return nil
		}, nil),
		Middleware: []taskmill.Middleware{func(ctx context.Context, task *taskmill.TaskInfo, next func(context.Context) error) error {
			see(task.ID)
			if err := next(ctx); err != nil {
				return errors.New("the middleware's own error")
			}
			return nil
		}},
		Queues:        defaultQueue(10),
		LeaseDuration: 2 * time.Second,
	})

	for _, id := range ids {
		if info := e.waitState(t, id, taskmill.StateCompleted); info.Attempt != 1 || len(info.Errors) != 0 {
			t.Errorf("a task fetched with the malformed one took %d attempts, with errors %+v", info.Attempt, info.Errors)
		}
	}
	info := e.waitState(t, bad, taskmill.StateDead)
	if n := len(info.Errors); info.Attempt != 1 || n != 1 || info.Errors[0].Code != taskmill.CodeInvalidRow {
		t.Errorf("the malformed task: attempt %d, errors %+v; want one attempt that failed with invalid_row", info.Attempt, info.Errors)
	}
	if sawBad.Load() {
		t.Error("the handler or the middleware ran for the malformed task")
	}
}

// A task with no attempt left does not wait in a queue: a failed last attempt
// makes it dead. But a row can say so, such as a dead task that an UPDATE by
// hand made available again. A fetch that counted its attempt past the
// maximum broke the table's rule, and failed for every task it took.
func TestTaskWithoutAttemptsLeftRunsOnce(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	used := e.insertTask(t, "attempt = 3, max_attempts = 3")
	id := e.enqueue(t, sendEmail{}, nil)
	runWorker(t, e, &taskmill.WorkerConfig{
		Handlers: handlers(func(_ context.Context, task *taskmill.Task[sendEmail]) error {
			if task.ID == used {
				return context.DeadlineExceeded
			}
			return nil
		}, nil),
		Queues: defaultQueue(10),
	})
	e.waitState(t, id, taskmill.StateCompleted)
	if info := e.waitState(t, used, taskmill.StateDead); info.Attempt != 3 || len(info.Errors) != 1 {
		t.Fatalf("the task without attempts left: attempt %d, errors %+v; want one more failed attempt", info.Attempt, info.Errors)
	}
}

// The errors of a task are its history, which no handler needs: errors that
// are not an array count as none. The statements that record a failure
// append to them, and the one that rescues expired leases does so for up to
// 1,000 tasks of every queue: one row must not fail it for all of them.
func TestErrorsThatAreNotAnArrayCountAsNone(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	failing := e.insertTask(t, `errors = '{}', max_attempts = 1`)
	lost := e.insertTask(t, expiredLease+`, queue = 'billing', errors = '"gone"'`)
	other := e.insertTask(t, expiredLease+`, queue = 'reports'`)
	runWorker(t, e, &taskmill.WorkerConfig{
		Handlers: handlers(func(context.Context, *taskmill.Task[sendEmail]) error { return errors.New("no") }, nil),
		Queues:   defaultQueue(10),
	})

	if errs := e.waitState(t, failing, taskmill.StateDead).Errors; len(errs) != 1 || errs[0].Message != "no" {
		t.Errorf("the failed task has the errors %+v, want its failure alone", errs)
	}
	for _, id := range []int64{lost, other} {
		waitFor(t, "the expired lease to be rescued", func() bool { return e.get(t, id).State != taskmill.StateRunning })
		if errs := e.get(t, id).Errors; len(errs) != 1 || errs[0].Code != taskmill.CodeLeaseLost {
			t.Errorf("the rescued task %d has the errors %+v, want the lost lease alone", id, errs)
		}
	}
}

// The dashboard lists tasks with Client.List, 50 to a page: one row that
// does not decode must not hide the others.
func TestListShowsMalformedTasks(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	var ids []int64
	for _, set := range []string{
		`metadata = '{"tenant": "shop"}'`,
		`metadata = '"text"'`,
		`errors = '[7, {"attempt": 1, "at": "2026-01-02T03:04:05Z", "message": "kept"}]'`,
		`created_at = '-infinity', run_at = 'infinity', state = 'scheduled'`,
	} {
		ids = append(ids, e.insertTask(t, set))
	}
	res, err := e.client.List(t.Context(), nil)
	if err != nil || len(res.Tasks) != len(ids) {
		t.Fatalf("list: %v; want %d tasks", err, len(ids))
	}
	if got := e.get(t, ids[0]).Metadata; got["tenant"] != "shop" {
		t.Errorf("metadata of an ordinary task: %v", got)
	}
	if got := e.get(t, ids[1]).Metadata; got != nil {
		t.Errorf("metadata that is not an object decoded as %v, want none", got)
	}
	if got := e.get(t, ids[2]).Errors; len(got) != 1 || got[0].Message != "kept" {
		t.Errorf("errors with an entry that is no object decoded as %+v, want the entry that decodes", got)
	}
	if got := e.get(t, ids[3]); !got.CreatedAt.IsZero() || !got.RunAt.IsZero() {
		t.Errorf("infinite times decoded as %v and %v, want zero times", got.CreatedAt, got.RunAt)
	}
}

// A queue that no worker reads keeps a malformed row for good, and the
// maintainer counts every queue in one statement. One run time that is
// infinite must not stop the counts and the lag of all queues.
func TestStatisticsCountBesideAnInfiniteRunTime(t *testing.T) {
	t.Parallel()
	for _, set := range []string{
		`queue = 'parked', run_at = '-infinity'`,
		`queue = 'parked', run_at = 'infinity'`,
		`queue = 'parked', run_at = '-infinity', state = 'retryable', attempt = 1`,
		`queue = 'parked', run_at = 'infinity', state = 'scheduled'`,
	} {
		t.Run(set, func(t *testing.T) {
			t.Parallel()
			e := newEnv(t)
			e.insertTask(t, set)
			e.enqueue(t, sendEmail{}, &taskmill.EnqueueOptions{Queue: "orders", RunAt: time.Now().Add(-time.Hour)})
			runWorker(t, e, &taskmill.WorkerConfig{})

			orders, parked := e.countedQueue(t, "orders"), e.countedQueue(t, "parked")
			if n := orders.Counts[taskmill.StateAvailable].N; n != 1 || orders.Lag < 59*time.Minute {
				t.Errorf("the queue beside the malformed row: %d available tasks, lag %s; want 1 task, an hour late", n, orders.Lag)
			}
			// A run time that is no time gives no lag.
			if n := tasksOf(parked); n != 1 || parked.Lag != 0 {
				t.Errorf("the queue of the malformed row: %d tasks, lag %s; want 1 task and no lag", n, parked.Lag)
			}
		})
	}
}

// A role that only enqueues may change the rows of groups. A count that it
// set too low must fail no statement that takes a task out of its group: a
// completion ends the tasks of many groups in one transaction, and a rescue
// the expired leases of every queue.
func TestGroupCountThatIsTooLowFailsNoOtherGroup(t *testing.T) {
	t.Parallel()

	t.Run("a completion", func(t *testing.T) {
		t.Parallel()
		e := newEnv(t)
		e.rawGroups = true // one group's count is wrong on purpose
		var ids []int64
		for _, group := range []string{"spoiled", "a", "b", "c", "d", "e"} {
			ids = append(ids, e.enqueue(t, sendEmail{}, &taskmill.EnqueueOptions{GroupKey: group}))
		}
		e.exec(t, "UPDATE {s}.groups SET tasks = 0 WHERE group_key = 'spoiled'")

		// The handlers return together, so that one transaction completes
		// the tasks of every group.
		var runs atomic.Int64
		together := make(chan struct{})
		runWorker(t, e, &taskmill.WorkerConfig{
			Handlers: handlers(func(context.Context, *taskmill.Task[sendEmail]) error {
				if runs.Add(1) == int64(len(ids)) {
					close(together)
				}
				<-together
				return nil
			}, nil),
			Queues:        defaultQueue(10),
			LeaseDuration: 2 * time.Second,
		})
		for _, id := range ids {
			e.waitState(t, id, taskmill.StateCompleted)
		}
		if n := runs.Load(); n != int64(len(ids)) {
			t.Errorf("the handler ran %d times for %d tasks", n, len(ids))
		}
	})

	t.Run("a rescue", func(t *testing.T) {
		t.Parallel()
		e := newEnv(t)
		e.rawGroups = true
		// A canceled task of the group, whose worker is gone: the rescue
		// ends it, which takes it out of its group.
		canceled := e.insertTask(t, expiredLease+`, group_key = 'spoiled', cancel_requested_at = now()`)
		e.exec(t, "INSERT INTO {s}.groups (queue, group_key, tasks) VALUES ('default', 'spoiled', 0)")
		lost := e.insertTask(t, expiredLease+`, queue = 'reports'`)
		runWorker(t, e, &taskmill.WorkerConfig{})

		e.waitState(t, canceled, taskmill.StateCanceled)
		waitFor(t, "the expired lease of another queue to be rescued", func() bool {
			return e.get(t, lost).State != taskmill.StateRunning
		})
	})
}

// expiredLease is the SET clause that makes a task of insertTask one whose
// worker is gone: it runs, and its lease has expired.
const expiredLease = `state = 'running', attempt = 1, lease_token = 1, attempted_at = now(),
	lease_expires_at = now() - interval '1 second'`

// countedQueue waits until the maintainer has counted the queue, and returns
// it with its statistics.
func (e *env) countedQueue(t *testing.T, name string) taskmill.QueueInfo {
	t.Helper()
	var counted taskmill.QueueInfo
	waitFor(t, "the statistics of the queue "+name, func() bool {
		queues, err := e.client.Queues(t.Context())
		if err != nil {
			t.Fatalf("queues: %v", err)
		}
		i := slices.IndexFunc(queues, func(q taskmill.QueueInfo) bool { return q.Name == name && !q.RefreshedAt.IsZero() })
		if i >= 0 {
			counted = queues[i]
		}
		return i >= 0
	})
	return counted
}

// tasksOf returns how many tasks a queue's statistics count, in all states.
func tasksOf(q taskmill.QueueInfo) int64 {
	var n int64
	for _, c := range q.Counts {
		n += c.N
	}
	return n
}
