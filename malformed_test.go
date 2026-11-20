package taskmill_test

import (
	"context"
	"errors"
	"log/slog"
	"math"
	"slices"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/code19m/taskmill"
)

// The tests in this file write rows that taskmill never writes, as a role
// with INSERT on tasks can, or an UPDATE by hand. Such a task must fail
// alone: the tasks fetched, counted, or rescued with it are not its business.
// The small tables refuse such a row instead.

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

// stateOf returns the state of a task, or "" if the task is gone.
func (e *env) stateOf(t *testing.T, id int64) taskmill.State {
	t.Helper()
	info, err := e.client.Get(t.Context(), id)
	if errors.Is(err, taskmill.ErrNotFound) {
		return ""
	}
	if err != nil {
		t.Fatalf("get task %d: %v", id, err)
	}
	return info.State
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

// Each row is a FIFO group of three tasks that only a write by hand gives:
// its row names a task that is not its dead head as the one that halts it,
// or two of its tasks ran at once. A halt must end when the task that it
// names leaves the group, however the task leaves, and only then. A halt
// that stays names a task that is no longer counted, or no longer exists:
// nothing ends it, or a retry of the task ends it and takes the task for one
// that is still counted, so the count is one short and the group's last task
// waits in pending for good.
func TestHaltEndsWhenTheTaskItNamesLeaves(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ctx := t.Context()
	c := e.client
	w := newWorker(t, e, &taskmill.WorkerConfig{Handlers: noopHandlers()})

	// What a row does to its group: it acts on one task, on every task that
	// matches a filter, or it rescues the tasks whose worker is gone. The
	// tasks of a group count from 1, the oldest.
	type op = func(context.Context, int64) error
	type act = func(group string, ids []int64) error
	on := func(i int, do op) act {
		return func(_ string, ids []int64) error { return do(ctx, ids[i]) }
	}
	matching := func(do func(context.Context, *taskmill.BulkParams) (taskmill.BulkResult, error), f taskmill.TaskFilter) act {
		return func(group string, _ []int64) error {
			f.GroupKeys = []string{group}
			_, err := do(ctx, bulk(&f))
			return err
		}
	}
	rescue := func(string, []int64) error {
		taskmill.Rescue(ctx, w)
		return nil
	}

	run := func(ctx context.Context, id int64) error {
		_, err := w.RunTask(ctx, id)
		return err
	}
	cancelThen := func(next op) op {
		return func(ctx context.Context, id int64) error {
			if err := c.Cancel(ctx, id); err != nil {
				return err
			}
			return next(ctx, id)
		}
	}

	const (
		// The tasks of a group, as the SET clauses of insertTask.
		waits   = `state = 'available'`
		behind  = `state = 'pending'`
		dead    = `state = 'dead', finalized_at = now()`
		stopped = expiredLease + `, cancel_requested_at = now()` // its worker is gone, and it was asked to stop
		lost    = expiredLease + `, max_attempts = 1`            // its worker is gone, on its last attempt
		aReport = `, kind = 'report.build'`

		gone = taskmill.State("") // the task no longer exists
	)
	type tasks = [3]string
	type states = [3]taskmill.State
	available, pending := taskmill.StateAvailable, taskmill.StatePending
	canceled, isDead := taskmill.StateCanceled, taskmill.StateDead
	theHead := taskmill.TaskFilter{States: []taskmill.State{available}}
	reports := taskmill.TaskFilter{Kinds: []string{report{}.Kind()}}
	twoReports := tasks{waits + aReport, behind + aReport, behind} // the head and the next task

	for _, tt := range []struct {
		name  string // the group's key: what happens in it
		tasks tasks  // oldest first
		named int    // the task that the group's row names as halting it, or 0
		do    act
		want  states
		count int // what the group counts afterwards
		halt  int // the task that halts it afterwards, or 0
	}{
		// The halt names the head. When the head leaves, the halt ends and
		// the next task takes over.
		{"deleted", tasks{waits, behind, behind}, 1, on(1, c.Delete), states{gone, available, pending}, 2, 0},
		{"canceled, then retried", tasks{waits, behind, behind}, 1, on(1, cancelThen(c.Retry)), states{pending, available, pending}, 3, 0},
		{"canceled, then deleted", tasks{waits, behind, behind}, 1, on(1, cancelThen(c.Delete)), states{gone, available, pending}, 2, 0},
		{"canceled in bulk", tasks{waits, behind, behind}, 1, matching(c.CancelMatching, theHead), states{canceled, available, pending}, 2, 0},
		{"deleted in bulk", tasks{waits, behind, behind}, 1, matching(c.DeleteMatching, theHead), states{gone, available, pending}, 2, 0},
		{"completed", tasks{waits, behind, behind}, 1, on(1, run), states{taskmill.StateCompleted, available, pending}, 2, 0},
		{"canceled with the next task", twoReports, 1, matching(c.CancelMatching, reports), states{canceled, canceled, available}, 1, 0},
		// The halt names a task behind the head. It ends with that task,
		// and the head stays the head.
		{"a pending task is named", tasks{waits, behind, behind}, 2, on(2, c.Cancel), states{available, canceled, pending}, 2, 0},
		// A halt stays when another task leaves: a second task that is not
		// pending, beside the dead head, or one that a rescue ends in the
		// batch in which the head becomes dead.
		{"another task is named", tasks{dead, waits, behind}, 1, on(2, c.Cancel), states{isDead, canceled, pending}, 2, 1},
		{"rescued with a dead task", tasks{stopped, lost, behind}, 0, rescue, states{canceled, isDead, pending}, 2, 2},
	} {
		ids := make([]int64, 1, 1+len(tt.tasks)) // ids[0] is no task
		for _, set := range tt.tasks {
			ids = append(ids, e.insertTask(t, set+`, group_key = '`+tt.name+`'`))
		}
		e.exec(t, `INSERT INTO {s}.groups (queue, group_key, tasks, halted_task_id, halted_at)
		           VALUES ('default', $1, 3, nullif($2::bigint, 0), CASE WHEN $2::bigint <> 0 THEN now() END)`,
			tt.name, ids[tt.named])

		if err := tt.do(tt.name, ids); err != nil {
			t.Fatalf("%s: %v", tt.name, err)
		}

		for i, want := range tt.want {
			if got := e.stateOf(t, ids[i+1]); got != want {
				t.Errorf("%s: task %d of the group is %q, want %q", tt.name, i+1, got, want)
			}
		}
		if n := e.count(t, `SELECT count(*) FROM {s}.groups WHERE group_key = $1 AND tasks = $2
		                    AND halted_task_id IS NOT DISTINCT FROM nullif($3::bigint, 0)`,
			tt.name, tt.count, ids[tt.halt]); n != 1 {
			t.Errorf("%s: the group does not count %d tasks, or its halt is wrong", tt.name, tt.count)
		}
	}
}

// A group has two tasks that run only after a write by hand, such as an
// UPDATE that makes a pending task available. A transaction that completes
// both must take both off the group's count: a row that still counts one
// stays for good, and every task enqueued into the group later waits behind
// a head that does not exist.
func TestCompletionCountsEveryTaskOfAGroup(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	first := e.insertTask(t, `group_key = 'both'`)
	second := e.insertTask(t, `group_key = 'both'`)
	e.exec(t, "INSERT INTO {s}.groups (queue, group_key, tasks) VALUES ('default', 'both', 2)")

	// While a transaction holds the group's row, the worker keeps both
	// completions. A worker that stops records what it kept in one
	// transaction, which waits for the row.
	tx := e.begin(t)
	e.txExec(t, tx, "SELECT FROM {s}.groups WHERE group_key = 'both' FOR UPDATE")

	ran := make(chan struct{}, 2)
	stop := runWorker(t, e, &taskmill.WorkerConfig{
		Handlers: handlers(func(context.Context, *taskmill.Task[sendEmail]) error {
			ran <- struct{}{}
			return nil
		}, nil),
		Queues: defaultQueue(2),
	})
	recv(t, ran, "the first task to run")
	recv(t, ran, "the second task to run")

	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		stop()
	}()

	e.waitBlocked(t, "UPDATE {s}.groups")
	if err := tx.Commit(t.Context()); err != nil {
		t.Fatal(err)
	}
	recv(t, stopped, "the worker to stop")

	e.wantState(t, first, taskmill.StateCompleted)
	e.wantState(t, second, taskmill.StateCompleted)
	// The check of the groups at the end of the test fails on a row that stays.
}

// A group's row that is gone, which only a DELETE by hand gives, must not go
// unseen: every way in which a task leaves its group, or halts it, reports it.
func TestMissingGroupRowIsReported(t *testing.T) {
	t.Parallel()
	logs := &logRecorder{}
	e := newEnv(t).withLogger(t, slog.New(logs))
	ctx := t.Context()

	// One task in each group, named after the way in which it leaves, and
	// two in the group that a bulk action empties.
	ids := make(map[string]int64)
	for _, group := range []string{"completed", "dead", "stopped", "canceled"} {
		ids[group] = e.enqueue(t, sendEmail{}, &taskmill.EnqueueOptions{GroupKey: group})
	}
	for range 2 {
		e.enqueue(t, sendEmail{}, &taskmill.EnqueueOptions{GroupKey: "bulk", Queue: "other"})
	}
	e.exec(t, "DELETE FROM {s}.groups")

	w := newWorker(t, e, &taskmill.WorkerConfig{
		Handlers: handlers(func(ctx context.Context, task *taskmill.Task[sendEmail]) error {
			switch task.GroupKey {
			case "dead":
				return taskmill.Permanent(errors.New("bad address"))
			case "stopped": // canceled while it runs: the worker records the cancel
				if err := e.client.Cancel(ctx, task.ID); err != nil {
					return err
				}
				return errors.New("stopped")
			}
			return nil
		}, nil),
	})

	for group, want := range map[string]taskmill.State{
		"completed": taskmill.StateCompleted, "dead": taskmill.StateDead, "stopped": taskmill.StateCanceled,
	} {
		if info, err := w.RunTask(ctx, ids[group]); err != nil || info.State != want {
			t.Fatalf("the task of the group %q: %v, %+v, want it %s", group, err, info, want)
		}
	}

	if err := e.client.Cancel(ctx, ids["canceled"]); err != nil {
		t.Fatal(err)
	}
	res, err := e.client.CancelMatching(ctx, bulk(&taskmill.TaskFilter{Queues: []string{"other"}}))
	if err != nil || res.Changed != 2 {
		t.Fatalf("the bulk cancel: %v, %+v", err, res)
	}
	e.wantState(t, ids["canceled"], taskmill.StateCanceled)

	// A line names its task if one task left the group.
	want := []string{"other/bulk task_id="}
	for group, id := range ids {
		want = append(want, "default/"+group+" task_id="+strconv.FormatInt(id, 10))
	}

	var reported []string
	for _, r := range logs.all() {
		if r.Message == "fifo group missing" {
			reported = append(reported, attr(r, "queue")+"/"+attr(r, "group")+" task_id="+attr(r, "task_id"))
		}
	}

	slices.Sort(want)
	slices.Sort(reported)
	if !slices.Equal(reported, want) {
		t.Fatalf("groups reported as missing: %q, want %q", reported, want)
	}
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

// The tables of the workers, the schedules, and the settings are small, and
// every value in them is taskmill's own. So they refuse a value that no
// release writes, where it is written, with the name of the rule that it
// breaks: the statements that read these tables for every worker, schedule,
// and queue count on it. What a release writes, they accept to its limits.
func TestSmallTablesRefuseWhatNoReleaseWrites(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ctx := t.Context()
	e.oldWorker(t)
	e.exec(t, `INSERT INTO {s}.schedules (name, queue, kind, cron, next_run_at)
	           VALUES ('nightly', 'default', 'email.send', '0 3 * * *', now())`)

	type write struct{ value, table, set, constraint string }
	refused := []write{
		{"a check-in without end", "workers", "heartbeat_at = 'infinity'", "workers_times_check"},
		{"a start before all time", "workers", "started_at = '-infinity'", "workers_times_check"},
		{"a queue that is NULL", "workers", "queues = '{default,NULL}'", "workers_names_check"},
		{"a kind that is NULL", "workers", "kinds = '{NULL}'", "workers_names_check"},
		{"queues in two dimensions", "workers", "queues = '{{a,b},{c,d}}'", "workers_names_check"},
		{"kinds in two dimensions, with a NULL", "workers", "kinds = '{{a,b},{c,NULL}}'", "workers_names_check"},
		{"a schedule that is NULL", "workers", "schedules = '{a,NULL}', schedule_hashes = '{x,y}'", "workers_schedules_check"},
		{"hashes in two dimensions", "workers", "schedules = '{a,b}', schedule_hashes = '{{x},{y}}'", "workers_schedules_check"},
		{"more schedules than hashes", "workers", "schedules = '{a,b}', schedule_hashes = '{x}'", "workers_schedules_check"},
		{"schedules without hashes", "workers", "schedules = '{}'", "workers_schedules_check"},
		{"a lease below zero", "workers", "lease_duration = '-1 microsecond'", "workers_lease_check"},
		{"a lease above 300 years", "workers", "lease_duration = '1000000 years'", "workers_lease_check"},
		{"a next run without end", "schedules", "next_run_at = 'infinity'", "schedules_times_check"},
		{"a last run before all time", "schedules", "last_run_at = '-infinity'", "schedules_times_check"},
		{"a retention below zero", "settings", "dead_retention = '-1 day'", "settings_retention_check"},
		{"a retention under 1 hour", "settings", "history_retention = '59 minutes'", "settings_retention_check"},
		{"a retention above 300 years", "settings", "history_retention = '1000000 years'", "settings_retention_check"},
		// PostgreSQL stores a period as months, days, and microseconds, each
		// with a sign of its own, and compares periods by their totals. These
		// two count as 1 hour and as nothing. But a month is not 30 days on
		// the calendar, so a time moved back by one of them is thousands of
		// years ahead: a retention that deletes what just finished, and a
		// lease that no time can be moved by.
		{
			"a retention of 1 hour in all, with months below zero", "settings",
			"dead_retention = '-3000000 mons 90000000 days 1 hour'", "settings_retention_check",
		},
		{
			"a lease of nothing in all, with months below zero", "workers",
			"lease_duration = '-4000000 mons 120000000 days'", "workers_lease_check",
		},
	}

	// So no field of a period may be negative: not its months, not its days,
	// and not the rest. Each of these is inside its range in all.
	for _, period := range []struct{ table, column, constraint string }{
		{"settings", "history_retention", "settings_retention_check"},
		{"settings", "dead_retention", "settings_retention_check"},
		{"workers", "lease_duration", "workers_lease_check"},
	} {
		for _, p := range []string{"-1 mon 40 days", "2 mons -20 days", "7 days -1 hour"} {
			refused = append(refused, write{
				"the period " + p + " as " + period.column, period.table, period.column + " = '" + p + "'", period.constraint,
			})
		}
	}

	if e.infinitePeriods(t) {
		refused = append(refused,
			write{"a lease without end", "workers", "lease_duration = 'infinity'", "workers_lease_check"},
			write{"a retention without end", "settings", "dead_retention = 'infinity'", "settings_retention_check"})
	}

	for _, tt := range refused {
		_, err := e.pool.Exec(ctx, replaceSchema("UPDATE {s}."+tt.table+" SET "+tt.set, e.schema))
		pgErr, ok := errors.AsType[*pgconn.PgError](err)
		if !ok || pgErr.Code != "23514" || pgErr.ConstraintName != tt.constraint { // check_violation
			t.Errorf("%s: got %v, want a violation of the check %s", tt.value, err, tt.constraint)
		}
	}

	// The limits of what a release writes: the longest lease of a worker,
	// and the shortest and the longest retention, which keeps tasks for good.
	// Both are microseconds alone. The schema's own retention periods are
	// days alone.
	longest := time.Duration(math.MaxInt64)
	e.exec(t, "UPDATE {s}.workers SET lease_duration = $1 * interval '1 microsecond'", longest.Microseconds())
	e.exec(t, "UPDATE {s}.settings SET history_retention = DEFAULT, dead_retention = DEFAULT")

	for _, d := range []time.Duration{time.Hour, longest} {
		err := e.client.UpdateSettings(ctx, func(s *taskmill.Settings) { s.HistoryRetention, s.DeadRetention = d, d })
		if err != nil {
			t.Errorf("a retention of %s, which UpdateSettings accepts: %v", d, err)
		}
	}
}
