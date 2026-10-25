package taskmill_test

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/code19m/taskmill"
)

func noopHandlers() *taskmill.Handlers {
	return handlers(func(context.Context, *taskmill.Task[sendEmail]) error { return nil }, nil)
}

func TestPauseAndResumeQueue(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	if err := e.client.PauseQueue(t.Context(), taskmill.DefaultQueue); err != nil {
		t.Fatal(err)
	}
	runWorker(t, e, &taskmill.WorkerConfig{
		Handlers: noopHandlers(),
		Queues:   map[string]taskmill.QueueOptions{taskmill.DefaultQueue: {}, "canary": {}},
	})

	// The canary, enqueued after the paused task into a queue that is not
	// paused, shows that the worker is fetching.
	id := e.enqueue(t, sendEmail{}, nil)
	e.waitState(t, e.enqueue(t, sendEmail{}, &taskmill.EnqueueOptions{Queue: "canary"}), taskmill.StateCompleted)
	e.wantState(t, id, taskmill.StateAvailable)

	if err := e.client.ResumeQueue(t.Context(), taskmill.DefaultQueue); err != nil {
		t.Fatal(err)
	}
	e.waitState(t, id, taskmill.StateCompleted)
}

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

func TestSettings(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	s, err := e.client.Settings(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if s.HistoryRetention != 7*24*time.Hour || s.DeadRetention != 30*24*time.Hour {
		t.Fatalf("defaults: %+v", s)
	}
	if err := e.client.UpdateSettings(t.Context(), &taskmill.Settings{HistoryRetention: 48 * time.Hour}); err != nil {
		t.Fatal(err)
	}
	if s, _ = e.client.Settings(t.Context()); s.HistoryRetention != 48*time.Hour || s.DeadRetention != 30*24*time.Hour {
		t.Fatalf("after update: %+v", s)
	}
	err = e.client.UpdateSettings(t.Context(), &taskmill.Settings{DeadRetention: time.Minute})
	if !errors.Is(err, taskmill.ErrInvalidArgument) {
		t.Fatalf("too short retention: got %v", err)
	}
}

func TestQueuesAndWorkers(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	for range 3 {
		e.enqueue(t, sendEmail{}, &taskmill.EnqueueOptions{Queue: "backlog"})
	}
	if err := e.client.UpdateQueue(t.Context(), "limited", &taskmill.QueueSettings{MaxRunning: 2}); err != nil {
		t.Fatal(err)
	}
	runWorker(t, e, &taskmill.WorkerConfig{
		Handlers: noopHandlers(),
		Queues:   map[string]taskmill.QueueOptions{"mail": {}},
	})

	byName := map[string]taskmill.QueueInfo{}
	waitFor(t, "statistics to show the backlog", func() bool {
		queues, err := e.client.Queues(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		for _, q := range queues {
			byName[q.Name] = q
		}
		return byName["backlog"].Counts[taskmill.StateAvailable] == 3
	})
	if byName["mail"].Workers != 1 || byName["limited"].Settings.MaxRunning != 2 {
		t.Fatalf("got %+v", byName)
	}
	// Queues without tasks are counted too, so they show when.
	if byName["limited"].RefreshedAt.IsZero() || byName["mail"].RefreshedAt.IsZero() {
		t.Fatalf("empty queues were not counted: %+v", byName)
	}

	waitFor(t, "the only worker to be elected maintainer", func() bool {
		workers, err := e.client.Workers(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if len(workers) != 1 || workers[0].ID == "" || !slices.Equal(workers[0].Queues, []string{"mail"}) ||
			!slices.Equal(workers[0].Kinds, []string{"email.send"}) {
			t.Fatalf("got workers %+v", workers)
		}
		return workers[0].Maintainer
	})
}

func TestQueuesMeasureLag(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	// The oldest available task has the lowest priority: lag spans every
	// priority.
	e.enqueue(t, sendEmail{}, &taskmill.EnqueueOptions{
		Queue: "behind", Priority: taskmill.PriorityLow, RunAt: time.Now().Add(-time.Hour),
	})
	e.enqueue(t, sendEmail{}, &taskmill.EnqueueOptions{
		Queue: "behind", Priority: taskmill.PriorityCritical, RunAt: time.Now().Add(-time.Minute),
	})
	// A retry that is overdue counts, and so does its wait before it is
	// promoted.
	retry := e.enqueue(t, sendEmail{}, &taskmill.EnqueueOptions{Queue: "retrying"})
	e.exec(t, "UPDATE {s}.tasks SET state = 'retryable', run_at = now() - interval '30 minutes' WHERE id = $1", retry)
	e.enqueue(t, sendEmail{}, &taskmill.EnqueueOptions{Queue: "idle", RunAt: time.Now().Add(time.Hour)})
	stop := runWorker(t, e, &taskmill.WorkerConfig{
		Handlers: noopHandlers(),
		Queues:   map[string]taskmill.QueueOptions{"mail": {}},
	})

	lag := map[string]time.Duration{}
	waitFor(t, "statistics to count every queue", func() bool {
		queues, err := e.client.Queues(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		for _, q := range queues {
			lag[q.Name] = q.Lag
		}
		return len(lag) >= 4 && lag["behind"] > 0 && lag["retrying"] > 0
	})
	for queue, want := range map[string]time.Duration{"behind": time.Hour, "retrying": 30 * time.Minute} {
		if lag[queue] < want || lag[queue] > want+time.Minute {
			t.Errorf("queue %s: got lag %s, want about %s", queue, lag[queue], want)
		}
	}
	if lag["idle"] != 0 {
		t.Errorf("got lag %s for a queue without due tasks", lag["idle"])
	}

	// Without a maintainer, the lag keeps growing, by the database's clock.
	stop()
	waitFor(t, "the lag to grow without a maintainer", func() bool {
		queues, err := e.client.Queues(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		for _, q := range queues {
			if q.Name == "behind" {
				return q.Lag > lag["behind"]+500*time.Millisecond
			}
		}
		return false
	})
}

func TestListPagesNewestFirst(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	var ids []int64
	for i := range 5 {
		ids = append(ids, e.enqueue(t, sendEmail{UserID: int64(i)}, nil))
	}
	ids = append(ids, e.enqueue(t, report{Name: "r"}, nil))
	runWorker(t, e, &taskmill.WorkerConfig{
		Handlers: noopHandlers(),
		Queues:   map[string]taskmill.QueueOptions{"reports-done": {}},
	})
	e.exec(t, "UPDATE {s}.tasks SET queue = 'reports-done', kind = 'email.send' WHERE id = $1", ids[5])
	e.exec(t, "SELECT pg_notify($1, 'a|reports-done')", e.schema)
	e.waitState(t, ids[5], taskmill.StateCompleted)

	var got []int64
	cursor := ""
	for page := 0; ; page++ {
		res, err := e.client.List(t.Context(), &taskmill.ListParams{Limit: 4, Cursor: cursor})
		if err != nil {
			t.Fatal(err)
		}
		for _, task := range res.Tasks {
			got = append(got, task.ID)
		}
		if res.Next == "" {
			break
		}
		cursor = res.Next
		if page > 3 {
			t.Fatal("too many pages")
		}
	}
	want := slices.Clone(ids)
	slices.Reverse(want)
	if !slices.Equal(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}

	completed, err := e.client.List(t.Context(), &taskmill.ListParams{States: []taskmill.State{taskmill.StateCompleted}})
	if err != nil {
		t.Fatal(err)
	}
	if len(completed.Tasks) != 1 || completed.Tasks[0].ID != ids[5] || completed.Tasks[0].State != taskmill.StateCompleted {
		t.Fatalf("completed tasks: %+v", completed.Tasks)
	}
	available, err := e.client.List(t.Context(), &taskmill.ListParams{
		Queues: []string{taskmill.DefaultQueue}, States: []taskmill.State{taskmill.StateAvailable}, Kinds: []string{"email.send"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(available.Tasks) != 5 {
		t.Fatalf("got %d available tasks, want 5", len(available.Tasks))
	}
	if _, err := e.client.List(t.Context(), &taskmill.ListParams{Cursor: "%%%"}); !errors.Is(err, taskmill.ErrInvalidArgument) {
		t.Fatalf("bad cursor: got %v", err)
	}
	unknown := &taskmill.ListParams{States: []taskmill.State{"bogus"}}
	if _, err := e.client.List(t.Context(), unknown); !errors.Is(err, taskmill.ErrInvalidArgument) {
		t.Fatalf("unknown state: got %v", err)
	}
}

func TestListSomeQueues(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	for _, q := range []string{"a", "b", "c"} {
		e.enqueue(t, sendEmail{}, &taskmill.EnqueueOptions{Queue: q})
	}
	for _, c := range []struct {
		queues []string
		want   int
	}{{nil, 3}, {[]string{"a", "b"}, 2}, {[]string{"c"}, 1}, {[]string{"none"}, 0}} {
		res, err := e.client.List(t.Context(), &taskmill.ListParams{Queues: c.queues})
		if err != nil {
			t.Fatal(err)
		}
		if len(res.Tasks) != c.want {
			t.Fatalf("queues %v: got %d tasks, want %d", c.queues, len(res.Tasks), c.want)
		}
	}
	if _, err := e.client.List(t.Context(), &taskmill.ListParams{Queues: []string{""}}); !errors.Is(err, taskmill.ErrInvalidArgument) {
		t.Fatalf("empty queue name: got %v", err)
	}
}

func TestListSomeGroups(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	a1 := e.enqueue(t, sendEmail{}, &taskmill.EnqueueOptions{GroupKey: "a"})
	b := e.enqueue(t, sendEmail{}, &taskmill.EnqueueOptions{GroupKey: "b"})
	e.enqueue(t, sendEmail{}, &taskmill.EnqueueOptions{GroupKey: "c"})
	a2 := e.enqueue(t, sendEmail{}, &taskmill.EnqueueOptions{GroupKey: "a"})
	page, err := e.client.List(t.Context(), &taskmill.ListParams{GroupKeys: []string{"a", "b"}})
	if err != nil {
		t.Fatal(err)
	}
	if got := ids(page.Tasks); !slices.Equal(got, []int64{a2, b, a1}) {
		t.Fatalf("got %v, want %v", got, []int64{a2, b, a1})
	}
}

func ids(tasks []taskmill.TaskInfo) []int64 {
	out := make([]int64, len(tasks))
	for i, t := range tasks {
		out[i] = t.ID
	}
	return out
}

func TestGroups(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	for _, key := range []string{"Account-1", "account-2", "account-2", "order-9"} {
		e.enqueue(t, sendEmail{}, &taskmill.EnqueueOptions{GroupKey: key})
	}
	// A dead head halts its group, and the task behind it waits.
	runWorker(t, e, &taskmill.WorkerConfig{
		Handlers: handlers(func(context.Context, *taskmill.Task[sendEmail]) error {
			return taskmill.Permanent(errors.New("rejected"))
		}, nil),
		Queues: map[string]taskmill.QueueOptions{"other": {}},
	})
	head := e.enqueue(t, sendEmail{}, &taskmill.EnqueueOptions{Queue: "other", GroupKey: "order-9"})
	e.enqueue(t, sendEmail{}, &taskmill.EnqueueOptions{Queue: "other", GroupKey: "order-9"})
	e.waitState(t, head, taskmill.StateDead)

	for _, c := range []struct {
		params taskmill.GroupParams
		want   []taskmill.GroupInfo
	}{
		{taskmill.GroupParams{Search: "ACCOUNT"}, []taskmill.GroupInfo{
			{Queue: taskmill.DefaultQueue, Key: "Account-1", Tasks: 1},
			{Queue: taskmill.DefaultQueue, Key: "account-2", Tasks: 2},
		}},
		{taskmill.GroupParams{Queues: []string{"other"}}, []taskmill.GroupInfo{
			{Queue: "other", Key: "order-9", Tasks: 2, HaltedBy: head},
		}},
		{taskmill.GroupParams{Limit: 1}, []taskmill.GroupInfo{{Queue: taskmill.DefaultQueue, Key: "Account-1", Tasks: 1}}},
	} {
		got, err := e.client.Groups(t.Context(), &c.params)
		if err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(got, c.want) {
			t.Fatalf("%+v: got %+v, want %+v", c.params, got, c.want)
		}
	}
	if _, err := e.client.Groups(t.Context(), &taskmill.GroupParams{Limit: -1}); !errors.Is(err, taskmill.ErrInvalidArgument) {
		t.Fatalf("negative limit: got %v", err)
	}
}

func TestScheduleEnqueuesEachRunOnce(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	schedules := []taskmill.Schedule{{Name: "digest", Cron: "@hourly", Args: sendEmail{To: "digest"}}}
	a := newWorker(t, e, &taskmill.WorkerConfig{Schedules: schedules})
	b := newWorker(t, e, &taskmill.WorkerConfig{Schedules: schedules})
	ctx := t.Context()
	tasks := func() int { return e.count(t, "SELECT count(*) FROM {s}.tasks") }
	// Each run is due at a different time, as cron runs are: runs are
	// deduplicated by their due time.
	makeDue := func(ago time.Duration) {
		e.exec(t, "UPDATE {s}.schedules SET next_run_at = date_trunc('second', now()) - $1 * interval '1 second'",
			ago.Seconds())
	}

	// The first pass stores the schedule, due at the next full hour.
	taskmill.RunSchedules(ctx, a)
	ss, err := e.client.Schedules(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(ss) != 1 || ss[0].Kind != "email.send" || ss[0].Queue != taskmill.DefaultQueue || ss[0].NextRunAt.Minute() != 0 {
		t.Fatalf("got %+v", ss)
	}
	if n := tasks(); n != 0 {
		t.Fatalf("a schedule that is not due enqueued %d tasks", n)
	}

	// Two maintainers whose terms overlap both find the run due: it is
	// enqueued once, to run at the time it was scheduled for.
	makeDue(time.Second)
	var slot time.Time
	if err := e.pool.QueryRow(ctx, replaceSchema("SELECT next_run_at FROM {s}.schedules", e.schema)).Scan(&slot); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for _, w := range []*taskmill.Worker{a, b} {
		wg.Go(func() { taskmill.RunSchedules(ctx, w) })
	}
	wg.Wait()
	if n := tasks(); n != 1 {
		t.Fatalf("one due run enqueued %d tasks", n)
	}
	page, err := e.client.List(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := page.Tasks[0].RunAt; !got.Equal(slot) {
		t.Fatalf("the task runs at %s, want its slot %s", got, slot)
	}

	// A paused schedule does not run; once resumed, its missed run does.
	if err := e.client.PauseSchedule(ctx, "digest"); err != nil {
		t.Fatal(err)
	}
	makeDue(time.Minute)
	taskmill.RunSchedules(ctx, a)
	if n := tasks(); n != 1 {
		t.Fatal("a paused schedule ran")
	}
	if err := e.client.ResumeSchedule(ctx, "digest"); err != nil {
		t.Fatal(err)
	}
	taskmill.RunSchedules(ctx, a)
	if n := tasks(); n != 2 {
		t.Fatal("a resumed schedule did not run")
	}

	if err := e.client.PauseSchedule(ctx, "unknown"); !errors.Is(err, taskmill.ErrNotFound) {
		t.Fatalf("unknown schedule: got %v", err)
	}
}

func TestRunScheduleNow(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	w := newWorker(t, e, &taskmill.WorkerConfig{
		Schedules: []taskmill.Schedule{{
			Name:    "yearly-report",
			Cron:    "@yearly",
			Args:    sendEmail{To: "report"},
			Options: &taskmill.EnqueueOptions{Priority: taskmill.PriorityHigh},
		}},
	})
	ctx := t.Context()
	taskmill.RunSchedules(ctx, w)
	before, err := e.client.Schedules(ctx)
	if err != nil {
		t.Fatal(err)
	}

	asked := time.Now()
	if err := e.client.RunSchedule(ctx, "yearly-report"); err != nil {
		t.Fatal(err)
	}
	taskmill.RunSchedules(ctx, w)
	page, err := e.client.List(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Tasks) != 1 {
		t.Fatalf("got %d tasks, want 1", len(page.Tasks))
	}
	// The run has the schedule's options, and runs at the time of the request.
	if task := page.Tasks[0]; task.Priority != taskmill.PriorityHigh || task.RunAt.Sub(asked).Abs() > 5*time.Second {
		t.Fatalf("got priority %s and run time %s, want high and about %s", task.Priority, task.RunAt, asked)
	}
	// The timetable stays as it was.
	after, err := e.client.Schedules(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !after[0].NextRunAt.Equal(before[0].NextRunAt) || after[0].LastRunAt.IsZero() {
		t.Fatalf("next run %s, want %s; last run %s", after[0].NextRunAt, before[0].NextRunAt, after[0].LastRunAt)
	}

	if err := e.client.PauseSchedule(ctx, "yearly-report"); err != nil {
		t.Fatal(err)
	}
	if err := e.client.RunSchedule(ctx, "yearly-report"); !errors.Is(err, taskmill.ErrInvalidState) {
		t.Fatalf("paused schedule: got %v, want ErrInvalidState", err)
	}
	if err := e.client.RunSchedule(ctx, "unknown"); !errors.Is(err, taskmill.ErrNotFound) {
		t.Fatalf("unknown schedule: got %v, want ErrNotFound", err)
	}
}

func TestRemovedSchedulesArePruned(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	w := newWorker(t, e, &taskmill.WorkerConfig{
		Schedules: []taskmill.Schedule{{Name: "kept", Cron: "@yearly", Args: sendEmail{}}},
	})
	ctx := t.Context()
	taskmill.RunSchedules(ctx, w)
	for _, name := range []string{"removed-today", "removed-long-ago"} {
		e.exec(t, `INSERT INTO {s}.schedules (name, queue, kind, cron, next_run_at) VALUES ($1, 'default', 'email.send', '@daily', now())`, name)
	}
	e.exec(t, "UPDATE {s}.schedules SET updated_at = now() - interval '2 days' WHERE name IN ('kept', 'removed-long-ago')")

	// A worker that defines no schedules, such as one that only does
	// maintenance, deletes none.
	taskmill.PruneSchedules(ctx, newWorker(t, e, &taskmill.WorkerConfig{}))
	if n := e.count(t, "SELECT count(*) FROM {s}.schedules"); n != 3 {
		t.Fatalf("a maintainer without schedules left %d of 3", n)
	}

	taskmill.PruneSchedules(ctx, w)
	ss, err := e.client.Schedules(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, s := range ss {
		names = append(names, s.Name)
	}
	if !slices.Equal(names, []string{"kept", "removed-today"}) {
		t.Fatalf("got schedules %v", names)
	}
}

func TestScheduleDoesNotWaitForAHeldGroup(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	w := newWorker(t, e, &taskmill.WorkerConfig{Schedules: []taskmill.Schedule{
		{Name: "ledger", Cron: "@hourly", Args: sendEmail{}, Options: &taskmill.EnqueueOptions{GroupKey: "g"}},
	}})
	ctx := t.Context()
	taskmill.RunSchedules(ctx, w)
	e.exec(t, "UPDATE {s}.schedules SET next_run_at = date_trunc('second', now()) - interval '1 second'")
	e.enqueue(t, sendEmail{}, &taskmill.EnqueueOptions{GroupKey: "g"})
	tx := e.begin(t)
	e.txExec(t, tx, "SELECT FROM {s}.groups WHERE group_key = 'g' FOR UPDATE")

	start := time.Now()
	taskmill.RunSchedules(ctx, w)
	if d := time.Since(start); d > 5*time.Second {
		t.Fatalf("the schedule waited %s for the group", d)
	}
	if n := e.count(t, "SELECT count(*) FROM {s}.tasks"); n != 1 {
		t.Fatalf("got %d tasks while the group was held, want 1", n)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	taskmill.RunSchedules(ctx, w)
	if n := e.count(t, "SELECT count(*) FROM {s}.tasks"); n != 2 {
		t.Fatalf("got %d tasks after the group was free, want 2", n)
	}
}

func TestMaintainerRunsDueSchedules(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ran := make(chan struct{}, 1)
	runWorker(t, e, &taskmill.WorkerConfig{
		Handlers: handlers(func(context.Context, *taskmill.Task[sendEmail]) error {
			ran <- struct{}{}
			return nil
		}, nil),
		Queues:    defaultQueue(1),
		Schedules: []taskmill.Schedule{{Name: "digest", Cron: "@hourly", Args: sendEmail{To: "digest"}}},
	})
	waitFor(t, "the schedule to be stored", func() bool { return e.count(t, "SELECT count(*) FROM {s}.schedules") == 1 })
	e.exec(t, "UPDATE {s}.schedules SET next_run_at = date_trunc('second', now()) - interval '1 second'")
	select {
	case <-ran:
	case <-time.After(10 * time.Second):
		t.Fatal("the due schedule did not run")
	}
}
