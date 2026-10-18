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
	if err := e.client.UpdateQueue(t.Context(), "fire-and-forget", &taskmill.QueueSettings{DiscardCompleted: true}); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{}, 1)
	runWorker(t, e, &taskmill.WorkerConfig{
		Handlers: handlers(func(context.Context, *taskmill.Task[sendEmail]) error {
			done <- struct{}{}
			return nil
		}, nil),
		Queues: map[string]taskmill.QueueOptions{"fire-and-forget": {}},
	})

	id := e.enqueue(t, sendEmail{}, &taskmill.EnqueueOptions{Queue: "fire-and-forget"})
	recv(t, done, "the worker to stop")
	deadline := time.Now().Add(5 * time.Second)
	for {
		_, err := e.client.Get(t.Context(), id)
		if errors.Is(err, taskmill.ErrNotFound) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("completed task still exists: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
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
		ID:       "reporter",
		Handlers: noopHandlers(),
		Queues:   map[string]taskmill.QueueOptions{"mail": {}},
	})

	deadline := time.Now().Add(10 * time.Second)
	for {
		queues, err := e.client.Queues(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		byName := map[string]taskmill.QueueInfo{}
		for _, q := range queues {
			byName[q.Name] = q
		}
		if byName["backlog"].Counts[taskmill.StateAvailable] == 3 {
			if byName["mail"].Workers != 1 || byName["limited"].Settings.MaxRunning != 2 {
				t.Fatalf("got %+v", byName)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("statistics never showed the backlog: %+v", byName)
		}
		time.Sleep(50 * time.Millisecond)
	}

	workers, err := e.client.Workers(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(workers) != 1 || workers[0].ID != "reporter" || !slices.Equal(workers[0].Queues, []string{"mail"}) {
		t.Fatalf("got workers %+v", workers)
	}
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
		Queue: taskmill.DefaultQueue, States: []taskmill.State{taskmill.StateAvailable}, Kind: "email.send",
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
}

func TestScheduleEnqueuesEachRunOnce(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	schedules := []taskmill.Schedule{{Name: "digest", Cron: "@hourly", Args: sendEmail{To: "digest"}}}
	a := newWorker(t, e, &taskmill.WorkerConfig{ID: "a", Schedules: schedules})
	b := newWorker(t, e, &taskmill.WorkerConfig{ID: "b", Schedules: schedules})
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
	// enqueued once.
	makeDue(time.Second)
	var wg sync.WaitGroup
	for _, w := range []*taskmill.Worker{a, b} {
		wg.Go(func() { taskmill.RunSchedules(ctx, w) })
	}
	wg.Wait()
	if n := tasks(); n != 1 {
		t.Fatalf("one due run enqueued %d tasks", n)
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
