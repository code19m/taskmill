package taskmill_test

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/code19m/taskmill"
	"github.com/code19m/taskmill/internal/testdb"
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
	runWorker(t, e, &taskmill.WorkerConfig{Handlers: noopHandlers(), Queues: defaultQueue(1)})

	id := e.enqueue(t, sendEmail{}, nil)
	time.Sleep(500 * time.Millisecond)
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
	<-done
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
	e := newEnvWith(t, testdb.PoolSize(t, 16), nil)
	ran := make(chan int64, 10)
	hs := handlers(func(_ context.Context, task *taskmill.Task[sendEmail]) error {
		ran <- task.ID
		return nil
	}, nil)
	schedules := []taskmill.Schedule{{Name: "digest", Cron: "@hourly", Args: sendEmail{To: "digest"}}}
	// Two workers compete to run the schedule.
	runWorker(t, e, &taskmill.WorkerConfig{ID: "a", Handlers: hs, Queues: defaultQueue(1), Schedules: schedules})
	runWorker(t, e, &taskmill.WorkerConfig{ID: "b", Handlers: hs, Queues: defaultQueue(1), Schedules: schedules})

	var info taskmill.ScheduleInfo
	deadline := time.Now().Add(10 * time.Second)
	for {
		ss, err := e.client.Schedules(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if len(ss) == 1 {
			info = ss[0]
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("schedule was not stored")
		}
		time.Sleep(50 * time.Millisecond)
	}
	if info.Kind != "email.send" || info.Queue != taskmill.DefaultQueue || info.NextRunAt.Minute() != 0 {
		t.Fatalf("got %+v", info)
	}

	// Make a run due now.
	e.exec(t, "UPDATE {s}.schedules SET next_run_at = date_trunc('second', now()) - interval '1 second'")
	select {
	case <-ran:
	case <-time.After(10 * time.Second):
		t.Fatal("due schedule did not run")
	}
	time.Sleep(1500 * time.Millisecond)
	if n := e.count(t, "SELECT count(*) FROM {s}.tasks") + e.count(t, "SELECT count(*) FROM {s}.task_history"); n != 1 {
		t.Fatalf("one due run created %d tasks", n)
	}

	// A paused schedule does not run.
	if err := e.client.PauseSchedule(t.Context(), "digest"); err != nil {
		t.Fatal(err)
	}
	e.exec(t, "UPDATE {s}.schedules SET next_run_at = date_trunc('second', now()) - interval '1 second'")
	select {
	case <-ran:
		t.Fatal("paused schedule ran")
	case <-time.After(1500 * time.Millisecond):
	}
	if err := e.client.ResumeSchedule(t.Context(), "digest"); err != nil {
		t.Fatal(err)
	}
	select {
	case <-ran:
	case <-time.After(10 * time.Second):
		t.Fatal("resumed schedule did not run")
	}
	if err := e.client.PauseSchedule(t.Context(), "unknown"); !errors.Is(err, taskmill.ErrNotFound) {
		t.Fatalf("unknown schedule: got %v", err)
	}
}
