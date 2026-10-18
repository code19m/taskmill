package taskmilltest_test

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/code19m/taskmill"
	"github.com/code19m/taskmill/internal/testdb"
	"github.com/code19m/taskmill/taskmilltest"
)

type greet struct {
	Name string `json:"name"`
}

func (greet) Kind() string { return "greet" }

func TestEnqueuedSeesOnlyCommittedTasks(t *testing.T) {
	t.Parallel()
	pool := testdb.Pool(t)
	client := taskmilltest.NewClient(t, pool, nil)
	ctx := t.Context()

	_ = pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		if _, err := client.EnqueueTx(ctx, taskmill.Pgx(tx), greet{Name: "rolled back"}, nil); err != nil {
			t.Fatal(err)
		}
		return errors.New("roll back")
	})
	for _, name := range []string{"ada", "grace"} {
		if _, err := client.Enqueue(ctx, greet{Name: name}, nil); err != nil {
			t.Fatal(err)
		}
	}

	got := taskmilltest.Enqueued[greet](t, client)
	if len(got) != 2 || got[0].Args.Name != "ada" || got[1].Args.Name != "grace" {
		t.Fatalf("got %+v", got)
	}
}

func TestRunRecordsTheOutcome(t *testing.T) {
	t.Parallel()
	pool := testdb.Pool(t)
	client := taskmilltest.NewClient(t, pool, nil)

	ok := taskmill.HandlerFunc[greet](func(context.Context, *taskmill.Task[greet]) error { return nil })
	info, err := taskmilltest.Run(t, client, ok, greet{Name: "ada"}, nil)
	if err != nil || info.State != taskmill.StateCompleted {
		t.Fatalf("got %s, %v", info.State, err)
	}

	boom := errors.New("boom")
	failing := taskmill.HandlerFunc[greet](func(context.Context, *taskmill.Task[greet]) error { return boom })
	info, err = taskmilltest.Run(t, client, failing, greet{}, &taskmilltest.RunOptions{
		Enqueue: &taskmill.EnqueueOptions{MaxAttempts: 1},
	})
	if !errors.Is(err, boom) || info.State != taskmill.StateDead || info.Errors[0].Message != "boom" {
		t.Fatalf("got %s with %+v, %v", info.State, info.Errors, err)
	}
}

func TestRunSupportsCompleteTx(t *testing.T) {
	t.Parallel()
	pool := testdb.Pool(t)
	client := taskmilltest.NewClient(t, pool, nil)

	h := taskmill.HandlerFunc[greet](func(ctx context.Context, task *taskmill.Task[greet]) error {
		return pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
			if _, err := tx.Exec(ctx, "SELECT $1::text", task.Args.Name); err != nil {
				return err
			}
			return task.CompleteTx(ctx, taskmill.Pgx(tx))
		})
	})
	info, err := taskmilltest.Run(t, client, h, greet{Name: "ada"}, nil)
	if err != nil || info.State != taskmill.StateCompleted || info.Attempt != 1 {
		t.Fatalf("got %s on attempt %d, %v", info.State, info.Attempt, err)
	}
}

func TestWorkProcessesEverything(t *testing.T) {
	t.Parallel()
	pool := testdb.Pool(t)
	client := taskmilltest.NewClient(t, pool, nil)
	ctx := t.Context()

	// Each greeting enqueues the next one, through a FIFO group.
	var hs taskmill.Handlers
	taskmill.AddHandler(&hs, taskmill.HandlerFunc[greet](func(ctx context.Context, task *taskmill.Task[greet]) error {
		if len(task.Args.Name) < 5 {
			_, err := client.Enqueue(ctx, greet{Name: task.Args.Name + "!"}, &taskmill.EnqueueOptions{GroupKey: "g"})
			return err
		}
		return nil
	}), nil)
	if _, err := client.Enqueue(ctx, greet{Name: "a"}, &taskmill.EnqueueOptions{GroupKey: "g"}); err != nil {
		t.Fatal(err)
	}

	taskmilltest.Work(t, client, &taskmill.WorkerConfig{
		Handlers: &hs,
		Queues:   map[string]taskmill.QueueOptions{taskmill.DefaultQueue: {}},
	})
	got := taskmilltest.Enqueued[greet](t, client)
	if len(got) != 5 {
		t.Fatalf("got %d tasks, want 5", len(got))
	}
	for _, task := range got {
		if task.State != taskmill.StateCompleted {
			t.Fatalf("task %q is %s", task.Args.Name, task.State)
		}
	}
}

func TestRunReportsPanics(t *testing.T) {
	t.Parallel()
	client := taskmilltest.NewClient(t, testdb.Pool(t), nil)
	h := taskmill.HandlerFunc[greet](func(context.Context, *taskmill.Task[greet]) error { panic("boom") })
	info, err := taskmilltest.Run(t, client, h, greet{}, nil)
	if err == nil || info.State != taskmill.StateRetryable || info.Errors[0].Code != "panic" {
		t.Fatalf("got %s with %+v, %v", info.State, info.Errors, err)
	}
}

func TestExampleRuns(t *testing.T) {
	t.Parallel()
	testSignUp(t, testdb.Pool(t), func(int64) error { return nil })
}
