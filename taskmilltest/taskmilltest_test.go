package taskmilltest_test

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

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
	info, err = taskmilltest.Run(t, client, failing, greet{}, nil)
	if !errors.Is(err, boom) || info.State != taskmill.StateRetryable || info.Errors[0].Message != "boom" {
		t.Fatalf("got %s with %+v, %v", info.State, info.Errors, err)
	}

	bad := taskmill.HandlerFunc[greet](func(context.Context, *taskmill.Task[greet]) error { return taskmill.Permanent(boom) })
	if info, _ = taskmilltest.Run(t, client, bad, greet{}, nil); info.State != taskmill.StateDead {
		t.Fatalf("got %s, want dead", info.State)
	}
}

func TestRunTakesEnqueueOptions(t *testing.T) {
	t.Parallel()
	pool := testdb.Pool(t)
	client := taskmilltest.NewClient(t, pool, nil)

	night := time.Date(2026, time.November, 12, 3, 0, 0, 0, time.FixedZone("CET", 3600))
	var got *taskmill.Task[greet]
	h := taskmill.HandlerFunc[greet](func(_ context.Context, task *taskmill.Task[greet]) error {
		got = task
		return nil
	})
	opts := &taskmill.EnqueueOptions{
		Queue: "nightly", GroupKey: "ada",
		Metadata: map[string]string{taskmill.MetadataScheduledFor: night.Format(time.RFC3339)},
	}

	info, err := taskmilltest.Run(t, client, h, greet{Name: "ada"}, opts)
	if err != nil || info.State != taskmill.StateCompleted {
		t.Fatalf("got %s, %v", info.State, err)
	}
	if got.Queue != "nightly" || got.GroupKey != "ada" || !got.ScheduledFor().Equal(night) {
		t.Errorf("the handler got queue %q, group %q, and a run for %s; want nightly, ada, and %s", got.Queue, got.GroupKey, got.ScheduledFor(), night)
	}
}

// fatalTB records the message of a test that a helper fails, and ends the
// helper's goroutine as Fatal does.
type fatalTB struct {
	testing.TB
	msg string
}

func (tb *fatalTB) Fatalf(format string, args ...any) {
	tb.msg = fmt.Sprintf(format, args...)
	runtime.Goexit()
}

func TestRunFailsTheTestForADedupeKeyThatIsTaken(t *testing.T) {
	t.Parallel()
	pool := testdb.Pool(t)
	client := taskmilltest.NewClient(t, pool, nil)

	opts := &taskmill.EnqueueOptions{DedupeKey: "ada"}
	held, err := client.Enqueue(t.Context(), greet{Name: "ada"}, opts)
	if err != nil {
		t.Fatal(err)
	}

	ran := false
	h := taskmill.HandlerFunc[greet](func(context.Context, *taskmill.Task[greet]) error {
		ran = true
		return nil
	})
	tb := &fatalTB{TB: t}
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = taskmilltest.Run(tb, client, h, greet{Name: "ada"}, opts)
	}()
	<-done

	if want := "task " + strconv.FormatInt(held.ID, 10) + " holds the dedupe key"; ran || !strings.Contains(tb.msg, want) {
		t.Fatalf("the handler ran: %t; the test failed with %q, want a message with %q", ran, tb.msg, want)
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

// stubbornError is an error whose Error method panics with the error itself,
// so that the panic's value panics again when it is formatted.
type stubbornError struct{}

func (e *stubbornError) Error() string { panic(e) }

func TestRunReportsPanics(t *testing.T) {
	t.Parallel()
	client := taskmilltest.NewClient(t, testdb.Pool(t), nil)

	for name, value := range map[string]any{
		"a string": "boom",
		"a value that panics when it is formatted": &stubbornError{},
	} {
		h := taskmill.HandlerFunc[greet](func(context.Context, *taskmill.Task[greet]) error { panic(value) })
		info, err := taskmilltest.Run(t, client, h, greet{}, nil)
		if err == nil || info.State != taskmill.StateRetryable || info.Errors[0].Code != taskmill.CodePanic {
			t.Errorf("a handler that panics with %s: got %s with %+v, %v", name, info.State, info.Errors, err)
		}
	}
}

// measure has a payload that encodes but does not decode, because its field
// is an interface: the kind of mistake that a handler's test should catch.
type measure struct {
	Shape shape `json:"shape"`
}

func (measure) Kind() string { return "measure" }

type shape interface{ Area() float64 }

type square struct {
	Side float64 `json:"side"`
}

func (s square) Area() float64 { return s.Side * s.Side }

func TestRunReportsAHandlerThatDidNotRun(t *testing.T) {
	t.Parallel()
	client := taskmilltest.NewClient(t, testdb.Pool(t), nil)
	ran := false
	h := taskmill.HandlerFunc[measure](func(context.Context, *taskmill.Task[measure]) error {
		ran = true
		return nil
	})

	info, err := taskmilltest.Run(t, client, h, measure{Shape: square{Side: 2}}, nil)
	if ran || info.State != taskmill.StateDead || info.Errors[0].Code != taskmill.CodeInvalidPayload {
		t.Fatalf("the handler ran (%v), or the task is not dead of its payload: %s with %+v", ran, info.State, info.Errors)
	}
	// A test that checks only the error must not pass, and the error says why.
	if err == nil || !strings.Contains(err.Error(), info.Errors[0].Message) {
		t.Fatalf("got the error %v, want one with the reason: %s", err, info.Errors[0].Message)
	}
}

func TestExampleRuns(t *testing.T) {
	t.Parallel()
	testSignUp(t, testdb.Pool(t), func(int64) error { return nil })
}

// A worker that a test stops gives up the statements that it had sent, and
// the database can still run one of them when the test ends. It takes its
// locks in another order than the drop of the test's schema does: here it
// holds the activity table and then wants the tasks, which a drop locks long
// before the activity, as a rule. A drop that began meanwhile would wait for
// the statement, and the statement for the drop: PostgreSQL would end the
// drop, as a deadlock, and the test would fail at its end.
func TestNewClientDropsItsSchemaAfterTheStatementsThatItsTestGaveUp(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name   string
		shared bool // other tests use the pool while the schema is dropped
	}{
		{name: "on a pool of its own"},
		{name: "on a pool that other tests use", shared: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			pool := testdb.Pool(t)

			if tt.shared {
				stop, stopped := make(chan struct{}), make(chan struct{})
				go func() {
					defer close(stopped)
					for {
						select {
						case <-stop:
							return
						case <-time.After(time.Millisecond):
							_, _ = pool.Exec(context.Background(), "SELECT 1")
						}
					}
				}()
				// Clean-ups run last first: this one after the schema's drop.
				t.Cleanup(func() {
					close(stop)
					<-stopped
				})
			}

			client := taskmilltest.NewClient(t, pool, nil)
			schema := schemaOf(t, pool, client)
			testdb.GiveUp(t, pool, schema+".activity", schema+".tasks")

			// The test ends here, and NewClient drops the schema.
		})
	}
}

// schemaOf returns the schema that NewClient made for client: the one of the
// tasks table that an enqueue locks.
func schemaOf(t *testing.T, pool *pgxpool.Pool, client *taskmill.Client) string {
	t.Helper()
	var schema string
	err := pgx.BeginFunc(t.Context(), pool, func(tx pgx.Tx) error {
		if _, err := client.EnqueueTx(t.Context(), taskmill.Pgx(tx), greet{}, nil); err != nil {
			return err
		}
		return tx.QueryRow(t.Context(), `
SELECT n.nspname FROM pg_locks l JOIN pg_class c ON c.oid = l.relation JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE l.pid = pg_backend_pid() AND c.relname = 'tasks'`).Scan(&schema)
	})
	if err != nil {
		t.Fatal(err)
	}
	return schema
}
