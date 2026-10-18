package taskmill_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/code19m/taskmill"
	"github.com/code19m/taskmill/internal/testdb"
)

func TestMaintainerRescuesExpiredLeases(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ran := make(chan int, 1)
	runWorker(t, e, &taskmill.WorkerConfig{
		Handlers: handlers(func(_ context.Context, task *taskmill.Task[sendEmail]) error {
			ran <- task.Attempt
			return nil
		}, nil),
		Queues: defaultQueue(1),
	})

	// A worker that crashed while running the task.
	e.exec(t, `INSERT INTO {s}.tasks (queue, kind, state, run_at, payload, max_attempts, attempt, lease_token,
	                                 lease_expires_at, attempted_at, attempted_by)
	           VALUES ('default', 'email.send', 'running', now(), '{}', 5, 1, 1,
	                   now() - interval '1 second', now(), 'crashed')`)
	id := int64(e.count(t, "SELECT max(id) FROM {s}.tasks"))

	select {
	case attempt := <-ran:
		if attempt != 2 {
			t.Fatalf("rescued task ran as attempt %d, want 2", attempt)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("expired lease was not rescued")
	}
	info := e.waitState(t, id, taskmill.StateCompleted)
	if len(info.Errors) != 1 || info.Errors[0].Code != "lease_expired" {
		t.Fatalf("got errors %+v", info.Errors)
	}
}

func TestMaintainerExpiresDeadlines(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	runWorker(t, e, &taskmill.WorkerConfig{}) // maintenance only

	id := e.enqueue(t, sendEmail{}, &taskmill.EnqueueOptions{Queue: "unserved", Deadline: time.Now().Add(100 * time.Millisecond)})
	info := e.waitState(t, id, taskmill.StateDead)
	if len(info.Errors) != 1 || info.Errors[0].Code != "deadline_exceeded" {
		t.Fatalf("got errors %+v", info.Errors)
	}
}

func TestOneMaintainerIsElected(t *testing.T) {
	t.Parallel()
	e := newEnvWith(t, testdb.PoolSize(t, 16), nil)
	for i := range 3 {
		runWorker(t, e, &taskmill.WorkerConfig{ID: fmt.Sprintf("w%d", i)})
	}
	time.Sleep(time.Second)
	if n := e.count(t, "SELECT count(*) FROM {s}.leader"); n != 1 {
		t.Fatalf("got %d leaders, want 1", n)
	}
	if n := e.count(t, "SELECT count(*) FROM {s}.workers"); n != 3 {
		t.Fatalf("got %d registered workers, want 3", n)
	}
}

func TestRescueCapsTheBackoffOfManyAttempts(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	runWorker(t, e, &taskmill.WorkerConfig{}) // maintenance only

	// A task on attempt 2000 whose worker crashed: the doubling backoff would
	// overflow without a cap.
	e.exec(t, `INSERT INTO {s}.tasks (queue, kind, state, run_at, payload, max_attempts, attempt, lease_token,
	                                 lease_expires_at, attempted_at, attempted_by)
	           VALUES ('unserved', 'email.send', 'running', now(), '{}', 5000, 2000, 1,
	                   now() - interval '1 second', now(), 'crashed')`)
	id := int64(e.count(t, "SELECT max(id) FROM {s}.tasks"))

	info := e.waitState(t, id, taskmill.StateRetryable)
	if d := time.Until(info.RunAt); d < 50*time.Minute || d > 70*time.Minute {
		t.Fatalf("retry in %s, want about the maximum backoff of 1 hour", d)
	}
}

func TestRetentionFinishesAnInterruptedDetach(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	day := time.Now().UTC().AddDate(0, 0, -30).Truncate(24 * time.Hour)
	part := "task_history_" + day.Format("20060102")
	e.exec(t, fmt.Sprintf("CREATE TABLE {s}.%s (LIKE {s}.task_history INCLUDING DEFAULTS INCLUDING CONSTRAINTS)", part))
	e.exec(t, fmt.Sprintf("ALTER TABLE {s}.task_history ATTACH PARTITION {s}.%s FOR VALUES FROM ('%s') TO ('%s')",
		part, day.Format(time.RFC3339), day.AddDate(0, 0, 1).Format(time.RFC3339)))

	// A transaction that reads history makes the detach wait, and a timeout
	// interrupts it: the partition is left pending detach.
	reader := e.begin(t)
	e.txExec(t, reader, "SELECT count(*) FROM {s}.task_history")
	conn, err := e.pool.Acquire(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	detach := replaceSchema(fmt.Sprintf("ALTER TABLE {s}.task_history DETACH PARTITION {s}.%s CONCURRENTLY", part), e.schema)
	if _, err := conn.Exec(t.Context(), "SET statement_timeout = '200ms'"); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(t.Context(), detach); err == nil {
		t.Fatal("detach was not interrupted")
	}
	if _, err := conn.Exec(t.Context(), "RESET statement_timeout"); err != nil {
		t.Fatal(err)
	}
	conn.Release()
	if err := reader.Rollback(t.Context()); err != nil {
		t.Fatal(err)
	}
	pending := "SELECT count(*) FROM pg_inherits WHERE inhparent = '{s}.task_history'::regclass AND inhdetachpending"
	if e.count(t, pending) != 1 {
		t.Fatal("partition is not pending detach")
	}

	runWorker(t, e, &taskmill.WorkerConfig{}) // retention runs in the first cycle
	waitFor(t, "the expired partition to be dropped", func() bool {
		return e.count(t, "SELECT count(*) FROM pg_tables WHERE schemaname = $1 AND tablename = $2", e.schema, part) == 0
	})
}
