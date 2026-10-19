package taskmill_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
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
	if len(info.Errors) != 1 || info.Errors[0].Code != taskmill.CodeLeaseExpired {
		t.Fatalf("got errors %+v", info.Errors)
	}
}

func TestMaintenanceDoesNotWaitForHeldGroups(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	runWorker(t, e, &taskmill.WorkerConfig{}) // maintenance only

	// Two tasks whose worker crashed, one of them in a group that an
	// application transaction holds, as an enqueue into the group does. The
	// group is held before its task expires, so the maintainer never sees the
	// task with the group free.
	e.exec(t, "INSERT INTO {s}.groups (queue, group_key, tasks) VALUES ('unserved', 'g', 1)")
	tx := e.begin(t)
	e.txExec(t, tx, "SELECT FROM {s}.groups WHERE group_key = 'g' FOR UPDATE")
	expired := `INSERT INTO {s}.tasks (queue, kind, state, run_at, payload, max_attempts, attempt, lease_token,
	                                  lease_expires_at, attempted_at, attempted_by, group_key)
	            VALUES ('unserved', 'email.send', 'running', now(), '{}', 5, 1, 1,
	                    now() - interval '1 second', now(), 'crashed', $1)
	            RETURNING id`
	var grouped, plain int64
	if err := e.pool.QueryRow(t.Context(), replaceSchema(expired, e.schema), "g").Scan(&grouped); err != nil {
		t.Fatal(err)
	}
	if err := e.pool.QueryRow(t.Context(), replaceSchema(expired, e.schema), nil).Scan(&plain); err != nil {
		t.Fatal(err)
	}
	due := e.enqueue(t, sendEmail{}, &taskmill.EnqueueOptions{Queue: "unserved", RunAt: time.Now().Add(200 * time.Millisecond)})

	e.waitState(t, plain, taskmill.StateRetryable)
	e.waitState(t, due, taskmill.StateAvailable)
	e.wantState(t, grouped, taskmill.StateRunning)

	if err := tx.Commit(t.Context()); err != nil {
		t.Fatal(err)
	}
	e.waitState(t, grouped, taskmill.StateRetryable)
}

func TestOneMaintainerLeadsAndAnotherTakesOver(t *testing.T) {
	t.Parallel()
	e := newEnvWith(t, testdb.PoolSize(t, 16), nil)
	logs := make([]*logRecorder, 3)
	stops := make([]func(), 3)
	for i := range logs {
		logs[i] = &logRecorder{}
		stops[i] = runWorker(t, e.withLogger(t, slog.New(logs[i])), &taskmill.WorkerConfig{})
	}
	leader := func() (id string, expires time.Time) {
		_ = e.pool.QueryRow(t.Context(), replaceSchema("SELECT worker_id, expires_at FROM {s}.leader", e.schema)).
			Scan(&id, &expires)
		return id, expires
	}
	waitFor(t, "a maintainer to be elected", func() bool { id, _ := leader(); return id != "" })
	first, elected := leader()

	// While the leader renews its lease, the others keep trying to take over,
	// and fail.
	waitFor(t, "the leader to renew its lease twice", func() bool {
		_, expires := leader()
		return expires.Sub(elected) >= 2*testTiming.Maintainer
	})
	total, leading := 0, 0
	for i, l := range logs {
		if n := l.count("elected maintainer"); n > 0 {
			total, leading = total+n, i
		}
	}
	if total != 1 {
		t.Fatalf("%d workers were elected, want 1", total)
	}

	stops[leading]()
	waitFor(t, "another worker to take over", func() bool { id, _ := leader(); return id != "" && id != first })
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

func TestRetentionDeletesOldFinishedTasks(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.rawGroups = true // the halting head below is written by hand
	insert := `INSERT INTO {s}.tasks (queue, kind, state, run_at, payload, max_attempts, finalized_at, group_key)
	           VALUES ('default', 'email.send', $1, now(), '{}', 1, now() - $2 * interval '1 day', $3) RETURNING id`
	var oldDead, oldCanceled, recentDead, haltingHead int64
	for _, x := range []struct {
		id    *int64
		state string
		days  int
		group any
	}{
		{&oldDead, "dead", 40, nil},
		{&oldCanceled, "canceled", 40, nil},
		{&recentDead, "dead", 1, nil},
		{&haltingHead, "dead", 40, "ledger"},
	} {
		if err := e.pool.QueryRow(t.Context(), replaceSchema(insert, e.schema), x.state, x.days, x.group).Scan(x.id); err != nil {
			t.Fatal(err)
		}
	}
	e.exec(t, "INSERT INTO {s}.groups (queue, group_key, tasks, halted_task_id, halted_at) VALUES ('default', 'ledger', 1, $1, now())", haltingHead)
	e.exec(t, "INSERT INTO {s}.dedupe_keys (key, task_id, expires_at) VALUES (sha256('k'), $1, now() - interval '1 second')", recentDead)
	e.exec(t, "INSERT INTO {s}.workers (id, queues, hostname, version, heartbeat_at) VALUES ('gone', '{}', 'h', 'v', now() - interval '1 hour')")

	runWorker(t, e, &taskmill.WorkerConfig{}) // retention runs in the first cycle
	// Retention deletes stale workers last.
	waitFor(t, "the stale worker to be deleted", func() bool {
		return e.count(t, "SELECT count(*) FROM {s}.workers WHERE id = 'gone'") == 0
	})

	for _, id := range []int64{oldDead, oldCanceled} {
		if _, err := e.client.Get(t.Context(), id); !errors.Is(err, taskmill.ErrNotFound) {
			t.Errorf("old finished task %d: got %v, want ErrNotFound", id, err)
		}
	}
	e.wantState(t, recentDead, taskmill.StateDead)
	e.wantState(t, haltingHead, taskmill.StateDead) // it keeps its group halted
	if n := e.count(t, "SELECT count(*) FROM {s}.dedupe_keys"); n != 0 {
		t.Errorf("%d expired dedupe keys were kept", n)
	}
}

func TestRetentionSkipsRowsBeingChanged(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	insert := `INSERT INTO {s}.tasks (queue, kind, state, run_at, payload, max_attempts, finalized_at)
	           VALUES ('default', 'email.send', 'dead', now(), '{}', 1, now() - interval '40 days') RETURNING id`
	var retried, old int64
	for _, id := range []*int64{&retried, &old} {
		if err := e.pool.QueryRow(t.Context(), replaceSchema(insert, e.schema)).Scan(id); err != nil {
			t.Fatal(err)
		}
	}
	e.exec(t, `INSERT INTO {s}.dedupe_keys (key, task_id, expires_at)
	           VALUES (sha256('reclaimed'), $1, now() - interval '1 second'), (sha256('expired'), $1, now() - interval '1 second')`, old)

	// While retention runs, one transaction retries a dead task, and another
	// claims an expired dedupe key for a new window.
	tx := e.begin(t)
	e.txExec(t, tx, "UPDATE {s}.tasks SET state = 'available', finalized_at = NULL, attempt = 0 WHERE id = $1", retried)
	e.txExec(t, tx, "UPDATE {s}.dedupe_keys SET expires_at = now() + interval '1 hour' WHERE key = sha256('reclaimed')")
	runWorker(t, e, &taskmill.WorkerConfig{}) // retention runs in the first cycle
	waitFor(t, "retention to delete the rows nobody changes", func() bool {
		return e.count(t, "SELECT count(*) FROM {s}.tasks WHERE id = $1", old) == 0 &&
			e.count(t, "SELECT count(*) FROM {s}.dedupe_keys WHERE key = sha256('expired')") == 0
	})
	if err := tx.Commit(t.Context()); err != nil {
		t.Fatal(err)
	}

	e.wantState(t, retried, taskmill.StateAvailable)
	if n := e.count(t, "SELECT count(*) FROM {s}.dedupe_keys WHERE key = sha256('reclaimed')"); n != 1 {
		t.Fatal("retention deleted a dedupe key that was claimed again")
	}
}

func TestStatisticsCountEveryQueue(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.exec(t, `INSERT INTO {s}.tasks (queue, kind, state, run_at, payload, max_attempts)
	           SELECT 'big', 'email.send', 'available', now(), '{}', 1 FROM generate_series(1, 10001)`)
	for range 5 {
		e.enqueue(t, sendEmail{}, &taskmill.EnqueueOptions{Queue: "small"})
	}
	e.enqueue(t, sendEmail{}, &taskmill.EnqueueOptions{Queue: "small", RunAt: time.Now().Add(time.Hour)})
	runWorker(t, e, &taskmill.WorkerConfig{}) // the maintainer refreshes statistics

	var big, small taskmill.QueueInfo
	waitFor(t, "statistics for both queues", func() bool {
		queues, err := e.client.Queues(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		for _, q := range queues {
			switch q.Name {
			case "big":
				big = q
			case "small":
				small = q
			}
		}
		return !big.RefreshedAt.IsZero() && !small.RefreshedAt.IsZero()
	})
	if big.Counts[taskmill.StateAvailable] != 10_000 || !big.Capped {
		t.Errorf("big: got %v available, capped %v; want 10000, capped", big.Counts[taskmill.StateAvailable], big.Capped)
	}
	if small.Counts[taskmill.StateAvailable] != 5 || small.Counts[taskmill.StateScheduled] != 1 || small.Capped {
		t.Errorf("small: got %v, capped %v; want 5 available and 1 scheduled", small.Counts, small.Capped)
	}
}

func TestSlowRetentionDoesNotDelayPromotion(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	day := time.Now().UTC().AddDate(0, 0, -30).Truncate(24 * time.Hour)
	part := "task_history_" + day.Format("20060102")
	e.exec(t, fmt.Sprintf("CREATE TABLE {s}.%s (LIKE {s}.task_history INCLUDING DEFAULTS INCLUDING CONSTRAINTS)", part))
	e.exec(t, fmt.Sprintf("ALTER TABLE {s}.task_history ATTACH PARTITION {s}.%s FOR VALUES FROM ('%s') TO ('%s')",
		part, day.Format(time.RFC3339), day.AddDate(0, 0, 1).Format(time.RFC3339)))

	// A long reader of history, such as a report, makes the detach of the
	// expired partition wait.
	reader := e.begin(t)
	e.txExec(t, reader, "SELECT count(*) FROM {s}.task_history")

	started := make(chan time.Time, 1)
	runWorker(t, e, &taskmill.WorkerConfig{
		Handlers: handlers(func(context.Context, *taskmill.Task[sendEmail]) error {
			started <- time.Now()
			return nil
		}, nil),
		Queues: defaultQueue(1),
	})
	e.waitBlocked(t, "DETACH PARTITION")
	runAt := time.Now().Add(200 * time.Millisecond)
	e.enqueue(t, sendEmail{}, &taskmill.EnqueueOptions{RunAt: runAt})
	select {
	case at := <-started:
		if late := at.Sub(runAt); late > 3*time.Second {
			t.Fatalf("the task started %s late", late)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the scheduled task was not promoted while retention waited")
	}
	if err := reader.Rollback(t.Context()); err != nil {
		t.Fatal(err)
	}
}
