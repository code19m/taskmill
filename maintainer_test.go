package taskmill_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"math"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/code19m/taskmill"
	"github.com/code19m/taskmill/internal/testdb"
)

func TestMaintainerRescuesExpiredLeases(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	logs := &logRecorder{}
	ran := make(chan int, 1)
	runWorker(t, e.withLogger(t, slog.New(logs)), &taskmill.WorkerConfig{
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
	if len(info.Errors) != 1 || info.Errors[0].Code != taskmill.CodeLeaseLost {
		t.Fatalf("got errors %+v", info.Errors)
	}

	// A task whose crashed attempt was its last dies, and that needs a person.
	last := e.count(t, `INSERT INTO {s}.tasks (queue, kind, state, run_at, payload, max_attempts, attempt, lease_token,
	                                          lease_expires_at, attempted_at, attempted_by)
	                    VALUES ('default', 'email.send', 'running', now(), '{}', 1, 1, 1,
	                            now() - interval '1 second', now(), 'crashed')
	                    RETURNING id`)
	e.waitState(t, int64(last), taskmill.StateDead)
	waitFor(t, "the dead task to be logged", func() bool { return logs.count("attempt failed; the task is dead") == 1 })
	for _, r := range logs.all() {
		if r.Message == "attempt failed; the task is dead" && (r.Level != slog.LevelError || attr(r, "task_id") != strconv.Itoa(last)) {
			t.Errorf("the dead task was logged at %s with task_id %s, want an error with %d", r.Level, attr(r, "task_id"), last)
		}
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

// Every worker that does not lead tries to, every election interval. A try
// writes only when it takes or renews the lease: while another worker's
// lease lasts, it changes nothing, so it must not cost a transaction ID and
// a WAL flush either.
func TestElectionTryWritesOnlyWhenItWins(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	try := func(worker string, wantLead bool) {
		t.Helper()
		leads, wrote, err := taskmill.Elect(t.Context(), e.client, worker)
		if err != nil {
			t.Fatal(err)
		}
		if leads != wantLead || wrote != wantLead {
			t.Fatalf("worker %s: leads %t, wrote %t; want both %t", worker, leads, wrote, wantLead)
		}
	}
	try("a", true) // nobody leads
	try("a", true) // it renews its lease
	try("b", false)
	// The leader crashed, and its lease ran out.
	e.exec(t, "UPDATE {s}.leader SET expires_at = now() - interval '1 second'")
	try("b", true)
	try("a", false)
}

// TestMaintainerStoppedWhileRenewingResigns checks that a maintainer that
// stops while it renews its lease still resigns, so that another worker
// takes over at once, not once the lease expires.
func TestMaintainerStoppedWhileRenewingResigns(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	stop := runWorker(t, e, &taskmill.WorkerConfig{}) // maintenance only
	waitFor(t, "a maintainer", func() bool { return e.count(t, "SELECT count(*) FROM {s}.leader") == 1 })

	// The test holds the lease's row, so the next renewal waits, as it does
	// on a slow database or for a full pool, and the worker stops meanwhile.
	tx := e.begin(t)
	e.txExec(t, tx, "SELECT FROM {s}.leader FOR UPDATE")
	e.waitBlocked(t, "INSERT INTO {s}.leader")
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		stop()
	}()

	// A worker that resigns waits for the row too, until the transaction
	// ends; one that does not stops at once.
	waitFor(t, "the worker to resign or stop", func() bool {
		select {
		case <-stopped:
			return true
		default:
			return e.count(t, `SELECT count(*) FROM pg_stat_activity
			                   WHERE wait_event_type = 'Lock' AND strpos(query, $1) > 0`,
				replaceSchema("DELETE FROM {s}.leader", e.schema)) > 0
		}
	})
	if err := tx.Rollback(t.Context()); err != nil {
		t.Fatal(err)
	}
	<-stopped
	if n := e.count(t, "SELECT count(*) FROM {s}.leader"); n != 0 {
		t.Fatal("the maintainer stopped without resigning; another worker waits for its lease to expire")
	}
}

// A worker can hold the maintainer's lease without knowing it: a renewal
// failed, as here, or the answer to its election was lost. When it stops, it
// must give the lease up all the same.
func TestWorkerThatForgotItLeadsStillResigns(t *testing.T) {
	t.Parallel()
	logs := &logRecorder{}
	e := newEnv(t).withLogger(t, slog.New(logs))
	stop := runWorker(t, e, &taskmill.WorkerConfig{}) // maintenance only
	waitFor(t, "a maintainer", func() bool { return e.count(t, "SELECT count(*) FROM {s}.leader") == 1 })

	// From now on, every renewal fails, and the lease stays in the table.
	e.exec(t, "ALTER TABLE {s}.leader ADD CONSTRAINT no_renewal CHECK (false) NOT VALID")
	waitFor(t, "the worker to stop leading", func() bool { return logs.count("no longer maintainer") == 1 })

	stop()
	if n := e.count(t, "SELECT count(*) FROM {s}.leader"); n != 0 {
		t.Fatal("the worker stopped without resigning; another worker waits for its lease to expire")
	}
}

// TestStoppingWorkerHandsMaintenanceOverBeforeItDrains checks that a worker
// resigns as maintainer when it starts to stop, not when its running tasks
// end: another worker leads at once, and enqueues the runs of its schedules.
func TestStoppingWorkerHandsMaintenanceOverBeforeItDrains(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ctx := t.Context()

	// The old worker leads, and runs a task that ends only when the test
	// says so.
	started, unblock := make(chan struct{}, 1), make(chan struct{})
	stopOld := runWorker(t, e, &taskmill.WorkerConfig{
		Handlers: handlers(func(context.Context, *taskmill.Task[sendEmail]) error {
			started <- struct{}{}
			<-unblock
			return nil
		}, nil),
		Queues:       defaultQueue(1),
		DrainTimeout: time.Minute,
	})
	finish := sync.OnceFunc(func() { close(unblock) })
	t.Cleanup(finish) // before the worker's own cleanup, which waits for the task
	e.enqueue(t, sendEmail{}, nil)
	recv(t, started, "the old worker's task to start")
	waitFor(t, "the old worker to lead", func() bool { return e.count(t, "SELECT count(*) FROM {s}.leader") == 1 })

	// The new worker has a schedule that the old one lacks, as in a deploy
	// that adds one.
	var hs taskmill.Handlers
	ran := make(chan struct{}, 1)
	taskmill.AddHandler(&hs, taskmill.HandlerFunc[report](func(context.Context, *taskmill.Task[report]) error {
		ran <- struct{}{}
		return nil
	}), nil)
	runWorker(t, e, &taskmill.WorkerConfig{
		Handlers:  &hs,
		Queues:    map[string]taskmill.QueueOptions{"reports": {Concurrency: 1}},
		Schedules: []taskmill.Schedule{{Name: "yearly-report", Cron: "@yearly", Args: report{}}},
	})

	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		stopOld()
	}()
	// Only the maintainer stores its schedules, and enqueues their runs.
	waitFor(t, "the new worker to lead, and store its schedule", func() bool {
		schedules, err := e.client.Schedules(ctx)
		if err != nil {
			t.Fatal(err)
		}
		return len(schedules) == 1
	})
	if err := e.client.RunSchedule(ctx, "yearly-report"); err != nil {
		t.Fatal(err)
	}
	recv(t, ran, "the schedule's run")
	select {
	case <-stopped:
		t.Fatal("the old worker stopped before its task ended, so the test did not see it drain")
	default:
	}
	finish()
	<-stopped
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

func TestPromotionStaysOnItsIndexBesideABacklog(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	// Run times in the past belong to available tasks, and those of
	// scheduled tasks are a year ahead. The statistics see the two columns
	// apart, so they suggest that a quarter of the tasks are due.
	e.exec(t, `INSERT INTO {s}.tasks (queue, kind, state, run_at, payload, max_attempts)
	           SELECT 'default', 'email.send',
	                  CASE WHEN i % 2 = 0 THEN 'available' ELSE 'scheduled' END::{s}.task_state,
	                  now() + CASE WHEN i % 2 = 0 THEN interval '-1 hour' ELSE interval '1 year' END, '{}', 1
	           FROM generate_series(1, 100000) i`)
	e.exec(t, "ANALYZE {s}.tasks")

	plan, err := taskmill.ExplainPromote(t.Context(), e.client)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(plan, "Seq Scan") || !strings.Contains(plan, "Index Scan using tasks_due_idx") {
		t.Fatalf("promotion does not read its indexes:\n%s", plan)
	}
}

// Retention reads the tasks it deletes, not the table and not every old
// task, though the planner cannot know the age from which a task is old.
func TestRetentionReadsOnlyTheTasksItDeletes(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	// Tasks that wait, and after them as many dead tasks of an hour ago:
	// with so many, a plan that does not know the age expects to meet an
	// old task soon wherever it reads. The table is large enough that the
	// planner deletes the picked tasks by their IDs, at half the cost of
	// reading the table for them.
	e.exec(t, `INSERT INTO {s}.tasks (queue, kind, state, run_at, payload, max_attempts, finalized_at)
	           SELECT 'q' || i % 10, 'email.send', CASE WHEN i > 200000 THEN 'dead' ELSE 'scheduled' END::{s}.task_state,
	                  now() + interval '1 year', '{}', 1, CASE WHEN i > 200000 THEN now() - interval '1 hour' END
	           FROM generate_series(1, 400000) i`)
	e.exec(t, "ANALYZE {s}.tasks")

	for _, c := range []struct {
		what, retention string
		most            int64
	}{
		{"no task is old enough", "30 days", 100},
		// A batch reads its tasks once to pick them, and once to delete them.
		{"every dead task is old enough", "1 minute", 2100},
	} {
		e.exec(t, "UPDATE {s}.settings SET dead_retention = $1::interval", c.retention)
		n, err := taskmill.RetentionRowsRead(t.Context(), e.client)
		if err != nil {
			t.Fatal(err)
		}
		if n > c.most {
			t.Errorf("%s: a batch read %d rows of tasks, want at most %d", c.what, n, c.most)
		}
	}
}

func TestMaintainerWakesQueuesWhoseTasksWait(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	e.exec(t, `INSERT INTO {s}.tasks (queue, kind, state, run_at, payload, max_attempts)
	           SELECT q, 'email.send', 'available', now() - interval '1 minute', '{}', 1
	           FROM unnest(ARRAY['waiting', 'paused']) q`)
	e.enqueue(t, sendEmail{}, &taskmill.EnqueueOptions{Queue: "fresh"})
	if err := e.client.PauseQueue(ctx, "paused"); err != nil {
		t.Fatal(err)
	}

	conn, err := pgx.Connect(ctx, testdb.DirectURL(t))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close(t.Context()) }()
	if _, err := conn.Exec(ctx, "LISTEN "+e.schema); err != nil {
		t.Fatal(err)
	}
	// Tasks that waited longer than a maintenance interval wake their queue.
	w := newWorker(t, e, &taskmill.WorkerConfig{Handlers: noopHandlers()})
	timing := testTiming
	timing.Maintainer = 10 * time.Second
	taskmill.SetTiming(w, timing)
	taskmill.WakeQueues(ctx, w)

	for _, want := range []string{"a|waiting", "h|"} {
		n, err := conn.WaitForNotification(ctx)
		if err != nil || n.Payload != want {
			t.Fatalf("got notification %v, %v; want %s", n, err, want)
		}
	}
	quiet, stop := context.WithTimeout(ctx, 200*time.Millisecond)
	defer stop()
	if n, err := conn.WaitForNotification(quiet); err == nil {
		t.Fatalf("got notification %q; a paused queue, or one whose task just arrived, is not woken", n.Payload)
	}
}

func TestRetentionFinishesAnInterruptedDetach(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	part := e.attachHistory(t, time.Now().UTC().AddDate(0, 0, -30).Truncate(24*time.Hour))

	// A transaction that reads history makes the detach wait, and a timeout
	// interrupts it: the partition is left pending detach.
	// The timeout is a session setting, which needs a connection of its own.
	reader := e.begin(t)
	e.txExec(t, reader, "SELECT count(*) FROM {s}.task_history")
	conn, err := pgx.Connect(t.Context(), testdb.DirectURL(t))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close(t.Context()) }()
	detach := replaceSchema(fmt.Sprintf("ALTER TABLE {s}.task_history DETACH PARTITION {s}.%s CONCURRENTLY", part), e.schema)
	if _, err := conn.Exec(t.Context(), "SET statement_timeout = '200ms'"); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(t.Context(), detach); err == nil {
		t.Fatal("detach was not interrupted")
	}
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

// attachHistory attaches a history partition for the UTC day that starts at
// day, as the maintainer creates them, and returns its name.
func (e *env) attachHistory(t *testing.T, day time.Time) string {
	t.Helper()
	part := "task_history_" + day.Format("20060102")
	e.exec(t, fmt.Sprintf("CREATE TABLE {s}.%s (LIKE {s}.task_history INCLUDING DEFAULTS INCLUDING CONSTRAINTS)", part))
	e.exec(t, fmt.Sprintf("ALTER TABLE {s}.task_history ATTACH PARTITION {s}.%s FOR VALUES FROM ('%s') TO ('%s')",
		part, day.Format(time.RFC3339), day.AddDate(0, 0, 1).Format(time.RFC3339)))
	return part
}

// newClockEnv returns an env on a database whose clock the test sets: its
// pool's sessions find a now() of their own before PostgreSQL's, which
// returns at until set changes it. Their time zone is zone.
func newClockEnv(t *testing.T, at time.Time, zone string) (e *env, set func(time.Time)) {
	t.Helper()
	admin := testdb.Pool(t)
	clock := testdb.Schema(t, admin)
	for _, sql := range []string{
		"CREATE SCHEMA " + clock,
		"CREATE TABLE " + clock + ".clock (at timestamptz NOT NULL)",
		"INSERT INTO " + clock + ".clock VALUES ('" + at.Format(time.RFC3339) + "')",
		"CREATE FUNCTION " + clock + ".now() RETURNS timestamptz LANGUAGE sql STABLE AS $$ SELECT at FROM " + clock + ".clock $$",
	} {
		if _, err := admin.Exec(t.Context(), sql); err != nil {
			t.Fatal(err)
		}
	}
	cfg, err := pgxpool.ParseConfig(testdb.DirectURL(t)) // a proxy refuses the parameters
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = clock + ", pg_catalog"
	cfg.ConnConfig.RuntimeParams["timezone"] = zone
	pool, err := pgxpool.NewWithConfig(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return newEnvWith(t, pool, nil), func(at time.Time) {
		t.Helper()
		if _, err := admin.Exec(t.Context(), "UPDATE "+clock+".clock SET at = $1", at); err != nil {
			t.Fatal(err)
		}
	}
}

// A completed task's day is the database's, so the history partitions must
// follow the database's clock too, wherever the application's clock stands:
// here, 30 days ahead of the database's.
func TestHistoryPartitionsFollowTheDatabaseClock(t *testing.T) {
	t.Parallel()
	e, _ := newClockEnv(t, time.Now().AddDate(0, 0, -30), "UTC")
	w := newWorker(t, e, &taskmill.WorkerConfig{Handlers: noopHandlers(), NoMaintainer: true})

	id := e.enqueue(t, sendEmail{}, nil)
	if _, err := w.RunTask(t.Context(), id); err != nil {
		t.Fatal(err)
	}
	if _, err := e.client.Get(t.Context(), id); err != nil {
		t.Fatalf("the history has no partition for the database's day: %v", err)
	}
	taskmill.RunRetention(t.Context(), w)
	if _, err := e.client.Get(t.Context(), id); err != nil {
		t.Fatalf("retention dropped the history of the database's day: %v", err)
	}
}

// The default periods count days. For retention a day is 24 hours, whatever
// the sessions' time zone: here one whose clocks went forward that week, so
// that 7 of its days are 167 hours.
func TestRetentionCountsDaysOf24Hours(t *testing.T) {
	t.Parallel()
	// In half an hour, at midnight UTC, the history of 25 March will be 7
	// days old, and a task that died at midnight on 3 March 30 days.
	at := time.Date(2026, time.April, 1, 23, 30, 0, 0, time.UTC)
	e, setNow := newClockEnv(t, at, "Europe/Berlin")
	w := newWorker(t, e, &taskmill.WorkerConfig{NoMaintainer: true})
	part := e.attachHistory(t, time.Date(2026, time.March, 25, 0, 0, 0, 0, time.UTC))
	e.exec(t, `INSERT INTO {s}.tasks (queue, kind, state, run_at, payload, max_attempts, finalized_at)
	           VALUES ('default', 'email.send', 'dead', now(), '{}', 1, '2026-03-03 00:00:00+00')`)
	kept := func() (history, dead int) {
		return e.count(t, "SELECT count(*) FROM pg_tables WHERE schemaname = $1 AND tablename = $2", e.schema, part),
			e.count(t, "SELECT count(*) FROM {s}.tasks")
	}

	taskmill.RunRetention(t.Context(), w)
	if history, dead := kept(); history != 1 || dead != 1 {
		t.Fatalf("half an hour early, retention kept %d of 1 day of history and %d of 1 dead task", history, dead)
	}
	setNow(at.Add(time.Hour))
	taskmill.RunRetention(t.Context(), w)
	if history, dead := kept(); history != 0 || dead != 0 {
		t.Fatalf("half an hour late, retention kept %d day of history and %d dead task", history, dead)
	}
}

// A period can be longer than any history: the longest duration is how Go
// code says "for good", and SQL can store thousands of years.
func TestLongestHistoryRetentionKeepsHistory(t *testing.T) {
	t.Parallel()
	longest := time.Duration(math.MaxInt64)
	for _, tc := range []struct {
		name string
		set  func(t *testing.T, e *env)
	}{
		{"the longest duration", func(t *testing.T, e *env) {
			t.Helper()
			if err := e.client.UpdateSettings(t.Context(), func(s *taskmill.Settings) { s.HistoryRetention = longest }); err != nil {
				t.Fatal(err)
			}
		}},
		// Its cutoff is a year before our era, which has the digits of a
		// year long after today.
		{"5,000 years, stored by hand", func(t *testing.T, e *env) {
			t.Helper()
			e.exec(t, "UPDATE {s}.settings SET history_retention = interval '5000 years'")
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			e := newEnv(t)
			w := newWorker(t, e, &taskmill.WorkerConfig{Handlers: noopHandlers(), NoMaintainer: true})
			id := e.enqueue(t, sendEmail{}, nil)
			if _, err := w.RunTask(t.Context(), id); err != nil {
				t.Fatal(err)
			}
			tc.set(t, e)

			taskmill.RunRetention(t.Context(), w)
			if _, err := e.client.Get(t.Context(), id); err != nil {
				t.Fatalf("retention dropped history that is kept for good: %v", err)
			}
			// The database stores whole microseconds, and Go reads at most
			// its longest duration.
			s, err := e.client.Settings(t.Context())
			if err != nil || s.HistoryRetention != longest.Truncate(time.Microsecond) {
				t.Fatalf("got a history retention of %s, %v; want %s", s.HistoryRetention, err, longest.Truncate(time.Microsecond))
			}
		})
	}
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
	// More than two batches of old tasks in a second queue.
	e.exec(t, `INSERT INTO {s}.tasks (queue, kind, state, run_at, payload, max_attempts, finalized_at)
	           SELECT 'reports', 'report.build', 'canceled', now(), '{}', 1, now() - interval '40 days'
	           FROM generate_series(1, 2500)`)
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
	if n := e.count(t, "SELECT count(*) FROM {s}.tasks WHERE queue = 'reports'"); n != 0 {
		t.Errorf("%d old tasks of the second queue were kept", n)
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
	insert := func(queue, state, runAt string, n int) {
		e.exec(t, `INSERT INTO {s}.tasks (queue, kind, state, run_at, payload, max_attempts)
		           SELECT $1, 'email.send', $2::{s}.task_state, now() + $3::interval, '{}', 1 FROM generate_series(1, $4)`,
			queue, state, runAt, n)
	}
	// Each count stops at 10,000 per queue and state, and is capped only if
	// more tasks are left, also for scheduled tasks, which are counted
	// across all queues.
	insert("big", "available", "0 s", 10_001)
	insert("exact", "available", "0 s", 10_000)
	insert("later", "scheduled", "1 hour", 15_000)
	insert("later-exact", "scheduled", "1 hour", 10_000)
	for range 5 {
		e.enqueue(t, sendEmail{}, &taskmill.EnqueueOptions{Queue: "small"})
	}
	e.enqueue(t, sendEmail{}, &taskmill.EnqueueOptions{Queue: "small", RunAt: time.Now().Add(time.Hour)})
	// Halted groups stop at 10,000 per queue too.
	e.exec(t, `WITH dead AS (
	               INSERT INTO {s}.tasks (queue, kind, state, run_at, payload, max_attempts, attempt, group_key, finalized_at)
	               SELECT 'halted', 'email.send', 'dead', now(), '{}', 1, 1, 'order-' || i, now() FROM generate_series(1, 10001) i
	               RETURNING id, group_key
	           )
	           INSERT INTO {s}.groups (queue, group_key, tasks, halted_task_id, halted_at)
	           SELECT 'halted', group_key, 1, id, now() FROM dead`)
	// As in an outage, more tasks wait to retry than the 10,000 whose
	// failures are read. New failures retry first, and the task that failed
	// most retries after them, though not last: tasks that failed a little
	// less retry later, as they failed more recently. And a queue whose
	// tasks all retry before the others' is read too.
	e.exec(t, `INSERT INTO {s}.tasks (queue, kind, state, run_at, payload, max_attempts, attempt)
	           SELECT v.queue, 'email.send', 'retryable', now() + v.wait + i * interval '1 ms', '{}', 25, v.attempt
	           FROM (VALUES ('retrying', 2, interval '1 minute', 10000), ('retrying', 12, interval '4 minutes', 1),
	                        ('retrying', 9, interval '5 minutes', 5), ('early', 5, interval '30 seconds', 1))
	                AS v(queue, attempt, wait, n),
	                generate_series(1, v.n) AS i`)
	runWorker(t, e, &taskmill.WorkerConfig{}) // the maintainer refreshes statistics

	counted := countedQueues(t, e, "big", "exact", "later", "later-exact", "small", "halted", "retrying", "early")
	for _, c := range []struct {
		queue string
		state taskmill.State
		want  taskmill.TaskCount
	}{
		{"big", taskmill.StateAvailable, taskmill.TaskCount{N: 10_000, Capped: true}},
		{"exact", taskmill.StateAvailable, taskmill.TaskCount{N: 10_000}},
		{"later", taskmill.StateScheduled, taskmill.TaskCount{N: 10_000, Capped: true}},
		{"later-exact", taskmill.StateScheduled, taskmill.TaskCount{N: 10_000}},
		{"halted", taskmill.StateDead, taskmill.TaskCount{N: 10_000, Capped: true}},
		{"retrying", taskmill.StateRetryable, taskmill.TaskCount{N: 10_000, Capped: true}},
	} {
		if got := counted[c.queue].Counts[c.state]; got != c.want {
			t.Errorf("%s: got %+v %s; want %+v", c.queue, got, c.state, c.want)
		}
	}
	if got := counted["halted"].HaltedGroups; got != 10_000 {
		t.Errorf("halted: got %d halted groups, want 10,000", got)
	}
	for queue, want := range map[string]int{"retrying": 12, "early": 5} {
		if got := counted[queue].MostFailures; got != want {
			t.Errorf("%s: got %d most failures, want %d", queue, got, want)
		}
	}
	want := map[taskmill.State]taskmill.TaskCount{
		taskmill.StateAvailable: {N: 5},
		taskmill.StateScheduled: {N: 1},
	}
	if got := counted["small"].Counts; !maps.Equal(got, want) {
		t.Errorf("small: got %+v; want %+v", got, want)
	}
}

func TestStatisticsCapTheDueCountsOfEveryQueue(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.exec(t, `INSERT INTO {s}.tasks (queue, kind, state, run_at, payload, max_attempts)
	           SELECT 'flood', 'email.send', 'scheduled', now() + interval '1 hour', '{}', 1 FROM generate_series(1, 100001)`)
	for range 3 {
		e.enqueue(t, sendEmail{}, &taskmill.EnqueueOptions{Queue: "small"})
	}
	e.exec(t, `INSERT INTO {s}.tasks (queue, kind, state, run_at, payload, max_attempts, finalized_at)
	           SELECT 'small', 'email.send', 'dead', now(), '{}', 1, now() FROM generate_series(1, 2)`)
	e.exec(t, `INSERT INTO {s}.queues (name) VALUES ('idle')`)
	runWorker(t, e, &taskmill.WorkerConfig{}) // the maintainer refreshes statistics

	// The shared cap was passed by flood alone, so small may have scheduled and
	// retryable tasks that were not counted, but its other counts are exact.
	counted := countedQueues(t, e, "flood", "small", "idle")
	want := map[taskmill.State]taskmill.TaskCount{
		taskmill.StateAvailable: {N: 3},
		taskmill.StateDead:      {N: 2},
		taskmill.StateCanceled:  {N: 0},
		taskmill.StateScheduled: {N: 0, Capped: true},
		taskmill.StateRetryable: {N: 0, Capped: true},
	}
	if got := counted["small"].Counts; !maps.Equal(got, want) {
		t.Errorf("small: got %+v; want %+v", got, want)
	}
	want = map[taskmill.State]taskmill.TaskCount{
		taskmill.StateScheduled: {N: 10_000, Capped: true},
		taskmill.StateRetryable: {N: 0, Capped: true},
	}
	if got := counted["flood"].Counts; !maps.Equal(got, want) {
		t.Errorf("flood: got %+v; want %+v", got, want)
	}
	want = map[taskmill.State]taskmill.TaskCount{
		taskmill.StateAvailable: {N: 0},
		taskmill.StateScheduled: {N: 0, Capped: true},
		taskmill.StateRetryable: {N: 0, Capped: true},
	}
	if got := counted["idle"].Counts; !maps.Equal(got, want) { // a known queue without tasks
		t.Errorf("idle: got %+v; want %+v", got, want)
	}
}

// countedQueues waits until the maintainer has counted the named queues, and
// returns them by name.
func countedQueues(t *testing.T, e *env, names ...string) map[string]taskmill.QueueInfo {
	t.Helper()
	out := make(map[string]taskmill.QueueInfo)
	waitFor(t, "statistics for every queue", func() bool {
		queues, err := e.client.Queues(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		for _, q := range queues {
			if !q.RefreshedAt.IsZero() {
				out[q.Name] = q
			}
		}
		for _, name := range names {
			if _, ok := out[name]; !ok {
				return false
			}
		}
		return true
	})
	return out
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
