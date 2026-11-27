package taskmill_test

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/code19m/taskmill"
)

func TestActivityCountsAttempts(t *testing.T) {
	t.Parallel()
	e := newEnv(t)

	stop := runWorker(t, e, &taskmill.WorkerConfig{
		Handlers: handlers(func(_ context.Context, task *taskmill.Task[sendEmail]) error {
			if task.Args.UserID == 1 {
				return taskmill.Permanent(errors.New("no such user"))
			}
			return nil
		}, nil),
		Queues: defaultQueue(2),
	})

	ok1, ok2 := e.enqueue(t, sendEmail{}, nil), e.enqueue(t, sendEmail{}, nil)
	bad := e.enqueue(t, sendEmail{UserID: 1}, nil)
	e.waitState(t, ok1, taskmill.StateCompleted)
	e.waitState(t, ok2, taskmill.StateCompleted)
	e.waitState(t, bad, taskmill.StateDead)

	// The maintainer counts the tasks each refresh; then a worker that stops
	// writes the attempts it counted.
	waitFor(t, "the maintainer to count the dead task", func() bool {
		points, err := e.client.Activity(t.Context(), nil)
		return err == nil && len(points) > 0 && points[len(points)-1].Counts[taskmill.StateDead] == 1
	})
	stop()

	points, err := e.client.Activity(t.Context(), &taskmill.ActivityParams{Queues: []string{taskmill.DefaultQueue}})
	if err != nil {
		t.Fatal(err)
	}

	var completed, failed int64
	for _, p := range points {
		completed, failed = completed+p.Completed, failed+p.Failed
	}
	if completed != 2 || failed != 1 {
		t.Fatalf("got %d completed and %d failed attempts, want 2 and 1: %+v", completed, failed, points)
	}
}

// A worker adds its counts to the activity once, also when it cannot tell
// whether a write of them committed: a write that runs out of time may
// commit all the same, as one whose commit waits for a standby, or not.
// Here a trigger delays the first write that adds completed attempts past
// the write's deadline, before or at its commit, and records in
// slow_writes whether that write committed.
func TestActivityCountsAWriteInDoubtOnce(t *testing.T) {
	t.Parallel()
	const (
		atCommit = "CONSTRAINT TRIGGER slow_write AFTER INSERT OR UPDATE ON {s}.activity DEFERRABLE INITIALLY DEFERRED"
		// The trigger goes on through cancels, the worker's at its deadline
		// or others that a pooler sends, as a commit that waits for a
		// standby does. Only a cancel in the microseconds between two steps
		// ends it, so a step does not divide the worker's deadline: its
		// cancel lands well inside one.
		committing = `FOR i IN 1..29 LOOP
            BEGIN
                PERFORM pg_sleep(0.07);
            EXCEPTION WHEN query_canceled THEN
            END;
        END LOOP;`
	)
	for _, c := range []struct {
		name      string
		trigger   string // the trigger, and when it runs
		slow      string // what it does: it takes longer than the write's deadline
		committed bool   // whether the slow write commits
	}{
		{"a commit that ends after the deadline", atCommit, committing, true},
		{"a commit that fails after the deadline", atCommit, committing + " RAISE EXCEPTION 'no';", false},
		{"a statement that the deadline cancels", "TRIGGER slow_write BEFORE INSERT OR UPDATE ON {s}.activity", "PERFORM pg_sleep(1);", false},
	} {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			e := newEnv(t)
			e.exec(t, `CREATE TABLE {s}.slow_writes (at timestamptz)`)
			e.exec(t, `CREATE SEQUENCE {s}.slow_once`)
			e.exec(t, `
CREATE FUNCTION {s}.slow_write() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.completed > coalesce(OLD.completed, 0) AND nextval('{s}.slow_once') = 1 THEN
        INSERT INTO {s}.slow_writes VALUES (now());
        `+c.slow+`
    END IF;
    RETURN NEW;
END $$`)
			e.exec(t, `CREATE `+c.trigger+` FOR EACH ROW EXECUTE FUNCTION {s}.slow_write()`)

			w := newWorker(t, e, &taskmill.WorkerConfig{Handlers: noopHandlers(), Queues: defaultQueue(5)})
			taskmill.SetTiming(w, taskmill.Timing{Activity: 300 * time.Millisecond})
			ids := make([]int64, 5)
			for i := range ids {
				ids[i] = e.enqueue(t, sendEmail{}, nil)
			}
			stop := startWorker(t, w)
			for _, id := range ids {
				e.waitState(t, id, taskmill.StateCompleted)
			}
			waitFor(t, "the counts of the 5 tasks", func() bool { return completedAttempts(t, e) >= 5 })
			stop()

			if n := completedAttempts(t, e); n != 5 {
				t.Errorf("the activity counts %d completed attempts, want 5", n)
			}
			if e.count(t, `SELECT count(*) FROM {s}.slow_once WHERE is_called`) == 0 {
				t.Fatal("no write was slow")
			}
			if got := e.count(t, `SELECT count(*) FROM {s}.slow_writes`) == 1; got != c.committed {
				t.Errorf("the slow write committed: %t, want %t", got, c.committed)
			}
		})
	}
}

// A write that the worker tries again adds its counts once: if the first
// try was lost, as in a failover to a standby that lacks it, and not again
// if the first try committed. The same flush then writes the counts made
// meanwhile, under a number of their own, so that they count also when the
// first try committed. The next write adds its own counts, except those
// that the worker could not write for an hour.
func TestActivityWriteTriedAgainAddsItsCountsOnce(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	w := newWorker(t, e, &taskmill.WorkerConfig{Handlers: noopHandlers(), Queues: defaultQueue(1)})

	for _, c := range []struct {
		how       string
		number    int64
		age       time.Duration
		meanwhile int // attempts counted while the write failed
		want      int
	}{
		{"a write whose first try was lost", 1, 0, 0, 3},
		{"the same write, whose first try committed, with counts made meanwhile", 1, 0, 2, 5},
		{"the next write, with counts made meanwhile", 3, 0, 2, 10},
		{"a write of counts from over an hour ago", 5, 61 * time.Minute, 0, 10},
	} {
		taskmill.Tally(w, taskmill.DefaultQueue, c.meanwhile)
		taskmill.FlushActivityAgain(t.Context(), w, c.number, taskmill.DefaultQueue, 3, c.age)
		if n := e.count(t, "SELECT coalesce(sum(completed), 0) FROM {s}.activity"); n != c.want {
			t.Fatalf("after %s, the activity counts %d completed attempts, want %d", c.how, n, c.want)
		}
	}
}

// A write that fails is tried again before any other: the counts made
// meanwhile wait, so that a later write cannot take the number that the
// failed write must still claim. Here a transaction holds activity_writes,
// and the writes fail at their deadline.
func TestActivityWriteThatFailedGoesFirst(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	w := newWorker(t, e, &taskmill.WorkerConfig{Handlers: noopHandlers(), Queues: defaultQueue(1)})

	busy := e.begin(t)
	e.txExec(t, busy, "LOCK TABLE {s}.activity_writes")
	ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
	taskmill.FlushActivityAgain(ctx, w, 1, taskmill.DefaultQueue, 3, 0)
	cancel()
	taskmill.Tally(w, taskmill.DefaultQueue, 2)
	ctx, cancel = context.WithTimeout(t.Context(), 200*time.Millisecond)
	taskmill.FlushActivity(ctx, w)
	cancel()
	if err := busy.Rollback(t.Context()); err != nil {
		t.Fatal(err)
	}

	taskmill.FlushActivity(t.Context(), w) // the failed write, then the counts made meanwhile
	if n := e.count(t, "SELECT coalesce(sum(completed), 0) FROM {s}.activity"); n != 5 {
		t.Fatalf("the activity counts %d completed attempts, want 5", n)
	}
}

// A worker that could not write its counts for an hour drops them, and
// still writes once the database answers again, so that its log of the
// failed writes ends, also when it counts nothing more. Here the failed
// write's counts are over an hour old when the database answers.
func TestActivityRecoveryIsLoggedAfterTheCountsAgeOut(t *testing.T) {
	t.Parallel()
	logs := &logRecorder{}
	e := newEnv(t).withLogger(t, slog.New(logs))
	w := newWorker(t, e, &taskmill.WorkerConfig{Handlers: noopHandlers(), Queues: defaultQueue(1)})

	busy := e.begin(t)
	e.txExec(t, busy, "LOCK TABLE {s}.activity_writes")
	ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
	taskmill.FlushActivityAgain(ctx, w, 1, taskmill.DefaultQueue, 3, 0)
	cancel()
	if err := busy.Rollback(t.Context()); err != nil {
		t.Fatal(err)
	}
	if n := logs.count("operation failed"); n != 1 {
		t.Fatalf("got %d lines of a failed write, want 1", n)
	}

	taskmill.FlushActivityAgain(t.Context(), w, 1, taskmill.DefaultQueue, 3, 61*time.Minute) // the same write, an hour later
	if n := logs.count("operation recovered"); n != 1 {
		t.Errorf("got %d lines of a recovery, want 1", n)
	}
	if n := e.count(t, "SELECT coalesce(sum(completed), 0) FROM {s}.activity"); n != 0 {
		t.Errorf("the activity counts %d completed attempts, want none", n)
	}
}

// Each worker numbers its writes in a row of its own, under a random key: a
// worker that got the ID of a worker from the last day, as one of the same
// host name and process ID can, still adds its counts, though its writes
// have lower numbers.
func TestActivityOfAWorkerWithAnEarlierWorkersID(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	var total int
	for _, number := range []int64{5, 1} { // the earlier worker's write, then the later one's
		w := newWorker(t, e, &taskmill.WorkerConfig{Handlers: noopHandlers(), Queues: defaultQueue(1)})
		taskmill.SetWorkerID(w, "host-1-abcdef")
		taskmill.FlushActivityAgain(t.Context(), w, number, taskmill.DefaultQueue, 3, 0)
		total += 3
		if n := e.count(t, "SELECT coalesce(sum(completed), 0) FROM {s}.activity"); n != total {
			t.Fatalf("after write %d of a worker, the activity counts %d completed attempts, want %d", number, n, total)
		}
	}
}

// completedAttempts returns the completed attempts that the activity of the
// last hour counts.
func completedAttempts(t *testing.T, e *env) int64 {
	t.Helper()
	points, err := e.client.Activity(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	var n int64
	for _, p := range points {
		n += p.Completed
	}
	return n
}

func TestActivitySteps(t *testing.T) {
	t.Parallel()
	e := newEnv(t)

	base := time.Now().Truncate(5 * time.Minute).Add(-20 * time.Minute)
	insert := `INSERT INTO {s}.activity (minute, queue, completed, failed, scheduled, pending, available,
	                                     running, retryable, dead, canceled)
	           VALUES ($1, $2, $3, $4, $5, 0, $6, 0, 0, 0, 0)`
	e.exec(t, insert, base, "a", 3, 1, 4, 5)
	e.exec(t, insert, base, "b", 2, 0, 0, 1)
	e.exec(t, insert, base.Add(time.Minute), "a", 1, 0, 9, 2)

	// A minute in which workers counted attempts, but no maintainer counted
	// tasks.
	e.exec(t, `INSERT INTO {s}.activity (minute, queue, completed) VALUES ($1, 'a', 4)`, base.Add(5*time.Minute))

	for _, c := range []struct {
		queues []string
		want   []taskmill.Activity
	}{
		{nil, []taskmill.Activity{
			{At: base, Completed: 6, Failed: 1, Counts: counts(9, 6)},
			{At: base.Add(5 * time.Minute), Completed: 4},
		}},
		{[]string{"b"}, []taskmill.Activity{{At: base, Completed: 2, Counts: counts(0, 1)}}},
	} {
		points, err := e.client.Activity(t.Context(), &taskmill.ActivityParams{
			Queues: c.queues, Since: base.Add(-time.Hour), Step: 5 * time.Minute,
		})
		if err != nil {
			t.Fatal(err)
		}
		if !sameActivity(points, c.want) {
			t.Fatalf("queues %v: got %+v, want %+v", c.queues, points, c.want)
		}
	}

	for _, step := range []time.Duration{-time.Minute, 90 * time.Second, 25 * time.Hour} {
		if _, err := e.client.Activity(t.Context(), &taskmill.ActivityParams{Step: step}); !errors.Is(err, taskmill.ErrInvalidArgument) {
			t.Fatalf("step %v: got %v", step, err)
		}
	}
	// A time that PostgreSQL cannot store.
	since := lastTimestamp.Add(time.Microsecond)
	if _, err := e.client.Activity(t.Context(), &taskmill.ActivityParams{Since: since}); !errors.Is(err, taskmill.ErrInvalidArgument) {
		t.Fatalf("a start after the last timestamp: got %v", err)
	}
}

// The maintainer counts the tasks several times a minute, and a minute of
// activity keeps the most that it counted in each state. So a queue that
// fills and drains within a minute still shows in the chart.
func TestActivityKeepsTheMostTasksCountedInAMinute(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	w := newWorker(t, e, &taskmill.WorkerConfig{})
	ctx := t.Context()

	refresh := func() {
		t.Helper()
		if err := taskmill.RefreshStats(ctx, w); err != nil {
			t.Fatal(err)
		}
	}

	for range 5 {
		e.enqueue(t, sendEmail{}, nil)
	}
	refresh()
	e.exec(t, "DELETE FROM {s}.tasks WHERE id > (SELECT min(id) FROM {s}.tasks)")
	refresh()

	// The two refreshes ran in one minute, or in two: the first one's count
	// is the most of its minute either way.
	points, err := e.client.Activity(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}

	var most int64
	for _, p := range points {
		most = max(most, p.Counts[taskmill.StateAvailable])
	}
	if most != 5 {
		t.Fatalf("the activity shows at most %d available tasks, want the 5 counted first: %+v", most, points)
	}
}

// counts returns the counts of a step in which the scheduled and available
// tasks were the given numbers, and no others.
func counts(scheduled, available int64) map[taskmill.State]int64 {
	return map[taskmill.State]int64{
		taskmill.StateScheduled: scheduled, taskmill.StatePending: 0, taskmill.StateAvailable: available,
		taskmill.StateRunning: 0, taskmill.StateRetryable: 0, taskmill.StateDead: 0, taskmill.StateCanceled: 0,
	}
}

func sameActivity(got, want []taskmill.Activity) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		g, w := got[i], want[i]
		if !g.At.Equal(w.At) || g.Completed != w.Completed || g.Failed != w.Failed || len(g.Counts) != len(w.Counts) {
			return false
		}
		for s, n := range w.Counts {
			if g.Counts[s] != n {
				return false
			}
		}
	}
	return true
}

func TestActivityIsKeptForADay(t *testing.T) {
	t.Parallel()
	e := newEnv(t)

	insert := `INSERT INTO {s}.activity (minute, queue, completed) VALUES (date_trunc('minute', now()) - $1::interval, 'a', 1)`
	e.exec(t, insert, "25 hours")
	e.exec(t, insert, "23 hours")

	// So are the numbers of the workers' last writes: a write tried again
	// carries only counts from the last hour, so its first try was within
	// the hour.
	writes := `INSERT INTO {s}.activity_writes (writer, last_write, written_at) VALUES ($1, 1, now() - $2::interval)`
	const gone, idle = "00000000-0000-7000-8000-000000000001", "00000000-0000-7000-8000-000000000002"
	e.exec(t, writes, gone, "25 hours")
	e.exec(t, writes, idle, "23 hours")

	taskmill.RunRetention(t.Context(), newWorker(t, e, &taskmill.WorkerConfig{}))
	if n := e.count(t, "SELECT count(*) FROM {s}.activity"); n != 1 {
		t.Fatalf("got %d minutes of activity, want the one from 23 hours ago", n)
	}
	if n := e.count(t, "SELECT count(*) FROM {s}.activity_writes WHERE writer = $1", gone); n != 0 {
		t.Error("the number of a worker that wrote 25 hours ago was kept")
	}
	if n := e.count(t, "SELECT count(*) FROM {s}.activity_writes WHERE writer = $1", idle); n != 1 {
		t.Error("the number of a worker that wrote 23 hours ago was deleted")
	}
}
