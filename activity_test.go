package taskmill_test

import (
	"context"
	"errors"
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
	const atCommit = "CONSTRAINT TRIGGER slow_write AFTER INSERT OR UPDATE ON {s}.activity DEFERRABLE INITIALLY DEFERRED"
	for _, c := range []struct {
		name      string
		trigger   string // the trigger, and when it runs
		fail      string // what it does after its delay
		committed bool   // whether the slow write commits
	}{
		{"a commit that ends after the deadline", atCommit, "", true},
		{"a commit that fails after the deadline", atCommit, "RAISE EXCEPTION 'no';", false},
		{"a statement that ends after the deadline", "TRIGGER slow_write BEFORE INSERT OR UPDATE ON {s}.activity", "", false},
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
        BEGIN
            PERFORM pg_sleep(1);
        EXCEPTION WHEN query_canceled THEN -- the worker gave up: go on, as a commit that waits for a standby does
            PERFORM pg_sleep(1);
        END;
        INSERT INTO {s}.slow_writes VALUES (now());
        `+c.fail+`
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

// A write in doubt whose transaction the server never assigned, as after a
// failover to a standby that lacks it, did not commit there: the worker
// adds its counts again.
func TestActivityCountsAWriteLostInAFailover(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	w := newWorker(t, e, &taskmill.WorkerConfig{Handlers: noopHandlers(), Queues: defaultQueue(1)})

	var next uint64
	if err := e.pool.QueryRow(t.Context(), `SELECT pg_snapshot_xmax(pg_current_snapshot())::text::bigint`).Scan(&next); err != nil {
		t.Fatal(err)
	}
	taskmill.FlushActivityInDoubt(t.Context(), w, next+1_000_000, taskmill.DefaultQueue, 3)
	if n := completedAttempts(t, e); n != 3 {
		t.Fatalf("the activity counts %d completed attempts, want 3", n)
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

	taskmill.RunRetention(t.Context(), newWorker(t, e, &taskmill.WorkerConfig{}))
	if n := e.count(t, "SELECT count(*) FROM {s}.activity"); n != 1 {
		t.Fatalf("got %d minutes of activity, want the one from 23 hours ago", n)
	}
}
