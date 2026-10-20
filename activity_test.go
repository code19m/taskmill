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
