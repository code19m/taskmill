package taskmill_test

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"math"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/code19m/taskmill"
)

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

func TestUpdateQueue(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ctx := t.Context()
	want := func(maxRunning int, paused bool) {
		t.Helper()
		queues, err := e.client.Queues(ctx)
		if err != nil {
			t.Fatal(err)
		}
		i := slices.IndexFunc(queues, func(q taskmill.QueueInfo) bool { return q.Name == "partner" })
		if i < 0 || queues[i].Settings.MaxRunning != maxRunning || queues[i].Paused != paused {
			t.Fatalf("queues: %+v; want partner with max %d, paused %v", queues, maxRunning, paused)
		}
	}
	// It creates the queue, and keeps what update does not change.
	if err := e.client.UpdateQueue(ctx, "partner", func(s *taskmill.QueueSettings) { s.MaxRunning = 3 }); err != nil {
		t.Fatal(err)
	}
	if err := e.client.PauseQueue(ctx, "partner"); err != nil {
		t.Fatal(err)
	}
	if err := e.client.UpdateQueue(ctx, "partner", func(s *taskmill.QueueSettings) { s.MaxRunning++ }); err != nil {
		t.Fatal(err)
	}
	want(4, true)
	for _, update := range []func(*taskmill.QueueSettings){
		nil,
		func(s *taskmill.QueueSettings) { s.MaxRunning = -1 },
		// One more than the column holds.
		func(s *taskmill.QueueSettings) { s.MaxRunning = math.MaxInt32; s.MaxRunning++ },
	} {
		if err := e.client.UpdateQueue(ctx, "partner", update); !errors.Is(err, taskmill.ErrInvalidArgument) {
			t.Fatalf("got %v, want ErrInvalidArgument", err)
		}
	}

	// Concurrent updates never lose each other's changes.
	increment := func(s *taskmill.QueueSettings) { s.MaxRunning++ }
	interleave(t,
		func(wait func()) error {
			return e.client.UpdateQueue(ctx, "partner", func(s *taskmill.QueueSettings) { wait(); increment(s) })
		},
		func(ctx context.Context) error { return e.client.UpdateQueue(ctx, "partner", increment) })
	want(6, true)

	// An update that updates the same queue does not deadlock: the inner
	// update lands first, and the outer one runs again on its result.
	inner := sync.OnceValue(func() error {
		ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		return e.client.UpdateQueue(ctx, "partner", func(s *taskmill.QueueSettings) { s.MaxRunning = 9 })
	})
	if err := e.client.UpdateQueue(ctx, "partner", func(s *taskmill.QueueSettings) {
		if err := inner(); err != nil {
			t.Error(err)
		}
		increment(s)
	}); err != nil {
		t.Fatal(err)
	}
	want(10, true)

	// Zero removes the limit.
	if err := e.client.UpdateQueue(ctx, "partner", func(s *taskmill.QueueSettings) { s.MaxRunning = 0 }); err != nil {
		t.Fatal(err)
	}
	want(0, true)
}

func TestManyConcurrentUpdatesLand(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ctx := t.Context()
	const updaters, calls = 16, 10
	// A try loses only to an update of another updater that landed during
	// the try. One updater's tries do not overlap, so each loses to a
	// different update, and an update conflicts once 10 of its tries lost.
	// So an updater conflicts at most once for each 10 of the other
	// updaters' updates. How often they conflict depends on the machine's
	// speed: never while this test runs alone, and a few dozen times in all
	// while other tests load the database too.
	const maxConflicts = (updaters - 1) * calls / 10
	increment := func(s *taskmill.QueueSettings) { s.MaxRunning++ }
	var wg sync.WaitGroup
	for range updaters {
		wg.Go(func() {
			conflicts := 0
			for range calls {
				// Try again after a conflict, as ErrConflict says.
				err := e.client.UpdateQueue(ctx, "busy", increment)
				for errors.Is(err, taskmill.ErrConflict) && conflicts < maxConflicts {
					conflicts++
					err = e.client.UpdateQueue(ctx, "busy", increment)
				}
				if err != nil {
					t.Errorf("%v, after %d conflicts of at most %d", err, conflicts, maxConflicts)
					return
				}
			}
		})
	}
	wg.Wait()
	queues, err := e.client.Queues(ctx)
	if err != nil {
		t.Fatal(err)
	}
	i := slices.IndexFunc(queues, func(q taskmill.QueueInfo) bool { return q.Name == "busy" })
	// Every update landed once: none was lost, and none that conflicted
	// landed too.
	if i < 0 || queues[i].Settings.MaxRunning != updaters*calls {
		t.Fatalf("queues: %+v; want busy with max %d", queues, updaters*calls)
	}
}

// interleave runs first and, while first's update waits in wait, runs second
// to the end, with a context that ends it if it waits for first. It fails the
// test unless both succeed. wait blocks only on its first call, so an update
// that runs again does not wait.
func interleave(t *testing.T, first func(wait func()) error, second func(ctx context.Context) error) {
	t.Helper()
	waiting, release := make(chan struct{}), make(chan struct{})
	errs := make(chan error, 1)
	go func() {
		errs <- first(sync.OnceFunc(func() {
			close(waiting)
			<-release
		}))
	}()
	select {
	case <-waiting:
	case err := <-errs:
		t.Fatalf("the first update ended before its update ran: %v", err)
	}
	func() {
		defer close(release) // also when second fails the test
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		defer cancel()
		if err := second(ctx); err != nil {
			t.Fatalf("the second update: %v", err)
		}
	}()
	if err := recv(t, errs, "the first update"); err != nil {
		t.Fatal(err)
	}
}

func TestSlowUpdateDoesNotHoldUpFetches(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ctx := t.Context()
	limit := func(n int) func(*taskmill.QueueSettings) {
		return func(s *taskmill.QueueSettings) { s.MaxRunning = n }
	}
	if err := e.client.UpdateQueue(ctx, taskmill.DefaultQueue, limit(2)); err != nil {
		t.Fatal(err)
	}
	runWorker(t, e, &taskmill.WorkerConfig{
		Handlers: noopHandlers(),
		Queues:   map[string]taskmill.QueueOptions{taskmill.DefaultQueue: {}},
	})

	// Fetches from a queue with a limit lock its row, which an update leaves
	// alone while its update runs.
	interleave(t,
		func(wait func()) error {
			return e.client.UpdateQueue(ctx, taskmill.DefaultQueue, func(s *taskmill.QueueSettings) { wait(); limit(3)(s) })
		},
		func(ctx context.Context) error {
			res, err := e.client.Enqueue(ctx, sendEmail{}, nil)
			if err != nil {
				return err
			}
			for {
				task, err := e.client.Get(ctx, res.ID)
				if err != nil || task.State == taskmill.StateCompleted {
					return err
				}
				select {
				case <-ctx.Done():
					return fmt.Errorf("task %d is %s: %w", res.ID, task.State, ctx.Err())
				case <-time.After(10 * time.Millisecond):
				}
			}
		})
}

func TestQueuesAndWorkers(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	for range 3 {
		e.enqueue(t, sendEmail{}, &taskmill.EnqueueOptions{Queue: "backlog"})
	}
	if err := e.client.UpdateQueue(t.Context(), "limited", func(s *taskmill.QueueSettings) { s.MaxRunning = 2 }); err != nil {
		t.Fatal(err)
	}
	runWorker(t, e, &taskmill.WorkerConfig{
		Handlers: noopHandlers(),
		Queues:   map[string]taskmill.QueueOptions{"mail": {}},
	})

	byName := map[string]taskmill.QueueInfo{}
	waitFor(t, "statistics to show the backlog", func() bool {
		queues, err := e.client.Queues(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		for _, q := range queues {
			byName[q.Name] = q
		}
		return byName["backlog"].Counts[taskmill.StateAvailable].N == 3
	})
	if byName["mail"].Workers != 1 || byName["limited"].Settings.MaxRunning != 2 {
		t.Fatalf("got %+v", byName)
	}
	// Queues without tasks are counted too, so they show when.
	if byName["limited"].RefreshedAt.IsZero() || byName["mail"].RefreshedAt.IsZero() {
		t.Fatalf("empty queues were not counted: %+v", byName)
	}

	waitFor(t, "the only worker to be elected maintainer", func() bool {
		workers, err := e.client.Workers(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if len(workers) != 1 || workers[0].ID == "" || !slices.Equal(workers[0].Queues, []string{"mail"}) ||
			!slices.Equal(workers[0].Kinds, []string{"email.send"}) {
			t.Fatalf("got workers %+v", workers)
		}
		return workers[0].Maintainer
	})
}

func TestQueuesMeasureLag(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	// The oldest available task has the lowest priority: lag spans every
	// priority.
	e.enqueue(t, sendEmail{}, &taskmill.EnqueueOptions{
		Queue: "behind", Priority: taskmill.PriorityLow, RunAt: time.Now().Add(-time.Hour),
	})
	e.enqueue(t, sendEmail{}, &taskmill.EnqueueOptions{
		Queue: "behind", Priority: taskmill.PriorityCritical, RunAt: time.Now().Add(-time.Minute),
	})
	// A retry that is overdue counts, and so does its wait before it is
	// promoted.
	retry := e.enqueue(t, sendEmail{}, &taskmill.EnqueueOptions{Queue: "retrying"})
	e.exec(t, "UPDATE {s}.tasks SET state = 'retryable', run_at = now() - interval '30 minutes' WHERE id = $1", retry)
	e.enqueue(t, sendEmail{}, &taskmill.EnqueueOptions{Queue: "idle", RunAt: time.Now().Add(time.Hour)})
	// A task due for longer than a duration can hold: the lag stops at the
	// longest duration, and does not wrap around to none.
	e.enqueue(t, sendEmail{}, &taskmill.EnqueueOptions{Queue: "ancient", RunAt: time.Date(1700, 1, 1, 0, 0, 0, 0, time.UTC)})
	longest := time.Duration(math.MaxInt64).Truncate(time.Microsecond)
	stop := runWorker(t, e, &taskmill.WorkerConfig{
		Handlers: noopHandlers(),
		Queues:   map[string]taskmill.QueueOptions{"mail": {}},
	})

	lag := map[string]time.Duration{}
	waitFor(t, "statistics to count every queue", func() bool {
		queues, err := e.client.Queues(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		for _, q := range queues {
			lag[q.Name] = q.Lag
		}
		return len(lag) >= 5 && lag["behind"] > 0 && lag["retrying"] > 0
	})
	for queue, want := range map[string]time.Duration{"behind": time.Hour, "retrying": 30 * time.Minute, "ancient": longest} {
		if over := lag[queue] - want; over < 0 || over > time.Minute {
			t.Errorf("queue %s: got lag %s, want about %s", queue, lag[queue], want)
		}
	}
	if lag["idle"] != 0 {
		t.Errorf("got lag %s for a queue without due tasks", lag["idle"])
	}

	// Without a maintainer, the lag keeps growing, by the database's clock.
	stop()
	waitFor(t, "the lag to grow without a maintainer", func() bool {
		queues, err := e.client.Queues(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		for _, q := range queues {
			if q.Name == "behind" {
				return q.Lag > lag["behind"]+500*time.Millisecond
			}
		}
		return false
	})
}

func TestQueuesShowWorkThatIsStuckWithoutLag(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	// As in an outbox while its broker is down: the first message of a key
	// keeps failing, and the messages behind it wait as pending.
	for range 3 {
		e.enqueue(t, sendEmail{}, &taskmill.EnqueueOptions{Queue: "relay", GroupKey: "order-1"})
	}
	// A group whose first task dies is halted, and its other tasks wait.
	for range 2 {
		e.enqueue(t, sendEmail{}, &taskmill.EnqueueOptions{Queue: "orders", GroupKey: "order-2", MaxAttempts: 1})
	}
	fail := func(context.Context, *taskmill.Task[sendEmail]) error { return errors.New("broker unreachable") }
	runWorker(t, e, &taskmill.WorkerConfig{
		Handlers: handlers(fail, &taskmill.HandlerOptions{Retry: retryUntil(3)}),
		Queues:   map[string]taskmill.QueueOptions{"relay": {}, "orders": {}},
	})

	byName := map[string]taskmill.QueueInfo{}
	waitFor(t, "statistics to show both queues stuck", func() bool {
		queues, err := e.client.Queues(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		for _, q := range queues {
			byName[q.Name] = q
		}
		return byName["relay"].MostFailures == 3 && byName["orders"].HaltedGroups == 1
	})
	for name, want := range map[string]struct {
		halted, failures int
		counts           map[taskmill.State]taskmill.TaskCount
	}{
		"relay":  {0, 3, map[taskmill.State]taskmill.TaskCount{taskmill.StateRetryable: {N: 1}, taskmill.StatePending: {N: 2}}},
		"orders": {1, 0, map[taskmill.State]taskmill.TaskCount{taskmill.StateDead: {N: 1}, taskmill.StateCanceled: {N: 0}, taskmill.StatePending: {N: 1}}},
	} {
		q := byName[name]
		if q.HaltedGroups != want.halted || q.MostFailures != want.failures || q.Lag != 0 || !maps.Equal(q.Counts, want.counts) {
			t.Errorf("%s: got %d halted groups, %d most failures, lag %s, counts %+v; want %d, %d, no lag, %+v",
				name, q.HaltedGroups, q.MostFailures, q.Lag, q.Counts, want.halted, want.failures, want.counts)
		}
	}
}

// retryUntil retries a failed attempt after a millisecond until the given
// attempt has failed, and then after an hour, so that the task waits to
// retry after failing that many times in a row.
type retryUntil int

func (n retryUntil) Backoff(attempt int) time.Duration {
	if attempt < int(n) {
		return time.Millisecond
	}
	return time.Hour
}
