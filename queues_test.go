package taskmill_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"math"
	"slices"
	"strings"
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

// A queue whose settings are back to their defaults keeps no row, so a
// pause or a limit that named a queue by mistake, and was undone, does not
// list that queue for good. A queue that keeps one setting keeps its row.
func TestQueueWithoutSettingsIsNotKept(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ctx := t.Context()
	limit := func(n int) func(*taskmill.QueueSettings) { return func(s *taskmill.QueueSettings) { s.MaxRunning = n } }
	for _, err := range []error{
		e.client.PauseQueue(ctx, "emials"), e.client.ResumeQueue(ctx, "emials"),
		e.client.UpdateQueue(ctx, "retired", limit(4)), e.client.UpdateQueue(ctx, "retired", limit(0)),
		e.client.PauseQueue(ctx, "limited"), e.client.UpdateQueue(ctx, "limited", limit(2)), e.client.ResumeQueue(ctx, "limited"),
		e.client.UpdateQueue(ctx, "stopped", limit(3)), e.client.PauseQueue(ctx, "stopped"), e.client.UpdateQueue(ctx, "stopped", limit(0)),
	} {
		if err != nil {
			t.Fatal(err)
		}
	}

	queues, err := e.client.Queues(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, q := range queues {
		names = append(names, q.Name)
	}
	if !slices.Equal(names, []string{"limited", "stopped"}) {
		t.Fatalf("got the queues %v, want only those that keep a setting", names)
	}
	if queues[0].Paused || queues[0].Settings.MaxRunning != 2 || !queues[1].Paused || queues[1].Settings.MaxRunning != 0 {
		t.Fatalf("got %+v, want limited with its limit, and stopped paused", queues)
	}
}

// A later release may add a setting to the queues table that this one does
// not know. A row that holds such a setting is kept: deleting it would lose
// the setting. One whose new column is NULL holds nothing, and goes.
func TestQueueKeepsASettingOfALaterRelease(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ctx := t.Context()
	e.exec(t, "ALTER TABLE {s}.queues ADD COLUMN max_per_second integer")
	limit := func(n int) func(*taskmill.QueueSettings) { return func(s *taskmill.QueueSettings) { s.MaxRunning = n } }

	for _, name := range []string{"kept", "gone"} {
		if err := e.client.UpdateQueue(ctx, name, limit(4)); err != nil {
			t.Fatal(err)
		}
	}
	e.exec(t, "UPDATE {s}.queues SET max_per_second = 10 WHERE name = 'kept'")
	for _, name := range []string{"kept", "gone"} {
		if err := errors.Join(e.client.PauseQueue(ctx, name), e.client.ResumeQueue(ctx, name), e.client.UpdateQueue(ctx, name, limit(0))); err != nil {
			t.Fatal(err)
		}
	}

	if n := e.count(t, "SELECT count(*) FROM {s}.queues WHERE name = 'kept' AND max_per_second = 10"); n != 1 {
		t.Error("the row with the setting of a later release was deleted")
	}
	if n := e.count(t, "SELECT count(*) FROM {s}.queues WHERE name = 'gone'"); n != 0 {
		t.Error("the row without settings was kept")
	}
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

// An update that removes the last setting deletes the queue's row. A
// concurrent update that read the old limit must not write a value computed
// from it: it runs again on the defaults.
func TestUpdateQueueRunsAgainAfterTheQueueLostItsRow(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ctx := t.Context()
	if err := e.client.UpdateQueue(ctx, "partner", func(s *taskmill.QueueSettings) { s.MaxRunning = 5 }); err != nil {
		t.Fatal(err)
	}

	removeLimit := sync.OnceValue(func() error {
		return e.client.UpdateQueue(ctx, "partner", func(s *taskmill.QueueSettings) { s.MaxRunning = 0 })
	})
	var seen []int
	err := e.client.UpdateQueue(ctx, "partner", func(s *taskmill.QueueSettings) {
		if err := removeLimit(); err != nil {
			t.Error(err)
		}
		seen = append(seen, s.MaxRunning)
		s.MaxRunning *= 2
	})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(seen, []int{5, 0}) {
		t.Fatalf("update saw %v, want [5 0]", seen)
	}
	if n := e.count(t, "SELECT count(*) FROM {s}.queues WHERE name = 'partner'"); n != 0 {
		t.Fatal("the queue keeps a row, which holds the doubled old limit")
	}
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
			!slices.Equal(workers[0].Kinds, []string{"email.send"}) || !workers[0].Candidate {
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

// A worker that cannot read its queues' settings keeps the ones that it has:
// a paused queue stays paused. Here no read can end, whichever loop of the
// worker starts it, and the worker gives each one up by itself.
func TestFailedSettingsRefreshKeepsQueuesPaused(t *testing.T) {
	t.Parallel()
	logs := &logRecorder{}
	e := newEnv(t).withLogger(t, slog.New(logs))

	if err := e.client.PauseQueue(t.Context(), taskmill.DefaultQueue); err != nil {
		t.Fatal(err)
	}
	w := newWorker(t, e, &taskmill.WorkerConfig{
		Handlers: noopHandlers(),
		Queues:   map[string]taskmill.QueueOptions{taskmill.DefaultQueue: {}, "canary": {}},
	})

	// The worker reads the settings this often, and a read may take as long.
	taskmill.SetTiming(w, taskmill.Timing{Settings: 100 * time.Millisecond})
	startWorker(t, w)

	// The canary's queue is not paused. A worker fetches nothing before it
	// has read the settings, so the canary's task shows that it has.
	canary := &taskmill.EnqueueOptions{Queue: "canary"}
	e.waitState(t, e.enqueue(t, sendEmail{}, canary), taskmill.StateCompleted)

	// From here on, every read of the settings waits for the queues' table.
	tx := e.begin(t)
	e.txExec(t, tx, "LOCK TABLE {s}.queues IN ACCESS EXCLUSIVE MODE")

	waitFor(t, "the worker to give up a read of the settings", func() bool {
		return len(operationLines(logs.all())["load queue settings"]) > 0
	})

	// The canary, enqueued after the paused queue's task, shows that the
	// worker is fetching.
	id := e.enqueue(t, sendEmail{}, nil)
	e.waitState(t, e.enqueue(t, sendEmail{}, canary), taskmill.StateCompleted)
	e.wantState(t, id, taskmill.StateAvailable)
}

// Pause is a tool for incidents: a worker must not fetch from a queue before
// it knows the queue's settings. Here its first read of them cannot end. The
// worker gives that read up by itself, fetches nothing, says that it is not
// ready, and reads again until a read works.
func TestWorkerDoesNotFetchBeforeItKnowsItsQueuesSettings(t *testing.T) {
	t.Parallel()
	logs := &logRecorder{}
	e := newEnv(t).withLogger(t, slog.New(logs))

	if err := e.client.PauseQueue(t.Context(), taskmill.DefaultQueue); err != nil {
		t.Fatal(err)
	}
	paused := e.enqueue(t, sendEmail{}, nil)
	ran := make(chan string, 2)

	w := newWorker(t, e, &taskmill.WorkerConfig{
		Handlers: handlers(func(_ context.Context, task *taskmill.Task[sendEmail]) error {
			ran <- task.Queue
			return nil
		}, nil),
		Queues:       map[string]taskmill.QueueOptions{taskmill.DefaultQueue: {}, "canary": {}},
		NoMaintainer: true,
	})

	// A read may take as long as the interval between two reads.
	taskmill.SetTiming(w, taskmill.Timing{Settings: 100 * time.Millisecond})

	// The worker starts while a read of the settings cannot end.
	tx := e.begin(t)
	e.txExec(t, tx, "LOCK TABLE {s}.queues IN ACCESS EXCLUSIVE MODE")
	stop := startWorker(t, w)

	waitFor(t, "the worker to give up its read of the settings", func() bool {
		return len(operationLines(logs.all())["load queue settings"]) > 0
	})

	// A worker that takes no task is not ready, and says why.
	if err := w.Health(); err == nil || !strings.Contains(err.Error(), "settings") {
		t.Errorf("a worker that has not read its queues' settings reports the health %v", err)
	}

	if err := tx.Rollback(t.Context()); err != nil {
		t.Fatal(err)
	}

	// The canary's queue is not paused: its task shows that the worker read
	// the settings again, and fetches.
	e.enqueue(t, sendEmail{}, &taskmill.EnqueueOptions{Queue: "canary"})
	if queue := recv(t, ran, "the canary's task to run"); queue != "canary" {
		t.Fatalf("a task of the paused queue %q ran: the worker fetched before it knew the queue's settings", queue)
	}
	if err := w.Health(); err != nil {
		t.Errorf("the worker read its queues' settings, and reports the health %v", err)
	}

	// A worker that stops gives back a task that it fetched and did not
	// start, so the task's state alone does not show a fetch.
	stop()
	if info := e.get(t, paused); info.State != taskmill.StateAvailable || !info.AttemptedAt.IsZero() {
		t.Fatalf("the worker fetched a task of the paused queue before it knew the queue's settings: it is %s, fetched at %s",
			info.State, info.AttemptedAt)
	}
}
