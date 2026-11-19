package taskmill_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"math"
	"reflect"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/code19m/taskmill"
	"github.com/code19m/taskmill/internal/testdb"
)

func noopHandlers() *taskmill.Handlers {
	return handlers(func(context.Context, *taskmill.Task[sendEmail]) error { return nil }, nil)
}

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

func TestDiscardCompletedSkipsHistory(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	done := make(chan struct{}, 1)
	runWorker(t, e, &taskmill.WorkerConfig{
		Handlers: handlers(func(context.Context, *taskmill.Task[sendEmail]) error {
			done <- struct{}{}
			return nil
		}, &taskmill.HandlerOptions{DiscardCompleted: true}),
	})

	id := e.enqueue(t, sendEmail{}, nil)
	recv(t, done, "the handler to run")
	waitFor(t, "the completed task to be deleted", func() bool {
		_, err := e.client.Get(t.Context(), id)
		return errors.Is(err, taskmill.ErrNotFound)
	})
	if n := e.count(t, "SELECT count(*) FROM {s}.task_history"); n != 0 {
		t.Fatalf("history holds %d tasks", n)
	}
}

func TestReadsRejectNamesThatNoTaskHas(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ctx := t.Context()
	for _, s := range []string{"a\x00b", "\xff", strings.Repeat("q", 257)} {
		for name, call := range map[string]func() error{
			"List queue": func() error {
				_, err := e.client.List(ctx, &taskmill.ListParams{Filter: taskmill.TaskFilter{Queues: []string{s}}})
				return err
			},
			"List kind": func() error {
				_, err := e.client.List(ctx, &taskmill.ListParams{Filter: taskmill.TaskFilter{Kinds: []string{s}}})
				return err
			},
			"List group": func() error {
				_, err := e.client.List(ctx, &taskmill.ListParams{Filter: taskmill.TaskFilter{GroupKeys: []string{s}}})
				return err
			},
			"Count kind": func() error {
				_, err := e.client.Count(ctx, &taskmill.TaskFilter{Kinds: []string{s}})
				return err
			},
			"RetryMatching group": func() error {
				_, err := e.client.RetryMatching(ctx, &taskmill.BulkParams{Filter: taskmill.TaskFilter{GroupKeys: []string{s}}})
				return err
			},
			"Groups queue":  func() error { _, err := e.client.Groups(ctx, &taskmill.GroupParams{Queues: []string{s}}); return err },
			"Groups search": func() error { _, err := e.client.Groups(ctx, &taskmill.GroupParams{Search: s}); return err },
			"Groups cursor": func() error {
				_, err := e.client.Groups(ctx, &taskmill.GroupParams{Cursor: base64.RawURLEncoding.EncodeToString([]byte("q\x00" + s))})
				return err
			},
			"Activity queue": func() error {
				_, err := e.client.Activity(ctx, &taskmill.ActivityParams{Queues: []string{s}})
				return err
			},
			"PauseSchedule":  func() error { return e.client.PauseSchedule(ctx, s) },
			"ResumeSchedule": func() error { return e.client.ResumeSchedule(ctx, s) },
			"RunSchedule":    func() error { return e.client.RunSchedule(ctx, s) },
		} {
			if err := call(); !errors.Is(err, taskmill.ErrInvalidArgument) {
				t.Errorf("%s(%q): got %v, want ErrInvalidArgument", name, s, err)
			}
		}
	}
}

func TestSettings(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ctx := t.Context()
	want := func(history, dead time.Duration) {
		t.Helper()
		s, err := e.client.Settings(ctx)
		if err != nil || s.HistoryRetention != history || s.DeadRetention != dead {
			t.Fatalf("settings: %+v, %v; want history %s, dead %s", s, err, history, dead)
		}
	}
	want(7*24*time.Hour, 30*24*time.Hour)
	if err := e.client.UpdateSettings(ctx, func(s *taskmill.Settings) { s.HistoryRetention = 48 * time.Hour }); err != nil {
		t.Fatal(err)
	}
	want(48*time.Hour, 30*24*time.Hour)
	for _, update := range []func(*taskmill.Settings){nil, func(s *taskmill.Settings) { s.DeadRetention = time.Minute }} {
		if err := e.client.UpdateSettings(ctx, update); !errors.Is(err, taskmill.ErrInvalidArgument) {
			t.Fatalf("got %v, want ErrInvalidArgument", err)
		}
	}
	want(48*time.Hour, 30*24*time.Hour)

	// Concurrent updates of different settings both land.
	interleave(t,
		func(wait func()) error {
			return e.client.UpdateSettings(ctx, func(s *taskmill.Settings) { wait(); s.DeadRetention = 10 * 24 * time.Hour })
		},
		func(ctx context.Context) error {
			return e.client.UpdateSettings(ctx, func(s *taskmill.Settings) { s.HistoryRetention = 72 * time.Hour })
		})
	want(72*time.Hour, 10*24*time.Hour)

	// A setting changed to zero goes back to its default.
	if err := e.client.UpdateSettings(ctx, func(s *taskmill.Settings) { s.HistoryRetention = 0 }); err != nil {
		t.Fatal(err)
	}
	want(7*24*time.Hour, 10*24*time.Hour)
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

func TestEndlessConflictsGiveUp(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ctx := t.Context()
	// Each run of the outer update changes the queue behind its back.
	runs := 0
	err := e.client.UpdateQueue(ctx, "contended", func(s *taskmill.QueueSettings) {
		runs++
		if err := e.client.UpdateQueue(ctx, "contended", func(s *taskmill.QueueSettings) { s.MaxRunning++ }); err != nil {
			t.Error(err)
		}
		s.MaxRunning = 100
	})
	if !errors.Is(err, taskmill.ErrConflict) || runs != 10 {
		t.Fatalf("got %v after %d runs, want ErrConflict after 10", err, runs)
	}
	err = e.client.UpdateSettings(ctx, func(*taskmill.Settings) {
		if err := e.client.UpdateSettings(ctx, func(s *taskmill.Settings) { s.HistoryRetention += time.Hour }); err != nil {
			t.Error(err)
		}
	})
	if !errors.Is(err, taskmill.ErrConflict) {
		t.Fatalf("got %v, want ErrConflict", err)
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

func TestListPagesNewestFirst(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	var ids []int64
	for i := range 5 {
		ids = append(ids, e.enqueue(t, sendEmail{UserID: int64(i)}, nil))
	}
	ids = append(ids, e.enqueue(t, report{Name: "r"}, nil))
	runWorker(t, e, &taskmill.WorkerConfig{
		Handlers: noopHandlers(),
		Queues:   map[string]taskmill.QueueOptions{"reports-done": {}},
	})
	e.exec(t, "UPDATE {s}.tasks SET queue = 'reports-done', kind = 'email.send' WHERE id = $1", ids[5])
	e.exec(t, "SELECT pg_notify($1, 'a|reports-done')", e.schema)
	e.waitState(t, ids[5], taskmill.StateCompleted)

	var got []int64
	cursor := ""
	for page := 0; ; page++ {
		res, err := e.client.List(t.Context(), &taskmill.ListParams{Limit: 4, Cursor: cursor})
		if err != nil {
			t.Fatal(err)
		}
		for _, task := range res.Tasks {
			got = append(got, task.ID)
		}
		if res.Next == "" {
			break
		}
		cursor = res.Next
		if page > 3 {
			t.Fatal("too many pages")
		}
	}
	want := slices.Clone(ids)
	slices.Reverse(want)
	if !slices.Equal(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}

	completed, err := e.client.List(t.Context(), &taskmill.ListParams{Filter: taskmill.TaskFilter{States: []taskmill.State{taskmill.StateCompleted}}})
	if err != nil {
		t.Fatal(err)
	}
	if len(completed.Tasks) != 1 || completed.Tasks[0].ID != ids[5] || completed.Tasks[0].State != taskmill.StateCompleted {
		t.Fatalf("completed tasks: %+v", completed.Tasks)
	}
	available, err := e.client.List(t.Context(), &taskmill.ListParams{Filter: taskmill.TaskFilter{
		Queues: []string{taskmill.DefaultQueue}, States: []taskmill.State{taskmill.StateAvailable}, Kinds: []string{"email.send"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if len(available.Tasks) != 5 {
		t.Fatalf("got %d available tasks, want 5", len(available.Tasks))
	}
}

// List returns 100 tasks a page when the caller names no page size, and
// refuses a page size, a cursor, a state, or a range of finish times that it
// cannot use: a range on a list that is not in finish order, one that does
// not start before it ends, and a bound that PostgreSQL cannot store, also
// one that only reaches past the last time when it is rounded up to the
// microsecond.
func TestListChecksItsParams(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.exec(t, `INSERT INTO {s}.tasks (queue, kind, state, run_at, payload, max_attempts)
	           SELECT 'default', 'email.send', 'scheduled', now() + interval '1 day', '{}', 1 FROM generate_series(1, 101)`)
	if page, err := e.client.List(t.Context(), &taskmill.ListParams{Summary: true}); err != nil || len(page.Tasks) != 100 || page.Next == "" {
		t.Fatalf("no limit: got a page of %d tasks, next %q, %v; want 100 tasks and a next page", len(page.Tasks), page.Next, err)
	}
	at := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	dead := taskmill.TaskFilter{States: []taskmill.State{taskmill.StateDead}}
	for _, p := range []taskmill.ListParams{
		{Limit: -1},
		{Limit: 1001},
		{Cursor: "%%%"},
		{Cursor: "eA"}, // "x"
		{Filter: taskmill.TaskFilter{States: []taskmill.State{"bogus"}}},
		{FinishedSince: at},
		{FinishedBefore: at, Filter: taskmill.TaskFilter{States: []taskmill.State{taskmill.StateCompleted}}},
		{FinishedBefore: at, Filter: taskmill.TaskFilter{States: []taskmill.State{taskmill.StateDead, taskmill.StateAvailable}}},
		{Filter: dead, FinishedSince: at, FinishedBefore: at},
		{Filter: dead, FinishedSince: at.Add(time.Nanosecond), FinishedBefore: at},
		{Filter: dead, FinishedSince: firstTimestamp.Add(-time.Microsecond)},
		{Filter: dead, FinishedSince: lastTimestamp.Add(time.Nanosecond)},
		{Filter: dead, FinishedBefore: lastTimestamp.Add(time.Microsecond)},
		{Filter: dead, FinishedBefore: time.Unix(math.MaxInt64, 0)},
	} {
		if _, err := e.client.List(t.Context(), &p); !errors.Is(err, taskmill.ErrInvalidArgument) {
			t.Errorf("%+v: got %v, want ErrInvalidArgument", p, err)
		}
	}
}

// A list of only dead tasks, only canceled tasks, or both comes in the order
// in which the tasks finished, latest first, and by ID among the tasks of
// one finish time. Its pages chain in that order: through tasks that share
// a finish time, as those that one bulk action cancels do, and past the
// finish times that only an UPDATE by hand writes, the infinite ones. Every
// other list keeps the order of the IDs. A cursor of one order is no cursor
// for the other, and the cursors of earlier releases are all of the ID
// order.
func TestListsOfFinishedTasksComeLatestFirst(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.rawGroups = true // the dead task of a group is written by hand, without its group
	ctx := t.Context()
	// Three dead tasks of the queue "a" share a finish time, and the one with
	// the highest ID was written first: the index holds the tasks of one
	// finish time in the order in which they were written.
	e.exec(t, `INSERT INTO {s}.tasks (id, queue, kind, state, finalized_at, group_key, run_at, payload, max_attempts)
	           OVERRIDING SYSTEM VALUE
	           SELECT v.id, v.queue, v.kind, v.state::{s}.task_state, v.finished::timestamptz, v.grp, now(), '{}', 1
	           FROM (VALUES (9, 'a', 'report.build', 'dead', '2026-01-01 00:00:03+00', NULL),
	                        (1, 'a', 'email.send', 'dead', '2026-01-01 00:00:03+00', NULL),
	                        (5, 'a', 'email.send', 'dead', '2026-01-01 00:00:03+00', NULL),
	                        (2, 'a', 'email.send', 'dead', '2026-01-01 00:00:01+00', 'g'),
	                        (3, 'a', 'email.send', 'canceled', '2026-01-01 00:00:02+00', NULL),
	                        (4, 'b', 'report.build', 'dead', '2026-01-01 00:00:02+00', NULL),
	                        (6, 'b', 'email.send', 'canceled', 'infinity', NULL),
	                        (7, 'a', 'email.send', 'dead', '-infinity', NULL),
	                        (8, 'a', 'email.send', 'available', NULL, NULL)
	                ) AS v(id, queue, kind, state, finished, grp)`)
	dead, canceled := []taskmill.State{taskmill.StateDead}, []taskmill.State{taskmill.StateCanceled}
	finished := []taskmill.State{taskmill.StateDead, taskmill.StateCanceled}
	for _, c := range []struct {
		filter taskmill.TaskFilter
		want   []int64
	}{
		{taskmill.TaskFilter{States: finished}, []int64{6, 9, 5, 1, 4, 3, 2, 7}},
		{taskmill.TaskFilter{States: dead}, []int64{9, 5, 1, 4, 2, 7}},
		{taskmill.TaskFilter{States: canceled}, []int64{6, 3}},
		{taskmill.TaskFilter{States: finished, Queues: []string{"a"}}, []int64{9, 5, 1, 3, 2, 7}},
		{taskmill.TaskFilter{States: finished, Queues: []string{"a", "a"}}, []int64{9, 5, 1, 3, 2, 7}},
		{taskmill.TaskFilter{States: finished, Queues: []string{"b", "a"}}, []int64{6, 9, 5, 1, 4, 3, 2, 7}},
		{taskmill.TaskFilter{States: dead, Kinds: []string{"report.build"}}, []int64{9, 4}},
		{taskmill.TaskFilter{States: dead, GroupKeys: []string{"g"}}, []int64{2}},
		// A list that mixes dead tasks with others keeps the order of the IDs.
		{taskmill.TaskFilter{States: []taskmill.State{taskmill.StateDead, taskmill.StateAvailable}}, []int64{9, 8, 7, 5, 4, 2, 1}},
	} {
		for _, limit := range []int{1, 2, 3, 100} {
			got, next := listed(t, e.client, taskmill.ListParams{Filter: c.filter, Limit: limit}, len(c.want))
			if next != "" || !slices.Equal(got, c.want) {
				t.Errorf("%+v in pages of %d: got %v and next %q, want %v and no next page", c.filter, limit, got, next, c.want)
			}
		}
	}

	byID := taskmill.ListParams{Limit: 1}
	byFinish := taskmill.ListParams{Limit: 1, Filter: taskmill.TaskFilter{States: dead}}
	first, err := e.client.List(ctx, &byID)
	if err != nil {
		t.Fatal(err)
	}
	latest, err := e.client.List(ctx, &byFinish)
	if err != nil {
		t.Fatal(err)
	}
	byID.Cursor, byFinish.Cursor = latest.Next, first.Next
	for _, p := range []taskmill.ListParams{byID, byFinish} {
		if _, err := e.client.List(ctx, &p); !errors.Is(err, taskmill.ErrInvalidArgument) {
			t.Errorf("a list of states %v with the cursor of the other order: got %v, want ErrInvalidArgument", p.Filter.States, err)
		}
	}
}

// A list in finish order takes a range of finish times: the tasks that
// finished at FinishedSince or later, and before FinishedBefore. A task that
// finished a microsecond outside the range is left out. A bound can be finer
// than the microseconds that the database keeps, and still every task is on
// its own side of it. The pages of a range chain inside it, also through the
// tasks of one finish time.
//
// The bounds are instants. The same range selects the same tasks whatever
// the location of its times, and whatever the time zone of the session: here
// one whose clocks go back an hour in the middle of the range, so that its
// local times come twice.
func TestListsOfFinishedTasksTakeARangeOfFinishTimes(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	// The range of most lists starts at second 1 and ends at second 3, with
	// America/St_Johns back on standard time from second 2.
	start := time.Date(2026, time.November, 1, 4, 29, 58, 0, time.UTC)
	e.exec(t, `INSERT INTO {s}.tasks (id, queue, kind, state, finalized_at, run_at, payload, max_attempts)
	           OVERRIDING SYSTEM VALUE
	           SELECT v.id, v.queue, v.kind, v.state::{s}.task_state, $1::timestamptz + v.finished * interval '1 microsecond', now(), '{}', 1
	           FROM (VALUES (1, 'a', 'email.send', 'dead', 999999),
	                        (2, 'a', 'email.send', 'dead', 1000000),
	                        (3, 'a', 'email.send', 'canceled', 1000001),
	                        (4, 'b', 'email.send', 'dead', 2000000),
	                        (5, 'a', 'report.build', 'dead', 2000000),
	                        (6, 'a', 'email.send', 'dead', 2000000),
	                        (7, 'a', 'email.send', 'canceled', 2999999),
	                        (8, 'a', 'email.send', 'dead', 3000000),
	                        (9, 'b', 'email.send', 'dead', 4000000),
	                        (10, 'a', 'email.send', 'available', NULL)
	                ) AS v(id, queue, kind, state, finished)`, start)
	stJohns, err := time.LoadLocation("America/St_Johns")
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := pgxpool.ParseConfig(testdb.DirectURL(t)) // the time zone is a parameter of the session
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams["timezone"] = stJohns.String()
	pool, err := pgxpool.NewWithConfig(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	inStJohns, err := taskmill.NewClient(t.Context(), pool, &taskmill.ClientConfig{Schema: e.schema, Logger: testLogger(t)})
	if err != nil {
		t.Fatal(err)
	}

	second := func(n int) time.Time { return start.Add(time.Duration(n) * time.Second) }
	dead, canceled := []taskmill.State{taskmill.StateDead}, []taskmill.State{taskmill.StateCanceled}
	finished := []taskmill.State{taskmill.StateDead, taskmill.StateCanceled}
	for _, c := range []struct {
		filter        taskmill.TaskFilter
		since, before time.Time
		want          []int64
	}{
		{taskmill.TaskFilter{States: finished}, second(1), second(3), []int64{7, 6, 5, 4, 3, 2}},
		{taskmill.TaskFilter{States: dead}, second(1), second(3), []int64{6, 5, 4, 2}},
		{taskmill.TaskFilter{States: canceled}, second(1), second(3), []int64{7, 3}},
		{taskmill.TaskFilter{States: finished, Queues: []string{"a"}}, second(1), second(3), []int64{7, 6, 5, 3, 2}},
		{taskmill.TaskFilter{States: finished, Kinds: []string{"report.build"}}, second(1), second(3), []int64{5}},
		{taskmill.TaskFilter{States: finished}, second(2), time.Time{}, []int64{9, 8, 7, 6, 5, 4}},
		{taskmill.TaskFilter{States: finished}, time.Time{}, second(2), []int64{3, 2, 1}},
		// A nanosecond after a bound, the task that finished at the bound is
		// before it.
		{taskmill.TaskFilter{States: finished}, second(1).Add(time.Nanosecond), second(3).Add(time.Nanosecond), []int64{8, 7, 6, 5, 4, 3}},
		{taskmill.TaskFilter{States: finished}, second(3).Add(time.Microsecond), second(4), nil},
		{taskmill.TaskFilter{States: finished}, firstTimestamp, lastTimestamp, []int64{9, 8, 7, 6, 5, 4, 3, 2, 1}},
	} {
		for _, zone := range []struct {
			session string
			client  *taskmill.Client
			bounds  *time.Location
		}{
			{"the test pool's", e.client, time.UTC},
			{"the test pool's", e.client, time.FixedZone("ahead", (5*60+45)*60)},
			{stJohns.String(), inStJohns, time.UTC},
			{stJohns.String(), inStJohns, stJohns},
		} {
			// The zero time is no bound in any location.
			params := taskmill.ListParams{Filter: c.filter, FinishedSince: c.since.In(zone.bounds), FinishedBefore: c.before.In(zone.bounds)}
			for _, limit := range []int{1, 2, 100} {
				params.Limit = limit
				got, next := listed(t, zone.client, params, len(c.want)+1)
				if next != "" || !slices.Equal(got, c.want) {
					t.Errorf("%+v in pages of %d, in a session of %s time zone: got %v and next %q, want %v and no next page",
						params, limit, zone.session, got, next, c.want)
				}
			}
		}
	}
}

// A summary is the same page of tasks without what makes a task large: no
// payload, no metadata, and only the newest error. Everything else is as the
// full list returns it, for unfinished tasks and for completed ones.
func TestListSummary(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ctx := t.Context()
	opts := &taskmill.EnqueueOptions{Metadata: map[string]string{"tenant": "7"}}
	failed := e.enqueue(t, sendEmail{To: "failed"}, opts)
	e.exec(t, `UPDATE {s}.tasks SET errors = '[{"attempt": 1, "at": "2026-01-01T00:00:00Z", "message": "first"},
	           {"attempt": 2, "at": "2026-01-02T00:00:00Z", "message": "last", "code": "timeout"}]' WHERE id = $1`, failed)
	e.enqueue(t, sendEmail{To: "waiting"}, opts)
	runWorker(t, e, &taskmill.WorkerConfig{Handlers: noopHandlers(), Queues: map[string]taskmill.QueueOptions{"done": {}}})
	completed := e.enqueue(t, sendEmail{To: "completed"}, &taskmill.EnqueueOptions{Queue: "done", Metadata: opts.Metadata})
	e.waitState(t, completed, taskmill.StateCompleted)

	full, err := e.client.List(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	brief, err := e.client.List(ctx, &taskmill.ListParams{Summary: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(full.Tasks) != 3 || len(brief.Tasks) != 3 {
		t.Fatalf("got %d tasks in full and %d in summary, want 3 of each", len(full.Tasks), len(brief.Tasks))
	}
	for i, want := range full.Tasks {
		if len(want.Payload) == 0 || len(want.Metadata) == 0 {
			t.Fatalf("the full list left out the payload or the metadata of task %d: %+v", want.ID, want)
		}
		got := brief.Tasks[i]
		if got.Payload != nil || got.Metadata != nil {
			t.Errorf("the summary of task %d has payload %q and metadata %v, want none", got.ID, got.Payload, got.Metadata)
		}
		want.Payload, want.Metadata = nil, nil
		if n := len(want.Errors); n > 1 {
			want.Errors = want.Errors[n-1:]
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("the summary of task %d:\n got %+v\nwant %+v", got.ID, got, want)
		}
	}
	if last := brief.Tasks[2]; last.ID != failed || len(last.Errors) != 1 || last.Errors[0].Message != "last" {
		t.Errorf("the summary of the failed task: %+v, want only its newest error", last)
	}
}

func TestListSomeQueues(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	for _, q := range []string{"a", "b", "c"} {
		e.enqueue(t, sendEmail{}, &taskmill.EnqueueOptions{Queue: q})
	}
	for _, c := range []struct {
		queues []string
		want   int
	}{{nil, 3}, {[]string{"a", "b"}, 2}, {[]string{"c"}, 1}, {[]string{"none"}, 0}} {
		res, err := e.client.List(t.Context(), &taskmill.ListParams{Filter: taskmill.TaskFilter{Queues: c.queues}})
		if err != nil {
			t.Fatal(err)
		}
		if len(res.Tasks) != c.want {
			t.Fatalf("queues %v: got %d tasks, want %d", c.queues, len(res.Tasks), c.want)
		}
	}
	if _, err := e.client.List(t.Context(), &taskmill.ListParams{Filter: taskmill.TaskFilter{Queues: []string{""}}}); !errors.Is(err, taskmill.ErrInvalidArgument) {
		t.Fatalf("empty queue name: got %v", err)
	}
}

func TestListSomeGroups(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	a1 := e.enqueue(t, sendEmail{}, &taskmill.EnqueueOptions{GroupKey: "a"})
	b := e.enqueue(t, sendEmail{}, &taskmill.EnqueueOptions{GroupKey: "b"})
	e.enqueue(t, sendEmail{}, &taskmill.EnqueueOptions{GroupKey: "c"})
	a2 := e.enqueue(t, sendEmail{}, &taskmill.EnqueueOptions{GroupKey: "a"})
	for _, c := range []struct {
		groups []string
		want   []int64
	}{{[]string{"a", "b"}, []int64{a2, b, a1}}, {[]string{"a"}, []int64{a2, a1}}} {
		page, err := e.client.List(t.Context(), &taskmill.ListParams{Filter: taskmill.TaskFilter{GroupKeys: c.groups}})
		if err != nil {
			t.Fatal(err)
		}
		if got := ids(page.Tasks); !slices.Equal(got, c.want) {
			t.Fatalf("groups %v: got %v, want %v", c.groups, got, c.want)
		}
	}
}

func TestListSomeStatesAndKinds(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.rawGroups = true // the pending task is written by hand, without its group
	// One task in each state, of a kind named after the state.
	e.exec(t, `INSERT INTO {s}.tasks (queue, kind, state, run_at, payload, max_attempts, group_key, lease_expires_at, finalized_at)
	           SELECT 'default', s, s::{s}.task_state, now(), '{}', 1, CASE WHEN s = 'pending' THEN 'g' END,
	                  CASE WHEN s = 'running' THEN now() END, CASE WHEN s IN ('dead', 'canceled') THEN now() END
	           FROM unnest('{scheduled,pending,available,running,retryable,dead,canceled}'::text[]) AS s`)
	e.exec(t, `INSERT INTO {s}.task_history (id, finalized_at, queue, created_at, kind, run_at, attempted_by, attempted_at,
	                                         priority, attempt, max_attempts, payload, metadata, errors)
	           VALUES (100, now(), 'default', now(), 'completed', now(), 'w', now(), 2, 1, 1, '{}', '{}', '[]')`)
	list := func(filter taskmill.TaskFilter) []taskmill.State {
		t.Helper()
		page, err := e.client.List(t.Context(), &taskmill.ListParams{Filter: filter})
		if err != nil {
			t.Fatal(err)
		}
		var states []taskmill.State
		for _, task := range page.Tasks {
			if string(task.State) != task.Kind {
				t.Fatalf("task %d of kind %s is listed as %s", task.ID, task.Kind, task.State)
			}
			states = append(states, task.State)
		}
		slices.Sort(states)
		return states
	}
	all := []taskmill.State{
		taskmill.StateAvailable, taskmill.StateCanceled, taskmill.StateCompleted, taskmill.StateDead,
		taskmill.StatePending, taskmill.StateRetryable, taskmill.StateRunning, taskmill.StateScheduled,
	}
	if got := list(taskmill.TaskFilter{}); !slices.Equal(got, all) {
		t.Fatalf("no filter: got %v, want %v", got, all)
	}
	for _, s := range all {
		if got := list(taskmill.TaskFilter{States: []taskmill.State{s}}); !slices.Equal(got, []taskmill.State{s}) {
			t.Errorf("state %s: got %v", s, got)
		}
		if got := list(taskmill.TaskFilter{Kinds: []string{string(s)}}); !slices.Equal(got, []taskmill.State{s}) {
			t.Errorf("kind %s: got %v", s, got)
		}
	}
	two := []taskmill.State{taskmill.StateCompleted, taskmill.StateDead}
	if got := list(taskmill.TaskFilter{States: two}); !slices.Equal(got, two) {
		t.Errorf("states %v: got %v", two, got)
	}
	if got := list(taskmill.TaskFilter{Kinds: []string{"completed", "dead"}}); !slices.Equal(got, two) {
		t.Errorf("kinds of %v: got %v", two, got)
	}
}

// A page of tasks in ID order reads about its own rows where an index holds
// the rows of its filters in that order: not the newer tasks of other
// queues, however many they are. Two lists have such an index, so they hold
// whatever the planner expects: the completed tasks of one queue, and the
// waiting tasks of one group.
func TestTaskPagesReadOnlyTheirRows(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.rawGroups = true // pending tasks are written by hand, without their groups
	// The oldest tasks are those of the queue "rare": pending ones of two
	// groups, and completed ones. The others are newer, and many: here the
	// planner's choice depends on the size, and with 5,000 tasks in each
	// table it reads the history without its index.
	e.exec(t, `INSERT INTO {s}.tasks (queue, kind, state, run_at, payload, max_attempts, group_key)
	           SELECT CASE WHEN i <= 400 THEN 'rare' ELSE 'busy' END, 'email.send',
	                  CASE WHEN i <= 400 THEN 'pending' ELSE 'available' END::{s}.task_state,
	                  now(), '{}', 1, CASE WHEN i <= 400 THEN 'g' || i % 2 END
	           FROM generate_series(1, 40000) i`)
	e.exec(t, `INSERT INTO {s}.task_history (id, finalized_at, queue, created_at, kind, run_at, attempted_by, attempted_at,
	                                         priority, attempt, max_attempts, payload, metadata, errors)
	           SELECT 1000000 + i, now(), CASE WHEN i <= 400 THEN 'rare' ELSE 'busy' END, now(), 'email.send', now(), 'w', now(),
	                  2, 1, 1, '{}', '{}', '[]'
	           FROM generate_series(1, 40000) i`)
	e.exec(t, "ANALYZE {s}.tasks")
	e.exec(t, "ANALYZE {s}.task_history")

	rare := []string{"rare"}
	for _, filter := range []taskmill.TaskFilter{
		{Queues: rare, States: []taskmill.State{taskmill.StateCompleted}},
		{Queues: rare, States: []taskmill.State{taskmill.StatePending}, GroupKeys: []string{"g1"}},
	} {
		const limit = 50
		read, err := taskmill.ListRowsRead(t.Context(), e.client, &taskmill.ListParams{Filter: filter, Limit: limit}, false)
		if err != nil {
			t.Fatal(err)
		}
		// A page reads one row past its limit, to know whether more follow.
		if read > limit+1 {
			t.Errorf("%+v: read %d rows of tasks, want at most %d", filter, read, limit+1)
		}
	}
}

// A page of dead or canceled tasks reads about its own rows, whatever the
// planner expects: a range of the index of finished tasks for each queue
// and state, which ends with the page. It holds for one queue and for all,
// for a queue with many such tasks and for one with none beside it, and
// with a plan that knows neither the filters nor the size of the page.
//
// The statistics are wrong in the three ways that an incident makes them
// wrong. They expect few dead tasks of a kind that just started to fail:
// with sorts on, the planner then reads every dead task of the queue and
// sorts them, 1,920 rows for a page of 50. They know nothing of a queue
// whose tasks died since the last ANALYZE. And they expect dead tasks in
// every queue, where one has none.
//
// What a page reads besides its own rows is bounded too: the tasks that
// share a finish time with the task where it starts or with the one where
// it ends, as the 300 do that one statement canceled here, and one index
// entry for each queue that holds dead or canceled tasks, when no queue is
// named. A later page reads no more than the first one: not the tasks of
// the pages before it.
func TestPagesOfFinishedTasksReadOnlyTheirRows(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	// A backlog in two queues. Every fifth task of the queue "busy" is dead,
	// 1,920 that died one after the other, and the 200 that died last are
	// of a kind of their own: one task in 50.
	e.exec(t, `INSERT INTO {s}.tasks (queue, kind, state, run_at, payload, max_attempts, finalized_at)
	           SELECT CASE WHEN i <= 400 THEN 'rare' ELSE 'busy' END,
	                  CASE WHEN i > 9000 AND i % 5 = 0 THEN 'webhook.send' ELSE 'email.send' END,
	                  CASE WHEN i > 400 AND i % 5 = 0 THEN 'dead' ELSE 'available' END::{s}.task_state, now(), '{}', 1,
	                  CASE WHEN i > 400 AND i % 5 = 0 THEN now() - (10000 - i) * interval '1 second' END
	           FROM generate_series(1, 10000) i`)
	e.exec(t, "ANALYZE {s}.tasks")
	// What the statistics do not know: the queue "late" got its dead tasks
	// since, and the queue "bulk" its canceled ones, 300 in each statement.
	e.exec(t, `INSERT INTO {s}.tasks (queue, kind, state, run_at, payload, max_attempts, finalized_at)
	           SELECT 'late', 'email.send', 'dead', now(), '{}', 1, now() - i * interval '1 second' FROM generate_series(1, 2000) i`)
	e.exec(t, `INSERT INTO {s}.tasks (queue, kind, state, run_at, payload, max_attempts, finalized_at)
	           SELECT 'bulk', 'email.send', 'canceled', now(), '{}', 1, now() - (i / 300) * interval '1 minute'
	           FROM generate_series(0, 899) i`)

	const (
		limit = 50
		page  = limit + 2 // one row past the limit, to know whether more follow, and one past its finish time
		batch = 300       // the tasks that share a finish time
		found = 3         // an index entry for each queue that holds such tasks
		// Until PostgreSQL 18, a plan reports the rows of a scan that ran
		// several times as their rounded average: up to half a row off for
		// each of the three queues, in each state, and for each queue found.
		rounded = 5
	)
	dead, canceled := []taskmill.State{taskmill.StateDead}, []taskmill.State{taskmill.StateCanceled}
	both := []taskmill.State{taskmill.StateDead, taskmill.StateCanceled}
	for _, c := range []struct {
		what   string
		filter taskmill.TaskFilter
		after  int // the page follows one of this many tasks
		most   int64
	}{
		{"a queue with many dead tasks", taskmill.TaskFilter{Queues: []string{"busy"}, States: dead}, 0, page},
		{"the kind that started to fail", taskmill.TaskFilter{Queues: []string{"busy"}, States: dead, Kinds: []string{"webhook.send"}}, 0, page},
		{"a queue with none, beside one with many", taskmill.TaskFilter{Queues: []string{"rare"}, States: both}, 0, 0},
		{"a queue that the statistics do not know", taskmill.TaskFilter{Queues: []string{"late"}, States: dead}, 0, page},
		{"tasks that one statement canceled", taskmill.TaskFilter{Queues: []string{"bulk"}, States: canceled}, 0, page + batch},
		{"all queues", taskmill.TaskFilter{States: dead}, 0, found + 2*page + rounded},
		{"all queues, both states", taskmill.TaskFilter{States: both}, 0, found + 3*page + batch + rounded},
		// A later page reads the task that it starts below once more.
		{"a later page", taskmill.TaskFilter{Queues: []string{"busy"}, States: dead}, 1000, page + 1},
		// It starts 10 tasks before the end of one statement's 300, and ends
		// in the next statement's.
		{"a page across two statements", taskmill.TaskFilter{Queues: []string{"bulk"}, States: canceled}, batch - 10, page + 2*batch},
	} {
		params := taskmill.ListParams{Filter: c.filter, Limit: limit}
		if c.after > 0 {
			before, err := e.client.List(t.Context(), &taskmill.ListParams{Filter: c.filter, Limit: c.after, Summary: true})
			if err != nil {
				t.Fatal(err)
			}
			params.Cursor = before.Next
		}
		for _, generic := range []bool{false, true} {
			read, err := taskmill.ListRowsRead(t.Context(), e.client, &params, generic)
			if err != nil {
				t.Fatal(err)
			}
			if read > c.most {
				t.Errorf("%s (generic plan %t): a page of %d read %d rows of tasks, want at most %d", c.what, generic, limit, read, c.most)
			}
		}
	}

	// The database does not compile the page's statement (JIT), though it
	// would compile any: until PostgreSQL 18, sorts turned off make the
	// statement look costly enough to compile, which takes longer than the
	// read.
	compiled, err := taskmill.ListCompiled(t.Context(), e.client, &taskmill.ListParams{Filter: taskmill.TaskFilter{States: dead}})
	if err != nil {
		t.Fatal(err)
	}
	if compiled {
		t.Error("the database compiled the statement of a page of dead tasks")
	}
}

// A page of a range of finish times reads the rows of its range only: each
// queue's read starts where the range ends, and stops where the range
// starts or with the page, whichever comes first. So a range that holds less
// than a page does not read on into the older tasks, and neither does a
// filter by kind, which without a range reads a queue's dead tasks until it
// has a page. List runs the statement with a custom plan, which sees the
// bounds that are set.
func TestRangesOfFinishedTasksReadOnlyTheirRows(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	// Two queues with 3,000 dead tasks each, one a second. The queue "busy"
	// also has 600 canceled tasks, one every 5 seconds.
	start := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	e.exec(t, `INSERT INTO {s}.tasks (queue, kind, state, run_at, payload, max_attempts, finalized_at)
	           SELECT v.queue, 'email.send', v.state::{s}.task_state, now(), '{}', 1, $1::timestamptz + i * interval '1 second'
	           FROM (VALUES ('busy', 'dead'), ('busy', 'canceled'), ('other', 'dead')) AS v(queue, state),
	                generate_series(0, 2999) i
	           WHERE v.state = 'dead' OR i % 5 = 0`, start)
	e.exec(t, "ANALYZE {s}.tasks")

	const (
		limit = 50
		page  = limit + 2 // one row past the limit, to know whether more follow, and one past its finish time
		found = 2         // an index entry for each queue that holds such tasks
		// Until PostgreSQL 18, a plan reports the rows of a scan that ran
		// several times as their rounded average: up to half a row off for
		// each of the two queues, in each state, and for each queue found.
		rounded = 3
	)
	second := func(n int) time.Time { return start.Add(time.Duration(n) * time.Second) }
	dead := []taskmill.State{taskmill.StateDead}
	both := []taskmill.State{taskmill.StateDead, taskmill.StateCanceled}
	busy := []string{"busy"}
	for _, c := range []struct {
		what          string
		filter        taskmill.TaskFilter
		since, before time.Time
		after         int // the page follows one of this many tasks
		most          int64
	}{
		{"a range of many pages", taskmill.TaskFilter{Queues: busy, States: dead}, second(1000), second(2000), 0, page},
		// A later page reads the task that it starts below once more.
		{"a later page of the range", taskmill.TaskFilter{Queues: busy, States: dead}, second(1000), second(2000), 100, page + 1},
		{"a range of less than a page", taskmill.TaskFilter{Queues: busy, States: dead}, second(1000), second(1020), 0, 20},
		{"a range with no start", taskmill.TaskFilter{Queues: busy, States: dead}, time.Time{}, second(20), 0, 20},
		{"a range with no end", taskmill.TaskFilter{Queues: busy, States: dead}, second(2980), time.Time{}, 0, 20},
		{"a range that holds nothing", taskmill.TaskFilter{Queues: busy, States: both}, second(-60), second(0), 0, 0},
		{"a kind with no task in the range", taskmill.TaskFilter{Queues: busy, States: dead, Kinds: []string{"report.build"}}, second(1000), second(1500), 0, 500},
		{"both states", taskmill.TaskFilter{Queues: busy, States: both}, second(1000), second(1020), 0, 20 + 4},
		{"all queues, both states", taskmill.TaskFilter{States: both}, second(1000), second(1020), 0, found + 2*20 + 4 + rounded},
	} {
		params := taskmill.ListParams{Filter: c.filter, Limit: limit, FinishedSince: c.since, FinishedBefore: c.before}
		if c.after > 0 {
			params.Limit = c.after
			before, err := e.client.List(t.Context(), &params)
			if err != nil {
				t.Fatal(err)
			}
			params.Limit, params.Cursor = limit, before.Next
		}
		read, err := taskmill.ListRowsRead(t.Context(), e.client, &params, false)
		if err != nil {
			t.Fatal(err)
		}
		if read > c.most {
			t.Errorf("%s: a page of %d read %d rows of tasks, want at most %d", c.what, limit, read, c.most)
		}
	}
}

// Each read of the operations API is one round trip to the database, also
// those that run in a transaction of their own, with a time limit and plan
// settings: a dashboard far from its database makes several per page.
func TestReadsTakeOneRoundTrip(t *testing.T) {
	t.Parallel()
	trips := &roundTrips{}
	e := newTracedEnv(t, trips)
	ctx := t.Context()
	id := e.enqueue(t, sendEmail{}, &taskmill.EnqueueOptions{GroupKey: "g"})
	for name, read := range map[string]func() error{
		"Get":       func() error { _, err := e.client.Get(ctx, id); return err },
		"List":      func() error { _, err := e.client.List(ctx, nil); return err },
		"Count":     func() error { _, err := e.client.Count(ctx, nil); return err },
		"Groups":    func() error { _, err := e.client.Groups(ctx, nil); return err },
		"Queues":    func() error { _, err := e.client.Queues(ctx); return err },
		"Activity":  func() error { _, err := e.client.Activity(ctx, nil); return err },
		"Settings":  func() error { _, err := e.client.Settings(ctx); return err },
		"Workers":   func() error { _, err := e.client.Workers(ctx); return err },
		"Schedules": func() error { _, err := e.client.Schedules(ctx); return err },
	} {
		before := trips.n.Load()
		if err := read(); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if n := trips.n.Load() - before; n != 1 {
			t.Errorf("%s made %d round trips to the database, want 1", name, n)
		}
	}
}

// A read with a time limit sets it in the round trip that carries its
// statement. The limit must hold for that statement, and end with it: the
// connection goes back to the pool for statements that have no limit, also
// after a read that ran out of time. A pool that had to replace the
// connection of each such read would open a new one for every slow page of
// the dashboard.
func TestReadSettingsHoldForTheReadOnly(t *testing.T) {
	t.Parallel()
	trips := &roundTrips{}
	cfg, err := pgxpool.ParseConfig(testdb.URL(t))
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.Tracer = trips
	cfg.MaxConns = 1 // every statement of the test uses the one connection
	pool, err := pgxpool.NewWithConfig(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	e := newEnvWith(t, pool, nil)
	ctx := t.Context()
	const brief = "SELECT set_config('statement_timeout', '50ms', true)"

	before := trips.n.Load()
	got, err := taskmill.ReadAlone(ctx, e.client, brief, "SELECT current_setting('statement_timeout')")
	if err != nil || !slices.Equal(got, []string{"50ms"}) {
		t.Fatalf("the statement ran with a time limit of %v, %v; want 50ms", got, err)
	}
	if n := trips.n.Load() - before; n != 1 {
		t.Errorf("the read made %d round trips, want 1", n)
	}
	unlimited := func(after string) {
		t.Helper()
		if n := e.count(t, "SELECT count(*) FROM pg_sleep(0.1)"); n != 1 {
			t.Fatalf("a statement %s did not run", after)
		}
	}
	unlimited("after the read")
	opened := pool.Stat().NewConnsCount()
	_, err = taskmill.ReadAlone(ctx, e.client, brief, "SELECT pg_sleep(30)::text")
	if pgErr, ok := errors.AsType[*pgconn.PgError](err); !ok || pgErr.Code != "57014" { // query_canceled
		t.Fatalf("a statement past its time limit: got %v, want it canceled", err)
	}
	unlimited("after the read that was canceled")
	if n := pool.Stat().NewConnsCount() - opened; n != 0 {
		t.Errorf("the pool opened %d connections after the canceled read, want it to use the same one again", n)
	}
}

// listed returns the IDs of the tasks that List returns for params, page
// after page, up to a number of pages, and the cursor of the page that it
// did not read, if the list has more.
func listed(t *testing.T, client *taskmill.Client, params taskmill.ListParams, pages int) (got []int64, next string) {
	t.Helper()
	for range pages {
		page, err := client.List(t.Context(), &params)
		if err != nil {
			t.Fatalf("%+v: %v", params, err)
		}
		got = append(got, ids(page.Tasks)...)
		if params.Cursor = page.Next; page.Next == "" {
			break
		}
	}
	return got, params.Cursor
}

func ids(tasks []taskmill.TaskInfo) []int64 {
	out := make([]int64, len(tasks))
	for i, t := range tasks {
		out[i] = t.ID
	}
	return out
}

func TestGroups(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	for _, key := range []string{"Account-1", "account-2", "account-2", "order-9"} {
		e.enqueue(t, sendEmail{}, &taskmill.EnqueueOptions{GroupKey: key})
	}
	// A dead head halts its group, and the task behind it waits. A group
	// halted by its only task is listed too.
	runWorker(t, e, &taskmill.WorkerConfig{
		Handlers: handlers(func(context.Context, *taskmill.Task[sendEmail]) error {
			return taskmill.Permanent(errors.New("rejected"))
		}, nil),
		Queues: map[string]taskmill.QueueOptions{"other": {}},
	})
	head := e.enqueue(t, sendEmail{}, &taskmill.EnqueueOptions{Queue: "other", GroupKey: "order-9"})
	e.enqueue(t, sendEmail{}, &taskmill.EnqueueOptions{Queue: "other", GroupKey: "order-9"})
	solo := e.enqueue(t, sendEmail{}, &taskmill.EnqueueOptions{Queue: "other", GroupKey: "solo"})
	e.waitState(t, head, taskmill.StateDead)
	e.waitState(t, solo, taskmill.StateDead)

	all := []taskmill.GroupInfo{
		{Queue: taskmill.DefaultQueue, Key: "Account-1", Tasks: 1},
		{Queue: taskmill.DefaultQueue, Key: "account-2", Tasks: 2},
		{Queue: taskmill.DefaultQueue, Key: "order-9", Tasks: 1},
		{Queue: "other", Key: "order-9", Tasks: 2, HaltedBy: head},
		{Queue: "other", Key: "solo", Tasks: 1, HaltedBy: solo},
	}
	for _, c := range []struct {
		params taskmill.GroupParams
		want   []taskmill.GroupInfo
	}{
		{taskmill.GroupParams{}, all},
		{taskmill.GroupParams{Search: "ACCOUNT"}, all[:2]},
		{taskmill.GroupParams{Queues: []string{"other"}}, all[3:]},
		{taskmill.GroupParams{Halted: true}, all[3:]},
		{taskmill.GroupParams{Halted: true, Search: "solo"}, all[4:]},
		// Pages of 2 go through every group, in order.
		{taskmill.GroupParams{Limit: 2}, all},
	} {
		var got []taskmill.GroupInfo
		params := c.params
		for {
			page, err := e.client.Groups(t.Context(), &params)
			if err != nil {
				t.Fatal(err)
			}
			got = append(got, page.Groups...)
			if page.Next == "" {
				break
			}
			params.Cursor = page.Next
		}
		if !slices.Equal(got, c.want) {
			t.Fatalf("%+v: got %+v, want %+v", c.params, got, c.want)
		}
	}
	// A page without groups is an empty list, not nil: a caller that encodes
	// it as JSON has always got [] for it.
	none, err := e.client.Groups(t.Context(), &taskmill.GroupParams{Search: "no-such-group"})
	if err != nil || none.Groups == nil || len(none.Groups) != 0 || none.Next != "" {
		t.Fatalf("no groups: got %#v, %v; want an empty list that is not nil", none, err)
	}
	for _, params := range []taskmill.GroupParams{{Limit: -1}, {Cursor: "%%%"}, {Cursor: "b25seS1vbmU"}} { // "only-one"
		if _, err := e.client.Groups(t.Context(), &params); !errors.Is(err, taskmill.ErrInvalidArgument) {
			t.Fatalf("%+v: got %v, want ErrInvalidArgument", params, err)
		}
	}
	// Each named queue reads up to a page, so a call names at most 100.
	queues := make([]string, 101)
	for i := range queues {
		queues[i] = fmt.Sprintf("queue-%d", i)
	}
	if _, err := e.client.Groups(t.Context(), &taskmill.GroupParams{Queues: queues[:100]}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.client.Groups(t.Context(), &taskmill.GroupParams{Queues: queues}); !errors.Is(err, taskmill.ErrInvalidArgument) {
		t.Fatalf("101 queues: got %v, want ErrInvalidArgument", err)
	}
}

func TestGroupPagesReadOnlyTheirRows(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.rawGroups = true // groups written by hand, without tasks
	e.exec(t, `INSERT INTO {s}.groups (queue, group_key, tasks, halted_task_id, halted_at)
	           SELECT q, format('key-%s', lpad(i::text, 4, '0')), 1,
	                  CASE WHEN i % 10 = 0 THEN i END, CASE WHEN i % 10 = 0 THEN now() END
	           FROM unnest('{a,b,c,d,e,f,g,h}'::text[]) AS q, generate_series(1, 1000) AS i`)
	e.exec(t, "ANALYZE {s}.groups")
	page, err := e.client.Groups(t.Context(), &taskmill.GroupParams{Queues: []string{"b"}, Limit: 990})
	if err != nil {
		t.Fatal(err)
	}

	// A page far into the groups reads at most one more row than it returns
	// from each queue it reads, whether it asks for queues or halted groups:
	// none of the groups before it, or of the queues it does not ask for.
	for _, params := range []taskmill.GroupParams{
		{},
		{Halted: true},
		{Queues: []string{"a", "b", "g"}},
		{Queues: []string{"a", "b", "g"}, Halted: true},
	} {
		params.Limit, params.Cursor = 10, page.Next
		read, err := taskmill.GroupRowsRead(t.Context(), e.client, &params)
		if err != nil {
			t.Fatal(err)
		}
		if most := int64(max(1, len(params.Queues)) * (params.Limit + 1)); read > most {
			t.Errorf("%+v: read %d rows of groups, want at most %d", params, read, most)
		}
	}
}

func TestScheduleEnqueuesEachRunOnce(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	schedules := []taskmill.Schedule{{Name: "digest", Cron: "@hourly", Args: sendEmail{To: "digest"}}}
	a := newWorker(t, e, &taskmill.WorkerConfig{Schedules: schedules})
	b := newWorker(t, e, &taskmill.WorkerConfig{Schedules: schedules})
	ctx := t.Context()
	tasks := func() int { return e.count(t, "SELECT count(*) FROM {s}.tasks") }
	// Each run is due at a different time, as cron runs are: runs are
	// deduplicated by their due time.
	makeDue := func(ago time.Duration) {
		e.exec(t, "UPDATE {s}.schedules SET next_run_at = date_trunc('second', now()) - $1 * interval '1 second'",
			ago.Seconds())
	}

	// The first pass stores the schedule, due at the next full hour.
	taskmill.RunSchedules(ctx, a)
	ss, err := e.client.Schedules(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(ss) != 1 || ss[0].Kind != "email.send" || ss[0].Queue != taskmill.DefaultQueue || ss[0].NextRunAt.UTC().Minute() != 0 {
		t.Fatalf("got %+v", ss)
	}
	if n := tasks(); n != 0 {
		t.Fatalf("a schedule that is not due enqueued %d tasks", n)
	}

	// Two maintainers whose terms overlap both find the run due: it is
	// enqueued once, to run at the time it was scheduled for.
	makeDue(time.Second)
	var slot time.Time
	if err := e.pool.QueryRow(ctx, replaceSchema("SELECT next_run_at FROM {s}.schedules", e.schema)).Scan(&slot); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for _, w := range []*taskmill.Worker{a, b} {
		wg.Go(func() { taskmill.RunSchedules(ctx, w) })
	}
	wg.Wait()
	if n := tasks(); n != 1 {
		t.Fatalf("one due run enqueued %d tasks", n)
	}
	page, err := e.client.List(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := page.Tasks[0].RunAt; !got.Equal(slot) {
		t.Fatalf("the task runs at %s, want its slot %s", got, slot)
	}

	// A paused schedule does not run; once resumed, its missed run does.
	if err := e.client.PauseSchedule(ctx, "digest"); err != nil {
		t.Fatal(err)
	}
	makeDue(time.Minute)
	taskmill.RunSchedules(ctx, a)
	if n := tasks(); n != 1 {
		t.Fatal("a paused schedule ran")
	}
	if err := e.client.ResumeSchedule(ctx, "digest"); err != nil {
		t.Fatal(err)
	}
	taskmill.RunSchedules(ctx, a)
	if n := tasks(); n != 2 {
		t.Fatal("a resumed schedule did not run")
	}

	if err := e.client.PauseSchedule(ctx, "unknown"); !errors.Is(err, taskmill.ErrNotFound) {
		t.Fatalf("unknown schedule: got %v", err)
	}
}

// adjust is a task whose payload holds a float.
type adjust struct {
	Delta float64 `json:"delta"`
}

func (adjust) Kind() string { return "test.adjust" }

// ticking is a task whose payload differs every time it is encoded.
type ticking struct {
	encoded *atomic.Int64
}

func (ticking) Kind() string { return "test.ticking" }

func (a ticking) MarshalJSON() ([]byte, error) {
	return fmt.Appendf(nil, `{"n":%d}`, a.encoded.Add(1)), nil
}

func TestScheduleRunsWhateverItsStoredPayloadLooksLike(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	// jsonb rewrites the numbers of the first two payloads: -0 becomes 0,
	// 1e2 becomes 100, and 1e400, which no float64 holds, is written in
	// full. The third payload really changes with every pass.
	w := newWorker(t, e, &taskmill.WorkerConfig{Schedules: []taskmill.Schedule{
		{Name: "negative-zero", Cron: "@hourly", Args: adjust{Delta: math.Copysign(0, -1)}},
		{Name: "exponents", Cron: "@hourly", Args: rawArgs{Body: json.RawMessage(`[1e2, 1.50e1, 1e400]`)}},
		{Name: "ticking", Cron: "@hourly", Args: ticking{encoded: new(atomic.Int64)}},
	}})
	ctx := t.Context()
	storedAt := func(name string) time.Time {
		var at time.Time
		err := e.pool.QueryRow(ctx, replaceSchema("SELECT updated_at FROM {s}.schedules WHERE name = $1", e.schema), name).Scan(&at)
		if err != nil {
			t.Fatal(err)
		}
		return at
	}

	// A definition that did not change is stored once, not in every pass.
	taskmill.RunSchedules(ctx, w)
	for _, name := range []string{"negative-zero", "exponents"} {
		first := storedAt(name)
		taskmill.RunSchedules(ctx, w)
		if again := storedAt(name); !again.Equal(first) {
			t.Errorf("schedule %s did not change, but the next pass stored it again", name)
		}
	}

	// Each schedule runs when it is due, also the one that changes in the
	// same pass.
	e.exec(t, "UPDATE {s}.schedules SET next_run_at = date_trunc('second', now()) - interval '1 second'")
	taskmill.RunSchedules(ctx, w)
	for _, kind := range []string{"test.adjust", "raw", "test.ticking"} {
		if n := e.count(t, "SELECT count(*) FROM {s}.tasks WHERE kind = $1", kind); n != 1 {
			t.Errorf("the due schedule of kind %s enqueued %d tasks, want 1", kind, n)
		}
	}
}

func TestRunScheduleNow(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	w := newWorker(t, e, &taskmill.WorkerConfig{
		Schedules: []taskmill.Schedule{{
			Name:    "yearly-report",
			Cron:    "@yearly",
			Args:    sendEmail{To: "report"},
			Options: &taskmill.EnqueueOptions{Priority: taskmill.PriorityHigh},
		}},
	})
	ctx := t.Context()
	taskmill.RunSchedules(ctx, w)
	before, err := e.client.Schedules(ctx)
	if err != nil {
		t.Fatal(err)
	}

	asked := time.Now()
	if err := e.client.RunSchedule(ctx, "yearly-report"); err != nil {
		t.Fatal(err)
	}
	taskmill.RunSchedules(ctx, w)
	page, err := e.client.List(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Tasks) != 1 {
		t.Fatalf("got %d tasks, want 1", len(page.Tasks))
	}
	// The run has the schedule's options, and runs at the time of the request.
	task := page.Tasks[0]
	if task.Priority != taskmill.PriorityHigh || task.RunAt.Sub(asked).Abs() > 5*time.Second {
		t.Fatalf("got priority %s and run time %s, want high and about %s", task.Priority, task.RunAt, asked)
	}
	// It is a run of the schedule, for the time of the request.
	if got := task.Metadata[taskmill.MetadataSchedule]; got != "yearly-report" || !task.ScheduledFor().Equal(task.RunAt) {
		t.Fatalf("the run is of schedule %q, for %s; want yearly-report, for %s", got, task.ScheduledFor(), task.RunAt)
	}
	// The timetable stays as it was.
	after, err := e.client.Schedules(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !after[0].NextRunAt.Equal(before[0].NextRunAt) || after[0].LastRunAt.IsZero() {
		t.Fatalf("next run %s, want %s; last run %s", after[0].NextRunAt, before[0].NextRunAt, after[0].LastRunAt)
	}

	if err := e.client.PauseSchedule(ctx, "yearly-report"); err != nil {
		t.Fatal(err)
	}
	if err := e.client.RunSchedule(ctx, "yearly-report"); !errors.Is(err, taskmill.ErrInvalidState) {
		t.Fatalf("paused schedule: got %v, want ErrInvalidState", err)
	}
	if err := e.client.RunSchedule(ctx, "unknown"); !errors.Is(err, taskmill.ErrNotFound) {
		t.Fatalf("unknown schedule: got %v, want ErrNotFound", err)
	}
}

// scheduleDueAt stores at as the time the env's schedule is due next.
// RunSchedule stores the time of the request there, so this also stands for
// a request made at that time.
func (e *env) scheduleDueAt(t *testing.T, at time.Time) {
	t.Helper()
	e.exec(t, "UPDATE {s}.schedules SET next_run_at = $1", at)
}

// runTimes returns the run times of the env's tasks, oldest task first.
func (e *env) runTimes(t *testing.T) []time.Time {
	t.Helper()
	page, err := e.client.List(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	var at []time.Time
	for _, task := range slices.Backward(page.Tasks) {
		at = append(at, task.RunAt)
	}
	return at
}

// sameTimes reports whether got holds the times in want, in order.
func sameTimes(got []time.Time, want ...time.Time) bool {
	return slices.EqualFunc(got, want, time.Time.Equal)
}

// newDailySchedule stores a schedule that runs every day at the database's
// current minute. It returns a worker that defines the schedule, and that
// minute of today: the time of a regular run, which has passed. No other
// regular run comes due while the test runs.
func newDailySchedule(t *testing.T, e *env) (*taskmill.Worker, time.Time) {
	t.Helper()
	var slot time.Time
	if err := e.pool.QueryRow(t.Context(), "SELECT date_trunc('minute', now())").Scan(&slot); err != nil {
		t.Fatal(err)
	}
	slot = slot.UTC()
	w := newWorker(t, e, &taskmill.WorkerConfig{Schedules: []taskmill.Schedule{
		{Name: "digest", Cron: fmt.Sprintf("%d %d * * *", slot.Minute(), slot.Hour()), Args: sendEmail{To: "digest"}},
	}})
	taskmill.RunSchedules(t.Context(), w)
	return w, slot
}

func TestRunScheduleJustAfterARegularRunAddsARun(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	w, slot := newDailySchedule(t, e)
	ctx := t.Context()

	// The maintainer enqueues today's regular run.
	e.scheduleDueAt(t, slot)
	taskmill.RunSchedules(ctx, w)
	// An operator asks for a run in the same second.
	request := slot.Add(time.Microsecond)
	e.scheduleDueAt(t, request)
	taskmill.RunSchedules(ctx, w)
	if got := e.runTimes(t); !sameTimes(got, slot, request) {
		t.Fatalf("got runs at %s, want the regular run at %s and the requested run at %s", got, slot, request)
	}
}

func TestRunScheduleJustBeforeARegularRunKeepsThatRun(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	w, slot := newDailySchedule(t, e)
	ctx := t.Context()

	// An operator asked for a run a moment before today's regular run, and
	// the maintainer passes by a moment after it.
	request := slot.Add(-300 * time.Millisecond)
	e.scheduleDueAt(t, request)
	taskmill.RunSchedules(ctx, w)
	taskmill.RunSchedules(ctx, w)
	if got := e.runTimes(t); !sameTimes(got, request, slot) {
		t.Fatalf("got runs at %s, want the requested run at %s and the regular run at %s", got, request, slot)
	}
	// The timetable goes on from there: the next regular run is tomorrow's.
	ss, err := e.client.Schedules(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if want := slot.AddDate(0, 0, 1); !ss[0].NextRunAt.Equal(want) {
		t.Fatalf("the next run is at %s, want %s", ss[0].NextRunAt, want)
	}
}

func TestScheduleCollapsesTheRegularRunsItMissed(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	w := newWorker(t, e, &taskmill.WorkerConfig{Schedules: []taskmill.Schedule{
		{Name: "digest", Cron: "* * * * *", Args: sendEmail{To: "digest"}},
	}})
	ctx := t.Context()
	taskmill.RunSchedules(ctx, w)

	// No maintainer ran for an hour, so 60 regular runs were missed.
	var missed time.Time
	if err := e.pool.QueryRow(ctx, "SELECT date_trunc('minute', now()) - interval '1 hour'").Scan(&missed); err != nil {
		t.Fatal(err)
	}
	e.scheduleDueAt(t, missed)
	taskmill.RunSchedules(ctx, w)
	if got := e.runTimes(t); !sameTimes(got, missed) {
		t.Fatalf("got runs at %s, want one run at %s, the first that was missed", got, missed)
	}
	ss, err := e.client.Schedules(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if next := ss[0].NextRunAt; !next.After(missed.Add(time.Hour)) {
		t.Fatalf("the next run is at %s, among the missed runs: it will run them one by one", next)
	}
}

func TestRunScheduleWhileARegularRunIsDueKeepsThatRun(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	w, slot := newDailySchedule(t, e)
	ctx := t.Context()

	// Today's regular run is due, and the maintainer has not passed by yet.
	// The request must not take its place: that run is the run.
	e.scheduleDueAt(t, slot)
	if err := e.client.RunSchedule(ctx, "digest"); err != nil {
		t.Fatal(err)
	}
	taskmill.RunSchedules(ctx, w)
	taskmill.RunSchedules(ctx, w)
	if got := e.runTimes(t); !sameTimes(got, slot) {
		t.Fatalf("got runs at %s, want the regular run at %s", got, slot)
	}
}

func TestRemovedSchedulesArePruned(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	w := newWorker(t, e, &taskmill.WorkerConfig{
		Schedules: []taskmill.Schedule{{Name: "kept", Cron: "@yearly", Args: sendEmail{}}},
	})
	ctx := t.Context()
	taskmill.RunSchedules(ctx, w)
	for _, name := range []string{"removed-today", "removed-long-ago"} {
		e.exec(t, `INSERT INTO {s}.schedules (name, queue, kind, cron, next_run_at) VALUES ($1, 'default', 'email.send', '@daily', now())`, name)
	}
	e.exec(t, "UPDATE {s}.schedules SET updated_at = now() - interval '2 days' WHERE name IN ('kept', 'removed-long-ago')")

	// A worker that defines no schedules, such as one that only does
	// maintenance, deletes none.
	taskmill.PruneSchedules(ctx, newWorker(t, e, &taskmill.WorkerConfig{}))
	if n := e.count(t, "SELECT count(*) FROM {s}.schedules"); n != 3 {
		t.Fatalf("a maintainer without schedules left %d of 3", n)
	}

	taskmill.PruneSchedules(ctx, w)
	ss, err := e.client.Schedules(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, s := range ss {
		names = append(names, s.Name)
	}
	if !slices.Equal(names, []string{"kept", "removed-today"}) {
		t.Fatalf("got schedules %v", names)
	}
}

// The day counts from when a maintainer last defined the schedule, not from
// the schedule's last run or change. So a schedule that ran and was paused
// long ago keeps both while a worker that does not define it leads, as in a
// rolling deploy, a canary, or a rollback.
func TestScheduleOutlivesAMaintainerThatDoesNotDefineIt(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	defining := func(name string) *taskmill.Worker {
		return newWorker(t, e, &taskmill.WorkerConfig{
			Schedules: []taskmill.Schedule{{Name: name, Cron: "@weekly", Args: sendEmail{}}},
		})
	}
	with, without := defining("weekly"), defining("other")
	ctx := t.Context()
	taskmill.RunSchedules(ctx, with)
	if err := e.client.PauseSchedule(ctx, "weekly"); err != nil {
		t.Fatal(err)
	}
	e.exec(t, "UPDATE {s}.schedules SET last_run_at = now() - interval '2 days', updated_at = now() - interval '2 days'")

	taskmill.PruneSchedules(ctx, with)
	taskmill.PruneSchedules(ctx, without)
	ss, err := e.client.Schedules(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(ss) != 1 || !ss[0].Paused || ss[0].LastRunAt.IsZero() {
		t.Fatalf("got schedules %+v, want the paused one with its last run", ss)
	}

	// The maintainer that defines it notes so once an hour, not in every
	// pass.
	e.exec(t, "UPDATE {s}.schedules SET updated_at = now() - interval '30 minutes'")
	taskmill.PruneSchedules(ctx, with)
	if n := e.count(t, "SELECT count(*) FROM {s}.schedules WHERE updated_at < now() - interval '29 minutes'"); n != 1 {
		t.Fatal("a pass within the hour wrote the schedule again")
	}
}

// A pass skips a schedule whose row another transaction holds: that one
// runs, pauses, or deletes it. So a pass waits for no row, and two
// maintainers that prune at the same time cannot deadlock.
func TestPruningSchedulesWaitsForNoRow(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	w := newWorker(t, e, &taskmill.WorkerConfig{
		Schedules: []taskmill.Schedule{{Name: "kept", Cron: "@yearly", Args: sendEmail{}}},
	})
	taskmill.RunSchedules(t.Context(), w)
	e.exec(t, `INSERT INTO {s}.schedules (name, queue, kind, cron, next_run_at) VALUES ('removed', 'default', 'email.send', '@daily', now())`)
	e.exec(t, "UPDATE {s}.schedules SET updated_at = now() - interval '2 days'")
	tx := e.begin(t)
	e.txExec(t, tx, "SELECT FROM {s}.schedules WHERE name = 'removed' FOR UPDATE")

	// A pass that waited for the row would not end while the test holds it.
	passed := make(chan struct{})
	go func() {
		defer close(passed)
		taskmill.PruneSchedules(t.Context(), w)
	}()
	recv(t, passed, "the pass to end while a schedule's row is held")
	if n := e.count(t, "SELECT count(*) FROM {s}.schedules WHERE updated_at > now() - interval '1 day'"); n != 1 {
		t.Fatal("the pass did not note the schedule that it defines")
	}
}

func TestScheduledRunKnowsItsTimeOnEveryAttempt(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ctx := t.Context()
	// The schedule runs every day at this minute, on the clock of a shop
	// that is 5 hours 30 minutes ahead of UTC.
	var slot time.Time
	if err := e.pool.QueryRow(ctx, "SELECT date_trunc('minute', now())").Scan(&slot); err != nil {
		t.Fatal(err)
	}
	slot = slot.In(time.FixedZone("shop", (5*60+30)*60))

	// The handler fails, then snoozes, then loses its lease, and then
	// succeeds. Each time, it notes what it knows about its run.
	type seen struct {
		attempt             int
		schedule            string
		scheduledFor, runAt time.Time
	}
	calls := make(chan seen, 4)
	leaseLost := make(chan struct{})
	var called atomic.Int32
	h := func(ctx context.Context, task *taskmill.Task[sendEmail]) error {
		calls <- seen{task.Attempt, task.Metadata[taskmill.MetadataSchedule], task.ScheduledFor(), task.RunAt}
		switch called.Add(1) {
		case 1:
			return errors.New("the database is down")
		case 2:
			return taskmill.Snooze(10 * time.Millisecond)
		case 3:
			select {
			case <-leaseLost:
			case <-ctx.Done():
			}
			return errors.New("too late: the task was rescued")
		}
		return nil
	}
	runWorker(t, e, &taskmill.WorkerConfig{
		Handlers: handlers(h, &taskmill.HandlerOptions{Retry: fastRetry}),
		Queues:   defaultQueue(1),
		Schedules: []taskmill.Schedule{{
			Name: "nightly-report", Cron: fmt.Sprintf("%d %d * * *", slot.Minute(), slot.Hour()), Location: slot.Location(),
			Args: sendEmail{To: "report"},
			// taskmill's own keys win over the same keys of the schedule.
			Options: &taskmill.EnqueueOptions{Metadata: map[string]string{taskmill.MetadataSchedule: "mine", "team": "billing"}},
		}},
	})
	waitFor(t, "the schedule to be stored", func() bool { return e.count(t, "SELECT count(*) FROM {s}.schedules") == 1 })
	e.scheduleDueAt(t, slot)

	var got []seen
	for range 3 {
		got = append(got, recv(t, calls, "an attempt"))
	}
	id := int64(e.count(t, "SELECT id FROM {s}.tasks"))
	// The worker of the third call is lost: the maintainer rescues the task,
	// and records why. The rescue stays on record, while the state that it
	// leaves lasts only until the next attempt. A lease that the worker
	// renews meanwhile is expired again, but only the third call's, which
	// is the task's second attempt.
	waitFor(t, "the rescue", func() bool {
		e.exec(t, "UPDATE {s}.tasks SET lease_expires_at = now() - interval '1 second' WHERE state = 'running' AND attempt = 2")
		return slices.ContainsFunc(e.get(t, id).Errors, func(a taskmill.AttemptError) bool { return a.Code == taskmill.CodeLeaseLost })
	})
	// A rescued task waits the default backoff, 2 seconds here, which says
	// nothing about its times: the test makes it due now.
	e.exec(t, "UPDATE {s}.tasks SET run_at = now() WHERE id = $1 AND state = 'retryable'", id)
	close(leaseLost)
	got = append(got, recv(t, calls, "the attempt after the rescue"))

	for i, call := range got {
		if want := []int{1, 2, 2, 3}[i]; call.attempt != want {
			t.Errorf("call %d is attempt %d, want %d", i+1, call.attempt, want)
		}
		// The time is on the schedule's clock, so its day and hour are those
		// that the expression names.
		if call.schedule != "nightly-report" || call.scheduledFor.Format(time.RFC3339) != slot.Format(time.RFC3339) {
			t.Errorf("call %d is a run of schedule %q for %s, want nightly-report for %s",
				i+1, call.schedule, call.scheduledFor.Format(time.RFC3339), slot.Format(time.RFC3339))
		}
	}
	// RunAt is the time the run is for on the first attempt only.
	if first, last := got[0].runAt, got[3].runAt; !first.Equal(slot) || !last.After(slot) {
		t.Errorf("the run time was %s on the first attempt and %s on the last, want %s and a later time", first, last, slot)
	}

	// An operator sees the same in the finished task.
	task := e.waitState(t, id, taskmill.StateCompleted)
	want := map[string]string{
		taskmill.MetadataSchedule:     "nightly-report",
		taskmill.MetadataScheduledFor: slot.Format(time.RFC3339),
		"team":                        "billing",
	}
	if !maps.Equal(task.Metadata, want) || !task.ScheduledFor().Equal(slot) {
		t.Errorf("the task has the metadata %v, for %s; want %v", task.Metadata, task.ScheduledFor(), want)
	}
}

func TestScheduledRunKeepsItsTimeWhateverEnqueueMiddlewareDoes(t *testing.T) {
	t.Parallel()
	// The first middleware notes the metadata that it gets, and writes over
	// taskmill's keys. The second replaces the whole map, as a middleware
	// that builds its own may do.
	var seen map[string]string
	overwrite := func(ctx context.Context, tasks []*taskmill.EnqueueTask, next func(context.Context) error) error {
		seen = maps.Clone(tasks[0].Metadata)
		tasks[0].Metadata[taskmill.MetadataSchedule] = "mine"
		tasks[0].Metadata[taskmill.MetadataScheduledFor] = "never"
		return next(ctx)
	}
	replace := func(ctx context.Context, tasks []*taskmill.EnqueueTask, next func(context.Context) error) error {
		tasks[0].Metadata = map[string]string{"trace": "abc"}
		return next(ctx)
	}
	e := newEnvWith(t, testdb.Pool(t), &taskmill.ClientConfig{EnqueueMiddleware: []taskmill.EnqueueMiddleware{overwrite, replace}})
	w, slot := newDailySchedule(t, e)
	e.scheduleDueAt(t, slot)
	taskmill.RunSchedules(t.Context(), w)

	page, err := e.client.List(t.Context(), nil)
	if err != nil || len(page.Tasks) != 1 {
		t.Fatalf("got %d tasks, %v; want the scheduled run", len(page.Tasks), err)
	}
	own := map[string]string{
		taskmill.MetadataSchedule:     "digest",
		taskmill.MetadataScheduledFor: slot.Format(time.RFC3339),
	}
	// The middleware sees taskmill's keys, but cannot change or drop them.
	if !maps.Equal(seen, own) {
		t.Errorf("the middleware got the metadata %v, want %v", seen, own)
	}
	stored := maps.Clone(own)
	stored["trace"] = "abc"
	if task := page.Tasks[0]; !maps.Equal(task.Metadata, stored) || !task.ScheduledFor().Equal(slot) {
		t.Errorf("the run has the metadata %v, for %s; want %v", task.Metadata, task.ScheduledFor(), stored)
	}
}

func TestEnqueuedTaskCanStandForAScheduledRun(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	// A test of a handler sets taskmill's keys itself: Enqueue keeps them.
	id := e.enqueue(t, sendEmail{To: "report"}, &taskmill.EnqueueOptions{Metadata: map[string]string{
		taskmill.MetadataSchedule:     "nightly-report",
		taskmill.MetadataScheduledFor: "2026-11-12T03:00:00+01:00",
	}})
	want := time.Date(2026, 11, 12, 2, 0, 0, 0, time.UTC)
	if got := e.get(t, id).ScheduledFor(); !got.Equal(want) || got.Hour() != 3 {
		t.Fatalf("the task is for %s, want 03:00 at %s", got, want)
	}
}

func TestNewWorkerCountsTheMetadataThatItAddsToAScheduledRun(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	// This metadata fits the limit of 64 KiB as it is, but not with the two
	// keys of a scheduled run: every run would fail.
	opts := &taskmill.EnqueueOptions{Metadata: map[string]string{"k": strings.Repeat("x", 64<<10-20)}}
	if _, err := e.client.Enqueue(t.Context(), sendEmail{}, opts); err != nil {
		t.Fatalf("the metadata alone: %v", err)
	}
	_, err := taskmill.NewWorker(e.client, &taskmill.WorkerConfig{
		Schedules: []taskmill.Schedule{{Name: "digest", Cron: "@daily", Args: sendEmail{}, Options: opts}},
	})
	if !errors.Is(err, taskmill.ErrInvalidArgument) {
		t.Fatalf("got %v, want ErrInvalidArgument", err)
	}
}

func TestScheduleDoesNotWaitForAHeldGroup(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	w := newWorker(t, e, &taskmill.WorkerConfig{Schedules: []taskmill.Schedule{
		{Name: "ledger", Cron: "@hourly", Args: sendEmail{}, Options: &taskmill.EnqueueOptions{GroupKey: "g"}},
	}})
	// The run gives up on the held group after 50 ms, not after a second.
	taskmill.SetTiming(w, taskmill.Timing{LockWait: 50 * time.Millisecond})
	ctx := t.Context()
	taskmill.RunSchedules(ctx, w)
	e.exec(t, "UPDATE {s}.schedules SET next_run_at = date_trunc('second', now()) - interval '1 second'")
	e.enqueue(t, sendEmail{}, &taskmill.EnqueueOptions{GroupKey: "g"})
	tx := e.begin(t)
	e.txExec(t, tx, "SELECT FROM {s}.groups WHERE group_key = 'g' FOR UPDATE")

	start := time.Now()
	taskmill.RunSchedules(ctx, w)
	if d := time.Since(start); d > 5*time.Second {
		t.Fatalf("the schedule waited %s for the group", d)
	}
	if n := e.count(t, "SELECT count(*) FROM {s}.tasks"); n != 1 {
		t.Fatalf("got %d tasks while the group was held, want 1", n)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	taskmill.RunSchedules(ctx, w)
	if n := e.count(t, "SELECT count(*) FROM {s}.tasks"); n != 2 {
		t.Fatalf("got %d tasks after the group was free, want 2", n)
	}
}

func TestMaintainerRunsDueSchedules(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ran := make(chan struct{}, 1)
	runWorker(t, e, &taskmill.WorkerConfig{
		Handlers: handlers(func(context.Context, *taskmill.Task[sendEmail]) error {
			ran <- struct{}{}
			return nil
		}, nil),
		Queues:    defaultQueue(1),
		Schedules: []taskmill.Schedule{{Name: "digest", Cron: "@hourly", Args: sendEmail{To: "digest"}}},
	})
	waitFor(t, "the schedule to be stored", func() bool { return e.count(t, "SELECT count(*) FROM {s}.schedules") == 1 })
	e.exec(t, "UPDATE {s}.schedules SET next_run_at = date_trunc('second', now()) - interval '1 second'")
	select {
	case <-ran:
	case <-time.After(10 * time.Second):
		t.Fatal("the due schedule did not run")
	}
}
