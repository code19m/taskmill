package taskmill_test

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/code19m/taskmill"
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
	for _, update := range []func(*taskmill.QueueSettings){nil, func(s *taskmill.QueueSettings) { s.MaxRunning = -1 }} {
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
		return len(lag) >= 4 && lag["behind"] > 0 && lag["retrying"] > 0
	})
	for queue, want := range map[string]time.Duration{"behind": time.Hour, "retrying": 30 * time.Minute} {
		if lag[queue] < want || lag[queue] > want+time.Minute {
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
	if _, err := e.client.List(t.Context(), &taskmill.ListParams{Cursor: "%%%"}); !errors.Is(err, taskmill.ErrInvalidArgument) {
		t.Fatalf("bad cursor: got %v", err)
	}
	unknown := &taskmill.ListParams{Filter: taskmill.TaskFilter{States: []taskmill.State{"bogus"}}}
	if _, err := e.client.List(t.Context(), unknown); !errors.Is(err, taskmill.ErrInvalidArgument) {
		t.Fatalf("unknown state: got %v", err)
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
	page, err := e.client.List(t.Context(), &taskmill.ListParams{Filter: taskmill.TaskFilter{GroupKeys: []string{"a", "b"}}})
	if err != nil {
		t.Fatal(err)
	}
	if got := ids(page.Tasks); !slices.Equal(got, []int64{a2, b, a1}) {
		t.Fatalf("got %v, want %v", got, []int64{a2, b, a1})
	}
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
	for _, params := range []taskmill.GroupParams{{Limit: -1}, {Cursor: "%%%"}, {Cursor: "b25seS1vbmU"}} { // "only-one"
		if _, err := e.client.Groups(t.Context(), &params); !errors.Is(err, taskmill.ErrInvalidArgument) {
			t.Fatalf("%+v: got %v, want ErrInvalidArgument", params, err)
		}
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
	if len(ss) != 1 || ss[0].Kind != "email.send" || ss[0].Queue != taskmill.DefaultQueue || ss[0].NextRunAt.Minute() != 0 {
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
	if task := page.Tasks[0]; task.Priority != taskmill.PriorityHigh || task.RunAt.Sub(asked).Abs() > 5*time.Second {
		t.Fatalf("got priority %s and run time %s, want high and about %s", task.Priority, task.RunAt, asked)
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

func TestScheduleDoesNotWaitForAHeldGroup(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	w := newWorker(t, e, &taskmill.WorkerConfig{Schedules: []taskmill.Schedule{
		{Name: "ledger", Cron: "@hourly", Args: sendEmail{}, Options: &taskmill.EnqueueOptions{GroupKey: "g"}},
	}})
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
