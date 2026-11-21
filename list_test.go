package taskmill_test

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"math"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/code19m/taskmill"
	"github.com/code19m/taskmill/internal/testdb"
)

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
				_, err := e.client.Count(ctx, &taskmill.CountParams{Filter: taskmill.TaskFilter{Kinds: []string{s}}})
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
// refuses a page size, a cursor, a state, or a range that it cannot use: a
// range of finish times on a list that is not in finish order, a range of
// each time on one list, a range that does not start before it ends, and a
// bound that PostgreSQL cannot store, also one that only reaches past the
// last time when it is rounded up to the microsecond.
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
		{CreatedSince: at, CreatedBefore: at},
		{CreatedSince: at.Add(time.Nanosecond), CreatedBefore: at},
		{Filter: dead, CreatedSince: at.Add(time.Hour), CreatedBefore: at},
		{CreatedSince: firstTimestamp.Add(-time.Microsecond)},
		{CreatedSince: lastTimestamp.Add(time.Nanosecond)},
		{CreatedBefore: lastTimestamp.Add(time.Microsecond)},
		{CreatedBefore: time.Unix(math.MaxInt64, 0)},
		// A list has one order: a range of creation times puts it in creation
		// order, where it takes no range of finish times.
		{Filter: dead, CreatedSince: at, FinishedSince: at},
		{Filter: dead, CreatedBefore: at, FinishedBefore: at},
		{CreatedSince: at, FinishedBefore: at},
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
	inStJohns, stJohns := clientIn(t, e, "America/St_Johns")

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

// Any list takes a range of creation times: the tasks created at
// CreatedSince or later, and before CreatedBefore, whatever their states,
// the completed ones too. A task created a microsecond outside the range is
// left out. A bound can be finer than the microseconds that the database
// keeps, and still every task is on its own side of it.
//
// The list is then in creation order, the latest first, and by ID among the
// tasks of one creation time: not in ID order, which these tasks' IDs do not
// follow. Its pages chain in that order inside the range, through the tasks
// of one creation time, and past the creation times that only an UPDATE by
// hand writes, the infinite ones.
//
// The bounds are instants. The same range selects the same tasks whatever
// the location of its times, and whatever the time zone of the session: the
// test pool's, which is far from UTC, and one whose clocks go back an hour
// in the middle of the range, so that its local times come twice.
func TestListsTakeARangeOfCreationTimes(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.rawGroups = true // the pending task is written by hand, without its group

	// The range of most lists starts at second 1 and ends at second 3, with
	// America/St_Johns back on standard time from second 2. The task with the
	// highest ID is the oldest, and the completed tasks are 11, 12, and 13.
	start := time.Date(2026, time.November, 1, 4, 29, 58, 0, time.UTC)
	e.exec(t, `INSERT INTO {s}.tasks (id, queue, kind, state, created_at, group_key, lease_expires_at, finalized_at, run_at, payload, max_attempts)
	           OVERRIDING SYSTEM VALUE
	           SELECT v.id, v.queue, v.kind, v.state::{s}.task_state, $1::timestamptz + v.created * interval '1 microsecond', v.grp,
	                  CASE WHEN v.state = 'running' THEN now() END, CASE WHEN v.state IN ('dead', 'canceled') THEN now() END, now(), '{}', 1
	           FROM (VALUES (20, 'a', 'email.send', 'available', 999999, NULL),
	                        (9, 'a', 'email.send', 'available', 1000000, NULL),
	                        (3, 'a', 'email.send', 'running', 1000001, NULL),
	                        (4, 'b', 'email.send', 'dead', 2000000, NULL),
	                        (5, 'a', 'report.build', 'scheduled', 2000000, NULL),
	                        (6, 'a', 'email.send', 'running', 2000000, NULL),
	                        (7, 'a', 'email.send', 'pending', 2999999, 'g'),
	                        (8, 'a', 'email.send', 'canceled', 3000000, NULL),
	                        (1, 'b', 'email.send', 'retryable', 4000000, NULL)
	                ) AS v(id, queue, kind, state, created, grp)`, start)
	e.exec(t, `INSERT INTO {s}.task_history (id, finalized_at, queue, created_at, kind, run_at, attempted_by, attempted_at,
	                                         priority, attempt, max_attempts, payload, metadata, errors)
	           SELECT v.id, now(), v.queue, $1::timestamptz + v.created * interval '1 microsecond', 'email.send', now(), 'w', now(),
	                  2, 1, 1, '{}', '{}', '[]'
	           FROM (VALUES (11, 'a', 2000000), (12, 'b', 2500000), (13, 'a', 5000000)) AS v(id, queue, created)`, start)
	inStJohns, stJohns := clientIn(t, e, "America/St_Johns")

	second := func(n int) time.Time { return start.Add(time.Duration(n) * time.Second) }
	states := func(s ...taskmill.State) taskmill.TaskFilter { return taskmill.TaskFilter{States: s} }
	for _, c := range []struct {
		filter        taskmill.TaskFilter
		since, before time.Time
		want          []int64
	}{
		{taskmill.TaskFilter{}, second(1), second(3), []int64{7, 12, 11, 6, 5, 4, 3, 9}},
		{taskmill.TaskFilter{Queues: []string{"a"}}, second(1), second(3), []int64{7, 11, 6, 5, 3, 9}},
		{taskmill.TaskFilter{Queues: []string{"b", "a", "b"}}, second(1), second(3), []int64{7, 12, 11, 6, 5, 4, 3, 9}},
		{taskmill.TaskFilter{Queues: []string{"none"}}, second(1), second(3), nil},
		{states(taskmill.StateCompleted), second(1), second(3), []int64{12, 11}},
		{states(taskmill.StateRunning), second(1), second(3), []int64{6, 3}},
		{states(taskmill.StateRunning, taskmill.StateCompleted, taskmill.StateAvailable), second(1), second(3), []int64{12, 11, 6, 3, 9}},
		{states(taskmill.StatePending, taskmill.StateScheduled), second(1), second(3), []int64{7, 5}},
		// A list of only dead and canceled tasks takes the range too, and is
		// then in creation order, not in finish order.
		{states(taskmill.StateDead, taskmill.StateCanceled), second(1), second(4), []int64{8, 4}},
		{states(taskmill.StateDead), second(1), second(3), []int64{4}},
		{taskmill.TaskFilter{Kinds: []string{"report.build"}}, second(1), second(3), []int64{5}},
		{taskmill.TaskFilter{GroupKeys: []string{"g"}}, second(1), second(3), []int64{7}},
		{taskmill.TaskFilter{}, second(2), time.Time{}, []int64{13, 1, 8, 7, 12, 11, 6, 5, 4}},
		{taskmill.TaskFilter{}, time.Time{}, second(2), []int64{3, 9, 20}},
		// A nanosecond after a bound, the task created at the bound is
		// before it.
		{taskmill.TaskFilter{}, second(1).Add(time.Nanosecond), second(3).Add(time.Nanosecond), []int64{8, 7, 12, 11, 6, 5, 4, 3}},
		{taskmill.TaskFilter{}, second(5).Add(time.Microsecond), second(6), nil},
		{taskmill.TaskFilter{}, firstTimestamp, lastTimestamp, []int64{13, 1, 8, 7, 12, 11, 6, 5, 4, 3, 9, 20}},
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
			params := taskmill.ListParams{Filter: c.filter, CreatedSince: c.since.In(zone.bounds), CreatedBefore: c.before.In(zone.bounds)}
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

	// Two tasks get the creation times that only an UPDATE by hand writes.
	// A range with one bound holds the infinite time on its side, and a page
	// continues past either.
	e.exec(t, "UPDATE {s}.tasks SET created_at = CASE id WHEN 9 THEN 'infinity' ELSE '-infinity' END::timestamptz WHERE id IN (9, 20)")
	for _, c := range []struct {
		since, before time.Time
		want          []int64
	}{
		{second(4), time.Time{}, []int64{9, 13, 1}},
		{time.Time{}, second(2), []int64{3, 20}},
		{second(1), second(2), []int64{3}},
	} {
		params := taskmill.ListParams{CreatedSince: c.since, CreatedBefore: c.before, Limit: 1}
		if got, next := listed(t, e.client, params, len(c.want)+1); next != "" || !slices.Equal(got, c.want) {
			t.Errorf("%+v in pages of 1: got %v and next %q, want %v and no next page", params, got, next, c.want)
		}
	}
}

// clientIn returns a client of e's schema whose sessions are in the time
// zone, and the zone.
func clientIn(t *testing.T, e *env, zone string) (*taskmill.Client, *time.Location) {
	t.Helper()
	location, err := time.LoadLocation(zone)
	if err != nil {
		t.Fatal(err)
	}

	cfg, err := pgxpool.ParseConfig(testdb.DirectURL(t)) // the time zone is a parameter of the session
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams["timezone"] = zone

	pool, err := pgxpool.NewWithConfig(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)

	client, err := taskmill.NewClient(t.Context(), pool, &taskmill.ClientConfig{Schema: e.schema, Logger: testLogger(t)})
	if err != nil {
		t.Fatal(err)
	}
	return client, location
}

// A task's creation time is the start of the transaction that enqueued it,
// and its ID is as new as its insert. So a transaction that stays open, and
// enqueues at its end, leaves a task with an older creation time and a
// higher ID than a task that was enqueued meanwhile. A range of creation
// times holds each of the two by its creation time, and lists them in the
// order of that time, whatever their IDs say.
func TestRangeOfCreationTimesHoldsTheTaskOfALongTransaction(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ctx := t.Context()

	long := e.begin(t)
	var began time.Time
	if err := long.QueryRow(ctx, "SELECT now()").Scan(&began); err != nil {
		t.Fatal(err)
	}

	waitFor(t, "the database's clock to pass the start of the transaction", func() bool {
		return e.count(t, "SELECT count(*) WHERE clock_timestamp() > $1", began) == 1
	})

	meanwhile := e.enqueue(t, sendEmail{To: "meanwhile"}, nil)
	late, err := e.client.EnqueueTx(ctx, taskmill.Pgx(long), sendEmail{To: "late"}, nil)
	if err != nil {
		t.Fatal(err)
	}

	if err := long.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	first, err := e.client.Get(ctx, meanwhile)
	if err != nil {
		t.Fatal(err)
	}
	if late.ID <= meanwhile || !first.CreatedAt.After(began) {
		t.Fatalf("the task of the long transaction has ID %d and was created at %s, the other ID %d at %s: want the higher ID with the earlier time",
			late.ID, began, meanwhile, first.CreatedAt)
	}

	for _, c := range []struct {
		what   string
		params taskmill.ListParams
		want   []int64
	}{
		{"every task", taskmill.ListParams{}, []int64{late.ID, meanwhile}},
		{"the range of both", taskmill.ListParams{CreatedSince: began}, []int64{meanwhile, late.ID}},
		{"the range that ends with the later creation time", taskmill.ListParams{CreatedBefore: first.CreatedAt}, []int64{late.ID}},
		{"the range that starts with it", taskmill.ListParams{CreatedSince: first.CreatedAt}, []int64{meanwhile}},
	} {
		for _, limit := range []int{1, 100} {
			c.params.Limit = limit
			if got, next := listed(t, e.client, c.params, 3); next != "" || !slices.Equal(got, c.want) {
				t.Errorf("%s, in pages of %d: got %v and next %q, want %v and no next page", c.what, limit, got, next, c.want)
			}
		}
	}
}

// A range of creation times holds a task wherever it is in its life: while
// it waits, while a worker runs it, which takes it out of the index of
// creation time, and once it is completed, in the history.
func TestRangeOfCreationTimesFollowsATaskThroughItsLife(t *testing.T) {
	t.Parallel()
	e := newEnv(t)

	var before time.Time
	if err := e.pool.QueryRow(t.Context(), "SELECT now()").Scan(&before); err != nil {
		t.Fatal(err)
	}

	// The worker completes the tasks of user 1, and keeps those of user 2
	// running. No worker reads the queue "idle".
	release := make(chan struct{})
	defer close(release)
	runWorker(t, e, &taskmill.WorkerConfig{
		Handlers: handlers(func(_ context.Context, task *taskmill.Task[sendEmail]) error {
			if task.Args.UserID == 2 {
				<-release
			}
			return nil
		}, nil),
		Queues: defaultQueue(2),
	})

	completed := e.enqueue(t, sendEmail{UserID: 1}, nil)
	running := e.enqueue(t, sendEmail{UserID: 2}, nil)
	waiting := e.enqueue(t, sendEmail{UserID: 3}, &taskmill.EnqueueOptions{Queue: "idle"})
	e.waitState(t, completed, taskmill.StateCompleted)
	e.waitState(t, running, taskmill.StateRunning)

	page, err := e.client.List(t.Context(), &taskmill.ListParams{CreatedSince: before})
	if err != nil {
		t.Fatal(err)
	}

	want := []taskmill.State{taskmill.StateAvailable, taskmill.StateRunning, taskmill.StateCompleted}
	var states []taskmill.State
	for _, task := range page.Tasks {
		states = append(states, task.State)
	}
	if !slices.Equal(ids(page.Tasks), []int64{waiting, running, completed}) || !slices.Equal(states, want) {
		t.Errorf("the tasks created since the test began: %v in the states %v, want the three, the latest first, in the states %v",
			ids(page.Tasks), states, want)
	}

	for _, c := range []struct {
		filter taskmill.TaskFilter
		want   []int64
	}{
		{taskmill.TaskFilter{States: []taskmill.State{taskmill.StateRunning}}, []int64{running}},
		{taskmill.TaskFilter{States: []taskmill.State{taskmill.StateCompleted, taskmill.StateAvailable}}, []int64{waiting, completed}},
		{taskmill.TaskFilter{Queues: []string{taskmill.DefaultQueue}}, []int64{running, completed}},
	} {
		if got, _ := listed(t, e.client, taskmill.ListParams{Filter: c.filter, CreatedSince: before}, 1); !slices.Equal(got, c.want) {
			t.Errorf("%+v, created since the test began: got %v, want %v", c.filter, got, c.want)
		}
	}
}

// A cursor of a list in creation order continues inside the range that it
// is passed with: below the end of a range that ends before the cursor's
// place, and not past the start of one. Each of the three orders refuses the
// cursors of the other two.
func TestCursorOfCreationOrderStaysInItsRange(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ctx := t.Context()

	// Tasks 1 to 6 were created one a second, and died in the other order.
	start := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	e.exec(t, `INSERT INTO {s}.tasks (queue, kind, state, created_at, finalized_at, run_at, payload, max_attempts)
	           SELECT 'a', 'email.send', 'dead', $1::timestamptz + i * interval '1 second', $1::timestamptz - i * interval '1 second', now(), '{}', 1
	           FROM generate_series(1, 6) i`, start)
	second := func(n int) time.Time { return start.Add(time.Duration(n) * time.Second) }

	page, err := e.client.List(ctx, &taskmill.ListParams{CreatedSince: second(1), Limit: 2})
	if err != nil || !slices.Equal(ids(page.Tasks), []int64{6, 5}) || page.Next == "" {
		t.Fatalf("the first page by creation time: %+v, %v; want the tasks 6 and 5, and a next page", page, err)
	}

	for _, c := range []struct {
		what   string
		params taskmill.ListParams
		want   []int64
	}{
		{"its own range", taskmill.ListParams{CreatedSince: second(1)}, []int64{4, 3, 2, 1}},
		{"a range that starts later", taskmill.ListParams{CreatedSince: second(3)}, []int64{4, 3}},
		{"a range that ends above its place", taskmill.ListParams{CreatedBefore: second(6)}, []int64{4, 3, 2, 1}},
		{"a range that ends below its place", taskmill.ListParams{CreatedBefore: second(3)}, []int64{2, 1}},
		{"a range that ends at its place", taskmill.ListParams{CreatedSince: second(2), CreatedBefore: second(5)}, []int64{4, 3, 2}},
	} {
		c.params.Cursor = page.Next
		if got, next := listed(t, e.client, c.params, 1); next != "" || !slices.Equal(got, c.want) {
			t.Errorf("the cursor after task 5, with %s: got %v and next %q, want %v and no next page", c.what, got, next, c.want)
		}
	}

	dead := taskmill.TaskFilter{States: []taskmill.State{taskmill.StateDead}}
	orders := map[string]taskmill.ListParams{
		"ID order":       {Limit: 2},
		"creation order": {Limit: 2, CreatedSince: second(1)},
		"finish order":   {Limit: 2, Filter: dead},
	}

	cursors := make(map[string]string)
	for name, p := range orders {
		page, err := e.client.List(ctx, &p)
		if err != nil || page.Next == "" {
			t.Fatalf("the first page in %s: %+v, %v; want a next page", name, page, err)
		}
		cursors[name] = page.Next
	}

	for name, p := range orders {
		for from, cursor := range cursors {
			p.Cursor = cursor
			if _, err := e.client.List(ctx, &p); (err == nil) != (from == name) || err != nil && !errors.Is(err, taskmill.ErrInvalidArgument) {
				t.Errorf("a list in %s with the cursor of a list in %s: got %v, want ErrInvalidArgument for the cursor of another order only", name, from, err)
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

// A page of a range of creation times reads about its own rows, wherever the
// range lies and however long it is: for each queue, a range of the index
// of creation time of the tasks that are not running, and one of the
// history's, each from the page's position back to the start of the range,
// or up to the page. So a range that holds less than a page does not read on
// into the older tasks, a later page reads no task of the pages before it,
// and a filter by kind or by state reads no task outside the range. The
// bounds are conditions of the indexes under any plan, also one that knows
// neither the filters nor the size of the page.
//
// What a page reads besides is bounded too: the running tasks of its
// queues, which no index holds by creation time, and one index entry for
// each queue that each of the three places holds, when no queue is named.
//
// The bounds hold whatever the planner expects. Here its statistics first
// say that every task of the queue runs, as they do when a backlog arrives
// in a table that held little else: without its settings, the statement
// would then read the whole table to find the 30 tasks that run.
func TestRangesOfCreationTimesReadOnlyTheirRows(t *testing.T) {
	t.Parallel()
	e := newEnv(t)

	e.exec(t, `INSERT INTO {s}.tasks (queue, kind, state, lease_expires_at, payload, max_attempts, run_at)
	           SELECT 'busy', 'email.send', 'running', now(), '{}', 1, now() FROM generate_series(1, 2000)`)
	e.exec(t, "ANALYZE {s}.tasks")
	e.exec(t, "DELETE FROM {s}.tasks")

	// Two queues with 3,000 available tasks and 3,000 completed ones each,
	// one a second. The queue "busy" also has 600 scheduled tasks, one every
	// 5 seconds, and runs 30 tasks.
	start := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	e.exec(t, `INSERT INTO {s}.tasks (queue, kind, state, created_at, lease_expires_at, run_at, payload, max_attempts)
	           SELECT v.queue, 'email.send', v.state::{s}.task_state, $1::timestamptz + i * interval '1 second',
	                  CASE WHEN v.state = 'running' THEN now() END, now(), '{}', 1
	           FROM (VALUES ('busy', 'available'), ('busy', 'scheduled'), ('busy', 'running'), ('other', 'available')) AS v(queue, state),
	                generate_series(0, 2999) i
	           WHERE v.state = 'available' OR v.state = 'scheduled' AND i % 5 = 0 OR v.state = 'running' AND i BETWEEN 1000 AND 1029`, start)
	e.exec(t, `INSERT INTO {s}.task_history (id, finalized_at, queue, created_at, kind, run_at, attempted_by, attempted_at,
	                                         priority, attempt, max_attempts, payload, metadata, errors)
	           SELECT 1000000 + 2 * i + v.n, now(), v.queue, $1::timestamptz + i * interval '1 second', 'email.send', now(), 'w', now(),
	                  2, 1, 1, '{}', '{}', '[]'
	           FROM (VALUES ('busy', 0), ('other', 1)) AS v(queue, n), generate_series(0, 2999) i`, start)
	e.exec(t, "ANALYZE {s}.task_history")

	const (
		limit   = 50
		page    = limit + 1 // one row past the limit, to know whether more follow
		running = 30        // the running tasks of the queue "busy"
		// Until PostgreSQL 18, a plan reports the rows of a scan that ran
		// several times as their rounded average: up to half a row off for
		// each of the two queues, in each of the three places, and for each
		// queue found.
		rounded = 6
	)

	second := func(n int) time.Time { return start.Add(time.Duration(n) * time.Second) }
	completed := []taskmill.State{taskmill.StateCompleted}
	busy := []string{"busy"}
	type bounded struct {
		what   string
		params taskmill.ListParams
		most   int64
	}
	var cases []bounded
	for _, c := range []struct {
		what          string
		filter        taskmill.TaskFilter
		since, before time.Time
		after         int // the page follows one of this many tasks
		most          int64
	}{
		{"a range of many pages", taskmill.TaskFilter{Queues: busy}, second(1000), second(2000), 0, 2*page + running},
		{"a later page of the range", taskmill.TaskFilter{Queues: busy}, second(1000), second(2000), 100, 2*page + running},
		{"the oldest tasks", taskmill.TaskFilter{Queues: busy}, time.Time{}, second(100), 0, 2*page + running},
		{"completed tasks", taskmill.TaskFilter{Queues: busy, States: completed}, second(1000), second(2000), 0, page},
		{"a later page of completed tasks", taskmill.TaskFilter{Queues: busy, States: completed}, second(1000), second(2000), 500, page},
		{"a range of less than a page", taskmill.TaskFilter{Queues: busy}, second(2000), second(2020), 0, 20 + 4 + 20 + running},
		{"a range with no start", taskmill.TaskFilter{Queues: busy, States: completed}, time.Time{}, second(20), 0, 20},
		{"a range with no end", taskmill.TaskFilter{Queues: busy, States: completed}, second(2980), time.Time{}, 0, 20},
		{"a range that holds nothing", taskmill.TaskFilter{Queues: busy, States: completed}, second(-60), second(0), 0, 0},
		{"a kind with no task in the range", taskmill.TaskFilter{Queues: busy, States: completed, Kinds: []string{"report.build"}}, second(1000), second(1500), 0, 500},
		// The index holds the queue's tasks of every state but running.
		{"one state", taskmill.TaskFilter{Queues: busy, States: []taskmill.State{taskmill.StateScheduled}}, second(1000), second(1100), 0, 100 + 20},
		{"running tasks", taskmill.TaskFilter{Queues: busy, States: []taskmill.State{taskmill.StateRunning}}, second(0), second(10), 0, running},
		{"all queues, completed tasks", taskmill.TaskFilter{States: completed}, second(1000), second(1020), 0, 2 + 2*20 + rounded},
		{"all queues", taskmill.TaskFilter{}, second(1000), second(1020), 0, 2 + 1 + 2 + 2*20 + 4 + running + 2*20 + rounded},
	} {
		params := taskmill.ListParams{Filter: c.filter, Limit: limit, CreatedSince: c.since, CreatedBefore: c.before}
		if c.after > 0 {
			params.Limit = c.after
			before, err := e.client.List(t.Context(), &params)
			if err != nil {
				t.Fatal(err)
			}
			params.Limit, params.Cursor = limit, before.Next
		}
		cases = append(cases, bounded{c.what, params, c.most})
	}

	for _, statistics := range []string{"of a table where every task runs", "of the table as it is"} {
		for _, c := range cases {
			checkRowsRead(t, e.client, c.what+", with the statistics "+statistics, &c.params, c.most)
		}
		e.exec(t, "ANALYZE {s}.tasks")
	}

	compiled, err := taskmill.ListCompiled(t.Context(), e.client, &taskmill.ListParams{CreatedSince: second(1000)})
	if err != nil {
		t.Fatal(err)
	}
	if compiled {
		t.Error("the database compiled the statement of a page in creation order")
	}
}

// checkRowsRead fails t if the read of a page of tasks on params reads more
// than most rows of tasks, with a custom plan or with a generic one.
func checkRowsRead(t *testing.T, client *taskmill.Client, what string, params *taskmill.ListParams, most int64) {
	t.Helper()
	for _, generic := range []bool{false, true} {
		read, err := taskmill.ListRowsRead(t.Context(), client, params, generic)
		if err != nil {
			t.Fatal(err)
		}
		if read > most {
			t.Errorf("%s (generic plan %t): a page of %d read %d rows of tasks, want at most %d", what, generic, params.Limit, read, most)
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
