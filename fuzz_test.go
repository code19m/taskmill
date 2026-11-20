package taskmill_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"math"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/code19m/taskmill"
)

// FuzzList lists tasks with any filters, range of creation times, range of
// finish times, page size, and cursor, as a service passes them on from its
// own users: List returns a page or ErrInvalidArgument, and each page's
// cursor leads to the next page, in the order of the list and inside its
// range. A bound is a number of microseconds since the year 2000, as a
// cursor counts them, and 0 is none.
func FuzzList(f *testing.F) {
	f.Add("", 0, "", "", "", "", int64(0), int64(0), int64(0), int64(0))
	f.Add("MTA", 1, "available", taskmill.DefaultQueue, "email.send", "g", int64(0), int64(0), int64(0), int64(0))
	f.Add("LTE", 1000, "completed", "", "", "", int64(0), int64(0), int64(0), int64(0))
	f.Add("OTIyMzM3MjAzNjg1NDc3NTgwOA", 1001, "nope", "", "", "", int64(0), int64(0), int64(0), int64(0)) // the cursor is 2^63
	f.Add("!", -1, "", "", "", "", int64(0), int64(0), int64(0), int64(0))
	// Text that PostgreSQL rejects once failed the call.
	f.Add("", 0, "", "\x00", "\xff", "\xc3", int64(0), int64(0), int64(0), int64(0))
	const y2k = 946_684_800 // the year 2000 in Unix seconds
	now := (time.Now().Unix() - y2k) * 1e6
	// Cursors of a list in the order of a time, which hold a time as
	// PostgreSQL stores it, and an ID: the first and the last time that a
	// column can hold, the two infinite times, and times and text that no
	// column holds, which once would have failed the call in the database.
	// A list of canceled tasks takes those of the finish order, and a list
	// with a range of creation times those of the creation order.
	for _, position := range []string{
		"0,2", "-211813488000000000,1", "9223371331199999999,1", "9223372036854775807,9223372036854775807",
		"-9223372036854775808,0", "-211813488000000001,1", "9223371331200000000,1", "1,", ",1", "1.5,1",
	} {
		f.Add(base64.RawURLEncoding.EncodeToString([]byte("f"+position)), 1, "canceled", "", "", "", int64(0), int64(0), int64(0), int64(0))
		f.Add(base64.RawURLEncoding.EncodeToString([]byte("c"+position)), 1, "", "", "", "", now-3_600e6, int64(0), int64(0), int64(0))
		f.Add(base64.RawURLEncoding.EncodeToString([]byte("c"+position)), 1, "canceled", "", "", "", int64(0), int64(0), int64(0), int64(0))
	}
	// Ranges of each time: the hours around now, the first and the last
	// time that a column can hold, times that none holds, and a range that
	// ends before it starts.
	for _, r := range [][2]int64{
		{now - 3_600e6, now + 3_600e6},
		{now, 0},
		{0, now},
		{-211813488000000000, 9223371331199999999},
		{-211813488000000001, 0},
		{0, 9223371331200000000},
		{math.MinInt64, math.MaxInt64},
		{2, 1},
	} {
		f.Add("", 1, "canceled", "", "", "", int64(0), int64(0), r[0], r[1])
		for _, state := range []string{"", "available", "completed", "canceled"} {
			f.Add("", 1, state, "", "", "", r[0], r[1], int64(0), int64(0))
		}
	}
	f.Add("", 1, "available", "", "", "", int64(0), int64(0), now, int64(0)) // not a list in finish order
	f.Add("", 1, "canceled", "", "", "", now, int64(0), int64(0), now)       // a range of each time
	e := newEnv(f)
	for i := range 3 {
		res, err := e.client.Enqueue(f.Context(), sendEmail{UserID: int64(i)}, &taskmill.EnqueueOptions{GroupKey: "g"})
		if err != nil {
			f.Fatal(err)
		}
		if i > 0 { // two canceled tasks, for the lists in finish order
			if err := e.client.Cancel(f.Context(), res.ID); err != nil {
				f.Fatal(err)
			}
		}
	}
	// A task with the newest ID and the oldest creation time, as a long
	// transaction leaves it, and a completed one: the creation order is not
	// the order of the IDs, and reads the history too.
	if _, err := e.pool.Exec(f.Context(), strings.ReplaceAll(`
		WITH old AS (
			INSERT INTO {s}.tasks (queue, kind, state, created_at, run_at, payload, max_attempts)
			VALUES ('default', 'email.send', 'available', now() - interval '1 day', now(), '{}', 1)
		)
		INSERT INTO {s}.task_history (id, finalized_at, queue, created_at, kind, run_at, attempted_by, attempted_at,
		                              priority, attempt, max_attempts, payload, metadata, errors)
		VALUES (100, now(), 'default', now() - interval '1 hour', 'email.send', now(), 'w', now(), 2, 1, 1, '{}', '{}', '[]')`,
		"{s}", e.schema)); err != nil {
		f.Fatal(err)
	}
	bound := func(micros int64) time.Time {
		if micros == 0 {
			return time.Time{}
		}
		return time.Unix(y2k+micros/1e6, micros%1e6*1e3)
	}

	f.Fuzz(func(t *testing.T, cursor string, limit int, state, queue, kind, group string, created, createdEnd, finished, finishedEnd int64) {
		p := &taskmill.ListParams{
			Cursor: cursor, Limit: limit,
			CreatedSince: bound(created), CreatedBefore: bound(createdEnd), FinishedSince: bound(finished), FinishedBefore: bound(finishedEnd),
			Filter: taskmill.TaskFilter{States: only(taskmill.State(state)), Queues: only(queue), Kinds: only(kind), GroupKeys: only(group)},
		}
		if _, err := e.client.List(t.Context(), p); errors.Is(err, taskmill.ErrInvalidArgument) {
			return
		}
		listPages(t, e.client, p)
	})
}

// listPages lists every page of p, which names one state at most, and fails
// t unless the tasks come in the order of the list: with a range of creation
// times by when they were created, without one dead or canceled tasks by
// when they finished, each then by ID, and others by ID alone, the latest
// and the newest first. It also fails t for a task outside the range of p.
func listPages(t *testing.T, client *taskmill.Client, p *taskmill.ListParams) {
	t.Helper()
	at := orderTime(p)
	follows := func(task, last *taskmill.TaskInfo) bool {
		if !at(task).Equal(at(last)) {
			return at(task).Before(at(last))
		}
		return task.ID < last.ID
	}
	var last *taskmill.TaskInfo
	for {
		res, err := client.List(t.Context(), p)
		if err != nil {
			t.Fatalf("List(%+v): %v", p, err)
		}
		for i := range res.Tasks {
			task := &res.Tasks[i]
			if last != nil && !follows(task, last) {
				t.Fatalf("List(%+v): task %d, created at %s and finished at %s, follows task %d, created at %s and finished at %s",
					p, task.ID, task.CreatedAt, task.FinishedAt, last.ID, last.CreatedAt, last.FinishedAt)
			}
			if outsideRange(task.CreatedAt, p.CreatedSince, p.CreatedBefore) || outsideRange(task.FinishedAt, p.FinishedSince, p.FinishedBefore) {
				t.Fatalf("List(%+v): task %d, created at %s and finished at %s, is outside the range", p, task.ID, task.CreatedAt, task.FinishedAt)
			}
			last = task
		}
		if res.Next == "" {
			return
		}
		p.Cursor = res.Next
	}
}

// orderTime returns the time of a task that orders the list of p, which
// names one state at most: the zero time for every task of a list in ID
// order.
func orderTime(p *taskmill.ListParams) func(*taskmill.TaskInfo) time.Time {
	states := p.Filter.States
	switch {
	case !p.CreatedSince.IsZero() || !p.CreatedBefore.IsZero():
		return func(task *taskmill.TaskInfo) time.Time { return task.CreatedAt }
	case slices.Equal(states, only(taskmill.StateDead)) || slices.Equal(states, only(taskmill.StateCanceled)):
		return func(task *taskmill.TaskInfo) time.Time { return task.FinishedAt }
	}
	return func(*taskmill.TaskInfo) time.Time { return time.Time{} }
}

// outsideRange reports whether the time at is outside the range from since
// up to before, where a zero time is no bound.
func outsideRange(at, since, before time.Time) bool {
	return !since.IsZero() && at.Before(since) || !before.IsZero() && !at.Before(before)
}

// only returns a filter that holds v, or no filter if v is empty.
func only[T ~string](v T) []T {
	if v == "" {
		return nil
	}
	return []T{v}
}

// rawArgs is a task whose arguments hold any JSON text, as when a service
// keeps the body of a webhook it received.
type rawArgs struct {
	Body json.RawMessage `json:"body"`
}

func (rawArgs) Kind() string { return "raw" }

// FuzzPayload enqueues any JSON text in a task's arguments, and decodes the
// stored task as a handler does: Enqueue stores it or returns
// ErrInvalidArgument, PostgreSQL stores no more than the payload limit
// counted, and the handler gets the value that was enqueued.
func FuzzPayload(f *testing.F) {
	for _, body := range []string{
		`{}`, `[1, 2.50, -0, 1E2, 9007199254740993]`, `{"a": 1, "a": 2}`, `"\u00e9\\u0000"`, `"a\u0000b"`,
		strings.Repeat("[", 64) + strings.Repeat("]", 64),
		`[-12.50, 1E+6, -1.5e-3, 1.000e2, 0e-5, 123.456e-7, 1e-300, 1.7976931348623157e308, 5e-324, -0.000e10, 100e-1]`,
		// A payload whose numbers PostgreSQL writes out to 1.2 MiB.
		"[" + strings.Repeat("1e131071, ", 9) + "1e131071]",
		// Text that PostgreSQL rejects once failed the call.
		"\"\xc3\"", `"\ud800"`, `1e131072`, `0e-99999`,
	} {
		f.Add([]byte(body))
	}
	e := newEnv(f)

	f.Fuzz(func(t *testing.T, body []byte) {
		res, err := e.client.Enqueue(t.Context(), rawArgs{Body: body}, nil)
		if errors.Is(err, taskmill.ErrInvalidArgument) {
			return
		}
		if err != nil {
			t.Fatalf("enqueue %q: %v", body, err)
		}
		info, err := e.client.Get(t.Context(), res.ID)
		if err != nil {
			t.Fatal(err)
		}
		payload, _ := json.Marshal(rawArgs{Body: body}) // as Enqueue encoded it
		var stored bytes.Buffer
		if err := json.Compact(&stored, info.Payload); err != nil {
			t.Fatal(err)
		}
		if counted := taskmill.StoredSize(payload); stored.Len() > counted || counted > 1<<20 {
			t.Fatalf("enqueued %q, which counted as %d bytes, and PostgreSQL stored %d", body, counted, stored.Len())
		}
		var args rawArgs
		if err := json.Unmarshal(info.Payload, &args); err != nil {
			t.Fatalf("decode %q: %v", info.Payload, err)
		}
		var want, got any
		errWant, errGot := json.Unmarshal(body, &want), json.Unmarshal(args.Body, &got)
		if (errWant == nil) != (errGot == nil) || !reflect.DeepEqual(want, got) {
			t.Fatalf("enqueued %q, the handler got %q", body, args.Body)
		}
	})
}

// FuzzBulkCursor checks that a bulk action takes any cursor that it gave, and
// refuses any other with ErrInvalidArgument, never with another error.
func FuzzBulkCursor(f *testing.F) {
	for _, cursor := range []string{"", "MA", "LTE", "%%%", "OTIyMzM3MjAzNjg1NDc3NTgwOA", "MTAw"} {
		f.Add(cursor)
	}
	e := newEnv(f)
	for i := range 3 {
		if _, err := e.client.Enqueue(f.Context(), sendEmail{UserID: int64(i)}, &taskmill.EnqueueOptions{GroupKey: "g"}); err != nil {
			f.Fatal(err)
		}
	}
	f.Fuzz(func(t *testing.T, cursor string) {
		params := &taskmill.BulkParams{Filter: taskmill.TaskFilter{GroupKeys: []string{"g"}}, Cursor: cursor}
		for _, do := range []func(context.Context, *taskmill.BulkParams) (taskmill.BulkResult, error){
			e.client.CancelMatching, e.client.RetryMatching,
		} {
			res, err := do(t.Context(), params)
			if err != nil && !errors.Is(err, taskmill.ErrInvalidArgument) {
				t.Fatalf("cursor %q: %v", cursor, err)
			}
			if err == nil && res.Next != "" {
				t.Fatalf("cursor %q: a finished action gave %q", cursor, res.Next)
			}
		}
		e.checkGroups(t)
	})
}

// FuzzGroups lists FIFO groups with any search, page size, and cursor, as a
// service passes them on from its own users: Groups returns a page or
// ErrInvalidArgument, and each page's cursor leads to the next one, in order.
func FuzzGroups(f *testing.F) {
	f.Add("", 0, false, "")
	f.Add("g", 1, true, "")
	f.Add("", 2, false, "%%%")
	f.Add("", 1, false, "ZGVmYXVsdABn") // "default\x00g"
	f.Add("\x00", -1, false, "AA")
	e := newEnv(f)
	for _, key := range []string{"g", "G2", "g\x1fkey", "h"} {
		if _, err := e.client.Enqueue(f.Context(), sendEmail{}, &taskmill.EnqueueOptions{GroupKey: key}); err != nil {
			f.Fatal(err)
		}
	}
	f.Fuzz(func(t *testing.T, search string, limit int, halted bool, cursor string) {
		p := &taskmill.GroupParams{Search: search, Limit: limit, Halted: halted, Cursor: cursor}
		if _, err := e.client.Groups(t.Context(), p); errors.Is(err, taskmill.ErrInvalidArgument) {
			return
		}
		groupPages(t, e.client, p)
	})
}

// groupPages lists up to 10 pages of groups from p, and fails t unless each
// page's cursor leads on without repeating a group.
func groupPages(t *testing.T, client *taskmill.Client, p *taskmill.GroupParams) {
	t.Helper()
	seen := map[[2]string]bool{}
	for range 10 {
		page, err := client.Groups(t.Context(), p)
		if err != nil {
			t.Fatalf("Groups(%+v): %v", p, err)
		}
		for _, g := range page.Groups {
			if key := [2]string{g.Queue, g.Key}; seen[key] {
				t.Fatalf("Groups(%+v): group %q comes twice", p, key)
			} else {
				seen[key] = true
			}
		}
		if page.Next == "" {
			return
		}
		p.Cursor = page.Next
	}
}
