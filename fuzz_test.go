package taskmill_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/code19m/taskmill"
)

// FuzzList lists tasks with any filters, page size, and cursor, as a service
// passes them on from its own users: List returns a page or
// ErrInvalidArgument, and each page's cursor leads to the next, older page.
func FuzzList(f *testing.F) {
	f.Add("", 0, "", "", "", "")
	f.Add("MTA", 1, "available", taskmill.DefaultQueue, "email.send", "g")
	f.Add("LTE", 1000, "completed", "", "", "")
	f.Add("OTIyMzM3MjAzNjg1NDc3NTgwOA", 1001, "nope", "", "", "") // the cursor is 2^63
	f.Add("!", -1, "", "", "", "")
	// Text that PostgreSQL rejects once failed the call.
	f.Add("", 0, "", "\x00", "\xff", "\xc3")
	e := newEnv(f)
	for i := range 3 {
		if _, err := e.client.Enqueue(f.Context(), sendEmail{UserID: int64(i)}, &taskmill.EnqueueOptions{GroupKey: "g"}); err != nil {
			f.Fatal(err)
		}
	}

	f.Fuzz(func(t *testing.T, cursor string, limit int, state, queue, kind, group string) {
		p := &taskmill.ListParams{
			Cursor: cursor, Limit: limit,
			Filter: taskmill.TaskFilter{States: only(taskmill.State(state)), Queues: only(queue), Kinds: only(kind), GroupKeys: only(group)},
		}
		if _, err := e.client.List(t.Context(), p); errors.Is(err, taskmill.ErrInvalidArgument) {
			return
		}
		listPages(t, e.client, p)
	})
}

// listPages lists every page of p, and fails t unless the tasks come newest
// first.
func listPages(t *testing.T, client *taskmill.Client, p *taskmill.ListParams) {
	t.Helper()
	var last int64
	for {
		res, err := client.List(t.Context(), p)
		if err != nil {
			t.Fatalf("List(%+v): %v", p, err)
		}
		for _, task := range res.Tasks {
			if last != 0 && task.ID >= last {
				t.Fatalf("List(%+v): task %d follows task %d", p, task.ID, last)
			}
			last = task.ID
		}
		if res.Next == "" {
			return
		}
		p.Cursor = res.Next
	}
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
