# Part 6: Test it

**Goal:** tests that check what your code enqueues, and what your handler
does, against a real PostgreSQL. **Time:** about 10 minutes. **You start
from:** the program of [part 5](5-outbox.md), or of
[part 4](4-queues-delays-schedules.md) if you skipped the outbox.

## Step 1. Let a test replace the mail server

The handler fails at random, so a test cannot know its result. Give the
handler its mail function as an argument. Replace `sendReceipt` with these
two functions:

```go
// sendReceipt returns the handler: the code that runs one SendReceipt task.
// It sends the email with send.
func sendReceipt(send func(to string) error, logger *slog.Logger) taskmill.HandlerFunc[SendReceipt] {
	return func(ctx context.Context, t *taskmill.Task[SendReceipt]) error {
		if t.Args.Email == "" {
			// A retry cannot help, so give up now: the task becomes dead.
			return taskmill.Permanent(errors.New("the order has no email address"))
		}
		if err := send(t.Args.Email); err != nil {
			return err // taskmill tries again soon
		}
		logger.LogAttrs(ctx, slog.LevelInfo, "receipt sent",
			slog.Int64(logKeyTaskID, t.ID), slog.String(logKeyTo, t.Args.Email))
		return nil
	}
}

// sendMail stands for your mail server. One time in three it fails, as a
// mail server sometimes does.
func sendMail(string) error {
	if rand.IntN(3) == 0 {
		return errors.New("mail server did not answer")
	}
	return nil
}
```

In `run`, pass `sendMail` to the handler:

```go
	taskmill.AddHandler(&handlers, sendReceipt(sendMail, logger), nil)
```

Start the shop, and place an order. **You should see** `receipt sent` in
the log, as before: the shop works as it did.

## Step 2. Test that an order enqueues its tasks

The package `taskmilltest` works against a real PostgreSQL. Create
`main_test.go`:

<!-- grows -->
```go
package main

import (
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/code19m/taskmill"
	"github.com/code19m/taskmill/outbox"
	"github.com/code19m/taskmill/taskmilltest"
)
```

Add a helper that connects to the test database, and the first test. It
places an order, as `curl` does, and checks the tasks that the transaction
enqueued:

```go
// testPool connects to the test database, or skips the test without one. It
// creates the shop's table there.
func testPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("set TEST_DATABASE_URL to run this test")
	}
	pool, err := pgxpool.New(t.Context(), url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if err := createTables(t.Context(), pool); err != nil {
		t.Fatal(err)
	}
	return pool
}

// TestPlaceOrder checks that an order enqueues its receipt and its reminder.
func TestPlaceOrder(t *testing.T) {
	pool := testPool(t)
	client := taskmilltest.NewClient(t, pool, nil) // a new schema, dropped when the test ends
	logger := slog.New(slog.DiscardHandler)

	// Place an order, as curl does.
	form := strings.NewReader("email=ann@example.com")
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/orders", form)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()
	placeOrder(pool, client, outbox.New(client), logger).ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body)
	}

	// Enqueued returns the committed tasks of one kind, oldest first.
	receipts := taskmilltest.Enqueued[SendReceipt](t, client)
	if len(receipts) != 1 || receipts[0].Args.Email != "ann@example.com" {
		t.Fatalf("got %d receipts: %+v", len(receipts), receipts)
	}
	reminders := taskmilltest.Enqueued[RemindCustomer](t, client)
	if len(reminders) != 1 || reminders[0].State != taskmill.StateScheduled {
		t.Fatalf("got %d reminders: %+v", len(reminders), reminders)
	}
}
```

Run the test. `TEST_DATABASE_URL` names a database that the tests may
write to, such as the shop's own:

```sh
TEST_DATABASE_URL='postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable' go test -v .
```

**You should see** the test pass:

```text
=== RUN   TestPlaceOrder
--- PASS: TestPlaceOrder (0.11s)
PASS
ok  	shop	0.459s
```

Each test gets taskmill's tables in a
[schema](../concepts/glossary.md#schema) of its own, so the shop's tasks
and the tests' tasks never mix. The `orders` table is shared.

## Step 3. Test the handler

`taskmilltest.Run` enqueues one task and runs your handler on it once, as
a worker would. Add a test for an order without an address:

```go
// TestReceiptWithoutAddress checks that a receipt with no address is dead
// after one attempt.
func TestReceiptWithoutAddress(t *testing.T) {
	client := taskmilltest.NewClient(t, testPool(t), nil)
	handler := sendReceipt(sendMail, slog.New(slog.DiscardHandler))

	// Run enqueues one task, and runs the handler on it once, as a worker would.
	info, err := taskmilltest.Run(t, client, handler, SendReceipt{OrderID: 1})
	if err == nil || info.State != taskmill.StateDead {
		t.Fatalf("got %s, %v; want a dead task", info.State, err)
	}
}
```

Run the tests again. **You should see** two tests pass.

## Step 4. Test the retries

The last test gives the handler a mail function that fails twice. Then it
runs the task until it is [finished](../concepts/glossary.md#finished).
`RunTask` runs a task now: it does not wait for the backoff. Add `"errors"`
to the imports, and add the test:

```go
// TestReceiptRetries checks that a receipt is sent after the mail server
// failed twice.
func TestReceiptRetries(t *testing.T) {
	client := taskmilltest.NewClient(t, testPool(t), nil)
	failures := 2
	send := func(string) error { // fails twice, then works
		if failures > 0 {
			failures--
			return errors.New("mail server did not answer")
		}
		return nil
	}
	var handlers taskmill.Handlers
	taskmill.AddHandler(&handlers, sendReceipt(send, slog.New(slog.DiscardHandler)), nil)
	worker, err := taskmill.NewWorker(client, &taskmill.WorkerConfig{Handlers: &handlers})
	if err != nil {
		t.Fatal(err)
	}
	res, err := client.Enqueue(t.Context(), SendReceipt{OrderID: 1, Email: "ann@example.com"}, nil)
	if err != nil {
		t.Fatal(err)
	}

	// RunTask runs the task now: it does not wait for the backoff.
	info := &taskmill.TaskInfo{}
	for !info.State.Finished() {
		if info, err = worker.RunTask(t.Context(), res.ID); err != nil {
			t.Fatal(err)
		}
	}
	if info.State != taskmill.StateCompleted || info.Attempt != 3 {
		t.Fatalf("got %s after %d attempts; want completed after 3", info.State, info.Attempt)
	}
}
```

Run the tests once more. **You should see** all three pass:

```text
=== RUN   TestPlaceOrder
--- PASS: TestPlaceOrder (0.11s)
=== RUN   TestReceiptWithoutAddress
--- PASS: TestReceiptWithoutAddress (0.06s)
=== RUN   TestReceiptRetries
--- PASS: TestReceiptRetries (0.07s)
PASS
ok  	shop	0.588s
```

## What you learned

| In the code | taskmill calls it |
|---|---|
| `taskmilltest.NewClient(t, pool, nil)` | a client in a new schema, which is dropped when the test ends |
| `taskmilltest.Enqueued[SendReceipt](t, client)` | the committed tasks of one kind, oldest first |
| `taskmilltest.Run(t, client, handler, args)` | one [attempt](../concepts/glossary.md#attempt) of your handler, as a worker runs it |
| `worker.RunTask(ctx, id)` | run one task now, without a running worker and without the backoff |

## Go deeper

- [Test your code](../guides/testing.md): which helper to use, and code that uses `database/sql`
- [Do work once](../recipes/do-work-once.md): handlers that are safe to run twice
- [Package `taskmilltest`](https://pkg.go.dev/github.com/code19m/taskmill/taskmilltest) on pkg.go.dev

**Next:** [Part 7: Ready to ship](7-ready-to-ship.md)
