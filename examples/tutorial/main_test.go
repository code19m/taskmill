package main

import (
	"errors"
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
