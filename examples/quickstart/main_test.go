package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/code19m/taskmill"
	"github.com/code19m/taskmill/internal/testdb"
	"github.com/code19m/taskmill/taskmilltest"
)

// Parts 1 and 2 of the tutorial, docs/tutorial, show code that the reader
// writes and later replaces, so main.go does not have it. That code is here,
// where it compiles and runs: the test in internal/docs finds each code block
// of those pages in main.go or in this file.

// enqueueFirstTask is the end of run in part 1 of the tutorial: one task,
// enqueued when the program starts.
func enqueueFirstTask(ctx context.Context, client *taskmill.Client, logger *slog.Logger) error {
	// Add one task. Part 2 replaces these lines with a real order.
	res, err := client.Enqueue(ctx, SendReceipt{OrderID: 1, Email: "user1@example.com"}, nil)
	if err != nil {
		return err
	}
	logger.LogAttrs(ctx, slog.LevelInfo, "task enqueued", slog.Int64(logKeyTaskID, res.ID))
	return nil
}

// TestFirstTask runs part 1 of the tutorial: the task that the program
// enqueues at its start gets its receipt sent.
func TestFirstTask(t *testing.T) {
	t.Parallel()
	client := taskmilltest.NewClient(t, testdb.Pool(t), nil)
	logger := slog.New(slog.DiscardHandler)

	if err := enqueueFirstTask(t.Context(), client, logger); err != nil {
		t.Fatal(err)
	}
	tasks := taskmilltest.Enqueued[SendReceipt](t, client)
	if len(tasks) != 1 || tasks[0].Args.Email != "user1@example.com" {
		t.Fatalf("got %d tasks: %+v", len(tasks), tasks)
	}

	// The handler fails one attempt in three, and the task has 25 attempts.
	var handlers taskmill.Handlers
	taskmill.AddHandler(&handlers, sendReceipt(logger), nil)
	worker, err := taskmill.NewWorker(client, &taskmill.WorkerConfig{Handlers: &handlers})
	if err != nil {
		t.Fatal(err)
	}
	info := &tasks[0].TaskInfo
	for !info.State.Finished() {
		if info, err = worker.RunTask(t.Context(), info.ID); err != nil {
			t.Fatal(err)
		}
	}
	if info.State != taskmill.StateCompleted {
		t.Fatalf("the task is %s after %d attempts, want completed", info.State, info.Attempt)
	}
}

// TestPlaceOrder checks that POST /orders saves the order with its task.
func TestPlaceOrder(t *testing.T) {
	t.Parallel()
	pool, client := testShop(t)

	form := strings.NewReader("email=ann@example.com")
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/orders", form)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()
	placeOrder(pool, client, slog.New(slog.DiscardHandler)).ServeHTTP(rec, req)
	if want := "order 1 placed, task 1 enqueued\n"; rec.Code != http.StatusOK || rec.Body.String() != want {
		t.Fatalf("got %d %q, want 200 %q", rec.Code, rec.Body, want)
	}

	var email string
	if err := pool.QueryRow(t.Context(), "SELECT email FROM orders WHERE id = 1").Scan(&email); err != nil || email != "ann@example.com" {
		t.Fatalf("order 1 has the email %q, %v", email, err)
	}
	tasks := taskmilltest.Enqueued[SendReceipt](t, client)
	if len(tasks) != 1 || tasks[0].Args != (SendReceipt{OrderID: 1, Email: "ann@example.com"}) {
		t.Fatalf("got %d tasks: %+v", len(tasks), tasks)
	}
}

// TestRollback runs the experiment of part 2 of the tutorial: when the
// order's transaction fails after the enqueue, neither the order nor its task
// exists.
func TestRollback(t *testing.T) {
	t.Parallel()
	pool, client := testShop(t)
	ctx, email := t.Context(), "ann@example.com"

	// placeOrder's transaction, with the line that the experiment changes.
	var orderID int64
	var res taskmill.EnqueueResult
	err := pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		err := tx.QueryRow(ctx, "INSERT INTO orders (email) VALUES ($1) RETURNING id", email).Scan(&orderID)
		if err != nil {
			return err
		}
		res, err = client.EnqueueTx(ctx, taskmill.Pgx(tx), SendReceipt{OrderID: orderID, Email: email}, nil)
		return errors.Join(err, errors.New("the payment failed"))
	})
	if err == nil || err.Error() != "the payment failed" {
		t.Fatalf("the transaction returned %v, want only the payment's error", err)
	}

	var orders int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM orders").Scan(&orders); err != nil || orders != 0 {
		t.Fatalf("%d orders, %v; want none", orders, err)
	}
	if _, err := client.Get(ctx, res.ID); !errors.Is(err, taskmill.ErrNotFound) {
		t.Fatalf("task %d: got %v, want ErrNotFound", res.ID, err)
	}
}

// testShop returns a pool and a client in a schema of the test's own. The
// shop's SQL names no schema, so the pool's search path puts the orders table
// there too.
func testShop(t *testing.T) (*pgxpool.Pool, *taskmill.Client) {
	t.Helper()
	schema := testdb.Schema(t, testdb.Pool(t))
	// A pooler in transaction mode refuses a search path: connect without it.
	cfg, err := pgxpool.ParseConfig(testdb.DirectURL(t))
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)

	if err := taskmill.Migrate(t.Context(), pool, &taskmill.MigrateConfig{Schema: schema}); err != nil {
		t.Fatal(err)
	}
	if err := createTables(t.Context(), pool); err != nil {
		t.Fatal(err)
	}
	client, err := taskmill.NewClient(t.Context(), pool, &taskmill.ClientConfig{Schema: schema})
	if err != nil {
		t.Fatal(err)
	}
	return pool, client
}
