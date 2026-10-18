package taskmill_test

import (
	"context"
	"database/sql"
	"errors"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/code19m/taskmill"
)

// SendReceipt is the payload of an example task.
type SendReceipt struct {
	OrderID int64  `json:"order_id"`
	Email   string `json:"email"`
}

// Kind implements taskmill.Args.
func (SendReceipt) Kind() string { return "email.send_receipt" }

func deliverReceipt(context.Context, string, int64) error { return nil }

// This example enqueues a task in the same transaction as the business data
// it belongs to, and runs a worker until the process is told to stop.
func Example() {
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, os.Getenv("DATABASE_URL"))
	if err != nil {
		log.Fatal(err)
	}
	if err := taskmill.Migrate(ctx, pool, nil); err != nil {
		log.Fatal(err)
	}
	client, err := taskmill.NewClient(ctx, pool, nil)
	if err != nil {
		log.Fatal(err)
	}

	err = pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		// ... insert the order in tx ...
		_, err := client.EnqueueTx(ctx, taskmill.Pgx(tx), SendReceipt{OrderID: 42, Email: "a@example.com"}, nil)
		return err
	})
	if err != nil {
		log.Fatal(err)
	}

	var handlers taskmill.Handlers
	taskmill.AddHandler(&handlers, taskmill.HandlerFunc[SendReceipt](
		func(ctx context.Context, t *taskmill.Task[SendReceipt]) error {
			return deliverReceipt(ctx, t.Args.Email, t.Args.OrderID)
		},
	), nil)

	worker, err := taskmill.NewWorker(client, &taskmill.WorkerConfig{
		Handlers: &handlers,
		Queues:   map[string]taskmill.QueueOptions{taskmill.DefaultQueue: {Concurrency: 20}},
	})
	if err != nil {
		log.Fatal(err)
	}
	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := worker.Run(ctx); err != nil {
		log.Print(err)
	}
}

// This example enqueues through database/sql, as bun, sqlx, and plain
// database/sql code do.
func ExampleSQL() {
	ctx := context.Background()
	var client *taskmill.Client // from taskmill.NewClient
	db, err := sql.Open("pgx", os.Getenv("DATABASE_URL"))
	if err != nil {
		log.Fatal(err)
	}
	if err := enqueueWithSQL(ctx, db, client); err != nil {
		log.Fatal(err)
	}
}

func enqueueWithSQL(ctx context.Context, db *sql.DB, client *taskmill.Client) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }() // a no-op after Commit

	// ... write the business data in tx ...
	if _, err := client.EnqueueTx(ctx, taskmill.SQL(tx), SendReceipt{OrderID: 7}, nil); err != nil {
		return err
	}
	return tx.Commit()
}

// This example deduplicates an enqueue: enqueueing the same key again within
// the window reports the existing task instead of creating another.
func ExampleClient_Enqueue_deduplicated() {
	ctx := context.Background()
	var client *taskmill.Client // from taskmill.NewClient
	res, err := client.Enqueue(ctx, SendReceipt{OrderID: 7}, &taskmill.EnqueueOptions{
		DedupeKey:    "receipt-7",
		DedupeWindow: 24 * time.Hour,
	})
	if err != nil {
		log.Fatal(err)
	}
	if res.Duplicate {
		log.Printf("already enqueued as task %d", res.ID)
	}
}

// This example runs tasks of one FIFO group one at a time, in order.
func ExampleEnqueueOptions_fifoGroup() {
	ctx := context.Background()
	var client *taskmill.Client // from taskmill.NewClient
	for _, entry := range []int64{1, 2, 3} {
		_, err := client.Enqueue(ctx, SendReceipt{OrderID: entry}, &taskmill.EnqueueOptions{GroupKey: "customer-7"})
		if err != nil {
			log.Fatal(err)
		}
	}
}

// ApplyPayment is the payload of an example task whose effects live in the
// same database.
type ApplyPayment struct {
	AccountID int64 `json:"account_id"`
	Cents     int64 `json:"cents"`
}

// Kind implements taskmill.Args.
func (ApplyPayment) Kind() string { return "billing.apply_payment" }

// This example completes a task in the handler's own transaction, so the
// balance update happens exactly once even if the task runs again.
func ExampleTask_CompleteTx() {
	var pool *pgxpool.Pool // the application's pool
	handler := func(ctx context.Context, t *taskmill.Task[ApplyPayment]) error {
		return pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
			_, err := tx.Exec(ctx, "UPDATE accounts SET balance = balance + $1 WHERE id = $2", t.Args.Cents, t.Args.AccountID)
			if err != nil {
				return err
			}
			return t.CompleteTx(ctx, taskmill.Pgx(tx))
		})
	}
	var handlers taskmill.Handlers
	taskmill.AddHandler(&handlers, taskmill.HandlerFunc[ApplyPayment](handler), nil)
}

// This example shows the errors a handler returns to control retries.
func ExampleHandlerFunc() {
	handler := taskmill.HandlerFunc[SendReceipt](func(ctx context.Context, t *taskmill.Task[SendReceipt]) error {
		if t.Args.Email == "" {
			return taskmill.Permanent(errors.New("no email address")) // dead at once
		}
		if rateLimited() {
			return taskmill.Snooze(time.Minute) // later, without using an attempt
		}
		return deliverReceipt(ctx, t.Args.Email, t.Args.OrderID) // retried with backoff on error
	})
	var handlers taskmill.Handlers
	taskmill.AddHandler(&handlers, handler, &taskmill.HandlerOptions{Timeout: 30 * time.Second})
}

func rateLimited() bool { return false }

// This example runs a task every night at 03:00 Berlin time.
func ExampleSchedule() {
	var client *taskmill.Client // from taskmill.NewClient
	var handlers taskmill.Handlers
	berlin, err := time.LoadLocation("Europe/Berlin")
	if err != nil {
		log.Fatal(err)
	}
	_, err = taskmill.NewWorker(client, &taskmill.WorkerConfig{
		Handlers: &handlers,
		Queues:   map[string]taskmill.QueueOptions{taskmill.DefaultQueue: {}},
		Schedules: []taskmill.Schedule{
			{Name: "nightly-receipts", Cron: "0 3 * * *", Location: berlin, Args: SendReceipt{}},
		},
	})
	if err != nil {
		log.Fatal(err)
	}
}
