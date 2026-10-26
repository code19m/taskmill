package taskmill_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
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

	// The worker serves the queues of its handlers' kinds: here the default
	// queue.
	worker, err := taskmill.NewWorker(client, &taskmill.WorkerConfig{Handlers: &handlers})
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

	// The "pgx" driver comes from: import _ "github.com/jackc/pgx/v5/stdlib"
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
	taskmill.AddHandler(&handlers, taskmill.HandlerFunc[SendReceipt](
		func(ctx context.Context, t *taskmill.Task[SendReceipt]) error {
			// t.RunAt is the night the run is for.
			return deliverReceipt(ctx, t.Args.Email, t.Args.OrderID)
		},
	), nil)
	berlin, err := time.LoadLocation("Europe/Berlin")
	if err != nil {
		log.Fatal(err)
	}
	_, err = taskmill.NewWorker(client, &taskmill.WorkerConfig{
		Handlers: &handlers,
		Schedules: []taskmill.Schedule{
			{Name: "nightly-receipts", Cron: "0 3 * * *", Location: berlin, Args: SendReceipt{}},
		},
	})
	if err != nil {
		log.Fatal(err)
	}
}

// This example logs every attempt with its duration, as tracing and metrics
// middleware would measure it.
func ExampleMiddleware() {
	logAttempts := taskmill.Middleware(func(ctx context.Context, task *taskmill.TaskInfo, next func(context.Context) error) error {
		start := time.Now()
		err := next(ctx)
		log.Printf("task %d (%s), attempt %d: %v after %s", task.ID, task.Kind, task.Attempt, err, time.Since(start))
		return err
	})
	var client *taskmill.Client // from taskmill.NewClient
	var handlers taskmill.Handlers
	_, err := taskmill.NewWorker(client, &taskmill.WorkerConfig{
		Handlers:   &handlers,
		Middleware: []taskmill.Middleware{logAttempts},
	})
	if err != nil {
		log.Fatal(err)
	}
}

type requestIDKey struct{}

// This example tags every task with the ID of the request that enqueued it,
// and logs the tasks that were enqueued.
func ExampleEnqueueMiddleware() {
	tagRequests := taskmill.EnqueueMiddleware(func(ctx context.Context, tasks []*taskmill.EnqueueTask, next func(context.Context) error) error {
		if id, ok := ctx.Value(requestIDKey{}).(string); ok {
			for _, task := range tasks {
				task.Metadata["request_id"] = id
			}
		}
		if err := next(ctx); err != nil {
			return err
		}
		for _, task := range tasks {
			log.Printf("enqueued task %d (%s) in queue %s", task.Result.ID, task.Kind, task.Queue)
		}
		return nil
	})
	ctx := context.Background()
	var pool *pgxpool.Pool // from pgxpool.New
	_, err := taskmill.NewClient(ctx, pool, &taskmill.ClientConfig{
		EnqueueMiddleware: []taskmill.EnqueueMiddleware{tagRequests},
	})
	if err != nil {
		log.Fatal(err)
	}
}

// receiptMailer sends receipts in bulk.
type receiptMailer struct{}

func (receiptMailer) HandleBatch(ctx context.Context, tasks []*taskmill.Task[SendReceipt]) []error {
	errs := make([]error, len(tasks))
	for i, t := range tasks {
		errs[i] = deliverReceipt(ctx, t.Args.Email, t.Args.OrderID) // or one bulk call to the mail service
	}
	return errs
}

// This example runs receipts in batches of up to 50.
func ExampleAddBatchHandler() {
	var handlers taskmill.Handlers
	taskmill.AddBatchHandler[SendReceipt](&handlers, receiptMailer{}, &taskmill.HandlerOptions{BatchSize: 50})
	_ = taskmill.WorkerConfig{Handlers: &handlers} // serves the kind's default queue
}

// This example prints the migrations, for teams that apply them with their
// own migration tool.
func ExampleMigrations() {
	ms, err := taskmill.Migrations("taskmill")
	if err != nil {
		log.Fatal(err)
	}
	for _, m := range ms {
		fmt.Printf("%04d_%s\n", m.Version, m.Name)
	}
	// Output:
	// 0000_bootstrap
	// 0001_initial
	// 0002_queue_lag
}

// This example retries every dead task of one queue, once the cause of their
// failures is fixed.
func ExampleClient_RetryMatching() {
	ctx := context.Background()
	var client *taskmill.Client // from taskmill.NewClient
	n, err := client.RetryMatching(ctx, &taskmill.TaskFilter{
		Queues: []string{"emails"},
		States: []taskmill.State{taskmill.StateDead},
	})
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("retried %d tasks", n)
}
