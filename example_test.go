package taskmill_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"log/slog"
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
	// Without a logger, taskmill logs nothing, not even errors.
	client, err := taskmill.NewClient(ctx, pool, &taskmill.ClientConfig{Logger: slog.Default()})
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
			return taskmill.Permanent(errors.New("no email address")) // dead right away
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
	// 0003_halted_groups
	// 0004_stuck_work
}

// This example retries every dead task of one queue, once the cause of their
// failures is fixed.
func ExampleClient_RetryMatching() {
	ctx := context.Background()
	var client *taskmill.Client // from taskmill.NewClient
	res, err := client.RetryMatching(ctx, &taskmill.BulkParams{Filter: taskmill.TaskFilter{
		Queues: []string{"emails"},
		States: []taskmill.State{taskmill.StateDead},
	}})
	if err != nil {
		log.Fatal(err)
	}
	// Other transactions held the skipped tasks, their FIFO groups, or their
	// dedupe keys: run it again for them.
	log.Printf("retried %d tasks, skipped %d", res.Changed, res.Skipped)
}

// This example enqueues many tasks in one call: one round trip and one
// transaction, so all of them exist or none does.
func ExampleClient_EnqueueMany() {
	ctx := context.Background()
	var client *taskmill.Client // from taskmill.NewClient
	params := make([]taskmill.EnqueueParams, 0, 100)
	for orderID := int64(1); orderID <= 100; orderID++ {
		params = append(params, taskmill.EnqueueParams{Args: SendReceipt{OrderID: orderID}})
	}
	results, err := client.EnqueueMany(ctx, params)
	if err != nil {
		log.Fatal(err)
	}
	// The results are in the order of params.
	log.Printf("enqueued %d tasks; the first is task %d", len(results), results[0].ID)
}

// ResizeImage is the payload of an example task whose kind has defaults of
// its own.
type ResizeImage struct {
	ImageID int64 `json:"image_id"`
}

// Kind implements taskmill.Args.
func (ResizeImage) Kind() string { return "image.resize" }

// EnqueueDefaults implements taskmill.ArgsWithDefaults.
func (ResizeImage) EnqueueDefaults() taskmill.EnqueueOptions {
	return taskmill.EnqueueOptions{Queue: "images", MaxAttempts: 5}
}

// This example gives a kind its own queue and number of attempts. An option
// set in the call wins over the kind's default, field by field.
func ExampleArgsWithDefaults() {
	ctx := context.Background()
	var client *taskmill.Client // from taskmill.NewClient

	// Goes to the queue "images", with at most 5 attempts.
	if _, err := client.Enqueue(ctx, ResizeImage{ImageID: 7}, nil); err != nil {
		log.Fatal(err)
	}
	// Still goes to "images" with 5 attempts, but with a high priority.
	_, err := client.Enqueue(ctx, ResizeImage{ImageID: 8}, &taskmill.EnqueueOptions{Priority: taskmill.PriorityHigh})
	if err != nil {
		log.Fatal(err)
	}
}

// This example lets at most 4 tasks of a queue run at the same time, across
// all workers. The setting is stored in the database, so one call is enough.
func ExampleClient_UpdateQueue() {
	ctx := context.Background()
	var client *taskmill.Client // from taskmill.NewClient
	err := client.UpdateQueue(ctx, "partner-api", func(s *taskmill.QueueSettings) { s.MaxRunning = 4 })
	if err != nil {
		log.Fatal(err)
	}
}

// This example pauses a queue during an incident, and resumes it afterwards.
func ExampleClient_PauseQueue() {
	ctx := context.Background()
	var client *taskmill.Client // from taskmill.NewClient

	// Workers stop fetching the queue's tasks. Running tasks finish.
	if err := client.PauseQueue(ctx, "partner-api"); err != nil {
		log.Fatal(err)
	}
	// ... the partner's API works again ...
	if err := client.ResumeQueue(ctx, "partner-api"); err != nil {
		log.Fatal(err)
	}
}

// This example prints each queue's available tasks and its lag.
func ExampleClient_Queues() {
	ctx := context.Background()
	var client *taskmill.Client // from taskmill.NewClient
	queues, err := client.Queues(ctx)
	if err != nil {
		log.Fatal(err)
	}
	for _, q := range queues {
		// Counting stops at a cap: Capped then says that N is a lower bound.
		// Lag has no cap.
		available := q.Counts[taskmill.StateAvailable]
		log.Printf("queue %s: %d available (capped: %t), the oldest has waited %s",
			q.Name, available.N, available.Capped, q.Lag)
	}
}

// This example prints the dead tasks of one queue with their last errors,
// newest first, one page at a time.
func ExampleClient_List() {
	ctx := context.Background()
	var client *taskmill.Client // from taskmill.NewClient
	params := &taskmill.ListParams{Filter: taskmill.TaskFilter{
		Queues: []string{"emails"},
		States: []taskmill.State{taskmill.StateDead},
	}}
	for {
		page, err := client.List(ctx, params)
		if err != nil {
			log.Fatal(err)
		}
		for _, task := range page.Tasks {
			if n := len(task.Errors); n > 0 {
				log.Printf("task %d (%s): %s", task.ID, task.Kind, task.Errors[n-1].Message)
			}
		}
		if page.Next == "" {
			break
		}
		params.Cursor = page.Next
	}
}

// This example cancels a task, and tells apart the two reasons why a task
// cannot be canceled.
func ExampleClient_Cancel() {
	ctx := context.Background()
	var client *taskmill.Client // from taskmill.NewClient
	var id int64                // from EnqueueResult.ID
	err := client.Cancel(ctx, id)
	switch {
	case errors.Is(err, taskmill.ErrNotFound):
		log.Print("the task is gone: deleted, or past its retention")
	case errors.Is(err, taskmill.ErrInvalidState):
		log.Print("the task already finished")
	case err != nil:
		log.Fatal(err)
	}
}

// This example answers a readiness probe from a worker's health. Do not base
// a liveness probe on it: Health fails while the database is out of reach,
// which a restart does not fix.
func ExampleWorker_Health() {
	var worker *taskmill.Worker // from taskmill.NewWorker; its Run runs in another goroutine

	// Health is nil while Run runs and the worker's last check-in is at most
	// one LeaseDuration old. It reads only memory, so call it at every probe.
	if err := worker.Health(); err != nil {
		log.Printf("not ready: %v", err) // answer the probe with 503
	}
}

// This example runs one task now, in the calling goroutine, as a test or an
// operator's tool does. Run does not need to be running.
func ExampleWorker_RunTask() {
	ctx := context.Background()
	var client *taskmill.Client // from taskmill.NewClient
	var handlers taskmill.Handlers
	taskmill.AddHandler(&handlers, taskmill.HandlerFunc[SendReceipt](
		func(ctx context.Context, t *taskmill.Task[SendReceipt]) error {
			return deliverReceipt(ctx, t.Args.Email, t.Args.OrderID)
		},
	), nil)
	worker, err := taskmill.NewWorker(client, &taskmill.WorkerConfig{Handlers: &handlers, NoMaintainer: true})
	if err != nil {
		log.Fatal(err)
	}

	res, err := client.Enqueue(ctx, SendReceipt{OrderID: 7}, &taskmill.EnqueueOptions{RunAt: time.Now().Add(time.Hour)})
	if err != nil {
		log.Fatal(err)
	}
	// RunTask ignores the run time, so the task runs now.
	info, err := worker.RunTask(ctx, res.ID)
	if err != nil {
		log.Fatal(err) // such as ErrInvalidState for a task that is running or has finished
	}
	log.Printf("task %d is %s after attempt %d", info.ID, info.State, info.Attempt)
}

// fixedDelay retries every failed attempt after the same wait.
type fixedDelay time.Duration

// Backoff implements taskmill.RetryPolicy.
func (d fixedDelay) Backoff(int) time.Duration { return time.Duration(d) }

// This example retries the tasks of one kind every 30 seconds, instead of
// with the default exponential backoff.
func ExampleRetryPolicy() {
	var handlers taskmill.Handlers
	taskmill.AddHandler(&handlers, taskmill.HandlerFunc[SendReceipt](
		func(ctx context.Context, t *taskmill.Task[SendReceipt]) error {
			return deliverReceipt(ctx, t.Args.Email, t.Args.OrderID)
		},
	), &taskmill.HandlerOptions{Retry: fixedDelay(30 * time.Second)})
}
