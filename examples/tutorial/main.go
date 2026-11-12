// Command tutorial is the program that the reader of the tutorial,
// docs/tutorial, has built after part 7: the shop of the quickstart, grown
// into one binary with two roles. The web role takes orders and shows the
// dashboard. The worker role sends receipts and reminders, builds a report
// every minute, and publishes order events through the outbox.
//
//	docker run --name taskmill-pg -e POSTGRES_PASSWORD=postgres -p 5432:5432 -d postgres:18
//	go run ./examples/tutorial worker
//	go run ./examples/tutorial web
//
// Both roles read the database from DATABASE_URL, by default the one that the
// docker command above starts. The web role listens on ADDR, by default
// localhost:8080, with the dashboard at http://localhost:8080/taskmill/. The
// worker role answers its readiness probe on HEALTH_ADDR, by default
// localhost:8081. Ctrl-C stops a role.
package main

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/code19m/taskmill"
	"github.com/code19m/taskmill/outbox"
	"github.com/code19m/taskmill/ui"
)

// defaultDatabaseURL is the database that the quickstart's docker command starts.
const defaultDatabaseURL = "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable" //nolint:gosec // the tutorial's local database

// Log attribute keys.
const (
	logKeyError        = "error"
	logKeyEvent        = "event"
	logKeyOrder        = "order"
	logKeyScheduledFor = "scheduled_for"
	logKeyTaskID       = "task_id"
	logKeyTo           = "to"
	logKeyURL          = "url"
)

// defaultAddr is where the shop listens.
const defaultAddr = "localhost:8080"

// SendReceipt is a task: a Go struct, saved as JSON, with a Kind method.
type SendReceipt struct {
	OrderID int64  `json:"order_id"`
	Email   string `json:"email"`
}

// Kind names the task. It tells taskmill which handler runs it.
func (SendReceipt) Kind() string { return "email.send_receipt" }

// EnqueueDefaults puts receipts in a queue of their own, and gives up on one
// after 5 attempts.
func (SendReceipt) EnqueueDefaults() taskmill.EnqueueOptions {
	return taskmill.EnqueueOptions{Queue: "emails", MaxAttempts: 5}
}

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
	if rand.IntN(3) == 0 { //nolint:gosec // a made-up failure needs no secure randomness
		return errors.New("mail server did not answer")
	}
	return nil
}

// RemindCustomer asks a customer how the order went.
type RemindCustomer struct {
	OrderID int64  `json:"order_id"`
	Email   string `json:"email"`
}

// Kind names the task.
func (RemindCustomer) Kind() string { return "email.remind_customer" }

// remindCustomer returns the handler of RemindCustomer tasks.
func remindCustomer(logger *slog.Logger) taskmill.HandlerFunc[RemindCustomer] {
	return func(ctx context.Context, t *taskmill.Task[RemindCustomer]) error {
		logger.LogAttrs(ctx, slog.LevelInfo, "reminder sent",
			slog.Int64(logKeyTaskID, t.ID), slog.String(logKeyTo, t.Args.Email))
		return nil
	}
}

// BuildReport builds the sales report. A schedule enqueues it every minute.
type BuildReport struct{}

// Kind names the task.
func (BuildReport) Kind() string { return "reports.build" }

// buildReport returns the handler of BuildReport tasks.
func buildReport(logger *slog.Logger) taskmill.HandlerFunc[BuildReport] {
	return func(ctx context.Context, t *taskmill.Task[BuildReport]) error {
		// Pretend that the report takes 5 seconds. Stop early if the context ends.
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(5 * time.Second):
		}
		// RunAt is the minute that the schedule enqueued this run for.
		logger.LogAttrs(ctx, slog.LevelInfo, "sales report built", slog.Time(logKeyScheduledFor, t.RunAt))
		return nil
	}
}

// publish stands for your broker client, such as Kafka's: it writes each
// message to the log. One time in four it fails, as a broker sometimes does.
func publish(logger *slog.Logger) outbox.PublisherFunc {
	return func(ctx context.Context, ds []outbox.Delivery) []error {
		errs := make([]error, len(ds)) // one result per delivery, in the same order
		for i, d := range ds {
			if rand.IntN(4) == 0 { //nolint:gosec // a made-up failure needs no secure randomness
				errs[i] = errors.New("broker did not answer") // the relay tries this message again later
				continue
			}
			logger.LogAttrs(ctx, slog.LevelInfo, "event published",
				slog.String(logKeyOrder, string(d.Key)), slog.String(logKeyEvent, string(d.Value)))
		}
		return errs
	}
}

func main() {
	// Debug level shows taskmill's line for each retry.
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
	if err := run(logger); err != nil {
		logger.LogAttrs(context.Background(), slog.LevelError, "shop stopped", slog.Any(logKeyError, err))
		os.Exit(1)
	}
}

// run runs the role that the first argument names, until Ctrl-C or SIGTERM.
func run(logger *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	role := ""
	if len(os.Args) == 2 {
		role = os.Args[1]
	}
	switch role {
	case "web":
		return runWeb(ctx, logger)
	case "worker":
		return runWorker(ctx, logger)
	default:
		return errors.New("usage: shop web|worker")
	}
}

// connect opens a pool of conns connections to the database.
func connect(ctx context.Context, conns int32) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(cmp.Or(os.Getenv("DATABASE_URL"), defaultDatabaseURL))
	if err != nil {
		return nil, err
	}
	cfg.MaxConns = conns
	return pgxpool.NewWithConfig(ctx, cfg)
}

// setUp creates taskmill's tables and the shop's, and returns the client.
// Every role calls it when it starts.
func setUp(ctx context.Context, pool *pgxpool.Pool, logger *slog.Logger) (*taskmill.Client, error) {
	// Create taskmill's tables, or update them. It is safe at every start.
	if err := taskmill.Migrate(ctx, pool, &taskmill.MigrateConfig{Logger: logger}); err != nil {
		return nil, err
	}
	if err := createTables(ctx, pool); err != nil {
		return nil, err
	}
	// The client adds and manages tasks. Without a logger, taskmill logs nothing.
	return taskmill.NewClient(ctx, pool, &taskmill.ClientConfig{Logger: logger})
}

// serve starts an HTTP server on addr. The function that it returns stops
// the server: it waits up to 10 seconds for open requests to finish.
func serve(ctx context.Context, addr string, handler http.Handler) (stop func(), err error) {
	ln, err := new(net.ListenConfig).Listen(ctx, "tcp", addr)
	if err != nil {
		return nil, err
	}
	server := &http.Server{Handler: handler, ReadHeaderTimeout: 10 * time.Second}
	go func() { _ = server.Serve(ln) }() // it returns when Shutdown is called
	return func() {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cancel()
		_ = server.Shutdown(ctx)
	}, nil
}

// runWeb takes orders and shows the dashboard. It runs no tasks: it only
// enqueues them.
func runWeb(ctx context.Context, logger *slog.Logger) error {
	pool, err := connect(ctx, 10) // for the web traffic: this role runs no tasks
	if err != nil {
		return err
	}
	defer pool.Close()
	client, err := setUp(ctx, pool, logger)
	if err != nil {
		return err
	}
	handler, err := routes(pool, client, logger)
	if err != nil {
		return err
	}

	// Serve until Ctrl-C or SIGTERM. Set ADDR if port 8080 is taken.
	addr := cmp.Or(os.Getenv("ADDR"), defaultAddr)
	stop, err := serve(ctx, addr, handler)
	if err != nil {
		return err
	}
	logger.LogAttrs(ctx, slog.LevelInfo, "open the dashboard", slog.String(logKeyURL, "http://"+addr+"/taskmill/"))
	<-ctx.Done()
	stop()
	return nil
}

// routes returns the web role's HTTP handler: the orders and the dashboard.
func routes(pool *pgxpool.Pool, client *taskmill.Client, logger *slog.Logger) (http.Handler, error) {
	// The outbox stores messages for your broker in your own transaction.
	ob := outbox.New(client)

	// The shop's own server takes the orders.
	mux := http.NewServeMux()
	mux.Handle("POST /orders", placeOrder(pool, client, ob, logger))

	// The dashboard is an http.Handler. In a real app, put your login in front of it.
	dashboard, err := ui.New(client, &ui.Config{Prefix: "/taskmill"})
	if err != nil {
		return nil, err
	}
	mux.Handle("/taskmill/", dashboard)
	return mux, nil
}

// runWorker runs the tasks, the schedule, and the outbox's relay.
func runWorker(ctx context.Context, logger *slog.Logger) error {
	pool, err := connect(ctx, 17) // 12 for the tasks that run at the same time, 5 for the worker
	if err != nil {
		return err
	}
	defer pool.Close()
	client, err := setUp(ctx, pool, logger)
	if err != nil {
		return err
	}
	worker, err := newWorker(client, logger)
	if err != nil {
		return err
	}

	// A platform such as Kubernetes asks this server if the worker is ready.
	// Set HEALTH_ADDR if port 8081 is taken.
	stop, err := serve(ctx, cmp.Or(os.Getenv("HEALTH_ADDR"), "localhost:8081"), health(worker))
	if err != nil {
		return err
	}
	defer stop()

	// Run the worker until Ctrl-C or SIGTERM. Then it lets running tasks finish.
	return worker.Run(ctx)
}

// newWorker returns the shop's worker, with its handlers, queues, and
// schedule.
func newWorker(client *taskmill.Client, logger *slog.Logger) (*taskmill.Worker, error) {
	// The worker takes tasks from the database and runs their handlers.
	var handlers taskmill.Handlers
	taskmill.AddHandler(&handlers, sendReceipt(sendMail, logger), nil)
	taskmill.AddHandler(&handlers, remindCustomer(logger), nil)
	taskmill.AddHandler(&handlers, buildReport(logger), nil)
	ob := outbox.New(client)
	ob.AddRelay(&handlers, publish(logger), nil)
	return taskmill.NewWorker(client, &taskmill.WorkerConfig{
		Handlers: &handlers,
		Queues: map[string]taskmill.QueueOptions{
			"default":    {Concurrency: 5},
			"emails":     {Concurrency: 5},
			outbox.Queue: {Concurrency: 2},
		},
		Schedules: []taskmill.Schedule{
			{Name: "sales-report", Cron: "* * * * *", Args: BuildReport{}},
		},
	})
}

// health answers a readiness probe: 200 while the worker runs and reaches
// the database, and 503 otherwise.
func health(worker *taskmill.Worker) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if err := worker.Health(); err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
		}
	})
}

// createTables creates the shop's own table, unless it exists. A real app
// leaves this to its migration tool.
func createTables(ctx context.Context, pool *pgxpool.Pool) error {
	_, err := pool.Exec(ctx, `CREATE TABLE IF NOT EXISTS orders (
		id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, email text NOT NULL)`)
	return err
}

// placeOrder saves an order and adds its receipt task in one transaction:
// both are saved, or neither is.
func placeOrder(pool *pgxpool.Pool, client *taskmill.Client, ob *outbox.Outbox, logger *slog.Logger) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		email := r.FormValue("email")
		var orderID int64
		var res, reminder taskmill.EnqueueResult
		err := pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
			err := tx.QueryRow(ctx, "INSERT INTO orders (email) VALUES ($1) RETURNING id", email).Scan(&orderID)
			if err != nil {
				return err
			}
			res, err = client.EnqueueTx(ctx, taskmill.Pgx(tx), SendReceipt{OrderID: orderID, Email: email}, nil)
			if err != nil {
				return err
			}
			// Ask the customer a minute later how it went: once per address in an hour.
			reminder, err = client.EnqueueTx(ctx, taskmill.Pgx(tx), RemindCustomer{OrderID: orderID, Email: email}, &taskmill.EnqueueOptions{
				RunAt:        time.Now().Add(time.Minute),
				DedupeKey:    "remind-" + email,
				DedupeWindow: time.Hour, // counts from now, so it must be longer than the delay
			})
			if err != nil {
				return err
			}
			// Store the order's events. Messages with the same key stay in order.
			key := []byte(strconv.FormatInt(orderID, 10))
			return ob.Store(ctx, taskmill.Pgx(tx),
				outbox.Message{Topic: "orders", Key: key, Value: []byte("order.placed")},
				outbox.Message{Topic: "orders", Key: key, Value: []byte("order.paid")},
			)
		})
		if err != nil {
			logger.LogAttrs(ctx, slog.LevelError, "placing the order failed", slog.Any(logKeyError, err))
			http.Error(w, "placing the order failed", http.StatusInternalServerError)
			return
		}
		if reminder.Duplicate {
			logger.LogAttrs(ctx, slog.LevelInfo, "the customer already has a reminder", slog.Int64(logKeyTaskID, reminder.ID))
		}
		_, _ = fmt.Fprintf(w, "order %d placed, task %d enqueued\n", orderID, res.ID)
	})
}
