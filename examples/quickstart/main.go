// Command quickstart is the program of the quickstart tutorial,
// docs/start/quickstart.md: a tiny shop that emails a receipt for each order,
// as a background task, and shows the tasks in the dashboard.
//
//	docker run --name taskmill-pg -e POSTGRES_PASSWORD=postgres -p 5432:5432 -d postgres:18
//	go run ./examples/quickstart
//
// It reads the database from DATABASE_URL, by default the one that the docker
// command above starts. The dashboard is at http://localhost:8080/taskmill/.
// Ctrl-C stops it.
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
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/code19m/taskmill"
	"github.com/code19m/taskmill/ui"
)

const (
	// defaultDatabaseURL is the database that the docker command starts.
	defaultDatabaseURL = "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable" //nolint:gosec // the tutorial's local database
	addr               = "localhost:8080"
)

// Log attribute keys.
const (
	logKeyError  = "error"
	logKeyTaskID = "task_id"
	logKeyTo     = "to"
	logKeyURL    = "url"
)

// SendReceipt is a task: a Go struct, saved as JSON, with a Kind method.
type SendReceipt struct {
	OrderID int64  `json:"order_id"`
	Email   string `json:"email"`
}

// Kind names the task. It tells taskmill which handler runs it.
func (SendReceipt) Kind() string { return "email.send_receipt" }

// sendReceipt returns the handler: the code that runs one SendReceipt task.
func sendReceipt(logger *slog.Logger) taskmill.HandlerFunc[SendReceipt] {
	return func(ctx context.Context, t *taskmill.Task[SendReceipt]) error {
		if t.Args.Email == "" {
			// A retry cannot help, so give up now: the task becomes dead.
			return taskmill.Permanent(errors.New("the order has no email address"))
		}
		if rand.IntN(3) == 0 { //nolint:gosec // a made-up failure needs no secure randomness
			// Pretend that the mail server failed: taskmill tries again soon.
			return errors.New("mail server did not answer")
		}
		logger.LogAttrs(ctx, slog.LevelInfo, "receipt sent",
			slog.Int64(logKeyTaskID, t.ID), slog.String(logKeyTo, t.Args.Email))
		return nil
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

// run starts the shop, and runs it until Ctrl-C.
func run(logger *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// 15 connections: 10 for the tasks that run at the same time, 5 for the worker.
	cfg, err := pgxpool.ParseConfig(cmp.Or(os.Getenv("DATABASE_URL"), defaultDatabaseURL))
	if err != nil {
		return err
	}
	cfg.MaxConns = 15
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return err
	}
	defer pool.Close()

	// Create taskmill's tables, or update them. It is safe at every start.
	if err := taskmill.Migrate(ctx, pool, nil); err != nil {
		return err
	}
	_, err = pool.Exec(ctx, `CREATE TABLE IF NOT EXISTS orders (
		id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, email text NOT NULL)`)
	if err != nil {
		return err
	}

	// The client adds and manages tasks. Without a logger, taskmill logs nothing.
	client, err := taskmill.NewClient(ctx, pool, &taskmill.ClientConfig{Logger: logger})
	if err != nil {
		return err
	}

	// The worker takes tasks from the database and runs their handlers.
	var handlers taskmill.Handlers
	taskmill.AddHandler(&handlers, sendReceipt(logger), nil)
	worker, err := taskmill.NewWorker(client, &taskmill.WorkerConfig{Handlers: &handlers})
	if err != nil {
		return err
	}

	// The dashboard is an http.Handler. In a real app, put your login in front of it.
	dashboard, err := ui.New(client, &ui.Config{Prefix: "/taskmill"})
	if err != nil {
		return err
	}
	mux := http.NewServeMux()
	mux.Handle("/taskmill/", dashboard)
	mux.Handle("POST /orders", placeOrder(pool, client, logger))
	ln, err := new(net.ListenConfig).Listen(ctx, "tcp", addr)
	if err != nil {
		return err
	}
	server := &http.Server{Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	go func() { _ = server.Serve(ln) }() // it returns when Shutdown is called
	logger.LogAttrs(ctx, slog.LevelInfo, "open the dashboard", slog.String(logKeyURL, "http://"+addr+"/taskmill/"))

	// Run the worker until Ctrl-C. Then it lets running tasks finish.
	err = worker.Run(ctx)
	_ = server.Shutdown(context.WithoutCancel(ctx))
	return err
}

// placeOrder saves an order and adds its receipt task in one transaction:
// both are saved, or neither is.
func placeOrder(pool *pgxpool.Pool, client *taskmill.Client, logger *slog.Logger) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		email := r.FormValue("email")
		var res taskmill.EnqueueResult
		err := pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
			var orderID int64
			err := tx.QueryRow(ctx, "INSERT INTO orders (email) VALUES ($1) RETURNING id", email).Scan(&orderID)
			if err != nil {
				return err
			}
			res, err = client.EnqueueTx(ctx, taskmill.Pgx(tx), SendReceipt{OrderID: orderID, Email: email}, nil)
			return err
		})
		if err != nil {
			logger.LogAttrs(ctx, slog.LevelError, "placing the order failed", slog.Any(logKeyError, err))
			http.Error(w, "placing the order failed", http.StatusInternalServerError)
			return
		}
		_, _ = fmt.Fprintf(w, "task %d: http://%s/taskmill/tasks/%d\n", res.ID, addr, res.ID)
	})
}
