// Command enqueue-over-http is the program of the recipe "Work with other
// languages", docs/recipes/work-with-other-languages.md: a Go service whose
// tasks programs in other languages enqueue over HTTP.
//
//	docker run --name taskmill-pg -e POSTGRES_PASSWORD=postgres -p 5432:5432 -d postgres:18
//	go run ./examples/enqueue-over-http
//	curl -i localhost:8080/tasks/email.send_receipt -H 'Idempotency-Key: order-42' -d '{"order_id": 42}'
//
// It reads the database from DATABASE_URL, by default the one that the docker
// command above starts, and the address to listen on from ADDR, by default
// localhost:8080. Ctrl-C stops it.
package main

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/code19m/taskmill"
)

// defaultDatabaseURL is the database that the docker command above starts.
const defaultDatabaseURL = "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable" //nolint:gosec // the example's local database

// Log attribute keys.
const (
	logKeyAddr    = "addr"
	logKeyError   = "error"
	logKeyOrderID = "order_id"
)

// SendReceipt is a task that programs in other languages may enqueue.
type SendReceipt struct {
	OrderID int64 `json:"order_id"`
}

// Kind names the task.
func (SendReceipt) Kind() string { return "email.send_receipt" }

// enqueuers maps each kind that other programs may enqueue to the function
// that decodes it into its Go type and enqueues it. Other kinds are refused.
var enqueuers = map[string]enqueuer{
	SendReceipt{}.Kind(): enqueueJSON[SendReceipt],
}

// enqueuer decodes a task from JSON, and enqueues it.
type enqueuer func(ctx context.Context, client *taskmill.Client, body io.Reader, opts *taskmill.EnqueueOptions) (taskmill.EnqueueResult, error)

// errBadTask is the error of a body that is not a task of its kind.
var errBadTask = errors.New("the body is not a task of this kind")

// enqueueJSON is the enqueuer of the tasks of type A. It refuses unknown
// fields, so a producer learns of a mistake right away.
func enqueueJSON[A taskmill.Args](ctx context.Context, client *taskmill.Client, body io.Reader, opts *taskmill.EnqueueOptions) (taskmill.EnqueueResult, error) {
	var args A
	dec := json.NewDecoder(body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&args); err != nil {
		return taskmill.EnqueueResult{}, fmt.Errorf("%w: %w", errBadTask, err)
	}
	return client.Enqueue(ctx, args, opts)
}

// enqueueHandler serves POST /tasks/{kind}. The body is the task as JSON, and
// the Idempotency-Key header, if any, is its dedupe key: a request sent again
// makes no second task.
func enqueueHandler(client *taskmill.Client, logger *slog.Logger) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		enqueue, ok := enqueuers[r.PathValue("kind")]
		if !ok {
			http.Error(w, "unknown kind", http.StatusNotFound)
			return
		}
		body := http.MaxBytesReader(w, r.Body, 1<<20)
		opts := &taskmill.EnqueueOptions{DedupeKey: r.Header.Get("Idempotency-Key")}
		res, err := enqueue(r.Context(), client, body, opts)
		switch {
		case errors.Is(err, errBadTask) || errors.Is(err, taskmill.ErrInvalidArgument):
			http.Error(w, err.Error(), http.StatusBadRequest)
		case err != nil: // the database is slow or gone: the producer sends the request again
			logger.LogAttrs(r.Context(), slog.LevelError, "enqueue failed", slog.Any(logKeyError, err))
			http.Error(w, "enqueue failed, try again", http.StatusServiceUnavailable)
		default:
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusAccepted)
			_, _ = fmt.Fprintf(w, "{\"id\":%d,\"duplicate\":%t}\n", res.ID, res.Duplicate)
		}
	})
}

// routes returns the service's HTTP routes.
func routes(client *taskmill.Client, logger *slog.Logger) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("POST /tasks/{kind}", enqueueHandler(client, logger)) // behind your service's own authentication
	return mux
}

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	if err := run(logger); err != nil {
		logger.LogAttrs(context.Background(), slog.LevelError, "the service stopped", slog.Any(logKeyError, err))
		os.Exit(1)
	}
}

// run serves the routes and runs the worker until Ctrl-C.
func run(logger *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := pgxpool.New(ctx, cmp.Or(os.Getenv("DATABASE_URL"), defaultDatabaseURL))
	if err != nil {
		return err
	}
	defer pool.Close()
	if err := taskmill.Migrate(ctx, pool, &taskmill.MigrateConfig{Logger: logger}); err != nil {
		return err
	}
	client, err := taskmill.NewClient(ctx, pool, &taskmill.ClientConfig{Logger: logger})
	if err != nil {
		return err
	}
	defer client.Close()

	var handlers taskmill.Handlers
	taskmill.AddHandler(&handlers, taskmill.HandlerFunc[SendReceipt](
		func(ctx context.Context, t *taskmill.Task[SendReceipt]) error {
			logger.LogAttrs(ctx, slog.LevelInfo, "receipt sent", slog.Int64(logKeyOrderID, t.Args.OrderID))
			return nil
		},
	), nil)
	worker, err := taskmill.NewWorker(client, &taskmill.WorkerConfig{Handlers: &handlers})
	if err != nil {
		return err
	}

	addr := cmp.Or(os.Getenv("ADDR"), "localhost:8080")
	ln, err := new(net.ListenConfig).Listen(ctx, "tcp", addr)
	if err != nil {
		return err
	}
	server := &http.Server{Handler: routes(client, logger), ReadHeaderTimeout: 10 * time.Second}
	go func() { _ = server.Serve(ln) }() // it returns when Shutdown is called
	defer func() {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cancel()
		_ = server.Shutdown(ctx)
	}()
	logger.LogAttrs(ctx, slog.LevelInfo, "listening", slog.String(logKeyAddr, addr))
	return worker.Run(ctx)
}
