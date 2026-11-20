// Command modules is the program of the recipe "Organize tasks by module",
// docs/recipes/organize-tasks-by-module.md: one binary of two modules, orders
// and billing, which share one taskmill schema. Each module owns its task
// types, its queue, its handlers, and its schedules, and main puts them
// together.
//
//	docker run --name taskmill-pg -e POSTGRES_PASSWORD=postgres -p 5432:5432 -d postgres:18
//	go run ./examples/modules
//
// It reads the database from DATABASE_URL, by default the one that the docker
// command above starts. It places one order, logs the receipt and the
// invoice, and runs until Ctrl-C.
package main

import (
	"cmp"
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/code19m/taskmill"
	"github.com/code19m/taskmill/examples/modules/internal/billing"
	"github.com/code19m/taskmill/examples/modules/internal/orders"
)

// defaultDatabaseURL is the database that the docker command above starts.
const defaultDatabaseURL = "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable" //nolint:gosec // the example's local database

// logKeyError is the log attribute key of an error.
const logKeyError = "error"

// module is what main needs from each module.
type module interface {
	Register(handlers *taskmill.Handlers)
	Schedules() []taskmill.Schedule
}

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	if err := run(logger); err != nil {
		logger.LogAttrs(context.Background(), slog.LevelError, "the app stopped", slog.Any(logKeyError, err))
		os.Exit(1)
	}
}

// run starts the app, and runs it until Ctrl-C.
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

	ordersModule := orders.New(pool, client, logger)
	if err := ordersModule.CreateTables(ctx); err != nil {
		return err
	}
	worker, err := newWorker(client, ordersModule, billing.New(logger))
	if err != nil {
		return err
	}
	if _, err := ordersModule.PlaceOrder(ctx, "ada@example.com"); err != nil {
		return err
	}
	return worker.Run(ctx)
}

// newWorker returns a worker that runs the tasks and the schedules of every
// module.
func newWorker(client *taskmill.Client, modules ...module) (*taskmill.Worker, error) {
	var handlers taskmill.Handlers
	var schedules []taskmill.Schedule
	for _, m := range modules {
		m.Register(&handlers)                           // NewWorker rejects a kind that two modules register
		schedules = append(schedules, m.Schedules()...) // and a schedule name that two modules use
	}
	return taskmill.NewWorker(client, &taskmill.WorkerConfig{Handlers: &handlers, Schedules: schedules})
}
