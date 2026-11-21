// Command billing is the billing service of the recipe "Split work across
// services", docs/recipes/split-across-services.md. It migrates its own
// taskmill schema, billing, and runs the invoices that the orders service
// enqueues there.
//
//	docker run --name taskmill-pg -e POSTGRES_PASSWORD=postgres -p 5432:5432 -d postgres:18
//	go run ./examples/services/billing
//
// It reads the database from DATABASE_URL, by default the one that the docker
// command above starts, and runs until Ctrl-C.
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
	"github.com/code19m/taskmill/examples/services/internal/billingtasks"
)

// defaultDatabaseURL is the database that the docker command above starts.
const defaultDatabaseURL = "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable" //nolint:gosec // the example's local database

// Log attribute keys.
const (
	logKeyError   = "error"
	logKeyOrderID = "order_id"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	if err := run(logger); err != nil {
		logger.LogAttrs(context.Background(), slog.LevelError, "billing stopped", slog.Any(logKeyError, err))
		os.Exit(1)
	}
}

// run runs billing's worker until Ctrl-C.
func run(logger *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := pgxpool.New(ctx, cmp.Or(os.Getenv("DATABASE_URL"), defaultDatabaseURL))
	if err != nil {
		return err
	}
	defer pool.Close()
	if err := taskmill.Migrate(ctx, pool, &taskmill.MigrateConfig{Schema: "billing", Logger: logger}); err != nil {
		return err
	}
	client, err := taskmill.NewClient(ctx, pool, &taskmill.ClientConfig{Schema: "billing", Logger: logger})
	if err != nil {
		return err
	}
	defer client.Close()

	var handlers taskmill.Handlers
	taskmill.AddHandler(&handlers, taskmill.HandlerFunc[billingtasks.CreateInvoice](
		func(ctx context.Context, t *taskmill.Task[billingtasks.CreateInvoice]) error {
			logger.LogAttrs(ctx, slog.LevelInfo, "invoice created", slog.Int64(logKeyOrderID, t.Args.OrderID))
			return nil
		},
	), nil)
	worker, err := taskmill.NewWorker(client, &taskmill.WorkerConfig{Handlers: &handlers})
	if err != nil {
		return err
	}
	return worker.Run(ctx)
}
