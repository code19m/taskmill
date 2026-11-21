// Command orders is the orders service of the recipe "Split work across
// services", docs/recipes/split-across-services.md. It has its own taskmill
// schema, orders, and enqueues billing's tasks into billing's schema, in the
// transaction that saves the order.
//
//	go run ./examples/services/billing   # first: it migrates billing's schema
//	go run ./examples/services/orders
//
// It reads the database from DATABASE_URL, by default the one that the docker
// command of the billing service starts. It places one order, and runs until
// Ctrl-C.
package main

import (
	"cmp"
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/code19m/taskmill"
	"github.com/code19m/taskmill/examples/services/internal/billingtasks"
)

// defaultDatabaseURL is the database that the billing service's docker command starts.
const defaultDatabaseURL = "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable" //nolint:gosec // the example's local database

// Log attribute keys.
const (
	logKeyError   = "error"
	logKeyOrderID = "order_id"
)

// SendReceipt is the orders service's own task.
type SendReceipt struct {
	OrderID int64 `json:"order_id"`
}

// Kind names the task.
func (SendReceipt) Kind() string { return "orders.send_receipt" }

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	if err := run(logger); err != nil {
		logger.LogAttrs(context.Background(), slog.LevelError, "orders stopped", slog.Any(logKeyError, err))
		os.Exit(1)
	}
}

// run places one order, and runs the orders service's worker until Ctrl-C.
func run(logger *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := pgxpool.New(ctx, cmp.Or(os.Getenv("DATABASE_URL"), defaultDatabaseURL))
	if err != nil {
		return err
	}
	defer pool.Close()
	if err := createTables(ctx, pool); err != nil {
		return err
	}

	// In the orders service: its own schema, and a client for billing's schema.
	if err := taskmill.Migrate(ctx, pool, &taskmill.MigrateConfig{Schema: "orders"}); err != nil {
		return err
	}
	orders, err := taskmill.NewClient(ctx, pool, &taskmill.ClientConfig{Schema: "orders", Logger: logger})
	if err != nil {
		return err
	}
	defer orders.Close()
	billing, err := taskmill.NewClient(ctx, pool, &taskmill.ClientConfig{Schema: "billing"}) // only to enqueue
	if err != nil {
		return err // ErrSchemaOutdated until billing has migrated its schema
	}
	defer billing.Close()

	if _, err := placeOrder(ctx, pool, orders, billing, "ada@example.com"); err != nil {
		return err
	}

	var handlers taskmill.Handlers
	taskmill.AddHandler(&handlers, taskmill.HandlerFunc[SendReceipt](
		func(ctx context.Context, t *taskmill.Task[SendReceipt]) error {
			logger.LogAttrs(ctx, slog.LevelInfo, "receipt sent", slog.Int64(logKeyOrderID, t.Args.OrderID))
			return nil
		},
	), nil)
	worker, err := taskmill.NewWorker(orders, &taskmill.WorkerConfig{Handlers: &handlers}) // never on billing's client
	if err != nil {
		return err
	}
	return worker.Run(ctx)
}

// createTables creates the service's own table, unless it exists. A real app
// leaves this to its migration tool.
func createTables(ctx context.Context, pool *pgxpool.Pool) error {
	_, err := pool.Exec(ctx, `CREATE TABLE IF NOT EXISTS orders (
		id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, email text NOT NULL)`)
	return err
}

// placeOrder saves an order with its receipt task in the orders schema, and
// its invoice task in billing's schema, in one transaction.
func placeOrder(ctx context.Context, pool *pgxpool.Pool, orders, billing *taskmill.Client, email string) (int64, error) {
	var orderID int64
	err := pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		err := tx.QueryRow(ctx, "INSERT INTO orders (email) VALUES ($1) RETURNING id", email).Scan(&orderID)
		if err != nil {
			return err
		}
		if _, err := orders.EnqueueTx(ctx, taskmill.Pgx(tx), SendReceipt{OrderID: orderID}, nil); err != nil {
			return err
		}
		_, err = billing.EnqueueTx(ctx, taskmill.Pgx(tx), billingtasks.CreateInvoice{OrderID: orderID}, nil)
		return err // billing's task commits with the order
	})
	return orderID, err
}
