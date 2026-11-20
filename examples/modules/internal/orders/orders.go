// Package orders is the orders module of the example: it takes orders.
package orders

import (
	"context"
	"log/slog"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/code19m/taskmill"
	"github.com/code19m/taskmill/examples/modules/internal/billing/billingtasks"
	"github.com/code19m/taskmill/examples/modules/internal/orders/ordertasks"
)

// logKeyOrderID is the log attribute key of an order's ID.
const logKeyOrderID = "order_id"

// Module is the orders module, with what it needs.
type Module struct {
	pool   *pgxpool.Pool
	client *taskmill.Client
	logger *slog.Logger
}

// New returns the orders module.
func New(pool *pgxpool.Pool, client *taskmill.Client, logger *slog.Logger) *Module {
	return &Module{pool: pool, client: client, logger: logger}
}

// CreateTables creates the module's table, unless it exists. A real app
// leaves this to its migration tool.
func (m *Module) CreateTables(ctx context.Context) error {
	_, err := m.pool.Exec(ctx, `CREATE TABLE IF NOT EXISTS orders (
		id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, email text NOT NULL)`)
	return err
}

// Register adds the handlers of the module's tasks.
func (m *Module) Register(handlers *taskmill.Handlers) {
	taskmill.AddHandler(handlers, taskmill.HandlerFunc[ordertasks.SendReceipt](m.sendReceipt), nil)
}

// Schedules returns the module's schedules: it has none.
func (*Module) Schedules() []taskmill.Schedule { return nil }

// PlaceOrder saves an order with its tasks: the module's own receipt, and
// billing's invoice.
func (m *Module) PlaceOrder(ctx context.Context, email string) (int64, error) {
	var orderID int64
	err := pgx.BeginFunc(ctx, m.pool, func(tx pgx.Tx) error {
		err := tx.QueryRow(ctx, "INSERT INTO orders (email) VALUES ($1) RETURNING id", email).Scan(&orderID)
		if err != nil {
			return err
		}
		_, err = m.client.EnqueueManyTx(ctx, taskmill.Pgx(tx), []taskmill.EnqueueParams{
			{Args: ordertasks.SendReceipt{OrderID: orderID}},
			{Args: billingtasks.CreateInvoice{OrderID: orderID}}, // billing's type, so billing's queue
		})
		return err
	})
	return orderID, err
}

func (m *Module) sendReceipt(ctx context.Context, t *taskmill.Task[ordertasks.SendReceipt]) error {
	m.logger.LogAttrs(ctx, slog.LevelInfo, "receipt sent", slog.Int64(logKeyOrderID, t.Args.OrderID))
	return nil
}
