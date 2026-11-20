package orders_test

import (
	"log/slog"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/code19m/taskmill"
	"github.com/code19m/taskmill/examples/modules/internal/billing"
	"github.com/code19m/taskmill/examples/modules/internal/billing/billingtasks"
	"github.com/code19m/taskmill/examples/modules/internal/orders"
	"github.com/code19m/taskmill/internal/testdb"
	"github.com/code19m/taskmill/taskmilltest"
)

// TestPlaceOrder checks that an order asks billing for its invoice, and that
// the work of both modules gets done.
func TestPlaceOrder(t *testing.T) {
	t.Parallel()
	pool, client := testApp(t)
	logger := slog.New(slog.DiscardHandler)
	ordersModule := orders.New(pool, client, logger)
	if err := ordersModule.CreateTables(t.Context()); err != nil {
		t.Fatal(err)
	}

	orderID, err := ordersModule.PlaceOrder(t.Context(), "ada@example.com")
	if err != nil {
		t.Fatal(err)
	}
	invoices := taskmilltest.Enqueued[billingtasks.CreateInvoice](t, client) // billing's type
	if len(invoices) != 1 || invoices[0].Args.OrderID != orderID || invoices[0].Queue != "billing" {
		t.Fatalf("got %+v, want one invoice for order %d in queue billing", invoices, orderID)
	}

	// Work runs until every task is done, so it needs every module that the order reaches.
	var handlers taskmill.Handlers
	ordersModule.Register(&handlers)
	billing.New(logger).Register(&handlers)
	taskmilltest.Work(t, client, &taskmill.WorkerConfig{Handlers: &handlers})

	invoices = taskmilltest.Enqueued[billingtasks.CreateInvoice](t, client)
	if invoices[0].State != taskmill.StateCompleted {
		t.Fatalf("the invoice task is %s, want completed", invoices[0].State)
	}
}

// testApp returns a pool and a client in a schema of the test's own. The
// module's SQL names no schema, so the pool's search path puts the orders
// table there too.
func testApp(t *testing.T) (*pgxpool.Pool, *taskmill.Client) {
	t.Helper()
	schema := testdb.Schema(t, testdb.Pool(t))
	// A pooler in transaction mode refuses a search path: connect without it.
	cfg, err := pgxpool.ParseConfig(testdb.DirectURL(t))
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if err := taskmill.Migrate(t.Context(), pool, &taskmill.MigrateConfig{Schema: schema}); err != nil {
		t.Fatal(err)
	}
	client, err := taskmill.NewClient(t.Context(), pool, &taskmill.ClientConfig{Schema: schema})
	if err != nil {
		t.Fatal(err)
	}
	return pool, client
}
