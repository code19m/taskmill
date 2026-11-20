package main

import (
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/code19m/taskmill/examples/services/internal/billingtasks"
	"github.com/code19m/taskmill/internal/testdb"
	"github.com/code19m/taskmill/taskmilltest"
)

// TestPlaceOrder checks that an order enqueues its receipt in the orders
// schema, and its invoice in billing's schema.
func TestPlaceOrder(t *testing.T) {
	t.Parallel()
	pool := testPool(t)
	orders := taskmilltest.NewClient(t, pool, nil)  // a schema for the orders service
	billing := taskmilltest.NewClient(t, pool, nil) // and one for billing's
	orderID, err := placeOrder(t.Context(), pool, orders, billing, "ada@example.com")
	if err != nil {
		t.Fatal(err)
	}

	// Enqueued decodes the payload into billing's type, as billing's worker does.
	invoices := taskmilltest.Enqueued[billingtasks.CreateInvoice](t, billing)
	if len(invoices) != 1 || invoices[0].Args.OrderID != orderID {
		t.Fatalf("got %+v, want one invoice for order %d", invoices, orderID)
	}
	if receipts := taskmilltest.Enqueued[SendReceipt](t, orders); len(receipts) != 1 {
		t.Fatalf("got %d receipts, want 1", len(receipts))
	}
}

// testPool returns a pool whose search path is a schema of the test's own,
// with the orders table: the service's SQL names no schema.
func testPool(t *testing.T) *pgxpool.Pool {
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
	if _, err := pool.Exec(t.Context(), "CREATE SCHEMA "+pgx.Identifier{schema}.Sanitize()); err != nil {
		t.Fatal(err)
	}
	if err := createTables(t.Context(), pool); err != nil {
		t.Fatal(err)
	}
	return pool
}
