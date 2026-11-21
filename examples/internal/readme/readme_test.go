package readme

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/code19m/taskmill"
	"github.com/code19m/taskmill/internal/testdb"
)

// mailbox is a Mailer that hands over the order ID of each receipt it sends.
type mailbox chan int64

func (m mailbox) SendReceipt(ctx context.Context, orderID int64) error {
	select {
	case m <- orderID:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// TestQuickLook runs the README's code: once an order's transaction commits,
// the worker sends the order's receipt, and it stops when its context ends.
func TestQuickLook(t *testing.T) {
	t.Parallel()
	pool, client := testShop(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	err := pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		return placeOrder(ctx, tx, client, "ann@example.com")
	})
	if err != nil {
		t.Fatal(err)
	}

	sent := make(mailbox)
	done := make(chan error, 1)
	go func() { done <- runWorker(ctx, client, sent) }()
	select {
	case orderID := <-sent:
		if orderID != 1 {
			t.Errorf("the receipt of order %d was sent, want order 1", orderID)
		}
	case err := <-done:
		t.Fatalf("the worker stopped before it sent the receipt: %v", err)
	case <-time.After(30 * time.Second):
		t.Fatal("no receipt was sent in 30 seconds")
	}

	cancel()
	if err := <-done; err != nil {
		t.Fatalf("the worker returned %v after its context ended, want nil", err)
	}
}

// testShop returns a pool and a client in a schema of the test's own. The
// README's SQL names no schema, so the pool's search path puts the orders
// table there too.
func testShop(t *testing.T) (*pgxpool.Pool, *taskmill.Client) {
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
	_, err = pool.Exec(t.Context(), `CREATE TABLE orders (
		id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, email text NOT NULL)`)
	if err != nil {
		t.Fatal(err)
	}
	client, err := taskmill.NewClient(t.Context(), pool, &taskmill.ClientConfig{Schema: schema})
	if err != nil {
		t.Fatal(err)
	}
	return pool, client
}
