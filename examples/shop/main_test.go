package main

import (
	"context"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/code19m/taskmill"
	"github.com/code19m/taskmill/internal/testdb"
)

// TestShop runs the shop briefly: its tasks complete, its dashboard answers,
// and it stops cleanly when its context ends, as on Ctrl-C.
func TestShop(t *testing.T) {
	t.Parallel()
	pool := testdb.PoolSize(t, 80)
	schema := testdb.Schema(t, pool)
	ln, err := new(net.ListenConfig).Listen(t.Context(), "tcp", "localhost:0")
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	stopped := make(chan error, 1)
	logger := slog.New(slog.NewTextHandler(t.Output(), &slog.HandlerOptions{Level: slog.LevelWarn}))
	go func() { stopped <- runShop(ctx, pool, schema, ln, logger) }()
	stop := sync.OnceValue(func() error {
		cancel()
		select {
		case err := <-stopped:
			return err
		case <-time.After(30 * time.Second):
			t.Fatal("the shop did not stop within 30 seconds")
			return nil
		}
	})
	t.Cleanup(func() { _ = stop() })

	// The root redirects to the dashboard. The listener queues the request
	// until the shop has set up its schema and serves.
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://"+ln.Addr().String()+"/", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK || resp.Request.URL.Path != "/taskmill/" {
		t.Fatalf("GET /: got %d from %s, want 200 from /taskmill/", resp.StatusCode, resp.Request.URL.Path)
	}

	// Orders come in every second; their receipts and ledger entries
	// complete.
	client, err := taskmill.NewClient(t.Context(), pool, &taskmill.ClientConfig{Schema: schema})
	if err != nil {
		t.Fatal(err)
	}
	waitCompleted(t, client, SendReceipt{}.Kind())
	waitCompleted(t, client, PostLedgerEntry{}.Kind())

	if err := stop(); err != nil {
		t.Fatalf("the shop stopped with %v", err)
	}
}

// waitCompleted waits up to 30 seconds for a task of kind to complete.
func waitCompleted(t *testing.T, client *taskmill.Client, kind string) {
	t.Helper()
	params := &taskmill.ListParams{Kinds: []string{kind}, States: []taskmill.State{taskmill.StateCompleted}, Limit: 1}
	for deadline := time.Now().Add(30 * time.Second); ; time.Sleep(50 * time.Millisecond) {
		page, err := client.List(t.Context(), params)
		if err != nil {
			t.Fatal(err)
		}
		if len(page.Tasks) > 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("no %s task completed within 30 seconds", kind)
		}
	}
}
