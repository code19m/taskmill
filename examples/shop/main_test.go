package main

import (
	"bytes"
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

	// The shop's log shows only if the test fails. It goes to a buffer, not
	// to t's log: the client's trailing notification can log after t ends.
	var logs lockedBuffer
	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("the shop's log:\n%s", logs.String())
		}
	})

	ctx, cancel := context.WithCancel(context.Background())
	stopped := make(chan error, 1)
	go func() { stopped <- runShop(ctx, pool, schema, ln, slog.New(slog.NewTextHandler(&logs, nil))) }()
	// Stopping, as on Ctrl-C, must return nil in time. This runs also when
	// the test fails early, and then says why the shop failed.
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-stopped:
			if err != nil {
				t.Errorf("the shop stopped with %v", err)
			}
		case <-time.After(30 * time.Second):
			t.Error("the shop did not stop within 30 seconds")
		}
	})

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
}

// TestShopRestarts opens the shop twice on one schema: the second start keeps
// what the first one stored.
func TestShopRestarts(t *testing.T) {
	t.Parallel()
	pool := testdb.Pool(t)
	schema := testdb.Schema(t, pool)
	logger := slog.New(slog.DiscardHandler)

	s, err := openShop(t.Context(), pool, schema, logger)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.placeOrder(t.Context(), 1); err != nil {
		t.Fatal(err)
	}
	if s, err = openShop(t.Context(), pool, schema, logger); err != nil {
		t.Fatalf("restart: %v", err)
	}
	var orders int
	if err := pool.QueryRow(t.Context(), "SELECT count(*) FROM "+s.orders).Scan(&orders); err != nil || orders != 1 {
		t.Fatalf("after a restart: %d orders, %v; want 1", orders, err)
	}
}

// waitCompleted waits up to 30 seconds for a task of kind to complete.
func waitCompleted(t *testing.T, client *taskmill.Client, kind string) {
	t.Helper()
	params := &taskmill.ListParams{
		Filter: taskmill.TaskFilter{Kinds: []string{kind}, States: []taskmill.State{taskmill.StateCompleted}}, Limit: 1,
	}
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

// lockedBuffer is a buffer that goroutines may write to at once.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}
