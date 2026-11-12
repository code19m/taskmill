package main

// This file is not part of the tutorial. It lets the tutorial's tests, in
// main_test.go, run on the repository's test database, and it runs the whole
// shop once.

import (
	"bytes"
	"cmp"
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/code19m/taskmill"
	"github.com/code19m/taskmill/internal/testdb"
	"github.com/code19m/taskmill/outbox"
	"github.com/code19m/taskmill/taskmilltest"
)

// TestMain points the tests at the repository's test database. They read
// TEST_DATABASE_URL, as the tutorial's reader sets it. The shop's SQL names
// no schema, so every connection gets a search path with a schema of this
// run's own, which TestMain drops at the end: runs share no table.
func TestMain(m *testing.M) {
	code, err := runTests(m)
	if err != nil {
		fmt.Fprintln(os.Stderr, "examples/tutorial:", err)
		code = 1
	}
	os.Exit(code)
}

func runTests(m *testing.M) (int, error) {
	// A pooler in transaction mode refuses a search path: connect without it.
	dsn := cmp.Or(os.Getenv(testdb.EnvListenURL), os.Getenv(testdb.EnvURL))
	if dsn == "" {
		return m.Run(), nil // the tests skip
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return 0, err
	}
	defer func() { _ = conn.Close(context.WithoutCancel(ctx)) }()

	var b [6]byte
	_, _ = rand.Read(b[:]) // never returns an error
	schema := "tutorial_" + hex.EncodeToString(b[:])
	if _, err := conn.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		return 0, err
	}
	if err := os.Setenv("TEST_DATABASE_URL", dsn); err != nil {
		return 0, err
	}
	if err := os.Setenv("PGOPTIONS", "-c search_path="+schema); err != nil { // pgx reads it, as libpq does
		return 0, err
	}
	code := m.Run()

	// The tests ran longer than ctx may allow.
	drop, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, err = conn.Exec(drop, "DROP SCHEMA "+schema+" CASCADE")
	return code, err
}

// TestShop runs the shop's two roles on one schema: orders placed over HTTP
// get their receipts and their events, the dashboard answers, the worker
// reports that it is ready, and it stops cleanly when its context ends, as on
// Ctrl-C.
func TestShop(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set TEST_DATABASE_URL to run this test")
	}
	t.Setenv("DATABASE_URL", dsn)         // connect reads it
	pool, err := connect(t.Context(), 17) // the worker's pool, as runWorker sizes it
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if err := createTables(t.Context(), pool); err != nil {
		t.Fatal(err)
	}
	client := taskmilltest.NewClient(t, pool, nil)

	// The shop's log shows only if the test fails. It goes to a buffer, not
	// to t's log: the client's trailing notification can log after t ends.
	var logs lockedBuffer
	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("the shop's log:\n%s", logs.String())
		}
	})
	logger := slog.New(slog.NewTextHandler(&logs, nil))

	// The worker role. Stopping it, as on Ctrl-C, must return nil in time.
	worker, err := newWorker(client, logger)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	stopped := make(chan error, 1)
	go func() { stopped <- worker.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-stopped:
			if err != nil {
				t.Errorf("the worker stopped with %v", err)
			}
		case <-time.After(30 * time.Second):
			t.Error("the worker did not stop within 30 seconds")
		}
	})

	// The web role: its dashboard answers, and it takes orders.
	handler, err := routes(pool, client, logger)
	if err != nil {
		t.Fatal(err)
	}
	web := httptest.NewServer(handler)
	t.Cleanup(web.Close)
	if status := request(t, http.MethodGet, web.URL+"/taskmill/", nil); status != http.StatusOK {
		t.Fatalf("GET /taskmill/: got %d, want 200", status)
	}
	// Three orders: the mail server fails one attempt in three, and a receipt
	// is dead after five, so one receipt alone could fail the test by chance.
	for i := range 3 {
		form := url.Values{"email": {fmt.Sprintf("customer%d@example.com", i)}}
		if status := request(t, http.MethodPost, web.URL+"/orders", form); status != http.StatusOK {
			t.Fatalf("POST /orders: got %d, want 200", status)
		}
	}

	// A receipt completes, and the relay publishes every event: a published
	// message leaves the outbox's queue.
	waitFor(t, client, "a receipt to complete", &taskmill.TaskFilter{
		Kinds: []string{SendReceipt{}.Kind()}, States: []taskmill.State{taskmill.StateCompleted},
	}, func(n int) bool { return n > 0 })
	waitFor(t, client, "the outbox to empty", &taskmill.TaskFilter{
		Queues: []string{outbox.Queue},
	}, func(n int) bool { return n == 0 })

	// The worker has checked in by now, so it is ready.
	probe := httptest.NewServer(health(worker))
	t.Cleanup(probe.Close)
	if status := request(t, http.MethodGet, probe.URL+"/healthz", nil); status != http.StatusOK {
		t.Fatalf("GET /healthz: got %d, want 200", status)
	}
}

// request sends a request, with form as its body if it is not nil, and
// returns the status of the answer.
func request(t *testing.T, method, target string, form url.Values) int {
	t.Helper()
	var body io.Reader
	if form != nil {
		body = bytes.NewBufferString(form.Encode())
	}
	req, err := http.NewRequestWithContext(t.Context(), method, target, body)
	if err != nil {
		t.Fatal(err)
	}
	if form != nil {
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	return resp.StatusCode
}

// waitFor waits up to 30 seconds until done accepts the number of tasks that
// match filter, counted up to 100.
func waitFor(t *testing.T, client *taskmill.Client, what string, filter *taskmill.TaskFilter, done func(n int) bool) {
	t.Helper()
	params := &taskmill.ListParams{Filter: *filter, Limit: 100}
	for deadline := time.Now().Add(30 * time.Second); ; time.Sleep(50 * time.Millisecond) {
		page, err := client.List(t.Context(), params)
		if err != nil {
			t.Fatal(err)
		}
		if done(len(page.Tasks)) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("waited 30 seconds for %s: %d tasks match", what, len(page.Tasks))
		}
	}
}

// lockedBuffer is a buffer that goroutines may write to at the same time.
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
