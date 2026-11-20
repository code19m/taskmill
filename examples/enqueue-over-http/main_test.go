package main

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/code19m/taskmill/internal/testdb"
	"github.com/code19m/taskmill/taskmilltest"
)

// TestEnqueueOverHTTP checks the answers of the endpoint, and the tasks that
// it enqueues.
func TestEnqueueOverHTTP(t *testing.T) {
	t.Parallel()
	client := taskmilltest.NewClient(t, testdb.Pool(t), nil)
	server := httptest.NewServer(routes(client, slog.New(slog.DiscardHandler)))
	t.Cleanup(server.Close)

	tests := []struct {
		name, kind, key, body string
		wantStatus            int
		wantBody              string
	}{
		{"new task", "email.send_receipt", "order-42", `{"order_id": 42}`, http.StatusAccepted, `"duplicate":false`},
		{"same key again", "email.send_receipt", "order-42", `{"order_id": 42}`, http.StatusAccepted, `"duplicate":true`},
		{"unknown kind", "admin.drop_tables", "", `{}`, http.StatusNotFound, "unknown kind"},
		{"unknown field", "email.send_receipt", "", `{"order": 42}`, http.StatusBadRequest, "unknown field"},
		{"wrong type", "email.send_receipt", "", `{"order_id": "42"}`, http.StatusBadRequest, "cannot unmarshal"},
		{"too large", "email.send_receipt", "", `{"order_id": 42, "x": "` + strings.Repeat("x", 1<<20) + `"}`, http.StatusBadRequest, "too large"},
	}
	for _, tt := range tests { // in order: the second request repeats the first
		req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, server.URL+"/tasks/"+tt.kind, strings.NewReader(tt.body))
		if err != nil {
			t.Fatal(err)
		}
		if tt.key != "" {
			req.Header.Set("Idempotency-Key", tt.key)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if err != nil {
			t.Fatal(err)
		}
		if resp.StatusCode != tt.wantStatus || !strings.Contains(string(body), tt.wantBody) {
			t.Errorf("%s: got %d %q, want %d and %q", tt.name, resp.StatusCode, body, tt.wantStatus, tt.wantBody)
		}
	}

	tasks := taskmilltest.Enqueued[SendReceipt](t, client)
	if len(tasks) != 1 || tasks[0].Args.OrderID != 42 {
		t.Fatalf("got %+v, want one task for order 42", tasks)
	}
}
