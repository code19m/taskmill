package ui_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"

	"github.com/code19m/taskmill"
	"github.com/code19m/taskmill/internal/testdb"
	"github.com/code19m/taskmill/ui"
)

type greet struct {
	Name string `json:"name"`
}

func (greet) Kind() string { return "greet" }

func setup(t *testing.T) *taskmill.Client {
	t.Helper()
	pool := testdb.Pool(t)
	schema := testdb.Schema(t, pool)
	if err := taskmill.Migrate(t.Context(), pool, &taskmill.MigrateConfig{Schema: schema}); err != nil {
		t.Fatal(err)
	}
	client, err := taskmill.NewClient(t.Context(), pool, &taskmill.ClientConfig{Schema: schema})
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func get(t *testing.T, h http.Handler, target string) (int, string, http.Header) {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, target, nil))
	body, _ := io.ReadAll(rec.Body)
	return rec.Code, string(body), rec.Header()
}

func post(t *testing.T, h http.Handler, target, fetchSite string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, target, strings.NewReader(url.Values{}.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	if fetchSite != "" {
		req.Header.Set("Sec-Fetch-Site", fetchSite)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestPagesRender(t *testing.T) {
	t.Parallel()
	client := setup(t)
	ctx := t.Context()
	res, err := client.Enqueue(ctx, greet{Name: "<script>alert(1)</script>"}, &taskmill.EnqueueOptions{Queue: "mail"})
	if err != nil {
		t.Fatal(err)
	}
	if err := client.UpdateQueue(ctx, "mail", &taskmill.QueueSettings{MaxRunning: 3}); err != nil {
		t.Fatal(err)
	}
	h := ui.Handler(client, &ui.Config{Prefix: "/admin/tasks"})
	id := strconv.FormatInt(res.ID, 10)

	code, body, header := get(t, h, "/admin/tasks/")
	if code != http.StatusOK || !strings.Contains(body, "mail") || !strings.Contains(body, "max 3 running") {
		t.Fatalf("overview: %d\n%s", code, body)
	}
	if csp := header.Get("Content-Security-Policy"); !strings.Contains(csp, "default-src 'none'") {
		t.Fatalf("missing security headers: %v", header)
	}

	code, body, _ = get(t, h, "/admin/tasks/tasks?queue=mail&state=available")
	if code != http.StatusOK || !strings.Contains(body, `href="/admin/tasks/tasks/`+id+`"`) {
		t.Fatalf("task list: %d\n%s", code, body)
	}

	code, body, _ = get(t, h, "/admin/tasks/tasks/"+id)
	if code != http.StatusOK || strings.Contains(body, "<script>alert") || !strings.Contains(body, "&lt;script&gt;") {
		t.Fatalf("task page did not escape the payload: %d\n%s", code, body)
	}

	if code, _, _ = get(t, h, "/admin/tasks/tasks/999999"); code != http.StatusNotFound {
		t.Fatalf("unknown task: %d", code)
	}
	if code, _, _ = get(t, h, "/admin/tasks/static/style.css"); code != http.StatusOK {
		t.Fatalf("stylesheet: %d", code)
	}
}

func TestActions(t *testing.T) {
	t.Parallel()
	client := setup(t)
	ctx := t.Context()
	res, err := client.Enqueue(ctx, greet{Name: "x"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	h := ui.Handler(client, nil)
	id := strconv.FormatInt(res.ID, 10)

	if rec := post(t, h, "/tasks/"+id+"/cancel", "cross-site"); rec.Code != http.StatusForbidden {
		t.Fatalf("cross-site cancel: %d", rec.Code)
	}
	if rec := post(t, h, "/tasks/"+id+"/cancel", "same-origin"); rec.Code != http.StatusSeeOther {
		t.Fatalf("cancel: %d %s", rec.Code, rec.Body)
	}
	if info, _ := client.Get(ctx, res.ID); info.State != taskmill.StateCanceled {
		t.Fatalf("task is %s after cancel", info.State)
	}
	if rec := post(t, h, "/tasks/"+id+"/cancel", "same-origin"); rec.Code != http.StatusConflict {
		t.Fatalf("second cancel: %d", rec.Code)
	}
	if rec := post(t, h, "/tasks/"+id+"/retry", "same-origin"); rec.Code != http.StatusSeeOther {
		t.Fatalf("retry: %d", rec.Code)
	}
	if rec := post(t, h, "/queues/default/pause", "same-origin"); rec.Code != http.StatusSeeOther {
		t.Fatalf("pause: %d", rec.Code)
	}
	if rec := post(t, h, "/tasks/"+id+"/delete", "same-origin"); rec.Code != http.StatusSeeOther {
		t.Fatalf("delete: %d", rec.Code)
	}
	if _, err := client.Get(context.WithoutCancel(ctx), res.ID); err == nil {
		t.Fatal("deleted task still exists")
	}
}

func TestReadOnly(t *testing.T) {
	t.Parallel()
	client := setup(t)
	res, err := client.Enqueue(t.Context(), greet{Name: "x"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	h := ui.Handler(client, &ui.Config{ReadOnly: true})
	id := strconv.FormatInt(res.ID, 10)

	_, body, _ := get(t, h, "/tasks/"+id)
	if strings.Contains(body, "/cancel") || !strings.Contains(body, "read-only") {
		t.Fatalf("read-only page offers actions:\n%s", body)
	}
	if rec := post(t, h, "/tasks/"+id+"/cancel", "same-origin"); rec.Code != http.StatusForbidden {
		t.Fatalf("read-only cancel: %d", rec.Code)
	}
}
