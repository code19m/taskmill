package ui_test

import (
	"context"
	"html"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

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
	res, err := client.Enqueue(ctx, greet{Name: "<script>alert(1)</script>"},
		&taskmill.EnqueueOptions{Queue: "mail", GroupKey: "topic\x1fkey", Priority: taskmill.PriorityHigh})
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
	if !strings.Contains(body, "<dd>high</dd>") || !strings.Contains(body, "topic␟key") ||
		!strings.Contains(body, "group=topic%1fkey") {
		t.Fatalf("task page: want the priority's name and a visible link to the group\n%s", body)
	}
	code, body, _ = get(t, h, "/admin/tasks/tasks?queue=mail&group=topic%1Fkey")
	if code != http.StatusOK || !strings.Contains(body, `href="/admin/tasks/tasks/`+id+`"`) {
		t.Fatalf("group's tasks: %d\n%s", code, body)
	}
	if code, _, _ = get(t, h, "/admin/tasks/tasks?state=bogus"); code != http.StatusBadRequest {
		t.Fatalf("unknown state: %d", code)
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
	logs := &actionLog{}
	h := ui.Handler(client, &ui.Config{Logger: slog.New(logs)})
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
	want := []string{"cancel task " + id, "retry task " + id, "pause queue default", "delete task " + id}
	if !slices.Equal(logs.actions, want) {
		t.Fatalf("logged actions %v, want %v", logs.actions, want)
	}
}

// actionLog records the actions that the web interface logs.
type actionLog struct {
	mu      sync.Mutex
	actions []string
}

func (*actionLog) Enabled(context.Context, slog.Level) bool { return true }

func (l *actionLog) Handle(_ context.Context, r slog.Record) error {
	var action, target string
	r.Attrs(func(a slog.Attr) bool {
		switch a.Key {
		case "action":
			action = a.Value.String()
		case "target":
			target = a.Value.String()
		}
		return true
	})
	if r.Message == "web interface action" {
		l.mu.Lock()
		l.actions = append(l.actions, action+" "+target)
		l.mu.Unlock()
	}
	return nil
}

func (l *actionLog) WithAttrs([]slog.Attr) slog.Handler { return l }

func (l *actionLog) WithGroup(string) slog.Handler { return l }

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

func TestQueueActionsWorkForAnyName(t *testing.T) {
	t.Parallel()
	client := setup(t)
	ctx := t.Context()
	const name = "a/b?c#d%2Fe"
	if err := client.UpdateQueue(ctx, name, nil); err != nil {
		t.Fatal(err)
	}
	h := ui.Handler(client, &ui.Config{Prefix: "/admin"})

	_, body, _ := get(t, h, "/admin/")
	m := regexp.MustCompile(`action="([^"]*/pause)"`).FindStringSubmatch(body)
	if m == nil {
		t.Fatalf("no pause form in\n%s", body)
	}
	if rec := post(t, h, html.UnescapeString(m[1]), "same-origin"); rec.Code != http.StatusSeeOther {
		t.Fatalf("pause: got %d", rec.Code)
	}
	queues, err := client.Queues(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, q := range queues {
		if q.Paused != (q.Name == name) {
			t.Fatalf("queue %q paused: %v", q.Name, q.Paused)
		}
	}
}

func TestScheduleActionsWorkForAnyName(t *testing.T) {
	t.Parallel()
	client := setup(t)
	ctx := t.Context()
	const name = "a/b?c#d%2Fe"

	// A worker stores its schedules in its first maintenance cycle.
	w, err := taskmill.NewWorker(client, &taskmill.WorkerConfig{
		Schedules: []taskmill.Schedule{{Name: name, Cron: "@daily", Args: greet{}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	wctx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- w.Run(wctx) }()
	deadline := time.Now().Add(15 * time.Second)
	for {
		ss, err := client.Schedules(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if len(ss) == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the schedule was not stored")
		}
		time.Sleep(20 * time.Millisecond)
	}
	stop()
	if err := <-done; err != nil {
		t.Fatal(err)
	}

	h := ui.Handler(client, &ui.Config{Prefix: "/admin"})
	_, body, _ := get(t, h, "/admin/")
	m := regexp.MustCompile(`action="([^"]*/schedules/[^"]*/pause)"`).FindStringSubmatch(body)
	if m == nil {
		t.Fatalf("no schedule pause form in\n%s", body)
	}
	if rec := post(t, h, html.UnescapeString(m[1]), "same-origin"); rec.Code != http.StatusSeeOther {
		t.Fatalf("pause: got %d", rec.Code)
	}
	ss, err := client.Schedules(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(ss) != 1 || ss[0].Name != name || !ss[0].Paused {
		t.Fatalf("got %+v", ss)
	}
}
