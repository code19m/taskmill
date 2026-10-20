package ui_test

import (
	"compress/gzip"
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

func get(t *testing.T, h http.Handler, target string, headers ...string) (int, string, http.Header) {
	t.Helper()
	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, target, nil)
	for i := 0; i+1 < len(headers); i += 2 {
		req.Header.Set(headers[i], headers[i+1])
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
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
	if code != http.StatusOK || !strings.Contains(body, "mail") || !strings.Contains(body, "max 3") {
		t.Fatalf("overview: %d\n%s", code, body)
	}
	if csp := header.Get("Content-Security-Policy"); !strings.Contains(csp, "default-src 'none'") ||
		!strings.Contains(csp, "frame-ancestors 'none'") || header.Get("X-Frame-Options") != "DENY" {
		t.Fatalf("missing security headers: %v", header)
	}
	for _, path := range []string{"/admin/tasks/schedules", "/admin/tasks/workers"} {
		if code, _, _ := get(t, h, path); code != http.StatusOK {
			t.Fatalf("%s: %d", path, code)
		}
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
		!strings.Contains(body, "group=topic%1Fkey") {
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

func TestOverviewPeriods(t *testing.T) {
	t.Parallel()
	h := ui.Handler(setup(t), nil)
	// The charts' period is chosen in the address; an unknown one shows the
	// last hour.
	for query, want := range map[string]string{"?range=24h": "24h", "?range=7y": "1h"} {
		_, body, _ := get(t, h, "/"+query)
		if !strings.Contains(body, `aria-current="true">`+want+`<`) || !strings.Contains(body, `class="series"`) {
			t.Fatalf("overview %s: want the %s period selected\n%s", query, want, body)
		}
	}
}

func TestTaskFilters(t *testing.T) {
	t.Parallel()
	client := setup(t)
	ctx := t.Context()
	mail, err := client.Enqueue(ctx, greet{Name: "a"}, &taskmill.EnqueueOptions{Queue: "mail", GroupKey: "acct-1"})
	if err != nil {
		t.Fatal(err)
	}
	news, err := client.Enqueue(ctx, greet{Name: "b"}, &taskmill.EnqueueOptions{Queue: "news", GroupKey: "acct-2"})
	if err != nil {
		t.Fatal(err)
	}
	h := ui.Handler(client, &ui.Config{Prefix: "/admin/tasks"})
	link := func(id int64) string { return `href="/admin/tasks/tasks/` + strconv.FormatInt(id, 10) + `"` }
	for _, c := range []struct {
		query      string
		mail, news bool
	}{
		{"queue=mail&queue=news", true, true},
		{"queue=news", false, true},
		{"state=dead&state=available&queue=mail", true, false},
		{"state=dead&state=canceled", false, false},
		{"group=acct-1&group=acct-2", true, true},
		{"kind=greet&group=acct-2", false, true},
		{"kind=other", false, false},
	} {
		_, body, _ := get(t, h, "/admin/tasks/tasks?"+c.query)
		if strings.Contains(body, link(mail.ID)) != c.mail || strings.Contains(body, link(news.ID)) != c.news {
			t.Fatalf("%s: want mail %v, news %v\n%s", c.query, c.mail, c.news, body)
		}
	}

	// The group filter offers the groups that have tasks, and searches them.
	if _, body, _ := get(t, h, "/admin/tasks/tasks"); !strings.Contains(body, `name="group" value="acct-1"`) {
		t.Fatalf("the group filter does not offer acct-1\n%s", body)
	}
	code, body, header := get(t, h, "/admin/tasks/choices/groups?search=T-2")
	if code != http.StatusOK || header.Get("Content-Type") != "application/json" || strings.TrimSpace(body) != `["acct-2"]` {
		t.Fatalf("group search: %d %q", code, body)
	}
}

func TestLiveUpdateResponses(t *testing.T) {
	t.Parallel()
	h := ui.Handler(setup(t), nil)
	// Live updates ask for a page they already show, and are told it did not
	// change; a client that accepts it gets the page compressed.
	_, plain, header := get(t, h, "/workers")
	etag := header.Get("ETag")
	if code, body, _ := get(t, h, "/workers", "If-None-Match", etag); etag == "" || code != http.StatusNotModified || body != "" {
		t.Fatalf("unchanged page: ETag %q, got %d", etag, code)
	}
	code, zipped, header := get(t, h, "/workers", "Accept-Encoding", "gzip")
	if code != http.StatusOK || header.Get("Content-Encoding") != "gzip" {
		t.Fatalf("compressed page: %d, %v", code, header)
	}
	zr, err := gzip.NewReader(strings.NewReader(zipped))
	if err != nil {
		t.Fatal(err)
	}
	if unzipped, err := io.ReadAll(zr); err != nil || string(unzipped) != plain {
		t.Fatalf("compressed page differs: %v", err)
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
	_, body, _ := get(t, h, "/admin/schedules")
	form := func(action string) string {
		m := regexp.MustCompile(`action="([^"]*/schedules/[^"]*/` + action + `)"`).FindStringSubmatch(body)
		if m == nil {
			t.Fatalf("no schedule %s form in\n%s", action, body)
		}
		return html.UnescapeString(m[1])
	}
	run, pause := form("run"), form("pause")

	if rec := post(t, h, run, "same-origin"); rec.Code != http.StatusSeeOther {
		t.Fatalf("run now: got %d", rec.Code)
	}
	ss, err := client.Schedules(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(ss) != 1 || ss[0].NextRunAt.After(time.Now()) {
		t.Fatalf("run now left the schedule not due: %+v", ss)
	}

	if rec := post(t, h, pause, "same-origin"); rec.Code != http.StatusSeeOther {
		t.Fatalf("pause: got %d", rec.Code)
	}
	if ss, err = client.Schedules(ctx); err != nil {
		t.Fatal(err)
	}
	if len(ss) != 1 || ss[0].Name != name || !ss[0].Paused {
		t.Fatalf("got %+v", ss)
	}
	if rec := post(t, h, run, "same-origin"); rec.Code != http.StatusConflict {
		t.Fatalf("run now while paused: got %d, want %d", rec.Code, http.StatusConflict)
	}
}

func TestBasicAuth(t *testing.T) {
	t.Parallel()
	client := setup(t)
	h := ui.Handler(client, &ui.Config{BasicAuth: &ui.BasicAuth{Username: "ops", Password: "s3cret"}})

	for _, creds := range [][2]string{{}, {"ops", "wrong"}, {"other", "s3cret"}} {
		req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/static/style.css", nil)
		if creds[0] != "" {
			req.SetBasicAuth(creds[0], creds[1])
		}
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusUnauthorized || !strings.Contains(rec.Header().Get("WWW-Authenticate"), `Basic realm="taskmill"`) {
			t.Fatalf("credentials %q: got %d, %v", creds, rec.Code, rec.Header())
		}
	}
	// A live update after the password changed must not open a login prompt
	// every few seconds: the page shows that the session ended instead.
	code, _, header := get(t, h, "/", "X-Taskmill-Live", "1")
	if code != http.StatusUnauthorized || header.Get("WWW-Authenticate") != "" {
		t.Fatalf("live update without credentials: %d, %v", code, header)
	}
	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/", nil)
	req.SetBasicAuth("ops", "s3cret")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("logged in: got %d", rec.Code)
	}

	defer func() {
		if recover() == nil {
			t.Fatal("an empty password was accepted")
		}
	}()
	ui.Handler(client, &ui.Config{BasicAuth: &ui.BasicAuth{Username: "ops"}})
}

func TestFrameAncestors(t *testing.T) {
	t.Parallel()
	client := setup(t)
	h := ui.Handler(client, &ui.Config{FrameAncestors: []string{"'self'", "https://admin.example.com"}})
	_, _, header := get(t, h, "/workers")
	if csp := header.Get("Content-Security-Policy"); !strings.Contains(csp, "frame-ancestors 'self' https://admin.example.com") ||
		header.Get("X-Frame-Options") != "" {
		t.Fatalf("got %v", header)
	}

	defer func() {
		if recover() == nil {
			t.Fatal("an entry that injects a directive was accepted")
		}
	}()
	ui.Handler(client, &ui.Config{FrameAncestors: []string{"https://a.example; script-src *"}})
}

func TestStaticFilesAreVersionedAndCached(t *testing.T) {
	t.Parallel()
	client := setup(t)
	h := ui.Handler(client, nil)
	_, body, _ := get(t, h, "/workers")
	m := regexp.MustCompile(`href="(/static/style\.css\?v=[0-9a-f]+)"`).FindStringSubmatch(body)
	if m == nil {
		t.Fatalf("no versioned stylesheet link in\n%s", body)
	}
	code, _, header := get(t, h, m[1])
	if code != http.StatusOK || !strings.Contains(header.Get("Cache-Control"), "immutable") {
		t.Fatalf("stylesheet: %d, %v", code, header)
	}
	// A page from another version of the server links other versions, which
	// must not be cached as this one.
	code, _, header = get(t, h, "/static/style.css?v=00000000")
	if code != http.StatusOK || header.Get("Cache-Control") != "no-cache" {
		t.Fatalf("stylesheet of another version: %d, %v", code, header)
	}
}

func TestActionNotices(t *testing.T) {
	t.Parallel()
	client := setup(t)
	h := ui.Handler(client, nil)
	rec := post(t, h, "/queues/default/pause", "same-origin")
	if rec.Code != http.StatusSeeOther || rec.Header().Get("Location") != "/?done=queue-pause" {
		t.Fatalf("pause: %d to %q", rec.Code, rec.Header().Get("Location"))
	}
	if _, body, _ := get(t, h, "/?done=queue-pause"); !strings.Contains(body, "Queue paused.") {
		t.Fatal("no notice after pausing")
	}
	// Only fixed notices are shown, never text from the address.
	if _, body, _ := get(t, h, "/?done=%3Cb%3Ehi"); strings.Contains(body, "data-toast") {
		t.Fatal("an unknown notice was shown")
	}
}
