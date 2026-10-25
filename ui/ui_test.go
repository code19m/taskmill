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

	"github.com/jackc/pgx/v5/pgxpool"

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
	client, _, _ := setupDB(t)
	return client
}

// setupDB migrates a fresh schema, and returns a client for it, with its
// pool and name. When the test ends, it checks the schema's FIFO groups.
func setupDB(t *testing.T) (*taskmill.Client, *pgxpool.Pool, string) {
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
	t.Cleanup(func() {
		if !t.Failed() {
			testdb.CheckGroups(t, pool, schema)
		}
	})
	return client, pool, schema
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

func TestBulkActions(t *testing.T) {
	t.Parallel()
	client, pool, schema := setupDB(t)
	ctx := t.Context()
	var ids []int64
	for range 4 {
		res, err := client.Enqueue(ctx, greet{Name: "x"}, &taskmill.EnqueueOptions{Queue: "mail"})
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, res.ID)
	}
	if err := client.Cancel(ctx, ids[0]); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE `+schema+`.tasks SET state = 'running', attempt = 1, lease_token = 1,
	    lease_expires_at = now() + interval '1 hour', attempted_at = now(), attempted_by = 'w' WHERE id = $1`, ids[3]); err != nil {
		t.Fatal(err)
	}
	logs := &actionLog{}
	h := ui.Handler(client, &ui.Config{Logger: slog.New(logs)})

	// An action's page counts the tasks once, and confirms with or without
	// the script.
	code, body, _ := get(t, h, "/tasks/bulk/cancel?queue=mail")
	if code != http.StatusOK || !strings.Contains(body, "Cancel 3 tasks</button>") || strings.Contains(body, `tabindex="-1" data-live`) ||
		!strings.Contains(body, `<form class="bulk-actions" method="post" action="/tasks/bulk/cancel?queue=mail">`) {
		t.Fatalf("cancel page: %d\n%s", code, body)
	}
	if rec := post(t, h, "/tasks/bulk/cancel?queue=mail", "cross-site"); rec.Code != http.StatusForbidden {
		t.Fatalf("cross-site bulk cancel: %d", rec.Code)
	}
	rec := post(t, h, "/tasks/bulk/cancel?queue=mail", "same-origin")
	if rec.Code != http.StatusSeeOther || rec.Header().Get("Location") != "/tasks?count=3&done=cancel-all&queue=mail" {
		t.Fatalf("bulk cancel: %d to %q", rec.Code, rec.Header().Get("Location"))
	}
	if _, body, _ := get(t, h, rec.Header().Get("Location")); !strings.Contains(body, "3 tasks canceled.") {
		t.Fatal("no notice after the bulk cancel")
	}
	// The running task was asked to stop: canceling it again is not offered.
	if _, body, _ := get(t, h, "/tasks/bulk/cancel?queue=mail"); !strings.Contains(body, "No task matches.") || strings.Contains(body, `method="post"`) {
		t.Fatalf("cancel page offers the running task again:\n%s", body)
	}

	if rec := post(t, h, "/tasks/bulk/delete?state=canceled", "same-origin"); rec.Code != http.StatusSeeOther {
		t.Fatalf("bulk delete: %d", rec.Code)
	}
	if code, _, _ := get(t, h, "/tasks/bulk/delete"); code != http.StatusBadRequest {
		t.Fatalf("bulk delete page without filters: %d", code)
	}
	if rec := post(t, h, "/tasks/bulk/delete", "same-origin"); rec.Code != http.StatusConflict {
		t.Fatalf("bulk delete without filters: %d", rec.Code)
	}
	if code, _, _ := get(t, h, "/tasks/bulk/archive?queue=mail"); code != http.StatusNotFound {
		t.Fatalf("unknown bulk action: %d", code)
	}
	want := []string{"cancel tasks queue=mail", "delete tasks state=canceled"}
	if !slices.Equal(logs.actions, want) {
		t.Fatalf("logged actions %v, want %v", logs.actions, want)
	}
}

// The task list offers the bulk actions that the selected states allow,
// without counting the tasks.
func TestTaskListOffersBulkActions(t *testing.T) {
	t.Parallel()
	h := ui.Handler(setup(t), nil)
	for path, want := range map[string][]string{
		"/tasks":                            nil,
		"/tasks?queue=mail":                 {"/tasks/bulk/retry?queue=mail", "/tasks/bulk/cancel?queue=mail", "/tasks/bulk/delete?queue=mail"},
		"/tasks?queue=mail&state=available": {"/tasks/bulk/cancel?", "/tasks/bulk/delete?"},
		"/tasks?state=dead":                 {"/tasks/bulk/retry?state=dead", "/tasks/bulk/delete?state=dead"},
		"/tasks?state=running&kind=greet":   {"/tasks/bulk/cancel?kind=greet&amp;state=running"},
	} {
		_, body, _ := get(t, h, path)
		if got := strings.Count(body, `href="/tasks/bulk/`); got != len(want) {
			t.Errorf("%s offers %d bulk actions, want %d", path, got, len(want))
		}
		for _, link := range want {
			if !strings.Contains(body, `href="`+link) {
				t.Errorf("%s does not offer %s", path, link)
			}
		}
	}
}

// A bulk action that runs out of time says how far it got, and leads back to
// its page to go on.
func TestBulkActionStopsInTime(t *testing.T) {
	t.Parallel()
	client := setup(t)
	params := make([]taskmill.EnqueueParams, 5000)
	for i := range params {
		params[i] = taskmill.EnqueueParams{Args: greet{Name: "x"}, Options: &taskmill.EnqueueOptions{GroupKey: strconv.Itoa(i % 10)}}
	}
	for range 6 {
		if _, err := client.EnqueueMany(t.Context(), params); err != nil {
			t.Fatal(err)
		}
	}
	h := ui.Handler(client, nil)
	ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
	defer cancel()
	req := httptest.NewRequestWithContext(ctx, http.MethodPost, "/tasks/bulk/cancel?queue=default", nil)
	req.Header.Set("Sec-Fetch-Site", "same-origin")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	loc, _ := url.Parse(rec.Header().Get("Location"))
	q := loc.Query()
	n, _ := strconv.Atoi(q.Get("count"))
	if rec.Code != http.StatusSeeOther || loc.Path != "/tasks/bulk/cancel" || q.Get("done") != "cancel-more" || n >= 30000 {
		t.Fatalf("slow bulk cancel: %d to %q", rec.Code, loc)
	}
	_, body, _ := get(t, h, loc.String())
	if !strings.Contains(body, html.EscapeString(number(n)+" tasks canceled so far; more remain.")) || !strings.Contains(body, "Cancel ") {
		t.Fatalf("the page after a partial cancel:\n%s", body)
	}
}

// number formats n as the interface does.
func number(n int) string {
	s := strconv.Itoa(n)
	for i := len(s) - 3; i > 0; i -= 3 {
		s = s[:i] + "," + s[i:]
	}
	return s
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
	if _, body, _ := get(t, h, "/tasks?queue=default"); strings.Contains(body, "/tasks/bulk/") {
		t.Fatalf("read-only task list offers bulk actions:\n%s", body)
	}
	if code, _, _ := get(t, h, "/tasks/bulk/cancel?queue=default"); code != http.StatusForbidden {
		t.Fatalf("read-only bulk cancel page: %d", code)
	}
	if rec := post(t, h, "/tasks/bulk/cancel?queue=default", "same-origin"); rec.Code != http.StatusForbidden {
		t.Fatalf("read-only bulk cancel: %d", rec.Code)
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
	for _, path := range []string{"/?done=%3Cb%3Ehi", "/?done=retry-all&count=%3Cb%3E"} {
		if _, body, _ := get(t, h, path); strings.Contains(body, "data-toast") {
			t.Fatalf("%s: a notice was shown", path)
		}
	}
}
