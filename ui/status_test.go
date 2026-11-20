package ui_test

import (
	"context"
	"errors"
	"html"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/code19m/taskmill/internal/testdb"
	"github.com/code19m/taskmill/ui"
)

// A page that the dashboard shows in place of another says what happened,
// in plain words, and offers the way on, the most useful first. A page of a
// cause that may pass tries its address again, with the script, and a
// failed action leads back to where it was sent from: it is never sent
// again. No page shows a detail of the error, which the log has.
func TestStatusPages(t *testing.T) {
	t.Parallel()
	pool, down := poolThatCanFail(t)
	client, _, schema := setupOn(t, pool)
	t.Cleanup(func() { down.Store(false) })
	logs := &actionLog{}
	h := handler(t, client, &ui.Config{Logger: slog.New(logs)})
	res, err := client.Enqueue(t.Context(), greet{Name: "x"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	id := strconv.FormatInt(res.ID, 10)
	serve := func(method, target string) *httptest.ResponseRecorder {
		if method == http.MethodPost {
			return post(t, h, target, "same-origin")
		}
		req := httptest.NewRequestWithContext(t.Context(), method, target, nil)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		return rec
	}
	check := func(what string, rec *httptest.ResponseRecorder, w wantStatus) {
		t.Helper()
		checkStatusPage(t, what, rec, w, schema)
	}

	check("a task that does not exist", serve(http.MethodGet, "/tasks/999999"), wantStatus{
		http.StatusNotFound, "Task not found", "Task 999999 does not exist.", []string{"primary /tasks Back to the tasks"}, false,
	})
	check("an address of no page", serve(http.MethodGet, "/nowhere"), wantStatus{
		http.StatusNotFound, "Page not found", "Nothing exists at this address.", []string{"primary / Back to the overview"}, false,
	})
	check("an action on a task that does not exist", serve(http.MethodPost, "/tasks/999999/retry"), wantStatus{
		http.StatusNotFound, "Not found", "It may have been deleted meanwhile.", []string{"primary /tasks Back to the tasks"}, false,
	})

	// A statement that the database refuses is a failure that the dashboard
	// does not know: here, a table that is not there.
	rename := func(from, to string) {
		if _, err := pool.Exec(context.WithoutCancel(t.Context()), "ALTER TABLE "+schema+"."+from+" RENAME TO "+to); err != nil {
			t.Fatal(err)
		}
	}
	rename("workers", "workers_gone")
	check("a failure", serve(http.MethodGet, "/workers"), wantStatus{
		http.StatusInternalServerError, "Something went wrong",
		"The dashboard could not read what this page shows. The server log has the details.",
		[]string{"primary /workers Try again", "quiet / Back to the overview"},
		true,
	})
	rename("workers_gone", "workers")

	down.Store(true)
	pool.Reset() // the connections that the pool holds go too
	const unreachable, title = "The dashboard is running, but it cannot connect to its database.", "Can’t reach the database"
	check("the overview without its database", serve(http.MethodGet, "/"), wantStatus{
		http.StatusServiceUnavailable, title, unreachable, []string{"primary / Try again"}, true,
	})
	check("a list without its database", serve(http.MethodGet, "/tasks?state=dead"), wantStatus{
		http.StatusServiceUnavailable, title, unreachable, []string{"primary /tasks?state=dead Try again", "quiet / Back to the overview"}, true,
	})
	check("an action without its database", serve(http.MethodPost, "/tasks/"+id+"/retry"), wantStatus{
		http.StatusServiceUnavailable, title, unreachable + " The action did not finish: check the page before you try again.",
		[]string{"primary /tasks/" + id + " Back to the task"},
		true,
	})
	check("a bulk action without its database", serve(http.MethodPost, "/tasks/bulk/cancel?queue=default"), wantStatus{
		http.StatusServiceUnavailable, title, "The action did not finish", []string{"primary /tasks?queue=default Back to the tasks"}, true,
	})
	down.Store(false)
	if code, _, _ := get(t, h, "/"); code != http.StatusOK {
		t.Errorf("the overview with its database back: %d", code)
	}
	// The log has what the pages leave out.
	logs.mu.Lock()
	defer logs.mu.Unlock()
	if n := strings.Count(strings.Join(logs.messages, "\n"), "dashboard request failed"); n != 5 {
		t.Errorf("logged %q, want a failed request for each of the 5 failures", logs.messages)
	}
}

// wantStatus is what a status page should say.
type wantStatus struct {
	code    int
	title   string
	text    string
	actions []string // each link's look, address, and label, the main one first
	retry   bool     // it tries again, with the script
}

// checkStatusPage checks a status page, and that it shows no detail of an
// error: no address, no class of error, and no name in the database.
func checkStatusPage(t *testing.T, what string, rec *httptest.ResponseRecorder, w wantStatus, schema string) {
	t.Helper()
	page := regexp.MustCompile(`(?s)<template data-unreachable>.*?</template>`).ReplaceAllString(rec.Body.String(), "")
	var actions []string
	for _, m := range regexp.MustCompile(`<a class="btn (primary|quiet)" href="([^"]*)">.*?</svg>([^<]+)</a>`).FindAllStringSubmatch(page, -1) {
		actions = append(actions, m[1]+" "+html.UnescapeString(m[2])+" "+m[3])
	}
	if rec.Code != w.code || !strings.Contains(page, "<h1>"+w.title+"</h1>") || !strings.Contains(page, w.text) ||
		!slices.Equal(actions, w.actions) || strings.Contains(page, `data-live="retry"`) != w.retry ||
		strings.Contains(page, "data-retry") != w.retry || (rec.Header().Get("Retry-After") != "") != (w.code == http.StatusServiceUnavailable) {
		t.Errorf("%s: %d, actions %q, want %d %q saying %q, actions %q, trying again: %t\n%s",
			what, rec.Code, actions, w.code, w.title, w.text, w.actions, w.retry, page)
	}
	for _, detail := range []string{"taskmill:", "127.0.0.1", "dial tcp", "refused", "failed to connect", "user=", "SQLSTATE", "workers_gone", schema} {
		if strings.Contains(page, detail) {
			t.Errorf("%s: the page shows %q, a detail of the error\n%s", what, detail, page)
		}
	}
}

// poolThatCanFail returns a pool of the test database that cannot connect
// while down is set, as when the database is out of reach.
func poolThatCanFail(t *testing.T) (*pgxpool.Pool, *atomic.Bool) {
	t.Helper()
	cfg, err := pgxpool.ParseConfig(testdb.URL(t))
	if err != nil {
		t.Fatal(err)
	}
	down := new(atomic.Bool)
	dial := cfg.ConnConfig.DialFunc
	cfg.ConnConfig.DialFunc = func(ctx context.Context, network, addr string) (net.Conn, error) {
		if down.Load() {
			return nil, &net.OpError{Op: "dial", Net: network, Err: errors.New("connect: connection refused")}
		}
		return dial(ctx, network, addr)
	}
	pool, err := pgxpool.NewWithConfig(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return pool, down
}

// A request that runs out of time gets the page that says so, whoever set
// its deadline: the dashboard, or the application, whose middleware may give
// a request less time. Only a request whose client went away gets no answer.
func TestRequestsPastTheirDeadline(t *testing.T) {
	t.Parallel()
	logs := &actionLog{}
	h := handler(t, setup(t), &ui.Config{Logger: slog.New(logs)})
	late, cancel := context.WithDeadline(t.Context(), time.Now().Add(-time.Second))
	defer cancel()
	gone, cancel := context.WithCancel(t.Context())
	cancel()
	serve := func(ctx context.Context, method, target string) *httptest.ResponseRecorder {
		req := httptest.NewRequestWithContext(ctx, method, target, nil)
		req.Header.Set("Sec-Fetch-Site", "same-origin")
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		return rec
	}
	requests := []struct{ method, target string }{
		{http.MethodGet, "/"},
		{http.MethodGet, "/tasks?state=dead"},
		{http.MethodGet, "/tasks/1"},
		{http.MethodGet, "/tasks/bulk/retry?state=dead"},
		{http.MethodGet, "/schedules"},
		{http.MethodGet, "/workers"},
		{http.MethodPost, "/tasks/1/retry"},
		{http.MethodPost, "/tasks/bulk/retry?state=dead"},
		{http.MethodPost, "/queues/pause?name=default"},
	}
	for _, r := range requests {
		rec := serve(late, r.method, r.target)
		if body := rec.Body.String(); rec.Code != http.StatusServiceUnavailable || rec.Header().Get("Retry-After") == "" ||
			!strings.Contains(body, "<h1>Taking too long</h1>") {
			t.Errorf("%s %s past its deadline: status %d, want 503 and a page that says so\n%s", r.method, r.target, rec.Code, body)
		}
		if rec := serve(gone, r.method, r.target); rec.Body.Len() != 0 {
			t.Errorf("%s %s from a client that went away: an answer of %d bytes, want none", r.method, r.target, rec.Body.Len())
		}
	}
	want := slices.Repeat([]string{"dashboard request timed out"}, len(requests))
	if !slices.Equal(logs.messages, want) {
		t.Errorf("logged %q, want one %q for each request past its deadline, and nothing else", logs.messages, want[0])
	}
}
