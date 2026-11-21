package ui_test

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/code19m/taskmill"
)

// FuzzPages requests any path and query string, as a GET or as a POST:
// filters, ranges, times, cursors, searches, task IDs, and the names in
// actions come from the browser, so a request may get an error page but
// never a server error.
func FuzzPages(f *testing.F) {
	for _, target := range []string{
		"/", "/?range=7d", "/?range=%ff",
		"/tasks?state=dead&queue=mail&kind=greet&group=topic%1Fkey",
		"/tasks?state=completed&state=available&cursor=MTA", "/tasks?cursor=LTE", "/tasks?cursor=%25", "/tasks?limit=250", "/tasks?limit=-1",
		"/tasks?state=nope", "/tasks/1", "/tasks/-1", "/tasks/9223372036854775808",
		"/tasks?state=dead&since=2026-11-19T14:05Z&before=2026-11-20T00:00:00%2B05:00", "/tasks?state=canceled&since=2026-11-19T14:05",
		"/tasks?state=dead&before=0001-01-01T00:00:01Z", "/tasks?state=dead&since=9999-12-31T23:59:59Z&limit=250",
		"/tasks?state=dead&since=0000-01-01T00:00Z", "/tasks?since=2026-11-19T14:05Z", "/tasks?state=dead&before=%ff",
		"/tasks?queue=mail&since=2026-11-19T14:05Z&before=2026-11-20T00:00:00%2B05:00&limit=250", "/tasks?state=completed&before=9999-12-31T23:59:59Z",
		"/tasks?since=2026-11-19T14:05Z&cursor=MTA", "/tasks?state=dead&since=2026-11-19T14:05Z&cursor=YzAsMQ", "/tasks?since=0001-01-01T00:00:01Z&cursor=YzAsMQ",
		"/choices/groups?search=%25_", "/schedules", "/workers", "/static/0/ui.css", "/static/0/app.js",
		// Text that PostgreSQL rejects once failed the request.
		"/tasks?queue=%00", "/tasks?kind=%ff", "/tasks?group=%c3", "/choices/groups?search=%00",
	} {
		f.Add(target, false)
	}

	for _, target := range []string{
		"/tasks/1/retry", "/tasks/1/cancel", "/queues/pause?name=mail", "/queues/resume?name=%ff", "/queues/pause",
		// Text that PostgreSQL rejects once failed the request.
		"/schedules/run?name=%00", "/schedules/pause?name=%ff",
	} {
		f.Add(target, true)
	}

	client := setup(f)
	if _, err := client.Enqueue(f.Context(), greet{Name: "a"},
		&taskmill.EnqueueOptions{Queue: "mail", GroupKey: "topic\x1fkey"}); err != nil {
		f.Fatal(err)
	}

	h := handler(f, client, nil)

	f.Fuzz(func(t *testing.T, target string, post bool) {
		u, err := url.ParseRequestURI(target)
		if err != nil {
			return // no browser sends it
		}

		method := http.MethodGet
		if post {
			method = http.MethodPost
		}

		req := httptest.NewRequestWithContext(t.Context(), method, "/", nil)
		req.URL, req.RequestURI = u, target
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code >= http.StatusInternalServerError {
			t.Fatalf("%s %q: status %d", method, target, rec.Code)
		}
	})
}
