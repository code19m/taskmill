package ui_test

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/code19m/taskmill"
	"github.com/code19m/taskmill/ui"
)

// FuzzPages requests the read-only pages with any path and query string:
// filters, ranges, cursors, searches, and task IDs come from the browser, so
// a request may get an error page but never a server error.
func FuzzPages(f *testing.F) {
	for _, target := range []string{
		"/", "/?range=7d", "/?range=%ff",
		"/tasks?state=dead&queue=mail&kind=greet&group=topic%1Fkey",
		"/tasks?state=completed&state=available&cursor=MTA", "/tasks?cursor=LTE", "/tasks?cursor=%25",
		"/tasks?queue=%00", "/tasks?kind=%ff", "/tasks?group=%c3", "/tasks?state=nope",
		"/tasks/1", "/tasks/-1", "/tasks/9223372036854775808",
		"/choices/groups?search=%25_", "/choices/groups?search=%00", "/schedules", "/workers",
		"/static/ui.css?v=0",
	} {
		f.Add(target)
	}
	client := setup(f)
	if _, err := client.Enqueue(f.Context(), greet{Name: "a"},
		&taskmill.EnqueueOptions{Queue: "mail", GroupKey: "topic\x1fkey"}); err != nil {
		f.Fatal(err)
	}
	h := ui.Handler(client, nil)

	f.Fuzz(func(t *testing.T, target string) {
		u, err := url.ParseRequestURI(target)
		if err != nil {
			return // no browser sends it
		}
		req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/", nil)
		req.URL, req.RequestURI = u, target
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code == http.StatusInternalServerError {
			t.Fatalf("GET %q: server error", target)
		}
	})
}
