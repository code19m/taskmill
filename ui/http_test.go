package ui_test

import (
	"compress/gzip"
	"errors"
	"io"
	"io/fs"
	"maps"
	"net/http"
	"net/http/httptest"
	"os"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/code19m/taskmill"
	"github.com/code19m/taskmill/ui"
)

func TestLiveUpdateResponses(t *testing.T) {
	t.Parallel()
	h := handler(t, setup(t), nil)

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
	if unzipped, err := io.ReadAll(zr); err != nil || rendered.ReplaceAllString(string(unzipped), "") != rendered.ReplaceAllString(plain, "") {
		t.Fatalf("compressed page differs: %v", err)
	}
}

// rendered is a page's time of rendering, from which its relative times count.
var rendered = regexp.MustCompile(` data-now="[^"]+"`)

// A live update asks for the page that it shows, and is told that the page
// did not change while only the time did: the script keeps the relative
// times of the page on screen counting from its time of rendering, in the
// server's words, so a later copy that says the same once its times counted
// on is the page that the client has. Each page here has times that count,
// and the second between its two copies changes their words. A page whose
// data changed is sent anew, also when only an instant changed, such as a
// worker's check-in by a millisecond.
func TestLiveUpdatesOfPagesWithTimesThatCount(t *testing.T) {
	t.Parallel()
	client, pool, schema := setupDB(t)
	ctx := t.Context()
	res, err := client.Enqueue(ctx, greet{Name: "now"}, nil)
	if err != nil {
		t.Fatal(err)
	}

	// A running task, whose timeline counts how long it runs; the counts
	// of the maintainer; a worker that checked in; and a schedule that is
	// due within a minute, which the overview's Schedules card names.
	id := strconv.FormatInt(res.ID, 10)
	if _, err := pool.Exec(ctx, `UPDATE `+schema+`.tasks SET state = 'running', attempt = 1, attempted_at = now(),
	    attempted_by = 'w', lease_expires_at = now() + interval '1 minute' WHERE id = $1`, res.ID); err != nil {
		t.Fatal(err)
	}
	storeCounts(t, pool, schema, "0 seconds", false)
	if _, err := pool.Exec(ctx, `INSERT INTO `+schema+`.workers (id, queues, hostname, version) VALUES ('w', '{default}', 'h', 'v')`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO `+schema+`.schedules (name, queue, kind, cron, next_run_at, last_run_at)
	    VALUES ('minutely', 'default', 'greet', '* * * * *', now() + interval '30 seconds', now() - interval '30 seconds')`); err != nil {
		t.Fatal(err)
	}

	// What a page words to the minute, such as the presets of the filter by
	// time, changes at the minute: both copies come within one.
	if left := time.Until(time.Now().Truncate(time.Minute).Add(time.Minute)); left < 5*time.Second {
		time.Sleep(left)
	}

	h := handler(t, client, nil)
	targets := []string{"/", "/tasks", "/tasks/" + id, "/schedules", "/workers"}
	first, etags := map[string]string{}, map[string]string{}
	for _, target := range targets {
		_, body, header := get(t, h, target)
		first[target], etags[target] = rendered.ReplaceAllString(body, ""), header.Get("ETag")
	}

	time.Sleep(time.Until(time.Now().Truncate(time.Second).Add(time.Second))) // the next second
	for _, target := range targets {
		if _, later, _ := get(t, h, target); rendered.ReplaceAllString(later, "") == first[target] {
			t.Fatalf("%s: a second later, the page says the same: its times do not count", target)
		}
		if code, _, _ := get(t, h, target, "If-None-Match", etags[target]); code != http.StatusNotModified {
			t.Errorf("%s: a live update a second later got %d, want 304: only the words of its times changed", target, code)
		}
	}

	if _, err := pool.Exec(ctx, `UPDATE `+schema+`.workers SET heartbeat_at = heartbeat_at + interval '1 millisecond'`); err != nil {
		t.Fatal(err)
	}
	if code, _, _ := get(t, h, "/workers", "If-None-Match", etags["/workers"]); code != http.StatusOK {
		t.Errorf("a live update after a check-in got %d, want 200", code)
	}
}

// A response is compressed when the request accepts gzip, as RFC 9110 reads
// Accept-Encoding: over all its lines, by the weight of gzip, or else of "*",
// with a name and a weight in any case. A weight of 0 refuses, in any of its
// forms, and so does a weight that is not one.
func TestCompressionFollowsAcceptEncoding(t *testing.T) {
	t.Parallel()
	h := handler(t, setup(t), nil)
	for _, tc := range []struct {
		lines []string
		gzip  bool
	}{
		{nil, false},
		{[]string{"gzip"}, true},
		{[]string{"GZip ; Q=0.5"}, true},
		{[]string{"br", "gzip"}, true},
		{[]string{"br, *"}, true},
		{[]string{"deflate"}, false},
		{[]string{"gzip;q=0"}, false},
		{[]string{"gzip; Q=0"}, false},
		{[]string{"gzip;q=0.0"}, false},
		{[]string{"gzip;q=0.000, br"}, false},
		{[]string{"gzip;q=0, *"}, false},
		{[]string{"*;q=0"}, false},
		{[]string{"gzip;q=x"}, false},
		{[]string{"gzip;q=-1, *"}, false},
	} {
		req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/static/any/style.css", nil)
		for _, line := range tc.lines {
			req.Header.Add("Accept-Encoding", line)
		}
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if got := rec.Header().Get("Content-Encoding") == "gzip"; got != tc.gzip {
			t.Errorf("Accept-Encoding %q: compressed %v, want %v", tc.lines, got, tc.gzip)
		}
	}
}

func TestBasicAuth(t *testing.T) {
	t.Parallel()
	client := setup(t)
	h := handler(t, client, &ui.Config{BasicAuth: &ui.BasicAuth{Username: "ops", Password: "s3cret"}})

	for _, creds := range [][2]string{{}, {"ops", "wrong"}, {"other", "s3cret"}} {
		req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/static/any/style.css", nil)
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
}

// A dashboard with a prefix answers only for the paths under it, and never
// sends a client to a path outside it.
func TestPrefixKeepsRequestsAndRedirects(t *testing.T) {
	t.Parallel()
	h := handler(t, setup(t), &ui.Config{Prefix: "/tm"})
	for _, tc := range []struct {
		method, target string
		code           int
		location       string // of a redirect that the test knows
	}{
		{http.MethodGet, "/tm/tasks?state=dead", http.StatusOK, ""},
		// The prefix alone, and a path that is not clean, lead to the clean
		// path under the prefix, with the same method.
		{http.MethodGet, "/tm", http.StatusTemporaryRedirect, "/tm/"},
		{http.MethodGet, "/tm//tasks?state=dead", http.StatusTemporaryRedirect, "/tm/tasks?state=dead"},
		{http.MethodGet, "/tm//example.com/x", http.StatusTemporaryRedirect, "/tm/example.com/x"},
		{http.MethodGet, "/tm/../tasks", http.StatusTemporaryRedirect, "/tm/tasks"},
		{http.MethodPost, "/tm//tasks/1/retry", http.StatusTemporaryRedirect, "/tm/tasks/1/retry"},
		// An escaped slash is part of a name, here of a static file that
		// does not exist, and not of the path's shape.
		{http.MethodGet, "/tm/static/a%2F%2Fb", http.StatusNotFound, ""},
		// A path that only starts like the prefix is not under it.
		{http.MethodGet, "/tmx/tasks", http.StatusNotFound, ""},
		{http.MethodGet, "/tasks", http.StatusNotFound, ""},
	} {
		req := httptest.NewRequestWithContext(t.Context(), tc.method, tc.target, nil)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if got := rec.Header().Get("Location"); rec.Code != tc.code || tc.location != "" && got != tc.location {
			t.Errorf("%s %s: got %d to %q, want %d to %q", tc.method, tc.target, rec.Code, got, tc.code, tc.location)
		}
	}
}

// Every page, also one that reports an error, carries the headers that keep
// the dashboard from being framed, sniffed, or cached, and from loading or
// sending anything outside itself.
func TestSecurityHeaders(t *testing.T) {
	t.Parallel()
	h := handler(t, setup(t), nil)
	for _, target := range []string{"/", "/tasks?state=bogus"} {
		_, _, header := get(t, h, target)
		for name, want := range map[string]string{
			"Content-Security-Policy": "default-src 'none'; style-src 'self'; script-src 'self'; img-src 'self'; connect-src 'self'; " +
				"form-action 'self'; base-uri 'none'; frame-ancestors 'none'",
			"X-Frame-Options":        "DENY",
			"X-Content-Type-Options": "nosniff",
			"Referrer-Policy":        "same-origin",
			"Cache-Control":          "no-store",
		} {
			if got := header.Get(name); got != want {
				t.Errorf("%s: the header %s is %q, want %q", target, name, got, want)
			}
		}
	}
}

func TestFrameAncestors(t *testing.T) {
	t.Parallel()
	client := setup(t)
	h := handler(t, client, &ui.Config{FrameAncestors: []string{"'self'", "https://admin.example.com"}})
	_, _, header := get(t, h, "/workers")
	if csp := header.Get("Content-Security-Policy"); !strings.Contains(csp, "frame-ancestors 'self' https://admin.example.com") ||
		header.Get("X-Frame-Options") != "" {
		t.Fatalf("got %v", header)
	}
}

// New refuses a configuration that would serve the interface wrongly, with
// an error rather than a panic.
func TestNewChecksTheConfig(t *testing.T) {
	t.Parallel()
	client := setup(t)
	for name, c := range map[string]struct {
		client *taskmill.Client
		cfg    *ui.Config
	}{
		"no client":            {nil, nil},
		"prefix not a path":    {client, &ui.Config{Prefix: "taskmill"}},
		"no password":          {client, &ui.Config{BasicAuth: &ui.BasicAuth{Username: "ops"}}},
		"no username":          {client, &ui.Config{BasicAuth: &ui.BasicAuth{Password: "s3cret"}}},
		"colon in username":    {client, &ui.Config{BasicAuth: &ui.BasicAuth{Username: "ops:team", Password: "s3cret"}}},
		"line break":           {client, &ui.Config{BasicAuth: &ui.BasicAuth{Username: "ops", Password: "s3cret\n"}}},
		"injected directive":   {client, &ui.Config{FrameAncestors: []string{"https://a.example; script-src *"}}},
		"empty frame ancestor": {client, &ui.Config{FrameAncestors: []string{""}}},
	} {
		if h, err := ui.New(c.client, c.cfg); h != nil || !errors.Is(err, taskmill.ErrInvalidArgument) {
			t.Errorf("%s: got %v, %v; want ErrInvalidArgument", name, h, err)
		}
	}
}

// Every static file of a page is in the folder of the current version,
// which may be cached for a year, and has a type that the browser takes for
// it, whatever the host's tables say. A page from another version of the
// server links another folder: it gets the current files, which must not be
// cached as that version.
func TestStaticFilesAreVersionedAndCached(t *testing.T) {
	t.Parallel()
	client := setup(t)
	h := handler(t, client, nil)
	_, body, _ := get(t, h, "/workers")
	m := regexp.MustCompile(`href="/static/([0-9a-f]+)/style\.css"`).FindStringSubmatch(body)
	if m == nil {
		t.Fatalf("no versioned stylesheet link in\n%s", body)
	}
	version := m[1]

	paths := regexp.MustCompile(`(?:src|href)="(/static/[^"]*)"`).FindAllStringSubmatch(body, -1)
	for _, p := range paths {
		code, _, header := get(t, h, p[1])
		if !strings.HasPrefix(p[1], "/static/"+version+"/") || code != http.StatusOK ||
			!strings.Contains(header.Get("Cache-Control"), "immutable") {
			t.Errorf("%s: %d, %v; want it in the folder of version %s, and immutable", p[1], code, header, version)
		}
		if typ := header.Get("Content-Type"); typ == "" ||
			strings.HasSuffix(p[1], ".js") && typ != "text/javascript; charset=utf-8" {
			t.Errorf("%s is %q, not a type that a browser takes for it", p[1], typ)
		}
	}

	_, current, _ := get(t, h, "/static/"+version+"/style.css")
	code, other, header := get(t, h, "/static/00000000/style.css")
	if code != http.StatusOK || header.Get("Cache-Control") != "no-cache" || other != current {
		t.Fatalf("stylesheet of another version: %d, %v; want the current one, not cached", code, header)
	}
}

// The page loads the script's entry, app.js, as a module, and preloads
// every module that it imports, directly or not, so that the browser
// fetches them all at the same time, not each after the module that
// imports it. theme.js is a classic script, which runs before the page
// paints.
func TestPagePreloadsTheModulesOfTheScript(t *testing.T) {
	t.Parallel()
	_, body, _ := get(t, handler(t, setup(t), nil), "/workers")
	scripts := regexp.MustCompile(`<script (type="module" )?src="/static/[0-9a-f]+/([a-z]+\.js)"></script>`).FindAllStringSubmatch(body, -1)
	if len(scripts) != 2 || scripts[0][1] != "" || scripts[0][2] != "theme.js" || scripts[1][1] == "" || scripts[1][2] != "app.js" {
		t.Fatalf("the scripts of the page are %q, want theme.js, then the module app.js", scripts)
	}

	var preloaded []string
	for _, m := range regexp.MustCompile(`<link rel="modulepreload" href="/static/[0-9a-f]+/([a-z]+\.js)">`).FindAllStringSubmatch(body, -1) {
		preloaded = append(preloaded, m[1])
	}

	static := os.DirFS("static")
	imported := map[string]bool{}
	var follow func(name string)
	follow = func(name string) {
		source, err := fs.ReadFile(static, name)
		if err != nil {
			t.Fatal(err)
		}
		for _, m := range regexp.MustCompile(`from "\./([a-z]+\.js)";`).FindAllStringSubmatch(string(source), -1) {
			if !imported[m[1]] {
				imported[m[1]] = true
				follow(m[1])
			}
		}
	}
	follow("app.js")
	want := slices.Sorted(maps.Keys(imported))
	if !slices.Equal(preloaded, want) {
		t.Errorf("the page preloads %q, want the modules that app.js imports: %q", preloaded, want)
	}
}
