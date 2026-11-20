package ui_test

import (
	"compress/gzip"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"

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

	// A page names its time of rendering, which is no part of what it says:
	// a later copy that says the same is the page that the client has.
	rendered := regexp.MustCompile(` data-now="[^"]+"`)
	for later := plain; rendered.FindString(later) == rendered.FindString(plain); {
		if _, later, header = get(t, h, "/workers"); header.Get("ETag") != etag {
			t.Fatalf("a later copy of an unchanged page has the ETag %q, want %q", header.Get("ETag"), etag)
		}
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

func TestBasicAuth(t *testing.T) {
	t.Parallel()
	client := setup(t)
	h := handler(t, client, &ui.Config{BasicAuth: &ui.BasicAuth{Username: "ops", Password: "s3cret"}})

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
		"injected directive":   {client, &ui.Config{FrameAncestors: []string{"https://a.example; script-src *"}}},
		"empty frame ancestor": {client, &ui.Config{FrameAncestors: []string{""}}},
	} {
		if h, err := ui.New(c.client, c.cfg); h != nil || !errors.Is(err, taskmill.ErrInvalidArgument) {
			t.Errorf("%s: got %v, %v; want ErrInvalidArgument", name, h, err)
		}
	}
}

func TestStaticFilesAreVersionedAndCached(t *testing.T) {
	t.Parallel()
	client := setup(t)
	h := handler(t, client, nil)
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
