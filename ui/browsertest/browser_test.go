package browsertest

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"net/http"
	"net/http/httptest"
	"os"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/chromedp/cdproto/cdp"
	"github.com/chromedp/cdproto/input"
	"github.com/chromedp/cdproto/log"
	cdppage "github.com/chromedp/cdproto/page"
	"github.com/chromedp/cdproto/runtime"
	"github.com/chromedp/chromedp"
	"github.com/chromedp/chromedp/kb"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/code19m/taskmill"
	"github.com/code19m/taskmill/internal/testdb"
	"github.com/code19m/taskmill/ui"
)

// envChrome names the Chrome or Chromium that the tests run. `make
// test-browser` sets it to the first one installed. Without it, the tests
// skip: `make test`, which runs the tests of every module, leaves them to
// that target.
const envChrome = "TASKMILL_TEST_CHROME"

// zone is the time zone of the browser: 5 hours and 45 minutes from UTC, so
// that a time that the script words in the reader's zone shows that it does.
const zone = "Asia/Kathmandu"

// browser is the Chrome that every test opens its tab in; nil without one.
var browser context.Context

func TestMain(m *testing.M) { os.Exit(run(m)) }

// run starts Chrome, if the environment names one, and runs the tests.
func run(m *testing.M) int {
	chrome := os.Getenv(envChrome)
	if chrome == "" {
		return m.Run()
	}

	opts := append(chromedp.DefaultExecAllocatorOptions[:],
		chromedp.ExecPath(chrome), chromedp.Env("TZ="+zone), chromedp.WindowSize(1280, 900))
	if os.Getenv("CI") != "" {
		// On GitHub's Ubuntu runners, Chrome's sandbox lacks the user
		// namespaces that it needs. The tests open only their own pages.
		opts = append(opts, chromedp.NoSandbox)
	}
	alloc, cancel := chromedp.NewExecAllocator(context.Background(), opts...)
	defer cancel()
	ctx, stop := chromedp.NewContext(alloc)
	defer stop()
	if err := chromedp.Do(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "browsertest: start %s: %v\n", chrome, err)
		return 1
	}

	browser = ctx //nolint:fatcontext // the tests share one Chrome, which run starts and stops
	return m.Run()
}

// site is the dashboard of a test database, served for one test.
type site struct {
	url    string
	client *taskmill.Client
	pool   *pgxpool.Pool
	schema string

	// old has the site serve its pages as a server of another version
	// rendered them: with their static files in the folder of that version.
	old atomic.Bool
	// missing names a static file that the site answers 404 for, as a
	// server of 0.15 does for every module during a rolling deploy.
	missing atomic.Value // string

	mu    sync.Mutex
	live  []liveRequest // the requests of the script for a page, in the order of their answers
	files []string      // the paths of the static files that the browser asked for
}

// liveRequest is a request of the script for a page: a live update, or a
// page that it shows in place.
type liveRequest struct {
	path   string
	asked  bool // it named the copy that the page shows, by its ETag
	status int
}

// needChrome skips the test without Chrome.
func needChrome(t *testing.T) {
	t.Helper()
	if browser == nil {
		t.Skipf("%s is not set: run the browser tests with `make test-browser`, which finds Chrome", envChrome)
	}
}

// newSite serves the dashboard of a fresh schema. It skips the test without
// Chrome.
func newSite(t *testing.T) *site {
	t.Helper()
	return newSiteWith(t, nil)
}

// newSiteWith serves the dashboard of a fresh schema, configured by cfg. It
// skips the test without Chrome.
func newSiteWith(t *testing.T, cfg *ui.Config) *site {
	t.Helper()
	needChrome(t)
	pool := testdb.Pool(t)
	schema := testdb.Schema(t, pool)
	if err := taskmill.Migrate(t.Context(), pool, &taskmill.MigrateConfig{Schema: schema}); err != nil {
		t.Fatal(err)
	}
	client, err := taskmill.NewClient(t.Context(), pool, &taskmill.ClientConfig{Schema: schema})
	if err != nil {
		t.Fatal(err)
	}
	h, err := ui.New(client, cfg)
	if err != nil {
		t.Fatal(err)
	}

	s := &site{client: client, pool: pool, schema: schema}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/static/") {
			s.mu.Lock()
			s.files = append(s.files, r.URL.Path)
			s.mu.Unlock()
			if name, _ := s.missing.Load().(string); name != "" && strings.HasSuffix(r.URL.Path, "/"+name) {
				http.NotFound(w, r)
				return
			}
		} else if s.old.Load() && r.Header.Get("X-Taskmill-Live") == "" {
			serveOld(h, w, r)
			return
		}

		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		h.ServeHTTP(rec, r)
		if r.Method == http.MethodGet && r.Header.Get("X-Taskmill-Live") != "" {
			s.mu.Lock()
			s.live = append(s.live, liveRequest{r.URL.RequestURI(), r.Header.Get("If-None-Match") != "", rec.status})
			s.mu.Unlock()
		}
	}))
	t.Cleanup(srv.Close)
	s.url = srv.URL
	return s
}

// oldVersion is the version of the static files of the site's pages while
// it serves them as a server of another version rendered them.
const oldVersion = "00000000"

// staticFolder is the folder of a version of the static files, in a page.
var staticFolder = regexp.MustCompile(`/static/[0-9a-f]+/`)

// serveOld serves the page that h answers r with, as a server of another
// version rendered it: its static files are in the folder of oldVersion.
func serveOld(h http.Handler, w http.ResponseWriter, r *http.Request) {
	r.Header.Del("Accept-Encoding") // the page is rewritten as text
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, r)
	page := staticFolder.ReplaceAll(rec.Body.Bytes(), []byte("/static/"+oldVersion+"/"))
	maps.Copy(w.Header(), rec.Header())
	w.Header().Set("Content-Length", strconv.Itoa(len(page)))
	w.WriteHeader(rec.Code)
	_, _ = w.Write(page)
}

// staticFiles returns the paths of the static files that the browser asked
// for, so far.
func (s *site) staticFiles() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.files)
}

// exec runs a statement on the site's schema, in which {s} stands for the
// schema's name.
func (s *site) exec(t *testing.T, sql string, args ...any) {
	t.Helper()
	if _, err := s.pool.Exec(t.Context(), strings.ReplaceAll(sql, "{s}", s.schema), args...); err != nil {
		t.Fatal(err)
	}
}

// waitLive waits for a request of the script for a page that matches, among
// those after the first skip, and fails the test after 10 seconds.
func (s *site) waitLive(t *testing.T, skip int, match func(liveRequest) bool) {
	t.Helper()
	for deadline := time.Now().Add(10 * time.Second); ; time.Sleep(20 * time.Millisecond) {
		s.mu.Lock()
		found := slices.ContainsFunc(s.live[min(skip, len(s.live)):], match)
		s.mu.Unlock()
		if found {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("no such request of the script within 10 seconds; it made %+v", s.requests())
		}
	}
}

// requests returns the requests of the script for a page, so far.
func (s *site) requests() []liveRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.live)
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

// page is a tab of Chrome.
type page struct {
	t   *testing.T
	ctx context.Context
}

// newTab opens a tab, closed when the test ends. It fails the test if a
// script threw an exception in the tab or logged an error, if a static file
// did not load, or if the content security policy refused something. A
// request of the script for a page may fail, as a test may want it to. It
// skips the test without Chrome.
func newTab(t *testing.T) *page {
	t.Helper()
	needChrome(t)
	var mu sync.Mutex
	var errs []string
	ctx, cancel := chromedp.NewContext(browser, chromedp.WithListener(func(ev any) {
		var msg string
		switch e := ev.(type) {
		case *runtime.EventExceptionThrown:
			msg = "a script threw: " + (&chromedp.ExceptionError{ExceptionDetails: e.ExceptionDetails}).Error()
		case *runtime.EventConsoleAPICalled:
			if e.Type == runtime.ConsoleAPICalledTypeError || e.Type == runtime.ConsoleAPICalledTypeAssert {
				msg = fmt.Sprintf("a script logged a %s", e.Type)
			}
		case *log.EventEntryAdded:
			// Chrome logs every failed request, such as a page that a test
			// makes the server answer with 503. Only a static file that
			// does not load, by its URL, or a refusal of the content
			// security policy, by its source, is an error of the page.
			if e.Entry.Level == log.EntryLevelError &&
				(e.Entry.Source == log.EntrySourceSecurity || strings.Contains(e.Entry.URL, "/static/")) {
				msg = fmt.Sprintf("the browser logged an error: %s (%s)", e.Entry.Text, e.Entry.URL)
			}
		}
		if msg != "" {
			mu.Lock()
			errs = append(errs, msg)
			mu.Unlock()
		}
	}))
	t.Cleanup(func() {
		cancel()
		mu.Lock()
		defer mu.Unlock()
		for _, e := range errs {
			t.Error(e)
		}
	})
	return &page{t, ctx}
}

// open opens a tab at the path of the site.
func (s *site) open(t *testing.T, path string) *page {
	t.Helper()
	p := newTab(t)
	p.do(chromedp.Navigate(s.url + path))
	return p
}

func (p *page) do(actions ...chromedp.Action[chromedp.Void]) {
	p.t.Helper()
	if err := chromedp.Do(p.ctx, actions...); err != nil {
		p.t.Fatal(err)
	}
}

// eval returns the value of a JavaScript expression on the page.
func eval[T any](p *page, expr string) T {
	p.t.Helper()
	v, err := chromedp.Run(p.ctx, chromedp.Evaluate[T](expr))
	if err != nil {
		p.t.Fatalf("%s: %v", expr, err)
	}
	return v
}

// run runs JavaScript statements on the page.
func (p *page) run(js string) {
	p.t.Helper()
	eval[chromedp.Void](p, "{"+js+"}")
}

// waitFor waits until a JavaScript expression is true on the page, and fails
// the test after 10 seconds, saying that what did not happen.
func (p *page) waitFor(expr, what string) {
	p.t.Helper()
	for deadline := time.Now().Add(10 * time.Second); !eval[bool](p, "!!("+expr+")"); time.Sleep(20 * time.Millisecond) {
		if time.Now().After(deadline) {
			p.t.Fatalf("within 10 seconds, %s did not happen", what)
		}
	}
}

// middle returns the middle of the element that selector finds, in the
// window. It scrolls the element into view first if it is out of it.
func (p *page) middle(selector string) (x, y float64) {
	p.t.Helper()
	at := eval[[]float64](p, `(() => {
		const el = document.querySelector(`+js(selector)+`);
		let box = el.getBoundingClientRect();
		if (box.top < 0 || box.bottom > innerHeight) {
			el.scrollIntoView({ block: "center" });
			box = el.getBoundingClientRect();
		}
		return [box.x + box.width / 2, box.y + box.height / 2];
	})()`)
	return at[0], at[1]
}

// click presses and releases the mouse over the element that selector finds.
func (p *page) click(selector string) {
	p.t.Helper()
	x, y := p.middle(selector)
	p.do(chromedp.MouseClickXY(x, y))
}

// press presses and releases a key, such as kb.Enter, as a keyboard does:
// its keydown and its keypress come in one event, so no task of the page
// runs between them. chromedp.KeyEvent sends the keypress as an event of
// its own, and an answer that comes before it can move the focus: Enter on
// a link that opens the dialog would also press the dialog's button.
func (p *page) press(key string) {
	p.t.Helper()
	p.do(chromedp.Func(func(ctx context.Context, t *chromedp.Target) error {
		for _, r := range key {
			var events []*input.DispatchKeyEventParams
			for _, e := range kb.Encode(r) {
				if e.Type == kb.KeyChar { // the text of the keydown before it
					down := events[len(events)-1]
					down.Text, down.UnmodifiedText = e.Text, e.UnmodifiedText
					continue
				}
				events = append(events, e)
			}
			for _, e := range events {
				if _, err := cdp.Call(ctx, t, input.DispatchKeyEvent, *e); err != nil {
					return err
				}
			}
		}
		return nil
	}))
}

// hover moves the mouse over the element that selector finds.
func (p *page) hover(selector string) {
	p.t.Helper()
	x, y := p.middle(selector)
	p.do(chromedp.MouseEvent(chromedp.MouseMoved, x, y))
}

// history goes delta entries back, if negative, or forward in the tab's
// history, as the back and forward buttons do.
func (p *page) history(delta int) {
	p.t.Helper()
	entries, err := chromedp.Run(p.ctx, chromedp.NavigationEntries())
	if err != nil {
		p.t.Fatal(err)
	}

	i := entries.CurrentIndex + int64(delta)
	if i < 0 || i >= int64(len(entries.Entries)) {
		p.t.Fatalf("no entry %d away in the history, which has %d", delta, len(entries.Entries))
	}
	p.do(chromedp.Func(func(ctx context.Context, t *chromedp.Target) error {
		_, err := cdp.Call(ctx, t, cdppage.NavigateToHistoryEntry, cdppage.NavigateToHistoryEntryParams{EntryID: entries.Entries[i].ID})
		return err
	}))
}

// update has the page update itself now, as it does every 5 seconds: the
// browser says that it is online again, after which the script updates at
// once.
func (p *page) update() {
	p.t.Helper()
	p.run(`dispatchEvent(new Event("online"))`)
}

// js writes v as a JavaScript value.
func js(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err) // the tests write only values that JSON holds
	}
	return string(b)
}
