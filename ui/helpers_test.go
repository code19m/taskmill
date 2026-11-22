package ui_test

import (
	"context"
	"html"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/code19m/taskmill"
	"github.com/code19m/taskmill/internal/testdb"
	"github.com/code19m/taskmill/ui"
)

// TestMain runs the tests in a process far from UTC: see testdb.Main.
func TestMain(m *testing.M) { testdb.Main(m) }

type greet struct {
	Name string `json:"name"`
}

func (greet) Kind() string { return "greet" }

func setup(tb testing.TB) *taskmill.Client {
	tb.Helper()
	client, _, _ := setupDB(tb)
	return client
}

// keepQueue gives a queue settings that the tests never reach, a limit of
// 1,000 running tasks, so that the queue is stored and the overview lists
// it, also after a pause and a resume.
func keepQueue(ctx context.Context, client *taskmill.Client, name string) error {
	return client.UpdateQueue(ctx, name, func(s *taskmill.QueueSettings) { s.MaxRunning = 1000 })
}

// setupDB migrates a fresh schema, and returns a client for it, with its
// pool and name. When the test ends, it checks the schema's FIFO groups.
func setupDB(tb testing.TB) (*taskmill.Client, *pgxpool.Pool, string) {
	tb.Helper()
	return setupOn(tb, testdb.Pool(tb))
}

// setupOn is setupDB on a given pool.
func setupOn(tb testing.TB, pool *pgxpool.Pool) (*taskmill.Client, *pgxpool.Pool, string) {
	tb.Helper()
	schema := testdb.Schema(tb, pool)
	if err := taskmill.Migrate(tb.Context(), pool, &taskmill.MigrateConfig{Schema: schema}); err != nil {
		tb.Fatal(err)
	}

	client, err := taskmill.NewClient(tb.Context(), pool, &taskmill.ClientConfig{Schema: schema})
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() {
		if !tb.Failed() {
			testdb.CheckGroups(tb, pool, schema)
		}
	})
	return client, pool, schema
}

// setupTraced is setupDB with a pool of its own, whose connections report
// their statements to tracer.
func setupTraced(t *testing.T, tracer pgx.QueryTracer) (*taskmill.Client, *pgxpool.Pool, string) {
	t.Helper()
	cfg, err := pgxpool.ParseConfig(testdb.URL(t))
	if err != nil {
		t.Fatal(err)
	}

	cfg.ConnConfig.Tracer = tracer
	pool, err := pgxpool.NewWithConfig(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return setupOn(t, pool)
}

// handler returns the interface for client, and fails the test if cfg is
// refused.
func handler(tb testing.TB, client *taskmill.Client, cfg *ui.Config) http.Handler {
	tb.Helper()
	h, err := ui.New(client, cfg)
	if err != nil {
		tb.Fatal(err)
	}
	return h
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

// storeSchedules runs a worker until it has stored the schedules, which it
// does in its first maintenance cycle.
func storeSchedules(t *testing.T, client *taskmill.Client, schedules []taskmill.Schedule) {
	t.Helper()
	w, err := taskmill.NewWorker(client, &taskmill.WorkerConfig{Schedules: schedules})
	if err != nil {
		t.Fatal(err)
	}

	ctx, stop := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	for deadline := time.Now().Add(15 * time.Second); len(schedulesThat(t, client, nil)) < len(schedules); time.Sleep(20 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("the schedules were not stored")
		}
	}

	stop()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

// schedulesThat returns the names of the schedules that match, or of all
// schedules without a match.
func schedulesThat(t *testing.T, client *taskmill.Client, match func(taskmill.ScheduleInfo) bool) []string {
	t.Helper()
	stored, err := client.Schedules(t.Context())
	if err != nil {
		t.Fatal(err)
	}

	var names []string
	for _, sc := range stored {
		if match == nil || match(sc) {
			names = append(names, sc.Name)
		}
	}
	return names
}

// pausedQueues returns the names of the queues that are paused.
func pausedQueues(t *testing.T, client *taskmill.Client) []string {
	t.Helper()
	queues, err := client.Queues(t.Context())
	if err != nil {
		t.Fatal(err)
	}

	var names []string
	for _, q := range queues {
		if q.Paused {
			names = append(names, q.Name)
		}
	}
	return names
}

var (
	tableRow   = regexp.MustCompile(`(?s)<tr data-key="([^"]*)">(.*?)</tr>`)
	rowButton  = regexp.MustCompile(`(?s)<form method="post" action="([^"]*)"[^>]*><button[^>]*><svg.*?</svg>([^<]*)</button></form>`)
	rowAsks    = regexp.MustCompile(`(?s)<a class="btn[^"]*" href="([^"]*)" data-dialog><svg.*?</svg>([^<]*)</a>`)
	askingForm = regexp.MustCompile(`<form class="bulk-actions" method="post" action="([^"]*)">`)
)

// rowForms returns where each button in the table row of name sends its
// action, on the page at target, by the button's text. A button that asks
// first is a link to the page that asks, whose form sends the action. It
// reads each address from the page that holds it, and resolves it against
// that page's address, as a browser does. It reads the pages that ask, and
// they change nothing.
func rowForms(t *testing.T, h http.Handler, target, name string) map[string]string {
	t.Helper()
	resolve := func(page, ref string) *url.URL {
		t.Helper()
		base, err := url.Parse(page)
		if err != nil {
			t.Fatal(err)
		}
		u, err := base.Parse(html.UnescapeString(ref))
		if err != nil {
			t.Fatalf("%s: the address %q in the row of %q: %v", page, ref, name, err)
		}
		return u
	}

	_, body, _ := get(t, h, target)
	found := map[string]string{}
	for _, row := range tableRow.FindAllStringSubmatch(body, -1) {
		if html.UnescapeString(row[1]) != name {
			continue
		}
		for _, form := range rowButton.FindAllStringSubmatch(row[2], -1) {
			found[form[2]] = resolve(target, form[1]).RequestURI()
		}
		for _, link := range rowAsks.FindAllStringSubmatch(row[2], -1) {
			asking := resolve(target, link[1]).RequestURI()
			code, page, _ := get(t, h, asking)
			form := askingForm.FindStringSubmatch(page)
			if code != http.StatusOK || form == nil {
				t.Fatalf("%s, the page that asks before %s on %q: status %d, and no form\n%s", asking, link[2], name, code, page)
			}
			found[link[2]] = resolve(asking, form[1]).RequestURI()
		}
	}
	return found
}

// send posts a form to its action, from the page's own origin, and checks
// the status of the answer.
func send(t *testing.T, h http.Handler, action string, want int) {
	t.Helper()
	if rec := post(t, h, action, "same-origin"); rec.Code != want {
		t.Fatalf("POST %q: status %d, want %d", action, rec.Code, want)
	}
}
