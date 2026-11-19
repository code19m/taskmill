package ui_test

import (
	"compress/gzip"
	"context"
	"errors"
	"html"
	"io"
	"log/slog"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
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

// TestPagesHoldWhatTheScriptReads checks the parts of a page that the script
// finds by name. Without one of them, it loads full pages where it could show
// one in place, cannot say that an action failed, or cannot ask in its
// dialog. The pages of an action on many tasks, and those that say why
// something failed, also name the title of the dialog that shows them.
func TestPagesHoldWhatTheScriptReads(t *testing.T) {
	t.Parallel()
	h := handler(t, setup(t), nil)
	for path, title := range map[string]string{
		"/": "", "/tasks": "", "/schedules": "", "/workers": "",
		"/tasks/0":                     `data-dialog-title="Not found"`,
		"/tasks/bulk/retry?state=dead": `data-dialog-title="Retry tasks"`,
		"/tasks/bulk/retry":            `data-dialog-title="Choose filters first"`,
	} {
		_, body, _ := get(t, h, path)
		for _, part := range []string{
			`<main id="main"`, ` data-now="`, `<div class="main">`, `data-live-part="nav"`, "data-announce",
			"<template data-failure>", `id="confirm-dialog"`, "data-confirm-status", "data-confirm-text", "data-confirm-button",
			title,
		} {
			if !strings.Contains(body, part) {
				t.Errorf("%s: the page has no %s", path, part)
			}
		}
		if title == "" && strings.Contains(body, "data-dialog-title") {
			t.Errorf("%s: the page names a title for the dialog, which would show it in place of the page", path)
		}
	}
}

// Every icon that a page uses is one that the page defines: a name that the
// sprite does not have draws nothing, and nothing says so. And every button
// icon that the sprite defines is on some page here, so the pages of this
// test reach every button that has one.
func TestPagesDefineTheIconsTheyUse(t *testing.T) {
	t.Parallel()
	client, pool, schema := setupDB(t)
	ctx := t.Context()
	storeSchedules(t, client, []taskmill.Schedule{
		{Name: "nightly", Cron: "@daily", Args: greet{}}, {Name: "weekly", Cron: "@weekly", Args: greet{}},
	})
	// A paused schedule and queue, and one of each that runs; a task that
	// waits; and more dead tasks than a list shows, so that it continues.
	if err := errors.Join(
		client.PauseSchedule(ctx, "weekly"),
		client.PauseQueue(ctx, "mail"),
		client.UpdateQueue(ctx, "default", func(*taskmill.QueueSettings) {}),
	); err != nil {
		t.Fatal(err)
	}
	waiting, err := client.Enqueue(ctx, greet{Name: "a"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO `+schema+`.tasks (queue, kind, state, run_at, payload, max_attempts, finalized_at)
		SELECT 'mail', 'greet', 'dead', now(), '{}', 1, now() FROM generate_series(1, 260)`); err != nil {
		t.Fatal(err)
	}
	h := handler(t, client, nil)
	body, dead := listTasks(t, h, "/tasks?limit=250&state=dead")
	pages := []string{
		"/", "/schedules", "/workers", "/tasks?state=dead", "/tasks?limit=250&state=dead", linkNamed(t, body, "Earlier"),
		"/tasks/" + dead[0], "/tasks/" + strconv.FormatInt(waiting.ID, 10),
		"/tasks/bulk/delete?state=dead", "/tasks/bulk/retry?state=running", "/tasks?state=none",
	}

	used := map[string]bool{}
	for _, page := range pages {
		_, body, _ := get(t, h, page)
		for _, m := range regexp.MustCompile(`<use href="#([^"]+)"`).FindAllStringSubmatch(body, -1) {
			used[m[1]] = true
			if !strings.Contains(body, `<symbol id="`+m[1]+`"`) {
				t.Errorf("%s uses the icon %q, which the page does not define", page, m[1])
			}
		}
	}
	for _, m := range regexp.MustCompile(`<symbol id="(i-[^"]+)"`).FindAllStringSubmatch(body, -1) {
		if !used[m[1]] {
			t.Errorf("no page of this test uses the icon %q: nothing uses it, or the test no longer reaches its button", m[1])
		}
	}
}

// Every custom property that the stylesheet uses is one that it defines: a
// name that it does not have, such as one that was renamed, silently gives
// the browser's default, as a box with square corners. And every one that
// it defines is used, so that no name of an old design stays behind.
func TestStylesDefineThePropertiesTheyUse(t *testing.T) {
	t.Parallel()
	code, sheet, _ := get(t, handler(t, setup(t), nil), "/static/style.css")
	if code != http.StatusOK {
		t.Fatalf("stylesheet: %d", code)
	}
	defined, used := map[string]bool{}, map[string]bool{}
	for _, m := range regexp.MustCompile(`[{;\s](--[a-z0-9-]+)\s*:`).FindAllStringSubmatch(sheet, -1) {
		defined[m[1]] = true
	}
	for _, m := range regexp.MustCompile(`var\(\s*(--[a-z0-9-]+)`).FindAllStringSubmatch(sheet, -1) {
		used[m[1]] = true
		if !defined[m[1]] {
			t.Errorf("the stylesheet uses %s, which it does not define", m[1])
		}
	}
	if len(used) == 0 {
		t.Fatal("the stylesheet uses no custom property in the form that this test reads")
	}
	for name := range defined {
		if !used[name] {
			t.Errorf("the stylesheet defines %s, which nothing uses", name)
		}
	}
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
	if err := client.UpdateQueue(ctx, "mail", func(s *taskmill.QueueSettings) { s.MaxRunning = 3 }); err != nil {
		t.Fatal(err)
	}
	h := handler(t, client, &ui.Config{Prefix: "/admin/tasks"})
	id := strconv.FormatInt(res.ID, 10)

	code, body, _ := get(t, h, "/admin/tasks/")
	if code != http.StatusOK || !strings.Contains(body, "mail") || !strings.Contains(body, "max 3") {
		t.Fatalf("overview: %d\n%s", code, body)
	}
	// A card that lists things says how many, beside its title.
	if want := `<h2 id="queues-title">Queues <span class="subtle">1</span></h2>`; !strings.Contains(body, want) {
		t.Errorf("overview: want %s in\n%s", want, body)
	}
	for _, path := range []string{"/admin/tasks/schedules", "/admin/tasks/workers"} {
		if code, _, _ := get(t, h, path); code != http.StatusOK {
			t.Fatalf("%s: %d", path, code)
		}
	}

	code, body, _ = get(t, h, "/admin/tasks/tasks?queue=mail&state=available")
	if code != http.StatusOK || !strings.Contains(body, `href="/admin/tasks/tasks/`+id+`"`) ||
		!strings.Contains(body, `title="Priority: high">high</span>`) {
		t.Fatalf("task list: want the task with its priority: %d\n%s", code, body)
	}
	code, body, _ = get(t, h, "/admin/tasks/tasks/"+id)
	if code != http.StatusOK || strings.Contains(body, "<script>alert") || !strings.Contains(body, "&lt;script&gt;") {
		t.Fatalf("task page did not escape the payload: %d\n%s", code, body)
	}
	if !strings.Contains(body, "<dd>high</dd>") || !strings.Contains(body, `title="Outbox topic topic, key key"`) ||
		!strings.Contains(body, "group=topic%1Fkey") || !strings.Contains(body, `<h2>Errors <span class="subtle">0</span></h2>`) {
		t.Fatalf("task page: want the priority's name, a visible link to the group, and the count of its errors\n%s", body)
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

// A time shows in UTC, whatever the time zones of the database's sessions,
// of the dashboard's process, and of the time that the application passed:
// the tests run with three zones that are not UTC. Beside the time is its
// instant, by which the script shows it in the reader's time zone.
func TestTimesShowInUTC(t *testing.T) {
	t.Parallel()
	client := setup(t)
	runAt := time.Date(2031, time.March, 4, 5, 6, 7, 89_000_000, time.FixedZone("", -(9*60+30)*60))
	res, err := client.Enqueue(t.Context(), greet{}, &taskmill.EnqueueOptions{RunAt: runAt})
	if err != nil {
		t.Fatal(err)
	}
	_, body, _ := get(t, handler(t, client, nil), "/tasks/"+strconv.FormatInt(res.ID, 10))
	if want := `title="2031-03-04 14:36:07.089 UTC" data-time="2031-03-04T14:36:07.089Z"`; !strings.Contains(body, want) {
		t.Errorf("the page of a task that runs at %s does not show %s", runAt.Format(time.RFC3339Nano), want)
	}
}

// A page says most times by how long ago they were, or how far ahead they
// are. Each of these has the exact time as its title, with its instant: the
// tooltip that says when, in the reader's time zone with the script. The
// axis of a chart is no such time: it is a scale that counts back from now,
// and the chart's guide tells the time of each step.
func TestRelativeTimesHaveTheirExactTime(t *testing.T) {
	t.Parallel()
	client, pool, schema := setupDB(t)
	// What these pages tell the time of was minutes ago, or is minutes ahead.
	storeCounts(t, pool, schema, "5 minutes", false)
	var id int64
	if err := pool.QueryRow(t.Context(), `
	    WITH schedule AS (
	        INSERT INTO `+schema+`.schedules (name, queue, kind, cron, timezone, next_run_at, last_run_at)
	        VALUES ('nightly', 'mail', 'greet', '0 3 * * *', 'UTC', now() + interval '3 minutes', now() - interval '4 minutes')
	    ), worker AS (
	        INSERT INTO `+schema+`.workers (id, queues, hostname, version, started_at, heartbeat_at)
	        VALUES ('w', '{mail}', 'h', 'v', now() - interval '6 minutes', now() - interval '2 minutes')
	    )
	    INSERT INTO `+schema+`.tasks (queue, kind, state, payload, attempt, max_attempts, created_at, run_at, attempted_at, finalized_at, errors)
	    VALUES ('mail', 'greet', 'dead', '{}', 1, 1, now() - interval '9 minutes', now() - interval '8 minutes',
	        now() - interval '7 minutes', now() - interval '7 minutes',
	        jsonb_build_array(jsonb_build_object('attempt', 1, 'at', now() - interval '7 minutes', 'message', 'boom')))
	    RETURNING id`).Scan(&id); err != nil {
		t.Fatal(err)
	}
	h := handler(t, client, nil)
	var (
		axis     = regexp.MustCompile(`<div class="chart-x"[^>]*>.*?</div>`)
		relative = regexp.MustCompile(`<[^<>]*>(?:<svg[^<>]*>(?:<[^<>]*>)*</svg>)?[^<>]*\d+[smhd] (?:ago|from now)`) // with the tag that it stands in, and its icon
		exact    = regexp.MustCompile(`^<[^<>]* title="\d{4}-\d\d-\d\d \d\d:\d\d:\d\d\.\d{3} UTC" data-time="[^"]+Z">`)
	)
	for path, want := range map[string]int{
		"/":                                   1, // the age of the counts
		"/tasks":                              2, // created, and run at
		"/tasks?state=dead":                   2, // created, and finished
		"/tasks/" + strconv.FormatInt(id, 10): 5, // created, run at, last attempt, finished, and the error
		"/schedules":                          2, // the next run, and the last one
		"/workers":                            2, // started, and checked in
	} {
		_, body, _ := get(t, h, path)
		times := relative.FindAllString(axis.ReplaceAllString(body, ""), -1)
		if len(times) != want {
			t.Errorf("%s says %d times by how far they are from now, want %d: %q", path, len(times), want, times)
		}
		for _, said := range times {
			if !exact.MatchString(said) {
				t.Errorf("%s: a time has no exact time as its title: %s", path, said)
			}
		}
	}
}

// A page says when it was rendered, and words each relative time from that
// time and the time's own instant, both read to the second, as a clock
// shows them. The script counts from the same two instants, in the same
// words: a time that the server worded from another moment, or more
// exactly, would jump at each update.
func TestRelativeTimesAreWordedFromThePagesTimeOfRendering(t *testing.T) {
	t.Parallel()
	client, pool, schema := setupDB(t)
	// A worker that checked in half a minute ago, late in its second: the
	// seconds between the two clock readings are one more than the whole
	// seconds of its exact age, unless the page is rendered as late in its own.
	if _, err := pool.Exec(t.Context(), `INSERT INTO `+schema+`.workers (id, queues, hostname, version, started_at, heartbeat_at)
	    VALUES ('w', '{mail}', 'h', 'v', date_trunc('second', now()) - interval '1 hour',
	        date_trunc('second', now()) - interval '30 seconds' + interval '999 milliseconds')`); err != nil {
		t.Fatal(err)
	}
	_, body, _ := get(t, handler(t, client, nil), "/workers")
	page := regexp.MustCompile(`<main id="main"[^>]* data-now="([^"]+)"`).FindStringSubmatch(body)
	cell := regexp.MustCompile(` data-time="([^"]+\.999Z)">([^<]*)</td>`).FindStringSubmatch(body)
	if page == nil || cell == nil {
		t.Fatalf("the page does not say when it was rendered, or when the worker checked in:\n%s", body)
	}
	now, err := time.Parse(time.RFC3339, page[1])
	if err != nil {
		t.Fatal(err)
	}
	at, err := time.Parse(time.RFC3339, cell[1])
	if err != nil {
		t.Fatal(err)
	}
	if want := strconv.FormatInt(now.Unix()-at.Unix(), 10) + "s ago"; cell[2] != want {
		t.Errorf("a page rendered at %s says of %s: %q, want %q", page[1], cell[1], cell[2], want)
	}
}

func TestNavShowsTheStoredDeadCount(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		tasks  int64
		capped bool
		want   string
	}{
		// The maintainer stopped counting the dead tasks at the cap.
		{"capped", 10000, true, "10,000&#43;"},
		// No maintainer stores this count, but the column holds it, and a
		// page must show whatever the column holds.
		{"smallest", math.MinInt64, false, "-9,223,372,036,854,775,808"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			client, pool, schema := setupDB(t)
			if _, err := pool.Exec(t.Context(), `INSERT INTO `+schema+`.queue_stats (queue, state, tasks, capped, refreshed_at)
			    VALUES ('mail', 'dead', $1, $2, now())`, tc.tasks, tc.capped); err != nil {
				t.Fatal(err)
			}
			h := handler(t, client, nil)
			for _, path := range []string{"/", "/tasks?state=dead", "/workers"} {
				if _, body, _ := get(t, h, path); !strings.Contains(body, `<span>Dead</span><span class="count">`+tc.want+`</span>`) {
					t.Errorf("%s: want the nav's dead count as %s\n%s", path, tc.want, body)
				}
			}
		})
	}
}

// storeCounts stores the counts of a queue as the maintainer does, counted
// age ago: 3 dead tasks, or as many as the cap if the count stopped there.
func storeCounts(t *testing.T, pool *pgxpool.Pool, schema, age string, capped bool) {
	t.Helper()
	if _, err := pool.Exec(t.Context(), `INSERT INTO `+schema+`.queue_stats (queue, state, tasks, capped, refreshed_at)
	    VALUES ('mail', 'dead', $1, $2, now() - interval '`+age+`')`, map[bool]int{false: 3, true: 10000}[capped], capped); err != nil {
		t.Fatal(err)
	}
}

// The overview says when the maintainer last counted the tasks, at the end
// of its top bar: quiet while the count is fresh, and marked as stale once no
// maintainer has counted for over a minute, which is a reason to act. A
// count that stopped at its cap says so in its tooltip. The mark has a key,
// so that a live update keeps it as it is.
func TestOverviewSaysWhenItsCountsWereTaken(t *testing.T) {
	t.Parallel()
	const (
		fresh = `<span class="counted" data-key="counted" title="`
		late  = `<span class="counted stale" data-key="counted" title="`
		atCap = `title="At least 10,000 tasks: the count stopped at its cap">10,000&#43;<`
	)
	for _, tc := range []struct {
		name, age     string // the age of the counts; none are stored if it is empty
		capped        bool
		want, without []string
	}{
		{"not counted yet", "", false, nil, []string{`class="counted`, "ounted "}},
		{"counted now", "0 seconds", false, []string{fresh, "</svg>Counted "}, []string{late, "stopped at its cap", "hint-counts"}},
		{"at the cap", "0 seconds", true, []string{fresh, atCap}, []string{late}},
		{"not counted for a minute", "5 minutes", false, []string{late, "</svg>Counted 5m ago</span>"}, []string{fresh, "stopped at its cap"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			client, pool, schema := setupDB(t)
			if tc.age != "" {
				storeCounts(t, pool, schema, tc.age, tc.capped)
			}
			_, body, _ := get(t, handler(t, client, nil), "/")
			for _, want := range tc.want {
				if !strings.Contains(body, want) {
					t.Errorf("want %s in\n%s", want, body)
				}
			}
			for _, without := range tc.without {
				if strings.Contains(body, without) {
					t.Errorf("want no %s in\n%s", without, body)
				}
			}
		})
	}
}

// The overview shows both retention periods, each with the tasks that it
// holds for.
func TestOverviewShowsBothRetentionPeriods(t *testing.T) {
	t.Parallel()
	client := setup(t)
	err := client.UpdateSettings(t.Context(), func(s *taskmill.Settings) {
		s.HistoryRetention, s.DeadRetention = 3*24*time.Hour, 36*time.Hour
	})
	if err != nil {
		t.Fatal(err)
	}
	_, body, _ := get(t, handler(t, client, nil), "/")
	for _, want := range []string{`<dt>Completed</dt><dd>3 days</dd>`, `<dt>Dead and canceled</dt><dd>36 hours</dd>`} {
		if !strings.Contains(body, want) {
			t.Errorf("want %s in\n%s", want, body)
		}
	}
}

// The workers page marks the worker that holds the maintainer's lease, and
// no other: it is the one page that says which worker is the maintainer. A
// lease that ran out marks no worker.
func TestWorkersPageMarksTheMaintainer(t *testing.T) {
	t.Parallel()
	client, pool, schema := setupDB(t)
	if _, err := pool.Exec(t.Context(), `INSERT INTO `+schema+`.workers (id, queues, hostname, version)
	    VALUES ('lead', '{mail}', 'h', 'v'), ('other', '{mail}', 'h', 'v')`); err != nil {
		t.Fatal(err)
	}
	h := handler(t, client, nil)
	const mark = `<span class="chip">maintainer</span>`
	marked := func(lease string) []string {
		t.Helper()
		if _, err := pool.Exec(t.Context(), `INSERT INTO `+schema+`.leader (name, worker_id, elected_at, expires_at)
		    VALUES ('maintainer', 'lead', now(), now() + interval '`+lease+`')
		    ON CONFLICT (name) DO UPDATE SET expires_at = EXCLUDED.expires_at`); err != nil {
			t.Fatal(err)
		}
		_, body, _ := get(t, h, "/workers")
		var ids []string
		for _, row := range strings.Split(body, `<tr data-key="`)[1:] {
			if strings.Contains(row, mark) {
				ids = append(ids, row[:strings.IndexByte(row, '"')])
			}
		}
		return ids
	}
	if ids := marked("1 minute"); !slices.Equal(ids, []string{"lead"}) {
		t.Errorf("the page marks %v as the maintainer, want the worker that holds the lease", ids)
	}
	if ids := marked("-1 minute"); len(ids) != 0 {
		t.Errorf("the page marks %v as the maintainer after the lease ran out", ids)
	}
}

// The Host column names a worker's host, so the Worker column shows the part
// of the ID after it: the process ID and the random suffix, which tell the
// workers of one host apart, and which a long host name would push out of
// view. The whole ID is in the tooltip, and in the text that a screen reader
// reads. An ID that does not start with the host shows whole.
func TestWorkersShowTheirIDsWithoutTheHost(t *testing.T) {
	t.Parallel()
	client, pool, schema := setupDB(t)
	if _, err := pool.Exec(t.Context(), `INSERT INTO `+schema+`.workers (id, queues, hostname, version)
	    VALUES ('web-1.example.com-4242-ab12cd', '{mail}', 'web-1.example.com', 'v'),
	           ('web-1.example.com-4242-ffffff', '{mail}', 'web-1.example.com', 'v'),
	           ('lead', '{mail}', 'web-2', 'v'), ('web-3-', '{mail}', 'web-3', 'v'), ('-7-aa', '{mail}', '', 'v')`); err != nil {
		t.Fatal(err)
	}
	_, body, _ := get(t, handler(t, client, nil), "/workers")
	name, shown := regexp.MustCompile(`<span class="mono">(.*?)</span>(?:<span class="chip">|</div>)`), map[string]string{}
	for _, row := range tableRow.FindAllStringSubmatch(body, -1) {
		if m := name.FindStringSubmatch(row[2]); len(m) == 2 && strings.Contains(row[2], `<td title="`+row[1]+`">`) {
			shown[row[1]] = m[1]
		}
	}
	for id, want := range map[string]string{
		"web-1.example.com-4242-ab12cd": `<span class="visually-hidden">web-1.example.com-</span>4242-ab12cd`,
		"web-1.example.com-4242-ffffff": `<span class="visually-hidden">web-1.example.com-</span>4242-ffffff`,
		"lead":                          "lead",
		"web-3-":                        "web-3-",
		"-7-aa":                         "-7-aa",
	} {
		if shown[id] != want {
			t.Errorf("the worker %q shows as %q, want %q, with its ID as the tooltip\n%s", id, shown[id], want, body)
		}
	}
}

// A worker's row is one line, however much the worker handles. It shows the
// first queues that fit, and counts the rest and the kinds. Each count is a
// button, and both open the worker's one list of all its queues and kinds,
// each a link to its tasks: a popover, which has an id for an update to know
// it by. A row with nothing more to show has no button and no list.
func TestWorkersShowTheirQueuesAndKinds(t *testing.T) {
	t.Parallel()
	client, pool, schema := setupDB(t)
	if _, err := pool.Exec(t.Context(), `INSERT INTO `+schema+`.workers (id, queues, kinds, hostname, version) VALUES
	    ('many', '{default,emails,images,ledger,outbox,partner-api}', '{email.send,"orders&refunds"}', 'h', 'v'),
	    ('fits', '{default,orders,orders-priority}', '{email.send}', 'h', 'v'),
	    ('long', '{a-queue-with-a-name-longer-than-its-column,b}', '{}', 'h', 'v'),
	    ('words', '{почта,заказы,отчёты,склад}', '{}', 'h', 'v'),
	    ('lead', '{}', '{}', 'h', 'v')`); err != nil {
		t.Fatal(err)
	}
	_, body, _ := get(t, handler(t, client, nil), "/workers")
	rows := map[string]string{}
	for _, row := range tableRow.FindAllStringSubmatch(body, -1) {
		rows[row[1]] = row[2]
	}
	for _, c := range []struct {
		worker    string
		want, not []string
	}{
		{"many", []string{
			`<span title="default, emails, images, ledger, outbox, partner-api">default, emails, images</span>`,
			`popovertarget="lists-many">+3<span class="visually-hidden"> more queues</span></button>`,
			`popovertarget="lists-many">2 kinds<`,
			`id="lists-many" popover`,
			`<h2>Queues <span class="subtle">6</span></h2>`, `<a href="/tasks?queue=partner-api">partner-api</a>`,
			`<h2>Kinds <span class="subtle">2</span></h2>`, `<a href="/tasks?kind=orders%26refunds">orders&amp;refunds</a>`,
		}, nil},
		// A count takes room, so the queues that fit without one show whole.
		{"fits", []string{`>default, orders, orders-priority</span></div>`, `popovertarget="lists-fits">1 kind<`}, []string{">+"}},
		// The first queue shows, whatever its length. Without kinds, the
		// list has the queues alone.
		{"long", []string{
			`>a-queue-with-a-name-longer-than-its-column</span><button`, `>+1<span class="visually-hidden"> more queue</span>`,
			`<span class="tone-ghost">none</span><div class="hint-pop lists" id="lists-long" popover`,
		}, []string{"<h2>Kinds"}},
		// The room is in characters, not bytes.
		{"words", []string{`>почта, заказы, отчёты, склад</span></div>`}, []string{"popover"}},
		{"lead", []string{`<span class="tone-ghost">maintenance only</span>`, `<span class="tone-ghost">none</span></td>`}, []string{"popover"}},
	} {
		for _, want := range c.want {
			if !strings.Contains(rows[c.worker], want) {
				t.Errorf("the row of %q lacks %s\n%s", c.worker, want, rows[c.worker])
			}
		}
		for _, not := range c.not {
			if strings.Contains(rows[c.worker], not) {
				t.Errorf("the row of %q has %s\n%s", c.worker, not, rows[c.worker])
			}
		}
	}
}

// A worker built from taskmill's own tree stores Go's "(devel)" in place of a
// version. The page says what that means, as text, and its tooltip says it
// in full. A released version shows as it is, as code.
func TestWorkersSayALocalBuildInPlaceOfGosWord(t *testing.T) {
	t.Parallel()
	client, pool, schema := setupDB(t)
	if _, err := pool.Exec(t.Context(), `INSERT INTO `+schema+`.workers (id, queues, hostname, version)
	    VALUES ('released', '{mail}', 'h', 'v0.11.1'), ('built', '{mail}', 'h', '(devel)')`); err != nil {
		t.Fatal(err)
	}
	_, body, _ := get(t, handler(t, client, nil), "/workers")
	for _, want := range []string{
		`<td class="mono tone-faint" title="v0.11.1">v0.11.1</td>`,
		`<td class="tone-ghost" title="Built from taskmill's source, not from a release">local build</td>`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("want %s in\n%s", want, body)
		}
	}
	if strings.Contains(body, "devel") {
		t.Errorf("the page shows Go's word for a local build:\n%s", body)
	}
}

// A FIFO group key that the outbox made, of a topic and a message key, shows
// as its two parts wherever a page shows a group key, and its tooltip says
// both in words. The filter's value, a link, and a data-key carry the key
// itself, so the list filters as before. A key that only looks like one,
// with two separators, another control character, or an empty part, shows
// as it is, its control characters as pictures.
func TestGroupKeysOfTheOutboxShowAsTopicAndKey(t *testing.T) {
	t.Parallel()
	client := setup(t)
	const key = "orders\x1forder-13"
	var id int64
	for _, group := range []string{key, "a\x1fb\x1fc", "tab\there\x1fkey", "\x1fno-topic", "plain"} {
		res, err := client.Enqueue(t.Context(), greet{}, &taskmill.EnqueueOptions{Queue: "mail", GroupKey: group})
		if err != nil {
			t.Fatal(err)
		}
		if group == key {
			id = res.ID
		}
	}
	h := handler(t, client, nil)
	const parts = `<span title="Outbox topic orders, key order-13"><span class="topic">orders</span><span class="topic-sep" aria-hidden="true">›</span>order-13</span>`
	for path, wants := range map[string][]string{
		// The filter's options, and each task's group beside its queue.
		"/tasks?queue=mail": {
			`<label class="pop-opt" data-key="` + key + `"><input type="checkbox" class="chk-input" name="group" value="` + key + `">`,
			`<span class="pop-text mono">` + parts + `</span>`,
			`<td title="mail, FIFO group orders › order-13">mail<span class="group">` + parts + `</span></td>`,
			`<span class="pop-text mono">a␟b␟c</span>`, `<span class="pop-text mono">tab␉here␟key</span>`,
			`<span class="pop-text mono">␟no-topic</span>`, `<span class="pop-text mono">plain</span>`,
			`<td title="mail, FIFO group a␟b␟c">mail<span class="group">a␟b␟c</span></td>`,
		},
		// The filter's button, when the key is the one choice.
		"/tasks?group=orders%1Forder-13": {
			`aria-label="FIFO group: Outbox topic orders, key order-13"><span class="sel-label">` + parts + `</span>`,
			`name="group" value="` + key + `" checked>`,
		},
		"/tasks?group=a%1Fb%1Fc": {`aria-label="FIFO group: a␟b␟c"><span class="sel-label">a␟b␟c</span>`},
		// The task's page, and the page of an action on the group's tasks.
		"/tasks/" + strconv.FormatInt(id, 10):        {`<a class="mono" href="/tasks?group=orders%1Forder-13&amp;queue=mail">` + parts + `</a>`},
		"/tasks/bulk/cancel?group=orders%1Forder-13": {`<dt>FIFO group</dt><dd class="mono">` + parts + `</dd>`},
		"/tasks/bulk/cancel?group=a%1Fb%1Fc":         {`<dt>FIFO group</dt><dd class="mono">a␟b␟c</dd>`},
	} {
		code, body, _ := get(t, h, path)
		for _, want := range wants {
			if !strings.Contains(body, want) {
				t.Errorf("%s (%d): want %s in\n%s", path, code, want, body)
			}
		}
	}
	// The filter still selects by the key itself.
	if _, tasks := listTasks(t, h, "/tasks?group=orders%1Forder-13"); !slices.Equal(tasks, []string{strconv.FormatInt(id, 10)}) {
		t.Errorf("the list of the group shows the tasks %v, want %d", tasks, id)
	}
}

// The Schedules page says a schedule's cron expression in words where it can
// say exactly what the expression means, above the expression and the time
// zone that the times are in. An expression that it has no exact words for,
// and one that no schedule can run, show as they are stored.
func TestSchedulesSayTheirCronInWords(t *testing.T) {
	t.Parallel()
	client, pool, schema := setupDB(t)
	if _, err := pool.Exec(t.Context(), `INSERT INTO `+schema+`.schedules (name, queue, kind, cron, timezone, next_run_at)
	    VALUES ('nightly', 'default', 'greet', '0 3 * * *', 'Europe/Berlin', now()),
	           ('friday-13th', 'default', 'greet', '0 0 13 * 5', 'UTC', now()),
	           ('unreadable', 'default', 'greet', '<every> day', 'UTC', now())`); err != nil {
		t.Fatal(err)
	}
	_, body, _ := get(t, handler(t, client, nil), "/schedules")
	cronCell, cells := regexp.MustCompile(`(?s)<td class="wrap"[^>]*>(.*?)</td>`), map[string]string{}
	for _, row := range tableRow.FindAllStringSubmatch(body, -1) {
		if cell := cronCell.FindStringSubmatch(row[2]); cell != nil {
			cells[row[1]] = cell[1]
		}
	}
	for name, want := range map[string]string{
		"nightly":     `<div class="cron-words">Every day at 03:00</div><code class="chip mono">0 3 * * *</code> <span class="tone-ghost">Europe/Berlin</span>`,
		"friday-13th": `<code class="chip mono">0 0 13 * 5</code> <span class="tone-ghost">UTC</span>`,
		"unreadable":  `<code class="chip mono">&lt;every&gt; day</code> <span class="tone-ghost">UTC</span>`,
	} {
		if cells[name] != want {
			t.Errorf("the schedule %q shows its cron expression as\n%s\nwant\n%s", name, cells[name], want)
		}
	}
}

// A worker checks in every third of its lease: with a lease of 10 minutes,
// it is silent for more than 3 minutes while it is healthy. The page marks a
// worker as stale by the worker's own lease, not after a fixed time.
func TestWorkersAreStaleByTheirOwnLease(t *testing.T) {
	t.Parallel()
	client, pool, schema := setupDB(t)
	w, err := taskmill.NewWorker(client, &taskmill.WorkerConfig{LeaseDuration: 10 * time.Minute, NoMaintainer: true})
	if err != nil {
		t.Fatal(err)
	}
	ctx, stop := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	t.Cleanup(func() {
		stop()
		if err := <-done; err != nil {
			t.Error(err)
		}
	})

	// stale sets the last check-in of the workers to ago before now, and
	// returns how many of them the page then marks as stale. It first waits
	// for all of them to be registered. The page marks a worker with a color
	// and with a word: a color is never the only cue.
	h := handler(t, client, nil)
	stale := func(workers int64, ago string) int {
		t.Helper()
		for deadline := time.Now().Add(15 * time.Second); ; time.Sleep(20 * time.Millisecond) {
			tag, err := pool.Exec(t.Context(), "UPDATE "+schema+".workers SET heartbeat_at = now() - interval '"+ago+"'")
			if err != nil || time.Now().After(deadline) {
				t.Fatalf("%d of %d workers are registered: %v", tag.RowsAffected(), workers, err)
			}
			if tag.RowsAffected() == workers {
				break
			}
		}
		_, body, _ := get(t, h, "/workers")
		dots, words := strings.Count(body, `class="health stale"`), strings.Count(body, `<span class="chip warn">stale</span>`)
		if dots != words {
			t.Errorf("the page marks %d workers as stale by color, and %d in words\n%s", dots, words, body)
		}
		return dots
	}
	if n := stale(1, "5 minutes"); n != 0 {
		t.Error("a worker that checked in within its lease of 10 minutes shows as stale")
	}
	if n := stale(1, "11 minutes"); n != 1 {
		t.Error("a worker that made no check-in for longer than its lease shows as healthy")
	}

	// A worker of the previous release stores no lease: it is stale after 30
	// seconds, as before.
	if _, err := pool.Exec(t.Context(), `INSERT INTO `+schema+`.workers (id, queues, hostname, version)
	    VALUES ('old', '{}', 'h', 'v')`); err != nil {
		t.Fatal(err)
	}
	if n := stale(2, "20 seconds"); n != 0 {
		t.Errorf("%d workers that checked in 20 seconds ago show as stale", n)
	}
	if n := stale(2, "40 seconds"); n != 1 {
		t.Errorf("%d workers show as stale 40 seconds after their check-ins, want the one without a lease", n)
	}
}

func TestOverviewPeriods(t *testing.T) {
	t.Parallel()
	h := handler(t, setup(t), nil)
	// The charts' period is chosen in the address; an unknown one shows the
	// last hour. A day has steps of 15 minutes, and an hour steps of one: the
	// script tells the time of a step by them.
	for query, want := range map[string][2]string{"?range=24h": {"24h", "900"}, "?range=7y": {"1h", "60"}} {
		_, body, _ := get(t, h, "/"+query)
		if !strings.Contains(body, `aria-current="true">`+want[0]+`<`) || !strings.Contains(body, `class="series"`) ||
			strings.Count(body, `data-step="`+want[1]+`"`) != 2 {
			t.Fatalf("overview %s: want the %s period selected, in steps of %s seconds\n%s", query, want[0], want[1], body)
		}
	}
}

// TestOverviewShowsTheAttemptsOfAStep checks what the activity card says
// about attempts: the dial reads the newest complete minute against the
// highest rate of the period, and the chart holds each step's values for the
// script, which shows them under the pointer.
func TestOverviewShowsTheAttemptsOfAStep(t *testing.T) {
	t.Parallel()
	client, pool, schema := setupDB(t)
	h := handler(t, client, nil)

	// Nothing is recorded yet: the needle rests on zero, and the dial says so.
	// Each chart says so behind a hint beside its title, not over its plot.
	_, body, _ := get(t, h, "/")
	for _, want := range []string{
		`aria-label="Attempts a minute: no data"`, `aria-hidden="true">—</p>`, `class="dial-needle"[^>]* transform="rotate\(0\)"`,
		`<span class="chart-title">Tasks<span class="hint" data-key="hint-tasks"><button [^>]*popovertarget="hint-tasks"`,
		`<span class="hint-pop" id="hint-tasks" popover role="note">Nothing recorded yet\.</span></span></span>`,
		`<span class="hint-pop" id="hint-attempts" popover role="note">Nothing recorded yet\.</span>`,
	} {
		if !regexp.MustCompile(want).MatchString(body) {
			t.Errorf("without activity: want %s in\n%s", want, body)
		}
	}

	// The same counts in this minute and in the three before it: whichever of
	// them is the newest complete one when the page renders, it has 660
	// attempts, and so has the busiest one.
	if _, err := pool.Exec(t.Context(), `INSERT INTO `+schema+`.activity (minute, queue, completed, failed)
	    SELECT date_trunc('minute', now()) - n * interval '1 minute', 'mail', 600, 60 FROM generate_series(0, 3) n`); err != nil {
		t.Fatal(err)
	}
	_, body, _ = get(t, h, "/")
	for _, want := range []string{
		`aria-label="660 attempts a minute, on a dial from 0 to 1,000"`, `aria-hidden="true">660</p>`,
		`>0</text>`, `>250</text>`, `>500</text>`, `>750</text>`, `>1k</text>`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("want %s in\n%s", want, body)
		}
	}
	// The needle and the arc of the value turn together, 660 of 1,000 of a
	// half turn.
	if n := strings.Count(body, `transform="rotate(118.8)"`); n != 2 {
		t.Errorf("%d shapes are turned by 118.8 degrees, want the needle and the arc\n%s", n, body)
	}
	// The chart is a picture with a name, says when its steps start, and
	// holds one value per step for each series, with nothing for a step
	// without one: the step in progress, and the one before it while its
	// count may still grow. The legend names the series in the same order.
	// The range field after the picture is the chart's slider, for the
	// script: one stop per step, on the newest one. A chart that draws
	// something has no hint.
	for _, want := range []string{
		`<svg [^>]*role="img" aria-label="Attempts over the last 1h" data-start="\d+" data-step="60">`,
		`<g class="state-completed" data-values=" *(600 ){2,4} ?">`, `<g class="state-dead" data-values=" *(60 ){2,4} ?">`,
		`<span class="state-completed"><i></i>Completed</span><span class="state-dead"><i></i>Failed</span>`,
		`</svg>\s*<input class="chart-step js-only" type="range" min="1" max="60" value="60" aria-label="Attempts over the last 1h, step by step" autocomplete="off">`,
		`<span class="chart-title">Attempts</span>`,
	} {
		if !regexp.MustCompile(want).MatchString(body) {
			t.Errorf("want %s in\n%s", want, body)
		}
	}
	if strings.Contains(body, `tabindex="0"`) {
		t.Errorf("a picture takes the focus: without the script it does nothing with it\n%s", body)
	}
}

// Pause and Resume on the overview lead back to the overview with the period
// of the charts that the reader chose: each form carries the period of its
// page, and the server takes only a period that it knows. The first period
// is the overview's plain address. An action that is refused leads back
// there too.
func TestQueueActionsKeepThePeriodOfTheCharts(t *testing.T) {
	t.Parallel()
	client := setup(t)
	if err := client.UpdateQueue(t.Context(), "mail", func(*taskmill.QueueSettings) {}); err != nil {
		t.Fatal(err)
	}
	h := handler(t, client, &ui.Config{Prefix: "/admin"})
	for _, c := range []struct{ page, back string }{
		{"/admin/?range=24h", "/admin/?range=24h"},
		{"/admin/?range=6h", "/admin/?range=6h"},
		{"/admin/", "/admin/"},
		{"/admin/?range=1h", "/admin/"},
		{"/admin/?range=7y", "/admin/"},
	} {
		for _, step := range [][2]string{{"Pause", "queue-pause"}, {"Resume", "queue-resume"}} {
			sep := "?"
			if strings.Contains(c.back, "?") {
				sep = "&"
			}
			rec := post(t, h, rowForms(t, h, c.page, "mail")[step[0]], "same-origin")
			if got, want := rec.Header().Get("Location"), c.back+sep+"done="+step[1]; rec.Code != http.StatusSeeOther || got != want {
				t.Errorf("%s on %s: status %d to %q, want 303 to %q", step[0], c.page, rec.Code, got, want)
			}
		}
	}
	// What the address of an action says is not trusted: only a period's name
	// reaches the address that the action leads to.
	for _, period := range []string{"24h%26done%3Dretry", "%2F%2Fexample.com", "24h%0d%0aSet-Cookie:x"} {
		rec := post(t, h, "/admin/queues/pause?name=mail&range="+period, "same-origin")
		if got := rec.Header().Get("Location"); rec.Code != http.StatusSeeOther || got != "/admin/?done=queue-pause" {
			t.Errorf("Pause with the period %q: status %d to %q, want 303 to the overview's plain address", period, rec.Code, got)
		}
	}
	rec := post(t, h, "/admin/queues/pause?range=24h", "same-origin") // no queue
	if body := rec.Body.String(); rec.Code != http.StatusBadRequest || !strings.Contains(body, `<a class="btn" href="/admin/?range=24h">`) {
		t.Errorf("Pause without a queue on the 24h overview: status %d, want 400 and the way back to that overview\n%s", rec.Code, body)
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
	h := handler(t, client, &ui.Config{Prefix: "/admin/tasks"})
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

// deadTasks stores 260 dead tasks, more than a task list shows on one page,
// and task 261, which waits. Task i died (7i mod 260) minutes ago: every
// task at a time of its own, and in another order than the IDs. It returns
// the dashboard, and the IDs of the dead tasks, the one that died last
// first.
func deadTasks(t *testing.T) (h http.Handler, latestFirst []string) {
	t.Helper()
	client, pool, schema := setupDB(t)
	_, err := pool.Exec(t.Context(), `INSERT INTO `+schema+`.tasks (queue, kind, state, run_at, payload, max_attempts, finalized_at)
		SELECT 'mail', 'greet', 'dead', now(), '{}', 1, now() - (i * 7 % 260) * interval '1 minute' FROM generate_series(1, 260) i`)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.Enqueue(t.Context(), greet{Name: "a"}, nil); err != nil {
		t.Fatal(err)
	}
	latestFirst = make([]string, 260)
	for i := 1; i <= 260; i++ {
		latestFirst[i*7%260] = strconv.Itoa(i)
	}
	return handler(t, client, nil), latestFirst
}

// taskRow is the start of a task's row in a task list, with the task's ID.
var taskRow = regexp.MustCompile(`<tr data-href="[^"]*" data-key="(\d+)"`)

// listTasks requests a task list, and returns the page with the IDs of its
// tasks in their order. As a browser does, it keeps the place on the page
// that the address names to itself.
func listTasks(t *testing.T, h http.Handler, target string) (body string, ids []string) {
	t.Helper()
	target, _, _ = strings.Cut(target, "#")
	code, body, _ := get(t, h, target)
	if code != http.StatusOK {
		t.Fatalf("GET %s: status %d", target, code)
	}
	for _, m := range taskRow.FindAllStringSubmatch(body, -1) {
		ids = append(ids, m[1])
	}
	return body, ids
}

// linkNamed returns the address of the page's link with the label.
func linkNamed(t *testing.T, body, label string) string {
	t.Helper()
	m := regexp.MustCompile(`href="([^"]+)"[^>]*>(?:<svg.*?</svg>)?` + label + `<`).FindStringSubmatch(body)
	if m == nil {
		t.Fatalf("the page has no link %q", label)
	}
	return html.UnescapeString(m[1])
}

// sortedBy returns the header of the column that a task list marks as the
// one that orders it, and the name that the mark gives the order.
func sortedBy(t *testing.T, body string) (column, order string) {
	t.Helper()
	m := regexp.MustCompile(`<th[^>]* aria-sort="descending">([^<]+)<svg[^>]* role="img"><title>([^<]+)</title>`).FindAllStringSubmatch(body, -1)
	if len(m) != 1 {
		t.Fatalf("the list marks %d columns as the one that orders it, want one\n%s", len(m), body)
	}
	return m[0][1], m[0][2]
}

// A list of dead tasks shows the task that died last first, and shows when
// each task died: that column carries the mark of the list's order, which
// names it. A list that mixes dead tasks with others is in the order of the
// IDs, as every other list, and marks the ID column.
func TestListOfDeadTasksShowsTheLatestFirst(t *testing.T) {
	t.Parallel()
	h, latestFirst := deadTasks(t)
	body, ids := listTasks(t, h, "/tasks?state=dead")
	if column, order := sortedBy(t, body); column != "Finished" || order != "Latest first" {
		t.Errorf("the list of dead tasks marks its order on %q as %q, want Finished, latest first", column, order)
	}
	if !slices.Equal(ids, latestFirst[:50]) {
		t.Errorf("the list of dead tasks starts with %v, want the 50 that died last", ids)
	}
	mixed, ids := listTasks(t, h, "/tasks?state=dead&state=available")
	if !slices.Equal(ids[:3], []string{"261", "260", "259"}) || !strings.Contains(mixed, "<th>Run at</th>") || !strings.Contains(mixed, "Load more") {
		t.Errorf("a list of dead and available tasks starts with %v, or shows when its tasks finished\n%s", ids[:3], mixed)
	}
	if column, order := sortedBy(t, mixed); column != "ID" || order != "Newest first" {
		t.Errorf("a list of dead and available tasks marks its order on %q as %q, want ID, newest first", column, order)
	}
}

// "Load more" shows the same list with a page more of it, in the list's
// order, until the list is as long as one page gets: 250 tasks. Its link
// names the first task that it adds, where a browser without the script
// opens the longer list. From there, the list continues from its last task
// on a page of its own, under a link that names the order. That page leads
// back to the first one, which is how a reader tells the two apart.
func TestTaskListLoadsMore(t *testing.T) {
	t.Parallel()
	h, latestFirst := deadTasks(t)
	body, ids := listTasks(t, h, "/tasks?state=dead")
	if strings.Contains(body, `id="more"`) || strings.Contains(body, ">Latest<") {
		t.Errorf("the first 50 dead tasks have one named \"more\", or lead back to themselves\n%s", body)
	}
	for want := 100; want <= 250; want += 50 {
		more := linkNamed(t, body, "Load more")
		if more != "/tasks?limit="+strconv.Itoa(want)+"&state=dead#more" {
			t.Fatalf("after %d tasks, \"Load more\" leads to %s", want-50, more)
		}
		if body, ids = listTasks(t, h, more); !slices.Equal(ids, latestFirst[:want]) {
			t.Fatalf("%s shows %d tasks %v, want the %d that died last", more, len(ids), ids, want)
		}
		if added := `data-key="` + latestFirst[want-50] + `" id="more"`; !strings.Contains(body, added) || strings.Count(body, `id="more"`) != 1 {
			t.Errorf("%s: want the list's task %d, and no other, named \"more\"", more, want-49)
		}
	}
	if strings.Contains(body, "Load more") {
		t.Errorf("the list of 250 dead tasks still offers to load more\n%s", body)
	}
	earlier := linkNamed(t, body, "Earlier")
	if strings.Contains(earlier, "limit=") {
		t.Errorf("the page after 250 dead tasks is %s, want no limit in its address", earlier)
	}
	body, later := listTasks(t, h, earlier)
	if !slices.Equal(slices.Concat(ids, later), latestFirst) {
		t.Errorf("the page after 250 dead tasks shows %v, want the 10 that died first", later)
	}
	if linkNamed(t, body, "Latest") != "/tasks?state=dead" || strings.Contains(body, ">Earlier<") || strings.Contains(body, "Load more") {
		t.Errorf("the last page of dead tasks does not lead back to the first, or leads on\n%s", body)
	}
}

// Under its tasks, a list has the link that leads on, and says how many
// tasks it shows. "Load more" keeps the cursor of a later page, and stops
// at 250 tasks: the link from there leads to the page after the list's last
// task, and is not one that the script shows in place. A list that shows
// all its tasks still says how many, and a list without tasks has nothing
// to count. A page after the list's last task says that no more tasks
// follow, and still leads back to the first page. The tasks are in the
// order of the IDs, task 261 first.
func TestTaskListSaysHowItContinues(t *testing.T) {
	t.Parallel()
	h, _ := deadTasks(t)
	for _, c := range []struct{ target, want, not string }{
		{"/tasks", `href="/tasks?limit=100#more" data-more>`, `id="more"`},
		{"/tasks", "data-shown>50 shown<", ">Older<"},
		{"/tasks?cursor=MjAw", `href="/tasks?cursor=MjAw&amp;limit=100#more" data-more>`, ">Older<"}, // "200"
		{"/tasks?limit=230", `href="/tasks?limit=250#more" data-more>`, ">Older<"},
		{"/tasks?limit=250", `href="/tasks?cursor=MTI">Older<`, "data-more"}, // "12", the last of the 250 tasks
		{"/tasks?limit=250", "data-shown>250 shown<", "Load more"},
		{"/tasks?cursor=MTI", "data-shown>11 shown<", "data-more"},
		{"/tasks?state=available", "data-shown>1 shown<", "data-more"},
		{"/tasks?state=running", "No tasks match these filters", "data-shown"},
		{"/tasks?cursor=MQ", "No more tasks", "data-shown"}, // "1", the first task of all
	} {
		code, body, _ := get(t, h, c.target)
		if code != http.StatusOK || !strings.Contains(body, c.want) || strings.Contains(body, c.not) {
			t.Errorf("GET %s: status %d, want a page with %s, and without %s\n%s", c.target, code, c.want, c.not, body)
		}
	}
	if _, body, _ := get(t, h, "/tasks?cursor=MQ"); linkNamed(t, body, "Newest") != "/tasks" {
		t.Errorf("the page after the last task does not lead back to the first page\n%s", body)
	}
}

// A task list that is refused says what is wrong. The client refuses a
// filter or a cursor, and the page names it as the client does. An address
// with a cursor, as a bookmark of a list of dead tasks from the previous
// release is, also says where such a cursor comes from, and leads to the
// first page of the same list: its filter may be fine. Without a cursor,
// the filter is what is wrong, and the page offers to clear it. The
// dashboard itself refuses a limit that is not a number from 1 to 250,
// before it asks the client: the page says so, and leads to the first page
// of the same list, whatever else the address holds.
func TestTaskListSaysWhatItRefuses(t *testing.T) {
	t.Parallel()
	h := handler(t, setup(t), nil)
	const upgrade = "before an upgrade"
	for _, c := range []struct {
		target, wrong, back, label string
		cursor                     bool
	}{
		{"/tasks?state=dead&cursor=MTIz", "invalid cursor", "/tasks?state=dead", "Back to the first page", true}, // "123", of the ID order
		{"/tasks?cursor=%25", "invalid cursor", "/tasks", "Back to the first page", true},
		{"/tasks?state=bogus&cursor=MTIz", "unknown state", "/tasks?state=bogus", "Back to the first page", true},
		{"/tasks?state=bogus", "unknown state", "/tasks", "Clear the filters", false},
		{"/tasks?state=dead&limit=0", "the limit must be a number from 1 to 250", "/tasks?state=dead", "Back to the first page", false},
		{"/tasks?limit=251&cursor=MTIz", "the limit must be", "/tasks", "Back to the first page", false},
		{"/tasks?limit=many", "the limit must be", "/tasks", "Back to the first page", false},
	} {
		code, body, _ := get(t, h, c.target)
		if code != http.StatusBadRequest || !strings.Contains(body, "<h1>Invalid request</h1>") || !strings.Contains(body, c.wrong) ||
			strings.Contains(body, upgrade) != c.cursor || !strings.Contains(body, `<a class="btn" href="`+c.back+`">`) ||
			!strings.Contains(body, c.label+"</a>") {
			t.Errorf("GET %s: status %d, want 400, a page that says %q, and the link %q to %s\n%s", c.target, code, c.wrong, c.label, c.back, body)
		}
	}
}

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

func TestActions(t *testing.T) {
	t.Parallel()
	client := setup(t)
	ctx := t.Context()
	res, err := client.Enqueue(ctx, greet{Name: "x"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	logs := &actionLog{}
	h := handler(t, client, &ui.Config{Logger: slog.New(logs)})
	id := strconv.FormatInt(res.ID, 10)

	for _, route := range postRoutes(t) {
		if rec := post(t, h, route, "cross-site"); rec.Code != http.StatusForbidden {
			t.Errorf("POST %s from another site: %d", route, rec.Code)
		}
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
	if rec := post(t, h, "/queues/pause?name=default", "same-origin"); rec.Code != http.StatusSeeOther {
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
	h := handler(t, client, &ui.Config{Logger: slog.New(logs)})

	// An action's page counts the tasks once, and confirms with or without
	// the script.
	code, body, _ := get(t, h, "/tasks/bulk/cancel?queue=mail")
	if code != http.StatusOK || !strings.Contains(body, "Cancel 3 tasks</button>") || strings.Contains(body, ` data-live>`) ||
		!strings.Contains(body, `<form class="bulk-actions" method="post" action="/tasks/bulk/cancel?queue=mail">`) {
		t.Fatalf("cancel page: %d\n%s", code, body)
	}
	// With the script, the dialog asks what the page asks, under this title.
	if want := `data-dialog-title="Cancel 3 tasks?"`; !strings.Contains(body, want) {
		t.Fatalf("cancel page: want %s in\n%s", want, body)
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
	if _, body, _ := get(t, h, "/tasks/bulk/cancel?queue=mail"); !strings.Contains(body, "No task matches.") || strings.Contains(body, `method="post"`) ||
		!strings.Contains(body, `data-dialog-title="Cancel tasks"`) {
		t.Fatalf("cancel page offers the running task again, or asks about no task:\n%s", body)
	}

	if rec := post(t, h, "/tasks/bulk/delete?state=canceled", "same-origin"); rec.Code != http.StatusSeeOther {
		t.Fatalf("bulk delete: %d", rec.Code)
	}
	if code, _, _ := get(t, h, "/tasks/bulk/delete"); code != http.StatusBadRequest {
		t.Fatalf("bulk delete page without filters: %d", code)
	}
	// The dialog shows why an action failed, under the title of that page.
	if rec := post(t, h, "/tasks/bulk/delete", "same-origin"); rec.Code != http.StatusBadRequest ||
		!strings.Contains(rec.Body.String(), `data-dialog-title="Invalid request"`) {
		t.Fatalf("bulk delete without filters: %d\n%s", rec.Code, rec.Body)
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
// without counting the tasks. A list with a range, of finish times or of
// creation times, offers none: a bulk action acts on the filters alone, also
// on the tasks outside the range. Each is a link to the page that asks,
// marked for the script, which asks in its dialog.
func TestTaskListOffersBulkActions(t *testing.T) {
	t.Parallel()
	h := handler(t, setup(t), nil)
	for path, want := range map[string][]string{
		"/tasks":                            nil,
		"/tasks?queue=mail":                 {"/tasks/bulk/retry?queue=mail", "/tasks/bulk/cancel?queue=mail", "/tasks/bulk/delete?queue=mail"},
		"/tasks?queue=mail&state=available": {"/tasks/bulk/cancel?", "/tasks/bulk/delete?"},
		"/tasks?state=dead":                 {"/tasks/bulk/retry?state=dead", "/tasks/bulk/delete?state=dead"},
		"/tasks?state=running&kind=greet":   {"/tasks/bulk/cancel?kind=greet&amp;state=running"},
		// With a range of finish times.
		"/tasks?state=dead&since=2021-03-04T11:00Z":                 nil,
		"/tasks?state=canceled&queue=mail&before=2021-03-04T11:00Z": nil,
		// With a range of creation times.
		"/tasks?queue=mail&since=2021-03-04T11:00Z":                                         nil,
		"/tasks?queue=mail&state=available&since=2021-03-04T11:00Z&before=2021-03-05T00:00": nil,
		"/tasks?state=dead&state=running&before=2021-03-04T11:00Z":                          nil,
	} {
		_, body, _ := get(t, h, path)
		if got := strings.Count(body, `href="/tasks/bulk/`); got != len(want) {
			t.Errorf("%s offers %d bulk actions, want %d", path, got, len(want))
		}
		if got := len(regexp.MustCompile(`href="/tasks/bulk/[^"]*" data-dialog>`).FindAllString(body, -1)); got != len(want) {
			t.Errorf("%s marks %d bulk actions for the dialog, want %d", path, got, len(want))
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
	picks := &pickHook{}
	cfg, err := pgxpool.ParseConfig(testdb.URL(t))
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.Tracer = picks
	pool, err := pgxpool.NewWithConfig(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	client, _, _ := setupOn(t, pool)
	// Three batches of tasks.
	params := make([]taskmill.EnqueueParams, 3000)
	for i := range params {
		params[i] = taskmill.EnqueueParams{Args: greet{Name: "x"}, Options: &taskmill.EnqueueOptions{GroupKey: strconv.Itoa(i % 10)}}
	}
	if _, err := client.EnqueueMany(t.Context(), params); err != nil {
		t.Fatal(err)
	}
	h := handler(t, client, nil)
	form := regexp.MustCompile(`<form class="bulk-actions" method="post" action="([^"]*)">`)
	target, total, requests := "/tasks/bulk/cancel?queue=default", 0, 0
	for range 200 {
		// A request has 400 ms, so the action works for 200 ms of it. Its
		// second batch starts only after those: the action then ends that
		// batch, and stops short of the third.
		const deadline = 400 * time.Millisecond
		ctx, cancel := context.WithTimeout(t.Context(), deadline)
		began, batch := time.Now(), 0
		hold := func() {
			if batch++; batch == 2 {
				time.Sleep(time.Until(began.Add(deadline/2 + 20*time.Millisecond)))
			}
		}
		picks.at.Store(&hold)
		req := httptest.NewRequestWithContext(ctx, http.MethodPost, target, nil)
		req.Header.Set("Sec-Fetch-Site", "same-origin")
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		cancel()
		requests++
		loc, _ := url.Parse(rec.Header().Get("Location"))
		q := loc.Query()
		n, _ := strconv.Atoi(q.Get("count"))
		total += n
		if q.Get("done") == "cancel-all" {
			// The task list shows every task again.
			if total != 3000 || requests < 2 || loc.Path != "/tasks" || q.Has("cursor") {
				t.Fatalf("canceled %d tasks in %d requests, then went to %q; want 3,000 in two or more, and the task list",
					total, requests, loc)
			}
			return
		}
		// Its page goes on where it stopped.
		if rec.Code != http.StatusSeeOther || loc.Path != "/tasks/bulk/cancel" || q.Get("done") != "cancel-more" || q.Get("cursor") == "" {
			t.Fatalf("slow bulk cancel: %d to %q", rec.Code, loc)
		}
		_, body, _ := get(t, h, loc.String())
		match := form.FindStringSubmatch(body)
		if !strings.Contains(body, html.EscapeString(number(n)+" tasks canceled so far; more remain.")) || match == nil ||
			!strings.Contains(match[1], "cursor=") {
			t.Fatalf("the page after a partial cancel:\n%s", body)
		}
		target = html.UnescapeString(match[1])
	}
	t.Fatal("the cancel did not end")
}

// pickHook calls a function each time a batch of a bulk action on its pool
// picks its tasks.
type pickHook struct{ at atomic.Pointer[func()] }

func (h *pickHook) TraceQueryStart(ctx context.Context, _ *pgx.Conn, d pgx.TraceQueryStartData) context.Context {
	if f := h.at.Load(); f != nil && strings.Contains(d.SQL, "WITH candidates AS") {
		(*f)()
	}
	return ctx
}

func (*pickHook) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

// A bulk action that left tasks that other work held says so, and leads back
// to its page to try again.
func TestBulkActionTellsWhatItLeft(t *testing.T) {
	t.Parallel()
	client, pool, schema := setupDB(t)
	var ids []int64
	for range 3 {
		res, err := client.Enqueue(t.Context(), greet{Name: "x"}, nil)
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, res.ID)
	}
	tx, err := pool.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if _, err := tx.Exec(t.Context(), `SELECT FROM `+schema+`.tasks WHERE id = $1 FOR UPDATE`, ids[1]); err != nil {
		t.Fatal(err)
	}
	h := handler(t, client, nil)
	rec := post(t, h, "/tasks/bulk/cancel?queue=default", "same-origin")
	if loc := rec.Header().Get("Location"); loc != "/tasks/bulk/cancel?count=2&done=cancel-skipped&queue=default&skipped=1" {
		t.Fatalf("bulk cancel with a held task: %d to %q", rec.Code, loc)
	}
	_, body, _ := get(t, h, rec.Header().Get("Location"))
	if !strings.Contains(body, "2 tasks canceled; other work held 1 task. Try again for those.") || !strings.Contains(body, "Cancel 1 task</button>") {
		t.Fatalf("the page after a cancel that left a task:\n%s", body)
	}
}

// A retry that another task's dedupe key refuses says which task holds the
// key, on the page of a refused action. A bulk retry says how many tasks it
// left for that reason, and leads to the task list: trying again does not
// help them.
func TestRetryRefusedByADedupeKey(t *testing.T) {
	t.Parallel()
	client := setup(t)
	ctx := t.Context()
	opts := &taskmill.EnqueueOptions{DedupeKey: "order-42"}
	first, err := client.Enqueue(ctx, greet{Name: "x"}, opts)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Cancel(ctx, first.ID); err != nil {
		t.Fatal(err)
	}
	second, err := client.Enqueue(ctx, greet{Name: "x"}, opts)
	if err != nil || second.Duplicate {
		t.Fatalf("enqueue after the cancel: %+v, %v", second, err)
	}
	h := handler(t, client, nil)
	id, holder := strconv.FormatInt(first.ID, 10), strconv.FormatInt(second.ID, 10)

	rec := post(t, h, "/tasks/"+id+"/retry", "same-origin")
	if body := rec.Body.String(); rec.Code != http.StatusConflict ||
		!strings.Contains(body, "cannot retry task "+id+": task "+holder+" holds its dedupe key") ||
		!strings.Contains(body, `href="/tasks/`+id+`"`) {
		t.Fatalf("refused retry: %d\n%s", rec.Code, body)
	}
	if info, _ := client.Get(ctx, first.ID); info.State != taskmill.StateCanceled {
		t.Fatalf("task is %s after the refused retry", info.State)
	}

	rec = post(t, h, "/tasks/bulk/retry?state=canceled", "same-origin")
	if loc := rec.Header().Get("Location"); loc != "/tasks?conflicts=1&count=0&done=retry-conflicts&state=canceled" {
		t.Fatalf("bulk retry of a task whose key is taken: %d to %q", rec.Code, loc)
	}
	_, body, _ := get(t, h, rec.Header().Get("Location"))
	if !strings.Contains(body, "0 tasks retried; 1 task not, because another task holds the same dedupe key.") {
		t.Fatalf("the page after a retry that left a task:\n%s", body)
	}
}

// countingConn counts the bytes that a connection reads.
type countingConn struct {
	net.Conn
	read *atomic.Int64
}

func (c countingConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	c.read.Add(int64(n))
	return n, err
}

// roundTrips counts the round trips that a pool's connections make to the
// database: each statement sent alone, and each batch. It leaves out the
// round trip in which a connection first prepares the statements of a batch.
type roundTrips struct {
	n atomic.Int64
}

func (r *roundTrips) TraceQueryStart(ctx context.Context, _ *pgx.Conn, _ pgx.TraceQueryStartData) context.Context {
	r.n.Add(1)
	return ctx
}

func (*roundTrips) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

func (r *roundTrips) TraceBatchStart(ctx context.Context, _ *pgx.Conn, _ pgx.TraceBatchStartData) context.Context {
	r.n.Add(1)
	return ctx
}

func (*roundTrips) TraceBatchQuery(context.Context, *pgx.Conn, pgx.TraceBatchQueryData) {}

func (*roundTrips) TraceBatchEnd(context.Context, *pgx.Conn, pgx.TraceBatchEndData) {}

// setupCounting is setupDB with a pool of its own, which counts the bytes
// that its connections read from the database, and their round trips.
func setupCounting(t *testing.T) (client *taskmill.Client, pool *pgxpool.Pool, schema string, read *atomic.Int64, trips *roundTrips) {
	t.Helper()
	cfg, err := pgxpool.ParseConfig(testdb.URL(t))
	if err != nil {
		t.Fatal(err)
	}
	read, trips = new(atomic.Int64), &roundTrips{}
	cfg.ConnConfig.Tracer = trips
	cfg.ConnConfig.DialFunc = func(ctx context.Context, network, addr string) (net.Conn, error) {
		conn, err := (&net.Dialer{}).DialContext(ctx, network, addr)
		return countingConn{Conn: conn, read: read}, err
	}
	if pool, err = pgxpool.NewWithConfig(t.Context(), cfg); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	schema = testdb.Schema(t, pool)
	if err := taskmill.Migrate(t.Context(), pool, &taskmill.MigrateConfig{Schema: schema}); err != nil {
		t.Fatal(err)
	}
	if client, err = taskmill.NewClient(t.Context(), pool, &taskmill.ClientConfig{Schema: schema}); err != nil {
		t.Fatal(err)
	}
	return client, pool, schema, read, trips
}

// The task list shows no payload and no metadata, and one line of each
// task's last error. So a page must not read them from the database: a
// payload can take 1 MiB, and an open page asks again every 5 seconds. The
// page of a dead task that halts its FIFO group counts the tasks behind it,
// and the page of a task that waits names the task ahead of it: they must
// not read those tasks in full either.
func TestTaskPagesReadNoPayloadsTheyDoNotShow(t *testing.T) {
	t.Parallel()
	client, pool, schema, read, _ := setupCounting(t)
	ctx := t.Context()
	const page, payload = 50, 100 << 10
	params := make([]taskmill.EnqueueParams, page)
	for i := range params {
		params[i] = taskmill.EnqueueParams{Args: greet{Name: strings.Repeat("x", payload)}, Options: &taskmill.EnqueueOptions{
			GroupKey: "g", Metadata: map[string]string{"trace": strings.Repeat("m", payload/10)},
		}}
	}
	res, err := client.EnqueueMany(ctx, params)
	if err != nil {
		t.Fatal(err)
	}
	// Every task has a full error history, and the group's head is dead.
	if _, err := pool.Exec(ctx, `UPDATE `+schema+`.tasks SET errors = (
	        SELECT jsonb_agg(jsonb_build_object('attempt', n, 'at', now(), 'message', repeat('e', 1000) || n))
	        FROM generate_series(1, 20) n)`); err != nil {
		t.Fatal(err)
	}
	head, next := strconv.FormatInt(res[0].ID, 10), strconv.FormatInt(res[1].ID, 10)
	if _, err := pool.Exec(ctx, `UPDATE `+schema+`.tasks SET state = 'dead', finalized_at = now() WHERE id = $1`, res[0].ID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE `+schema+`.groups SET halted_task_id = $1, halted_at = now()`, res[0].ID); err != nil {
		t.Fatal(err)
	}
	h := handler(t, client, nil)

	// What a page reads must not grow with what its tasks hold: the limit is
	// far below one task's payload for each task.
	const limit = page * payload / 20
	for _, tt := range []struct{ target, want string }{
		{"/tasks", `<span title="` + strings.Repeat("e", 300) + `">`}, // the last error of each task
		{"/tasks/" + head, "49 tasks wait"},
		{"/tasks/" + next, `Waits behind <a class="strong" href="/tasks/` + head + `">`},
	} {
		before := read.Load()
		code, body, _ := get(t, h, tt.target)
		got := read.Load() - before
		if code != http.StatusOK || !strings.Contains(body, tt.want) {
			t.Fatalf("%s: %d, without %q", tt.target, code, tt.want)
		}
		// A task's own page shows its payload, metadata, and errors.
		if tt.target != "/tasks" {
			got -= payload + payload/10 + 20*1000
		}
		t.Logf("%s read %d bytes from the database beyond the task it shows in full", tt.target, got)
		if got > limit {
			t.Errorf("%s read %d bytes from the database, want at most %d: it reads what it does not show", tt.target, got, limit)
		}
	}
}

// A page asks the database as few times as it has things to show: each read
// of the client is one round trip, also those with a time limit of their
// own. It matters for a dashboard far from its database, whose pages
// refresh every 5 seconds.
func TestPagesMakeFewRoundTrips(t *testing.T) {
	t.Parallel()
	client, pool, schema, _, trips := setupCounting(t)
	ctx := t.Context()
	// One enqueue for the three tasks: the client sends a later notification
	// for enqueues that follow each other closely, and that would count.
	grouped := taskmill.EnqueueParams{Args: greet{Name: "x"}, Options: &taskmill.EnqueueOptions{GroupKey: "g"}}
	res, err := client.EnqueueMany(ctx, []taskmill.EnqueueParams{grouped, grouped, grouped})
	if err != nil {
		t.Fatal(err)
	}
	ids := []int64{res[0].ID, res[1].ID, res[2].ID}
	if _, err := pool.Exec(ctx, `UPDATE `+schema+`.tasks SET state = 'dead', finalized_at = now() WHERE id = $1`, ids[0]); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE `+schema+`.groups SET halted_task_id = $1, halted_at = now()`, ids[0]); err != nil {
		t.Fatal(err)
	}
	h := handler(t, client, nil)
	get(t, h, "/tasks") // reads the filters' choices, which the next pages reuse

	for _, tt := range []struct {
		target string
		want   int64
		reads  string
	}{
		{"/", 5, "the queues, the workers, the schedules, the settings, and the activity"},
		{"/tasks", 2, "the tasks, and the queues"},
		{"/tasks/" + strconv.FormatInt(ids[0], 10), 3, "the task, the count of the tasks behind it, and the queues"},
		{"/tasks/" + strconv.FormatInt(ids[1], 10), 3, "the task, the task ahead of it, and the queues"},
		{"/tasks/bulk/retry?state=dead", 2, "the count, and the queues"},
		{"/schedules", 2, "the schedules, and the queues"},
	} {
		before := trips.n.Load()
		if code, _, _ := get(t, h, tt.target); code != http.StatusOK {
			t.Fatalf("%s: %d", tt.target, code)
		}
		if n := trips.n.Load() - before; n > tt.want {
			t.Errorf("%s made %d round trips to the database, want at most %d: %s", tt.target, n, tt.want, tt.reads)
		}
	}
}

// A task list that the database does not finish within the list's time limit
// says so, as a request that ran out of time does, with a way to try again.
// A list with filters also says what may help: it can read many tasks that
// it does not show.
func TestTaskListThatRunsOutOfTime(t *testing.T) {
	t.Parallel()
	client, pool, schema := setupDB(t)
	ctx := t.Context()
	h := handler(t, client, nil)
	// The list waits for the table, and the database ends its statement, as
	// it does at the time limit: here, because the test cancels it.
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if _, err := tx.Exec(ctx, `LOCK TABLE `+schema+`.tasks IN ACCESS EXCLUSIVE MODE`); err != nil {
		t.Fatal(err)
	}
	const hint = "A queue and a state in the filters, or fewer filters, may help."
	for target, hinted := range map[string]bool{"/tasks?state=dead": true, "/tasks": false} {
		type answer struct {
			code   int
			body   string
			header http.Header
		}
		answered := make(chan answer, 1)
		go func() {
			code, body, header := get(t, h, target)
			answered <- answer{code, body, header}
		}()
		var got answer
		for waiting := true; waiting; {
			select {
			case got = <-answered:
				waiting = false
			case <-time.After(20 * time.Millisecond):
				_, err := pool.Exec(ctx, `SELECT pg_cancel_backend(pid) FROM pg_stat_activity
				                          WHERE wait_event_type = 'Lock' AND strpos(query, $1) > 0 AND pid <> pg_backend_pid()`, schema)
				if err != nil {
					t.Fatal(err)
				}
			}
		}
		if got.code != http.StatusServiceUnavailable || got.header.Get("Retry-After") == "" ||
			!strings.Contains(got.body, "Taking too long") || strings.Contains(got.body, hint) != hinted {
			t.Errorf("%s: %d, a hint about the filters: %t, want it: %t\n%s", target, got.code, strings.Contains(got.body, hint), hinted, got.body)
		}
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

// actionLog records what the dashboard logs: every message, and each action
// with its target.
type actionLog struct {
	mu       sync.Mutex
	messages []string
	actions  []string
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
	l.mu.Lock()
	defer l.mu.Unlock()
	l.messages = append(l.messages, r.Message)
	if r.Message == "dashboard action" {
		l.actions = append(l.actions, action+" "+target)
	}
	return nil
}

func (l *actionLog) WithAttrs([]slog.Attr) slog.Handler { return l }

func (l *actionLog) WithGroup(string) slog.Handler { return l }

func TestReadOnly(t *testing.T) {
	t.Parallel()
	client := setup(t)
	storeSchedules(t, client, []taskmill.Schedule{{Name: "nightly", Cron: "@daily", Args: greet{}}})
	res, err := client.Enqueue(t.Context(), greet{Name: "x"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.UpdateQueue(t.Context(), "default", func(*taskmill.QueueSettings) {}); err != nil {
		t.Fatal(err)
	}
	h := handler(t, client, &ui.Config{ReadOnly: true})
	id := strconv.FormatInt(res.ID, 10)

	// The pages that offer actions offer none, and say why.
	operate := handler(t, client, nil)
	for _, page := range []string{"/", "/schedules", "/tasks/" + id} {
		if _, body, _ := get(t, operate, page); !strings.Contains(body, `method="post"`) {
			t.Fatalf("%s offers no action that a read-only dashboard could hide\n%s", page, body)
		}
		if _, body, _ := get(t, h, page); strings.Contains(body, `method="post"`) || !strings.Contains(body, "read-only") {
			t.Errorf("read-only %s offers an action, or does not say that it is read-only\n%s", page, body)
		}
	}
	if _, body, _ := get(t, h, "/tasks?queue=default"); strings.Contains(body, "/tasks/bulk/") {
		t.Errorf("read-only task list offers bulk actions:\n%s", body)
	}
	if code, _, _ := get(t, h, "/tasks/bulk/cancel?queue=default"); code != http.StatusForbidden {
		t.Errorf("read-only bulk cancel page: %d", code)
	}
	// Every action is a POST, and each is refused.
	for _, route := range postRoutes(t) {
		if rec := post(t, h, route, "same-origin"); rec.Code != http.StatusForbidden {
			t.Errorf("read-only POST %s: %d", route, rec.Code)
		}
	}
}

// postRoutes returns a path for each POST route of the dashboard. It reads
// the routes from where New registers them, so it never misses a new one.
func postRoutes(t *testing.T) []string {
	t.Helper()
	src, err := os.ReadFile("ui.go")
	if err != nil {
		t.Fatal(err)
	}
	wildcard := regexp.MustCompile(`\{[^}]*\}`)
	var paths []string
	for _, m := range regexp.MustCompile(`mux\.Handle(?:Func)?\("POST (/[^"]*)"`).FindAllStringSubmatch(string(src), -1) {
		paths = append(paths, wildcard.ReplaceAllString(m[1], "1"))
	}
	if len(paths) == 0 {
		t.Fatal("ui.go registers no POST route in the form that this test reads")
	}
	return paths
}

// An action with a value that is not valid is a bad request, as a page with
// the same value is. Only an action that does not fit the task as it is now
// is a conflict.
func TestActionsRefuseValuesThatAreNotValid(t *testing.T) {
	t.Parallel()
	h := handler(t, setup(t), nil)
	for _, target := range []string{
		"/tasks/bulk/retry?state=bogus", "/tasks/bulk/retry?state=dead&cursor=zzz",
		"/queues/pause", "/queues/resume?name=%ff", "/schedules/run?name=",
	} {
		rec := post(t, h, target, "same-origin")
		if body := rec.Body.String(); rec.Code != http.StatusBadRequest || !strings.Contains(body, "<h1>Invalid request</h1>") {
			t.Errorf("POST %s: status %d, want 400 and a page that says so\n%s", target, rec.Code, body)
		}
	}
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

// A queue or a schedule may have any name, and the buttons of its row must
// act on it whatever the name holds: a colon, which starts an address with a
// scheme; dots, which a browser resolves out of a path; the characters that
// end a path or an attribute; or a line break. The test sends each form as
// the page renders it, to the address that a browser makes of its action.
func TestActionsWorkForAnyName(t *testing.T) {
	t.Parallel()
	client := setup(t)
	names := []string{"emails:high", ".", "..", "a/b?c#d%2Fe", `x"><script>`, "two\nlines"}
	schedules := make([]taskmill.Schedule, len(names))
	for i, name := range names {
		if err := client.UpdateQueue(t.Context(), name, func(*taskmill.QueueSettings) {}); err != nil {
			t.Fatal(err)
		}
		schedules[i] = taskmill.Schedule{Name: name, Cron: "@daily", Args: greet{}}
	}
	storeSchedules(t, client, schedules)
	h := handler(t, client, &ui.Config{Prefix: "/admin"})
	due := func(sc taskmill.ScheduleInfo) bool { return !sc.NextRunAt.After(time.Now()) }
	paused := func(sc taskmill.ScheduleInfo) bool { return sc.Paused }

	for _, name := range names {
		send(t, h, rowForms(t, h, "/admin/", name)["Pause"], http.StatusSeeOther)
		if got := pausedQueues(t, client); !slices.Equal(got, []string{name}) {
			t.Fatalf("after Pause on the queue %q, the queues %q are paused", name, got)
		}
		send(t, h, rowForms(t, h, "/admin/", name)["Resume"], http.StatusSeeOther)
		if got := pausedQueues(t, client); len(got) != 0 {
			t.Fatalf("after Resume on the queue %q, the queues %q are paused", name, got)
		}

		before := rowForms(t, h, "/admin/schedules", name)
		send(t, h, before["Run now"], http.StatusSeeOther)
		if got := schedulesThat(t, client, due); !slices.Contains(got, name) {
			t.Fatalf("after Run now on the schedule %q, the schedules %q are due", name, got)
		}
		send(t, h, before["Pause"], http.StatusSeeOther)
		if got := schedulesThat(t, client, paused); !slices.Equal(got, []string{name}) {
			t.Fatalf("after Pause on the schedule %q, the schedules %q are paused", name, got)
		}
		send(t, h, before["Run now"], http.StatusConflict) // a paused schedule does not run
		send(t, h, rowForms(t, h, "/admin/schedules", name)["Resume"], http.StatusSeeOther)
		if got := schedulesThat(t, client, paused); len(got) != 0 {
			t.Fatalf("after Resume on the schedule %q, the schedules %q are paused", name, got)
		}
	}
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
	tableRow  = regexp.MustCompile(`(?s)<tr data-key="([^"]*)">(.*?)</tr>`)
	rowButton = regexp.MustCompile(`(?s)<form method="post" action="([^"]*)"[^>]*><button[^>]*><svg.*?</svg>([^<]*)</button></form>`)
)

// rowForms returns where each button in the table row of name sends its
// form, on the page at target, by the button's text. It reads the form's
// action from the page, and resolves it against the page's address, as a
// browser does.
func rowForms(t *testing.T, h http.Handler, target, name string) map[string]string {
	t.Helper()
	page, err := url.Parse(target)
	if err != nil {
		t.Fatal(err)
	}
	_, body, _ := get(t, h, target)
	found := map[string]string{}
	for _, row := range tableRow.FindAllStringSubmatch(body, -1) {
		if html.UnescapeString(row[1]) != name {
			continue
		}
		for _, form := range rowButton.FindAllStringSubmatch(row[2], -1) {
			action, err := page.Parse(html.UnescapeString(form[1]))
			if err != nil {
				t.Fatalf("%s: the action %q in the row of %q: %v", target, form[1], name, err)
			}
			found[form[2]] = action.RequestURI()
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

func TestActionNotices(t *testing.T) {
	t.Parallel()
	client := setup(t)
	h := handler(t, client, nil)
	rec := post(t, h, "/queues/pause?name=default", "same-origin")
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
