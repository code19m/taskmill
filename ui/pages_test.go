package ui_test

import (
	"context"
	"errors"
	"fmt"
	"html"
	"log/slog"
	"math"
	"net"
	"net/http"
	"os"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
	"unicode"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/code19m/taskmill"
	"github.com/code19m/taskmill/internal/testdb"
	"github.com/code19m/taskmill/ui"
)

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
		"/tasks/0":                     `data-dialog-title="Task not found"`,
		"/tasks/bulk/retry?state=dead": `data-dialog-title="Retry tasks"`,
		"/tasks/bulk/retry":            `data-dialog-title="Choose filters first"`,
	} {
		_, body, _ := get(t, h, path)
		for _, part := range []string{
			`<main id="main"`, ` data-now="`, `<div class="main">`, `data-live-part="nav"`, "data-announce",
			"<template data-failure>", "<template data-unreachable>", `id="confirm-dialog"`, "data-confirm-status", "data-confirm-text", "data-confirm-button",
			title,
		} {
			if !strings.Contains(body, part) {
				t.Errorf("%s: the page has no %s", path, part)
			}
		}
		// The page that the script shows when no answer comes is a template, not part of the page.
		if title == "" && strings.Contains(regexp.MustCompile(`(?s)<template data-unreachable>.*?</template>`).ReplaceAllString(body, ""), "data-dialog-title") {
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
		keepQueue(ctx, client, "default"),
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
	code, sheet, _ := get(t, handler(t, setup(t), nil), "/static/any/style.css")
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

// The light theme is written twice: for a reader who chose it, and for a
// device that prefers it, without the script. Plain CSS cannot share one
// block between an attribute and a media query, so the two must stay equal.
func TestStylesWriteTheLightThemeTwiceAlike(t *testing.T) {
	t.Parallel()
	sheet, err := os.ReadFile("static/style.css")
	if err != nil {
		t.Fatal(err)
	}

	chosen := regexp.MustCompile(`(?s):root\[data-theme="light"\] \{(.*?)\}`).FindSubmatch(sheet)
	preferred := regexp.MustCompile(`(?s)\(prefers-color-scheme: light\) \{\s*:root:not\(\[data-theme\]\) \{(.*?)\}`).FindSubmatch(sheet)
	if chosen == nil || preferred == nil {
		t.Fatal("the stylesheet has no light theme in the form that this test reads")
	}

	if a, b := strings.Fields(string(chosen[1])), strings.Fields(string(preferred[1])); !slices.Equal(a, b) {
		t.Errorf("the two light themes differ:\n%s\n%s", strings.Join(a, " "), strings.Join(b, " "))
	}
}

// Every page renders under a prefix, with its links under it, for a task
// whose payload holds markup, whose priority is not normal, and whose FIFO
// group key the outbox made. Each case is one page.
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

	for _, c := range []struct {
		name, path string
		code       int
		want, not  []string // in the page, and not in it
	}{
		{
			"overview: the queue's limit, and the count of queues beside the card's title", "/admin/tasks/", http.StatusOK,
			[]string{"mail", "max 3", `<h2 id="queues-title">Queues <span class="subtle">1</span></h2>`},
			nil,
		},
		{"schedules", "/admin/tasks/schedules", http.StatusOK, nil, nil},
		{"workers", "/admin/tasks/workers", http.StatusOK, nil, nil},
		{
			"task list: the task, with its priority", "/admin/tasks/tasks?queue=mail&state=available", http.StatusOK,
			[]string{`href="/admin/tasks/tasks/` + id + `"`, `title="Priority: high">high</span>`},
			nil,
		},
		{
			"task page: the payload escaped, the priority's name, a visible link to the group, and the count of errors", "/admin/tasks/tasks/" + id, http.StatusOK,
			[]string{"&lt;script&gt;", "<dd>high</dd>", `title="Outbox topic topic, key key"`, "group=topic%1Fkey", `<h2>Errors <span class="subtle">0</span></h2>`},
			[]string{"<script>alert"},
		},
		{
			"the tasks of a group whose key has a control character", "/admin/tasks/tasks?queue=mail&group=topic%1Fkey", http.StatusOK,
			[]string{`href="/admin/tasks/tasks/` + id + `"`},
			nil,
		},
		{"an unknown state", "/admin/tasks/tasks?state=bogus", http.StatusBadRequest, nil, nil},
		{"an unknown task", "/admin/tasks/tasks/999999", http.StatusNotFound, nil, nil},
		{"the stylesheet", "/admin/tasks/static/any/style.css", http.StatusOK, nil, nil},
	} {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			code, body, _ := get(t, h, c.path)
			if code != c.code {
				t.Fatalf("%s: %d, want %d", c.path, code, c.code)
			}

			for _, part := range c.want {
				if !strings.Contains(body, part) {
					t.Errorf("%s: want %s in\n%s", c.path, part, body)
				}
			}

			for _, part := range c.not {
				if strings.Contains(body, part) {
					t.Errorf("%s: want no %s in\n%s", c.path, part, body)
				}
			}
		})
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
		relative = regexp.MustCompile(`<[^<>]*>(?:<svg[^<>]*>(?:<[^<>]*>)*</svg>)?[^<>]*(?:\d+[smhd] ago|in \d+[smhd])`) // with the tag that it stands in, and its icon
		exact    = regexp.MustCompile(`^<[^<>]* title="\d{4}-\d\d-\d\d \d\d:\d\d:\d\d\.\d{3} UTC" data-time="[^"]+Z">`)
	)
	for path, want := range map[string]int{
		"/":                                   1, // the age of the counts
		"/tasks":                              2, // created, and due
		"/tasks?state=dead":                   2, // created, and finished
		"/tasks/" + strconv.FormatInt(id, 10): 5, // created, due, started, and died, and the error
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

// The overview's Waiting card counts the tasks that wait for their time, for
// a retry, or behind another task of their FIFO group, and it leads to the
// list of all of them.
func TestOverviewWaitingCardLeadsToEveryWaitingTask(t *testing.T) {
	t.Parallel()
	client, pool, schema := setupDB(t)
	ctx := t.Context()
	res, err := client.EnqueueMany(ctx, []taskmill.EnqueueParams{
		{Args: greet{Name: "later"}, Options: &taskmill.EnqueueOptions{RunAt: time.Now().Add(time.Hour)}},
		{Args: greet{Name: "retried"}},
		{Args: greet{Name: "first"}, Options: &taskmill.EnqueueOptions{GroupKey: "g"}},
		{Args: greet{Name: "behind"}, Options: &taskmill.EnqueueOptions{GroupKey: "g"}},
	})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := pool.Exec(ctx, `UPDATE `+schema+`.tasks SET state = 'retryable', attempt = 1, run_at = now() + interval '1 hour' WHERE id = $1`, res[1].ID); err != nil {
		t.Fatal(err)
	}

	h := handler(t, client, nil)
	_, body, _ := get(t, h, "/")
	var link string
	for _, card := range regexp.MustCompile(`(?s)<a class="card stat[^"]*" href="([^"]*)">(.*?)</a>`).FindAllStringSubmatch(body, -1) {
		if strings.Contains(card[2], "Waiting</span>") {
			link = html.UnescapeString(card[1])
		}
	}
	if link == "" {
		t.Fatalf("the overview has no Waiting card\n%s", body)
	}

	_, ids := listTasks(t, h, link)
	want := []string{strconv.FormatInt(res[0].ID, 10), strconv.FormatInt(res[1].ID, 10), strconv.FormatInt(res[3].ID, 10)}
	slices.Sort(ids)
	slices.Sort(want)
	if !slices.Equal(ids, want) {
		t.Errorf("the Waiting card leads to %s, which lists the tasks %v, want the scheduled, retryable, and pending %v", link, ids, want)
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

// Every worker that stores schedules lists them, beside its queues and
// kinds: a worker without kinds, such as a maintainer that only does
// maintenance, has a count of its schedules in their place, which opens the
// list. A worker whose schedules differ from the maintainer's, by name, has a
// mark beside its ID, which opens the list too. There, each schedule that only
// one of the two defines says so, and comes first. A worker that stores no
// schedules, which takes no part in the election or runs an older release,
// and a stale worker, which the maintainer does not compare either, have no
// mark.
func TestWorkersListTheirSchedulesAndMarkThoseThatDiffer(t *testing.T) {
	t.Parallel()
	client, pool, schema := setupDB(t)
	if _, err := pool.Exec(t.Context(), `INSERT INTO `+schema+`.workers
	        (id, queues, kinds, hostname, version, lease_duration, schedules, schedule_hashes) VALUES
	    ('lead', '{}', '{}', 'h', 'v', '30 seconds', '{carts,nightly}', '{a,b}'),
	    ('same', '{mail}', '{email.send}', 'h', 'v', '30 seconds', '{carts,nightly}', '{a,b}'),
	    ('other', '{}', '{}', 'h', 'v', '30 seconds', '{archive,nightly}', '{c,b}'),
	    ('none', '{mail}', '{email.send}', 'h', 'v', '30 seconds', NULL, NULL),
	    ('gone', '{}', '{}', 'h', 'v', '30 seconds', '{}', '{}');
	    UPDATE `+schema+`.workers SET heartbeat_at = now() - interval '1 minute' WHERE id = 'gone';
	    INSERT INTO `+schema+`.leader (name, worker_id, elected_at, expires_at)
	    VALUES ('maintainer', 'lead', now(), now() + interval '1 minute')`); err != nil {
		t.Fatal(err)
	}

	_, body, _ := get(t, handler(t, client, nil), "/workers")
	rows := map[string]string{}
	for _, row := range tableRow.FindAllStringSubmatch(body, -1) {
		rows[row[1]] = row[2]
	}

	const mark = `<button type="button" class="chip" popovertarget="lists-other" title="Its schedules differ from the maintainer's">schedules differ</button>`
	for _, c := range []struct {
		worker    string
		want, not []string
	}{
		{"other", []string{
			mark, `popovertarget="lists-other">2 schedules<`, `id="lists-other" popover`,
			`<h2 class="wide">Schedules <span class="subtle">3</span></h2><ul class="wide">` +
				`<li class="item">carts <span class="subtle">missing</span></li>` +
				`<li class="item">archive <span class="subtle">not on the maintainer</span></li>` +
				`<li class="item">nightly</li></ul>`,
		}, nil},
		{"same", []string{`popovertarget="lists-same">1 kind<`, `<h2 class="wide">Schedules <span class="subtle">2</span></h2>`, `<li class="item">carts</li>`}, []string{"schedules differ", "missing"}},
		{"lead", []string{`popovertarget="lists-lead">2 schedules<`, `id="lists-lead" popover`, `<li class="item">nightly</li>`}, []string{"schedules differ", "missing"}},
		{"none", nil, []string{"schedules differ", "<h2 class=\"wide\">"}},
		{"gone", []string{`<span class="tone-ghost">none</span>`}, []string{"schedules differ", "popover"}},
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
	const parts = `<span><span class="topic">orders</span><span class="topic-sep" aria-hidden="true">›</span>order-13</span>`
	for path, wants := range map[string][]string{
		// The filter's options, and each task's group beside its queue.
		"/tasks?queue=mail": {
			`<label class="pop-opt" data-key="` + key + `"><input type="checkbox" class="chk-input" name="group" value="` + key + `">`,
			`<span class="pop-text mono">` + parts + `</span>`,
			`<td title="Outbox topic orders, key order-13">mail<span class="group">` + parts + `</span></td>`,
			`<span class="pop-text mono">a␟b␟c</span>`, `<span class="pop-text mono">tab␉here␟key</span>`,
			`<span class="pop-text mono">␟no-topic</span>`, `<span class="pop-text mono">plain</span>`,
			`<td title="FIFO group a␟b␟c">mail<span class="group">a␟b␟c</span></td>`,
		},
		// The filter's button, when the key is the one choice.
		"/tasks?group=orders%1Forder-13": {
			`aria-label="FIFO group: Outbox topic orders, key order-13"><span class="sel-label">` + parts + `</span>`,
			`name="group" value="` + key + `" checked>`,
		},
		"/tasks?group=a%1Fb%1Fc": {`aria-label="FIFO group: a␟b␟c"><span class="sel-label">a␟b␟c</span>`},
		// The task's page, and the page of an action on the group's tasks.
		"/tasks/" + strconv.FormatInt(id, 10):        {`<a class="mono" href="/tasks?group=orders%1Forder-13&amp;queue=mail" title="Outbox topic orders, key order-13">` + parts + `</a>`},
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

// sneaky is a kind whose name holds a character of no width.
type sneaky struct{}

func (sneaky) Kind() string { return "pay\u200bment" }

// A name can hold characters that no one could see: control characters, and
// format characters, which can reverse the direction of the text after them
// or have no width. Every page shows them, in queue names, kinds, FIFO group
// keys, metadata, schedules, and workers, as code points or control
// pictures, so that a name cannot hide or reorder a part of itself. Only the
// attributes that carry a name to the server, or to the script, keep it as
// it is.
func TestNamesShowTheCharactersThatNoOneCouldSee(t *testing.T) {
	t.Parallel()
	client, pool, schema := setupDB(t)
	const queue, group = "mail\u202eliam", "orders\u202e\x1fkey\u2066"
	res, err := client.Enqueue(t.Context(), sneaky{}, &taskmill.EnqueueOptions{
		Queue: queue, GroupKey: group, Metadata: map[string]string{"trace\ufeff": "id\x7f\u0085"},
	})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := pool.Exec(t.Context(), `INSERT INTO `+schema+`.schedules (name, queue, kind, cron, timezone, next_run_at)
	    VALUES ('night`+"\u202e"+`ly', $1, $2, '0 3 * * *', 'UTC', now())`, queue, sneaky{}.Kind()); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(t.Context(), `INSERT INTO `+schema+`.workers (id, queues, kinds, hostname, version)
	    VALUES ($1, $2, $3, $4, 'v')`, "web\u200f-1-ab12", []string{queue}, []string{sneaky{}.Kind()}, "web\u200f"); err != nil {
		t.Fatal(err)
	}

	h := handler(t, client, nil)
	carried := regexp.MustCompile(` (?:data-key|value|id|popovertarget)="[^"]*"`)
	for _, target := range []string{"/", "/tasks", "/tasks/" + strconv.FormatInt(res.ID, 10), "/schedules", "/workers"} {
		code, body, _ := get(t, h, target)
		if code != http.StatusOK || !strings.Contains(body, "&lt;U&#43;202E&gt;") {
			t.Errorf("%s (%d): no name shows its U+202E", target, code)
		}
		shown := carried.ReplaceAllString(body, "")
		if at := strings.IndexFunc(shown, func(r rune) bool { return unicode.In(r, unicode.Cf) || r == 0x7f || r == 0x85 }); at >= 0 {
			t.Errorf("%s: a character that no one could see is on the page\n%s", target, shown[max(0, at-200):min(len(shown), at+200)])
		}
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

// breakReads breaks the connection of each read on its pool that names its
// key, before the read starts, as a network that fails does.
type breakReads struct {
	key    string
	broken atomic.Int64
}

func (b *breakReads) breakIf(conn *pgx.Conn, args []any) {
	if strings.Contains(fmt.Sprint(args...), b.key) && conn.PgConn().Conn().Close() == nil {
		b.broken.Add(1)
	}
}

func (b *breakReads) TraceQueryStart(ctx context.Context, conn *pgx.Conn, d pgx.TraceQueryStartData) context.Context {
	b.breakIf(conn, d.Args)
	return ctx
}

func (*breakReads) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

func (b *breakReads) TraceBatchStart(ctx context.Context, conn *pgx.Conn, d pgx.TraceBatchStartData) context.Context {
	for _, q := range d.Batch.QueuedQueries {
		b.breakIf(conn, q.Arguments)
	}
	return ctx
}

func (*breakReads) TraceBatchQuery(context.Context, *pgx.Conn, pgx.TraceBatchQueryData) {}
func (*breakReads) TraceBatchEnd(context.Context, *pgx.Conn, pgx.TraceBatchEndData)     {}

// A task's page says how the task stands in its FIFO group. A dead task
// halts its group whether or not tasks wait behind it, since a task added
// later waits too: the page says so, and how many wait. A page whose group
// cannot be read shows the task without it, says so, and logs why.
func TestTaskPagesSayHowTheTaskStandsInItsGroup(t *testing.T) {
	t.Parallel()
	failing := &breakReads{key: "group-that-fails"}
	client, pool, schema := setupTraced(t, failing)
	ctx := t.Context()
	var params []taskmill.EnqueueParams
	for _, group := range []string{"full", "full", "full", "alone", failing.key, failing.key} {
		params = append(params, taskmill.EnqueueParams{Args: greet{Name: "x"}, Options: &taskmill.EnqueueOptions{GroupKey: group}})
	}
	res, err := client.EnqueueMany(ctx, params)
	if err != nil {
		t.Fatal(err)
	}

	for _, head := range []int64{res[0].ID, res[3].ID} {
		if _, err := pool.Exec(ctx, `UPDATE `+schema+`.tasks SET state = 'dead', finalized_at = now() WHERE id = $1`, head); err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, `UPDATE `+schema+`.groups g SET halted_task_id = t.id, halted_at = now()
		    FROM `+schema+`.tasks t WHERE t.id = $1 AND g.queue = t.queue AND g.group_key = t.group_key`, head); err != nil {
			t.Fatal(err)
		}
	}

	logs := &actionLog{}
	h := handler(t, client, &ui.Config{Logger: slog.New(logs)})
	for _, tc := range []struct {
		task  int64
		wants []string
	}{
		{res[0].ID, []string{"This task halts its FIFO group.</strong>", "2 tasks wait behind it until you retry or delete it."}},
		{res[3].ID, []string{"This task halts its FIFO group.</strong>", "A task added to the group waits behind it until you retry or delete it."}},
		{res[5].ID, []string{"<h1 class=\"task-heading\"><span>#" + strconv.FormatInt(res[5].ID, 10), "Its place in its FIFO group could not be read."}},
	} {
		code, body, _ := get(t, h, "/tasks/"+strconv.FormatInt(tc.task, 10))
		for _, want := range tc.wants {
			if code != http.StatusOK || !strings.Contains(body, want) {
				t.Errorf("task %d (%d): want %s on its page", tc.task, code, want)
			}
		}
	}

	logs.mu.Lock()
	defer logs.mu.Unlock()
	if failing.broken.Load() == 0 || !slices.Contains(logs.messages, "read FIFO group failed") {
		t.Errorf("broke %d reads of the group, and logged %q; want a warning for each", failing.broken.Load(), logs.messages)
	}
}

// The page of a dead task finds the FIFO group that the task halts among the
// halted groups of its queue whose keys contain the task's key, in one read
// of up to 1,000 of them, as many round trips as for a key that no other
// contains: an open page reads it at every update. Past those, the page says
// that the task's place in its group could not be read.
func TestTaskPageReadsBoundedHaltedGroups(t *testing.T) {
	t.Parallel()
	client, pool, schema, _, trips := setupCounting(t)
	ctx := t.Context()
	res, err := client.EnqueueMany(ctx, []taskmill.EnqueueParams{
		{Args: greet{Name: "x"}, Options: &taskmill.EnqueueOptions{GroupKey: "solo"}},
		{Args: greet{Name: "x"}, Options: &taskmill.EnqueueOptions{GroupKey: "k"}},
	})
	if err != nil {
		t.Fatal(err)
	}

	// The two tasks die and halt their groups, after 1,000 groups, each
	// halted by its one dead task, whose keys contain "k" and come first.
	if _, err := pool.Exec(ctx, `WITH dead AS (
	        INSERT INTO `+schema+`.tasks (run_at, state, max_attempts, queue, kind, group_key, payload, finalized_at)
	        SELECT now(), 'dead', 1, 'default', 'greet', 'ak' || lpad(n::text, 4, '0'), '{}', now() FROM generate_series(1, 1000) n
	        RETURNING id, queue, group_key)
	    INSERT INTO `+schema+`.groups (queue, group_key, tasks, halted_task_id, halted_at) SELECT queue, group_key, 1, id, now() FROM dead`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE `+schema+`.tasks SET state = 'dead', finalized_at = now() WHERE id = ANY($1)`,
		[]int64{res[0].ID, res[1].ID}); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE `+schema+`.groups g SET halted_task_id = t.id, halted_at = now()
	    FROM `+schema+`.tasks t WHERE t.id = ANY($1) AND g.queue = t.queue AND g.group_key = t.group_key`, []int64{res[0].ID, res[1].ID}); err != nil {
		t.Fatal(err)
	}

	logs := &actionLog{}
	h := handler(t, client, &ui.Config{Logger: slog.New(logs)})
	solo, k := "/tasks/"+strconv.FormatInt(res[0].ID, 10), "/tasks/"+strconv.FormatInt(res[1].ID, 10)
	if found, past := roundTripsOf(t, h, trips, solo), roundTripsOf(t, h, trips, k); past != found {
		t.Errorf("the page of a task behind 1,000 halted groups made %d round trips, want %d, as for a key that no other contains", past, found)
	}

	for target, want := range map[string]string{solo: "This task halts its FIFO group.", k: "Its place in its FIFO group could not be read."} {
		if _, body, _ := get(t, h, target); !strings.Contains(body, want) {
			t.Errorf("%s: want %q on the page", target, want)
		}
	}

	logs.mu.Lock()
	defer logs.mu.Unlock()
	if !slices.Contains(logs.messages, "read FIFO group failed") {
		t.Errorf("logged %q, want why the group could not be read", logs.messages)
	}
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

// roundTripsOf returns the round trips that a request for target makes.
func roundTripsOf(t *testing.T, h http.Handler, trips *roundTrips, target string) int64 {
	t.Helper()
	before := trips.n.Load()
	if code, _, _ := get(t, h, target); code != http.StatusOK {
		t.Fatalf("%s: %d", target, code)
	}
	return trips.n.Load() - before
}

// The overview of a longer period keeps its read of the activity for a while:
// each open overview would read the counts of every queue for hours again at
// each update. The overview of the hour reads it each time.
func TestOverviewKeepsTheActivityOfALongerPeriod(t *testing.T) {
	t.Parallel()
	client, _, _, _, trips := setupCounting(t)
	h := handler(t, client, nil)
	for _, target := range []string{"/?range=6h", "/?range=24h"} {
		// A read is kept for its window while as many of its steps are
		// complete: a step can end between two requests, but not again
		// before a third.
		first := roundTripsOf(t, h, trips, target)
		second, third := roundTripsOf(t, h, trips, target), roundTripsOf(t, h, trips, target)
		if second >= first && third >= first {
			t.Errorf("%s: the overviews after the first made %d round trips too, want one fewer: the activity is kept", target, first)
		}
	}

	if first, second := roundTripsOf(t, h, trips, "/"), roundTripsOf(t, h, trips, "/"); second != first {
		t.Errorf("the overview of the hour made %d round trips, then %d: it reads the activity each time", first, second)
	}
}

// While a read of the task list's filter choices fails, the choices of that
// read serve the next task lists for a few seconds: each would otherwise add
// three reads that fail to its own.
func TestTaskListsKeepAFailedReadOfTheirChoicesForAWhile(t *testing.T) {
	t.Parallel()
	client, pool, schema, _, trips := setupCounting(t)
	if _, err := pool.Exec(t.Context(), `ALTER TABLE `+schema+`.schedules RENAME TO schedules_gone`); err != nil {
		t.Fatal(err)
	}

	h := handler(t, client, nil)
	if first, second := roundTripsOf(t, h, trips, "/tasks"), roundTripsOf(t, h, trips, "/tasks"); second > 2 {
		t.Errorf("the task lists made %d round trips, then %d, want at most 2 the second time: the tasks, and the queues", first, second)
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
		{"/tasks/" + strconv.FormatInt(ids[0], 10), 3, "the task, its group, and the queues"},
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
