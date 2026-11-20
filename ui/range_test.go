package ui_test

import (
	"html"
	"net/http"
	"net/url"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/code19m/taskmill/ui"
)

// storeFinished stores one task in the state for each time, which finished
// then. The tasks get the next IDs of the schema, in the order of the times.
func storeFinished(t *testing.T, pool *pgxpool.Pool, schema, state string, at ...time.Time) {
	t.Helper()
	_, err := pool.Exec(t.Context(), `INSERT INTO `+schema+`.tasks (queue, kind, state, run_at, payload, max_attempts, finalized_at)
		SELECT 'mail', 'greet', '`+state+`', now(), '{}', 1, at FROM unnest($1::timestamptz[]) WITH ORDINALITY AS u(at, i) ORDER BY i`, at)
	if err != nil {
		t.Fatal(err)
	}
}

// storeCreated stores one task in the state for each time, which was
// created then: a completed one in the history, and any other among the
// tasks, where a dead or a canceled one finished two hours later. The tasks
// get the IDs from first on, in the order of the times.
func storeCreated(t *testing.T, pool *pgxpool.Pool, schema, state string, first int, at ...time.Time) {
	t.Helper()
	sql := `INSERT INTO ` + schema + `.tasks (id, queue, kind, state, run_at, payload, max_attempts, created_at, finalized_at)
		OVERRIDING SYSTEM VALUE
		SELECT $2 + i - 1, 'mail', 'greet', '` + state + `', now(), '{}', 1, at,
		       CASE WHEN '` + state + `' IN ('dead', 'canceled') THEN at + interval '2 hours' END
		FROM unnest($1::timestamptz[]) WITH ORDINALITY AS u(at, i)`
	if state == "completed" {
		sql = `INSERT INTO ` + schema + `.task_history (id, finalized_at, queue, created_at, kind, run_at, attempted_by, attempted_at,
		                                              priority, attempt, max_attempts, payload, metadata, errors)
			SELECT $2 + i - 1, now(), 'mail', at, 'greet', now(), 'w', now(), 2, 1, 1, '{}', '{}', '[]'
			FROM unnest($1::timestamptz[]) WITH ORDINALITY AS u(at, i)`
	}

	if _, err := pool.Exec(t.Context(), sql, at, first); err != nil {
		t.Fatal(err)
	}
}

// utc reads a time of 4 March 2021 in UTC, a day whose year is never the
// current one: a page words it the same way on any day.
func utc(t *testing.T, clock string) time.Time {
	t.Helper()
	at, err := time.Parse(time.RFC3339, "2021-03-04T"+clock+"Z")
	if err != nil {
		t.Fatal(err)
	}
	return at
}

// tagOf returns the first tag of the page that the pattern matches.
func tagOf(t *testing.T, body, pattern string) string {
	t.Helper()
	tag := regexp.MustCompile(pattern).FindString(body)
	if tag == "" {
		t.Fatalf("the page has no tag %s\n%s", pattern, body)
	}
	return tag
}

// attrOf returns the value of the tag's attribute, and whether the tag has it.
func attrOf(tag, name string) (string, bool) {
	m := regexp.MustCompile(` ` + name + `="([^"]*)"`).FindStringSubmatch(tag)
	if m == nil {
		return "", false
	}
	return html.UnescapeString(m[1]), true
}

// rangeFilter is what the filter by time of a page says and holds.
type rangeFilter struct {
	name, label, full, title    string // on the button, the time and its range; for a screen reader; and in the tooltip
	since, before               string // the fields, as a browser without the script sends them
	sinceInstant, beforeInstant string // the instants that the script reads
	seconds                     bool   // a field takes seconds
}

// readRangeFilter reads the filter by time from a task list.
func readRangeFilter(t *testing.T, body string) rangeFilter {
	t.Helper()
	summary := tagOf(t, body, `<details class="sel sel-range">\s*<summary[^>]*>`)
	button := regexp.MustCompile(regexp.QuoteMeta(summary) + `<span class="sel-name">([^<]*)</span><span class="sel-label">([^<]*)<`).FindStringSubmatch(body)
	if button == nil {
		t.Fatalf("the button of the filter by time does not name its time and its range\n%s", body)
	}

	var f rangeFilter
	f.name, f.label = html.UnescapeString(button[1]), html.UnescapeString(button[2])
	f.full, _ = attrOf(summary, "aria-label")
	f.title, _ = attrOf(summary, "title")
	since, before := tagOf(t, body, `<input[^>]* name="since"[^>]*>`), tagOf(t, body, `<input[^>]* name="before"[^>]*>`)
	f.since, _ = attrOf(since, "value")
	f.before, _ = attrOf(before, "value")
	f.sinceInstant, _ = attrOf(since, "data-time")
	f.beforeInstant, _ = attrOf(before, "data-time")
	_, sinceSteps := attrOf(since, "step")
	_, beforeSteps := attrOf(before, "step")
	f.seconds = sinceSteps || beforeSteps
	return f
}

// A list in finish order takes a range of finish times: the tasks that
// finished at "since" or later, and before "before". Each bound is an
// instant, whatever form the address gives it: with "Z", with an offset, also
// one whose plus nobody escaped, or without a zone, which is UTC. The test's
// process is 13 hours and 45 minutes ahead of UTC, and its database sessions
// further: a bound read in either zone selects other tasks.
func TestTaskListTakesARangeOfFinishTimes(t *testing.T) {
	t.Parallel()
	client, pool, schema := setupDB(t)

	// Tasks 1, 2, and 3 died at 10:00, 11:00, and 12:00 UTC. Task 4 was
	// canceled at 11:30.
	storeFinished(t, pool, schema, "dead", utc(t, "10:00:00"), utc(t, "11:00:00"), utc(t, "12:00:00"))
	storeFinished(t, pool, schema, "canceled", utc(t, "11:30:00"))
	h := handler(t, client, nil)
	for _, c := range []struct {
		query string
		want  []string
	}{
		{"state=dead", []string{"3", "2", "1"}},
		{"state=dead&since=2021-03-04T11:00Z", []string{"3", "2"}},                   // a task that finished at the start is in the range
		{"state=dead&since=2021-03-04T11:00:01Z", []string{"3"}},                     // to the second
		{"state=dead&before=2021-03-04T11:00:00Z", []string{"1"}},                    // a task that finished at the end is not
		{"state=dead&since=2021-03-04T11:00&before=2021-03-04T12:00", []string{"2"}}, // no zone, as a field sends it: UTC
		{"state=dead&since=2021-03-04T16:00:00%2B05:00", []string{"3", "2"}},
		{"state=dead&since=2021-03-04T16:00+05:00", []string{"3", "2"}}, // the query reads this plus as a space
		{"state=dead&before=2021-03-04T05:15-05:45", []string{"1"}},
		{"state=dead&state=canceled&since=2021-03-04T10:30Z&before=2021-03-04T12:00Z", []string{"4", "2"}},
		{"state=canceled&since=&before=", []string{"4"}}, // the empty fields of the form: no bound
		{"state=dead&since=2021-03-04T12:00:01Z", nil},
	} {
		body, ids := listTasks(t, h, "/tasks?"+c.query)
		if !slices.Equal(ids, c.want) {
			t.Errorf("/tasks?%s lists the tasks %v, want %v", c.query, ids, c.want)
		}
		if c.want == nil && !strings.Contains(body, "No tasks match these filters") {
			t.Errorf("/tasks?%s: a range without tasks does not say that no task matches the filters\n%s", c.query, body)
		}
	}
}

// A list that is not in finish order takes a range of creation times: the
// tasks created at "since" or later, and before "before", of the states
// that the list selects, the completed ones too. With a range, the list
// comes in the order of that time, the newest first, and marks the column
// of that time: not in the order of the IDs, which here run the other way.
// Without one, it is in the order of the IDs.
func TestTaskListTakesARangeOfCreationTimes(t *testing.T) {
	t.Parallel()
	client, pool, schema := setupDB(t)

	// Tasks 1, 2, and 3 were created at 12:00, 11:00, and 10:00 UTC, and
	// wait. Task 4 was created at 11:30 and completed, and task 5 at 10:30,
	// and died.
	storeCreated(t, pool, schema, "available", 1, utc(t, "12:00:00"), utc(t, "11:00:00"), utc(t, "10:00:00"))
	storeCreated(t, pool, schema, "completed", 4, utc(t, "11:30:00"))
	storeCreated(t, pool, schema, "dead", 5, utc(t, "10:30:00"))
	h := handler(t, client, nil)
	for _, c := range []struct {
		query string
		want  []string
	}{
		{"since=2021-03-04T10:00Z", []string{"1", "4", "2", "5", "3"}},
		{"since=2021-03-04T11:00Z", []string{"1", "4", "2"}},                        // a task created at the start is in the range
		{"since=2021-03-04T11:00:01Z", []string{"1", "4"}},                          // to the second
		{"before=2021-03-04T11:00:00Z", []string{"5", "3"}},                         // a task created at the end is not
		{"since=2021-03-04T10:30&before=2021-03-04T12:00", []string{"4", "2", "5"}}, // no zone, as a field sends it: UTC
		{"since=2021-03-04T16:00:00%2B05:00", []string{"1", "4", "2"}},
		{"before=2021-03-04T05:15-05:45", []string{"5", "3"}},
		{"state=available&since=2021-03-04T10:30Z", []string{"1", "2"}},
		{"state=completed&before=2021-03-04T12:00Z", []string{"4"}},
		{"state=dead&state=available&since=2021-03-04T10:00Z&before=2021-03-04T11:30Z", []string{"2", "5", "3"}},
		{"queue=mail&kind=greet&since=2021-03-04T11:30Z", []string{"1", "4"}},
		{"since=2021-03-04T12:00:01Z", nil},
	} {
		body, ids := listTasks(t, h, "/tasks?"+c.query)
		if !slices.Equal(ids, c.want) {
			t.Errorf("/tasks?%s lists the tasks %v, want %v", c.query, ids, c.want)
		}
		if c.want == nil {
			if !strings.Contains(body, "No tasks match these filters") {
				t.Errorf("/tasks?%s: a range without tasks does not say that no task matches the filters\n%s", c.query, body)
			}
			continue
		}
		if column, order := sortedBy(t, body); column != "Created" || order != "Newest first" {
			t.Errorf("/tasks?%s marks its order on %q as %q, want Created, newest first", c.query, column, order)
		}
	}

	body, ids := listTasks(t, h, "/tasks")
	if column, _ := sortedBy(t, body); column != "ID" || !slices.Equal(ids, []string{"5", "4", "3", "2", "1"}) {
		t.Errorf("without a range, the list is %v and marks its order on %q, want the order of the IDs", ids, column)
	}
}

// The range in the address of a list is of the time that orders the list:
// of creation times, and of finish times on a list in finish order. So an
// address with a range stays one when the reader changes the states: the
// same "since" and "before" then select by the other time, with no redirect
// and no error. Every list shows the filter, and its button and its menu
// name the time.
func TestRangeIsOfTheTimeThatOrdersTheList(t *testing.T) {
	t.Parallel()
	client, pool, schema := setupDB(t)

	// Task 1 was created at 10:00 UTC and died at 12:00. Task 2 was created
	// at 12:00 and died at 14:00. Task 3 was created at 10:30, and waits.
	storeCreated(t, pool, schema, "dead", 1, utc(t, "10:00:00"), utc(t, "12:00:00"))
	storeCreated(t, pool, schema, "available", 3, utc(t, "10:30:00"))
	h := handler(t, client, &ui.Config{Prefix: "/admin"})
	const hour = "since=2021-03-04T11:30Z&before=2021-03-04T12:30Z"
	for _, c := range []struct {
		query, time, column string
		want                []string
	}{
		{"state=dead&" + hour, "Finished", "Finished", []string{"1"}}, // it died in the hour
		{"state=dead&state=canceled&" + hour, "Finished", "Finished", []string{"1"}},
		{"state=dead&state=available&" + hour, "Created", "Created", []string{"2"}}, // it was created in the hour
		{hour, "Created", "Created", []string{"2"}},
		{"queue=mail&since=2021-03-04T10:30Z&before=2021-03-04T12:00Z", "Created", "Created", []string{"3"}},
		{"state=dead", "Finished", "Finished", []string{"2", "1"}},
		{"state=dead&state=running", "Created", "ID", []string{"2", "1"}},
		{"", "Created", "ID", []string{"3", "2", "1"}},
	} {
		target := "/admin/tasks?" + c.query
		code, body, _ := get(t, h, target)
		if code != http.StatusOK {
			t.Errorf("GET %s: status %d, want the list", target, code)
			continue
		}
		_, ids := listTasks(t, h, target)
		column, _ := sortedBy(t, body)
		if !slices.Equal(ids, c.want) || column != c.column {
			t.Errorf("GET %s lists %v in the order of %q, want %v in the order of %q", target, ids, column, c.want, c.column)
		}
		filter := readRangeFilter(t, body)
		menu := tagOf(t, body, `<div class="pop range"[^>]*>`)
		if label, _ := attrOf(menu, "aria-label"); filter.name != c.time+":" || !strings.HasPrefix(filter.full, c.time+": ") || label != c.time {
			t.Errorf("GET %s: the filter by time is named %q on its button, %q for a screen reader, and %q on its menu, want %s",
				target, filter.name, filter.full, label, c.time)
		}
	}
}

// The filter by time says the range that is set, in UTC, after the name of
// its time: in short on its button, and with the years and the zone in the
// button's tooltip and for a screen reader. Its two fields hold the bounds
// in UTC, as a browser without the script sends them back, and each bound's
// instant, which the script shows in the reader's time zone. A bound that is
// not at a whole minute shows its seconds, and its field takes them.
func TestRangeFilterSaysItsRange(t *testing.T) {
	t.Parallel()
	h := handler(t, setup(t), nil)
	for list, time := range map[string]string{"/tasks?state=dead": "Finished", "/tasks?queue=mail": "Created"} {
		for _, c := range []struct {
			query string
			want  rangeFilter
		}{
			{"", rangeFilter{label: "Any time", full: "Any time"}},
			{"&since=2021-03-04T16:00:00%2B05:00", rangeFilter{
				label: "Since 4 Mar 2021 11:00", full: "Since 4 Mar 2021 11:00 UTC", title: "Since 4 Mar 2021 11:00 UTC",
				since: "2021-03-04T11:00", sinceInstant: "2021-03-04T11:00:00Z",
			}},
			{"&before=2021-03-04T11:00:30Z", rangeFilter{
				label: "Before 4 Mar 2021 11:00:30", full: "Before 4 Mar 2021 11:00:30 UTC", title: "Before 4 Mar 2021 11:00:30 UTC",
				before: "2021-03-04T11:00:30", beforeInstant: "2021-03-04T11:00:30Z", seconds: true,
			}},
			{"&since=2021-03-04T10:00&before=2021-03-04T11:30", rangeFilter{
				label: "4 Mar 2021 10:00 to 11:30", full: "4 Mar 2021 10:00 UTC to 4 Mar 2021 11:30 UTC",
				title: "4 Mar 2021 10:00 UTC to 4 Mar 2021 11:30 UTC",
				since: "2021-03-04T10:00", sinceInstant: "2021-03-04T10:00:00Z", before: "2021-03-04T11:30", beforeInstant: "2021-03-04T11:30:00Z",
			}},
			{"&since=2021-03-04T23:00Z&before=2021-03-05T00:00Z", rangeFilter{
				label: "4 Mar 2021 23:00 to 5 Mar 2021 00:00", full: "4 Mar 2021 23:00 UTC to 5 Mar 2021 00:00 UTC",
				title: "4 Mar 2021 23:00 UTC to 5 Mar 2021 00:00 UTC",
				since: "2021-03-04T23:00", sinceInstant: "2021-03-04T23:00:00Z", before: "2021-03-05T00:00", beforeInstant: "2021-03-05T00:00:00Z",
			}},
		} {
			c.want.name, c.want.full = time+":", time+": "+c.want.full
			_, body, _ := get(t, h, list+c.query)
			if got := readRangeFilter(t, body); got != c.want {
				t.Errorf("%s%s: the filter by time is\n%+v, want\n%+v", list, c.query, got, c.want)
			}
			if !strings.Contains(body, `Times in <span data-zone>UTC</span>`) {
				t.Errorf("%s%s: the fields do not say that their times are in UTC", list, c.query)
			}
		}
	}

	// A day of this year needs no year on the button. Around midnight of a
	// new year, the day that the test chose may be last year's when the page
	// renders: then the page is right to name the year.
	at := time.Now().UTC().Add(-time.Hour).Truncate(time.Minute)
	_, body, _ := get(t, h, "/tasks?state=dead&since="+at.Format("2006-01-02T15:04"))
	if got, want := readRangeFilter(t, body).label, "Since "+at.Format("2 Jan 15:04"); got != want && at.Year() == time.Now().UTC().Year() {
		t.Errorf("the button says %q for a range since an hour ago, want %q", got, want)
	}
}

// The filter by time offers presets, on a list in either order: links to the
// same list, with its other filters, from a time ago, on its first page. A
// preset holds that time as an instant with "Z", rounded down to the minute,
// and no end. "Any time" leads to the list without a range, and is marked
// while no range is set.
func TestRangeFilterOffersPresets(t *testing.T) {
	t.Parallel()
	h := handler(t, setup(t), nil)
	for _, states := range []string{"&state=canceled", ""} {
		started := time.Now()
		_, body, _ := get(t, h, "/tasks?queue=mail&limit=100&since=2021-03-04T11:00Z&before=2021-03-04T12:00Z"+states)
		rendered := time.Now()
		for label, last := range map[string]time.Duration{
			"Last hour": time.Hour, "Last 6 hours": 6 * time.Hour, "Last 24 hours": 24 * time.Hour,
			"Last 7 days": 7 * 24 * time.Hour, "Last 30 days": 30 * 24 * time.Hour,
		} {
			link, err := url.Parse(linkNamed(t, body, label))
			if err != nil {
				t.Fatal(err)
			}
			q := link.Query()
			since, err := time.Parse("2006-01-02T15:04:00Z", q.Get("since"))
			if err != nil || since.After(rendered.Add(-last)) || !since.After(started.Add(-last-time.Minute)) {
				t.Errorf("%q leads to the list since %q, want the minute that held the time %s before the page was rendered, as an instant in UTC",
					label, q.Get("since"), last)
			}
			if q.Del("since"); link.Path != "/tasks" || "&"+q.Encode() != "&queue=mail"+states {
				t.Errorf("%q leads to %s, want the first page of the list with its other filters, and no end of the range", label, link)
			}
		}
		current := `aria-current="true">Any time<`
		if link := linkNamed(t, body, "Any time"); link != "/tasks?queue=mail"+states || strings.Contains(body, current) {
			t.Errorf("\"Any time\" leads to %s, or is marked while a range is set\n%s", link, body)
		}
		if _, body, _ := get(t, h, "/tasks?queue=mail"+states); !strings.Contains(body, current) {
			t.Errorf("\"Any time\" is not marked on a list without a range\n%s", body)
		}
	}
}

// Every link of a list with a range keeps the range, as instants in UTC,
// whatever form the address gave it: "Load more", the page after 250 tasks,
// and the way back to the first page. So no page shows a task from outside
// the range. "Clear" removes the range with the other filters.
func TestTaskListKeepsItsRangeOfFinishTimes(t *testing.T) {
	t.Parallel()
	client, pool, schema := setupDB(t)

	// Task i of 260 died (7i mod 260) minutes before noon, as the tasks of
	// deadTasks do before now. Task 261 died a day before them, and task 262
	// a day after.
	noon := utc(t, "12:00:00")
	at, latestFirst := []time.Time{260: noon.Add(-24 * time.Hour), 261: noon.Add(24 * time.Hour)}, make([]string, 260)
	for i := 1; i <= 260; i++ {
		at[i-1] = noon.Add(-time.Duration(i*7%260) * time.Minute)
		latestFirst[i*7%260] = strconv.Itoa(i)
	}

	storeFinished(t, pool, schema, "dead", at...)
	h := handler(t, client, nil)

	// The range starts at the time of its first task, 07:41 UTC.
	const kept = "before=2021-03-04T12%3A01%3A00Z&since=2021-03-04T07%3A41%3A00Z&state=dead"
	body, ids := listTasks(t, h, "/tasks?state=dead&since=2021-03-04T12:41:00%2B05:00&before=2021-03-04T12:01")
	if !slices.Equal(ids, latestFirst[:50]) {
		t.Fatalf("the list starts with %v, want the 50 tasks that died last in the range", ids)
	}

	for want := 100; want <= 250; want += 50 {
		more := linkNamed(t, body, "Load more")
		if more != "/tasks?"+strings.Replace(kept, "&since", "&limit="+strconv.Itoa(want)+"&since", 1)+"#more" {
			t.Fatalf("after %d tasks, \"Load more\" leads to %s", want-50, more)
		}
		if body, ids = listTasks(t, h, more); !slices.Equal(ids, latestFirst[:want]) {
			t.Fatalf("%s shows %d tasks %v, want the %d that died last in the range", more, len(ids), ids, want)
		}
	}

	earlier := linkNamed(t, body, "Earlier")
	if link, err := url.Parse(earlier); err != nil || link.Query().Get("since") != "2021-03-04T07:41:00Z" || link.Query().Get("before") != "2021-03-04T12:01:00Z" {
		t.Fatalf("the page after 250 tasks is %s, want it with the range", earlier)
	}

	body, later := listTasks(t, h, earlier)
	if !slices.Equal(later, latestFirst[250:]) {
		t.Errorf("the page after 250 tasks shows %v, want the 10 that died first in the range", later)
	}

	if latest, cleared := linkNamed(t, body, "Latest"), linkNamed(t, body, "Clear"); latest != "/tasks?"+kept || cleared != "/tasks" {
		t.Errorf("the last page leads back to %s, want the first page with the range, and clears the filters at %s", latest, cleared)
	}
}

// A list in creation order keeps its range the same way: through "Load
// more", on the page after 250 tasks, which starts below the list's last
// task in the order of the creation times, and on the way back.
func TestTaskListKeepsItsRangeOfCreationTimes(t *testing.T) {
	t.Parallel()
	client, pool, schema := setupDB(t)

	// Task i of 260 was created (7i mod 260) minutes before noon. Task 261
	// was created a day before them, and task 262 a day after.
	noon := utc(t, "12:00:00")
	at, newestFirst := []time.Time{260: noon.Add(-24 * time.Hour), 261: noon.Add(24 * time.Hour)}, make([]string, 260)
	for i := 1; i <= 260; i++ {
		at[i-1] = noon.Add(-time.Duration(i*7%260) * time.Minute)
		newestFirst[i*7%260] = strconv.Itoa(i)
	}

	storeCreated(t, pool, schema, "available", 1, at...)
	h := handler(t, client, nil)

	const kept = "before=2021-03-04T12%3A01%3A00Z&queue=mail&since=2021-03-04T07%3A41%3A00Z"
	body, ids := listTasks(t, h, "/tasks?queue=mail&since=2021-03-04T12:41:00%2B05:00&before=2021-03-04T12:01")
	if !slices.Equal(ids, newestFirst[:50]) {
		t.Fatalf("the list starts with %v, want the 50 tasks that were created last in the range", ids)
	}

	for want := 100; want <= 250; want += 50 {
		more := linkNamed(t, body, "Load more")
		if more != "/tasks?"+strings.Replace(kept, "&queue", "&limit="+strconv.Itoa(want)+"&queue", 1)+"#more" {
			t.Fatalf("after %d tasks, \"Load more\" leads to %s", want-50, more)
		}
		if body, ids = listTasks(t, h, more); !slices.Equal(ids, newestFirst[:want]) {
			t.Fatalf("%s shows %d tasks %v, want the %d that were created last in the range", more, len(ids), ids, want)
		}
	}

	older := linkNamed(t, body, "Older")
	if link, err := url.Parse(older); err != nil || link.Query().Get("since") != "2021-03-04T07:41:00Z" || link.Query().Get("before") != "2021-03-04T12:01:00Z" {
		t.Fatalf("the page after 250 tasks is %s, want it with the range", older)
	}

	body, later := listTasks(t, h, older)
	if !slices.Equal(later, newestFirst[250:]) {
		t.Errorf("the page after 250 tasks shows %v, want the 10 that were created first in the range", later)
	}

	if newest, cleared := linkNamed(t, body, "Newest"), linkNamed(t, body, "Clear"); newest != "/tasks?"+kept || cleared != "/tasks" {
		t.Errorf("the last page leads back to %s, want the first page with the range, and clears the filters at %s", newest, cleared)
	}

	// The cursor of that page is one of the creation order: the same list
	// without the range, which is in the order of the IDs, refuses it.
	link, err := url.Parse(older)
	if err != nil {
		t.Fatal(err)
	}

	if code, body, _ := get(t, h, "/tasks?queue=mail&cursor="+link.Query().Get("cursor")); code != http.StatusBadRequest || !strings.Contains(body, "Back to the first page") {
		t.Errorf("the list without the range takes the cursor of the list with it: status %d\n%s", code, body)
	}
}

// A range that is not one gets a page that says so, and leads to the same
// list without the range: a value that is not a time in one of the forms, a
// time with a fraction of a second, a time that the links of the page could
// not hold, or a range that does not start before it ends. The page never
// shows the value.
func TestTaskListRefusesARangeThatIsNotOne(t *testing.T) {
	t.Parallel()
	h := handler(t, setup(t), nil)
	const order = "The range must start before it ends."
	for _, c := range []struct{ target, wrong, back string }{
		{"/tasks?state=dead&since=yesterday", "Since must be a time such as 2026-01-31T14:05Z", "/tasks?state=dead"},
		{"/tasks?queue=mail&state=dead&before=2021-03-04&limit=100", "Before must be a time", "/tasks?queue=mail&state=dead"},
		{"/tasks?state=dead&since=2021-02-30T00:00Z", "Since must be a time", "/tasks?state=dead"},
		{"/tasks?state=dead&since=2021-03-04T11:00:00.5Z", "Since must be a time", "/tasks?state=dead"},
		{"/tasks?state=dead&since=2021-03-04T11:00:00%2B5", "Since must be a time", "/tasks?state=dead"},
		{"/tasks?state=dead&before=0001-01-01T00:00:00Z", "Before must be a time", "/tasks?state=dead"},        // the zero time: no bound, to the client
		{"/tasks?state=dead&before=0001-01-01T05:00:00%2B05:00", "Before must be a time", "/tasks?state=dead"}, // the same instant
		{"/tasks?state=dead&since=9999-12-31T23:59:59-00:01", "Since must be a time", "/tasks?state=dead"},     // the year 10000, in UTC
		{"/tasks?state=dead&since=%3Cscript%3Ealert(1)%3C/script%3E", "Since must be a time", "/tasks?state=dead"},
		{"/tasks?state=dead&since=2021-03-04T11:00Z&before=2021-03-04T11:00Z", order, "/tasks?state=dead"},
		{"/tasks?state=dead&since=2021-03-04T16:00%2B05:00&before=2021-03-04T10:59Z", order, "/tasks?state=dead"},
		// A list in the order of the IDs, whose range is one of creation times.
		{"/tasks?before=never", "Before must be a time", "/tasks"},
		{"/tasks?queue=mail&state=available&since=2021-03-04T11:00Z&before=2021-03-04T10:00Z&limit=100", order, "/tasks?queue=mail&state=available"},
	} {
		code, body, _ := get(t, h, c.target)
		if code != http.StatusBadRequest || !strings.Contains(body, "<h1>Invalid request</h1>") || !strings.Contains(body, c.wrong) ||
			!strings.Contains(body, `<a class="btn primary" href="`+html.EscapeString(c.back)+`">`) || !strings.Contains(body, "Clear the range</a>") {
			t.Errorf("GET %s: status %d, want 400, a page that says %q, and a link to %s that clears the range\n%s", c.target, code, c.wrong, c.back, body)
		}
		if strings.Contains(body, "alert(1)") || strings.Contains(body, "yesterday") {
			t.Errorf("GET %s: the page shows the value that it refused\n%s", c.target, body)
		}
	}
}

// A browser without the script sends the form of the filters as the page
// gives it: the checked boxes, and for the range the two fields of its own,
// which hold the bounds in UTC. It gets the list that it had, with the same
// range, in either order. The fields of the script's picker have no name, so
// no browser sends them, with the script or without it.
func TestFiltersFormKeepsTheRangeWithoutTheScript(t *testing.T) {
	t.Parallel()
	h := handler(t, setup(t), nil)
	for _, state := range []string{"dead", "available"} {
		_, body, _ := get(t, h, "/tasks?state="+state+"&since=2021-03-04T15:00:00%2B05:00&before=2021-03-04T11:30:30Z")
		form := tagOf(t, body, `(?s)<form class="filters"[^>]*>.*?</form>`)
		sent := url.Values{}
		for _, field := range regexp.MustCompile(`<input[^>]*>`).FindAllString(form, -1) {
			name, named := attrOf(field, "name")
			if kind, _ := attrOf(field, "type"); !named || kind == "checkbox" && !strings.Contains(field, " checked") {
				continue
			}
			value, _ := attrOf(field, "value")
			sent.Add(name, value)
		}
		if got, want := sent.Encode(), "before=2021-03-04T11%3A30%3A30&since=2021-03-04T10%3A00&state="+state; got != want {
			t.Fatalf("the form sends %s, want %s", got, want)
		}
		action, _ := attrOf(form, "action")
		code, again, _ := get(t, h, action+"?"+sent.Encode())
		if got, want := readRangeFilter(t, again), readRangeFilter(t, body); code != http.StatusOK || got != want {
			t.Errorf("the list that the form leads to has status %d and the range\n%+v, want\n%+v", code, got, want)
		}
	}
}

// The menu of the filter by time holds what the script's picker
// reads: a calendar with a grid that its title names, a row of seven
// weekdays, and the two steps of its month, which are plain buttons, so that
// a click on one never sends the form; and for each bound a date and a time,
// which say what they are and how to write them. The page shows none of it
// without the script, where the browser's own fields say From and Before:
// their second bound is the one of the address.
func TestRangeFilterHoldsWhatItsPickerReads(t *testing.T) {
	t.Parallel()
	_, body, _ := get(t, handler(t, setup(t), nil), "/tasks?state=dead&state=canceled")
	menu := tagOf(t, body, `(?s)<details class="sel sel-range">.*?</details>`)

	grid := tagOf(t, menu, `<div[^>]* class="cal-grid"[^>]*>`)
	title, _ := attrOf(grid, "aria-labelledby")
	if role, _ := attrOf(grid, "role"); role != "grid" || title == "" || !regexp.MustCompile(`<span class="cal-title" id="`+title+`"`).MatchString(menu) {
		t.Errorf("the calendar is %s, want a grid that the title of the month names\n%s", grid, menu)
	}

	if week := tagOf(t, menu, `(?s)<div class="cal-row cal-week"[^>]*>.*?</div>`); strings.Count(week, "<span>") != 7 {
		t.Errorf("the calendar's row of weekdays is %s, want seven days", week)
	}

	for step, name := range map[string]string{"-1": "Previous month", "1": "Next month"} {
		button := tagOf(t, menu, `<button[^>]* data-cal-step="`+step+`"[^>]*>`)
		kind, _ := attrOf(button, "type")
		if label, _ := attrOf(button, "aria-label"); kind != "button" || label != name || !strings.Contains(button, `class="cal-nav"`) {
			t.Errorf("the step %s of the month is %s, want a plain button named %q", step, button, name)
		}
	}

	type field struct{ bound, label, placeholder string }
	var fields []field
	for _, tag := range regexp.MustCompile(`<input[^>]* data-bound="[^"]*"[^>]*>`).FindAllString(menu, -1) {
		var f field
		f.bound, _ = attrOf(tag, "data-bound")
		f.label, _ = attrOf(tag, "aria-label")
		f.placeholder, _ = attrOf(tag, "placeholder")
		fields = append(fields, f)
	}

	want := []field{
		{"since", "From, date", "YYYY-MM-DD"},
		{"since", "From, time", "HH:MM"},
		{"before", "To, date", "YYYY-MM-DD"},
		{"before", "To, time", "HH:MM"},
	}
	if !slices.Equal(fields, want) {
		t.Errorf("the bounds of the picker are\n%+v, want\n%+v", fields, want)
	}

	for _, part := range []string{`<div class="cal js-only">`, `<div class="bounds js-only">`} {
		if !strings.Contains(menu, part) {
			t.Errorf("the menu has no %s: the picker must not show without the script", part)
		}
	}

	for name, label := range map[string]string{"since": "From", "before": "Before"} {
		if !regexp.MustCompile(`<label class="field">` + label + `<input[^>]* type="datetime-local" name="` + name + `"`).MatchString(menu) {
			t.Errorf("without the script, the menu has no field %q for %s\n%s", label, name, menu)
		}
	}
}
