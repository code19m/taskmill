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

// finishFilter is what the filter by finish time of a page says and holds.
type finishFilter struct {
	label, full, title          string // on the button, for a screen reader, and in the tooltip
	since, before               string // the fields, as a browser without the script sends them
	sinceInstant, beforeInstant string // the instants that the script reads
	seconds                     bool   // a field takes seconds
}

// readFinishFilter reads the filter by finish time from a task list.
func readFinishFilter(t *testing.T, body string) finishFilter {
	t.Helper()
	summary := tagOf(t, body, `<details class="sel sel-range">\s*<summary[^>]*>`)
	label := regexp.MustCompile(regexp.QuoteMeta(summary) + `<span class="sel-label">([^<]*)<`).FindStringSubmatch(body)
	if label == nil {
		t.Fatalf("the button of the filter by finish time has no label\n%s", body)
	}
	var f finishFilter
	f.label = html.UnescapeString(label[1])
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

// The filter by finish time says the range that is set, in UTC: in short on
// its button, and with the years and the zone in the button's tooltip and
// for a screen reader. Its two fields hold the bounds in UTC, as a browser
// without the script sends them back, and each bound's instant, which the
// script shows in the reader's time zone. A bound that is not at a whole
// minute shows its seconds, and its field takes them.
func TestFinishFilterSaysItsRange(t *testing.T) {
	t.Parallel()
	h := handler(t, setup(t), nil)
	for _, c := range []struct {
		query string
		want  finishFilter
	}{
		{"", finishFilter{label: "Any time", full: "Finished: Any time"}},
		{"&since=2021-03-04T16:00:00%2B05:00", finishFilter{
			label: "Since 4 Mar 2021 11:00", full: "Finished: Since 4 Mar 2021 11:00 UTC", title: "Since 4 Mar 2021 11:00 UTC",
			since: "2021-03-04T11:00", sinceInstant: "2021-03-04T11:00:00Z",
		}},
		{"&before=2021-03-04T11:00:30Z", finishFilter{
			label: "Before 4 Mar 2021 11:00:30", full: "Finished: Before 4 Mar 2021 11:00:30 UTC", title: "Before 4 Mar 2021 11:00:30 UTC",
			before: "2021-03-04T11:00:30", beforeInstant: "2021-03-04T11:00:30Z", seconds: true,
		}},
		{"&since=2021-03-04T10:00&before=2021-03-04T11:30", finishFilter{
			label: "4 Mar 2021 10:00 to 11:30", full: "Finished: 4 Mar 2021 10:00 UTC to 4 Mar 2021 11:30 UTC",
			title: "4 Mar 2021 10:00 UTC to 4 Mar 2021 11:30 UTC",
			since: "2021-03-04T10:00", sinceInstant: "2021-03-04T10:00:00Z", before: "2021-03-04T11:30", beforeInstant: "2021-03-04T11:30:00Z",
		}},
		{"&since=2021-03-04T23:00Z&before=2021-03-05T00:00Z", finishFilter{
			label: "4 Mar 2021 23:00 to 5 Mar 2021 00:00", full: "Finished: 4 Mar 2021 23:00 UTC to 5 Mar 2021 00:00 UTC",
			title: "4 Mar 2021 23:00 UTC to 5 Mar 2021 00:00 UTC",
			since: "2021-03-04T23:00", sinceInstant: "2021-03-04T23:00:00Z", before: "2021-03-05T00:00", beforeInstant: "2021-03-05T00:00:00Z",
		}},
	} {
		_, body, _ := get(t, h, "/tasks?state=dead"+c.query)
		if got := readFinishFilter(t, body); got != c.want {
			t.Errorf("/tasks?state=dead%s: the filter by finish time is\n%+v, want\n%+v", c.query, got, c.want)
		}
		if !strings.Contains(body, `Times in <span data-zone>UTC</span>`) {
			t.Errorf("/tasks?state=dead%s: the fields do not say that their times are in UTC", c.query)
		}
	}

	// A day of this year needs no year on the button. Around midnight of a
	// new year, the day that the test chose may be last year's when the page
	// renders: then the page is right to name the year.
	at := time.Now().UTC().Add(-time.Hour).Truncate(time.Minute)
	_, body, _ := get(t, h, "/tasks?state=dead&since="+at.Format("2006-01-02T15:04"))
	if got, want := readFinishFilter(t, body).label, "Since "+at.Format("2 Jan 15:04"); got != want && at.Year() == time.Now().UTC().Year() {
		t.Errorf("the button says %q for a range since an hour ago, want %q", got, want)
	}
}

// The filter by finish time offers presets: links to the same list, with its
// other filters, from a time ago, on its first page. A preset holds that
// time as an instant with "Z", rounded down to the minute, and no end. "Any
// time" leads to the list without a range, and is marked while no range is
// set.
func TestFinishFilterOffersPresets(t *testing.T) {
	t.Parallel()
	h := handler(t, setup(t), nil)
	started := time.Now()
	_, body, _ := get(t, h, "/tasks?state=canceled&queue=mail&limit=100&since=2021-03-04T11:00Z&before=2021-03-04T12:00Z")
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
		if q.Del("since"); link.Path != "/tasks" || q.Encode() != "queue=mail&state=canceled" {
			t.Errorf("%q leads to %s, want the first page of the list with its other filters, and no end of the range", label, link)
		}
	}
	current := `aria-current="true">Any time<`
	if link := linkNamed(t, body, "Any time"); link != "/tasks?queue=mail&state=canceled" || strings.Contains(body, current) {
		t.Errorf("\"Any time\" leads to %s, or is marked while a range is set\n%s", link, body)
	}
	if _, body, _ := get(t, h, "/tasks?state=canceled"); !strings.Contains(body, current) {
		t.Errorf("\"Any time\" is not marked on a list without a range\n%s", body)
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

// A range that is not one gets a page that says so, and leads to the same
// list without the range: a value that is not a time in one of the forms, a
// time with a fraction of a second, a time that the links of the page could
// not hold, or a range that does not start before it ends. The page never
// shows the value.
func TestTaskListRefusesARangeThatIsNotOne(t *testing.T) {
	t.Parallel()
	h := handler(t, setup(t), nil)
	const order = "the range of finish times must start before it ends"
	for _, c := range []struct{ target, wrong, back string }{
		{"/tasks?state=dead&since=yesterday", "since must be a time such as 2026-01-31T14:05Z", "/tasks?state=dead"},
		{"/tasks?queue=mail&state=dead&before=2021-03-04&limit=100", "before must be a time", "/tasks?queue=mail&state=dead"},
		{"/tasks?state=dead&since=2021-02-30T00:00Z", "since must be a time", "/tasks?state=dead"},
		{"/tasks?state=dead&since=2021-03-04T11:00:00.5Z", "since must be a time", "/tasks?state=dead"},
		{"/tasks?state=dead&since=2021-03-04T11:00:00%2B5", "since must be a time", "/tasks?state=dead"},
		{"/tasks?state=dead&before=0001-01-01T00:00:00Z", "before must be a time", "/tasks?state=dead"},        // the zero time: no bound, to the client
		{"/tasks?state=dead&before=0001-01-01T05:00:00%2B05:00", "before must be a time", "/tasks?state=dead"}, // the same instant
		{"/tasks?state=dead&since=9999-12-31T23:59:59-00:01", "since must be a time", "/tasks?state=dead"},     // the year 10000, in UTC
		{"/tasks?state=dead&since=%3Cscript%3Ealert(1)%3C/script%3E", "since must be a time", "/tasks?state=dead"},
		{"/tasks?state=dead&since=2021-03-04T11:00Z&before=2021-03-04T11:00Z", order, "/tasks?state=dead"},
		{"/tasks?state=dead&since=2021-03-04T16:00%2B05:00&before=2021-03-04T10:59Z", order, "/tasks?state=dead"},
		// A list that takes no range still refuses one that is not valid.
		{"/tasks?before=never", "before must be a time", "/tasks"},
	} {
		code, body, _ := get(t, h, c.target)
		if code != http.StatusBadRequest || !strings.Contains(body, "<h1>Invalid request</h1>") || !strings.Contains(body, c.wrong) ||
			!strings.Contains(body, `<a class="btn" href="`+html.EscapeString(c.back)+`">`) || !strings.Contains(body, "Clear the range</a>") {
			t.Errorf("GET %s: status %d, want 400, a page that says %q, and a link to %s that clears the range\n%s", c.target, code, c.wrong, c.back, body)
		}
		if strings.Contains(body, "alert(1)") || strings.Contains(body, "yesterday") {
			t.Errorf("GET %s: the page shows the value that it refused\n%s", c.target, body)
		}
	}
}

// Only a list in finish order takes a range, and only it shows the filter by
// finish time. A reader who sets a range, and then adds another state, asks
// for a list that takes none: the dashboard leads to the same list without
// the range, and keeps what else the address holds.
func TestListThatIsNotInFinishOrderDropsTheRange(t *testing.T) {
	t.Parallel()
	h := handler(t, setup(t), &ui.Config{Prefix: "/admin"})
	const shown = `<details class="sel sel-range">`
	for target, want := range map[string]string{
		"/admin/tasks?state=dead&state=available&queue=mail&since=2021-03-04T11:00Z": "/admin/tasks?queue=mail&state=dead&state=available",
		"/admin/tasks?before=2021-03-04T11:00&since=2021-03-04T10:00":                "/admin/tasks",
		"/admin/tasks?state=completed&limit=100&cursor=MTA&before=2021-03-04T11:00Z": "/admin/tasks?cursor=MTA&limit=100&state=completed",
	} {
		code, _, header := get(t, h, target)
		if code != http.StatusSeeOther || header.Get("Location") != want {
			t.Errorf("GET %s: status %d to %q, want 303 to %s", target, code, header.Get("Location"), want)
			continue
		}
		if code, body, _ := get(t, h, want); code != http.StatusOK || strings.Contains(body, shown) {
			t.Errorf("GET %s: status %d, want the list, without the filter by finish time\n%s", want, code, body)
		}
	}
	for target, want := range map[string]bool{
		"/admin/tasks?state=dead": true, "/admin/tasks?state=canceled&queue=mail": true, "/admin/tasks?state=canceled&state=dead": true,
		"/admin/tasks": false, "/admin/tasks?state=dead&state=running": false, "/admin/tasks?queue=mail": false,
	} {
		if code, body, _ := get(t, h, target); code != http.StatusOK || strings.Contains(body, shown) != want {
			t.Errorf("GET %s: status %d, the filter by finish time shows: %t, want %t", target, code, strings.Contains(body, shown), want)
		}
	}
}

// A browser without the script sends the form of the filters as the page
// gives it: the checked boxes, and for the range the two fields of its own,
// which hold the bounds in UTC. It gets the list that it had, with the same
// range. The fields of the script's picker have no name, so no browser sends
// them, with the script or without it.
func TestFiltersFormKeepsTheRangeWithoutTheScript(t *testing.T) {
	t.Parallel()
	h := handler(t, setup(t), nil)
	_, body, _ := get(t, h, "/tasks?state=dead&since=2021-03-04T15:00:00%2B05:00&before=2021-03-04T11:30:30Z")
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
	if got, want := sent.Encode(), "before=2021-03-04T11%3A30%3A30&since=2021-03-04T10%3A00&state=dead"; got != want {
		t.Fatalf("the form sends %s, want %s", got, want)
	}
	action, _ := attrOf(form, "action")
	code, again, _ := get(t, h, action+"?"+sent.Encode())
	if got, want := readFinishFilter(t, again), readFinishFilter(t, body); code != http.StatusOK || got != want {
		t.Errorf("the list that the form leads to has status %d and the range\n%+v, want\n%+v", code, got, want)
	}
}

// The menu of the filter by finish time holds what the script's picker
// reads: a calendar with a grid that its title names, a row of seven
// weekdays, and the two steps of its month, which are plain buttons, so that
// a click on one never sends the form; and for each bound a date and a time,
// which say what they are and how to write them. The page shows none of it
// without the script, where the browser's own fields say From and Before:
// their second bound is the one of the address.
func TestFinishFilterHoldsWhatItsPickerReads(t *testing.T) {
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
