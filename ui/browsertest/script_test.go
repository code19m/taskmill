package browsertest

import (
	"errors"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/chromedp/chromedp"
	"github.com/chromedp/chromedp/kb"

	"github.com/code19m/taskmill"
)

type greet struct{}

func (greet) Kind() string { return "greet" }

// enqueue enqueues n tasks, and returns their IDs.
func (s *site) enqueue(t *testing.T, n int) []int64 {
	t.Helper()
	params := make([]taskmill.EnqueueParams, n)
	for i := range params {
		params[i] = taskmill.EnqueueParams{Args: greet{}}
	}
	res, err := s.client.EnqueueMany(t.Context(), params)
	if err != nil {
		t.Fatal(err)
	}

	ids := make([]int64, n)
	for i, r := range res {
		ids[i] = r.ID
	}
	return ids
}

// die makes a task dead, as if it died minutes ago: the list of dead tasks
// shows the task that died last first.
func (s *site) die(t *testing.T, id int64, minutes float64) {
	t.Helper()
	s.exec(t, `UPDATE {s}.tasks SET state = 'dead', finalized_at = now() - $2 * interval '1 minute' WHERE id = $1`, id, minutes)
}

// A live update changes only what differs. A row that comes, goes, or moves
// up or down is the one row that it touches: every other row stays the same
// element, and an element in a row keeps the focus, also in the row that
// moves. The list of dead tasks comes by when they died, so a change of that
// time moves a row.
func TestUpdateTouchesOnlyTheRowsThatChange(t *testing.T) {
	s := newSite(t)
	ids := s.enqueue(t, 5)
	a, b, c, d, e := ids[0], ids[1], ids[2], ids[3], ids[4]
	for i, id := range []int64{a, b, c, d} {
		s.die(t, id, float64(i+1))
	}

	// Each row is marked with its key, so a row that the update made anew,
	// or that now shows another task, has another mark, and the rows that
	// the update adds to the body of the table, or moves in it, are
	// recorded. The focus is on the link of C.
	p := s.open(t, "/tasks?state=dead")
	p.run(`
		window.mark = () => { for (const row of document.querySelectorAll("tbody tr")) row.mark ??= row.dataset.key; };
		mark();
		window.touched = [];
		new MutationObserver((records) => {
			for (const r of records) for (const n of r.addedNodes) if (n.nodeType === 1) touched.push(n.dataset.key);
		}).observe(document.querySelector("tbody"), { childList: true });
		const link = document.querySelector('tr[data-key="` + key(c) + `"] a');
		link.focus();
		link.focused = true;`)

	for _, step := range []struct {
		name    string
		change  func()
		order   []int64
		touched []int64 // the rows that the update adds or moves
		made    []int64 // the rows that are elements of their own for the first time
	}{
		{"a row comes", func() { s.die(t, e, 2.5) }, []int64{a, b, e, c, d}, []int64{e}, []int64{e}},
		{"a row goes", func() {
			if err := s.client.Delete(t.Context(), b); err != nil {
				t.Fatal(err)
			}
		}, []int64{a, e, c, d}, nil, nil},
		{"the row with the focus moves up", func() { s.die(t, c, 0) }, []int64{c, a, e, d}, []int64{c}, nil},
		{"the row with the focus moves down", func() { s.die(t, c, 10) }, []int64{a, e, d, c}, []int64{c}, nil},
		{"two rows swap", func() { s.die(t, a, 2.5); s.die(t, e, 1) }, []int64{e, a, d, c}, []int64{a}, nil},
	} {
		p.run(`touched = []`)
		step.change()
		p.update()
		p.waitFor(`JSON.stringify([...document.querySelectorAll("tbody tr")].map((row) => row.dataset.key)) === `+js(js(keys(step.order))), step.name)

		if got := eval[[]string](p, `[...document.querySelectorAll("tbody tr")].filter((row) => row.mark !== row.dataset.key).map((row) => row.dataset.key)`); !slices.Equal(got, keys(step.made)) {
			t.Errorf("%s: the rows %v are new elements, or elements of other rows, want only %v", step.name, got, keys(step.made))
		}
		if got := eval[[]string](p, `touched`); !slices.Equal(got, keys(step.touched)) {
			t.Errorf("%s: the update added or moved the rows %v, want %v", step.name, got, keys(step.touched))
		}
		if !eval[bool](p, `document.activeElement.focused === true && document.activeElement.closest("tr").dataset.key === `+js(key(c))) {
			t.Errorf("%s: the link in the row of task %d lost the focus to %s", step.name, c,
				eval[string](p, `document.activeElement.outerHTML.slice(0, 80)`))
		}
		p.run(`mark()`)
	}
}

// A link shows its page in place, without loading the page anew, and the
// back and forward buttons show the pages that the tab showed, as they were.
func TestNavigationAndHistoryShowPagesInPlace(t *testing.T) {
	s := newSite(t)
	p := s.open(t, "/")
	p.run(`window.loaded = true`) // a page that the browser loads anew forgets it

	shows := func(path, title string) string {
		return `location.pathname === ` + js(path) + ` && document.querySelector("main h1")?.textContent === ` + js(title) +
			` && document.title === ` + js(title+" · Taskmill") +
			` && document.querySelector(".nav-item[aria-current=page]")?.getAttribute("href") === ` + js(path)
	}
	p.click(`.nav-item[href="/workers"]`)
	p.waitFor(shows("/workers", "Workers"), "the link to the workers")

	for _, step := range []struct {
		name, path, title string
		delta             int
	}{
		{"back", "/", "Overview", -1},
		{"forward", "/workers", "Workers", 1},
	} {
		p.history(step.delta)
		p.waitFor(shows(step.path, step.title), "the "+step.name+" button")
	}

	if !eval[bool](p, `window.loaded === true`) {
		t.Error("the browser loaded a page anew")
	}
}

// A preset of the range of time, a link in the range's menu, is a filter:
// its list shows in place, in the same entry of the history, and the menu
// closes and gives the focus back to its button. navigation.js follows the
// link, and closes the menu with menus.js.
func TestRangePresetShowsItsListInPlace(t *testing.T) {
	s := newSite(t)
	p := s.open(t, "/tasks")
	p.run(`window.loaded = true`)
	entries := eval[int](p, `history.length`)

	p.click(".sel-range > summary")
	p.waitFor(`document.querySelector(".sel-range[open][data-js-placed] > .pop")?.checkVisibility({ visibilityProperty: true })`,
		"the range's menu opening, placed")
	p.click(".range-presets > a")
	p.waitFor(`new URLSearchParams(location.search).has("since") && !document.querySelector(".range-presets > a[aria-current]")`,
		"the list of the preset showing")
	if !eval[bool](p, `!document.querySelector(".sel-range").open && document.activeElement === document.querySelector(".sel-range > summary")`) {
		t.Error("the range's menu stays open, or its button lost the focus")
	}
	if !eval[bool](p, `window.loaded === true && history.length === `+strconv.Itoa(entries)) {
		t.Error("the preset loaded the page anew, or added an entry to the history")
	}
}

// A menu shows only once the script has placed it, toward the side with
// room: in the task that opens it, it is hidden, so that no frame shows it
// where it does not fit, and no press lands where it was. The range menu
// ends the filters, and opens toward the left.
func TestMenuShowsOnceItIsPlaced(t *testing.T) {
	s := newSite(t)
	p := s.open(t, "/tasks")
	if !eval[bool](p, `(() => {
		const sel = document.querySelector(".sel-range");
		sel.open = true;
		return !sel.querySelector(".pop").checkVisibility({ visibilityProperty: true });
	})()`) {
		t.Error("the menu showed before the script placed it")
	}
	p.waitFor(`document.querySelector(".sel-range[data-js-placed] > .pop")?.checkVisibility({ visibilityProperty: true })`, "the menu showing")
	if !eval[bool](p, `document.querySelector(".sel-range > .pop").getBoundingClientRect().right <= innerWidth`) {
		t.Error("the menu runs past the window")
	}
}

// Menus that the reader opens while the script loads show as before. Once
// the script starts, they are placed, the kind search runs what was typed,
// and the range's calendar shows the month of the range's end; the focus,
// and the menus that are open, stay as the reader left them, here in the
// kind search with "zz" typed.
func TestMenusOpenedWhileTheScriptLoads(t *testing.T) {
	s := newSite(t)
	s.enqueue(t, 2)
	release := s.hold(t, "app.js")
	p := s.start(t, "/tasks?since=2025-08-03T00:00:00Z&before=2025-08-10T00:00:00Z")
	p.click(".sel-range > summary")
	p.click(`.sel[data-name="kind"] > summary`)
	p.click(`.sel[data-name="kind"] [data-sel-search]`)
	p.press("zz") // a search that finds no kind
	const shown = `[...document.querySelectorAll("details.sel[open] > .pop")].map((pop) => pop.checkVisibility({ visibilityProperty: true }) && pop.getBoundingClientRect().right <= innerWidth)`
	if got := eval[[]bool](p, shown); len(got) != 2 {
		t.Fatalf("got %d open menus while the script loads, want 2", len(got))
	}

	release()
	p.waitFor(`document.documentElement.classList.contains("js-started")`, "the script starting")
	if got := eval[[]bool](p, shown); !slices.Equal(got, []bool{true, true}) {
		t.Errorf("of the open menus, these show within the window: %v, want both", got)
	}
	if got := eval[string](p, `document.getElementById("cal-title").textContent`); !strings.Contains(got, "2025") {
		t.Errorf("the calendar shows %q, want a month of 2025", got)
	}
	if !eval[bool](p, `document.activeElement.matches('.sel[data-name="kind"] [data-sel-search]') && document.activeElement.value === "zz"`) {
		t.Error("the kind search lost the focus, or its text")
	}
	const kind = `document.querySelector('.sel[data-name="kind"]')`
	if !eval[bool](p, `!`+kind+`.querySelector("[data-sel-add]").hidden && [...`+kind+`.querySelectorAll(".pop-list .pop-opt")].every((row) => row.hidden)`) {
		t.Error("the kind search did not run what was typed while the script loaded")
	}
}

// A page that the back or forward button brings back comes without the
// menu that was open on it, as the browser brings a page back: the menu is
// closed, has forgotten its search, and the focus is on the page.
func TestPageComesBackWithItsMenusClosed(t *testing.T) {
	s := newSite(t)
	s.enqueue(t, 2)
	p := s.open(t, "/")
	p.click(`.nav-item[href="/tasks"]`)
	const kind = `document.querySelector('.sel[data-name="kind"]')`
	p.waitFor(`location.pathname === "/tasks" && `+kind, "the tasks")
	p.click(`.sel[data-name="kind"] > summary`)
	p.waitFor(kind+`.matches("[open][data-js-placed]")`, "the menu opening")
	p.press("zz") // a search that finds no kind
	p.waitFor(`!`+kind+`.querySelector("[data-sel-add]").hidden`, "the search")

	p.history(-1)
	p.waitFor(`location.pathname === "/"`, "the back button")
	// The menu as the forward button shows it, before the live update that
	// follows can mend it: this listener runs right after the script's own.
	p.run(`addEventListener("popstate", () => {
		const sel = ` + kind + `;
		window.cameBack = sel.querySelector("[data-sel-add]").hidden && sel.querySelectorAll(".pop-list .pop-opt[hidden]").length === 0;
	}, { once: true })`)
	p.history(1)
	p.waitFor(`location.pathname === "/tasks" && `+kind, "the forward button")
	if !eval[bool](p, `!document.querySelector("details.sel[open], details.sel[data-js-placed]") && document.activeElement === document.getElementById("main")`) {
		t.Error("a menu came back open, or took the focus")
	}
	if !eval[bool](p, `window.cameBack`) {
		t.Error("the menu came back with its search")
	}
}

// A menu that stays open while its page shows anew, as one that adds the
// value that the reader typed, keeps its place: the newer copy lacks the
// classes that placed it. On a phone, the queue menu opens toward the left.
func TestOpenMenuKeepsItsPlaceWhenItsPageShowsAnew(t *testing.T) {
	s := newSite(t)
	p := s.open(t, "/tasks")
	p.do(chromedp.EmulateViewport(390, 844))
	const sel = `document.querySelector('.sel[data-name="queue"]')`
	p.click(`.sel[data-name="queue"] > summary`)
	p.waitFor(sel+`.matches("[open][data-js-placed]")`, "the menu opening")
	p.press("mail" + kb.Enter)
	p.waitFor(`new URLSearchParams(location.search).get("queue") === "mail"`, "the list of mail")
	if !eval[bool](p, sel+`.open && `+sel+`.querySelector(".pop").getBoundingClientRect().right <= innerWidth`) {
		t.Error("the menu closed, or runs past the window")
	}
}

// "/" opens the kind filter with its search focused: the keys typed right
// after it search.
func TestSlashSearchesTheKinds(t *testing.T) {
	s := newSite(t)
	p := s.open(t, "/tasks")
	p.press("/email")
	p.waitFor(`document.activeElement.matches('.sel[data-name="kind"] [data-sel-search]') && document.activeElement.value === "email"`,
		"a search for email")
}

// An action that asks first asks in a dialog, which shows the page that
// asks. Escape closes the dialog, sends nothing, and gives the focus back to
// the button that opened it. Confirm sends the action, and the page that
// answers shows in place, with its notice. A deleted task's page is gone:
// the list of its queue and state takes its place in the history, so the
// back button leads to the list that the reader opened the task from.
func TestDialogConfirmsAnAction(t *testing.T) {
	s := newSite(t)
	id := s.enqueue(t, 1)[0]
	s.die(t, id, 1)
	p := s.open(t, "/tasks?state=dead")
	p.run(`window.loaded = true`)
	p.click(`tbody tr`)
	p.waitFor(`location.pathname === "/tasks/`+key(id)+`"`, "the task's page")
	entries := eval[int](p, `history.length`)
	const button = `a[href$="/delete"]`

	p.click(button)
	p.waitFor(`document.getElementById("confirm-dialog").open`, "the dialog")
	if got := eval[[]string](p, `(() => {
		const dialog = document.getElementById("confirm-dialog");
		const confirm = dialog.querySelector("[data-confirm-button]");
		return [dialog.querySelector("h2").textContent, dialog.querySelector("[data-confirm-text]").textContent,
			confirm.textContent, confirm.className];
	})()`); !slices.Equal(got, []string{"Delete task #" + key(id) + "?", "This cannot be undone.", "Delete", "btn danger-solid"}) {
		t.Errorf("the dialog says %q", got)
	}

	p.press(kb.Escape)
	p.waitFor(`!document.getElementById("confirm-dialog").open`, "Escape")
	p.waitFor(`document.activeElement === document.querySelector(`+js(button)+`)`, "the focus going back to the button")
	if _, err := s.client.Get(t.Context(), id); err != nil {
		t.Fatalf("Escape sent the action: %v", err)
	}

	p.click(button)
	p.waitFor(`document.getElementById("confirm-dialog").open`, "the dialog, again")
	p.click(`[data-confirm-button]`)
	p.waitFor(`location.pathname === "/tasks" && location.search === "?queue=default&state=dead" && document.querySelector(".toast")?.textContent === "Task deleted."`,
		"the list of the task's queue and state, with the notice of the action")
	if _, err := s.client.Get(t.Context(), id); !errors.Is(err, taskmill.ErrNotFound) {
		t.Errorf("the task is still there after the action: %v", err)
	}
	if !eval[bool](p, `window.loaded === true && history.length === `+strconv.Itoa(entries)) {
		t.Error("the browser loaded the page that answered the action anew, or the history kept the deleted task's page")
	}

	p.history(-1)
	p.waitFor(`location.pathname === "/tasks" && location.search === "?state=dead"`, "the back button leading to the list of dead tasks")
}

// An action keeps the focus on the button that sent it, so the next Tab
// goes on from there. While the action is on its way, the button is
// unavailable, and keeps the focus: the page's body never takes it. The
// answer puts Pause in place of Resume, which takes the focus. In a
// schedule's row, Run now comes and goes before Pause: Pause, confirmed in
// its dialog, leaves the focus on Resume, and Resume on Pause.
func TestActionKeepsTheFocusInItsRow(t *testing.T) {
	s := newSite(t)
	// A queue with settings stays on the overview once it resumes.
	if err := s.client.UpdateQueue(t.Context(), "mail", func(q *taskmill.QueueSettings) { q.MaxRunning = 10 }); err != nil {
		t.Fatal(err)
	}
	if err := s.client.PauseQueue(t.Context(), "mail"); err != nil {
		t.Fatal(err)
	}
	s.exec(t, `INSERT INTO {s}.schedules (name, queue, kind, cron, next_run_at) VALUES ('nightly', 'default', 'report', '0 3 * * *', now() + interval '1 hour')`)
	act := func(p *page, notice, row, want string) {
		t.Helper()
		p.press(kb.Enter)
		p.waitFor(`document.querySelector(".toast")?.textContent === `+js(notice), "the notice of the action")
		p.waitFor(`!document.querySelector("[data-js-pending]")`, "the end of the action")
		if got := eval[string](p, `document.activeElement.matches('tr[data-key=`+js(row)+`] .actions-col :is(a, button)') ? document.activeElement.textContent : document.activeElement.localName`); got != want {
			t.Errorf("after %q, the %s has the focus, want %s", notice, got, want)
		}
	}

	p := s.open(t, "/")
	p.run(`
		window.lost = false;
		document.addEventListener("focusout", (event) => { if (!event.relatedTarget && document.querySelector("[data-js-pending]")) window.lost = true; });
		document.querySelector('tr[data-key="mail"] form button').focus();`)
	act(p, "Queue resumed.", "mail", "Pause")
	if eval[bool](p, `window.lost`) {
		t.Error("the focus went to the page's body while the action was on its way")
	}

	p = s.open(t, "/schedules")
	p.run(`document.querySelector('tr[data-key="nightly"] a[href*="/pause"]').focus()`)
	p.press(kb.Enter)
	p.waitFor(`document.getElementById("confirm-dialog").open`, "the dialog")
	p.run(`document.querySelector("[data-confirm-button]").focus()`)
	act(p, "Schedule paused.", "nightly", "Resume")
	act(p, "Schedule resumed.", "nightly", "Pause")
}

// A period of the overview's charts is a filter of the page: its charts
// show in place, in the same entry of the history, where the reader is,
// with the focus on the period, and a screen reader hears which period
// they show.
func TestPeriodShowsItsChartsInPlace(t *testing.T) {
	s := newSite(t)
	p := s.open(t, "/")
	p.do(chromedp.EmulateViewport(375, 600))
	p.run(`window.loaded = true; document.querySelector(".activity").scrollIntoView()`)
	entries, y := eval[int](p, `history.length`), eval[float64](p, `scrollY`)
	if y == 0 {
		t.Fatal("the overview does not scroll at phone width")
	}

	p.click(`.activity a[href$="range=6h"]`)
	p.waitFor(`location.search === "?range=6h" && document.querySelector('.activity a[aria-current]')?.textContent === "6h"`, "the charts of 6 hours")
	if got := eval[[]any](p, `[window.loaded === true, history.length, scrollY, document.activeElement.textContent,
		document.querySelector("[data-announce]").textContent]`); got[0] != true || got[1] != float64(entries) || got[2] != y ||
		got[3] != "6h" || got[4] != "Activity of the last 6h" {
		t.Errorf("after the period, the page is %v; want it as it was, in the same entry of the history, scrolled to %v, "+
			"with the focus on the period, and the period announced", got, y)
	}
}

// The dialog of a bulk action on a list with a range names the range in the
// reader's time zone, in the words of the range's button, and not in the
// server's UTC: the reader confirms the range that the list shows.
func TestBulkDialogNamesTheRangeAsTheListDoes(t *testing.T) {
	s := newSite(t)
	s.die(t, s.enqueue(t, 1)[0], 30)
	since := time.Now().Add(-time.Hour).Truncate(time.Minute).UTC().Format(time.RFC3339)
	p := s.open(t, "/tasks?state=dead&since="+since)

	p.click(`a[data-dialog][href*="/retry"]`)
	p.waitFor(`document.getElementById("confirm-dialog").open`, "the dialog")
	got := eval[[]string](p, `[document.querySelector("#confirm-dialog dd:last-of-type").textContent,
		document.querySelector(".sel-range > summary").title]`)
	if got[0] != got[1] || !strings.HasSuffix(got[0], "UTC+5:45") {
		t.Errorf("the dialog names the range %q, and the list's button %q; want the same, in the reader's zone", got[0], got[1])
	}
}

// A live update asks for the page that it shows, by its ETag. While only the
// relative times of the page changed, which the script counts on by itself,
// it gets 304 Not Modified, and the page stays as it is; then it gets a page
// whose data changed, and shows it.
func TestLiveUpdateKeepsAnUnchangedPageAndShowsAChangedOne(t *testing.T) {
	s := newSite(t)
	first := s.enqueue(t, 1)[0]
	p := s.open(t, "/tasks")
	p.run(`document.querySelector("tbody tr").mark = true`)

	// The first update learns the ETag of the page on screen.
	skip := len(s.requests())
	p.update()
	s.waitLive(t, skip, func(r liveRequest) bool { return r.path == "/tasks" && r.status == 200 })

	// A second later, the time of the task's creation reads anew.
	time.Sleep(time.Until(time.Now().Truncate(time.Second).Add(time.Second)))
	skip = len(s.requests())
	p.update()
	s.waitLive(t, skip, func(r liveRequest) bool { return r.path == "/tasks" && r.asked && r.status == 304 })
	if !eval[bool](p, `document.querySelector("tbody tr").mark === true && document.querySelectorAll("tbody tr").length === 1`) {
		t.Error("the page changed after an update that it got 304 for")
	}

	second := s.enqueue(t, 1)[0]
	p.update()
	p.waitFor(`document.querySelectorAll("tbody tr").length === 2`, "the task enqueued since")
	if got := eval[[]string](p, `[...document.querySelectorAll("tbody tr")].map((row) => row.dataset.key + (row.mark ? "*" : ""))`); !slices.Equal(got, []string{key(second), key(first) + "*"}) {
		t.Errorf("the list after the update is %q, want the new task, then the task that was there, as the same element (*)", got)
	}
}

// What the pointer rests on shows its title at once, in a tooltip of the
// page's own: a time shows its instant in the reader's time zone. Meanwhile
// a screen reader hears the title as a description, and the title comes
// back when the pointer leaves.
func TestTooltipShowsTheTitleAtOnce(t *testing.T) {
	s := newSite(t)
	id := s.enqueue(t, 1)[0]
	info, err := s.client.Get(t.Context(), id)
	if err != nil {
		t.Fatal(err)
	}
	loc, err := time.LoadLocation(zone)
	if err != nil {
		t.Fatal(err)
	}

	p := s.open(t, "/tasks")
	const created = `tbody tr td[data-time]`
	want := info.CreatedAt.In(loc).Format("2006-01-02 15:04:05.000") + " UTC+5:45"
	p.hover(created)
	p.waitFor(`document.querySelector(".tooltip")?.textContent === `+js(want), "the tooltip of the time "+want)
	if !eval[bool](p, `(() => {
		const cell = document.querySelector(`+js(created)+`);
		return !cell.hasAttribute("title") && cell.getAttribute("aria-description") === `+js(want)+`;
	})()`) {
		t.Error("the title is not a description while the tooltip shows it")
	}

	p.hover(`main h1`)
	p.waitFor(`!document.querySelector(".tooltip")`, "the tooltip going away")
	if got := eval[string](p, `document.querySelector(`+js(created)+`).title`); got != want {
		t.Errorf("the title after the tooltip is %q, want %q", got, want)
	}
}

func key(id int64) string { return strconv.FormatInt(id, 10) }

func keys(ids []int64) []string {
	out := make([]string, len(ids))
	for i, id := range ids {
		out[i] = key(id)
	}
	return out
}
