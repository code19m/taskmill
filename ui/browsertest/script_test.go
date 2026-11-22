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
	p.waitFor(`document.querySelector(".sel-range").open`, "the range's menu opening")
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

// An action asks first, in a dialog. Escape closes the dialog, sends
// nothing, and gives the focus back to the button that opened it. Confirm
// sends the action, and the page that answers shows in place, with its
// notice.
func TestDialogConfirmsAnAction(t *testing.T) {
	s := newSite(t)
	id := s.enqueue(t, 1)[0]
	p := s.open(t, "/tasks/"+key(id))
	p.run(`window.loaded = true`)
	const button = `form[action$="/delete"] button`

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

	// The dialog fires its close event a moment after it closes. The test
	// waits for it, as a person who opens the dialog again does.
	p.run(`document.getElementById("confirm-dialog").addEventListener("close", () => { window.dialogClosed = true; }, { once: true })`)
	p.do(chromedp.KeyEvent(kb.Escape))
	p.waitFor(`window.dialogClosed && !document.getElementById("confirm-dialog").open`, "Escape")
	p.waitFor(`document.activeElement === document.querySelector(`+js(button)+`)`, "the focus going back to the button")
	if _, err := s.client.Get(t.Context(), id); err != nil {
		t.Fatalf("Escape sent the action: %v", err)
	}

	p.click(button)
	p.waitFor(`document.getElementById("confirm-dialog").open`, "the dialog, again")
	p.click(`[data-confirm-button]`)
	p.waitFor(`location.pathname === "/tasks" && location.search === "" && document.querySelector(".toast")?.textContent === "Task deleted."`,
		"the task list, with the notice of the action")
	if _, err := s.client.Get(t.Context(), id); !errors.Is(err, taskmill.ErrNotFound) {
		t.Errorf("the task is still there after the action: %v", err)
	}
	if !eval[bool](p, `window.loaded === true`) {
		t.Error("the browser loaded the page that answered the action anew")
	}
}

// A dialog that opens again at once, before the close event of its last
// closing, still sends the action that it then confirms: that late event is
// of the closing before. The test closes the dialog and opens it again in
// one turn of the page, so the event comes after, and confirms once the
// script has had it: the test's listener comes after the script's.
func TestDialogOpenedAgainAtOnceSendsItsAction(t *testing.T) {
	s := newSite(t)
	id := s.enqueue(t, 1)[0]
	p := s.open(t, "/tasks/"+key(id))
	const button = `form[action$="/delete"] button`

	p.click(button)
	p.waitFor(`document.getElementById("confirm-dialog").open`, "the dialog")
	p.run(`
		const dialog = document.getElementById("confirm-dialog");
		dialog.addEventListener("close", () => { window.lateClose = true; }, { once: true });
		dialog.querySelector('[value="cancel"]').click();
		document.querySelector(` + js(button) + `).click();`)
	p.waitFor(`window.lateClose && document.getElementById("confirm-dialog").open`, "the dialog, open again after the late close event")
	p.click(`[data-confirm-button]`)
	p.waitFor(`location.pathname === "/tasks" && document.querySelector(".toast")?.textContent === "Task deleted."`,
		"the task list, with the notice of the action")
	if _, err := s.client.Get(t.Context(), id); !errors.Is(err, taskmill.ErrNotFound) {
		t.Errorf("the task is still there after the action: %v", err)
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
