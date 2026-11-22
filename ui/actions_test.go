package ui_test

import (
	"context"
	"html"
	"log/slog"
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

	"github.com/code19m/taskmill"
	"github.com/code19m/taskmill/ui"
)

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

// Pause and Resume on the overview lead back to the overview with the period
// of the charts that the reader chose: each form carries the period of its
// page, and the server takes only a period that it knows. The first period
// is the overview's plain address. An action that is refused leads back
// there too.
func TestQueueActionsKeepThePeriodOfTheCharts(t *testing.T) {
	t.Parallel()
	client := setup(t)
	if err := keepQueue(t.Context(), client, "mail"); err != nil {
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
	if body := rec.Body.String(); rec.Code != http.StatusBadRequest || !strings.Contains(body, `<a class="btn primary" href="/admin/?range=24h">`) {
		t.Errorf("Pause without a queue on the 24h overview: status %d, want 400 and the way back to that overview\n%s", rec.Code, body)
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
		if err := keepQueue(t.Context(), client, name); err != nil {
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

// An action that cannot be undone, or that does more than its button says,
// asks first, with or without the script: its button is a link to the page
// that asks, which changes nothing, and whose form sends the action. With
// the script, the dialog shows that page, under the question that it
// names. Resume asks nothing. A deleted task's page is gone, so the delete
// leads to the list of the task's queue and state.
func TestActionsAskFirst(t *testing.T) {
	t.Parallel()
	client, pool, schema := setupDB(t)
	storeSchedules(t, client, []taskmill.Schedule{{Name: "nightly", Cron: "@daily", Args: greet{}}})
	if err := keepQueue(t.Context(), client, "mail"); err != nil {
		t.Fatal(err)
	}
	res, err := client.EnqueueMany(t.Context(), []taskmill.EnqueueParams{
		{Args: greet{}, Options: &taskmill.EnqueueOptions{Queue: "mail"}},
		{Args: greet{}, Options: &taskmill.EnqueueOptions{Queue: "mail"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	dead, running := strconv.FormatInt(res[0].ID, 10), strconv.FormatInt(res[1].ID, 10)
	if _, err := pool.Exec(t.Context(), `UPDATE `+schema+`.tasks SET state = 'dead', finalized_at = now() WHERE id = $1`, res[0].ID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(t.Context(), `UPDATE `+schema+`.tasks SET state = 'running', attempt = 1, attempted_at = now(),
	    attempted_by = 'w', lease_expires_at = now() + interval '1 minute' WHERE id = $1`, res[1].ID); err != nil {
		t.Fatal(err)
	}

	h := handler(t, client, nil)
	for _, c := range []struct {
		page, button, question, explain, sends string
	}{
		{"/tasks/" + dead, "Retry", "Retry task #" + dead + "?", "It runs again with a fresh set of attempts.", "/tasks/" + dead + "/retry"},
		{"/tasks/" + dead, "Delete", "Delete task #" + dead + "?", "This cannot be undone.", "/tasks/" + dead + "/delete?queue=mail&state=dead"},
		{"/tasks/" + running, "Cancel", "Cancel task #" + running + "?", "Its handler&#39;s context is canceled", "/tasks/" + running + "/cancel"},
		{"/?range=6h", "Pause", "Pause queue mail?", "Running tasks finish", "/queues/pause?name=mail&range=6h"},
		{"/schedules", "Run now", "Run nightly now?", "Its regular timetable stays as it is.", "/schedules/run?name=nightly"},
		{"/schedules", "Pause", "Pause nightly?", "Runs missed while paused collapse", "/schedules/pause?name=nightly"},
	} {
		_, page, _ := get(t, h, c.page)
		link := regexp.MustCompile(`<a class="btn[^"]*" href="([^"]*)" data-dialog><svg.*?</svg>` + c.button + `</a>`).FindStringSubmatch(page)
		if link == nil {
			t.Errorf("%s: %s is no link to the page that asks first\n%s", c.page, c.button, page)
			continue
		}

		code, asks, _ := get(t, h, html.UnescapeString(link[1]))
		form := askingForm.FindStringSubmatch(asks)
		if code != http.StatusOK || form == nil || html.UnescapeString(form[1]) != c.sends ||
			!strings.Contains(asks, `data-dialog-title="`+html.EscapeString(c.question)+`"`) || !strings.Contains(asks, "<p>"+c.explain) {
			t.Errorf("%s, %s: status %d, want a page that asks %q, says %q, and sends %s\n%s", c.page, link[1], code, c.question, c.explain, c.sends, asks)
		}
	}

	// Asking changed nothing.
	if info, err := client.Get(t.Context(), res[0].ID); err != nil || info.State != taskmill.StateDead {
		t.Fatalf("after the pages that ask, the dead task is %v, %v", info, err)
	}
	if paused := pausedQueues(t, client); len(paused) != 0 {
		t.Fatalf("after the page that asks, the queues %q are paused", paused)
	}

	rec := post(t, h, "/tasks/"+dead+"/delete?queue=mail&state=dead", "same-origin")
	if got := rec.Header().Get("Location"); rec.Code != http.StatusSeeOther || got != "/tasks?queue=mail&state=dead&done=delete" {
		t.Errorf("delete: status %d to %q, want 303 to the list of the task's queue and state", rec.Code, got)
	}

	for _, page := range []string{"/tasks/" + running + "/archive", "/queues/resume?name=mail", "/schedules/resume?name=nightly", "/tasks/x/delete"} {
		if code, _, _ := get(t, h, page); code != http.StatusNotFound {
			t.Errorf("%s: status %d, want 404: no action there asks first", page, code)
		}
	}
	if code, _, _ := get(t, h, "/tasks/"+dead+"/delete"); code != http.StatusNotFound {
		t.Errorf("the page that asks before deleting a deleted task: status %d, want 404", code)
	}
	if code, _, _ := get(t, h, "/schedules/run?name=gone"); code != http.StatusNotFound {
		t.Errorf("the page that asks before running a schedule that is not there: status %d, want 404", code)
	}
}

// An action with a value that is not valid is a bad request, as a page with
// the same value is, and so is the page that asks before an action on no
// queue or schedule. Only an action that does not fit the task as it is now
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
	for _, page := range []string{"/queues/pause", "/schedules/run", "/schedules/pause?name="} {
		if code, body, _ := get(t, h, page); code != http.StatusBadRequest || !strings.Contains(body, "<h1>Invalid request</h1>") {
			t.Errorf("GET %s: status %d, want 400 and a page that says so\n%s", page, code, body)
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

func TestReadOnly(t *testing.T) {
	t.Parallel()
	client := setup(t)
	storeSchedules(t, client, []taskmill.Schedule{{Name: "nightly", Cron: "@daily", Args: greet{}}})
	res, err := client.Enqueue(t.Context(), greet{Name: "x"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := keepQueue(t.Context(), client, "default"); err != nil {
		t.Fatal(err)
	}

	h := handler(t, client, &ui.Config{ReadOnly: true})
	id := strconv.FormatInt(res.ID, 10)

	// The pages that offer actions offer none, and say why. An action is a
	// form, or a link to the page that asks before it.
	operate := handler(t, client, nil)
	offers := func(body string) bool {
		return strings.Contains(body, `method="post"`) || strings.Contains(body, " data-dialog>")
	}
	for _, page := range []string{"/", "/schedules", "/tasks/" + id} {
		if _, body, _ := get(t, operate, page); !offers(body) {
			t.Fatalf("%s offers no action that a read-only dashboard could hide\n%s", page, body)
		}
		if _, body, _ := get(t, h, page); offers(body) || !strings.Contains(body, "read-only") {
			t.Errorf("read-only %s offers an action, or does not say that it is read-only\n%s", page, body)
		}
	}

	for _, page := range []string{"/tasks/" + id + "/delete", "/queues/pause?name=default", "/schedules/run?name=nightly"} {
		if code, _, _ := get(t, h, page); code != http.StatusForbidden {
			t.Errorf("read-only %s, the page that asks before an action: %d", page, code)
		}
	}
	for _, page := range []string{"/tasks/" + id + "/archive", "/queues/resume?name=default", "/schedules/resume?name=nightly"} {
		if code, _, _ := get(t, h, page); code != http.StatusNotFound {
			t.Errorf("read-only %s, where no action asks first: %d, want 404", page, code)
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
	client, _, _ := setupTraced(t, picks)

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

// number formats n as the interface does.
func number(n int) string {
	s := strconv.Itoa(n)
	for i := len(s) - 3; i > 0; i -= 3 {
		s = s[:i] + "," + s[i:]
	}
	return s
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

// countedStates records the states that each count of tasks on its pool
// reads.
type countedStates struct {
	mu     sync.Mutex
	counts [][]string
}

func (c *countedStates) TraceBatchStart(ctx context.Context, _ *pgx.Conn, d pgx.TraceBatchStartData) context.Context {
	for _, q := range d.Batch.QueuedQueries {
		if !strings.Contains(q.SQL, "unflagged") {
			continue
		}
		if states, ok := q.Arguments[len(q.Arguments)-1].([]string); ok {
			c.mu.Lock()
			c.counts = append(c.counts, slices.Sorted(slices.Values(states)))
			c.mu.Unlock()
		}
	}
	return ctx
}

func (*countedStates) TraceBatchQuery(context.Context, *pgx.Conn, pgx.TraceBatchQueryData) {}
func (*countedStates) TraceBatchEnd(context.Context, *pgx.Conn, pgx.TraceBatchEndData)     {}
func (*countedStates) TraceQueryStart(ctx context.Context, _ *pgx.Conn, _ pgx.TraceQueryStartData) context.Context {
	return ctx
}
func (*countedStates) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

// take returns the counts recorded since the last take.
func (c *countedStates) take() [][]string {
	c.mu.Lock()
	defer c.mu.Unlock()
	counts := c.counts
	c.counts = nil
	return counts
}

// The page that asks before a bulk action counts only the states that the
// action changes, of those that the filter selects: the others can hold many
// more tasks, such as every completed task of a kind before a retry, which
// the count would read for nothing. A filter with none of those states counts
// nothing.
func TestBulkPagesCountOnlyWhatTheActionChanges(t *testing.T) {
	t.Parallel()
	counts := &countedStates{}
	client, _, _ := setupTraced(t, counts)
	h := handler(t, client, nil)
	for _, tc := range []struct {
		target string
		want   []string // the states of the one count, sorted; nil for no count
	}{
		{"/tasks/bulk/retry?kind=greet", []string{"canceled", "dead"}},
		{"/tasks/bulk/cancel?queue=default", []string{"available", "pending", "retryable", "running", "scheduled"}},
		{"/tasks/bulk/delete?state=running&state=completed&state=dead", []string{"completed", "dead"}},
		{"/tasks/bulk/retry?state=running", nil},
	} {
		code, body, _ := get(t, h, tc.target)
		got := counts.take()
		if code != http.StatusOK || len(got) != min(len(tc.want), 1) || len(got) == 1 && !slices.Equal(got[0], tc.want) {
			t.Errorf("%s (%d): counted %q, want %q", tc.target, code, got, tc.want)
		}
		if tc.want == nil && !strings.Contains(body, "No task matches.") {
			t.Errorf("%s: the page does not say that no task matches", tc.target)
		}
	}
}

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
		!strings.Contains(body, "Cannot retry task "+id+": task "+holder+" holds its dedupe key.") ||
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
