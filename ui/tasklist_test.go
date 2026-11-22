package ui_test

import (
	"context"
	"encoding/json"
	"errors"
	"html"
	"net/http"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/code19m/taskmill"
	"github.com/code19m/taskmill/ui"
)

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
	if !slices.Equal(ids[:3], []string{"261", "260", "259"}) || !strings.Contains(mixed, "<th>Due</th>") || !strings.Contains(mixed, "Load more") {
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
		{"/tasks?state=dead&cursor=MTIz", "Invalid cursor.", "/tasks?state=dead", "Back to the first page", true}, // "123", of the ID order
		{"/tasks?cursor=%25", "Invalid cursor.", "/tasks", "Back to the first page", true},
		{"/tasks?state=bogus&cursor=MTIz", "Unknown state", "/tasks?state=bogus", "Back to the first page", true},
		{"/tasks?state=bogus", "Unknown state", "/tasks", "Clear the filters", false},
		{"/tasks?state=dead&limit=0", "The limit must be a number from 1 to 250.", "/tasks?state=dead", "Back to the first page", false},
		{"/tasks?limit=251&cursor=MTIz", "The limit must be", "/tasks", "Back to the first page", false},
		{"/tasks?limit=many", "The limit must be", "/tasks", "Back to the first page", false},
	} {
		code, body, _ := get(t, h, c.target)
		if code != http.StatusBadRequest || !strings.Contains(body, "<h1>Invalid request</h1>") || !strings.Contains(body, c.wrong) ||
			strings.Contains(body, upgrade) != c.cursor || !strings.Contains(body, `<a class="btn primary" href="`+c.back+`">`) ||
			!strings.Contains(body, c.label+"</a>") {
			t.Errorf("GET %s: status %d, want 400, a page that says %q, and the link %q to %s\n%s", c.target, code, c.wrong, c.label, c.back, body)
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

// listReads records the last page of tasks that a pool's connections read:
// its statement, with the settings that start its transaction and its
// arguments.
type listReads struct {
	mu                  sync.Mutex
	batch               []pgx.TraceBatchQueryData
	settings, statement string
	args                []any
}

func (*listReads) TraceQueryStart(ctx context.Context, _ *pgx.Conn, _ pgx.TraceQueryStartData) context.Context {
	return ctx
}

func (*listReads) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

func (*listReads) TraceBatchStart(ctx context.Context, _ *pgx.Conn, _ pgx.TraceBatchStartData) context.Context {
	return ctx
}

func (l *listReads) TraceBatchQuery(_ context.Context, _ *pgx.Conn, d pgx.TraceBatchQueryData) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.batch = append(l.batch, d)
}

func (l *listReads) TraceBatchEnd(context.Context, *pgx.Conn, pgx.TraceBatchEndData) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if b := l.batch; len(b) == 2 && strings.Contains(b[0].SQL, "statement_timeout") && strings.Contains(b[1].SQL, ") page(") &&
		!strings.HasPrefix(b[1].SQL, "EXPLAIN") {
		l.settings, l.statement, l.args = b[0].SQL, b[1].SQL, b[1].Args
	}
	l.batch = nil
}

// rowsRead runs the last page of tasks that reads recorded again, under
// EXPLAIN ANALYZE, and returns how many rows its scans read from the table
// of tasks and from the history: those that they returned, and those that
// their filters dropped.
func rowsRead(t *testing.T, pool *pgxpool.Pool, reads *listReads) (tasks, history int64) {
	t.Helper()
	reads.mu.Lock()
	b := &pgx.Batch{}
	b.Queue(reads.settings)
	b.Queue("EXPLAIN (ANALYZE, FORMAT JSON) "+reads.statement, reads.args...)
	reads.mu.Unlock()

	results := pool.SendBatch(t.Context(), b)
	var plan []byte
	if _, err := results.Exec(); err != nil {
		t.Fatal(err)
	}
	if err := results.QueryRow().Scan(&plan); err != nil {
		t.Fatal(err)
	}
	if err := results.Close(); err != nil {
		t.Fatal(err)
	}

	var root []struct{ Plan map[string]any }
	if err := json.Unmarshal(plan, &root); err != nil {
		t.Fatal(err)
	}
	var walk func(node map[string]any)
	walk = func(node map[string]any) {
		relation, _ := node["Relation Name"].(string)
		loops, _ := node["Actual Loops"].(float64)
		rows, _ := node["Actual Rows"].(float64)
		dropped, _ := node["Rows Removed by Filter"].(float64)
		rechecked, _ := node["Rows Removed by Index Recheck"].(float64)
		n := int64(loops * (rows + dropped + rechecked))
		switch {
		case relation == "tasks":
			tasks += n
		case strings.HasPrefix(relation, "task_history"):
			history += n
		}
		children, _ := node["Plans"].([]any)
		for _, child := range children {
			if child, ok := child.(map[string]any); ok {
				walk(child)
			}
		}
	}
	walk(root[0].Plan)
	return tasks, history
}

// The links of the pages to a task list read the tasks of their own queue
// or group, however many other tasks the database holds: the overview's
// links to a queue and to its tasks in a state, the links of a task's page
// to its FIFO group, and that of a schedule to its kind's tasks. None reads
// the newer tasks of the busy queue, its canceled tasks, or the history. A
// queue's link to its few tasks in a state reads the newer tasks of that
// queue, its own backlog, and no other queue's. No index holds a kind's
// tasks, so a worker's link to the tasks of a kind reads the table of
// tasks; it still leaves the history alone.
func TestLinksToTaskListsReadWhatTheyShow(t *testing.T) {
	t.Parallel()
	reads := &listReads{}
	client, pool, schema := setupTraced(t, reads)
	ctx := t.Context()
	if err := errors.Join(keepQueue(ctx, client, "slow"), keepQueue(ctx, client, "backlog")); err != nil {
		t.Fatal(err)
	}

	// A queue of tasks to retry, another with 3 of them before its backlog,
	// a FIFO group whose dead head halts it, and a task of a schedule's kind.
	if _, err := pool.Exec(ctx, `INSERT INTO `+schema+`.tasks (queue, kind, state, attempt, run_at, payload, max_attempts)
	    SELECT CASE WHEN i <= 500 THEN 'slow' ELSE 'backlog' END, 'greet', 'retryable', 1, now() + interval '1 hour', '{}', 2
	    FROM generate_series(1, 503) i`); err != nil {
		t.Fatal(err)
	}
	var params []taskmill.EnqueueParams
	for range 3 {
		params = append(params, taskmill.EnqueueParams{Args: greet{}, Options: &taskmill.EnqueueOptions{Queue: "fifo", GroupKey: "acct-1"}})
	}
	params = append(params, taskmill.EnqueueParams{Args: report{}, Options: &taskmill.EnqueueOptions{Queue: "sched"}})
	res, err := client.EnqueueMany(ctx, params)
	if err != nil {
		t.Fatal(err)
	}

	head := res[0].ID
	for _, sql := range []string{
		`UPDATE {s}.tasks SET state = 'dead', finalized_at = now() WHERE id = ` + strconv.FormatInt(head, 10),
		`UPDATE {s}.groups SET halted_task_id = ` + strconv.FormatInt(head, 10) + `, halted_at = now()`,
		`INSERT INTO {s}.schedules (name, queue, kind, cron, next_run_at) VALUES ('nightly', 'sched', 'report', '0 3 * * *', now() + interval '1 hour')`,
		`INSERT INTO {s}.workers (id, queues, kinds, hostname, version) VALUES ('w', '{sched}', '{report}', 'h', 'v')`,
		// Newer tasks of a busy queue and of the backlog, canceled tasks of
		// other groups, and a history of busy, fifo, and sched.
		`INSERT INTO {s}.tasks (queue, kind, state, run_at, payload, max_attempts)
		 SELECT 'busy', 'greet', 'available', now(), '{}', 1 FROM generate_series(1, 5000)`,
		`INSERT INTO {s}.tasks (queue, kind, state, run_at, payload, max_attempts)
		 SELECT 'backlog', 'greet', 'available', now(), '{}', 1 FROM generate_series(1, 2000)`,
		`INSERT INTO {s}.tasks (queue, kind, group_key, state, run_at, payload, max_attempts, finalized_at)
		 SELECT 'fifo', 'greet', 'acct-' || (i + 1), 'canceled', now(), '{}', 1, now() FROM generate_series(1, 2000) i`,
		`INSERT INTO {s}.task_history (id, finalized_at, queue, created_at, kind, group_key, run_at, attempted_by, attempted_at,
		     priority, attempt, max_attempts, payload, metadata, errors)
		 SELECT 1000000 + i, now(), (ARRAY['busy', 'fifo', 'sched'])[i % 3 + 1], now(), 'greet', 'acct-' || (i + 1), now(), 'w', now(),
		     2, 1, 1, '{}', '{}', '[]' FROM generate_series(1, 6000) i`,
		`ANALYZE {s}.tasks`,
		`ANALYZE {s}.task_history`,
	} {
		if _, err := pool.Exec(ctx, strings.ReplaceAll(sql, "{s}", schema)); err != nil {
			t.Fatal(err)
		}
	}

	h := handler(t, client, nil)
	_, overview, _ := get(t, h, "/")
	_, task, _ := get(t, h, "/tasks/"+strconv.FormatInt(head, 10))
	_, schedules, _ := get(t, h, "/schedules")
	_, workers, _ := get(t, h, "/workers")
	href := func(page, before, text string) string {
		t.Helper()
		m := regexp.MustCompile(`href="(/tasks\?[^"]*)"[^>]*>`+before).FindAllStringSubmatch(page, -1)
		for _, link := range m {
			if strings.Contains(link[0], text) {
				return html.UnescapeString(link[1])
			}
		}
		t.Fatalf("no link to a task list with %q before %q\n%s", text, before, page)
		return ""
	}

	const most = 51 // a page of tasks, and the task after it
	for _, c := range []struct {
		name, target string
		shows        int   // the tasks that the list shows
		tasks        int64 // the most rows of tasks that the list may read; -1 for any
	}{
		{"the overview's link to a queue", href(overview, "slow<", "queue=slow"), 50, most},
		{"the overview's link to a queue's tasks to retry", href(overview, "0<", "slow&amp;state=retryable"), 50, most},
		{"the overview's link to a queue's 3 tasks to retry, before its backlog", href(overview, "0<", "backlog&amp;state=retryable"), 3, 2003}, // every task of its queue
		{"a task's link to its FIFO group", href(task, "View group", "group=acct-1"), 3, most},
		{"a task's FIFO group", href(task, "acct-1<", "group=acct-1"), 3, most},
		{"a schedule's link to its kind's tasks", href(schedules, "sched<", "kind=report"), 1, most},
		{"a worker's link to a kind's tasks", href(workers, "report<", "kind=report"), 1, -1},
	} {
		if _, ids := listTasks(t, h, c.target); len(ids) != c.shows {
			t.Errorf("%s, %s, shows %d tasks, want %d", c.name, c.target, len(ids), c.shows)
		}
		tasks, history := rowsRead(t, pool, reads)
		if (c.tasks >= 0 && tasks > c.tasks) || history > most {
			t.Errorf("%s, %s, reads %d rows of tasks and %d of the history, want at most %d and %d", c.name, c.target, tasks, history, c.tasks, most)
		}
	}
}

// notDone is the part of a link's query, as a page writes it, that selects
// the unfinished and the dead tasks: every state but completed and canceled.
const notDone = "state=scheduled&amp;state=pending&amp;state=available&amp;state=running&amp;state=retryable&amp;state=dead"

type report struct{}

func (report) Kind() string { return "report" }
