package ui_test

import (
	"html"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/code19m/taskmill"
)

// A task's page shows its timeline: the steps that its row records, in the
// order they came, with the time between two of them, and what is yet to
// come. A line's class says whether it is a step or a gap, a plan or a
// fact, and its color. A gap that lasts until now counts from the step
// before it, as the script reads it. The times lie hours or minutes back,
// so that the relative words hold while the test runs.
func TestTaskTimeline(t *testing.T) {
	t.Parallel()
	client, pool, schema := setupDB(t)
	// A dead task that halts its FIFO group, where a task waits behind it.
	if _, err := pool.Exec(t.Context(), `
	    INSERT INTO `+schema+`.groups (queue, group_key, tasks, halted_task_id, halted_at) VALUES ('mail', 'g', 2, 127, now() - interval '4h');
	    INSERT INTO `+schema+`.tasks (id, queue, kind, state, payload, attempt, max_attempts, group_key, run_at, attempted_at, finalized_at)
	    OVERRIDING SYSTEM VALUE VALUES (127, 'mail', 'greet', 'dead', '{}', 1, 1, 'g', now() - interval '4h', now() - interval '4h', now() - interval '4h')`); err != nil {
		t.Fatal(err)
	}
	// The columns of a task, after its queue, kind, and payload.
	const cols = `state, attempt, max_attempts, group_key, created_at, run_at, attempted_at, errors, finalized_at`
	for _, c := range []struct {
		name   string
		values string // of cols
		want   []string
	}{
		{"scheduled", `'scheduled', 0, 25, NULL, now() - interval '2m10s', now() + interval '3m30s', NULL, '[]', NULL`, []string{
			"step: Created 2m ago",
			"step plan: Due in 3m",
		}},
		{"pending, due before it was created, behind the dead task of its group", `'pending', 0, 25, 'g', now() - interval '3h', now() - interval '3h5m', NULL, '[]', NULL`, []string{
			"step: Created 3h ago due at once",
			"gap warn: waiting 3h, behind #127 (counts)",
			"step plan: Start not yet",
		}},
		{"available, due after it was created", `'available', 0, 25, NULL, now() - interval '5h', now() - interval '4h', NULL, '[]', NULL`, []string{
			"step: Created 5h ago",
			"step: Due 4h ago",
			"gap: waiting 4h (counts)",
			"step plan: Start not yet",
		}},
		{"running its second attempt", `'running', 2, 25, NULL, now() - interval '6h', now() - interval '2h0.3s', now() - interval '2h',
			jsonb_build_array(jsonb_build_object('attempt', 1, 'at', now() - interval '5h', 'message', 'boom')), NULL`, []string{
			"step: Created 6h ago",
			"step: Due 2h ago",
			"gap: waited 0.3s",
			"step run: Started 2h ago attempt 2",
			"gap run: running for 2h (counts)",
		}},
		{"retryable after its first attempt", `'retryable', 1, 25, NULL, now() - interval '7m10s', now() + interval '3m30s', now() - interval '7m9.9s',
			jsonb_build_array(jsonb_build_object('attempt', 1, 'at', now() - interval '7m7.8s', 'message', 'boom')), NULL`, []string{
			"step: Created 7m ago",
			"gap: waited 0.1s",
			"step: Started 7m ago",
			"gap err: failed after 2.1s",
			"step plan: Due again in 3m attempt 2",
		}},
		{"dead after five attempts", `'dead', 5, 5, NULL, now() - interval '2h10m', now() - interval '12m30s', now() - interval '12m29.6s',
			jsonb_build_array(jsonb_build_object('attempt', 5, 'at', now() - interval '12m28.8s', 'message', 'boom')), now() - interval '12m28.8s'`, []string{
			"step: Created 2h ago",
			"step: Due 12m ago",
			"gap: waited 0.4s",
			"step: Started 12m ago attempt 5",
			"gap err: failed after 0.8s",
			"step err: Died 12m ago",
		}},
		{"canceled before it was due", `'canceled', 0, 25, NULL, now() - interval '1h10m', now() + interval '1h19m50s', NULL, '[]', now() - interval '40m10s'`, []string{
			"step: Created 1h ago",
			"step: Canceled 40m ago was due in 2h",
		}},
		{"canceled when its attempt failed", `'canceled', 1, 25, NULL, now() - interval '30m10s', now() - interval '30m10s', now() - interval '30m9.7s',
			jsonb_build_array(jsonb_build_object('attempt', 1, 'at', now() - interval '28m10s', 'message', 'context canceled')), now() - interval '28m10s'`, []string{
			"step: Created 30m ago due at once",
			"gap: waited 0.3s",
			"step: Started 30m ago",
			"gap err: failed after 1m 59s",
			"step: Canceled 28m ago",
		}},
		{"canceled once its retry was due", `'canceled', 1, 25, NULL, now() - interval '50m10s', now() - interval '45m10s', now() - interval '49m10s',
			jsonb_build_array(jsonb_build_object('attempt', 1, 'at', now() - interval '49m9s', 'message', 'boom')), now() - interval '40m10s'`, []string{
			"step: Created 50m ago",
			"step: Started 49m ago",
			"gap err: failed after 1s",
			"step: Due 45m ago",
			"step: Canceled 40m ago",
		}},
		{"canceled while it waited for a worker with a handler", `'canceled', 1, 25, NULL, now() - interval '30m10s', now() + interval '9m50s', now() - interval '20m10s',
			jsonb_build_array(jsonb_build_object('attempt', 1, 'at', now() - interval '25m', 'message', 'boom'),
			    jsonb_build_object('attempt', 1, 'at', now() - interval '20m9s', 'code', 'no_handler', 'message', 'no handler')), now() - interval '15m10s'`, []string{
			"step: Created 30m ago",
			"step: Started 20m ago",
			"step: Canceled 15m ago was due in 25m",
		}},
	} {
		lease := "NULL" // a running task holds a lease
		if strings.HasPrefix(c.values, "'running'") {
			lease = "now() + interval '1 hour'"
		}
		var id int64
		if err := pool.QueryRow(t.Context(), `INSERT INTO `+schema+`.tasks (queue, kind, payload, lease_expires_at, `+cols+`)
		    VALUES ('mail', 'greet', '{}', `+lease+`, `+c.values+`) RETURNING id`).Scan(&id); err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		checkTimeline(t, client, id, c.name, c.want)
	}

	// A completed task is read from the history, and one whose start came
	// before its due time, by the clocks of two machines, has no wait.
	if _, err := pool.Exec(t.Context(), `
	    INSERT INTO `+schema+`.task_history (id, queue, kind, payload, metadata, errors, attempted_by, priority, attempt, max_attempts,
	        created_at, run_at, attempted_at, finalized_at)
	    OVERRIDING SYSTEM VALUE VALUES
	        (1001, 'mail', 'greet', '{}', '{}', '[]', 'w', 2, 1, 25,
	         now() - interval '9m10s', now() - interval '9m10s', now() - interval '9m9.8s', now() - interval '9m8.5s'),
	        (1002, 'mail', 'greet', '{}', '{}', '[]', 'w', 2, 1, 25,
	         now() - interval '20m10s', now() - interval '19m10s', now() - interval '19m11s', now() - interval '19m8s')`); err != nil {
		t.Fatal(err)
	}
	checkTimeline(t, client, 1001, "completed", []string{
		"step: Created 9m ago due at once",
		"gap: waited 0.2s",
		"step: Started 9m ago",
		"gap: ran 1.3s",
		"step ok: Completed 9m ago",
	})
	checkTimeline(t, client, 1002, "completed, started before it was due", []string{
		"step: Created 20m ago",
		"step: Due 19m ago",
		"step: Started 19m ago",
		"gap: ran 3s",
		"step ok: Completed 19m ago",
	})
}

var (
	timelineList = regexp.MustCompile(`(?s)<ol class="timeline" aria-labelledby="timeline-title">(.*?)</ol>`)
	timelineLine = regexp.MustCompile(`(?s)<li class="([^"]*)">(.*?)</li>`)
	anyTag       = regexp.MustCompile(`<[^>]*>`)
	timeOf       = regexp.MustCompile(` data-time="([^"]+)"`)
	sinceOf      = regexp.MustCompile(` data-since="([^"]+)"`)
)

// checkTimeline checks the timeline on the page of task id: each line as
// its class and its words, with "(counts)" after a gap that counts. Such a
// gap must count from the time of the step before it.
func checkTimeline(t *testing.T, client *taskmill.Client, id int64, name string, want []string) {
	t.Helper()
	_, body, _ := get(t, handler(t, client, nil), "/tasks/"+strconv.FormatInt(id, 10))
	list := timelineList.FindStringSubmatch(body)
	if list == nil {
		t.Fatalf("%s: the page has no timeline:\n%s", name, body)
	}
	var got []string
	var stepAt string
	for _, li := range timelineLine.FindAllStringSubmatch(list[1], -1) {
		words := strings.Join(strings.Fields(html.UnescapeString(anyTag.ReplaceAllString(li[2], ""))), " ")
		if m := timeOf.FindStringSubmatch(li[2]); m != nil {
			stepAt = m[1]
		}
		if m := sinceOf.FindStringSubmatch(li[2]); m != nil {
			words += " (counts)"
			if m[1] != stepAt {
				t.Errorf("%s: %q counts from %s, not from the step before it, at %s", name, words, m[1], stepAt)
			}
		}
		got = append(got, li[1]+": "+words)
	}
	if !slices.Equal(got, want) {
		t.Errorf("%s: the timeline says\n\t%s\nwant\n\t%s", name, strings.Join(got, "\n\t"), strings.Join(want, "\n\t"))
	}
	// The task that it waits behind is a link, which a screen reader names
	// as a task.
	if strings.Contains(list[1], "behind") && !strings.Contains(list[1], `behind <a href="/tasks/127" aria-label="task 127">#127</a>`) {
		t.Errorf("%s: the task that it waits behind is no link named as a task:\n%s", name, list[1])
	}
}

// The two columns of a task's page end on one line: the left one is a
// stack of cards, whose last card, the errors, takes the room that the
// Details card leaves it, and an empty one fills it with its note. The
// style reads that order (.stack in style.css), whatever cards come before.
func TestTaskColumnsEndTogether(t *testing.T) {
	t.Parallel()
	client := setup(t)
	res, err := client.Enqueue(t.Context(), greet{Name: "a"}, &taskmill.EnqueueOptions{Metadata: map[string]string{"tenant": "acme"}})
	if err != nil {
		t.Fatal(err)
	}
	_, body, _ := get(t, handler(t, client, nil), "/tasks/"+strconv.FormatInt(res.ID, 10))
	columns := regexp.MustCompile(`(?s)<div class="task-layout">\s*<div class="stack">.*<h2>Metadata</h2>.*` +
		`<section class="card flush">\s*<div class="card-head"><h2>Errors <span class="subtle">0</span></h2></div>\s*` +
		`<div class="card-body"><div class="empty-note"><p class="empty-title">No errors</p></div></div>\s*</section>\s*</div>\s*<aside class="card flush details">`)
	if !columns.MatchString(body) {
		t.Errorf("the task's page does not end its left column with the card of its errors, beside the details:\n%s", body)
	}
}
