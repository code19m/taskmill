package ui

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/code19m/taskmill"
)

// errBudget is why a bulk action stopped before it was done.
var errBudget = errors.New("the bulk action used its time")

// selection is what a task list shows, and what a bulk action on it acts
// on: the tasks that its filters select, in its range of the time that
// orders it.
type selection struct {
	filter        taskmill.TaskFilter
	since, before time.Time // the bounds of the range; zero for none
	order         listOrder
}

// selectionOf reads the selection in the query of a task list.
func selectionOf(q url.Values) (selection, error) {
	since, before, err := listRange(q)
	f := taskFilter(q)
	return selection{filter: f, since: since, before: before, order: orderOf(f, !since.IsZero() || !before.IsZero())}, err
}

// filtered reports whether the selection's filters select tasks by what they
// are, as a bulk action needs: a range alone is no filter.
func (sel selection) filtered() bool {
	f := sel.filter
	return len(f.States)+len(f.Queues)+len(f.Kinds)+len(f.GroupKeys) > 0
}

// query returns the query of the selection, for links and redirects.
func (sel selection) query() url.Values {
	return withRange(filterQuery(sel.filter), sel.since, sel.before)
}

// ranges returns the range of the selection as the client takes it: of
// finish times on a list in finish order, and of creation times on every
// other.
func (sel selection) ranges() (createdSince, createdBefore, finishedSince, finishedBefore time.Time) {
	if sel.order.ByFinish {
		return time.Time{}, time.Time{}, sel.since, sel.before
	}
	return sel.since, sel.before, time.Time{}, time.Time{}
}

// bulkLinks lead to the pages of the bulk actions on the tasks that the
// filters select; empty for an action that no selected state allows.
type bulkLinks struct {
	Retry, Cancel, Delete string
}

func (s *server) bulkLinks(sel selection) *bulkLinks {
	states := sel.filter.States
	if len(states) == 0 {
		states = listedStates
	}

	q := sel.query().Encode()
	href := func(name string) string {
		a := bulkActions[name]
		if !slices.ContainsFunc(states, a.allows) {
			return ""
		}
		return s.path(a.path + "?" + q)
	}
	return &bulkLinks{href("retry"), href("cancel"), href("delete")}
}

// bulkAction is an action on every task that the task list's filters select.
type bulkAction struct {
	Verb    string // what its button says
	Icon    string
	Danger  bool
	Explain string // what it does
	None    string // why it may find no task
	path    string
	allows  func(taskmill.State) bool // the states that it changes
	do      func(*taskmill.Client, context.Context, *taskmill.BulkParams) (taskmill.BulkResult, error)
	count   func(*taskmill.TaskCounts) taskmill.TaskCount
}

// changes counts the tasks of the selection that the action would change.
// It counts only the states that the action changes: the others can hold
// many more tasks, such as every completed task of a kind, which the count
// would read for nothing. With none of those states left, it counts
// nothing. A state that is not one stays, for the client to refuse.
func (a bulkAction) changes(ctx context.Context, client *taskmill.Client, sel selection) (taskmill.TaskCount, error) {
	p := &taskmill.CountParams{Filter: sel.filter}
	states := p.Filter.States
	if len(states) == 0 {
		states = listedStates
	}
	p.Filter.States = slices.DeleteFunc(slices.Clone(states), func(st taskmill.State) bool {
		return slices.Contains(listedStates, st) && !a.allows(st)
	})
	if len(p.Filter.States) == 0 {
		return taskmill.TaskCount{}, nil
	}

	p.CreatedSince, p.CreatedBefore, p.FinishedSince, p.FinishedBefore = sel.ranges()
	counts, err := client.Count(ctx, p)
	if err != nil {
		return taskmill.TaskCount{}, err
	}
	return a.count(counts), nil
}

var bulkActions = map[string]bulkAction{
	"retry": {
		Verb: "Retry", Icon: "retry",
		Explain: "They run again with a fresh set of attempts. A dead task that halts its FIFO group resumes the group. A canceled task stays canceled if another task holds its dedupe key.",
		None:    "Retry applies to dead and canceled tasks.",
		path:    "/tasks/bulk/retry",
		allows:  taskmill.State.CanRetry,
		do:      (*taskmill.Client).RetryMatching,
		count:   func(c *taskmill.TaskCounts) taskmill.TaskCount { return c.Retry },
	},
	"cancel": {
		Verb: "Cancel", Icon: "cancel",
		Explain: "Waiting tasks will not run. Running tasks stop when their handlers return.",
		None:    "Cancel applies to unfinished tasks, except running tasks already asked to stop.",
		path:    "/tasks/bulk/cancel",
		allows:  taskmill.State.CanCancel,
		do:      (*taskmill.Client).CancelMatching,
		count:   func(c *taskmill.TaskCounts) taskmill.TaskCount { return c.Cancel },
	},
	"delete": {
		Verb: "Delete", Icon: "trash", Danger: true,
		Explain: "Running tasks are kept. This cannot be undone.",
		None:    "Delete applies to every task except running ones.",
		path:    "/tasks/bulk/delete",
		allows:  taskmill.State.CanDelete,
		do:      (*taskmill.Client).DeleteMatching,
		count:   func(c *taskmill.TaskCounts) taskmill.TaskCount { return c.Delete },
	},
}

// filterRow is one filter that a bulk action's page shows.
type filterRow struct {
	Title         string
	Values        []string
	Mono          bool
	Group         bool      // the values are FIFO group keys
	Since, Before time.Time // the bounds of a row of a range, which the script words in the reader's time zone
}

// bulkData is what a bulk action's page shows.
type bulkData struct {
	bulkAction
	Filters []filterRow
	N       int64  // the tasks that the action would change
	Tasks   string // N in words
	Path    string // where its form sends the action
	Back    string // the task list of the filters
}

// bulkPage shows what a bulk action would change, with a button to do it:
// the confirmation. Without the script it is a page, and with it, the
// dialog shows it over the task list. It counts the tasks once, and does
// not update itself.
func (s *server) bulkPage(w http.ResponseWriter, r *http.Request) {
	if s.refuseIfReadOnly(w, r) {
		return
	}

	a, ok := bulkActions[r.PathValue("action")]
	if !ok {
		s.notFound(w, r)
		return
	}

	sel, err := selectionOf(r.URL.Query())
	if err != nil {
		s.refuse(w, r, http.StatusBadRequest, "Invalid request", reason(err), backTo("/tasks?"+filterQuery(sel.filter).Encode()))
		return
	}
	if !sel.filtered() {
		s.refuse(w, r, http.StatusBadRequest, "Choose filters first",
			"A bulk action acts on the tasks that the task list's filters select.", link{"/tasks", "Back to the tasks", "back"})
		return
	}

	n, err := a.changes(r.Context(), s.client, sel)
	if errors.Is(err, taskmill.ErrInvalidArgument) {
		s.refuse(w, r, http.StatusBadRequest, "Invalid filter", reason(err), link{"/tasks", "Clear the filters", "clear"})
		return
	}
	if err != nil {
		s.fail(w, r, err)
		return
	}

	f := sel.filter
	var states, period []string
	for _, st := range f.States {
		states = append(states, string(st))
	}
	if !sel.since.IsZero() || !sel.before.IsZero() {
		_, full := rangeWords(sel.since, sel.before, time.Now())
		period = []string{full}
	}
	var rows []filterRow
	for _, row := range []filterRow{
		{Title: "State", Values: states},
		{Title: "Queue", Values: f.Queues},
		{Title: "Kind", Values: f.Kinds, Mono: true},
		{Title: "FIFO group", Values: f.GroupKeys, Mono: true, Group: true},
		{Title: sel.order.Time, Values: period, Since: sel.since, Before: sel.before},
	} {
		if len(row.Values) > 0 {
			rows = append(rows, row)
		}
	}

	// The form carries where a run that stopped early continues.
	action := sel.query()
	if cursor := r.URL.Query().Get("cursor"); cursor != "" {
		action.Set("cursor", cursor)
	}

	s.render(w, r, http.StatusOK, "bulk.html", page{Title: a.Verb + " tasks", Nav: "tasks", Static: true, Dead: s.deadCount(r), Data: bulkData{
		bulkAction: a, Filters: rows, N: n.N, Tasks: tasks(n),
		Path: s.path(a.path + "?" + action.Encode()), Back: s.path(listPath(sel.query())),
	}})
}

// notices are the messages shown after an action, by the code in the
// redirect's "done" parameter. Only these fixed texts are shown, so a link
// cannot make the dashboard display text of its choosing.
var notices = map[string]string{
	"retry":           "Task retried. It runs again with a fresh set of attempts.",
	"cancel":          "Task canceled. A running task stops when its handler returns.",
	"delete":          "Task deleted.",
	"queue-pause":     "Queue paused. Running tasks finish; no new ones start.",
	"queue-resume":    "Queue resumed.",
	"schedule-pause":  "Schedule paused.",
	"schedule-resume": "Schedule resumed.",
	"schedule-run":    "The schedule runs within a second.",
	"retry-all":       "{tasks} retried. They run again with fresh attempts.",
	"retry-more":      "{tasks} retried so far; more remain.",
	"retry-skipped":   "{tasks} retried; other work held {skipped}. Try again for those.",
	"retry-conflicts": "{tasks} retried; {conflicts} not, because another task holds the same dedupe key.",
	"cancel-all":      "{tasks} canceled. Running tasks stop when their handlers return.",
	"cancel-more":     "{tasks} canceled so far; more remain.",
	"cancel-skipped":  "{tasks} canceled; other work held {skipped}. Try again for those.",
	"delete-all":      "{tasks} deleted.",
	"delete-more":     "{tasks} deleted so far; more remain.",
	"delete-skipped":  "{tasks} deleted; other work held {skipped}. Try again for those.",
}

// notice returns the notice that a query asks for, with the numbers of
// tasks in its "count", "skipped", and "conflicts" parameters for a bulk
// action.
func notice(q url.Values) string {
	text := notices[q.Get("done")]
	if !strings.Contains(text, "{tasks}") {
		return text
	}

	n, err := strconv.ParseInt(q.Get("count"), 10, 64)
	if err != nil || n < 0 {
		return ""
	}

	text = strings.Replace(text, "{tasks}", tasks(taskmill.TaskCount{N: n}), 1)
	for _, key := range []string{"skipped", "conflicts"} {
		text = strings.Replace(text, "{"+key+"}", tasks(taskmill.TaskCount{N: queryNumber(q, key)}), 1)
	}
	return text
}

// queryNumber reads a count of tasks from the query; 0 if it is not one.
func queryNumber(q url.Values, key string) int64 {
	n, err := strconv.ParseInt(q.Get(key), 10, 64)
	if err != nil || n < 0 {
		return 0
	}
	return n
}

func (s *server) taskAction(w http.ResponseWriter, r *http.Request) {
	if s.refuseIfReadOnly(w, r) {
		return
	}

	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		s.notFound(w, r)
		return
	}

	ctx := r.Context()
	target := "/tasks/" + strconv.FormatInt(id, 10)
	var done string
	switch r.PathValue("action") {
	case "retry":
		err, done = s.client.Retry(ctx, id), "retry"
	case "cancel":
		err, done = s.client.Cancel(ctx, id), "cancel"
	case "delete":
		err, done, target = s.client.Delete(ctx, id), "delete", "/tasks"
	default:
		s.notFound(w, r)
		return
	}

	s.finishAction(w, r, err, target, done, done+" task", strconv.FormatInt(id, 10))
}

// queueAction pauses or resumes the queue that the query names. A name is
// any text, and a path cannot carry every name: a browser resolves a segment
// of dots away, escaped or not. So the name travels as the filters of a bulk
// action do. The query also names the period of the overview's charts, and
// the action leads back to the overview of that period: the charts stay as
// the reader set them.
func (s *server) queueAction(w http.ResponseWriter, r *http.Request) {
	if s.refuseIfReadOnly(w, r) {
		return
	}

	var err error
	var done, action string
	name := r.URL.Query().Get("name")
	back := "/"
	if p := periodOf(r.URL.Query().Get("range")); p != chartPeriods[0] {
		back = "/?range=" + p.Name
	}

	switch r.PathValue("action") {
	case "pause":
		err, done, action = s.client.PauseQueue(r.Context(), name), "queue-pause", "pause queue"
	case "resume":
		err, done, action = s.client.ResumeQueue(r.Context(), name), "queue-resume", "resume queue"
	default:
		s.notFound(w, r)
		return
	}

	s.finishAction(w, r, err, back, done, action, name)
}

// scheduleAction pauses, resumes, or runs the schedule that the query names,
// as queueAction names its queue.
func (s *server) scheduleAction(w http.ResponseWriter, r *http.Request) {
	if s.refuseIfReadOnly(w, r) {
		return
	}

	var err error
	var done, action string
	name := r.URL.Query().Get("name")
	switch r.PathValue("action") {
	case "pause":
		err, done, action = s.client.PauseSchedule(r.Context(), name), "schedule-pause", "pause schedule"
	case "resume":
		err, done, action = s.client.ResumeSchedule(r.Context(), name), "schedule-resume", "resume schedule"
	case "run":
		err, done, action = s.client.RunSchedule(r.Context(), name), "schedule-run", "run schedule"
	default:
		s.notFound(w, r)
		return
	}

	s.finishAction(w, r, err, "/schedules", done, action, name)
}

// finishAction logs an action that succeeded and redirects to target with
// the code of its notice, done, or explains why the action did not apply.
// The target may have a query of its own.
func (s *server) finishAction(w http.ResponseWriter, r *http.Request, err error, target, done, action, name string) {
	if err != nil {
		s.actionFailed(w, r, err, target)
		return
	}

	s.logger.LogAttrs(r.Context(), slog.LevelInfo, "dashboard action",
		slog.String(logKeyAction, action), slog.String(logKeyTarget, name))

	sep := "?"
	if strings.Contains(target, "?") {
		sep = "&"
	}
	http.Redirect(w, r, s.path(target+sep+"done="+done), http.StatusSeeOther)
}

// actionFailed explains why an action did not apply: the error says it, as
// for a retry that another task's dedupe key refuses. An action with a value
// that is not valid, such as an unknown state or an empty name, is a bad
// request, as the same value is on a page. An action that does not fit the
// task or the schedule as it is now is not possible.
func (s *server) actionFailed(w http.ResponseWriter, r *http.Request, err error, back string) {
	switch {
	case errors.Is(err, taskmill.ErrNotFound):
		if strings.HasPrefix(back, "/tasks/") {
			back = "/tasks" // the task's own page is gone too
		}
		s.status(w, r, http.StatusNotFound, "Not found", statusPage{
			Glyph: "search",
			Text:  "What the action was for no longer exists. It may have been deleted meanwhile.", Actions: []link{backTo(back)},
		})
	case errors.Is(err, taskmill.ErrInvalidArgument):
		s.refuse(w, r, http.StatusBadRequest, "Invalid request", reason(err), backTo(back))
	case errors.Is(err, taskmill.ErrInvalidState), errors.Is(err, taskmill.ErrConflict):
		s.refuse(w, r, http.StatusConflict, "Not possible", reason(err), backTo(back))
	default:
		s.failWith(w, r, err, "", back)
	}
}

// bulkAction retries, cancels, or deletes every task of the selection in
// the query: the tasks that its filters select, in its range. It works for
// half the time left before the request's deadline, so that it answers in
// time. If more tasks remain, it leads back to its page, whose form
// continues where it stopped. If it left tasks that other work held, it
// leads back to its page, which offers to try those again. Otherwise it
// leads to the task list, and says how many tasks a retry left because
// other tasks hold their dedupe keys: trying again does not help those.
// Only its page carries where a run continues: the task list always shows
// every task of the selection.
func (s *server) bulkAction(w http.ResponseWriter, r *http.Request) {
	if s.refuseIfReadOnly(w, r) {
		return
	}

	name := r.PathValue("action")
	a, ok := bulkActions[name]
	if !ok {
		s.notFound(w, r)
		return
	}

	sel, err := selectionOf(r.URL.Query())
	if err != nil {
		s.refuse(w, r, http.StatusBadRequest, "Invalid request", reason(err), backTo("/tasks?"+filterQuery(sel.filter).Encode()))
		return
	}

	end, _ := r.Context().Deadline() // the deadline handler sets one
	ctx, cancel := context.WithDeadlineCause(r.Context(), time.Now().Add(time.Until(end)/2), errBudget)
	defer cancel()

	p := &taskmill.BulkParams{Filter: sel.filter, Cursor: r.URL.Query().Get("cursor")}
	p.CreatedSince, p.CreatedBefore, p.FinishedSince, p.FinishedBefore = sel.ranges()
	res, err := a.do(s.client, ctx, p)
	more := err != nil && errors.Is(context.Cause(ctx), errBudget)
	if err == nil || more || res.Changed > 0 {
		s.logger.LogAttrs(r.Context(), slog.LevelInfo, "dashboard action",
			slog.String(logKeyAction, name+" tasks"), slog.String(logKeyTarget, sel.query().Encode()),
			slog.Int64(logKeyTasks, res.Changed), slog.Int64(logKeySkip, res.Skipped))
	}
	if err != nil && !more {
		s.actionFailed(w, r, err, listPath(sel.query()))
		return
	}

	q := sel.query()
	q.Set("count", strconv.FormatInt(res.Changed, 10))
	target := "/tasks?"
	switch {
	case more:
		target = a.path + "?"
		q.Set("done", name+"-more")
		q.Set("cursor", res.Next)
	case res.Skipped > 0:
		target = a.path + "?"
		q.Set("done", name+"-skipped")
		q.Set("skipped", strconv.FormatInt(res.Skipped, 10))
	case res.Conflicts > 0:
		q.Set("done", name+"-conflicts")
		q.Set("conflicts", strconv.FormatInt(res.Conflicts, 10))
	default:
		q.Set("done", name+"-all")
	}

	http.Redirect(w, r, s.path(target+q.Encode()), http.StatusSeeOther) //nolint:gosec // the request adds only a query to a fixed path
}

// refuseIfReadOnly refuses an action of a read-only dashboard, and says so.
func (s *server) refuseIfReadOnly(w http.ResponseWriter, r *http.Request) bool {
	if !s.readOnly {
		return false
	}
	s.status(w, r, http.StatusForbidden, "Read-only dashboard", statusPage{
		Glyph:   "lock",
		Text:    "This dashboard shows the tasks, and does not change them.",
		Actions: []link{{"/", "Back to the overview", "back"}},
	})
	return true
}
