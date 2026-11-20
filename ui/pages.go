package ui

import (
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"time"

	"github.com/code19m/taskmill"
)

const (
	pageSize = 50  // tasks that the task list shows at first, and that "Load more" adds
	maxShown = 250 // most tasks that the task list shows on one page, which it reads at every update

	// staleCounts is how old the counts of the overview may be before the
	// page warns about their age. The maintainer counts every 10 seconds:
	// after a minute, no worker runs as the maintainer, or it cannot count.
	staleCounts = time.Minute
)

// queueStates are the columns of the queue table.
var queueStates = []taskmill.State{
	taskmill.StateAvailable, taskmill.StateRunning, taskmill.StateScheduled, taskmill.StateRetryable,
	taskmill.StatePending, taskmill.StateDead, taskmill.StateCanceled,
}

// totals sums the task counts of every queue.
type totals struct {
	Available, Running, Waiting, Dead taskmill.TaskCount
	Capped                            bool // some count stopped at its cap
}

func sumQueues(queues []taskmill.QueueInfo) totals {
	var t totals
	add := func(sum *taskmill.TaskCount, n taskmill.TaskCount) {
		sum.N += n.N
		sum.Capped = sum.Capped || n.Capped
	}
	for _, q := range queues {
		add(&t.Available, q.Counts[taskmill.StateAvailable])
		add(&t.Running, q.Counts[taskmill.StateRunning])
		for _, st := range []taskmill.State{taskmill.StateScheduled, taskmill.StateRetryable, taskmill.StatePending} {
			add(&t.Waiting, q.Counts[st])
		}
		add(&t.Dead, q.Counts[taskmill.StateDead])
		for _, n := range q.Counts {
			t.Capped = t.Capped || n.Capped
		}
	}
	return t
}

func (s *server) overview(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	queues, err := s.client.Queues(ctx)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	workers, err := s.client.Workers(ctx)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	schedules, err := s.client.Schedules(ctx)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	settings, err := s.client.Settings(ctx)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	now := time.Now()
	span := rangeOf(r.URL.Query().Get("range"))
	win := windowOf(span, now)
	// The charts are not worth failing the page for: without their history,
	// they say so.
	activity, err := s.client.Activity(ctx, &taskmill.ActivityParams{Since: win.start, Step: win.step})
	if err != nil && ctx.Err() == nil {
		s.logger.LogAttrs(ctx, slog.LevelWarn, "read activity failed", slog.Any(logKeyError, err))
	}
	tasksChart, attemptsChart := taskChart(activity, win, now), attemptChart(activity, win, now)
	rate, peak := throughput(activity, win, now)
	if err != nil {
		tasksChart.Note, attemptsChart.Note = "History is unavailable.", "History is unavailable."
	}
	var nextRun time.Time
	for _, sc := range schedules {
		if !sc.Paused && (nextRun.IsZero() || sc.NextRunAt.Before(nextRun)) {
			nextRun = sc.NextRunAt
		}
	}
	var refreshed time.Time
	for _, q := range queues {
		if q.RefreshedAt.After(refreshed) {
			refreshed = q.RefreshedAt
		}
	}
	t := sumQueues(queues)
	s.render(w, r, http.StatusOK, "overview.html", page{Title: "Overview", Nav: "overview", Dead: t.Dead, Data: struct {
		Queues          []taskmill.QueueInfo
		States          []taskmill.State
		Totals          totals
		Workers         int
		Schedules       int
		NextRun         time.Time
		Settings        *taskmill.Settings
		RefreshedAt     time.Time
		Stale           bool // the counts are old enough to say so on the page
		Ranges          []chartRange
		Range           string
		Dial            dial
		Tasks, Attempts chart
	}{
		queues, queueStates, t, len(workers), len(schedules), nextRun, settings, refreshed,
		!refreshed.IsZero() && now.Sub(refreshed) > staleCounts,
		chartRanges, span.Name, newDial(rate, peak), tasksChart, attemptsChart,
	}})
}

func (s *server) tasks(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	f := taskFilter(q)
	first := maps.Clone(q) // the list's first page
	for _, key := range []string{"cursor", "limit", "done", "count", "conflicts"} {
		first.Del(key)
	}
	limit, err := shown(q.Get("limit"))
	if err != nil {
		s.refuse(w, r, http.StatusBadRequest, "Invalid request", reason(err), link{listPath(first), "Back to the first page", "first"})
		return
	}
	since, before, err := listRange(q)
	if err != nil {
		s.refuse(w, r, http.StatusBadRequest, "Invalid request", reason(err),
			link{listPath(withRange(first, time.Time{}, time.Time{})), "Clear the range", "clear"})
		return
	}
	ranged := !since.IsZero() || !before.IsZero()
	order := orderOf(f, ranged)
	first = withRange(first, since, before)
	// The list shows no payload, no metadata, and only each task's last
	// error. Its range is of the time that orders it.
	params := &taskmill.ListParams{Filter: f, Cursor: q.Get("cursor"), Limit: limit, Summary: true}
	if order.ByFinish {
		params.FinishedSince, params.FinishedBefore = since, before
	} else {
		params.CreatedSince, params.CreatedBefore = since, before
	}
	res, err := s.client.List(r.Context(), params)
	if errors.Is(err, taskmill.ErrInvalidArgument) {
		// The error names what the client refused: the filter, or the
		// cursor. With a cursor in the address, the filter may be fine, so
		// the way back is the first page of the same list. If the filter is
		// what is wrong, that page says so again, and offers to clear it.
		text, back := reason(err), link{"/tasks", "Clear the filters", "clear"}
		if params.Cursor != "" {
			text, back = text+cursorHint, link{listPath(first), "Back to the first page", "first"}
		}
		s.refuse(w, r, http.StatusBadRequest, "Invalid request", text, back)
		return
	}
	filtered := ranged || len(f.States)+len(f.Queues)+len(f.Kinds)+len(f.GroupKeys) > 0
	if err != nil {
		hint := ""
		if filtered {
			hint = filtersHint
		}
		s.failWith(w, r, err, hint, "")
		return
	}
	known, err := s.client.Queues(r.Context())
	if err != nil {
		s.fail(w, r, err)
		return
	}
	names := make([]string, 0, len(known))
	for _, kq := range known {
		names = append(names, kq.Name)
	}
	offered := s.filterChoices(r)
	more, next := s.continuation(first, params, res.Next)
	states := filterQuery(f)["state"]
	nav, title := "tasks", "Tasks"
	if slices.Equal(states, []string{string(taskmill.StateDead)}) {
		nav, title = "dead", "Dead tasks"
	}
	stateNames := make([]string, len(listedStates))
	for i, st := range listedStates {
		stateNames[i] = string(st)
	}
	groupFilter := newFilter("group", "FIFO group", "groups", offered.groups, f.GroupKeys).searchable()
	if offered.moreGroups {
		groupFilter.Source = s.path("/choices/groups")
	}
	// A bulk action acts on every task that the filters select, also outside
	// a range of time: a list with a range does not offer it.
	var bulk *bulkLinks
	if filtered && !ranged && !s.readOnly {
		bulk = s.bulkLinks(f)
	}
	s.render(w, r, http.StatusOK, "tasks.html", page{Title: title, Nav: nav, Dead: sumQueues(known).Dead, Data: struct {
		Tasks                         []taskmill.TaskInfo
		Seen                          int // tasks before those that the last "Load more" added
		Order                         listOrder
		States, Queues, Kinds, Groups filter
		Range                         *rangeFilter
		Filtered, Paged               bool
		First, More, Next             string
		Bulk                          *bulkLinks
	}{
		res.Tasks,
		limit - pageSize,
		order,
		newFilter("state", "State", "states", stateNames, states),
		newFilter("queue", "Queue", "queues", names, f.Queues).searchable(),
		newFilter("kind", "Kind", "kinds", offered.kinds, f.Kinds).searchable(),
		groupFilter,
		s.rangeFilter(order.Time, first, since, before, time.Now()),
		filtered, params.Cursor != "",
		s.path(listPath(first)), more, next,
		bulk,
	}})
}

// continuation returns the link that leads on from a task list: none if no
// task follows its last one, where the cursor after continues. The list
// continues in place: more leads to the same list with a page more of it,
// so that the live updates keep every task on screen up to date. It names
// the first task that it adds, "more": a browser without the script loads
// the longer list, and opens it there. Each update reads all the tasks on
// screen, so the list stops growing at maxShown. From there, next leads to
// a page of its own, which starts after the list's last task.
func (s *server) continuation(first url.Values, params *taskmill.ListParams, after string) (more, next string) {
	if after == "" {
		return "", ""
	}
	v := maps.Clone(first)
	if params.Limit >= maxShown {
		v.Set("cursor", after)
		return "", s.path("/tasks?" + v.Encode())
	}
	if params.Cursor != "" {
		v.Set("cursor", params.Cursor)
	}
	v.Set("limit", strconv.Itoa(min(params.Limit+pageSize, maxShown)))
	return s.path("/tasks?" + v.Encode() + "#more"), ""
}

// shown returns how many tasks the task list shows, as the limit in its
// address says: a page, if it says nothing.
func shown(limit string) (int, error) {
	if limit == "" {
		return pageSize, nil
	}
	n, err := strconv.Atoi(limit)
	if err != nil || n < 1 || n > maxShown {
		return 0, fmt.Errorf("the limit must be a number from 1 to %d", maxShown)
	}
	return n, nil
}

// listPath returns the path of the task list with the query q, if it has
// one.
func listPath(q url.Values) string {
	if len(q) == 0 {
		return "/tasks"
	}
	return "/tasks?" + q.Encode()
}

// listOrder is how a task list names its order: the column that orders it,
// what the mark of that column says, and the links to the first and to the
// next page.
type listOrder struct {
	Column            string // the column that orders the list: "id", "created", or "finished"
	Name, First, Next string
	Time              string // the time that the list's range is of: "Created" or "Finished"
	ByFinish          bool   // the list is in the order of when its tasks finished, and shows that time
}

// orderOf returns the order in which Client.List returns the tasks of f. A
// list of only dead tasks, only canceled tasks, or both comes by when the
// tasks finished, the latest first, and a range on it is one of finish
// times. On every other list, a range is one of creation times: with one,
// the list comes by when the tasks were created, the newest first, and
// without one by ID.
func orderOf(f taskmill.TaskFilter, ranged bool) listOrder {
	unfinished := func(s taskmill.State) bool { return s != taskmill.StateDead && s != taskmill.StateCanceled }
	if len(f.States) > 0 && !slices.ContainsFunc(f.States, unfinished) {
		return listOrder{Column: "finished", Name: "Latest first", First: "Latest", Next: "Earlier", Time: "Finished", ByFinish: true}
	}
	order := listOrder{Column: "id", Name: "Newest first", First: "Newest", Next: "Older", Time: "Created"}
	if ranged {
		order.Column = "created"
	}
	return order
}

// group describes a task's place in its FIFO group.
type group struct {
	Waiting taskmill.TaskCount // tasks waiting behind a dead task
	Head    *taskmill.TaskInfo // the task that a pending task waits behind, in summary
}

func (s *server) task(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		s.notFound(w, r)
		return
	}
	ctx := r.Context()
	info, err := s.client.Get(ctx, id)
	if errors.Is(err, taskmill.ErrNotFound) {
		s.taskNotFound(w, r, id)
		return
	}
	if err != nil {
		s.fail(w, r, err)
		return
	}
	g, err := s.groupOf(r, info)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	s.render(w, r, http.StatusOK, "task.html", page{Title: fmt.Sprintf("Task %d", id), Nav: "tasks", Dead: s.deadCount(r), Data: struct {
		Task  *taskmill.TaskInfo
		Group group
	}{info, g}})
}

// groupOf finds, for a dead task of a FIFO group, how many tasks wait behind
// it, and for a pending one, the task it waits behind.
func (s *server) groupOf(r *http.Request, t *taskmill.TaskInfo) (group, error) {
	var g group
	if t.GroupKey == "" {
		return g, nil
	}
	switch t.State {
	case taskmill.StateDead:
		counts, err := s.client.Count(r.Context(), &taskmill.TaskFilter{
			Queues: []string{t.Queue}, GroupKeys: []string{t.GroupKey}, States: []taskmill.State{taskmill.StatePending},
		})
		if err != nil {
			return g, err
		}
		g.Waiting = counts.States[taskmill.StatePending]
	case taskmill.StatePending:
		page, err := s.client.List(r.Context(), &taskmill.ListParams{Filter: taskmill.TaskFilter{
			Queues: []string{t.Queue}, GroupKeys: []string{t.GroupKey}, States: []taskmill.State{
				taskmill.StateScheduled, taskmill.StateAvailable, taskmill.StateRunning, taskmill.StateRetryable, taskmill.StateDead,
			},
		}, Limit: 1, Summary: true})
		if err != nil {
			return g, err
		}
		if len(page.Tasks) > 0 {
			g.Head = &page.Tasks[0]
		}
	default:
	}
	return g, nil
}

func (s *server) schedules(w http.ResponseWriter, r *http.Request) {
	schedules, err := s.client.Schedules(r.Context())
	if err != nil {
		s.fail(w, r, err)
		return
	}
	s.render(w, r, http.StatusOK, "schedules.html", page{Title: "Schedules", Nav: "schedules", Dead: s.deadCount(r), Data: schedules})
}

func (s *server) workers(w http.ResponseWriter, r *http.Request) {
	workers, err := s.client.Workers(r.Context())
	if err != nil {
		s.fail(w, r, err)
		return
	}
	s.render(w, r, http.StatusOK, "workers.html", page{Title: "Workers", Nav: "workers", Dead: s.deadCount(r), Data: workers})
}

// deadCount returns the dead tasks of every queue, or zero if they cannot be
// read; it only feeds the navigation.
func (s *server) deadCount(r *http.Request) taskmill.TaskCount {
	queues, err := s.client.Queues(r.Context())
	if err != nil {
		return taskmill.TaskCount{}
	}
	return sumQueues(queues).Dead
}
