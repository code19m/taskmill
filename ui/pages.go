package ui

import (
	"cmp"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
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

// totals sums the task counts of every queue. Waiting holds the scheduled,
// retryable, and pending tasks: the overview's card of that name leads to the
// list of the three states.
type totals struct {
	Available, Running, Waiting, Dead taskmill.TaskCount
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
	}
	return t
}

// overviewData is what the overview shows.
type overviewData struct {
	Queues          []taskmill.QueueInfo
	States          []taskmill.State // the columns of the queue table
	Totals          totals
	Workers         int
	Schedules       int
	NextRun         time.Time // of the schedules that are not paused
	Settings        *taskmill.Settings
	RefreshedAt     time.Time // when the maintainer last counted the tasks
	Stale           bool      // the counts are old enough to say so on the page
	Periods         []chartPeriod
	Period          string // the name of the period that the charts show
	Dial            dial
	Tasks, Attempts chart
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
	p := periodOf(r.URL.Query().Get("range"))
	win := windowOf(p, now)

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
	s.render(w, r, http.StatusOK, "overview.html", page{Title: "Overview", Nav: "overview", Dead: t.Dead, Data: overviewData{
		Queues: queues, States: queueStates, Totals: t,
		Workers: len(workers), Schedules: len(schedules), NextRun: nextRun,
		Settings: settings, RefreshedAt: refreshed, Stale: !refreshed.IsZero() && now.Sub(refreshed) > staleCounts,
		Periods: chartPeriods, Period: p.Name, Dial: newDial(rate, peak), Tasks: tasksChart, Attempts: attemptsChart,
	}})
}

// taskListData is what a task list shows.
type taskListData struct {
	Tasks                         []taskmill.TaskInfo
	Seen                          int // tasks before those that the last "Load more" added
	Order                         listOrder
	States, Queues, Kinds, Groups filter
	Range                         *rangeFilter
	Filtered, Paged               bool
	First, More, Next             string
	Bulk                          *bulkLinks
	Empty                         emptyNote // what the list says when it shows no task
}

func (s *server) taskList(w http.ResponseWriter, r *http.Request) {
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

	d := taskListData{
		Tasks: res.Tasks, Seen: limit - pageSize, Order: order,
		States:   newFilter("state", "State", "states", listedStateNames, states),
		Queues:   newFilter("queue", "Queue", "queues", names, f.Queues).searchable(),
		Kinds:    newFilter("kind", "Kind", "kinds", offered.kinds, f.Kinds).searchable(),
		Groups:   groupFilter,
		Range:    s.newRangeFilter(order.Time, first, since, before, time.Now()),
		Filtered: filtered, Paged: params.Cursor != "",
		First: s.path(listPath(first)), More: more, Next: next,
		Bulk: bulk,
	}
	if len(res.Tasks) == 0 {
		d.Empty = s.noTasks(&d, nav == "dead")
	}

	s.render(w, r, http.StatusOK, "tasks.html", page{Title: title, Nav: nav, Dead: sumQueues(known).Dead, Data: d})
}

// noTasks says why a task list shows no task: its page is past the end of
// the list, no task is dead, the filters select none, or no task is there
// yet. dead is true for a list of dead tasks.
func (s *server) noTasks(d *taskListData, dead bool) emptyNote {
	switch {
	case d.Paged:
		return emptyNote{
			Glyph: "inbox", Title: "No more tasks", Text: "The list ends before this page.",
			link: link{d.First, d.Order.First, "first"},
		}
	case dead && !d.Range.Set && d.Queues.Empty && d.Kinds.Empty && d.Groups.Empty:
		return emptyNote{
			Glyph: "done", Title: "No dead tasks",
			Text: "A task that fails all its attempts shows here, until you retry or delete it.",
		}
	case d.Filtered:
		return emptyNote{
			Glyph: "filter", Title: "No tasks match these filters", Text: "Fewer filters, or a longer range of time, may find some.",
			link: link{s.path("/tasks"), "Clear the filters", "clear"},
		}
	}
	return emptyNote{Glyph: "inbox", Title: "No tasks yet", Text: "Tasks show here once your app enqueues them."}
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

// Waits says how many tasks wait behind a dead task: "1 task waits", or
// "3 tasks wait".
func (g group) Waits() string {
	if g.Waiting == (taskmill.TaskCount{N: 1}) {
		return "1 task waits"
	}
	return tasks(g.Waiting) + " wait"
}

// taskData is what a task's page shows.
type taskData struct {
	Task  *taskmill.TaskInfo
	Group group
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

	s.render(w, r, http.StatusOK, "task.html", page{Title: fmt.Sprintf("Task %d", id), Nav: "tasks", Dead: s.deadCount(r), Data: taskData{Task: info, Group: g}})
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
		counts, err := s.client.Count(r.Context(), &taskmill.CountParams{Filter: taskmill.TaskFilter{
			Queues: []string{t.Queue}, GroupKeys: []string{t.GroupKey}, States: []taskmill.State{taskmill.StatePending},
		}})
		if err != nil {
			return g, err
		}
		g.Waiting = counts.States[taskmill.StatePending]
	case taskmill.StatePending:
		ahead, err := s.client.List(r.Context(), &taskmill.ListParams{Filter: taskmill.TaskFilter{
			Queues: []string{t.Queue}, GroupKeys: []string{t.GroupKey}, States: []taskmill.State{
				taskmill.StateScheduled, taskmill.StateAvailable, taskmill.StateRunning, taskmill.StateRetryable, taskmill.StateDead,
			},
		}, Limit: 1, Summary: true})
		if err != nil {
			return g, err
		}
		if len(ahead.Tasks) > 0 {
			g.Head = &ahead.Tasks[0]
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
	s.render(w, r, http.StatusOK, "workers.html", page{Title: "Workers", Nav: "workers", Dead: s.deadCount(r), Data: workerRows(workers)})
}

// workerRow is a worker as a row of the Workers page shows it.
type workerRow struct {
	taskmill.WorkerInfo

	// ScheduleList holds the worker's schedules, and the maintainer's that
	// it lacks, those that differ first. It is empty for a worker that
	// stores none.
	ScheduleList []scheduleItem
	Differs      bool // its schedules differ from the maintainer's, by name
}

// scheduleItem is a schedule in a worker's list.
type scheduleItem struct {
	Name     string
	Missing  bool // the maintainer defines it, and the worker does not
	OnlyHere bool // the worker defines it, and the maintainer does not
}

// workerRows returns the rows of the Workers page, with the schedules of
// each live worker compared with the maintainer's, as the maintainer
// compares them. The page compares their names alone: the definitions are
// only in the workers' table, and the maintainer's log names those that
// differ. A worker that stores no schedules, or a maintainer that stores
// none, compares with nothing.
func workerRows(workers []taskmill.WorkerInfo) []workerRow {
	var lead map[string]bool // the maintainer's schedules; nil if it stores none
	for _, w := range workers {
		if w.Maintainer && w.Schedules != nil {
			lead = make(map[string]bool, len(w.Schedules))
			for _, name := range w.Schedules {
				lead[name] = true
			}
		}
	}

	rows := make([]workerRow, len(workers))
	for i, w := range workers {
		rows[i] = workerRow{WorkerInfo: w}
		if w.Schedules == nil {
			continue
		}

		compared := lead
		if w.Stale {
			compared = nil
		}
		rows[i].ScheduleList = scheduleItems(w.Schedules, compared)
		rows[i].Differs = slices.ContainsFunc(rows[i].ScheduleList, func(s scheduleItem) bool { return s.Missing || s.OnlyHere })
	}
	return rows
}

// scheduleItems lists the schedules own, and those of lead that own lacks,
// and marks each one that one of the two lacks. Those that differ come
// first: the missing ones, then those that lead lacks, then the others,
// each by name. Without lead, it lists own alone.
func scheduleItems(own []string, lead map[string]bool) []scheduleItem {
	mine := make(map[string]bool, len(own))
	list := make([]scheduleItem, 0, len(own))
	for _, name := range own {
		mine[name] = true
		list = append(list, scheduleItem{Name: name, OnlyHere: lead != nil && !lead[name]})
	}
	for name := range lead {
		if !mine[name] {
			list = append(list, scheduleItem{Name: name, Missing: true})
		}
	}

	rank := func(s scheduleItem) int {
		switch {
		case s.Missing:
			return 0
		case s.OnlyHere:
			return 1
		default:
			return 2
		}
	}
	slices.SortFunc(list, func(a, b scheduleItem) int {
		return cmp.Or(cmp.Compare(rank(a), rank(b)), strings.Compare(a.Name, b.Name))
	})
	return list
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
