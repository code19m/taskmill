// Package ui serves taskmill's web interface: a dashboard of queues, tasks,
// schedules, and workers, with actions to pause and resume queues and
// schedules, run a schedule now, and retry, cancel, and delete tasks, one at
// a time or all that match the task list's filters.
//
// The interface is rendered on the server with the standard library. A small
// script, served with it, keeps pages live and adds dialogs; every
// page also works without it, and nothing needs a JavaScript build step. It
// reads only through the bounded queries of [taskmill.Client], so it is safe
// to point at a busy production database.
//
// Mount it in an application, behind the application's own authentication:
//
//	mux.Handle("/taskmill/", auth(ui.Handler(client, &ui.Config{Prefix: "/taskmill"})))
//
// Or protect it with [Config.BasicAuth], for example when the command
// github.com/code19m/taskmill/cmd/taskmill-ui serves it as a dashboard of
// its own. Another web application can show it in a frame once its origin is
// listed in [Config.FrameAncestors].
//
// Actions are protected against cross-site request forgery with
// [http.CrossOriginProtection], and [Config.ReadOnly] disables them entirely.
package ui

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"io"
	"log/slog"
	"maps"
	"mime"
	"net/http"
	"net/url"
	"path"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/code19m/taskmill"
)

//go:embed templates static
var assets embed.FS

// Log attribute keys.
const (
	logKeyAction = "action"
	logKeyError  = "error"
	logKeyPage   = "page"
	logKeyPath   = "path"
	logKeyTarget = "target"
	logKeyTasks  = "tasks"
)

const (
	pageSize       = 50
	groupPeek      = 100              // most waiting tasks of a group that a task page counts
	staleHeartbeat = 30 * time.Second // a worker silent this long is shown as stale
	statsCap       = 10_000           // the per-queue count cap of the statistics
	countCap       = 10_000           // where Client.Count stops counting a state
	requestTimeout = 10 * time.Second // bound on one request, so a slow database cannot hold it
	bulkBudget     = 5 * time.Second  // the time a bulk action works within a request; the rest waits for another
	maxChoices     = 100              // most values of one filter that a request may choose
	gzipMin        = 1024             // smallest response worth compressing
)

// liveHeader marks the requests of the page's live updates. They are never
// answered with a login prompt: the page shows that the session ended instead.
const liveHeader = "X-Taskmill-Live"

// errSlow is why a request that ran out of time was canceled.
var errSlow = errors.New("the database did not answer in time")

// errBudget is why a bulk action stopped before it was done.
var errBudget = errors.New("the bulk action used its time")

// Config configures the web interface. The zero value is valid.
type Config struct {
	// Prefix is the path the handler is mounted at, such as "/taskmill".
	// The handler strips it from requests and adds it to links and
	// redirects, so do not wrap the handler in http.StripPrefix.
	// Default: none, the root.
	Prefix string

	// ReadOnly hides and refuses every action that changes state.
	ReadOnly bool

	// BasicAuth, if set, requires every request to log in with HTTP basic
	// authentication. Serve the interface over HTTPS when it is set: basic
	// authentication sends the password with every request. Default: none;
	// the application in front of the handler authenticates.
	BasicAuth *BasicAuth

	// FrameAncestors lists the origins of the pages that may show the
	// interface in a frame, such as "https://admin.example.com", or "'self'"
	// for the application's own pages. Default: none; the interface cannot be
	// framed, which protects it from clickjacking.
	FrameAncestors []string

	// Logger receives every action taken, and the errors that the interface
	// shows as a server error. Default: logs are discarded.
	Logger *slog.Logger
}

// BasicAuth is the one user that may use the interface.
type BasicAuth struct {
	Username string
	Password string
}

// Handler returns the web interface for client. cfg may be nil. Handler
// panics if cfg.BasicAuth has an empty username or password, or if an entry
// of cfg.FrameAncestors is empty or holds a space, comma, or semicolon.
func Handler(client *taskmill.Client, cfg *Config) http.Handler {
	if cfg == nil {
		cfg = &Config{}
	}
	s := &server{
		client:   client,
		prefix:   strings.TrimRight(cfg.Prefix, "/"),
		readOnly: cfg.ReadOnly,
		logger:   cfg.Logger,
	}
	s.files, s.versions = loadStatic()
	if s.logger == nil {
		s.logger = slog.New(slog.DiscardHandler)
	}
	s.pages = template.Must(template.New("").Funcs(s.funcs()).ParseFS(assets, "templates/*.html"))

	mux := http.NewServeMux()
	mux.Handle("GET /static/{name}", s.static())
	mux.HandleFunc("GET /{$}", s.overview)
	mux.HandleFunc("GET /tasks", s.tasks)
	mux.HandleFunc("GET /tasks/{id}", s.task)
	mux.HandleFunc("GET /choices/groups", s.groupChoicesJSON)
	mux.HandleFunc("GET /schedules", s.schedules)
	mux.HandleFunc("GET /workers", s.workers)
	mux.HandleFunc("POST /tasks/{id}/{action}", s.taskAction)
	mux.HandleFunc("POST /tasks/bulk/{action}", s.bulkAction)
	mux.HandleFunc("POST /queues/{name}/{action}", s.queueAction)
	mux.HandleFunc("POST /schedules/{name}/{action}", s.scheduleAction)

	h := deadline(http.NewCrossOriginProtection().Handler(mux))
	if s.prefix != "" {
		h = http.StripPrefix(s.prefix, h)
	}
	if cfg.BasicAuth != nil {
		h = basicAuth(h, *cfg.BasicAuth)
	}
	return securityHeaders(h, frameAncestors(cfg.FrameAncestors))
}

type server struct {
	client   *taskmill.Client
	pages    *template.Template
	prefix   string
	readOnly bool
	logger   *slog.Logger
	versions map[string]string // static file name to a hash of its content
	files    map[string]staticFile
	choices  choiceCache
}

// page is what every template receives.
type page struct {
	Title    string
	Nav      string // the active section
	ReadOnly bool
	Notice   string
	Dead     int64 // dead tasks, for the navigation; 0 if unknown
	Data     any
}

// listedStates are the states the task list can filter by, in lifecycle
// order.
var listedStates = []taskmill.State{
	taskmill.StateScheduled, taskmill.StatePending, taskmill.StateAvailable, taskmill.StateRunning,
	taskmill.StateRetryable, taskmill.StateCompleted, taskmill.StateDead, taskmill.StateCanceled,
}

// queueStates are the columns of the queue table.
var queueStates = []taskmill.State{
	taskmill.StateAvailable, taskmill.StateRunning, taskmill.StateScheduled, taskmill.StateRetryable,
	taskmill.StatePending, taskmill.StateDead, taskmill.StateCanceled,
}

// totals sums the task counts of every queue.
type totals struct {
	Available, Running, Waiting, Dead int64
	Capped                            bool
}

func sumQueues(queues []taskmill.QueueInfo) totals {
	var t totals
	for _, q := range queues {
		t.Available += q.Counts[taskmill.StateAvailable]
		t.Running += q.Counts[taskmill.StateRunning]
		t.Waiting += q.Counts[taskmill.StateScheduled] + q.Counts[taskmill.StateRetryable] + q.Counts[taskmill.StatePending]
		t.Dead += q.Counts[taskmill.StateDead]
		t.Capped = t.Capped || q.Capped
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
	if err != nil {
		tasksChart.Note, attemptsChart.Note = "History is unavailable", "History is unavailable"
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
	var maintainer string
	for _, wk := range workers {
		if wk.Maintainer {
			maintainer = wk.ID
		}
	}
	t := sumQueues(queues)
	s.render(w, r, http.StatusOK, "overview.html", page{Title: "Overview", Nav: "overview", Dead: t.Dead, Data: struct {
		Queues          []taskmill.QueueInfo
		States          []taskmill.State
		Totals          totals
		Workers         int
		Maintainer      string
		Schedules       int
		NextRun         time.Time
		Settings        *taskmill.Settings
		RefreshedAt     time.Time
		Ranges          []chartRange
		Range           string
		Tasks, Attempts chart
	}{
		queues, queueStates, t, len(workers), maintainer, len(schedules), nextRun, settings, refreshed,
		chartRanges, span.Name, tasksChart, attemptsChart,
	}})
}

func (s *server) tasks(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	f := taskFilter(q)
	params := &taskmill.ListParams{
		Queues: f.Queues, States: f.States, Kinds: f.Kinds, GroupKeys: f.GroupKeys, Cursor: q.Get("cursor"), Limit: pageSize,
	}
	res, err := s.client.List(r.Context(), params)
	if errors.Is(err, taskmill.ErrInvalidArgument) {
		s.render(w, r, http.StatusBadRequest, "error.html",
			page{Title: "Invalid filter", Data: problem{err.Error(), "/tasks", "Clear the filters"}})
		return
	}
	if err != nil {
		s.fail(w, r, err)
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
	newest := maps.Clone(q)
	for _, key := range []string{"cursor", "done", "count"} {
		newest.Del(key)
	}
	next := ""
	if res.Next != "" {
		v := maps.Clone(newest)
		v.Set("cursor", res.Next)
		next = s.path("/tasks?" + v.Encode())
	}
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
	filtered := len(f.States)+len(f.Queues)+len(f.Kinds)+len(f.GroupKeys) > 0
	var bulk *bulkOffers
	if filtered && !s.readOnly {
		bulk = s.bulkOffers(r, f)
	}
	s.render(w, r, http.StatusOK, "tasks.html", page{Title: title, Nav: nav, Dead: sumQueues(known).Dead, Data: struct {
		Tasks                         []taskmill.TaskInfo
		States, Queues, Kinds, Groups filter
		Filtered, Paged               bool
		Newest, Next                  string
		Bulk                          *bulkOffers
	}{
		res.Tasks,
		newFilter("state", "State", "states", stateNames, states),
		newFilter("queue", "Queue", "queues", names, f.Queues).searchable(),
		newFilter("kind", "Kind", "kinds", offered.kinds, f.Kinds).searchable(),
		groupFilter,
		filtered, params.Cursor != "",
		s.path("/tasks?" + newest.Encode()), next,
		bulk,
	}})
}

// taskFilter reads the task list's filters from a query.
func taskFilter(q url.Values) taskmill.TaskFilter {
	f := taskmill.TaskFilter{Queues: selected(q["queue"]), Kinds: selected(q["kind"]), GroupKeys: selected(q["group"])}
	for _, st := range selected(q["state"]) {
		f.States = append(f.States, taskmill.State(st))
	}
	return f
}

// filterQuery is the query of a task filter, for links and redirects.
func filterQuery(f taskmill.TaskFilter) url.Values {
	v := url.Values{"queue": f.Queues, "kind": f.Kinds, "group": f.GroupKeys}
	for _, st := range f.States {
		v.Add("state", string(st))
	}
	return v
}

// bulkOffers are the actions on every task that the filters select, each
// with how many tasks it changes; nil for an action that changes none.
type bulkOffers struct {
	Retry, Cancel, Delete *bulkOffer
}

type bulkOffer struct {
	Path  string // where its form posts
	Count string // how many tasks it changes, such as "1,234" or "10,000+"
	Tasks string // the same with its noun, such as "1 task"
}

// The states that each bulk action changes: those its action on one task
// allows.
var (
	retryStates  = []taskmill.State{taskmill.StateDead, taskmill.StateCanceled}
	cancelStates = []taskmill.State{
		taskmill.StateScheduled, taskmill.StatePending, taskmill.StateAvailable, taskmill.StateRunning, taskmill.StateRetryable,
	}
	deleteStates = slices.DeleteFunc(slices.Clone(listedStates), func(st taskmill.State) bool { return st == taskmill.StateRunning })
)

// bulkOffers counts the tasks that each bulk action would change. The
// actions are not worth failing the page for: without their counts, they
// are not offered.
func (s *server) bulkOffers(r *http.Request, f taskmill.TaskFilter) *bulkOffers {
	ctx := r.Context()
	counts, err := s.client.Count(ctx, &f)
	if err != nil {
		if ctx.Err() == nil {
			s.logger.LogAttrs(ctx, slog.LevelWarn, "count tasks failed", slog.Any(logKeyError, err))
		}
		return nil
	}
	query := filterQuery(f).Encode()
	offer := func(action string, states []taskmill.State) *bulkOffer {
		var n int64
		capped := false
		for _, st := range states {
			n += counts[st]
			capped = capped || counts[st] >= countCap
		}
		if n == 0 {
			return nil
		}
		a := &bulkOffer{Path: s.path("/tasks/bulk/" + action + "?" + query), Count: number(n)}
		if capped {
			a.Count += "+"
		}
		a.Tasks = a.Count + " tasks"
		if a.Count == "1" {
			a.Tasks = "1 task"
		}
		return a
	}
	b := &bulkOffers{offer("retry", retryStates), offer("cancel", cancelStates), offer("delete", deleteStates)}
	if *b == (bulkOffers{}) {
		return nil
	}
	return b
}

// groupChoices is how many FIFO groups a filter lists; it searches the rest.
const groupChoices = 100

// choices are the kinds and FIFO groups that the task filters offer: the
// kinds that workers handle or schedules enqueue, and the first groups with
// unfinished tasks.
type choices struct {
	kinds, groups []string
	moreGroups    bool // there are more groups than listed
}

// choiceCache keeps the filter choices for choicesTTL. They change rarely,
// and every open task list would read them again with each live update.
type choiceCache struct {
	mu   sync.Mutex
	read time.Time
	last choices
}

const choicesTTL = 30 * time.Second

// filterChoices returns the filter choices, read at most choicesTTL ago.
// Choices read with an error are not kept, so the next request tries again.
func (s *server) filterChoices(r *http.Request) choices {
	s.choices.mu.Lock()
	if !s.choices.read.IsZero() && time.Since(s.choices.read) < choicesTTL {
		c := s.choices.last
		s.choices.mu.Unlock()
		return c
	}
	s.choices.mu.Unlock()
	c, complete := s.readChoices(r)
	if complete {
		s.choices.mu.Lock()
		s.choices.read, s.choices.last = time.Now(), c
		s.choices.mu.Unlock()
	}
	return c
}

// readChoices reads the filter choices, and reports whether every read
// succeeded. The filters are not worth failing the page for: without their
// choices, they offer what is chosen.
func (s *server) readChoices(r *http.Request) (c choices, complete bool) {
	ctx := r.Context()
	complete = true
	warn := func(what string, err error) {
		complete = false
		if ctx.Err() == nil {
			s.logger.LogAttrs(ctx, slog.LevelWarn, "read filter choices failed",
				slog.String(logKeyTarget, what), slog.Any(logKeyError, err))
		}
	}
	if workers, err := s.client.Workers(ctx); err != nil {
		warn("workers", err)
	} else {
		for _, w := range workers {
			c.kinds = append(c.kinds, w.Kinds...)
		}
	}
	if schedules, err := s.client.Schedules(ctx); err != nil {
		warn("schedules", err)
	} else {
		for _, sc := range schedules {
			c.kinds = append(c.kinds, sc.Kind)
		}
	}
	slices.Sort(c.kinds)
	c.kinds = slices.Compact(c.kinds)
	found, err := s.client.Groups(ctx, &taskmill.GroupParams{Limit: groupChoices})
	if err != nil {
		warn("groups", err)
	}
	for _, g := range found {
		c.groups = append(c.groups, g.Key)
	}
	slices.Sort(c.groups)
	c.groups, c.moreGroups = slices.Compact(c.groups), len(found) == groupChoices
	return c, complete
}

// groupChoicesJSON lists the keys of the FIFO groups that match a search, for
// the group filter when it has more groups than it lists.
func (s *server) groupChoicesJSON(w http.ResponseWriter, r *http.Request) {
	found, err := s.client.Groups(r.Context(), &taskmill.GroupParams{Search: r.URL.Query().Get("search"), Limit: groupChoices})
	if err != nil {
		if r.Context().Err() == nil {
			s.logger.LogAttrs(r.Context(), slog.LevelWarn, "search groups failed", slog.Any(logKeyError, err))
		}
		http.Error(w, "taskmill: search failed", http.StatusServiceUnavailable)
		return
	}
	keys := make([]string, 0, len(found))
	for _, g := range found {
		keys = append(keys, g.Key)
	}
	slices.Sort(keys)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(slices.Compact(keys)) // the client may be gone; there is no one to tell
}

// filter is a multiple choice filter of the task list.
type filter struct {
	Name    string // the query parameter
	Title   string // what it filters by
	Plural  string // what it chooses from
	Label   string // what the trigger shows: all, the one choice, or how many
	Empty   bool   // nothing is chosen
	Search  bool   // its menu has a search box, which can also add any value
	Source  string // where the search finds choices the menu does not list
	Options []option
}

func (f filter) searchable() filter {
	f.Search = true
	return f
}

type option struct {
	Value    string
	Selected bool
}

// newFilter lists the chosen values that are not among all options first,
// such as a queue that no longer has tasks or a value typed in the search,
// then all options.
func newFilter(name, title, plural string, all, chosen []string) filter {
	f := filter{Name: name, Title: title, Plural: plural, Empty: len(chosen) == 0}
	for _, v := range chosen {
		if !slices.Contains(all, v) {
			f.Options = append(f.Options, option{v, true})
		}
	}
	for _, v := range all {
		f.Options = append(f.Options, option{v, slices.Contains(chosen, v)})
	}
	switch len(chosen) {
	case 0:
		f.Label = "All " + plural
	case 1:
		f.Label = chosen[0]
	default:
		f.Label = strconv.Itoa(len(chosen)) + " " + plural
	}
	return f
}

// selected returns the non-empty values of a repeated query parameter,
// without duplicates, up to maxChoices of them.
func selected(values []string) []string {
	var out []string
	for _, v := range values[:min(len(values), maxChoices)] {
		if v != "" && !slices.Contains(out, v) {
			out = append(out, v)
		}
	}
	return out
}

// group describes a task's place in its FIFO group.
type group struct {
	Waiting int  // tasks waiting behind a dead task, up to groupPeek
	More    bool // more than groupPeek wait
	Head    *taskmill.TaskInfo
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
		s.notFound(w, r)
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
		page, err := s.client.List(r.Context(), &taskmill.ListParams{
			Queues: []string{t.Queue}, GroupKeys: []string{t.GroupKey}, States: []taskmill.State{taskmill.StatePending}, Limit: groupPeek,
		})
		if err != nil {
			return g, err
		}
		g.Waiting, g.More = len(page.Tasks), page.Next != ""
	case taskmill.StatePending:
		page, err := s.client.List(r.Context(), &taskmill.ListParams{
			Queues: []string{t.Queue}, GroupKeys: []string{t.GroupKey}, Limit: 1, States: []taskmill.State{
				taskmill.StateScheduled, taskmill.StateAvailable, taskmill.StateRunning, taskmill.StateRetryable, taskmill.StateDead,
			},
		})
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

// deadCount returns the dead tasks of every queue, or 0 if they cannot be
// read; it only feeds the navigation.
func (s *server) deadCount(r *http.Request) int64 {
	queues, err := s.client.Queues(r.Context())
	if err != nil {
		return 0
	}
	return sumQueues(queues).Dead
}

// notices are the messages shown after an action, by the code in the
// redirect's "done" parameter. Only these fixed texts are shown, so a link
// cannot make the interface display text of its choosing.
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
	"retry-more":      "{tasks} retried so far. Retry again to go on with the rest.",
	"cancel-all":      "{tasks} canceled. Running tasks stop when their handlers return.",
	"cancel-more":     "{tasks} canceled so far. Cancel again to go on with the rest.",
	"delete-all":      "{tasks} deleted.",
	"delete-more":     "{tasks} deleted so far. Delete again to go on with the rest.",
}

// notice returns the notice that a query asks for, with the number of tasks
// in its "count" parameter for a bulk action.
func notice(q url.Values) string {
	text := notices[q.Get("done")]
	if !strings.Contains(text, "{tasks}") {
		return text
	}
	n, err := strconv.ParseInt(q.Get("count"), 10, 64)
	if err != nil || n < 0 {
		return ""
	}
	tasks := number(n) + " tasks"
	if n == 1 {
		tasks = "1 task"
	}
	return strings.Replace(text, "{tasks}", tasks, 1)
}

func (s *server) taskAction(w http.ResponseWriter, r *http.Request) {
	if s.refuseIfReadOnly(w) {
		return
	}
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		s.notFound(w, r)
		return
	}
	ctx := r.Context()
	target := "/tasks/" + strconv.FormatInt(id, 10)
	var notice string
	switch r.PathValue("action") {
	case "retry":
		err, notice = s.client.Retry(ctx, id), "retry"
	case "cancel":
		err, notice = s.client.Cancel(ctx, id), "cancel"
	case "delete":
		err, notice, target = s.client.Delete(ctx, id), "delete", "/tasks"
	default:
		s.notFound(w, r)
		return
	}
	s.finishAction(w, r, err, target, notice, notice+" task", strconv.FormatInt(id, 10))
}

func (s *server) queueAction(w http.ResponseWriter, r *http.Request) {
	if s.refuseIfReadOnly(w) {
		return
	}
	var err error
	var notice, action string
	name := r.PathValue("name")
	switch r.PathValue("action") {
	case "pause":
		err, notice, action = s.client.PauseQueue(r.Context(), name), "queue-pause", "pause queue"
	case "resume":
		err, notice, action = s.client.ResumeQueue(r.Context(), name), "queue-resume", "resume queue"
	default:
		s.notFound(w, r)
		return
	}
	s.finishAction(w, r, err, "/", notice, action, name)
}

func (s *server) scheduleAction(w http.ResponseWriter, r *http.Request) {
	if s.refuseIfReadOnly(w) {
		return
	}
	var err error
	var notice, action string
	name := r.PathValue("name")
	switch r.PathValue("action") {
	case "pause":
		err, notice, action = s.client.PauseSchedule(r.Context(), name), "schedule-pause", "pause schedule"
	case "resume":
		err, notice, action = s.client.ResumeSchedule(r.Context(), name), "schedule-resume", "resume schedule"
	case "run":
		err, notice, action = s.client.RunSchedule(r.Context(), name), "schedule-run", "run schedule"
	default:
		s.notFound(w, r)
		return
	}
	s.finishAction(w, r, err, "/schedules", notice, action, name)
}

// finishAction logs an action that succeeded and redirects to target with
// its notice, or explains why the action did not apply.
func (s *server) finishAction(w http.ResponseWriter, r *http.Request, err error, target, notice, action, name string) {
	if err != nil {
		s.actionFailed(w, r, err, target)
		return
	}
	s.logger.LogAttrs(r.Context(), slog.LevelInfo, "web interface action",
		slog.String(logKeyAction, action), slog.String(logKeyTarget, name))
	http.Redirect(w, r, s.path(target+"?done="+notice), http.StatusSeeOther)
}

// actionFailed explains why an action did not apply.
func (s *server) actionFailed(w http.ResponseWriter, r *http.Request, err error, back string) {
	switch {
	case errors.Is(err, taskmill.ErrNotFound):
		s.notFound(w, r)
	case errors.Is(err, taskmill.ErrInvalidState), errors.Is(err, taskmill.ErrInvalidArgument):
		s.render(w, r, http.StatusConflict, "error.html", page{Title: "Not possible", Data: problem{err.Error(), back, "Go back"}})
	default:
		s.fail(w, r, err)
	}
}

// bulkAction retries, cancels, or deletes every task that the filters in
// the query select. It works for bulkBudget at most, so that it answers
// well within the request's deadline; the notice then says that more
// remain, and the page still offers the action for them.
func (s *server) bulkAction(w http.ResponseWriter, r *http.Request) {
	if s.refuseIfReadOnly(w) {
		return
	}
	var do func(context.Context, *taskmill.TaskFilter) (int64, error)
	action := r.PathValue("action")
	switch action {
	case "retry":
		do = s.client.RetryMatching
	case "cancel":
		do = s.client.CancelMatching
	case "delete":
		do = s.client.DeleteMatching
	default:
		s.notFound(w, r)
		return
	}
	f := taskFilter(r.URL.Query())
	query := filterQuery(f)
	ctx, cancel := context.WithTimeoutCause(r.Context(), bulkBudget, errBudget)
	defer cancel()
	n, err := do(ctx, &f)
	more := err != nil && errors.Is(context.Cause(ctx), errBudget)
	if err == nil || more || n > 0 {
		s.logger.LogAttrs(r.Context(), slog.LevelInfo, "web interface action",
			slog.String(logKeyAction, action+" tasks"), slog.String(logKeyTarget, query.Encode()), slog.Int64(logKeyTasks, n))
	}
	if err != nil && !more {
		s.actionFailed(w, r, err, "/tasks?"+query.Encode())
		return
	}
	done := action + "-all"
	if more {
		done = action + "-more"
	}
	query.Set("done", done)
	query.Set("count", strconv.FormatInt(n, 10))
	http.Redirect(w, r, s.path("/tasks?"+query.Encode()), http.StatusSeeOther)
}

func (s *server) refuseIfReadOnly(w http.ResponseWriter) bool {
	if !s.readOnly {
		return false
	}
	http.Error(w, "this interface is read-only", http.StatusForbidden)
	return true
}

// problem is what the error page shows: what went wrong, and where to go.
type problem struct {
	Message, Back, BackLabel string
}

func (s *server) notFound(w http.ResponseWriter, r *http.Request) {
	s.render(w, r, http.StatusNotFound, "error.html", page{Title: "Not found", Data: problem{
		"Nothing exists at this address. The task may have been deleted.", "/tasks", "Back to the tasks",
	}})
}

// fail answers a request whose reads or action failed. A request whose
// client went away gets no answer, and is not an error of the interface.
func (s *server) fail(w http.ResponseWriter, r *http.Request, err error) {
	ctx := r.Context()
	if errors.Is(context.Cause(ctx), errSlow) {
		s.logger.LogAttrs(ctx, slog.LevelWarn, "web interface request timed out",
			slog.String(logKeyPath, r.URL.Path), slog.Any(logKeyError, err))
		again, label := r.URL.RequestURI(), "Try again"
		if r.Method != http.MethodGet {
			again, label = "/", "Back to the overview"
		}
		w.Header().Set("Retry-After", "5")
		s.render(w, r, http.StatusServiceUnavailable, "error.html", page{Title: "Taking too long", Data: problem{
			"The database did not answer in time. Try again in a moment.", again, label,
		}})
		return
	}
	if ctx.Err() != nil {
		return
	}
	s.logger.LogAttrs(ctx, slog.LevelError, "web interface request failed",
		slog.String(logKeyPath, r.URL.Path), slog.Any(logKeyError, err))
	s.render(w, r, http.StatusInternalServerError, "error.html", page{Title: "Something went wrong", Data: problem{
		"The request failed; the server log has the details.", "/", "Back to the overview",
	}})
}

// buffers and gzipWriters are reused across requests: live updates render
// pages every few seconds.
var buffers, gzipWriters sync.Pool

func getBuffer() *bytes.Buffer {
	if b, ok := buffers.Get().(*bytes.Buffer); ok {
		b.Reset()
		return b
	}
	return new(bytes.Buffer)
}

func getGzip(w io.Writer) *gzip.Writer {
	if zw, ok := gzipWriters.Get().(*gzip.Writer); ok {
		zw.Reset(w)
		return zw
	}
	zw, _ := gzip.NewWriterLevel(w, gzip.BestSpeed) // the level is valid
	return zw
}

// render executes a page into a buffer before it answers, so that a failed
// template never sends half a page. A page that the client already has, by
// its ETag, is answered with 304 Not Modified, and others are compressed when
// the client accepts it: live updates fetch the page every few seconds.
func (s *server) render(w http.ResponseWriter, r *http.Request, status int, name string, p page) {
	p.ReadOnly = s.readOnly
	p.Notice = notice(r.URL.Query())
	buf := getBuffer()
	defer buffers.Put(buf)
	if err := s.pages.ExecuteTemplate(buf, name, p); err != nil {
		s.logger.LogAttrs(r.Context(), slog.LevelError, "render page failed",
			slog.String(logKeyPage, name), slog.Any(logKeyError, err))
		http.Error(w, "taskmill: the page failed to render; the server log has the details", http.StatusInternalServerError)
		return
	}
	h := w.Header()
	h.Set("Content-Type", "text/html; charset=utf-8")
	h.Set("Vary", "Accept-Encoding")
	if status == http.StatusOK {
		sum := sha256.Sum256(buf.Bytes())
		etag := `W/"` + hex.EncodeToString(sum[:8]) + `"`
		h.Set("ETag", etag)
		if r.Header.Get("If-None-Match") == etag {
			w.WriteHeader(http.StatusNotModified)
			return
		}
	}
	body := buf.Bytes()
	if len(body) >= gzipMin && acceptsGzip(r) {
		zbuf := getBuffer()
		defer buffers.Put(zbuf)
		zw := getGzip(zbuf)
		_, _ = zw.Write(body) // writes to a buffer cannot fail
		_ = zw.Close()
		gzipWriters.Put(zw)
		body = zbuf.Bytes()
		h.Set("Content-Encoding", "gzip")
	}
	h.Set("Content-Length", strconv.Itoa(len(body)))
	w.WriteHeader(status)
	_, _ = w.Write(body) // the client may be gone; there is no one to tell
}

func acceptsGzip(r *http.Request) bool {
	for part := range strings.SplitSeq(r.Header.Get("Accept-Encoding"), ",") {
		coding, params, _ := strings.Cut(strings.TrimSpace(part), ";")
		if strings.EqualFold(strings.TrimSpace(coding), "gzip") {
			return strings.ReplaceAll(params, " ", "") != "q=0"
		}
	}
	return false
}

// deadline bounds every request, so that a slow database cannot hold the
// interface's requests, and the connections they use, for long.
func deadline(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeoutCause(r.Context(), requestTimeout, errSlow)
		defer cancel()
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// path returns an absolute path inside the interface.
func (s *server) path(p string) string {
	return s.prefix + p
}

// asset returns the path of a static file, versioned by its content so that
// browsers may cache it for good.
func (s *server) asset(name string) string {
	return s.path("/static/" + name + "?v=" + s.versions[name])
}

func (s *server) funcs() template.FuncMap {
	return template.FuncMap{
		"path":      s.path,
		"asset":     s.asset,
		"ago":       ago,
		"timestamp": timestamp,
		"json":      prettyJSON,
		"states":    func() []taskmill.State { return listedStates },
		"lastError": lastError,
		"newest":    newestFirst,
		"visible":   visible,
		"num":       number,
		"count":     count,
		"short":     shortID,
		"stale":     func(t time.Time) bool { return time.Since(t) > staleHeartbeat },
		"percent":   percent,
		"period":    period,
		"dict":      dict,
		"lower":     strings.ToLower,
		// segment escapes a name for one path segment: queue and schedule
		// names may hold /, ?, #, or %.
		"segment": url.PathEscape,
		"query": func(pairs ...string) string {
			v := url.Values{}
			for i := 0; i+1 < len(pairs); i += 2 {
				if pairs[i+1] != "" {
					v.Set(pairs[i], pairs[i+1])
				}
			}
			if len(v) == 0 {
				return ""
			}
			return "?" + v.Encode()
		},
	}
}

// staticFile is a static file, kept with its compressed form.
type staticFile struct {
	body, gzipped []byte
	contentType   string
}

// loadStatic reads every static file, compresses it once, and hashes its
// content for its versioned URL.
func loadStatic() (map[string]staticFile, map[string]string) {
	files, versions := map[string]staticFile{}, map[string]string{}
	entries, _ := assets.ReadDir("static") // the directory is embedded above
	for _, e := range entries {
		body, _ := assets.ReadFile("static/" + e.Name())
		sum := sha256.Sum256(body)
		versions[e.Name()] = hex.EncodeToString(sum[:4])
		var z bytes.Buffer
		zw, _ := gzip.NewWriterLevel(&z, gzip.BestCompression) // the level is valid
		_, _ = zw.Write(body)
		_ = zw.Close()
		f := staticFile{body: body, contentType: mime.TypeByExtension(path.Ext(e.Name()))}
		if z.Len() < len(body) {
			f.gzipped = z.Bytes()
		}
		files[e.Name()] = f
	}
	return files, versions
}

// static serves the static files. A URL with the current version of a file
// may be cached for a year; any other, such as one from a page that an older
// server rendered, must be checked again.
func (s *server) static() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		name := r.PathValue("name")
		f, ok := s.files[name]
		if !ok {
			http.NotFound(w, r)
			return
		}
		h := w.Header()
		h.Set("Content-Type", f.contentType)
		h.Set("Vary", "Accept-Encoding")
		if r.URL.Query().Get("v") == s.versions[name] {
			h.Set("Cache-Control", "public, max-age=31536000, immutable")
		} else {
			h.Set("Cache-Control", "no-cache")
		}
		body := f.body
		if f.gzipped != nil && acceptsGzip(r) {
			body = f.gzipped
			h.Set("Content-Encoding", "gzip")
		}
		h.Set("Content-Length", strconv.Itoa(len(body)))
		_, _ = w.Write(body) // the client may be gone; there is no one to tell
	})
}

// basicAuth requires the one user of auth. It compares hashes of the
// credentials in constant time, so neither their content nor their length
// leaks through timing.
func basicAuth(next http.Handler, auth BasicAuth) http.Handler {
	if auth.Username == "" || auth.Password == "" {
		panic("ui: BasicAuth needs a username and a password")
	}
	wantUser, wantPass := sha256.Sum256([]byte(auth.Username)), sha256.Sum256([]byte(auth.Password))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, pass, ok := r.BasicAuth()
		gotUser, gotPass := sha256.Sum256([]byte(user)), sha256.Sum256([]byte(pass))
		match := subtle.ConstantTimeCompare(gotUser[:], wantUser[:]) & subtle.ConstantTimeCompare(gotPass[:], wantPass[:])
		if !ok || match != 1 {
			if r.Header.Get(liveHeader) == "" {
				w.Header().Set("WWW-Authenticate", `Basic realm="taskmill", charset="UTF-8"`)
			}
			http.Error(w, "taskmill: log in to continue", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// frameAncestors returns the frame-ancestors source list of the content
// security policy.
func frameAncestors(origins []string) string {
	if len(origins) == 0 {
		return "'none'"
	}
	for _, o := range origins {
		if o == "" || strings.ContainsAny(o, " \t\r\n,;") {
			panic(fmt.Sprintf("ui: invalid FrameAncestors entry %q", o))
		}
	}
	return strings.Join(origins, " ")
}

// securityHeaders sets headers that keep the interface from being framed by
// pages not listed, sniffed, or made to load anything but its own assets.
func securityHeaders(next http.Handler, ancestors string) http.Handler {
	csp := "default-src 'none'; style-src 'self'; script-src 'self'; img-src 'self'; connect-src 'self'; " +
		"form-action 'self'; base-uri 'none'; frame-ancestors " + ancestors
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := w.Header()
		h.Set("Content-Security-Policy", csp)
		h.Set("X-Content-Type-Options", "nosniff")
		h.Set("Referrer-Policy", "same-origin")
		h.Set("Cache-Control", "no-store")
		if ancestors == "'none'" {
			h.Set("X-Frame-Options", "DENY") // for browsers without frame-ancestors
		}
		next.ServeHTTP(w, r)
	})
}

// ago formats how long ago t was, or how far ahead it is.
func ago(t time.Time) string {
	if t.IsZero() {
		return "—"
	}
	d := time.Since(t)
	suffix := " ago"
	if d < 0 {
		d, suffix = -d, " from now"
	}
	switch {
	case d < time.Second:
		return "now"
	case d < time.Minute:
		return fmt.Sprintf("%ds%s", int(d.Seconds()), suffix)
	case d < time.Hour:
		return fmt.Sprintf("%dm%s", int(d.Minutes()), suffix)
	case d < 48*time.Hour:
		return fmt.Sprintf("%dh%s", int(d.Hours()), suffix)
	default:
		return fmt.Sprintf("%dd%s", int(d.Hours()/24), suffix)
	}
}

func timestamp(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format("2006-01-02 15:04:05.000 UTC")
}

// number formats n with thousands separators.
func number(n int64) string {
	s := strconv.FormatInt(n, 10)
	if n < 0 {
		return "-" + number(-n)
	}
	for i := len(s) - 3; i > 0; i -= 3 {
		s = s[:i] + "," + s[i:]
	}
	return s
}

// count formats a statistics count, marking one that may have stopped at
// its cap.
func count(n int64, capped bool) string {
	if capped && n >= statsCap {
		return number(n) + "+"
	}
	return number(n)
}

// shortID keeps the last part of a worker ID: its random suffix tells
// workers on one host apart.
func shortID(id string) string {
	if i := strings.LastIndexByte(id, '-'); i >= 0 && i < len(id)-1 {
		return id[i+1:]
	}
	return id
}

// period formats a retention period in whole days, or hours below two days.
func period(d time.Duration) string {
	if d >= 48*time.Hour {
		return strconv.Itoa(int(d/(24*time.Hour))) + " days"
	}
	if h := int(d / time.Hour); h != 1 {
		return strconv.Itoa(h) + " hours"
	}
	return "1 hour"
}

// percent returns part of whole as a percentage, from 0 to 100.
func percent(part, whole int) int {
	if whole <= 0 {
		return 0
	}
	return min(100, max(0, part*100/whole))
}

// dict builds a map from key and value pairs, for passing several values to
// a template.
func dict(pairs ...any) map[string]any {
	m := make(map[string]any, len(pairs)/2)
	for i := 0; i+1 < len(pairs); i += 2 {
		if k, ok := pairs[i].(string); ok {
			m[k] = pairs[i+1]
		}
	}
	return m
}

// visible shows the control characters of a name, such as the separator
// that the outbox puts between a topic and a key, as Unicode control
// pictures.
func visible(s string) string {
	return strings.Map(func(r rune) rune {
		if r < 0x20 {
			return 0x2400 + r
		}
		return r
	}, s)
}

// lastError returns the most recent attempt error, or nil.
func lastError(errs []taskmill.AttemptError) *taskmill.AttemptError {
	if len(errs) == 0 {
		return nil
	}
	return &errs[len(errs)-1]
}

// newestFirst returns errs, newest first.
func newestFirst(errs []taskmill.AttemptError) []taskmill.AttemptError {
	out := slices.Clone(errs)
	slices.Reverse(out)
	return out
}

// prettyJSON indents a payload for reading. It keeps the text as stored, so
// large integers stay exact, and characters such as "<" stay as they are: the
// template escapes them for HTML.
func prettyJSON(raw json.RawMessage) string {
	var b bytes.Buffer
	if err := json.Indent(&b, raw, "", "  "); err != nil {
		return string(raw)
	}
	return b.String()
}
