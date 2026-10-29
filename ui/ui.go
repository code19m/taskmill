// Package ui serves taskmill's dashboard of queues, tasks, schedules, and
// workers, with actions to pause and resume queues and schedules, run a
// schedule now, and retry, cancel, and delete tasks, one at a time or all
// that match the task list's filters.
//
// The dashboard is rendered on the server with the standard library. Two
// small scripts, served with it, keep pages live, apply filters as they
// change, confirm actions in dialogs, and apply the chosen theme. Every page
// also works without them, and nothing needs a JavaScript build step. The
// dashboard reads only through the bounded queries of [taskmill.Client], so
// it is safe to point at a busy production database.
//
// Mount it in an application, behind the application's own authentication:
//
//	dashboard, err := ui.New(client, &ui.Config{Prefix: "/taskmill"})
//	// ...
//	mux.Handle("/taskmill/", auth(dashboard))
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
	logKeySkip   = "skipped"
)

const (
	pageSize       = 50
	groupPeek      = 100              // most waiting tasks of a group that a task page counts
	staleCheckIn   = 30 * time.Second // a worker that has not checked in this long is shown as stale
	requestTimeout = 10 * time.Second // bound on one request, so a slow database cannot hold it
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

// Config configures the dashboard. The zero value is valid.
type Config struct {
	// Prefix is the path the handler is mounted at, such as "/taskmill".
	// The handler strips it from requests and adds it to links and
	// redirects, so do not wrap the handler in http.StripPrefix.
	// Default: none, the root.
	Prefix string

	// ReadOnly hides and refuses every action that changes state.
	ReadOnly bool

	// BasicAuth, if set, requires every request to log in with HTTP basic
	// authentication. Serve the dashboard over HTTPS when it is set: basic
	// authentication sends the password with every request. Default: none;
	// the application in front of the handler authenticates.
	BasicAuth *BasicAuth

	// FrameAncestors lists the origins of the pages that may show the
	// dashboard in a frame, such as "https://admin.example.com", or "'self'"
	// for the application's own pages. Default: none; the dashboard cannot be
	// framed, which protects it from clickjacking.
	FrameAncestors []string

	// Logger receives every action taken, and the errors that the dashboard
	// shows as a server error. Default: logs are discarded.
	Logger *slog.Logger
}

// BasicAuth is the one user that may use the dashboard.
type BasicAuth struct {
	Username string
	Password string
}

// New returns the dashboard for client. cfg may be nil. It returns an
// error that wraps [taskmill.ErrInvalidArgument] if client is nil, if
// cfg.Prefix does not start with a slash, if cfg.BasicAuth has an empty
// username or password, or if an entry of cfg.FrameAncestors is empty or
// holds a space, comma, or semicolon.
func New(client *taskmill.Client, cfg *Config) (http.Handler, error) {
	if cfg == nil {
		cfg = &Config{}
	}
	ancestors, err := checkConfig(client, cfg)
	if err != nil {
		return nil, err
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
	mux.HandleFunc("GET /tasks/bulk/{action}", s.bulkPage)
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
	return securityHeaders(h, ancestors), nil
}

// checkConfig checks the configuration of the dashboard, and returns the
// frame-ancestors source list of its content security policy.
func checkConfig(client *taskmill.Client, cfg *Config) (ancestors string, err error) {
	invalid := func(format string, args ...any) error {
		return fmt.Errorf("%w: ui: %s", taskmill.ErrInvalidArgument, fmt.Sprintf(format, args...))
	}
	switch {
	case client == nil:
		return "", invalid("client is nil")
	case cfg.Prefix != "" && !strings.HasPrefix(cfg.Prefix, "/"):
		return "", invalid("Prefix %q is not a path: start it with /", cfg.Prefix)
	case cfg.BasicAuth != nil && (cfg.BasicAuth.Username == "" || cfg.BasicAuth.Password == ""):
		return "", invalid("BasicAuth needs a username and a password")
	}
	for _, o := range cfg.FrameAncestors {
		if o == "" || strings.ContainsAny(o, " \t\r\n,;") {
			return "", invalid("FrameAncestors entry %q is empty or holds a space, comma, or semicolon", o)
		}
	}
	if len(cfg.FrameAncestors) == 0 {
		return "'none'", nil
	}
	return strings.Join(cfg.FrameAncestors, " "), nil
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
	Static   bool   // the page does not update itself
	ReadOnly bool
	Notice   string
	Dead     taskmill.TaskCount // dead tasks, for the navigation; zero if unknown
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
	params := &taskmill.ListParams{Filter: f, Cursor: q.Get("cursor"), Limit: pageSize}
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
	var bulk *bulkLinks
	if filtered && !s.readOnly {
		bulk = s.bulkLinks(f)
	}
	s.render(w, r, http.StatusOK, "tasks.html", page{Title: title, Nav: nav, Dead: sumQueues(known).Dead, Data: struct {
		Tasks                         []taskmill.TaskInfo
		States, Queues, Kinds, Groups filter
		Filtered, Paged               bool
		Newest, Next                  string
		Bulk                          *bulkLinks
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

// queryNumber reads a count of tasks from the query; 0 if it is not one.
func queryNumber(q url.Values, key string) int64 {
	n, err := strconv.ParseInt(q.Get(key), 10, 64)
	if err != nil || n < 0 {
		return 0
	}
	return n
}

// tasks says how many tasks n is: "1 task", "1,234 tasks", or "10,000+
// tasks" for a count that stopped at its cap.
func tasks(n taskmill.TaskCount) string {
	if n == (taskmill.TaskCount{N: 1}) {
		return "1 task"
	}
	return count(n) + " tasks"
}

// bulkLinks lead to the pages of the bulk actions on the tasks that the
// filters select; empty for an action that no selected state allows.
type bulkLinks struct {
	Retry, Cancel, Delete string
}

func (s *server) bulkLinks(f taskmill.TaskFilter) *bulkLinks {
	states := f.States
	if len(states) == 0 {
		states = listedStates
	}
	query := filterQuery(f).Encode()
	link := func(name string, allows func(taskmill.State) bool) string {
		if !slices.ContainsFunc(states, allows) {
			return ""
		}
		return s.path(bulkActions[name].path + "?" + query)
	}
	return &bulkLinks{
		link("retry", taskmill.State.CanRetry), link("cancel", taskmill.State.CanCancel), link("delete", taskmill.State.CanDelete),
	}
}

// bulkAction is an action on every task that the task list's filters select.
type bulkAction struct {
	Verb    string // what its button says
	Icon    string
	Danger  bool
	Explain string // what it does
	None    string // why it may find no task
	path    string
	do      func(*taskmill.Client, context.Context, *taskmill.BulkParams) (taskmill.BulkResult, error)
	count   func(*taskmill.TaskCounts) taskmill.TaskCount
}

var bulkActions = map[string]bulkAction{
	"retry": {
		Verb: "Retry", Icon: "retry",
		Explain: "They run again with a fresh set of attempts. A dead task that halts its FIFO group resumes the group.",
		None:    "Retry applies to dead and canceled tasks.",
		path:    "/tasks/bulk/retry",
		do:      (*taskmill.Client).RetryMatching,
		count:   func(c *taskmill.TaskCounts) taskmill.TaskCount { return c.Retry },
	},
	"cancel": {
		Verb: "Cancel", Icon: "cancel",
		Explain: "Waiting tasks will not run. Running tasks stop when their handlers return.",
		None:    "Cancel applies to unfinished tasks, except running tasks already asked to stop.",
		path:    "/tasks/bulk/cancel",
		do:      (*taskmill.Client).CancelMatching,
		count:   func(c *taskmill.TaskCounts) taskmill.TaskCount { return c.Cancel },
	},
	"delete": {
		Verb: "Delete", Icon: "trash", Danger: true,
		Explain: "Running tasks are kept. This cannot be undone.",
		None:    "Delete applies to every task except running ones.",
		path:    "/tasks/bulk/delete",
		do:      (*taskmill.Client).DeleteMatching,
		count:   func(c *taskmill.TaskCounts) taskmill.TaskCount { return c.Delete },
	},
}

// filterRow is one filter that a bulk action's page shows.
type filterRow struct {
	Title  string
	Values []string
	Mono   bool
}

// bulkPage shows what a bulk action would change, with a button to do it:
// the confirmation, with or without the script. It counts the tasks once,
// and does not update itself.
func (s *server) bulkPage(w http.ResponseWriter, r *http.Request) {
	if s.refuseIfReadOnly(w) {
		return
	}
	a, ok := bulkActions[r.PathValue("action")]
	if !ok {
		s.notFound(w, r)
		return
	}
	f := taskFilter(r.URL.Query())
	if len(f.States)+len(f.Queues)+len(f.Kinds)+len(f.GroupKeys) == 0 {
		s.render(w, r, http.StatusBadRequest, "error.html", page{Title: "Choose filters first", Data: problem{
			"A bulk action acts on the tasks that the task list's filters select.", "/tasks", "Back to the tasks",
		}})
		return
	}
	counts, err := s.client.Count(r.Context(), &f)
	if errors.Is(err, taskmill.ErrInvalidArgument) {
		s.render(w, r, http.StatusBadRequest, "error.html",
			page{Title: "Invalid filter", Data: problem{err.Error(), "/tasks", "Clear the filters"}})
		return
	}
	if err != nil {
		s.fail(w, r, err)
		return
	}
	var states []string
	for _, st := range f.States {
		states = append(states, string(st))
	}
	var rows []filterRow
	for _, row := range []filterRow{
		{"State", states, false}, {"Queue", f.Queues, false}, {"Kind", f.Kinds, true}, {"FIFO group", f.GroupKeys, true},
	} {
		if len(row.Values) > 0 {
			rows = append(rows, row)
		}
	}
	n := a.count(counts)
	// The form carries where a run that stopped early goes on.
	action := filterQuery(f)
	if cursor := r.URL.Query().Get("cursor"); cursor != "" {
		action.Set("cursor", cursor)
	}
	s.render(w, r, http.StatusOK, "bulk.html", page{Title: a.Verb + " tasks", Nav: "tasks", Static: true, Dead: s.deadCount(r), Data: struct {
		bulkAction
		Filters    []filterRow
		N          int64
		Tasks      string
		Path, Back string
	}{a, rows, n.N, tasks(n), s.path(a.path + "?" + action.Encode()), s.path("/tasks?" + filterQuery(f).Encode())}})
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
		found = &taskmill.GroupsResult{}
	}
	for _, g := range found.Groups {
		c.groups = append(c.groups, g.Key)
	}
	slices.Sort(c.groups)
	c.groups, c.moreGroups = slices.Compact(c.groups), found.Next != ""
	return c, complete
}

// groupChoicesJSON lists the keys of the FIFO groups that match a search, for
// the group filter when it has more groups than it lists.
func (s *server) groupChoicesJSON(w http.ResponseWriter, r *http.Request) {
	found, err := s.client.Groups(r.Context(), &taskmill.GroupParams{Search: r.URL.Query().Get("search"), Limit: groupChoices})
	if errors.Is(err, taskmill.ErrInvalidArgument) {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err != nil {
		if r.Context().Err() == nil {
			s.logger.LogAttrs(r.Context(), slog.LevelWarn, "search groups failed", slog.Any(logKeyError, err))
		}
		http.Error(w, "taskmill: search failed", http.StatusServiceUnavailable)
		return
	}
	keys := make([]string, 0, len(found.Groups))
	for _, g := range found.Groups {
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
		page, err := s.client.List(r.Context(), &taskmill.ListParams{Filter: taskmill.TaskFilter{
			Queues: []string{t.Queue}, GroupKeys: []string{t.GroupKey}, States: []taskmill.State{taskmill.StatePending},
		}, Limit: groupPeek})
		if err != nil {
			return g, err
		}
		g.Waiting, g.More = len(page.Tasks), page.Next != ""
	case taskmill.StatePending:
		page, err := s.client.List(r.Context(), &taskmill.ListParams{Filter: taskmill.TaskFilter{
			Queues: []string{t.Queue}, GroupKeys: []string{t.GroupKey}, States: []taskmill.State{
				taskmill.StateScheduled, taskmill.StateAvailable, taskmill.StateRunning, taskmill.StateRetryable, taskmill.StateDead,
			},
		}, Limit: 1})
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
	"cancel-all":      "{tasks} canceled. Running tasks stop when their handlers return.",
	"cancel-more":     "{tasks} canceled so far; more remain.",
	"cancel-skipped":  "{tasks} canceled; other work held {skipped}. Try again for those.",
	"delete-all":      "{tasks} deleted.",
	"delete-more":     "{tasks} deleted so far; more remain.",
	"delete-skipped":  "{tasks} deleted; other work held {skipped}. Try again for those.",
}

// notice returns the notice that a query asks for, with the numbers of
// tasks in its "count" and "skipped" parameters for a bulk action.
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
	return strings.Replace(text, "{skipped}", tasks(taskmill.TaskCount{N: queryNumber(q, "skipped")}), 1)
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
	s.logger.LogAttrs(r.Context(), slog.LevelInfo, "dashboard action",
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
// the query select. It works for half the time left before the request's
// deadline, so that it answers in time. If more tasks remain, it leads back
// to its page, whose form goes on where it stopped. If it left tasks that
// other work held, it leads back to its page, which offers to try those
// again. Otherwise it leads to the task list. Only its page carries where a
// run goes on: the task list always shows every task that the filters
// select.
func (s *server) bulkAction(w http.ResponseWriter, r *http.Request) {
	if s.refuseIfReadOnly(w) {
		return
	}
	name := r.PathValue("action")
	a, ok := bulkActions[name]
	if !ok {
		s.notFound(w, r)
		return
	}
	f := taskFilter(r.URL.Query())
	deadline, _ := r.Context().Deadline() // the deadline handler sets one
	ctx, cancel := context.WithDeadlineCause(r.Context(), time.Now().Add(time.Until(deadline)/2), errBudget)
	defer cancel()
	res, err := a.do(s.client, ctx, &taskmill.BulkParams{Filter: f, Cursor: r.URL.Query().Get("cursor")})
	more := err != nil && errors.Is(context.Cause(ctx), errBudget)
	if err == nil || more || res.Changed > 0 {
		s.logger.LogAttrs(r.Context(), slog.LevelInfo, "dashboard action",
			slog.String(logKeyAction, name+" tasks"), slog.String(logKeyTarget, filterQuery(f).Encode()),
			slog.Int64(logKeyTasks, res.Changed), slog.Int64(logKeySkip, res.Skipped))
	}
	if err != nil && !more {
		s.actionFailed(w, r, err, "/tasks?"+filterQuery(f).Encode())
		return
	}
	query := filterQuery(f)
	query.Set("count", strconv.FormatInt(res.Changed, 10))
	target := "/tasks?"
	switch {
	case more:
		target = a.path + "?"
		query.Set("done", name+"-more")
		query.Set("cursor", res.Next)
	case res.Skipped > 0:
		target = a.path + "?"
		query.Set("done", name+"-skipped")
		query.Set("skipped", strconv.FormatInt(res.Skipped, 10))
	default:
		query.Set("done", name+"-all")
	}
	http.Redirect(w, r, s.path(target+query.Encode()), http.StatusSeeOther)
}

func (s *server) refuseIfReadOnly(w http.ResponseWriter) bool {
	if !s.readOnly {
		return false
	}
	http.Error(w, "this dashboard is read-only", http.StatusForbidden)
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
// client went away gets no answer, and is not an error of the dashboard.
func (s *server) fail(w http.ResponseWriter, r *http.Request, err error) {
	ctx := r.Context()
	if errors.Is(context.Cause(ctx), errSlow) {
		s.logger.LogAttrs(ctx, slog.LevelWarn, "dashboard request timed out",
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
	s.logger.LogAttrs(ctx, slog.LevelWarn, "dashboard request failed",
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
// dashboard's requests, and the connections they use, for long.
func deadline(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeoutCause(r.Context(), requestTimeout, errSlow)
		defer cancel()
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// path returns an absolute path inside the dashboard.
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
		"stale":     func(t time.Time) bool { return time.Since(t) > staleCheckIn },
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

// securityHeaders sets headers that keep the dashboard from being framed by
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

// count formats a count of tasks, marking one that stopped at its cap.
func count(n taskmill.TaskCount) string {
	if n.Capped {
		return number(n.N) + "+"
	}
	return number(n.N)
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
