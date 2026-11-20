// Package ui serves taskmill's dashboard as an HTTP handler.
//
// The dashboard shows the queues, tasks, schedules, and workers of an
// installation. Its actions pause and resume queues and schedules, run a
// schedule now, and retry, cancel, and delete tasks, one at a time or all
// that match the task list's filters.
//
// [New] returns the handler, and [Config] sets its path, its login, and what
// it allows. Mount it in an application, behind the application's own
// authentication:
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
// Every action is a POST request, and a GET request never changes anything,
// so an application in front of the dashboard can allow or log the actions
// by their method. Actions are protected against cross-site request forgery
// with [http.CrossOriginProtection], and [Config.ReadOnly] disables them
// entirely.
//
// The dashboard is rendered on the server with the standard library. Two
// small scripts, served with it, keep pages live, show another page or the
// result of an action without loading the page anew, apply filters as they
// change, show times in the reader's time zone, confirm actions in dialogs,
// and apply the chosen theme. Every page also works without them, with its
// times in UTC, and nothing needs a JavaScript build step. The
// dashboard reads only through the bounded queries of [taskmill.Client], so
// it is safe to point at a busy production database.
//
// The [dashboard guide] shows how to run and protect it. The [manual] of the
// command lists the dashboard's routes and status codes, for the allow-list
// of a proxy.
//
// [dashboard guide]: https://github.com/code19m/taskmill/blob/main/docs/guides/dashboard.md
// [manual]: https://github.com/code19m/taskmill/blob/main/docs/reference/taskmill-ui.md
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
	"unicode"
	"unicode/utf8"

	"github.com/jackc/pgx/v5/pgconn"

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
	pageSize       = 50               // tasks that the task list shows at first, and that "Load more" adds
	maxShown       = 250              // most tasks that the task list shows on one page, which it reads at every update
	requestTimeout = 10 * time.Second // bound on one request, so a slow database cannot hold it
	maxChoices     = 100              // most values of one filter that a request may choose
	gzipMin        = 1024             // smallest response worth compressing

	// staleCounts is how old the counts of the overview may be before the
	// page warns about their age. The maintainer counts every 10 seconds:
	// after a minute, no worker runs as the maintainer, or it cannot count.
	staleCounts = time.Minute
)

// liveHeader marks the requests of the page's script: its live updates, the
// pages that it shows in place, and the actions that it sends. They are never
// answered with a login prompt: the page shows that the session ended, or the
// browser loads the page and asks then.
const liveHeader = "X-Taskmill-Live"

// errBudget is why a bulk action stopped before it was done.
var errBudget = errors.New("the bulk action used its time")

// Config configures the dashboard. The zero value is valid. [New] reports a
// field outside its limits as an error that wraps
// [taskmill.ErrInvalidArgument].
type Config struct {
	// Prefix is the path the handler is mounted at, such as "/taskmill". It
	// must start with a slash. The handler strips it from requests and adds
	// it to links and redirects, so do not wrap the handler in
	// http.StripPrefix. Default: none, the root.
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
	// for the application's own pages. An entry must not be empty, or hold a
	// space, a comma, or a semicolon. Default: none; the dashboard cannot be
	// framed, which protects it from clickjacking.
	FrameAncestors []string

	// Logger receives every action taken, and the errors that the dashboard
	// shows as a server error. Default: logs are discarded.
	Logger *slog.Logger
}

// BasicAuth is the one user that may use the dashboard. Set it in
// [Config.BasicAuth].
type BasicAuth struct {
	// Username is the user's name. Required.
	Username string

	// Password is the user's password. Required.
	Password string
}

// New returns the dashboard for client. cfg may be nil. It returns an
// error that wraps [taskmill.ErrInvalidArgument] if client is nil, if
// cfg.Prefix does not start with a slash, if cfg.BasicAuth has an empty
// username or password, or if an entry of cfg.FrameAncestors is empty or
// holds a space, comma, or semicolon.
//
// The handler has nothing to close: it starts no goroutine, and it reaches
// the database only through client. That is why New returns a plain
// [http.Handler], which the caller mounts, and may wrap in middleware of its
// own.
//
// The handler gives every request 10 seconds, or the time until the deadline
// of the request's context if the application set an earlier one, and then
// answers 503 with a page that says so. Serve it from an [http.Server] whose
// WriteTimeout is longer than that time, or zero: a shorter one closes the
// connection before the answer.
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
	mux.HandleFunc("POST /queues/{action}", s.queueAction)
	mux.HandleFunc("POST /schedules/{action}", s.scheduleAction)
	mux.HandleFunc("GET /", s.notFound)

	var h http.Handler = mux
	if s.prefix != "" {
		h = underPrefix(s.prefix, h)
	}
	h = deadline(http.NewCrossOriginProtection().Handler(h))
	if cfg.BasicAuth != nil {
		h = basicAuth(h, *cfg.BasicAuth)
	}
	return securityHeaders(h, ancestors), nil
}

// underPrefix serves h for the paths under prefix, with the prefix stripped,
// as http.StripPrefix does, and answers itself the requests that h would send
// out of the prefix. The prefix alone, and a path that is not clean, such as
// one with a double slash, redirect to the clean path under the prefix: h's
// mux would redirect them to that path at the host's root. A path that only
// starts like the prefix, such as /taskmillx for /taskmill, is not found.
func underPrefix(prefix string, h http.Handler) http.Handler {
	escaped := (&url.URL{Path: prefix}).EscapedPath()
	strip := http.StripPrefix(prefix, h)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// The escaped path decides, as in the mux: an escaped slash or dot
		// is part of a name, not of the path's shape.
		rest, ok := strings.CutPrefix(r.URL.EscapedPath(), escaped)
		if !ok || rest != "" && rest[0] != '/' {
			http.NotFound(w, r)
			return
		}
		clean := path.Clean("/" + rest)
		if strings.HasSuffix(rest, "/") && clean != "/" {
			clean += "/" // which path.Clean removes
		}
		if clean != rest {
			target := escaped + clean
			if r.URL.RawQuery != "" {
				target += "?" + r.URL.RawQuery
			}
			// The status keeps the request's method, as the mux's does.
			http.Redirect(w, r, target, http.StatusTemporaryRedirect) //nolint:gosec // the request adds only a path and a query after the prefix
			return
		}
		strip.ServeHTTP(w, r)
	})
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
	Retry    bool   // a status page of a cause that may pass: the script tries its address again
	ReadOnly bool
	Notice   string
	Dead     taskmill.TaskCount // dead tasks, for the navigation; zero if unknown
	Now      time.Time          // when the page was rendered: its relative times count from here
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
		Explain: "They run again with a fresh set of attempts. A dead task that halts its FIFO group resumes the group. A canceled task stays canceled if another task holds its dedupe key.",
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
	Group  bool // the values are FIFO group keys
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
	f := taskFilter(r.URL.Query())
	if len(f.States)+len(f.Queues)+len(f.Kinds)+len(f.GroupKeys) == 0 {
		s.refuse(w, r, http.StatusBadRequest, "Choose filters first",
			"A bulk action acts on the tasks that the task list's filters select.", link{"/tasks", "Back to the tasks", "back"})
		return
	}
	counts, err := s.client.Count(r.Context(), &f)
	if errors.Is(err, taskmill.ErrInvalidArgument) {
		s.refuse(w, r, http.StatusBadRequest, "Invalid filter", reason(err), link{"/tasks", "Clear the filters", "clear"})
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
		{Title: "State", Values: states},
		{Title: "Queue", Values: f.Queues},
		{Title: "Kind", Values: f.Kinds, Mono: true},
		{Title: "FIFO group", Values: f.GroupKeys, Mono: true, Group: true},
	} {
		if len(row.Values) > 0 {
			rows = append(rows, row)
		}
	}
	n := a.count(counts)
	// The form carries where a run that stopped early continues.
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
	var notice, action string
	name := r.URL.Query().Get("name")
	back := "/"
	if span := rangeOf(r.URL.Query().Get("range")); span != chartRanges[0] {
		back = "/?range=" + span.Name
	}
	switch r.PathValue("action") {
	case "pause":
		err, notice, action = s.client.PauseQueue(r.Context(), name), "queue-pause", "pause queue"
	case "resume":
		err, notice, action = s.client.ResumeQueue(r.Context(), name), "queue-resume", "resume queue"
	default:
		s.notFound(w, r)
		return
	}
	s.finishAction(w, r, err, back, notice, action, name)
}

// scheduleAction pauses, resumes, or runs the schedule that the query names,
// as queueAction names its queue.
func (s *server) scheduleAction(w http.ResponseWriter, r *http.Request) {
	if s.refuseIfReadOnly(w, r) {
		return
	}
	var err error
	var notice, action string
	name := r.URL.Query().Get("name")
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
// its notice, or explains why the action did not apply. The target may have
// a query of its own.
func (s *server) finishAction(w http.ResponseWriter, r *http.Request, err error, target, notice, action, name string) {
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
	http.Redirect(w, r, s.path(target+sep+"done="+notice), http.StatusSeeOther)
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
		s.status(w, r, http.StatusNotFound, "Not found", status{
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

// bulkAction retries, cancels, or deletes every task that the filters in
// the query select. It works for half the time left before the request's
// deadline, so that it answers in time. If more tasks remain, it leads back
// to its page, whose form continues where it stopped. If it left tasks that
// other work held, it leads back to its page, which offers to try those
// again. Otherwise it leads to the task list, and says how many tasks a
// retry left because other tasks hold their dedupe keys: trying again does
// not help those. Only its page carries where a run continues: the task
// list always shows every task that the filters select.
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
	case res.Conflicts > 0:
		query.Set("done", name+"-conflicts")
		query.Set("conflicts", strconv.FormatInt(res.Conflicts, 10))
	default:
		query.Set("done", name+"-all")
	}
	http.Redirect(w, r, s.path(target+query.Encode()), http.StatusSeeOther)
}

// refuseIfReadOnly refuses an action of a read-only dashboard, and says so.
func (s *server) refuseIfReadOnly(w http.ResponseWriter, r *http.Request) bool {
	if !s.readOnly {
		return false
	}
	s.status(w, r, http.StatusForbidden, "Read-only dashboard", status{
		Glyph:   "lock",
		Text:    "This dashboard shows the tasks, and does not change them.",
		Actions: []link{{"/", "Back to the overview", "back"}},
	})
	return true
}

// status is what a status page shows under its title, in place of a page
// that the dashboard cannot show: a picture of what happened, what it means
// and what to do, in plain words, and where to go. The details of an error
// stay in the server log.
type status struct {
	Glyph   string // the picture: a name of the "glyph" template
	Text    string
	Actions []link // the most useful first
	Retry   bool   // the cause may pass: with the script, the page tries its address again
}

// link is an action of a status page or of an empty list: where it leads,
// what it says, and the icon of the sprite before it.
type link struct {
	Href, Label, Icon string
}

// status answers with a status page.
func (s *server) status(w http.ResponseWriter, r *http.Request, code int, title string, st status) {
	s.render(w, r, code, "status.html", page{Title: title, Retry: st.Retry, Data: st})
}

// refuse answers a request that the dashboard refuses, and says why: a value
// that is not valid, or an action that does not fit.
func (s *server) refuse(w http.ResponseWriter, r *http.Request, code int, title, why string, back link) {
	s.status(w, r, code, title, status{Glyph: "refused", Text: why, Actions: []link{back}})
}

func (s *server) notFound(w http.ResponseWriter, r *http.Request) {
	s.status(w, r, http.StatusNotFound, "Page not found", status{
		Glyph:   "search",
		Text:    "Nothing exists at this address.",
		Actions: []link{{"/", "Back to the overview", "back"}},
	})
}

func (s *server) taskNotFound(w http.ResponseWriter, r *http.Request, id int64) {
	s.status(w, r, http.StatusNotFound, "Task not found", status{
		Glyph:   "search",
		Text:    fmt.Sprintf("Task %d does not exist. It may have been deleted, or removed after its retention period.", id),
		Actions: []link{{"/tasks", "Back to the tasks", "back"}},
	})
}

// unreachablePage is the status page that the script shows in place of a
// page that got no answer from the dashboard: the network is down, the
// dashboard is, or a proxy in front of it answered in its place. Its action
// leads to the address that the page has then.
var unreachablePage = page{Title: "Can’t reach the dashboard", Data: status{
	Glyph:   "offline",
	Text:    "No answer came from the dashboard. The network may be down, or the dashboard may be restarting.",
	Actions: []link{{"", "Try again", "retry"}}, Retry: true,
}}

// backTo is the way back to where an action leads.
func backTo(target string) link {
	label := "Back to the overview"
	switch p, _, _ := strings.Cut(target, "?"); {
	case strings.HasPrefix(p, "/tasks/"):
		label = "Back to the task"
	case p == "/tasks":
		label = "Back to the tasks"
	case p == "/schedules":
		label = "Back to the schedules"
	}
	return link{target, label, "back"}
}

// reason words an error that the reader can act on as a sentence, without
// the name of its class: the title says that in plain words.
func reason(err error) string {
	text := err.Error()
	for _, class := range []error{taskmill.ErrInvalidArgument, taskmill.ErrInvalidState, taskmill.ErrConflict} {
		if _, after, ok := strings.Cut(text, class.Error()+": "); ok {
			text = after
		}
	}
	first, size := utf8.DecodeRuneInString(text)
	text = string(unicode.ToUpper(first)) + text[size:]
	if !strings.HasSuffix(text, ".") {
		text += "."
	}
	return text
}

// cursorHint tells the reader of a task list that the client refused, at an
// address with a cursor, where such a cursor comes from: each release makes
// its own, and so does each of a list's two orders.
const cursorHint = " A link or a bookmark from before an upgrade can hold a cursor that this list no longer takes."

// filtersHint tells the reader of a task list that ran out of time what may
// help. A list by state, kind, or FIFO group can read many tasks that it does
// not show: only some lists have an index that holds their tasks in the
// list's order.
const filtersHint = " A list with these filters can read many tasks that it does not show. " +
	"A queue and a state in the filters, or fewer filters, may help."

// unfinished tells the reader of an action that failed that it may have
// changed something before it did.
const unfinished = " The action did not finish: check the page before you try again."

// timeLimit reports whether the database ended a read at its time limit: the
// client's reads of tasks, groups, and activity set one of 5 seconds.
func timeLimit(err error) bool {
	pgErr, ok := errors.AsType[*pgconn.PgError](err)
	return ok && pgErr.Code == "57014" // query_canceled
}

// unreachable reports whether err is a failure to connect to the database:
// it is down, out of reach, or refuses the dashboard's login. Then no
// statement ran.
func unreachable(err error) bool {
	_, ok := errors.AsType[*pgconn.ConnectError](err)
	return ok
}

// fail answers a request whose reads or action failed. A request whose
// client went away gets no answer, and is not an error of the dashboard.
func (s *server) fail(w http.ResponseWriter, r *http.Request, err error) {
	s.failWith(w, r, err, "", "")
}

// failWith is fail with a hint for the reader, shown if the request, or a
// read of it, ran out of time, and for an action, the page it leads back to.
// A request runs out of time at the deadline that the dashboard sets, or at
// an earlier one that the application set in the request's context: the
// answer is the same. Only a request that was canceled is one whose client
// went away.
//
// Each cause may pass, so the page tries again by itself, with the script:
// a page that the server cannot read now, it may read in a moment. The
// script waits longer after each failure, so that a page that fails for good
// costs a request a minute. An action is never sent again: its page tries
// the page that the action was sent from.
func (s *server) failWith(w http.ResponseWriter, r *http.Request, err error, hint, back string) {
	ctx := r.Context()
	actions := []link{{r.URL.RequestURI(), "Try again", "retry"}}
	if r.URL.Path != "/" {
		actions = append(actions, backTo("/"))
	}
	failed := "The dashboard could not read what this page shows. "
	if r.Method != http.MethodGet {
		if back == "" {
			back = "/"
		}
		actions, failed, hint = []link{backTo(back)}, "", unfinished
	}
	answer := func(code int, title, glyph, text string) {
		if code == http.StatusServiceUnavailable {
			w.Header().Set("Retry-After", "5")
		}
		s.status(w, r, code, title, status{Glyph: glyph, Text: text + hint, Actions: actions, Retry: true})
	}
	if errors.Is(ctx.Err(), context.DeadlineExceeded) || timeLimit(err) {
		s.logger.LogAttrs(ctx, slog.LevelWarn, "dashboard request timed out",
			slog.String(logKeyPath, r.URL.Path), slog.Any(logKeyError, err))
		answer(http.StatusServiceUnavailable, "Taking too long", "slow", "The database did not answer in time.")
		return
	}
	if ctx.Err() != nil {
		return
	}
	s.logger.LogAttrs(ctx, slog.LevelWarn, "dashboard request failed",
		slog.String(logKeyPath, r.URL.Path), slog.Any(logKeyError, err))
	if unreachable(err) {
		answer(http.StatusServiceUnavailable, "Can’t reach the database", "database",
			"The dashboard is running, but it cannot connect to its database.")
		return
	}
	answer(http.StatusInternalServerError, "Something went wrong", "alert", failed+"The server log has the details.")
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
// the client accepts it: live updates fetch the page every few seconds. The
// page names its time of rendering, from which the script keeps its
// relative times counting. That time is no part of the ETag: a page that
// says what the client's page says is the page that the client has.
func (s *server) render(w http.ResponseWriter, r *http.Request, status int, name string, p page) {
	p.ReadOnly = s.readOnly
	p.Notice = notice(r.URL.Query())
	p.Now = time.Now()
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
		before, after, _ := bytes.Cut(buf.Bytes(), []byte(`data-now="`+instant(p.Now)+`"`))
		sum := sha256.New()
		sum.Write(before)
		sum.Write(after)
		etag := `W/"` + hex.EncodeToString(sum.Sum(nil)[:8]) + `"`
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
		ctx, cancel := context.WithTimeout(r.Context(), requestTimeout)
		defer cancel()
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// path returns an absolute path inside the dashboard.
func (s *server) path(p string) string {
	return s.prefix + p
}

// asset returns the path of a static file, versioned by its content so that
// browsers may cache it forever.
func (s *server) asset(name string) string {
	return s.path("/static/" + name + "?v=" + s.versions[name])
}

func (s *server) funcs() template.FuncMap {
	return template.FuncMap{
		"path":        s.path,
		"asset":       s.asset,
		"ago":         ago,
		"timestamp":   timestamp,
		"instant":     instant,
		"fieldTime":   fieldTime,
		"json":        prettyJSON,
		"states":      func() []taskmill.State { return listedStates },
		"lastError":   lastError,
		"newest":      newestFirst,
		"timeline":    timeline,
		"visible":     visible,
		"outbox":      outbox,
		"num":         number,
		"count":       count,
		"atLeast":     atLeast,
		"percent":     percent,
		"period":      period,
		"cronWords":   cronWords,
		"afterHost":   afterHost,
		"fit":         fit,
		"join":        strings.Join,
		"dict":        dict,
		"unreachable": func() page { return unreachablePage },
		"lower":       strings.ToLower,
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

// ago words how long before now t was, or how long after it: "8s ago", or
// "in 3m". It reads both times to the second, as a clock shows them, so
// every relative time of a page changes on the second, with the others. The
// script counts from there in the same words: see ago in app.js.
func ago(now, t time.Time) string {
	if t.IsZero() {
		return "—"
	}
	d, ahead := now.Unix()-t.Unix(), false
	if d < 0 {
		d, ahead = -d, true
	}
	n, unit := d/86400, "d"
	switch {
	case d == 0:
		return "now"
	case d < 60:
		n, unit = d, "s"
	case d < 3600:
		n, unit = d/60, "m"
	case d < 48*3600:
		n, unit = d/3600, "h"
	}
	if ahead {
		return "in " + strconv.FormatInt(n, 10) + unit
	}
	return strconv.FormatInt(n, 10) + unit + " ago"
}

// span words a length of time at the scale of a handler's run:
// milliseconds below a second, as "35ms", tenths below ten seconds, as
// "1.3s", then "13s", "3m 12s", "2h 5m", and from two days, as ago, "3d 4h".
// Each part is cut down, never rounded up, as ago does, and a whole number
// of seconds has no tenths. Below a millisecond it is "<1ms", where "0ms"
// would read as no time; no time is "0s". A gap that counts is whole
// seconds, and the script counts it in the same words: see span in app.js.
func span(d time.Duration) string {
	switch {
	case d <= 0:
		return "0s"
	case d < time.Millisecond:
		return "<1ms"
	case d < time.Second:
		return strconv.Itoa(int(d/time.Millisecond)) + "ms"
	case d < 10*time.Second:
		tenths := int(d / (100 * time.Millisecond))
		if tenths%10 == 0 {
			return strconv.Itoa(tenths/10) + "s"
		}
		return strconv.Itoa(tenths/10) + "." + strconv.Itoa(tenths%10) + "s"
	}
	s := int64(d / time.Second)
	pair := func(n int64, unit string, rest int64, sub string) string {
		if rest == 0 {
			return strconv.FormatInt(n, 10) + unit
		}
		return strconv.FormatInt(n, 10) + unit + " " + strconv.FormatInt(rest, 10) + sub
	}
	switch {
	case s < 60:
		return pair(s, "s", 0, "")
	case s < 3600:
		return pair(s/60, "m", s%60, "s")
	case s < 48*3600:
		return pair(s/3600, "h", s/60%60, "m")
	}
	return pair(s/86400, "d", s/3600%24, "h")
}

// dueSlack is how long after its creation a task may be due and still be
// due at once. A task enqueued without a due time is due at its creation to
// the microsecond, as one statement sets both. A due time that the
// application took from its own clock is a few milliseconds off, by that
// clock and the trip to the database, and one before the creation was due
// when the task was created. Within that slack, the two times are one time
// to the reader, and the wait is counted from the creation.
const dueSlack = 50 * time.Millisecond

// entry is a line of a task's timeline: a step, or the gap between two
// steps. A step is a fact, or a plan that is yet to come.
type entry struct {
	Step   string             // a step's name, such as "Started"; empty for a gap
	At     time.Time          // when the step was or is due; zero for one with no time yet
	Note   string             // a step's second line, such as "attempt 2"
	Plan   bool               // the step is yet to come
	Gap    string             // a gap's words, such as "Waited"
	Took   string             // a gap's length of time, such as "0.2s"; empty if it has none
	Work   bool               // the gap is an attempt that ran, not a wait: its line is solid, not dashed
	Since  time.Time          // for a gap that lasts until now, when it began: the script counts its length
	Behind *taskmill.TaskInfo // for a gap, the task that this one waits behind in its FIFO group
	Tone   string             // the color of a step's dot, or of a gap's line and length: "run", "ok", "err", or "warn"
}

// timeline returns the steps of task t at now, with the time between them:
// when it was created, due, started, and finished, from the times that its
// row keeps, and what is yet to come. head is the task that t waits behind
// in its FIFO group, if any.
//
// The row keeps the start of the last attempt only, and one due time, which
// a retry and a snooze both move: so the timeline shows the last attempt,
// and leaves out what the row cannot tell, such as whether a due time came
// from a retry, or how long a wait was when the due time of the wait is
// gone. A gap between two times out of order, as the clocks of two machines
// can make them, is left out too.
func timeline(now time.Time, t, head *taskmill.TaskInfo) []entry {
	s := steps{now: now, t: t, started: t.AttemptedAt}
	if t.Attempt == 0 {
		s.started = time.Time{} // a snooze or a stop gave the attempt back
	}
	switch t.State {
	case taskmill.StateScheduled:
		s.created()
	case taskmill.StatePending, taskmill.StateAvailable:
		s.waiting(head)
	case taskmill.StateRunning:
		due := s.created()
		s.gap("Waited", due, s.started, "")
		if s.start("run") {
			s.tl = append(s.tl, entry{Gap: "Running for", Took: s.lasting(s.started), Work: true, Since: s.started, Tone: "run"})
		}
	case taskmill.StateRetryable:
		s.retrying()
	case taskmill.StateCompleted:
		due := s.created()
		s.gap("Waited", due, s.started, "")
		s.start("")
		s.gap("Ran", s.started, t.FinalizedAt, "ok")
		s.step("Completed", t.FinalizedAt, "", "ok")
	case taskmill.StateDead:
		due := s.created()
		s.gap("Waited", due, s.started, "")
		s.start("")
		s.gap("Failed after", s.started, t.FinalizedAt, "err")
		s.step("Died", t.FinalizedAt, "", "err")
	case taskmill.StateCanceled:
		s.canceled()
	default:
	}
	return s.tl
}

// steps builds the timeline of a task.
type steps struct {
	now     time.Time
	t       *taskmill.TaskInfo
	started time.Time // the start of the last attempt that counts; zero if none
	tl      []entry
}

func (s *steps) step(name string, at time.Time, note, tone string) {
	s.tl = append(s.tl, entry{Step: name, At: at, Note: note, Tone: tone})
}

// plan adds a step that is yet to come until its time, or has no time yet.
func (s *steps) plan(name string, at time.Time, note string) {
	s.tl = append(s.tl, entry{Step: name, At: at, Note: note, Plan: at.IsZero() || at.After(s.now)})
}

// gap adds the time from one step to the next, unless a time is missing or
// the two are out of order. A gap that ends a start, Waited, is a wait; any
// other is the attempt that ran.
func (s *steps) gap(words string, from, to time.Time, tone string) {
	if !from.IsZero() && !to.IsZero() && !to.Before(from) {
		s.tl = append(s.tl, entry{Gap: words, Took: span(to.Sub(from)), Work: words != "Waited", Tone: tone})
	}
}

// lasting words the time from since until now, in whole seconds of the
// clock, as ago reads them.
func (s *steps) lasting(since time.Time) string {
	return span(max(0, s.now.Truncate(time.Second).Sub(since.Truncate(time.Second))))
}

// created adds the step Created and, unless the task was due at once, the
// step Due. It returns the time from which the task waited.
func (s *steps) created() time.Time {
	if s.t.RunAt.Sub(s.t.CreatedAt) <= dueSlack {
		s.step("Created", s.t.CreatedAt, "due at once", "")
		return s.t.CreatedAt
	}
	s.step("Created", s.t.CreatedAt, "", "")
	s.plan("Due", s.t.RunAt, "")
	return s.t.RunAt
}

// start adds the start of the last attempt, if one counts, and reports
// whether it did.
func (s *steps) start(tone string) bool {
	if s.started.IsZero() {
		return false
	}
	note := ""
	if s.t.Attempt > 1 {
		note = "attempt " + strconv.Itoa(s.t.Attempt)
	}
	s.step("Started", s.started, note, tone)
	return true
}

// failed returns when the last attempt failed, if its error ends the
// history: an error from its start on that is no note of a wait for a
// handler. It returns zero otherwise.
func (s *steps) failed() time.Time {
	e := lastError(s.t.Errors)
	if e == nil || s.started.IsZero() || e.At.Before(s.started) || e.Code == taskmill.CodeNoHandler {
		return time.Time{}
	}
	return e.At
}

// waiting adds the steps of a task that waits to start: once it is due, it
// waits, and in a FIFO group behind the task before it.
func (s *steps) waiting(head *taskmill.TaskInfo) {
	wait := entry{Behind: head}
	if head != nil {
		wait.Tone = "warn"
	}
	if due := s.created(); !due.After(s.now) {
		wait.Gap, wait.Took, wait.Since = "Waiting", s.lasting(due), due
	}
	if wait.Took != "" || head != nil {
		s.tl = append(s.tl, wait)
	}
	s.plan("Start", time.Time{}, "")
}

// retrying adds the steps of a task that failed and runs again. Its due
// time is the next attempt's: the last one's is gone. A history of one
// error is that of a first attempt, which waited from the task's creation.
func (s *steps) retrying() {
	s.step("Created", s.t.CreatedAt, "", "")
	if len(s.t.Errors) == 1 {
		s.gap("Waited", s.t.CreatedAt, s.started, "")
	}
	s.start("")
	s.gap("Failed after", s.started, s.failed(), "err")
	s.plan("Due again", s.t.RunAt, "attempt "+strconv.Itoa(s.t.Attempt+1))
}

// canceled adds the steps that a canceled task reached: its due time if
// that came before the cancel, as the due time of its attempt if it came
// before the start, or else of the next attempt, after the start.
func (s *steps) canceled() {
	end := s.t.FinalizedAt
	reached := !s.t.RunAt.After(end)
	first := s.started.IsZero() || !s.t.RunAt.After(s.started)
	if reached && first {
		due := s.created()
		s.gap("Waited", due, s.started, "")
	} else {
		s.step("Created", s.t.CreatedAt, "", "")
	}
	if s.start("") {
		s.gap("Failed after", s.started, s.failed(), "err")
		if reached && !first {
			s.step("Due", s.t.RunAt, "", "")
		}
	}
	note := ""
	if !reached {
		note = "was due in " + span(s.t.RunAt.Sub(end))
	}
	s.step("Canceled", end, note, "")
}

func timestamp(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format("2006-01-02 15:04:05.000 UTC")
}

// number formats n with thousands separators. It works on the digits, not
// on -n, which the smallest number does not have.
func number(n int64) string {
	sign, s := "", strconv.FormatInt(n, 10)
	if n < 0 {
		sign, s = "-", s[1:]
	}
	for i := len(s) - 3; i > 0; i -= 3 {
		s = s[:i] + "," + s[i:]
	}
	return sign + s
}

// count formats a count of tasks, marking one that stopped at its cap.
func count(n taskmill.TaskCount) string {
	if n.Capped {
		return number(n.N) + "+"
	}
	return number(n.N)
}

// atLeast is the tooltip of a count that stopped at its cap, and empty for
// any other count: it says what the plus after the number means.
func atLeast(n taskmill.TaskCount) string {
	if !n.Capped {
		return ""
	}
	return "At least " + number(n.N) + " tasks: the count stopped at its cap"
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

// outboxKey is a FIFO group key that the outbox made, in its two parts.
type outboxKey struct{ Topic, Key string }

// String says the key in words, for a tooltip and a screen reader.
func (k outboxKey) String() string { return "Outbox topic " + k.Topic + ", key " + k.Key }

// outbox returns the parts of a FIFO group key that the outbox made, and nil
// for any other key. The outbox joins a message's topic and key with the
// unit separator, which no font draws (groupKey in package outbox): its key
// has one separator between two parts, and no other control character.
func outbox(group string) *outboxKey {
	topic, key, ok := strings.Cut(group, "\x1f")
	if !ok || topic == "" || key == "" || visible(topic+key) != topic+key {
		return nil
	}
	return &outboxKey{topic, key}
}

// afterHost returns the part of a worker's ID after its host name and the
// dash that joins them: the process ID and the random suffix, which tell the
// workers of one host apart. It returns nothing for an ID that does not start
// with the host, or that has nothing after it.
func afterHost(id, host string) string {
	rest, ok := strings.CutPrefix(id, host+"-")
	if !ok || host == "" {
		return ""
	}
	return rest
}

// queuesRoom is how many characters of queue names a row of the Workers page
// has room for, at the width of its Queues column. The count of the names
// that it leaves out takes moreRoom of them.
const (
	queuesRoom = 34
	moreRoom   = 4
)

// fitted is the start of a list of names, for one line of a table.
type fitted struct {
	Line string // the first names, with commas between them
	More int    // how many names follow them
}

// fit returns the queues that a row of the Workers page shows: all of them
// if the row has room, or else the first ones that leave room for the count
// of the rest, and the first one at least. The row is one line tall, however
// many queues a worker reads, and the page lists them all on demand.
func fit(names []string) fitted {
	if len(names) == 0 {
		return fitted{}
	}
	const comma = ", "
	total := (len(names) - 1) * len(comma)
	for _, name := range names {
		total += utf8.RuneCountInString(name)
	}
	if total <= queuesRoom {
		return fitted{Line: strings.Join(names, comma)}
	}
	n, used := 1, utf8.RuneCountInString(names[0])
	for ; n < len(names); n++ {
		used += len(comma) + utf8.RuneCountInString(names[n])
		if used > queuesRoom-moreRoom {
			break
		}
	}
	return fitted{Line: strings.Join(names[:n], comma), More: len(names) - n}
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
