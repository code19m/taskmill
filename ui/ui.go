// Package ui serves taskmill's web interface: a dashboard of queues, tasks,
// schedules, and workers, with actions to pause and resume queues and
// schedules, run a schedule now, and retry, cancel, and delete tasks.
//
// The interface is rendered on the server with the standard library. A small
// script, served with it, keeps pages live and adds dialogs and charts; every
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
	"crypto/sha256"
	"crypto/subtle"
	"embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"io/fs"
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

//go:embed templates static
var assets embed.FS

// Log attribute keys.
const (
	logKeyAction = "action"
	logKeyError  = "error"
	logKeyPage   = "page"
	logKeyPath   = "path"
	logKeyTarget = "target"
)

const (
	pageSize       = 50
	groupPeek      = 100              // most waiting tasks of a group that a task page counts
	staleHeartbeat = 30 * time.Second // a worker silent this long is shown as stale
	statsCap       = 10_000           // the per-queue count cap of the statistics
)

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
		versions: assetVersions(),
	}
	if s.logger == nil {
		s.logger = slog.New(slog.DiscardHandler)
	}
	s.pages = template.Must(template.New("").Funcs(s.funcs()).ParseFS(assets, "templates/*.html"))

	static, _ := fs.Sub(assets, "static") // the directory is embedded above
	mux := http.NewServeMux()
	mux.Handle("GET /static/", http.StripPrefix("/static/", immutable(http.FileServerFS(static))))
	mux.HandleFunc("GET /{$}", s.overview)
	mux.HandleFunc("GET /tasks", s.tasks)
	mux.HandleFunc("GET /tasks/{id}", s.task)
	mux.HandleFunc("GET /schedules", s.schedules)
	mux.HandleFunc("GET /workers", s.workers)
	mux.HandleFunc("POST /tasks/{id}/{action}", s.taskAction)
	mux.HandleFunc("POST /queues/{name}/{action}", s.queueAction)
	mux.HandleFunc("POST /schedules/{name}/{action}", s.scheduleAction)

	h := http.NewCrossOriginProtection().Handler(mux)
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
	s.render(w, r, "overview.html", page{Title: "Overview", Nav: "overview", Dead: t.Dead, Data: struct {
		Queues      []taskmill.QueueInfo
		States      []taskmill.State
		Totals      totals
		Workers     int
		Maintainer  string
		Schedules   int
		NextRun     time.Time
		Settings    *taskmill.Settings
		RefreshedAt time.Time
	}{queues, queueStates, t, len(workers), maintainer, len(schedules), nextRun, settings, refreshed}})
}

func (s *server) tasks(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	params := &taskmill.ListParams{
		Queue: q.Get("queue"), Kind: q.Get("kind"), GroupKey: q.Get("group"), Cursor: q.Get("cursor"), Limit: pageSize,
	}
	state := q.Get("state")
	if state != "" {
		params.States = []taskmill.State{taskmill.State(state)}
	}
	res, err := s.client.List(r.Context(), params)
	if errors.Is(err, taskmill.ErrInvalidArgument) {
		w.WriteHeader(http.StatusBadRequest)
		s.render(w, r, "error.html", page{Title: "Invalid filter", Data: err.Error()})
		return
	}
	if err != nil {
		s.fail(w, r, err)
		return
	}
	queues, err := s.client.Queues(r.Context())
	if err != nil {
		s.fail(w, r, err)
		return
	}
	next := ""
	if res.Next != "" {
		v := maps.Clone(q)
		v.Set("cursor", res.Next)
		next = s.path("/tasks?" + v.Encode())
	}
	nav := "tasks"
	if state == string(taskmill.StateDead) {
		nav = "dead"
	}
	filtered := params.Queue != "" || params.Kind != "" || params.GroupKey != "" || state != ""
	s.render(w, r, "tasks.html", page{Title: "Tasks", Nav: nav, Dead: sumQueues(queues).Dead, Data: struct {
		Tasks                     []taskmill.TaskInfo
		Queues                    []taskmill.QueueInfo
		Queue, Kind, Group, State string
		Filtered, Paged           bool
		Next                      string
	}{res.Tasks, queues, params.Queue, params.Kind, params.GroupKey, state, filtered, params.Cursor != "", next}})
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
	s.render(w, r, "task.html", page{Title: fmt.Sprintf("Task %d", id), Nav: "tasks", Dead: s.deadCount(r), Data: struct {
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
			Queue: t.Queue, GroupKey: t.GroupKey, States: []taskmill.State{taskmill.StatePending}, Limit: groupPeek,
		})
		if err != nil {
			return g, err
		}
		g.Waiting, g.More = len(page.Tasks), page.Next != ""
	case taskmill.StatePending:
		page, err := s.client.List(r.Context(), &taskmill.ListParams{
			Queue: t.Queue, GroupKey: t.GroupKey, Limit: 1, States: []taskmill.State{
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
	s.render(w, r, "schedules.html", page{Title: "Schedules", Nav: "schedules", Dead: s.deadCount(r), Data: schedules})
}

func (s *server) workers(w http.ResponseWriter, r *http.Request) {
	workers, err := s.client.Workers(r.Context())
	if err != nil {
		s.fail(w, r, err)
		return
	}
	s.render(w, r, "workers.html", page{Title: "Workers", Nav: "workers", Dead: s.deadCount(r), Data: workers})
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
	switch {
	case err == nil:
		s.logger.LogAttrs(r.Context(), slog.LevelInfo, "web interface action",
			slog.String(logKeyAction, action), slog.String(logKeyTarget, name))
		http.Redirect(w, r, s.path(target+"?done="+notice), http.StatusSeeOther)
	case errors.Is(err, taskmill.ErrNotFound):
		s.notFound(w, r)
	case errors.Is(err, taskmill.ErrInvalidState), errors.Is(err, taskmill.ErrInvalidArgument):
		w.WriteHeader(http.StatusConflict)
		s.render(w, r, "error.html", page{Title: "Not possible", Data: err.Error()})
	default:
		s.fail(w, r, err)
	}
}

func (s *server) refuseIfReadOnly(w http.ResponseWriter) bool {
	if !s.readOnly {
		return false
	}
	http.Error(w, "this interface is read-only", http.StatusForbidden)
	return true
}

func (s *server) notFound(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusNotFound)
	s.render(w, r, "error.html", page{Title: "Not found", Data: "Nothing exists at this address. The task may have been deleted."})
}

func (s *server) fail(w http.ResponseWriter, r *http.Request, err error) {
	s.logger.LogAttrs(r.Context(), slog.LevelError, "web interface request failed",
		slog.String(logKeyPath, r.URL.Path), slog.Any(logKeyError, err))
	w.WriteHeader(http.StatusInternalServerError)
	s.render(w, r, "error.html", page{Title: "Something went wrong", Data: "The request failed; the server log has the details."})
}

func (s *server) render(w http.ResponseWriter, r *http.Request, name string, p page) {
	p.ReadOnly = s.readOnly
	p.Notice = notices[r.URL.Query().Get("done")]
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := s.pages.ExecuteTemplate(w, name, p); err != nil {
		s.logger.LogAttrs(r.Context(), slog.LevelError, "render page failed",
			slog.String(logKeyPage, name), slog.Any(logKeyError, err))
	}
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

// assetVersions hashes the content of every static file.
func assetVersions() map[string]string {
	versions := map[string]string{}
	entries, _ := assets.ReadDir("static") // the directory is embedded above
	for _, e := range entries {
		data, _ := assets.ReadFile("static/" + e.Name())
		sum := sha256.Sum256(data)
		versions[e.Name()] = hex.EncodeToString(sum[:4])
	}
	return versions
}

// immutable lets browsers cache static files for a year: their URLs carry a
// hash of their content.
func immutable(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
		next.ServeHTTP(w, r)
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
			w.Header().Set("WWW-Authenticate", `Basic realm="taskmill", charset="UTF-8"`)
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
