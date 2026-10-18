// Package ui serves a web interface for operating taskmill: queues and their
// statistics, workers, schedules, and tasks, with actions to pause and resume
// queues and schedules and to retry, cancel, and delete tasks.
//
// The interface is server-rendered with the standard library and needs no
// JavaScript build step. It reads only through the bounded queries of
// [taskmill.Client], so it is safe to point at a busy production database.
//
// The handler has no authentication of its own: mount it behind the
// application's own middleware.
//
//	mux.Handle("/taskmill/", auth(ui.Handler(client, &ui.Config{Prefix: "/taskmill"})))
//
// Actions are protected against cross-site request forgery with
// [http.CrossOriginProtection], and [Config.ReadOnly] disables them entirely.
package ui

import (
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"io/fs"
	"log/slog"
	"maps"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/code19m/taskmill"
)

//go:embed templates static
var assets embed.FS

// Log attribute keys.
const (
	logKeyError = "error"
	logKeyPage  = "page"
	logKeyPath  = "path"
)

// Config configures the web interface. The zero value is valid.
type Config struct {
	// Prefix is the path the handler is mounted at, such as "/taskmill".
	// Links and redirects include it. Default: none, the root.
	Prefix string

	// ReadOnly hides and refuses every action that changes state.
	ReadOnly bool

	// Logger receives errors that the interface shows as a server error.
	// Default: logs are discarded.
	Logger *slog.Logger
}

// Handler returns the web interface for client. cfg may be nil.
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
	if s.logger == nil {
		s.logger = slog.New(slog.DiscardHandler)
	}
	s.pages = template.Must(template.New("").Funcs(template.FuncMap{
		"ago":       ago,
		"timestamp": timestamp,
		"path":      s.path,
		"json":      prettyJSON,
		"states":    func() []taskmill.State { return listedStates },
		"lastError": lastError,
		// segment escapes a name for one path segment: queue and schedule
		// names may hold /, ?, #, or %.
		"segment": url.PathEscape,
	}).ParseFS(assets, "templates/*.html"))

	static, _ := fs.Sub(assets, "static") // the directory is embedded above
	mux := http.NewServeMux()
	mux.Handle("GET /static/", http.StripPrefix("/static/", http.FileServerFS(static)))
	mux.HandleFunc("GET /{$}", s.overview)
	mux.HandleFunc("GET /tasks", s.tasks)
	mux.HandleFunc("GET /tasks/{id}", s.task)
	mux.HandleFunc("POST /tasks/{id}/{action}", s.taskAction)
	mux.HandleFunc("POST /queues/{name}/{action}", s.queueAction)
	mux.HandleFunc("POST /schedules/{name}/{action}", s.scheduleAction)

	h := http.NewCrossOriginProtection().Handler(mux)
	if s.prefix != "" {
		h = http.StripPrefix(s.prefix, h)
	}
	return securityHeaders(h)
}

// listedStates are the states the task list can filter by, in lifecycle
// order.
var listedStates = []taskmill.State{
	taskmill.StateScheduled, taskmill.StatePending, taskmill.StateAvailable, taskmill.StateRunning,
	taskmill.StateRetryable, taskmill.StateCompleted, taskmill.StateDead, taskmill.StateCanceled,
}

type server struct {
	client   *taskmill.Client
	pages    *template.Template
	prefix   string
	readOnly bool
	logger   *slog.Logger
}

// page is what every template receives.
type page struct {
	Title    string
	ReadOnly bool
	Notice   string
	Data     any
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
	s.render(w, r, "overview.html", "Overview", struct {
		Queues    []taskmill.QueueInfo
		Workers   []taskmill.WorkerInfo
		Schedules []taskmill.ScheduleInfo
		States    []taskmill.State
	}{queues, workers, schedules, []taskmill.State{
		taskmill.StateAvailable, taskmill.StateRunning, taskmill.StateScheduled, taskmill.StateRetryable,
		taskmill.StatePending, taskmill.StateDead, taskmill.StateCanceled,
	}})
}

func (s *server) tasks(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	params := &taskmill.ListParams{Queue: q.Get("queue"), Kind: q.Get("kind"), Cursor: q.Get("cursor"), Limit: 50}
	if state := q.Get("state"); state != "" {
		params.States = []taskmill.State{taskmill.State(state)}
	}
	res, err := s.client.List(r.Context(), params)
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
	s.render(w, r, "tasks.html", "Tasks", struct {
		Tasks              []taskmill.TaskInfo
		Queue, Kind, State string
		Next               string
	}{res.Tasks, params.Queue, params.Kind, q.Get("state"), next})
}

func (s *server) task(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		s.notFound(w, r)
		return
	}
	info, err := s.client.Get(r.Context(), id)
	if errors.Is(err, taskmill.ErrNotFound) {
		s.notFound(w, r)
		return
	}
	if err != nil {
		s.fail(w, r, err)
		return
	}
	s.render(w, r, "task.html", fmt.Sprintf("Task %d", id), info)
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
	target := s.path("/tasks/" + strconv.FormatInt(id, 10))
	switch r.PathValue("action") {
	case "retry":
		err = s.client.Retry(ctx, id)
	case "cancel":
		err = s.client.Cancel(ctx, id)
	case "delete":
		err = s.client.Delete(ctx, id)
		target = s.path("/tasks")
	default:
		s.notFound(w, r)
		return
	}
	s.finishAction(w, r, err, target)
}

func (s *server) queueAction(w http.ResponseWriter, r *http.Request) {
	if s.refuseIfReadOnly(w) {
		return
	}
	var err error
	name := r.PathValue("name")
	switch r.PathValue("action") {
	case "pause":
		err = s.client.PauseQueue(r.Context(), name)
	case "resume":
		err = s.client.ResumeQueue(r.Context(), name)
	default:
		s.notFound(w, r)
		return
	}
	s.finishAction(w, r, err, s.path("/"))
}

func (s *server) scheduleAction(w http.ResponseWriter, r *http.Request) {
	if s.refuseIfReadOnly(w) {
		return
	}
	var err error
	name := r.PathValue("name")
	switch r.PathValue("action") {
	case "pause":
		err = s.client.PauseSchedule(r.Context(), name)
	case "resume":
		err = s.client.ResumeSchedule(r.Context(), name)
	default:
		s.notFound(w, r)
		return
	}
	s.finishAction(w, r, err, s.path("/"))
}

// finishAction redirects after an action, or explains why it did not apply.
func (s *server) finishAction(w http.ResponseWriter, r *http.Request, err error, target string) {
	switch {
	case err == nil:
		http.Redirect(w, r, target, http.StatusSeeOther)
	case errors.Is(err, taskmill.ErrNotFound):
		s.notFound(w, r)
	case errors.Is(err, taskmill.ErrInvalidState), errors.Is(err, taskmill.ErrInvalidArgument):
		w.WriteHeader(http.StatusConflict)
		s.render(w, r, "error.html", "Not possible", strings.TrimPrefix(err.Error(), "taskmill: "))
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
	s.render(w, r, "error.html", "Not found", "Nothing exists at this address.")
}

func (s *server) fail(w http.ResponseWriter, r *http.Request, err error) {
	s.logger.LogAttrs(r.Context(), slog.LevelError, "web interface request failed",
		slog.String(logKeyPath, r.URL.Path), slog.Any(logKeyError, err))
	w.WriteHeader(http.StatusInternalServerError)
	s.render(w, r, "error.html", "Something went wrong", "The request failed; the server log has the details.")
}

func (s *server) render(w http.ResponseWriter, r *http.Request, name, title string, data any) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	err := s.pages.ExecuteTemplate(w, name, page{Title: title, ReadOnly: s.readOnly, Data: data})
	if err != nil {
		s.logger.LogAttrs(r.Context(), slog.LevelError, "render page failed",
			slog.String(logKeyPage, name), slog.Any(logKeyError, err))
	}
}

// path returns an absolute path inside the interface.
func (s *server) path(p string) string {
	return s.prefix + p
}

// securityHeaders sets headers that keep the interface from being framed,
// sniffed, or made to load anything but its own assets.
func securityHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := w.Header()
		h.Set("Content-Security-Policy", "default-src 'none'; style-src 'self'; script-src 'self'; img-src 'self'; form-action 'self'; frame-ancestors 'none'; base-uri 'none'")
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

// lastError returns the most recent attempt error, or nil.
func lastError(errs []taskmill.AttemptError) *taskmill.AttemptError {
	if len(errs) == 0 {
		return nil
	}
	return &errs[len(errs)-1]
}

func timestamp(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format("2006-01-02 15:04:05.000 UTC")
}

// prettyJSON indents a payload for reading. It keeps characters such as "<"
// as they are: the template escapes them for HTML.
func prettyJSON(raw json.RawMessage) string {
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		return string(raw)
	}
	var b strings.Builder
	enc := json.NewEncoder(&b)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "  ")
	if err := enc.Encode(v); err != nil {
		return string(raw)
	}
	return strings.TrimSuffix(b.String(), "\n")
}
