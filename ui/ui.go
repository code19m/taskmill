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
	"embed"
	"fmt"
	"html/template"
	"log/slog"
	"net/http"
	"strings"
	"time"
	"unicode"

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
// [Config.BasicAuth]. Neither field may hold a control character, such as a
// line break at the end of a value read from a file: a browser cannot send
// one.
type BasicAuth struct {
	// Username is the user's name. Required. It must not hold a colon:
	// basic authentication ends the name at the first one.
	Username string

	// Password is the user's password. Required.
	Password string
}

// New returns the dashboard for client. cfg may be nil. It returns an
// error that wraps [taskmill.ErrInvalidArgument] if client is nil, if
// cfg.Prefix does not start with a slash, if cfg.BasicAuth has an empty
// username or password, a username with a colon, or a control character in
// either, or if an entry of cfg.FrameAncestors is empty or holds a space,
// comma, or semicolon.
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
	mux.HandleFunc("GET /tasks", s.taskList)
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
	case cfg.BasicAuth != nil && strings.Contains(cfg.BasicAuth.Username, ":"):
		return "", invalid("BasicAuth's username holds a colon, which ends a username in basic authentication")
	case cfg.BasicAuth != nil && strings.ContainsFunc(cfg.BasicAuth.Username+cfg.BasicAuth.Password, unicode.IsControl):
		return "", invalid("BasicAuth's username or password holds a control character, such as a line break, which a browser cannot send")
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

// Live reports whether the script updates the page every few seconds: a page
// of a section does, unless it is Static. A status page of a cause that may
// pass tries its address again instead, as Retry says.
func (p page) Live() bool {
	return p.Nav != "" && !p.Static
}
