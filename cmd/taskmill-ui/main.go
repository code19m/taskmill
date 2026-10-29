// Command taskmill-ui serves taskmill's dashboard of an installation on its
// own, apart from the applications that use it.
//
//	go install github.com/code19m/taskmill/cmd/taskmill-ui@latest
//	TASKMILL_DATABASE_URL=postgres://... TASKMILL_UI_PASSWORD=... taskmill-ui
//
// Each release also publishes it as binaries and as the container image
// ghcr.io/code19m/taskmill-ui.
//
// It reads its settings from the environment:
//
//	TASKMILL_DATABASE_URL        the PostgreSQL connection string (required)
//	TASKMILL_SCHEMA              the installation's schema (default: taskmill)
//	TASKMILL_UI_ADDR             the address to listen on (default:
//	                             localhost:8080; use :8080 in a container)
//	TASKMILL_UI_USERNAME         the user of HTTP basic authentication
//	                             (default: admin)
//	TASKMILL_UI_PASSWORD         its password; without one, anyone who
//	                             reaches the address can use the dashboard
//	TASKMILL_UI_READ_ONLY        true to disable every action (any value
//	                             that strconv.ParseBool accepts)
//	TASKMILL_UI_PREFIX           the path the dashboard is served at, such as
//	                             /admin/jobs, when a proxy forwards that path
//	                             to it unchanged (default: the root)
//	TASKMILL_UI_FRAME_ANCESTORS  the origins of the pages that may show the
//	                             dashboard in a frame, separated by commas or
//	                             spaces, such as https://admin.example.com
//	                             (default: none)
//	TASKMILL_UI_LOG_FORMAT       the format of the logs on standard error:
//	                             text or json (default: text)
//
// For probes, it answers two paths at the root, with no login and whatever
// the prefix: /livez answers 200 while the server runs, and /readyz answers
// 200 while the database answers too, and 503 otherwise. Base only the
// readiness probe on /readyz: a restart does not bring the database back.
// /readyz asks the database at most once a second, and waits for it at most
// 2 seconds.
//
// It does not migrate the schema: the applications that own it do. It exports
// no metrics: the applications export the queue gauges, through the module
// github.com/code19m/taskmill/otel.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
	"unicode"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/code19m/taskmill"
	"github.com/code19m/taskmill/ui"
)

// Log attribute keys.
const (
	logKeyAddr     = "addr"
	logKeyError    = "error"
	logKeyPrefix   = "prefix"
	logKeyReadOnly = "read_only"
	logKeyVersion  = "version"
)

const (
	readyTimeout = 2 * time.Second // bound on one check of the database
	readyReuse   = time.Second     // how long /readyz reuses a check's answer
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	err := run(ctx, os.Getenv, os.Stderr)
	stop()
	if err != nil {
		os.Exit(1)
	}
}

// run serves the dashboard, set up as getenv says, until ctx ends. It logs to
// stderr, and logs why it stopped if it fails.
func run(ctx context.Context, getenv func(string) string, stderr io.Writer) error {
	logger, err := newLogger(getenv("TASKMILL_UI_LOG_FORMAT"), stderr)
	if err == nil {
		err = serve(ctx, getenv, logger)
	}
	if err != nil {
		logger.LogAttrs(ctx, slog.LevelError, "taskmill-ui stopped", slog.Any(logKeyError, err))
	}
	return err
}

// newLogger returns a logger that writes lines in format, text or json, to w.
// For any other format, it returns a text logger and an error.
func newLogger(format string, w io.Writer) (*slog.Logger, error) {
	switch format {
	case "", "text":
		return slog.New(slog.NewTextHandler(w, nil)), nil
	case "json":
		return slog.New(slog.NewJSONHandler(w, nil)), nil
	}
	return slog.New(slog.NewTextHandler(w, nil)), fmt.Errorf("TASKMILL_UI_LOG_FORMAT: %q is not text or json", format)
}

// config is the command's settings.
type config struct {
	databaseURL, schema, addr, prefix string
	readOnly                          bool
	auth                              *ui.BasicAuth
	frameAncestors                    []string
}

func loadConfig(getenv func(string) string) (config, error) {
	c := config{
		databaseURL: getenv("TASKMILL_DATABASE_URL"),
		schema:      getenv("TASKMILL_SCHEMA"),
		addr:        getenv("TASKMILL_UI_ADDR"),
		prefix:      getenv("TASKMILL_UI_PREFIX"),
	}
	if c.databaseURL == "" {
		return c, errors.New("set TASKMILL_DATABASE_URL to the PostgreSQL connection string")
	}
	if c.addr == "" {
		c.addr = "localhost:8080"
	}
	c.frameAncestors = strings.FieldsFunc(getenv("TASKMILL_UI_FRAME_ANCESTORS"), func(r rune) bool {
		return r == ',' || unicode.IsSpace(r)
	})
	if v := getenv("TASKMILL_UI_READ_ONLY"); v != "" {
		readOnly, err := strconv.ParseBool(v)
		if err != nil {
			return c, fmt.Errorf("TASKMILL_UI_READ_ONLY: %w", err)
		}
		c.readOnly = readOnly
	}
	if password := getenv("TASKMILL_UI_PASSWORD"); password != "" {
		user := getenv("TASKMILL_UI_USERNAME")
		if user == "" {
			user = "admin"
		}
		c.auth = &ui.BasicAuth{Username: user, Password: password}
	}
	return c, nil
}

// serve serves the dashboard, set up as getenv says, until ctx ends.
func serve(ctx context.Context, getenv func(string) string, logger *slog.Logger) error {
	cfg, err := loadConfig(getenv)
	if err != nil {
		return err
	}
	pool, err := pgxpool.New(ctx, cfg.databaseURL)
	if err != nil {
		return err
	}
	defer pool.Close()
	client, err := taskmill.NewClient(ctx, pool, &taskmill.ClientConfig{Schema: cfg.schema, Logger: logger})
	if err != nil {
		return err
	}

	h, err := handler(pool, client, cfg, logger)
	if err != nil {
		return err
	}
	ln, err := (&net.ListenConfig{}).Listen(ctx, "tcp", cfg.addr)
	if err != nil {
		return err
	}
	server := &http.Server{
		Handler:           h,
		ReadHeaderTimeout: 10 * time.Second,
		WriteTimeout:      30 * time.Second, // pages take at most 10 seconds to read
		IdleTimeout:       2 * time.Minute,
	}
	if cfg.auth == nil {
		logger.LogAttrs(ctx, slog.LevelWarn,
			"the dashboard has no password: set TASKMILL_UI_PASSWORD, unless something in front of it authenticates")
	}
	logger.LogAttrs(ctx, slog.LevelInfo, "taskmill-ui listening", slog.String(logKeyVersion, version()),
		slog.String(logKeyAddr, ln.Addr().String()), slog.String(logKeyPrefix, cfg.prefix), slog.Bool(logKeyReadOnly, cfg.readOnly))

	errs := make(chan error, 1)
	go func() { errs <- server.Serve(ln) }()
	select {
	case err := <-errs:
		return err
	case <-ctx.Done():
	}
	shutdown, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	return server.Shutdown(shutdown)
}

// version returns taskmill's version as built into the binary, such as v1.0.0.
func version() string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return "unknown"
	}
	for _, m := range append([]*debug.Module{&info.Main}, info.Deps...) {
		if m.Path == "github.com/code19m/taskmill" {
			return m.Version
		}
	}
	return "unknown"
}

// handler returns the dashboard for client, set up as cfg says, and the
// paths for probes at the root, which need no login: /livez answers while the
// server runs, and /readyz while pool reaches the database too.
func handler(pool *pgxpool.Pool, client *taskmill.Client, cfg config, logger *slog.Logger) (http.Handler, error) {
	dashboard, err := ui.New(client, &ui.Config{
		Prefix: cfg.prefix, ReadOnly: cfg.readOnly, BasicAuth: cfg.auth,
		FrameAncestors: cfg.frameAncestors, Logger: logger,
	})
	if err != nil {
		return nil, fmt.Errorf("TASKMILL_UI_*: %w", err)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /livez", func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "ok\n") })
	mux.Handle("GET /readyz", &readiness{pool: pool, logger: logger})
	mux.Handle("/", dashboard)
	return mux, nil
}

// readiness answers /readyz: whether the database answers. It asks the
// database at most once per readyReuse, and one request at a time, so that
// probes, and anyone else who reaches the address, cost the database little.
// It logs when its answer changes.
type readiness struct {
	pool   *pgxpool.Pool
	logger *slog.Logger

	mu      sync.Mutex
	checked time.Time // when the last check ended
	err     error     // why the database did not answer it, or nil
}

func (rd *readiness) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// The answer names no host or error: anyone may ask. The log has both.
	if rd.check(r.Context()) != nil {
		http.Error(w, "not ready: the database does not answer", http.StatusServiceUnavailable)
		return
	}
	_, _ = io.WriteString(w, "ok\n")
}

// check returns why the database does not answer, or nil.
func (rd *readiness) check(ctx context.Context) error {
	rd.mu.Lock()
	defer rd.mu.Unlock()
	if time.Since(rd.checked) < readyReuse {
		return rd.err
	}
	// The check answers the requests that wait for it too, so it outlasts
	// the request that started it.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), readyTimeout)
	defer cancel()
	err := rd.pool.Ping(ctx)
	switch {
	case err != nil && rd.err == nil:
		rd.logger.LogAttrs(ctx, slog.LevelWarn, "taskmill-ui not ready", slog.Any(logKeyError, err))
	case err == nil && rd.err != nil:
		rd.logger.LogAttrs(ctx, slog.LevelInfo, "taskmill-ui ready again")
	}
	rd.checked, rd.err = time.Now(), err
	return err
}
