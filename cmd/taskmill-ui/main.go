// Command taskmill-ui serves taskmill's dashboard as a program of its own,
// apart from the applications that use the installation.
//
//	go install github.com/code19m/taskmill/cmd/taskmill-ui@latest
//	TASKMILL_DATABASE_URL=postgres://... TASKMILL_UI_PASSWORD=... taskmill-ui
//
// Each release also publishes it as binaries and as the container image
// ghcr.io/code19m/taskmill-ui.
//
// It reads all its settings from environment variables, and has no flags.
// Only TASKMILL_DATABASE_URL is required. Without TASKMILL_UI_PASSWORD,
// anyone who reaches its address, localhost:8080 by default, can use the
// dashboard. It never migrates the schema: the applications that own it do.
//
// The [manual] lists every variable, why the command exits when it starts,
// its probes, and the dashboard's routes. To serve the dashboard from an
// application's own HTTP server instead, use package
// github.com/code19m/taskmill/ui.
//
// [manual]: https://github.com/code19m/taskmill/blob/main/docs/reference/taskmill-ui.md
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"net"
	"net/http"
	"os"
	"os/signal"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
	"unicode"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/code19m/taskmill"
	"github.com/code19m/taskmill/internal/buildinfo"
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

const readyTimeout = 2 * time.Second // bound on one check of the database

// These times are not configurable, but tests shorten them.
var (
	readTimeout = 10 * time.Second // for a whole request to arrive: its body is a small form at most
	readyReuse  = time.Second      // how long /readyz reuses a check's answer
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

// errDatabaseURL refuses a connection string, and says nothing about it, so
// that no part of its password reaches the log. pgx's own error quotes the
// string, and masks only what it takes for the password.
var errDatabaseURL = errors.New("TASKMILL_DATABASE_URL is not a valid connection string; to keep its password " +
	"out of the log, the error says no more. In a URL, percent-encode the password: @ is %40, and / is %2F")

// poolConfig returns the pool's settings for the connection string, or
// errDatabaseURL.
func poolConfig(connString string) (*pgxpool.Config, error) {
	cfg, err := pgxpool.ParseConfig(connString)
	if err != nil {
		return nil, errDatabaseURL
	}
	// Like libpq, pgx reads a URL's user and password before the first "@",
	// and reads none if a "/" comes first. So a password that holds one of
	// the two, not percent-encoded, is cut there. What follows, with the "@"
	// that ends the password, becomes the hosts, the database, or a
	// parameter, and the errors of a connection name all three. No host or
	// parameter has an "@" in its name, and a database with one is too rare
	// to keep.
	conn := cfg.ConnConfig
	names := slices.AppendSeq([]string{conn.Database, conn.Host}, maps.Keys(conn.RuntimeParams))
	for _, fallback := range conn.Fallbacks {
		names = append(names, fallback.Host)
	}
	if strings.Contains(strings.Join(names, ""), "@") {
		return nil, errDatabaseURL
	}
	return cfg, nil
}

// serve serves the dashboard, set up as getenv says, until ctx ends.
func serve(ctx context.Context, getenv func(string) string, logger *slog.Logger) error {
	cfg, err := loadConfig(getenv)
	if err != nil {
		return err
	}
	poolCfg, err := poolConfig(cfg.databaseURL)
	if err != nil {
		return err
	}
	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
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
		Handler:      h,
		ReadTimeout:  readTimeout,
		WriteTimeout: 30 * time.Second, // pages take at most 10 seconds to read
		IdleTimeout:  2 * time.Minute,
		// The server's own lines are in the log's format too. Each reports
		// a handler's bug, such as a panic, or connections that the server
		// could not accept: both need a person.
		ErrorLog: slog.NewLogLogger(logger.Handler(), slog.LevelError),
	}
	if cfg.auth == nil {
		logger.LogAttrs(ctx, slog.LevelWarn,
			"the dashboard has no password: set TASKMILL_UI_PASSWORD, unless something in front of it authenticates")
	}
	logger.LogAttrs(ctx, slog.LevelInfo, "taskmill-ui listening", slog.String(logKeyVersion, buildinfo.Version()),
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
