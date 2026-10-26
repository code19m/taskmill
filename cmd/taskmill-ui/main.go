// Command taskmill-ui serves taskmill's web interface on its own: a dashboard
// for an installation, run apart from the applications that use it.
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
//	TASKMILL_UI_READ_ONLY        "true" to disable every action
//	TASKMILL_UI_PREFIX           the path the dashboard is served at, such as
//	                             /admin/jobs, when a proxy forwards that path
//	                             to it unchanged (default: the root)
//	TASKMILL_UI_FRAME_ANCESTORS  the origins of the pages that may show the
//	                             dashboard in a frame, separated by commas or
//	                             spaces, such as https://admin.example.com
//	                             (default: none)
//
// It does not migrate the schema: the applications that own it do.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"runtime/debug"
	"strconv"
	"strings"
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

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	if err := run(os.Getenv, logger); err != nil {
		logger.LogAttrs(context.Background(), slog.LevelError, "taskmill-ui stopped", slog.Any(logKeyError, err))
		os.Exit(1)
	}
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
	if c.prefix != "" && !strings.HasPrefix(c.prefix, "/") {
		return c, fmt.Errorf("TASKMILL_UI_PREFIX: %q is not a path: start it with /", c.prefix)
	}
	c.frameAncestors = strings.FieldsFunc(getenv("TASKMILL_UI_FRAME_ANCESTORS"), func(r rune) bool {
		return r == ',' || unicode.IsSpace(r)
	})
	for _, origin := range c.frameAncestors {
		if strings.Contains(origin, ";") {
			return c, fmt.Errorf("TASKMILL_UI_FRAME_ANCESTORS: %q holds a semicolon", origin)
		}
	}
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

func run(getenv func(string) string, logger *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
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

	server := &http.Server{
		Addr:              cfg.addr,
		Handler:           handler(client, cfg, logger),
		ReadHeaderTimeout: 10 * time.Second,
		WriteTimeout:      30 * time.Second, // pages take at most 10 seconds to read
		IdleTimeout:       2 * time.Minute,
	}
	if cfg.auth == nil {
		logger.LogAttrs(ctx, slog.LevelWarn,
			"the dashboard has no password: set TASKMILL_UI_PASSWORD, unless something in front of it authenticates")
	}
	logger.LogAttrs(ctx, slog.LevelInfo, "taskmill-ui listening", slog.String(logKeyVersion, version()),
		slog.String(logKeyAddr, cfg.addr), slog.String(logKeyPrefix, cfg.prefix), slog.Bool(logKeyReadOnly, cfg.readOnly))

	errs := make(chan error, 1)
	go func() { errs <- server.ListenAndServe() }()
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

// handler returns the dashboard for client, set up as cfg says.
func handler(client *taskmill.Client, cfg config, logger *slog.Logger) http.Handler {
	return ui.Handler(client, &ui.Config{
		Prefix: cfg.prefix, ReadOnly: cfg.readOnly, BasicAuth: cfg.auth,
		FrameAncestors: cfg.frameAncestors, Logger: logger,
	})
}
