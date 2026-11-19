// Package testdb connects integration tests to PostgreSQL.
//
// Tests that need a database call [Pool]. They are skipped when the
// TASKMILL_TEST_DATABASE_URL environment variable is unset; `make test` sets
// it to the database that `make db` starts. In CI they fail instead, so that
// a run that tested nothing cannot pass: see [URL]. Each test gets its own
// schema, so tests can run in parallel and leave nothing behind.
//
// The tests run far from UTC, so that code that depends on a time zone
// fails: the sessions of a [Pool] in a time zone of their own, and the
// process of a package whose TestMain calls [Main] in another.
//
// To run the tests through a transaction-pooling proxy such as PgBouncer,
// point TASKMILL_TEST_DATABASE_URL at the proxy and TASKMILL_TEST_LISTEN_URL
// at the database itself: workers then listen for notifications on a direct
// connection, which the proxy cannot provide.
package testdb

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Environment variables that configure the test database.
const (
	EnvURL       = "TASKMILL_TEST_DATABASE_URL" // the test database URL
	EnvListenURL = "TASKMILL_TEST_LISTEN_URL"   // a direct URL, if EnvURL goes through a proxy
)

// URL returns the test database URL. If none is set, it skips the test, so
// that `go test ./...` works on a machine without a database. In CI, which
// sets the CI environment variable to any value, it fails the test instead.
func URL(tb testing.TB) string {
	tb.Helper()
	url := os.Getenv(EnvURL)
	switch {
	case url != "":
	case os.Getenv("CI") != "":
		tb.Fatalf("%s is not set, and in CI (the CI variable is set) the database tests must not skip: "+
			"set it to the URL of a test database, as `make db` and `make test` do", EnvURL)
	default:
		tb.Skipf("%s is not set; run `make db` and `make test`", EnvURL)
	}
	return url
}

// DirectURL returns a URL that reaches the test database without a proxy:
// EnvListenURL if it is set, or else EnvURL. Tests that set connection
// parameters, which proxies refuse, connect with it.
func DirectURL(tb testing.TB) string {
	tb.Helper()
	if url := os.Getenv(EnvListenURL); url != "" {
		return url
	}
	return URL(tb)
}

// ListenConfig returns the connection that workers listen on: EnvListenURL's,
// or nil if it is unset, which makes workers listen on their pools'.
func ListenConfig(tb testing.TB) *pgx.ConnConfig {
	tb.Helper()
	url := os.Getenv(EnvListenURL)
	if url == "" {
		return nil
	}
	cfg, err := pgx.ParseConfig(url)
	if err != nil {
		tb.Fatalf("testdb: parse %s: %v", EnvListenURL, err)
	}
	return cfg
}

// timeZone is the time zone of a pool's sessions: 24 hours and 45 minutes
// ahead of UTC, which no place on earth is. There, a statement that reads
// the session's time zone, where it means UTC or a moment, gets another day
// at any time of day, and hours that start a quarter past UTC's, so its test
// fails whenever it runs. In a real zone the day differs from UTC's only for
// a part of each day. The zone has no daylight saving time, because what
// that changes depends on the date: a test of it sets the database's clock.
const timeZone = "<+2445>-24:45"

// Main runs the tests of a package, as m.Run does, in a process whose local
// time zone is 13 hours and 45 minutes ahead of UTC, as the Chatham Islands
// are in summer. The driver reads a time into the local zone, so Go code
// that formats such a time, or takes its day or its hour, without naming a
// zone, gets another hour at any time of day, and another day for most of
// it. The machines of CI run in UTC, where such code passes.
//
// A package's tests call it from their TestMain:
//
//	func TestMain(m *testing.M) { testdb.Main(m) }
func Main(m *testing.M) {
	time.Local = time.FixedZone("+1345", (13*60+45)*60)
	os.Exit(m.Run())
}

// Pool returns a pool of up to 8 connections to the test database, closed
// when the test ends. Its sessions run in a time zone far from UTC.
func Pool(tb testing.TB) *pgxpool.Pool {
	tb.Helper()
	return PoolSize(tb, 8)
}

// PoolSize is [Pool] with a given number of connections, for tests that run
// many operations at once.
func PoolSize(tb testing.TB, conns int32) *pgxpool.Pool {
	tb.Helper()
	cfg, err := pgxpool.ParseConfig(URL(tb))
	if err != nil {
		tb.Fatalf("testdb: parse %s: %v", EnvURL, err)
	}
	cfg.MaxConns = conns
	cfg.ConnConfig.RuntimeParams["timezone"] = timeZone

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		tb.Fatalf("testdb: connect: %v", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		tb.Fatalf("testdb: connect: %v", err)
	}
	tb.Cleanup(pool.Close)
	return pool
}

// Schema returns a fresh schema name, and drops that schema when the test
// ends, on a connection of pool.
func Schema(tb testing.TB, pool *pgxpool.Pool) string {
	tb.Helper()
	var b [6]byte
	_, _ = rand.Read(b[:]) // never returns an error
	schema := "test_" + hex.EncodeToString(b[:])
	tb.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := drop(ctx, pool, schema); err != nil {
			tb.Errorf("testdb: drop schema %s: %v", schema, err)
		}
	})
	return schema
}

// drop drops schema once the statements that its test gave up have ended.
//
// A statement whose context ended can go on in the database. Its caller has
// returned, so a worker that was told to stop has stopped, and the pool
// closes the statement's connection in the background, until the server has
// ended the statement. Until then the statement may take locks, in another
// order than a drop of its schema does: PostgreSQL ends one of the two as a
// deadlock, and the test fails if that is the drop. The pool counts such a
// connection as acquired until it is closed, so the drop waits until the
// pool has none.
//
// A proxy closes the connection at once, and the server works on: nothing
// then tells when the statement has ended. So a run through a proxy, and
// only that, tries a drop again that PostgreSQL ended as a deadlock. Without
// a proxy, such a deadlock is a mistake in the order of a test's clean-up,
// and fails the test.
func drop(ctx context.Context, pool *pgxpool.Pool, schema string) error {
	for pool.Stat().AcquiredConns() > 0 {
		if ctx.Err() != nil {
			return errors.New("the pool's connections are still in use")
		}
		time.Sleep(time.Millisecond)
	}
	proxied := os.Getenv(EnvListenURL) != ""
	for {
		_, err := pool.Exec(ctx, "DROP SCHEMA IF EXISTS "+schema+" CASCADE")
		if pgErr, ok := errors.AsType[*pgconn.PgError](err); !proxied || !ok || pgErr.Code != "40P01" { // deadlock_detected
			return err
		}
	}
}

// CheckGroups fails the test unless every FIFO group of schema satisfies
// the invariants in docs/design/task-lifecycle.md: its count matches its
// unfinished tasks plus a halting dead head, it has exactly one head, and
// every unfinished grouped task belongs to a group row.
func CheckGroups(tb testing.TB, pool *pgxpool.Pool, schema string) {
	tb.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	r, err := pool.Query(ctx, strings.ReplaceAll(`
WITH members AS (
    SELECT g.queue, g.group_key, g.tasks, g.halted_task_id,
           count(t.id) FILTER (WHERE t.state = 'pending') AS pending,
           count(t.id) FILTER (WHERE t.state IN ('scheduled', 'available', 'running', 'retryable')) AS heads,
           count(t.id) FILTER (WHERE t.id = g.halted_task_id AND t.state = 'dead') AS halting
    FROM {s}.groups g
    LEFT JOIN {s}.tasks t ON t.queue = g.queue AND t.group_key = g.group_key
    GROUP BY g.queue, g.group_key, g.tasks, g.halted_task_id
)
SELECT format('group %s/%s counts %s tasks: %s pending, %s heads, %s halting dead head (halted by %s)',
              queue, group_key, tasks, pending, heads, halting, coalesce(halted_task_id::text, 'none'))
FROM members
WHERE tasks <> pending + heads + halting
   OR heads + halting <> 1
   OR (halted_task_id IS NOT NULL AND halting = 0)
UNION ALL
SELECT format('task %s in group %s/%s is %s, but the group has no row', t.id, t.queue, t.group_key, t.state)
FROM {s}.tasks t
WHERE t.group_key IS NOT NULL AND t.state IN ('scheduled', 'pending', 'available', 'running', 'retryable')
  AND NOT EXISTS (SELECT 1 FROM {s}.groups g WHERE g.queue = t.queue AND g.group_key = t.group_key)`, "{s}", schema))
	if err != nil {
		tb.Fatalf("testdb: check groups: %v", err)
	}
	defer r.Close()
	var broken []string
	for r.Next() {
		var s string
		if err := r.Scan(&s); err != nil {
			tb.Fatalf("testdb: check groups: %v", err)
		}
		broken = append(broken, s)
	}
	if err := r.Err(); err != nil {
		tb.Fatalf("testdb: check groups: %v", err)
	}
	for _, s := range broken {
		tb.Errorf("FIFO invariant broken: %s", s)
	}
}
