// Package testdb connects integration tests to PostgreSQL.
//
// Tests that need a database call [Pool]. They are skipped when the
// TASKMILL_TEST_DATABASE_URL environment variable is unset; `make test` sets
// it to the database that `make db` starts. Each test gets its own schema, so
// tests can run in parallel and leave nothing behind.
package testdb

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// EnvURL names the environment variable that holds the test database URL.
const EnvURL = "TASKMILL_TEST_DATABASE_URL"

// URL returns the test database URL, or skips the test if none is set.
func URL(tb testing.TB) string {
	tb.Helper()
	url := os.Getenv(EnvURL)
	if url == "" {
		tb.Skipf("%s is not set; run `make db` and `make test`", EnvURL)
	}
	return url
}

// Pool returns a pool of up to 8 connections to the test database, closed
// when the test ends.
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
// ends.
func Schema(tb testing.TB, pool *pgxpool.Pool) string {
	tb.Helper()
	var b [6]byte
	_, _ = rand.Read(b[:]) // never returns an error
	schema := "test_" + hex.EncodeToString(b[:])
	tb.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if _, err := pool.Exec(ctx, "DROP SCHEMA IF EXISTS "+schema+" CASCADE"); err != nil {
			tb.Errorf("testdb: drop schema %s: %v", schema, err)
		}
	})
	return schema
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
