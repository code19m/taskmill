// Package testdb connects integration tests to PostgreSQL.
//
// Tests that need a database call [Pool]. They are skipped when the
// TASKMILL_TEST_DATABASE_URL environment variable is unset; `make test` sets
// it to the database that `make db` starts. Each test gets its own schema, so
// tests can run in parallel and leave nothing behind.
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
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Environment variables that configure the test database.
const (
	EnvURL       = "TASKMILL_TEST_DATABASE_URL" // the test database URL
	EnvListenURL = "TASKMILL_TEST_LISTEN_URL"   // a direct URL, if EnvURL goes through a proxy
)

// URL returns the test database URL, or skips the test if none is set.
func URL(tb testing.TB) string {
	tb.Helper()
	url := os.Getenv(EnvURL)
	if url == "" {
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
