package taskmill_test

import (
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/code19m/taskmill"
	"github.com/code19m/taskmill/internal/testdb"
)

// sendEmail is the task most tests enqueue.
type sendEmail struct {
	To     string `json:"to"`
	UserID int64  `json:"user_id"`
}

func (sendEmail) Kind() string { return "email.send" }

// report is a task kind with its own defaults.
type report struct {
	Name string `json:"name"`
}

func (report) Kind() string { return "report.build" }

func (report) EnqueueDefaults() taskmill.EnqueueOptions {
	return taskmill.EnqueueOptions{Queue: "reports", Priority: taskmill.PriorityLow, MaxAttempts: 3}
}

// env is a migrated schema with a client.
type env struct {
	pool   *pgxpool.Pool
	schema string
	client *taskmill.Client
}

// newEnv migrates a fresh schema and returns a client for it.
func newEnv(t *testing.T) *env {
	t.Helper()
	return newEnvWith(t, testdb.Pool(t), nil)
}

// newEnvWith is newEnv with a given pool and client configuration.
func newEnvWith(t *testing.T, pool *pgxpool.Pool, cfg *taskmill.ClientConfig) *env {
	t.Helper()
	schema := testdb.Schema(t, pool)
	if err := taskmill.Migrate(t.Context(), pool, &taskmill.MigrateConfig{Schema: schema}); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	if cfg == nil {
		cfg = &taskmill.ClientConfig{}
	}
	cfg.Schema = schema
	client, err := taskmill.NewClient(t.Context(), pool, cfg)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	return &env{pool: pool, schema: schema, client: client}
}

// enqueue enqueues args and fails the test on error.
func (e *env) enqueue(t *testing.T, args taskmill.Args, opts *taskmill.EnqueueOptions) int64 {
	t.Helper()
	res, err := e.client.Enqueue(t.Context(), args, opts)
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	return res.ID
}

// get returns a task and fails the test on error.
func (e *env) get(t *testing.T, id int64) *taskmill.TaskInfo {
	t.Helper()
	info, err := e.client.Get(t.Context(), id)
	if err != nil {
		t.Fatalf("get task %d: %v", id, err)
	}
	return info
}

// exec runs a statement against the env's schema; {s} stands for the schema.
func (e *env) exec(t *testing.T, sql string, args ...any) {
	t.Helper()
	if _, err := e.pool.Exec(t.Context(), replaceSchema(sql, e.schema), args...); err != nil {
		t.Fatalf("exec %q: %v", sql, err)
	}
}

// count returns the result of a count query against the env's schema.
func (e *env) count(t *testing.T, sql string, args ...any) int {
	t.Helper()
	var n int
	if err := e.pool.QueryRow(t.Context(), replaceSchema(sql, e.schema), args...).Scan(&n); err != nil {
		t.Fatalf("query %q: %v", sql, err)
	}
	return n
}

func replaceSchema(sql, schema string) string {
	return strings.ReplaceAll(sql, "{s}", schema)
}

// wantState fails the test unless the task is in state.
func (e *env) wantState(t *testing.T, id int64, state taskmill.State) {
	t.Helper()
	if got := e.get(t, id).State; got != state {
		t.Fatalf("task %d is %s, want %s", id, got, state)
	}
}

// waitFor waits until cond holds.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// begin starts a transaction that is rolled back when the test ends, unless
// the test commits it.
func (e *env) begin(t *testing.T) pgx.Tx { //nolint:ireturn // pgx's transaction type is an interface
	t.Helper()
	tx, err := e.pool.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tx.Rollback(t.Context()) })
	return tx
}

// txExec runs a statement in tx; {s} stands for the schema.
func (e *env) txExec(t *testing.T, tx pgx.Tx, sql string, args ...any) {
	t.Helper()
	if _, err := tx.Exec(t.Context(), replaceSchema(sql, e.schema), args...); err != nil {
		t.Fatalf("exec %q: %v", sql, err)
	}
}

// waitBlocked waits until a statement that contains fragment, with {s} for
// the schema, waits for a lock, and returns its backend's process ID.
func (e *env) waitBlocked(t *testing.T, fragment string) int {
	t.Helper()
	var pid int
	waitFor(t, "a statement to wait for a lock", func() bool {
		err := e.pool.QueryRow(t.Context(), `
SELECT pid FROM pg_stat_activity
WHERE wait_event_type = 'Lock' AND strpos(query, $1) > 0
LIMIT 1`, replaceSchema(fragment, e.schema)).Scan(&pid)
		return err == nil
	})
	return pid
}
