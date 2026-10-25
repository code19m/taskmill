package taskmill_test

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib" // registers the "pgx" database/sql driver

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

	// rawGroups turns off the FIFO group check at the end of the test, for
	// tests that write group rows by hand.
	rawGroups bool
}

// newEnv migrates a fresh schema and returns a client for it.
func newEnv(t *testing.T) *env {
	t.Helper()
	return newEnvWith(t, testdb.Pool(t), nil)
}

// withLogger returns a copy of e whose client, and the workers created with
// it, log to logger.
func (e *env) withLogger(t *testing.T, logger *slog.Logger) *env {
	t.Helper()
	client, err := taskmill.NewClient(t.Context(), e.pool, &taskmill.ClientConfig{Schema: e.schema, Logger: logger})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	c := *e
	c.client = client
	return &c
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
	if cfg.Logger == nil {
		cfg.Logger = testLogger(t)
	}
	cfg.Schema = schema
	client, err := taskmill.NewClient(t.Context(), pool, cfg)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	e := &env{pool: pool, schema: schema, client: client}
	// Cleanups run last first, so this check runs after the test's workers
	// stopped.
	t.Cleanup(func() {
		if !e.rawGroups && !t.Failed() {
			e.checkGroups(t)
		}
	})
	return e
}

// checkGroups fails the test unless every FIFO group satisfies the
// invariants in docs/design/task-lifecycle.md: its count matches its
// unfinished tasks plus a halting dead head, it has exactly one head, and
// every unfinished grouped task belongs to a group row.
func (e *env) checkGroups(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var broken []string
	r, err := e.pool.Query(ctx, replaceSchema(`
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
  AND NOT EXISTS (SELECT 1 FROM {s}.groups g WHERE g.queue = t.queue AND g.group_key = t.group_key)`, e.schema))
	if err != nil {
		t.Fatalf("check groups: %v", err)
	}
	defer r.Close()
	for r.Next() {
		var s string
		if err := r.Scan(&s); err != nil {
			t.Fatalf("check groups: %v", err)
		}
		broken = append(broken, s)
	}
	if err := r.Err(); err != nil {
		t.Fatalf("check groups: %v", err)
	}
	for _, s := range broken {
		t.Errorf("FIFO invariant broken: %s", s)
	}
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

// waitState waits for a task to reach state.
func (e *env) waitState(t *testing.T, id int64, state taskmill.State) *taskmill.TaskInfo {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		info, err := e.client.Get(t.Context(), id)
		if err == nil && info.State == state {
			return info
		}
		if time.Now().After(deadline) {
			got := "error: " + fmt.Sprint(err)
			if info != nil {
				got = string(info.State)
			}
			t.Fatalf("task %d did not become %s; it is %s", id, state, got)
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
	// t.Context is canceled before cleanups run.
	t.Cleanup(func() { _ = tx.Rollback(context.Background()) })
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

// openSQL opens the test database through database/sql.
func openSQL(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("pgx", testdb.URL(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// testLogger writes warnings to t's log, which shows when the test fails: a
// lost lease then says why. It drops what arrives once t's cleanup starts,
// such as a trailing notification that fails on the closed pool: writing to
// a finished test's log is a data race.
func testLogger(t *testing.T) *slog.Logger {
	t.Helper()
	w := &testWriter{out: t.Output()}
	t.Cleanup(func() {
		w.mu.Lock()
		defer w.mu.Unlock()
		w.out = nil
	})
	return slog.New(slog.NewTextHandler(w, &slog.HandlerOptions{Level: slog.LevelWarn}))
}

type testWriter struct {
	mu  sync.Mutex
	out io.Writer // nil once the test's cleanup started
}

func (w *testWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.out == nil {
		return len(p), nil
	}
	return w.out.Write(p)
}

// logRecorder is a slog handler that keeps the messages it receives.
type logRecorder struct {
	mu       sync.Mutex
	messages []string
}

func (*logRecorder) Enabled(context.Context, slog.Level) bool { return true }

func (l *logRecorder) Handle(_ context.Context, r slog.Record) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.messages = append(l.messages, r.Message)
	return nil
}

func (l *logRecorder) WithAttrs([]slog.Attr) slog.Handler { return l }

func (l *logRecorder) WithGroup(string) slog.Handler { return l }

// count returns how many times message was logged.
func (l *logRecorder) count(message string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	n := 0
	for _, m := range l.messages {
		if m == message {
			n++
		}
	}
	return n
}

// listenerPID returns the process ID of the backend that listens for the
// env's notifications, or 0 if there is none.
func (e *env) listenerPID(t *testing.T) int {
	t.Helper()
	return e.count(t, "SELECT coalesce(max(pid), 0) FROM pg_stat_activity WHERE query = 'LISTEN ' || $1", e.schema)
}

// waitListening waits until a worker listens for the env's notifications.
func (e *env) waitListening(t *testing.T) {
	t.Helper()
	waitFor(t, "a worker to listen", func() bool { return e.listenerPID(t) != 0 })
}

// recv receives from ch, and fails the test if nothing arrives in time.
func recv[T any](t *testing.T, ch <-chan T, what string) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(15 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
		panic("unreachable")
	}
}

// setDefaultQueue changes the default queue's settings, and fails the test
// on error.
func (e *env) setDefaultQueue(t *testing.T, settings *taskmill.QueueSettings) {
	t.Helper()
	if err := e.client.UpdateQueue(t.Context(), taskmill.DefaultQueue, settings); err != nil {
		t.Fatalf("update the default queue: %v", err)
	}
}
