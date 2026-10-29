package taskmill_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/code19m/taskmill"
	"github.com/code19m/taskmill/internal/testdb"
)

func TestNewClientRequiresMigratedSchema(t *testing.T) {
	t.Parallel()
	pool := testdb.Pool(t)
	schema := testdb.Schema(t, pool)

	_, err := taskmill.NewClient(t.Context(), pool, &taskmill.ClientConfig{Schema: schema})
	if !errors.Is(err, taskmill.ErrSchemaOutdated) {
		t.Fatalf("NewClient before Migrate: got %v, want ErrSchemaOutdated", err)
	}

	if err := taskmill.Migrate(t.Context(), pool, &taskmill.MigrateConfig{Schema: schema}); err != nil {
		t.Fatal(err)
	}
	if _, err := taskmill.NewClient(t.Context(), pool, &taskmill.ClientConfig{Schema: schema}); err != nil {
		t.Fatalf("NewClient after Migrate: %v", err)
	}
}

func TestMigrateIsIdempotentAndConcurrencySafe(t *testing.T) {
	t.Parallel()
	pool := testdb.PoolSize(t, 16)
	schema := testdb.Schema(t, pool)

	var wg sync.WaitGroup
	errs := make([]error, 8)
	for i := range errs {
		wg.Go(func() {
			errs[i] = taskmill.Migrate(t.Context(), pool, &taskmill.MigrateConfig{Schema: schema})
		})
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("concurrent Migrate %d: %v", i, err)
		}
	}

	e := &env{pool: pool, schema: schema}
	all, err := taskmill.Migrations(schema)
	if err != nil {
		t.Fatal(err)
	}
	if n, want := e.count(t, "SELECT count(*) FROM {s}.migrations"), len(all)-1; n != want { // all but the bootstrap
		t.Fatalf("migrations recorded: got %d, want %d", n, want)
	}
	if n := e.count(t, "SELECT count(*) FROM {s}.settings"); n != 1 {
		t.Fatalf("settings rows: got %d, want 1", n)
	}
	partitions := e.count(t, "SELECT count(*) FROM pg_inherits WHERE inhparent = '{s}.task_history'::regclass")
	if partitions != 7 {
		t.Fatalf("history partitions: got %d, want 7", partitions)
	}
}

func TestMigrateWithARoleThatOnlyOwnsItsSchema(t *testing.T) {
	t.Parallel()
	admin := testdb.Pool(t)
	schema := testdb.Schema(t, admin)
	role := schema + "_owner"
	ctx := t.Context()
	// The role may connect, but not create schemas: an administrator
	// created its schema for it.
	pool := newRole(t, admin, role, "CREATE SCHEMA "+schema+" AUTHORIZATION "+role)

	// The schema exists and its migrations are due, as on install and on
	// every release with a new migration.
	if err := taskmill.Migrate(ctx, pool, &taskmill.MigrateConfig{Schema: schema}); err != nil {
		t.Fatalf("migrate as the schema's owner: %v", err)
	}
	if _, err := taskmill.NewClient(ctx, pool, &taskmill.ClientConfig{Schema: schema}); err != nil {
		t.Fatalf("new client: %v", err)
	}
}

func TestConcurrentMigratesOnWarmConnections(t *testing.T) {
	t.Parallel()
	const processes = 8
	pool := testdb.PoolSize(t, processes)
	// Open every connection first, as a running application has, so that
	// the migrations start together rather than as each connection opens.
	conns := make([]*pgxpool.Conn, processes)
	for i := range conns {
		c, err := pool.Acquire(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		conns[i] = c
	}
	for _, c := range conns {
		c.Release()
	}

	for range 5 {
		schema := testdb.Schema(t, pool)
		var wg sync.WaitGroup
		errs := make([]error, processes)
		for i := range errs {
			wg.Go(func() {
				errs[i] = taskmill.Migrate(t.Context(), pool, &taskmill.MigrateConfig{Schema: schema})
			})
		}
		wg.Wait()
		for i, err := range errs {
			if err != nil {
				t.Fatalf("concurrent Migrate %d: %v", i, err)
			}
		}
	}
}

func TestMigrationsForExternalTools(t *testing.T) {
	t.Parallel()
	ms, err := taskmill.Migrations("billing")
	if err != nil {
		t.Fatal(err)
	}
	if len(ms) < 2 || ms[0].Version != 0 || ms[1].Version != 1 {
		t.Fatalf("got versions %v, want the bootstrap then version 1", versions(ms))
	}
	for _, m := range ms {
		if strings.Contains(m.SQL, "__SCHEMA__") || strings.Contains(m.SQL, "taskmill.") {
			t.Fatalf("migration %d still names a placeholder or the default schema", m.Version)
		}
		if !strings.Contains(m.SQL, "billing.") {
			t.Fatalf("migration %d does not use the requested schema", m.Version)
		}
	}

	if _, err := taskmill.Migrations("Bad-Name"); !errors.Is(err, taskmill.ErrInvalidArgument) {
		t.Fatalf("invalid schema: got %v, want ErrInvalidArgument", err)
	}
}

func TestMigrationsApplyWithoutMigrate(t *testing.T) {
	t.Parallel()
	pool := testdb.Pool(t)
	schema := testdb.Schema(t, pool)
	applyWithoutMigrate(t, pool, schema)
	logs := &logRecorder{}
	e := (&env{pool: pool, schema: schema}).withLogger(t, slog.New(logs))

	// The schema has no history partitions yet, and no maintainer runs. The
	// worker's first task still completes into the history, where Get finds
	// it.
	id := e.enqueue(t, sendEmail{}, nil)
	runWorker(t, e, &taskmill.WorkerConfig{Handlers: noopHandlers(), NoMaintainer: true})
	e.waitState(t, id, taskmill.StateCompleted)
	for _, r := range logs.all() {
		if r.Level >= slog.LevelError {
			t.Errorf("logged %s %q", r.Level, r.Message)
		}
	}
}

func TestWorkerStartsWhenItsRoleMayNotCreatePartitions(t *testing.T) {
	t.Parallel()
	admin := testdb.Pool(t)
	schema := testdb.Schema(t, admin)
	applyWithoutMigrate(t, admin, schema)
	// The role may use the tables, but does not own them.
	role := schema + "_worker"
	pool := newRole(t, admin, role,
		"GRANT USAGE ON SCHEMA "+schema+" TO "+role,
		"GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA "+schema+" TO "+role,
		"GRANT USAGE ON ALL SEQUENCES IN SCHEMA "+schema+" TO "+role)
	logs := &logRecorder{}
	e := (&env{pool: pool, schema: schema}).withLogger(t, slog.New(logs))

	e.enqueue(t, sendEmail{}, nil)
	runWorker(t, e, &taskmill.WorkerConfig{Handlers: noopHandlers(), NoMaintainer: true})
	waitFor(t, "the task to complete without history", func() bool {
		return e.count(t, "SELECT count(*) FROM {s}.tasks") == 0
	})
	var warnings []string
	for _, r := range logs.all() {
		if r.Message == "prepare history partitions failed" {
			r.Attrs(func(a slog.Attr) bool {
				if a.Key == "error" {
					warnings = append(warnings, a.Value.String())
				}
				return true
			})
		}
	}
	if len(warnings) != 1 || !strings.Contains(warnings[0], "the role that runs workers must own the taskmill tables") {
		t.Fatalf("got warnings %q, want one that names the fix", warnings)
	}
}

// applyWithoutMigrate applies the schema the way teams with their own
// migration tools do: each migration from Migrations in a transaction, and
// the record of its version.
func applyWithoutMigrate(t *testing.T, pool *pgxpool.Pool, schema string) {
	t.Helper()
	ms, err := taskmill.Migrations(schema)
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range ms {
		sql := m.SQL
		if m.Version > 0 {
			sql += fmt.Sprintf("\nINSERT INTO %s.migrations (version, name) VALUES (%d, '%s');", schema, m.Version, m.Name)
		}
		if _, err := pool.Exec(t.Context(), sql); err != nil {
			t.Fatalf("apply migration %d: %v", m.Version, err)
		}
	}
}

// newRole creates a login role, runs setup as admin, and returns a pool that
// connects as the role. The role is dropped when the test ends.
func newRole(t *testing.T, admin *pgxpool.Pool, role string, setup ...string) *pgxpool.Pool {
	t.Helper()
	if _, err := admin.Exec(t.Context(), "CREATE ROLE "+role+" LOGIN PASSWORD 'role'"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = admin.Exec(context.Background(), "DROP OWNED BY "+role+"; DROP ROLE "+role)
	})
	for _, sql := range setup {
		if _, err := admin.Exec(t.Context(), sql); err != nil {
			t.Fatal(err)
		}
	}
	cfg, err := pgxpool.ParseConfig(testdb.URL(t))
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.User, cfg.ConnConfig.Password = role, "role"
	pool, err := pgxpool.NewWithConfig(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func versions(ms []taskmill.Migration) []int {
	out := make([]int, len(ms))
	for i, m := range ms {
		out[i] = m.Version
	}
	return out
}
