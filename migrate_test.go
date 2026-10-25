package taskmill_test

import (
	"errors"
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
	if n := e.count(t, "SELECT count(*) FROM {s}.migrations"); n != 1 {
		t.Fatalf("migrations recorded: got %d, want 1", n)
	}
	if n := e.count(t, "SELECT count(*) FROM {s}.settings"); n != 1 {
		t.Fatalf("settings rows: got %d, want 1", n)
	}
	partitions := e.count(t, "SELECT count(*) FROM pg_inherits WHERE inhparent = '{s}.task_history'::regclass")
	if partitions != 7 {
		t.Fatalf("history partitions: got %d, want 7", partitions)
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

	ms, err := taskmill.Migrations(schema)
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range ms {
		if _, err := pool.Exec(t.Context(), m.SQL); err != nil {
			t.Fatalf("apply migration %d: %v", m.Version, err)
		}
		if m.Version > 0 {
			_, err := pool.Exec(t.Context(), "INSERT INTO "+schema+".migrations (version, name) VALUES ($1, $2)", m.Version, m.Name)
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	if _, err := taskmill.NewClient(t.Context(), pool, &taskmill.ClientConfig{Schema: schema}); err != nil {
		t.Fatalf("NewClient after external migrations: %v", err)
	}
}

func versions(ms []taskmill.Migration) []int {
	out := make([]int, len(ms))
	for i, m := range ms {
		out[i] = m.Version
	}
	return out
}
