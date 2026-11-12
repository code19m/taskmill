package taskmill

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/code19m/taskmill/internal/migrations"
)

// historyDaysAhead is how many days of history partitions are kept ready, so
// that completions never wait for a partition.
const historyDaysAhead = 7

// partitionLockTimeout bounds the wait for the history table's lock while
// attaching a partition, so that a long transaction cannot stall completions
// behind it.
const partitionLockTimeout = "5s"

// MigrateConfig configures [Migrate]. The zero value is valid.
type MigrateConfig struct {
	// Schema is the PostgreSQL schema to migrate. It must match
	// ^[a-z_][a-z0-9_]{0,62}$. Default: "taskmill".
	Schema string

	// Logger receives one log line per applied migration, and a warning if
	// partitions cannot be prepared. Default: logs are discarded.
	Logger *slog.Logger
}

// Migrate creates or upgrades taskmill's schema, and prepares the next week's
// history partitions. It is safe to run from several processes at once: one
// applies the migrations while the others wait. A schema that is up to date
// is left as it is, so Migrate then needs no privilege to change it, and a
// failure to prepare partitions is only logged. cfg may be nil.
func Migrate(ctx context.Context, pool *pgxpool.Pool, cfg *MigrateConfig) error {
	if pool == nil {
		return invalidArgument("pool is nil")
	}
	if cfg == nil {
		cfg = &MigrateConfig{}
	}
	schema, err := validateSchema(cfg.Schema)
	if err != nil {
		return err
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}

	all, err := migrations.All(schema)
	if err != nil {
		return fmt.Errorf("taskmill: %w", err)
	}

	current, err := schemaVersion(ctx, pool, schema)
	if err != nil {
		return fmt.Errorf("taskmill: migrate schema %q: %w", schema, err)
	}
	if current >= len(all) {
		// The maintainer keeps the partitions ready too, so a role that may not
		// create them still starts.
		if err := ensureHistoryPartitions(ctx, pool, schema); err != nil {
			logger.LogAttrs(ctx, slog.LevelWarn, "prepare history partitions failed", slog.Any(logKeyError, err))
		}
		return nil
	}
	if err := createMigrationsTable(ctx, pool, schema); err != nil {
		return fmt.Errorf("taskmill: migrate schema %q: %w", schema, err)
	}
	if err := pgx.BeginTxFunc(ctx, pool, readCommitted, func(tx pgx.Tx) error {
		return applyMigrations(ctx, tx, schema, all, logger)
	}); err != nil {
		return fmt.Errorf("taskmill: migrate schema %q: %w", schema, err)
	}
	if err := ensureHistoryPartitions(ctx, pool, schema); err != nil {
		return fmt.Errorf("taskmill: migrate schema %q: %w", schema, err)
	}
	return nil
}

// schemaVersion returns the schema's migration version, or 0 if it has none.
func schemaVersion(ctx context.Context, pool *pgxpool.Pool, schema string) (int, error) {
	var version int
	sql := fmt.Sprintf("SELECT coalesce(max(version), 0) FROM %s.migrations", schema)
	err := pool.QueryRow(ctx, sql, unprepared).Scan(&version)
	if pgErr, ok := errors.AsType[*pgconn.PgError](err); ok && pgErr.Code == "42P01" { // undefined_table
		return 0, nil
	}
	return version, err
}

// unprepared runs a statement without preparing it by name. Statements that
// can fail to parse, such as reads of a table that may not exist yet, use it:
// PgBouncer keeps the name of a statement whose parse failed, and disconnects
// the client when it prepares that name again
// (https://github.com/pgbouncer/pgbouncer/issues/1630).
const unprepared = pgx.QueryExecModeSimpleProtocol

// readCommitted begins a transaction that sees, after it waited for another
// process, what that process committed. Migrate cannot count on the pool's
// default level: only NewClient checks it, later.
var readCommitted = pgx.TxOptions{IsoLevel: pgx.ReadCommitted}

// createMigrationsTable creates the schema and the table that records
// migrations, unless they exist, in a transaction of its own. Processes that
// do this at the same time need no lock: the one that comes second waits
// until the first commits, and then finds both or fails because one exists,
// with an error that depends on the moment. So after an error, the table
// decides: if it exists, another process created it.
func createMigrationsTable(ctx context.Context, pool *pgxpool.Pool, schema string) error {
	createErr := pgx.BeginTxFunc(ctx, pool, readCommitted, func(tx pgx.Tx) error {
		// CREATE SCHEMA IF NOT EXISTS would not do. PostgreSQL checks the
		// CREATE privilege on the database before it checks whether the
		// schema exists, and a role that owns its schema may lack it.
		var exists bool
		if err := tx.QueryRow(ctx, "SELECT EXISTS (SELECT FROM pg_catalog.pg_namespace WHERE nspname = $1)", schema).Scan(&exists); err != nil {
			return err
		}
		if !exists {
			if _, err := tx.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
				return err
			}
		}
		_, err := tx.Exec(ctx, migrationsTableSQL(schema))
		return err
	})
	if createErr == nil {
		return nil
	}
	var created bool
	if err := pool.QueryRow(ctx, "SELECT to_regclass($1) IS NOT NULL", schema+".migrations").Scan(&created); err != nil || !created {
		return createErr
	}
	return nil
}

// applyMigrations applies the migrations that the schema lacks, in tx. One
// process migrates at a time: it locks the migrations table, and the others
// wait for that lock, then find nothing left to do. Only a role that may
// change the table can take this lock, so a role without rights on the
// schema cannot keep Migrate waiting, as it could with an advisory lock,
// which any role may take. The lock lets everyone read the table, and ends
// with tx, so it works behind transaction-pooling proxies.
func applyMigrations(ctx context.Context, tx pgx.Tx, schema string, all []migrations.Migration, logger *slog.Logger) error {
	if _, err := tx.Exec(ctx, fmt.Sprintf("LOCK TABLE %s.migrations IN SHARE ROW EXCLUSIVE MODE", schema)); err != nil {
		return err
	}

	var current int
	if err := tx.QueryRow(ctx, fmt.Sprintf("SELECT coalesce(max(version), 0) FROM %s.migrations", schema)).Scan(&current); err != nil {
		return err
	}
	for _, m := range all[min(current, len(all)):] {
		start := time.Now()
		if _, err := tx.Exec(ctx, m.SQL); err != nil {
			return fmt.Errorf("migration %04d_%s: %w", m.Version, m.Name, err)
		}
		insert := fmt.Sprintf("INSERT INTO %s.migrations (version, name) VALUES ($1, $2)", schema)
		if _, err := tx.Exec(ctx, insert, m.Version, m.Name); err != nil {
			return err
		}
		logger.LogAttrs(ctx, slog.LevelInfo, "applied migration",
			slog.Int(logKeyVersion, m.Version), slog.String(logKeyName, m.Name),
			slog.Duration(logKeyDuration, time.Since(start)))
	}
	return nil
}

// bootstrapSQL creates the schema and the table that records migrations,
// for tools other than Migrate.
func bootstrapSQL(schema string) string {
	return fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s;\n", schema) + migrationsTableSQL(schema)
}

// migrationsTableSQL creates the table that records migrations.
func migrationsTableSQL(schema string) string {
	return fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %[1]s.migrations (
    version    integer PRIMARY KEY,
    name       text NOT NULL,
    applied_at timestamptz NOT NULL DEFAULT now()
);
`, schema)
}

// Migration is one versioned schema change, for teams that apply migrations
// with their own tools. [Migrations] returns them in order: first the
// bootstrap, version 0, which creates the schema and the migrations table,
// then every versioned migration. Apply them in order, each in a
// transaction, and record every version above 0, so that [NewClient] finds
// the schema up to date:
//
//	INSERT INTO <schema>.migrations (version, name) VALUES (<Version>, '<Name>')
type Migration struct {
	Version int
	Name    string
	SQL     string
}

// Migrations returns every migration for schema, in order. Teams that use
// their own migration tools apply these instead of calling [Migrate]. They
// create no history partitions: each worker creates them when it starts, and
// the maintainer keeps them ready, which needs the workers' role to own the
// taskmill tables.
func Migrations(schema string) ([]Migration, error) {
	schema, err := validateSchema(schema)
	if err != nil {
		return nil, err
	}
	all, err := migrations.All(schema)
	if err != nil {
		return nil, fmt.Errorf("taskmill: %w", err)
	}
	bootstrap := Migration{Version: 0, Name: "bootstrap", SQL: bootstrapSQL(schema)}
	out := make([]Migration, 0, len(all)+1)
	out = append(out, bootstrap)
	for _, m := range all {
		out = append(out, Migration{Version: m.Version, Name: m.Name, SQL: m.SQL})
	}
	return out, nil
}

// ensureHistoryPartitions creates the daily history partitions of the next
// historyDaysAhead days, today's first, skipping those that exist. Each
// partition is created detached and then attached, which does not block
// completions the way CREATE TABLE ... PARTITION OF would. An error for a
// missing privilege names the fix.
//
// A call that finds every partition ready takes no lock. Otherwise it first
// takes the lock on the history table that attaching takes anyway, so that
// processes that do this at the same time wait for each other, for at most
// partitionLockTimeout. Only a role that may change the table can take this
// lock, so a role without rights on the schema cannot keep workers from
// starting, as it could with an advisory lock, which any role may take.
func ensureHistoryPartitions(ctx context.Context, pool *pgxpool.Pool, schema string) error {
	err := pgx.BeginTxFunc(ctx, pool, readCommitted, func(tx pgx.Tx) error {
		days, err := missingHistoryDays(ctx, tx, schema)
		if err != nil || len(days) == 0 {
			return err
		}
		if _, err := tx.Exec(ctx, "SET LOCAL lock_timeout = '"+partitionLockTimeout+"'"); err != nil {
			return err
		}
		// ONLY: the partitions stay free for autovacuum.
		lock := fmt.Sprintf("LOCK TABLE ONLY %s.task_history IN SHARE UPDATE EXCLUSIVE MODE", schema)
		if _, err := tx.Exec(ctx, lock); err != nil {
			return err
		}
		// Another process may have created them while this one waited.
		if days, err = missingHistoryDays(ctx, tx, schema); err != nil {
			return err
		}
		for _, from := range days {
			name := historyPartition(from)
			create := fmt.Sprintf("CREATE TABLE %[1]s.%[2]s (LIKE %[1]s.task_history INCLUDING DEFAULTS INCLUDING CONSTRAINTS)",
				schema, name)
			attach := fmt.Sprintf("ALTER TABLE %[1]s.task_history ATTACH PARTITION %[1]s.%[2]s FOR VALUES FROM ('%[3]s') TO ('%[4]s')",
				schema, name, from.Format(time.RFC3339), from.AddDate(0, 0, 1).Format(time.RFC3339))
			if _, err := tx.Exec(ctx, create); err != nil {
				return fmt.Errorf("create %s: %w", name, err)
			}
			if _, err := tx.Exec(ctx, attach); err != nil {
				return fmt.Errorf("attach %s: %w", name, err)
			}
		}
		return nil
	})
	if pgErr, ok := errors.AsType[*pgconn.PgError](err); ok && pgErr.Code == "42501" { // insufficient_privilege
		return fmt.Errorf("prepare history partitions: %w; the role that runs workers must own the taskmill tables", err)
	}
	if err != nil {
		return fmt.Errorf("prepare history partitions: %w", err)
	}
	return nil
}

// missingHistoryDays returns the days among the next historyDaysAhead, from
// today, that have no history partition, in order. Today is the database's:
// its clock sets the day of a completed task.
func missingHistoryDays(ctx context.Context, tx pgx.Tx, schema string) ([]time.Time, error) {
	r, err := tx.Query(ctx, `
SELECT d.day
FROM (SELECT (now() AT TIME ZONE 'UTC')::date + i AS day FROM generate_series(0, $2::integer - 1) AS i) d
WHERE NOT EXISTS (
    SELECT FROM pg_inherits h JOIN pg_class c ON c.oid = h.inhrelid
    WHERE h.inhparent = $1::regclass AND c.relname = 'task_history_' || to_char(d.day, 'YYYYMMDD'))
ORDER BY d.day`, schema+".task_history", historyDaysAhead)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(r, pgx.RowTo[time.Time])
}

// historyPartition names the history partition of the UTC day that starts at
// day.
func historyPartition(day time.Time) string {
	return "task_history_" + day.UTC().Format("20060102")
}
