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
		if err := ensureHistoryPartitions(ctx, pool, schema, time.Now(), historyDaysAhead); err != nil {
			logger.LogAttrs(ctx, slog.LevelWarn, "prepare history partitions failed", slog.Any(logKeyError, err))
		}
		return nil
	}
	if err := pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		return applyMigrations(ctx, tx, schema, all, logger)
	}); err != nil {
		return fmt.Errorf("taskmill: migrate schema %q: %w", schema, err)
	}
	if err := ensureHistoryPartitions(ctx, pool, schema, time.Now(), historyDaysAhead); err != nil {
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

// applyMigrations applies the migrations that the schema lacks, in tx. One
// process migrates at a time; the others wait for its lock, then find nothing
// left to do. A transaction-scoped lock works behind transaction-pooling
// proxies.
func applyMigrations(ctx context.Context, tx pgx.Tx, schema string, all []migrations.Migration, logger *slog.Logger) error {
	if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", "taskmill.migrate."+schema); err != nil {
		return err
	}
	// Waiting for the lock does not make the connection read the catalog
	// changes that the lock's holder committed meanwhile. Its cache can still
	// hold that the schema does not exist, as schemaVersion found, and IF NOT
	// EXISTS would then create the schema again and fail. Reading a catalog
	// table makes the connection apply those changes first.
	if _, err := tx.Exec(ctx, "SELECT FROM pg_catalog.pg_namespace LIMIT 0"); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, bootstrapSQL(schema)); err != nil {
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

// bootstrapSQL creates the schema and the table that records migrations.
func bootstrapSQL(schema string) string {
	return fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS %[1]s;
CREATE TABLE IF NOT EXISTS %[1]s.migrations (
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
// their own migration tools apply these instead of calling [Migrate]. The
// maintainer then creates the daily history partitions when workers start,
// which needs the workers' role to own the taskmill tables.
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

// ensureHistoryPartitions creates the daily history partitions from day for
// the given number of days, skipping those that exist. Each partition is
// created detached and then attached, which does not block completions the
// way CREATE TABLE ... PARTITION OF would. An advisory lock keeps processes
// that do this at the same time from colliding.
func ensureHistoryPartitions(ctx context.Context, pool *pgxpool.Pool, schema string, day time.Time, days int) error {
	err := pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", "taskmill.partitions."+schema); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, "SET LOCAL lock_timeout = '"+partitionLockTimeout+"'"); err != nil {
			return err
		}

		existing, err := historyPartitions(ctx, tx, schema)
		if err != nil {
			return err
		}

		start := day.UTC().Truncate(24 * time.Hour)
		for i := range days {
			from := start.AddDate(0, 0, i)
			name := historyPartition(from)
			if existing[name] {
				continue
			}
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
	if err != nil {
		return fmt.Errorf("prepare history partitions: %w", err)
	}
	return nil
}

// historyPartitions returns the names of the attached history partitions.
func historyPartitions(ctx context.Context, tx pgx.Tx, schema string) (map[string]bool, error) {
	r, err := tx.Query(ctx, `
SELECT c.relname FROM pg_inherits i
JOIN pg_class c ON c.oid = i.inhrelid
WHERE i.inhparent = $1::regclass`, schema+".task_history")
	if err != nil {
		return nil, err
	}
	names, err := pgx.CollectRows(r, pgx.RowTo[string])
	if err != nil {
		return nil, err
	}
	existing := make(map[string]bool, len(names))
	for _, n := range names {
		existing[n] = true
	}
	return existing, nil
}

// historyPartition names the history partition of the UTC day that starts at
// day.
func historyPartition(day time.Time) string {
	return "task_history_" + day.UTC().Format("20060102")
}
