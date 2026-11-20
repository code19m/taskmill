package taskmill

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/code19m/taskmill/internal/migrations"
)

// MigrateConfig configures [Migrate]. The zero value is valid. Migrate
// reports a Schema that is not valid as an error that wraps
// [ErrInvalidArgument].
type MigrateConfig struct {
	// Schema is the PostgreSQL schema to migrate. It must match
	// ^[a-z_][a-z0-9_]{0,62}$. Default: "taskmill".
	Schema string

	// Logger receives one log line per applied migration, and a warning if
	// partitions cannot be prepared. Default: logs are discarded.
	Logger *slog.Logger
}

// Migrate creates or upgrades taskmill's schema, and prepares the next week's
// history partitions. It is safe to run from several processes at the same
// time: one applies the migrations while the others wait. A schema that is up to date
// is left as it is, so Migrate then needs no privilege to change it, and a
// failure to prepare partitions is only logged. cfg may be nil.
//
// The [setup guide] says when to call Migrate, and with which role.
//
// [setup guide]: https://github.com/code19m/taskmill/blob/main/docs/guides/setup.md
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
	fail := func(err error) error { return fmt.Errorf("taskmill: migrate schema %q: %w", schema, err) }
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
		return fail(err)
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
		return fail(err)
	}
	if err := pgx.BeginTxFunc(ctx, pool, readCommitted, func(tx pgx.Tx) error {
		return applyMigrations(ctx, tx, schema, all, logger)
	}); err != nil {
		return fail(err)
	}
	if err := ensureHistoryPartitions(ctx, pool, schema); err != nil {
		return fail(err)
	}
	return nil
}

// schemaVersion returns the schema's migration version, or 0 if it has none.
func schemaVersion(ctx context.Context, pool *pgxpool.Pool, schema string) (int, error) {
	version, err := readVersion(ctx, pool, schema)
	if pgCode(err) == codeUndefinedTable {
		return 0, nil
	}
	return version, err
}

// readVersion reads the version of the latest migration that the schema's
// migrations table records, on q, a pool or a transaction. NewClient reads
// it with the pool's isolation level instead: see schemaVersionSQL.
func readVersion(ctx context.Context, q interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}, schema string,
) (int, error) {
	var version int
	err := q.QueryRow(ctx, "SELECT coalesce(max(version), 0) FROM "+schema+".migrations", unprepared).Scan(&version)
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
//
// Only this wait has no limit but ctx: no worker writes the migrations
// table, so it waits for another migration alone. For the tables that
// workers use, each migration sets a lock timeout before its first
// statement, which holds until tx ends.
func applyMigrations(ctx context.Context, tx pgx.Tx, schema string, all []migrations.Migration, logger *slog.Logger) error {
	if _, err := tx.Exec(ctx, fmt.Sprintf("LOCK TABLE %s.migrations IN SHARE ROW EXCLUSIVE MODE", schema)); err != nil {
		return err
	}

	current, err := readVersion(ctx, tx, schema)
	if err != nil {
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
// taskmill tables. An empty schema means "taskmill". A name that does not
// match the pattern of [MigrateConfig.Schema] is an error that wraps
// [ErrInvalidArgument].
//
// The [recipe for migration tools] shows the steps with goose, Atlas, and
// Flyway.
//
// [recipe for migration tools]: https://github.com/code19m/taskmill/blob/main/docs/recipes/use-your-migration-tool.md
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
