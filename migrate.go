package taskmill

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
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

	// MaxBlockingIndexRows is the most rows, by PostgreSQL's estimate, that
	// a pending migration may read while it builds an index that blocks
	// writes to the table: migration 3 on groups, 7 on tasks and the
	// history, and 9 on tasks. Writes wait about half a second for each
	// million rows that migration 7 reads. Over the limit, Migrate fails
	// before it changes anything, with an error that wraps
	// [ErrBlockingMigration]: run it as a job of its own, at a quiet time,
	// with the limit raised. A negative value removes the limit. Default:
	// 1,000,000, which keeps the wait well under the 5 seconds for which a
	// migration may wait for a lock. Later migrations build their indexes
	// online, which blocks no writes.
	MaxBlockingIndexRows int64
}

// defaultMaxBlockingIndexRows is the default of
// MigrateConfig.MaxBlockingIndexRows.
const defaultMaxBlockingIndexRows = 1_000_000

// Migrate creates or upgrades taskmill's schema, and prepares the next week's
// history partitions. It is safe to run from several processes at the same
// time: one applies the migrations while the others wait. A schema that is up to date
// is left as it is, so Migrate then needs no privilege to change it, and a
// failure to prepare partitions is only logged. cfg may be nil.
//
// Migrate fills an existing schema only if it trusts the role that owns the
// schema, which can drop or replace every table in it, and the role that
// owns its migrations table, which could have created objects there first:
// their objects would run with the rights of the role that the pool connects
// as. It trusts a superuser, and a role that has the rights of the pool's
// role, such as that role itself. pg_database_owner, which owns the schema
// public, stands for the database's owner. A role set on the connection,
// with SET ROLE, SET SESSION AUTHORIZATION or the role parameter, does not
// count, since code that runs as that role can switch back, and must be
// trusted too, since it would own what Migrate creates. Otherwise Migrate
// fails with PostgreSQL's insufficient_privilege, before it changes
// anything. Of a schema that is up to date, it only reads the migrations
// table: a superuser, or a member of one, only one whose owners it trusts,
// and other roles one of a role that they trust or of the schema's owner, so
// that roles that own nothing can call it too. So run Migrate connected as
// the role that owns taskmill's tables, and give the CREATE privilege on the
// database only to roles that you trust: one could create taskmill's schema
// first, and fill it.
//
// A migration that builds an index does so online, without blocking writes,
// which can take minutes on a large table. Meanwhile Migrate keeps a second
// connection of the pool, so the pool needs at least 2, and its connections
// may have no lock_timeout, statement_timeout, or transaction_timeout,
// which would cancel the build. Otherwise Migrate fails before the build,
// with an error that wraps [ErrInvalidArgument]. An older migration that
// blocks writes while it builds an index is applied only on tables that
// [MigrateConfig.MaxBlockingIndexRows] allows.
//
// The [setup guide] says when to call Migrate, and with which role.
//
// [setup guide]: https://github.com/code19m/taskmill/blob/main/docs/guides/setup.md
func Migrate(ctx context.Context, pool *pgxpool.Pool, cfg *MigrateConfig) error {
	m, err := newMigrator(pool, cfg)
	if err != nil {
		return err
	}
	return m.run(ctx)
}

// migrator applies migrations to a schema.
type migrator struct {
	pool    *pgxpool.Pool
	schema  string
	logger  *slog.Logger
	maxRows int64 // MigrateConfig.MaxBlockingIndexRows, or below 0 for no limit
	all     []migrations.Migration
}

func newMigrator(pool *pgxpool.Pool, cfg *MigrateConfig) (*migrator, error) {
	if pool == nil {
		return nil, invalidArgument("pool is nil")
	}
	if cfg == nil {
		cfg = &MigrateConfig{}
	}
	schema, err := validateSchema(cfg.Schema)
	if err != nil {
		return nil, err
	}

	all, err := migrations.All(schema)
	if err != nil {
		return nil, fmt.Errorf("taskmill: %w", err)
	}

	m := &migrator{pool: pool, schema: schema, logger: cfg.Logger, maxRows: cfg.MaxBlockingIndexRows, all: all}
	if m.logger == nil {
		m.logger = slog.New(slog.DiscardHandler)
	}
	if m.maxRows == 0 {
		m.maxRows = defaultMaxBlockingIndexRows
	}
	return m, nil
}

func (m *migrator) run(ctx context.Context) error {
	fail := func(err error) error { return fmt.Errorf("taskmill: migrate schema %q: %w", m.schema, err) }
	current, err := schemaVersion(ctx, m.pool, m.schema)
	if err != nil {
		return fail(err)
	}

	if current >= len(m.all) {
		// The maintainer keeps the partitions ready too, so a role that may not
		// create them still starts.
		if err := ensureHistoryPartitions(ctx, m.pool, m.schema); err != nil {
			m.logger.LogAttrs(ctx, slog.LevelWarn, "prepare history partitions failed", slog.Any(logKeyError, err))
		}
		return nil
	}

	if err := createMigrationsTable(ctx, m.pool, m.schema); err != nil {
		return fail(err)
	}
	for done := false; !done; {
		if done, err = m.step(ctx); err != nil {
			return fail(err)
		}
	}
	if err := ensureHistoryPartitions(ctx, m.pool, m.schema); err != nil {
		return fail(err)
	}
	return nil
}

// schemaVersion returns the schema's migration version, or 0 if it has
// none, or if the bootstrap must first check who owns its migrations table.
//
// Reading a relation can run code of its owner's, as a view's condition
// does, with the rights of the role that reads it. So a superuser session
// reads only a migrations table whose owners the bootstrap would accept, and
// another session one that belongs to the schema's owner or to a role that
// it trusts. So a role that owns nothing reads a schema that is up to date:
// its owner can run code as that role anyway, whenever it uses taskmill's
// tables, which that owner can replace. schemaVersion leaves the rest to the
// bootstrap, also a table that the current role may not read, so that the
// bootstrap names the role to connect as if it does not trust an owner. One
// statement checks the owners, so that no table appears between two checks;
// only those owners could replace the table before the read.
func schemaVersion(ctx context.Context, pool *pgxpool.Pool, schema string) (int, error) {
	var version int
	err := pgx.BeginTxFunc(ctx, pool, readCommitted, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, safeSearchPathSQL); err != nil {
			return err
		}
		var readable bool
		err := tx.QueryRow(ctx, "SELECT EXISTS (SELECT FROM pg_catalog.pg_namespace n "+
			"JOIN pg_catalog.pg_class c ON c.relnamespace = n.oid AND c.relname = 'migrations' WHERE n.nspname = $1 AND "+
			"pg_catalog.has_schema_privilege(n.oid, 'USAGE') AND pg_catalog.has_table_privilege(c.oid, 'SELECT') AND "+
			"CASE WHEN "+superuserSQL+" THEN "+trustedSQL(roleSQL("n.nspowner"))+" AND "+trustedSQL(roleSQL("c.relowner"))+
			" ELSE "+roleSQL("c.relowner")+" = "+roleSQL("n.nspowner")+" OR "+trustedSQL(roleSQL("c.relowner"))+" END)",
			unprepared, schema).Scan(&readable)
		if err != nil || !readable {
			return err
		}
		version, err = readVersion(ctx, tx, schema)
		return err
	})
	if pgCode(err) == codeUndefinedTable { // dropped since the check
		return 0, nil
	}
	return version, err
}

// safeSearchPathSQL makes the statements after it in the transaction find
// PostgreSQL's functions and operators, and no other role's, whatever the
// session's search path.
const safeSearchPathSQL = "SELECT pg_catalog.set_config('search_path', 'pg_catalog, pg_temp', true)"

// readVersion reads the version of the latest migration that the schema's
// migrations table records, on q, a pool or a transaction. NewClient reads
// it with the pool's isolation level instead: see schemaVersionSQL.
func readVersion(ctx context.Context, q interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}, schema string,
) (int, error) {
	var version int
	err := q.QueryRow(ctx, "SELECT coalesce(pg_catalog.max(version), 0) FROM "+schema+".migrations", unprepared).Scan(&version)
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

// createMigrationsTable runs the bootstrap, which creates the schema and
// the table that records migrations, unless they exist, in a transaction of
// its own. Processes that do this at the same time need no lock: the one
// that comes second waits until the first commits, and then finds both or
// fails because one exists. So after an error it runs the bootstrap once
// more, which then finds both, and checks their owners.
func createMigrationsTable(ctx context.Context, pool *pgxpool.Pool, schema string) error {
	bootstrap := func() error {
		return pgx.BeginTxFunc(ctx, pool, readCommitted, func(tx pgx.Tx) error {
			_, err := tx.Exec(ctx, bootstrapSQL(schema))
			return err
		})
	}
	if bootstrap() == nil {
		return nil
	}
	return bootstrap()
}

// step applies the pending migrations up to the next online step, or that
// online step, in a transaction that holds the lock of the migrations table.
// It reports whether no migration is left.
//
// One process migrates at a time: the others wait for the lock, then find
// nothing left to do. It is a table lock, not an advisory lock, which any
// role may take: see the page on schema migrations. Only this wait has no
// limit but ctx, since it waits for another migration alone.
func (m *migrator) step(ctx context.Context) (done bool, err error) {
	tx, err := m.lock(ctx)
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback(ctx) }() // after a commit, it does nothing

	current, err := readVersion(ctx, tx, m.schema)
	if err != nil {
		return false, err
	}
	pending := m.all[min(current, len(m.all)):]
	if len(pending) == 0 {
		return true, tx.Commit(ctx)
	}
	if err := m.guard(ctx, tx, pending); err != nil {
		return false, err
	}

	applied := 1
	if pending[0].Online {
		err = m.applyOnline(ctx, tx, pending[0])
	} else {
		applied, err = m.applyInTx(ctx, tx, pending)
	}
	if err != nil {
		return false, err
	}
	return applied == len(pending), tx.Commit(ctx)
}

// lockPause is the first pause between two tries of a lock that another
// session holds, and lockPauseMax the longest: each pause doubles the one
// before.
const (
	lockPause    = 20 * time.Millisecond
	lockPauseMax = time.Second
)

// lock begins a transaction that holds the lock of the migrations table.
// It never waits in the lock's queue: a session that waits for a lock keeps
// a snapshot, and an online build of the lock's holder waits until every
// older snapshot is gone, while the holder waits for its build. So it tries
// the lock, and tries again after a pause, until ctx ends. See
// docs/design/schema-migrations.md.
func (m *migrator) lock(ctx context.Context) (pgx.Tx, error) { //nolint:ireturn // pgx's transaction type is an interface
	lock := fmt.Sprintf("LOCK TABLE %s.migrations IN SHARE ROW EXCLUSIVE MODE NOWAIT", m.schema)
	for pause := lockPause; ; pause = min(2*pause, lockPauseMax) {
		tx, err := m.pool.BeginTx(ctx, readCommitted)
		if err != nil {
			return nil, err
		}
		_, err = tx.Exec(ctx, lock)
		if err == nil {
			return tx, nil
		}

		_ = tx.Rollback(ctx)
		if pgCode(err) != codeLockNotAvailable {
			return nil, err
		}
		if pause == lockPause {
			m.logger.LogAttrs(ctx, slog.LevelInfo, "waiting for another process to migrate")
		}
		if err := sleep(ctx, pause); err != nil {
			return nil, err
		}
	}
}

// sleep waits for d, or until ctx ends.
func sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// estimateRowsSQL estimates the rows of the tables $1, and of their
// partitions, as the planner does: the rows for each page that VACUUM and
// ANALYZE last counted, times the pages that the table has now. A table that
// neither has seen yet counts as rows of 100 bytes, fewer than a task takes.
const estimateRowsSQL = `
SELECT coalesce(sum(CASE WHEN c.reltuples >= 0 AND c.relpages > 0
                         THEN c.reltuples / c.relpages * (pg_relation_size(c.oid) / current_setting('block_size')::integer)
                         ELSE pg_relation_size(c.oid) / 100 END), 0)::bigint
FROM pg_class c
WHERE c.relkind = 'r'
  AND (c.oid IN (SELECT to_regclass(t) FROM unnest($1::text[]) AS t)
       OR c.oid IN (SELECT i.inhrelid FROM pg_inherits i
                    WHERE i.inhparent IN (SELECT to_regclass(t) FROM unnest($1::text[]) AS t)))`

// guard fails if a pending migration would build an index that blocks
// writes to tables of more rows than m.maxRows.
func (m *migrator) guard(ctx context.Context, tx pgx.Tx, pending []migrations.Migration) error {
	if m.maxRows < 0 {
		return nil
	}
	for _, mig := range pending {
		if len(mig.Blocking) == 0 {
			continue
		}

		var rows int64
		if err := tx.QueryRow(ctx, estimateRowsSQL, mig.Blocking).Scan(&rows); err != nil {
			return err
		}
		if rows > m.maxRows {
			return fmt.Errorf("%w: migration %04d_%s builds an index that blocks writes to %s while it reads about %d rows, "+
				"more than MigrateConfig.MaxBlockingIndexRows (%d): run Migrate as a job of its own, at a quiet time, "+
				"with that limit raised above the rows; see %s",
				ErrBlockingMigration, mig.Version, mig.Name, strings.Join(mig.Blocking, " and "), rows, m.maxRows, blockingDocs)
		}
	}
	return nil
}

// blockingDocs explains what to do when the guard refuses a migration.
const blockingDocs = "https://github.com/code19m/taskmill/blob/main/docs/production/deploys.md#migrations-that-block-writes"

// applyInTx applies, in tx, the pending migrations up to the first online
// step, and returns how many it applied.
func (m *migrator) applyInTx(ctx context.Context, tx pgx.Tx, pending []migrations.Migration) (applied int, err error) {
	for _, mig := range pending {
		if mig.Online {
			break
		}

		start := time.Now()
		if _, err := tx.Exec(ctx, mig.SQL); err != nil {
			return 0, fmt.Errorf("migration %04d_%s: %w", mig.Version, mig.Name, err)
		}
		if err := m.record(ctx, tx, mig, start); err != nil {
			return 0, err
		}
		applied++
	}
	return applied, nil
}

// record records mig in tx, and logs it.
func (m *migrator) record(ctx context.Context, tx pgx.Tx, mig migrations.Migration, start time.Time) error {
	insert := fmt.Sprintf("INSERT INTO %s.migrations (version, name) VALUES ($1, $2)", m.schema)
	if _, err := tx.Exec(ctx, insert, mig.Version, mig.Name); err != nil {
		return err
	}
	m.logger.LogAttrs(ctx, slog.LevelInfo, "applied migration",
		slog.Int(logKeyVersion, mig.Version), slog.String(logKeyName, mig.Name),
		slog.Duration(logKeyDuration, time.Since(start)))
	return nil
}

// applyOnline runs the online step mig on other connections of the pool,
// one statement at a time, outside a transaction, while tx keeps the lock of
// the migrations table. tx holds no other lock, and no snapshot while it
// waits, so the builds never wait for it. Then it records mig in tx.
func (m *migrator) applyOnline(ctx context.Context, tx pgx.Tx, mig migrations.Migration) error {
	fail := func(err error) error { return fmt.Errorf("migration %04d_%s: %w", mig.Version, mig.Name, err) }
	if m.pool.Stat().MaxConns() < 2 {
		return fail(invalidArgument("it builds an index online, on a second connection, and the pool has only one: let it open at least 2"))
	}

	var timeouts string
	if err := m.pool.QueryRow(ctx, buildTimeoutsSQL).Scan(&timeouts); err != nil {
		return err
	}
	if timeouts != "" {
		return fail(invalidArgument("it builds an index online, which can take minutes, and the connections have %s, "+
			"which would cancel the build and leave an invalid index: run Migrate on connections without them, "+
			"as after ALTER ROLE ... SET statement_timeout = 0", timeouts))
	}

	if _, err := tx.Exec(ctx, keepOpenSQL); err != nil {
		return err
	}

	start := time.Now()
	for _, st := range mig.Statements {
		if err := m.runOnline(ctx, tx, mig, st); err != nil {
			return fail(err)
		}
	}
	return m.record(ctx, tx, mig, start)
}

// buildTimeoutsSQL lists the timeouts of the pool's connections that would
// cancel a build: lock_timeout too, since a build waits for every older
// transaction. It returns an empty string if none is set.
const buildTimeoutsSQL = `
SELECT concat_ws(', ',
    CASE WHEN current_setting('lock_timeout') <> '0' THEN 'lock_timeout = ' || current_setting('lock_timeout') END,
    CASE WHEN current_setting('statement_timeout') <> '0' THEN 'statement_timeout = ' || current_setting('statement_timeout') END,
    CASE WHEN coalesce(current_setting('transaction_timeout', true), '0') <> '0'
         THEN 'transaction_timeout = ' || current_setting('transaction_timeout', true) END)`

// keepOpenSQL keeps the transaction that holds the lock open while the builds
// take minutes: neither its idleness nor, from PostgreSQL 17, its age ends
// it. A proxy may still end it, which runOnline checks.
const keepOpenSQL = `
SELECT set_config('idle_in_transaction_session_timeout', '0', true),
       CASE WHEN current_setting('server_version_num')::integer >= 170000 THEN set_config('transaction_timeout', '0', true) END`

// indexState is the state of an index in the catalog.
type indexState int

const (
	indexMissing indexState = iota
	indexInvalid            // left by a build or a drop that failed or was canceled
	indexValid
)

// runOnline runs st, a statement of the online step mig, until its index is
// valid, or gone for a drop. An earlier Migrate may have left the index
// invalid, when its build failed or was canceled: it is dropped, and built
// again. The lock of the migrations table, in lock, keeps every other
// Migrate from building meanwhile, and awaitIndex waits for a build that
// still runs although its Migrate is gone.
func (m *migrator) runOnline(ctx context.Context, lock pgx.Tx, mig migrations.Migration, st migrations.Statement) error {
	build := st.Table != ""
	for range 3 { // a drop, a build, and a last look
		state, err := m.awaitIndex(ctx, mig, st)
		if err != nil {
			return err
		}
		if build && state == indexValid || !build && state == indexMissing {
			return nil
		}

		// Without the lock, another process may have applied later
		// migrations, such as one that drops this index. A ping takes no
		// snapshot, which a build could wait for.
		if err := lock.Conn().Ping(ctx); err != nil {
			return fmt.Errorf("the lock of the migrations table ended: %w", err)
		}
		stmt := st.SQL
		if build && state == indexInvalid {
			stmt = "DROP INDEX CONCURRENTLY " + st.Index
		}
		if _, err := m.pool.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("index %s: %w", st.Index, err)
		}
	}
	return fmt.Errorf("index %s: not built or dropped after 3 tries", st.Index)
}

// indexSQL returns the version of the schema $3, the state of the index $1,
// and whether a session holds a lock of the index's table $2 that a build
// waits for. Such a session is mostly a build of a Migrate whose process or
// connection is gone. Autovacuum takes such a lock too, and ends its work
// when a build waits for it.
const indexSQL = `
SELECT (SELECT coalesce(max(version), 0) FROM %s.migrations),
       (SELECT indisvalid FROM pg_index WHERE indexrelid = to_regclass($1)),
       EXISTS (SELECT FROM pg_locks l JOIN pg_stat_activity a ON a.pid = l.pid
               WHERE l.locktype = 'relation' AND l.granted AND l.pid <> pg_backend_pid()
                 AND l.relation = coalesce(to_regclass(nullif($2, '')), (SELECT indrelid FROM pg_index WHERE indexrelid = to_regclass($1)))
                 AND l.mode IN ('ShareUpdateExclusiveLock', 'ShareLock', 'ShareRowExclusiveLock', 'ExclusiveLock', 'AccessExclusiveLock')
                 AND a.backend_type <> 'autovacuum worker')`

// awaitIndex returns the state of the index of st, once no other session
// builds an index of its table. A second build that waited for the table
// would make PostgreSQL end one of the two as a deadlock, so awaitIndex
// looks without waiting, and looks again after a pause. It fails if another
// process applied mig meanwhile: then this one lost its lock.
func (m *migrator) awaitIndex(ctx context.Context, mig migrations.Migration, st migrations.Statement) (indexState, error) {
	query := fmt.Sprintf(indexSQL, m.schema)
	logged := false
	for pause := lockPause; ; pause = min(2*pause, lockPauseMax) {
		var version int
		var valid *bool
		var busy bool
		if err := m.pool.QueryRow(ctx, query, st.Index, st.Table).Scan(&version, &valid, &busy); err != nil {
			return 0, err
		}

		switch {
		case version >= mig.Version:
			return 0, errors.New("another process applied it meanwhile: this one lost the lock of the migrations table")
		case !busy && valid == nil:
			return indexMissing, nil
		case !busy && *valid:
			return indexValid, nil
		case !busy:
			return indexInvalid, nil
		case !logged:
			logged = true
			m.logger.LogAttrs(ctx, slog.LevelInfo, "waiting for an index build",
				slog.Int(logKeyVersion, mig.Version), slog.String(logKeyName, mig.Name))
		}
		if err := sleep(ctx, pause); err != nil {
			return 0, err
		}
	}
}

// bootstrapSQL returns a DO block that creates the schema and the table
// that records migrations, unless they exist, for Migrate and for other
// tools. It finds only PostgreSQL's functions and operators, whatever the
// session's search path.
//
// It refuses a schema whose owner the session does not trust, since that
// owner can drop or replace every table in it, or whose migrations table
// belongs to a role that the session does not trust, which could have
// created objects there first. It refuses a current role that the session
// does not trust as well, which would own what Migrate creates. The block
// reads both owners in one statement, so that no schema or table appears
// between two reads.
//
// CREATE SCHEMA IF NOT EXISTS would not do: PostgreSQL checks the CREATE
// privilege on the database before it checks whether the schema exists, and
// a role that owns its schema may lack it. Nor would CREATE TABLE IF NOT
// EXISTS: it would accept a table that another role created after the
// check. The block stays on one line, since goose splits a file at each
// line that ends in a semicolon.
func bootstrapSQL(schema string) string {
	block := `DO $$ DECLARE o pg_catalog.oid; m pg_catalog.oid; p pg_catalog.text := pg_catalog.current_setting('search_path'); BEGIN ` +
		`PERFORM pg_catalog.set_config('search_path', 'pg_catalog, pg_temp', true); ` +
		`SELECT {role(n.nspowner)}, {role(c.relowner)} INTO o, m FROM pg_namespace n ` +
		`LEFT JOIN pg_class c ON c.relnamespace = n.oid AND c.relname = 'migrations' WHERE n.nspname = '{s}'; ` +
		`IF NOT {trusted(current)} THEN ` +
		`RAISE EXCEPTION 'the role set on the connection would own what Migrate creates in schema {s}: connect as role % to migrate it', ` +
		`current_user USING ERRCODE = 'insufficient_privilege'; ` +
		`ELSIF o IS NULL THEN CREATE SCHEMA {s}; ` +
		`ELSIF NOT {trusted(o)} THEN ` +
		`RAISE EXCEPTION 'schema {s} belongs to a role that can drop or replace every table in it: connect as role % to migrate it%', ` +
		`pg_get_userbyid(o), CASE WHEN {super} THEN '' ELSE format(', or make role %I its owner', pg_get_userbyid({login})) END USING ERRCODE = 'insufficient_privilege'; ` +
		`ELSIF m IS NOT NULL AND NOT {trusted(m)} THEN ` +
		`RAISE EXCEPTION 'table {s}.migrations belongs to a role that could run code as role % through it: connect as role % to migrate the schema', ` +
		`pg_get_userbyid({login}), pg_get_userbyid(m) USING ERRCODE = 'insufficient_privilege'; ` +
		`END IF; ` +
		`IF m IS NULL THEN CREATE TABLE {s}.migrations (version integer PRIMARY KEY, name text NOT NULL, applied_at timestamptz NOT NULL DEFAULT now()); END IF; ` +
		`PERFORM pg_catalog.set_config('search_path', p, true); END $$;` + "\n"
	return strings.NewReplacer("{s}", schema, "{super}", superuserSQL, "{login}", loginSQL,
		"{role(n.nspowner)}", roleSQL("n.nspowner"), "{role(c.relowner)}", roleSQL("c.relowner"),
		"{trusted(current)}", trustedSQL("(SELECT oid FROM pg_roles WHERE rolname = current_user)"), "{trusted(o)}", trustedSQL("o"), "{trusted(m)}", trustedSQL("m"),
	).Replace(block)
}

// trustedSQL reports whether the session trusts the role whose OID the
// expression owner holds, whose objects would run with the rights of the
// role that the session logged in as: a superuser, or a role that has those
// rights already, such as the login role itself.
//
// It measures the login role, not the session user or the current role:
// code that runs as a role that the session set, with SET ROLE or SET
// SESSION AUTHORIZATION, can switch back to the login role. Nor does it
// trust a role that the login role is a member of: a role that may create
// roles can grant one that it created to anyone.
func trustedSQL(owner string) string {
	return strings.ReplaceAll(`(EXISTS (SELECT FROM pg_catalog.pg_roles WHERE oid = {owner} AND rolsuper) `+
		`OR pg_catalog.pg_has_role({owner}, `+loginSQL+`, 'USAGE'))`, "{owner}", owner)
}

// loginSQL is the OID of the role that the session logged in as, which SET
// SESSION AUTHORIZATION does not change, unlike session_user. A session
// may always read its own activity.
const loginSQL = `(SELECT usesysid FROM pg_catalog.pg_stat_get_activity(pg_catalog.pg_backend_pid()))`

// roleSQL returns the OID of the role that the owner OID owner stands for:
// the database's owner for pg_database_owner, which owns the schema public,
// and whose only member the database's owner is.
func roleSQL(owner string) string {
	return "CASE WHEN " + owner + " = 'pg_database_owner'::pg_catalog.regrole " +
		"THEN (SELECT datdba FROM pg_catalog.pg_database WHERE datname = pg_catalog.current_database()) ELSE " + owner + " END"
}

// superuserSQL reports whether the session can act as a superuser: its
// login role is one, or a member of one.
const superuserSQL = `EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolsuper AND pg_catalog.pg_has_role(` + loginSQL + `, oid, 'MEMBER'))`

// Migration is one versioned schema change, for teams that apply migrations
// with their own tools. [Migrations] returns them in order: first the
// bootstrap, version 0, which creates the schema and the migrations table,
// then every versioned migration. The bootstrap refuses what [Migrate]
// refuses, with PostgreSQL's insufficient_privilege: a schema or a
// migrations table whose owner it does not trust, or a role set on the
// connection that it does not trust. Apply them in order, each in a
// transaction unless it is Online, and record every version above 0, so
// that [NewClient] finds the schema up to date:
//
//	INSERT INTO <schema>.migrations (version, name) VALUES (<Version>, '<Name>')
type Migration struct {
	Version int
	Name    string
	SQL     string

	// Online reports a migration that builds or drops indexes with
	// CONCURRENTLY, which blocks no writes. PostgreSQL runs such a statement
	// only outside a transaction, alone: a query that holds several
	// statements runs them in one transaction, and fails. So apply each of
	// [Migration.Statements] in a query of its own, outside a transaction,
	// and record the migration once every statement succeeded. A build that
	// fails leaves the index that its statement names invalid: drop it with
	// DROP INDEX CONCURRENTLY before you try again. Tools such as goose,
	// Atlas, and Flyway split SQL into statements themselves.
	Online bool
}

// Statements returns the statements of an Online migration, in order and
// without comments, for a runner that applies them one at a time. It
// returns nil for any other migration, whose SQL runs whole, in a
// transaction.
func (m Migration) Statements() []string {
	if !m.Online {
		return nil
	}
	return migrations.Split(m.SQL)
}

// Migrations returns every migration for schema, in order. Teams that use
// their own migration tools apply these instead of calling [Migrate]. Their
// bootstrap refuses the schemas that Migrate refuses: see [Migration]. They
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

	return publicMigrations(schema, all), nil
}

// publicMigrations returns the bootstrap of schema, then the migrations all,
// as Migrations returns them.
func publicMigrations(schema string, all []migrations.Migration) []Migration {
	out := make([]Migration, 0, len(all)+1)
	out = append(out, Migration{Version: 0, Name: "bootstrap", SQL: bootstrapSQL(schema)})
	for _, m := range all {
		out = append(out, Migration{Version: m.Version, Name: m.Name, SQL: m.SQL, Online: m.Online})
	}
	return out
}
