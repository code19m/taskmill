package taskmill_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
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
	migrateAtOnce(t, pool, schema, 8)

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

	for round := range 6 {
		schema := testdb.Schema(t, pool)
		if round%2 == 1 {
			// An administrator created the schema: the processes then meet
			// at the migrations table.
			if _, err := pool.Exec(t.Context(), "CREATE SCHEMA "+schema); err != nil {
				t.Fatal(err)
			}
		}
		migrateAtOnce(t, pool, schema, processes)
	}
}

// Any role of the database may take any advisory lock, also a role with no
// right on taskmill's schema, and hold it for as long as it likes. Migrate
// and a starting worker must not wait for it.
func TestRoleWithoutRightsCannotHoldUpMigrateOrWorkers(t *testing.T) {
	t.Parallel()
	admin := testdb.Pool(t)

	// hold takes the locks that Migrate and the history partitions once
	// waited for, in a session of a new role, until the test ends.
	hold := func(t *testing.T, schema string) {
		t.Helper()
		conn, err := newRole(t, admin, schema+"_outsider").Acquire(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(conn.Release)

		for _, key := range []string{"taskmill.migrate." + schema, "taskmill.partitions." + schema} {
			if _, err := conn.Exec(t.Context(), "SELECT pg_advisory_lock(hashtextextended($1, 0))", key); err != nil {
				t.Fatal(err)
			}
		}
	}

	t.Run("migrate", func(t *testing.T) {
		t.Parallel()
		schema := testdb.Schema(t, admin)
		hold(t, schema)

		ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
		defer cancel()
		if err := taskmill.Migrate(ctx, admin, &taskmill.MigrateConfig{Schema: schema}); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("worker", func(t *testing.T) {
		t.Parallel()
		schema := testdb.Schema(t, admin)
		applyWithoutMigrate(t, admin, schema, 0) // without partitions: the worker creates them
		hold(t, schema)

		e := (&env{pool: admin, schema: schema}).withLogger(t, testLogger(t))
		id := e.enqueue(t, sendEmail{}, nil)
		runWorker(t, e, &taskmill.WorkerConfig{Handlers: noopHandlers(), NoMaintainer: true})
		e.waitState(t, id, taskmill.StateCompleted) // in the history, so the partitions exist
	})
}

// A migration changes tables that workers use all the time, and while it
// waits for a table's lock, every later statement on that table waits behind
// it. So a migration gives up after 5 seconds, and changes nothing: the next
// try migrates.
//
// The 5 seconds are in the text of each migration, which a test of
// internal/migrations holds: its first statement sets them. This test does
// not wait them out. It ends the migration's wait as PostgreSQL would at the
// timeout, by canceling the statement that waits.
func TestMigrateGivesUpOnBusyTables(t *testing.T) {
	t.Parallel()
	pool := testdb.Pool(t)
	schema := testdb.Schema(t, pool)
	applyWithoutMigrate(t, pool, schema, 1) // as before a deploy that brings a migration
	e := &env{pool: pool, schema: schema}
	version := e.count(t, "SELECT max(version) FROM {s}.migrations")

	// A transaction that writes the tables of the tasks stays open.
	busy := e.begin(t)
	var holder int
	if err := busy.QueryRow(t.Context(), "SELECT pg_backend_pid()").Scan(&holder); err != nil {
		t.Fatal(err)
	}

	r, err := pool.Query(t.Context(), "SELECT tablename FROM pg_tables WHERE schemaname = $1 AND tablename <> 'migrations'", schema)
	if err != nil {
		t.Fatal(err)
	}
	tables, err := pgx.CollectRows(r, pgx.RowTo[string])
	if err != nil {
		t.Fatal(err)
	}
	for _, table := range tables {
		e.txExec(t, busy, "LOCK TABLE {s}."+table+" IN ROW EXCLUSIVE MODE")
	}

	gaveUp := make(chan error, 1)
	go func() { gaveUp <- taskmill.Migrate(t.Context(), pool, &taskmill.MigrateConfig{Schema: schema}) }()
	waitFor(t, "the migration to wait for a busy table", func() bool {
		return e.count(t, "SELECT count(pg_cancel_backend(pid)) FROM pg_stat_activity WHERE $1 = ANY(pg_blocking_pids(pid))", holder) > 0
	})

	err = recv(t, gaveUp, "Migrate to give up")
	if pgErr, ok := errors.AsType[*pgconn.PgError](err); !ok || pgErr.Code != "57014" { // query_canceled
		t.Fatalf("migrate with busy tables: got %v, want the error of its canceled statement", err)
	}
	if got := e.count(t, "SELECT max(version) FROM {s}.migrations"); got != version {
		t.Fatalf("a migration that gave up left version %d, want %d", got, version)
	}

	if err := busy.Rollback(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := taskmill.Migrate(t.Context(), pool, &taskmill.MigrateConfig{Schema: schema}); err != nil {
		t.Fatalf("migrate again: %v", err)
	}
}

// The transactions of a running fleet take the locks of several tables in an
// order of their own: a statistics refresh reads tasks and then workers, and
// a run of a schedule writes schedules and then tasks. A migration that
// locks two of their tables in the other order deadlocks with them, and
// PostgreSQL ends one of the two. So a migration locks the tables that a
// transaction of the fleet holds together in the fleet's order: schedules,
// then tasks, then workers, which every check-in writes.
//
// Migrations 5 and 6 lock these tables. A fleet of the release before them
// runs on version 4, where both are pending and run in one transaction, and
// a fleet of the release between them on version 5.
//
// Migration 7 indexes tasks and then the history. A batch of DeleteMatching
// locks the rows of tasks that it picks, deletes its completed tasks from
// the history, and then deletes the picked tasks: the migration waits for
// such a batch to end before it takes tasks.
func TestMigrateDoesNotDeadlockWithTheFleet(t *testing.T) {
	t.Parallel()
	for _, version := range []int{4, 5} {
		t.Run(fmt.Sprintf("from version %d, a statistics refresh", version), func(t *testing.T) {
			t.Parallel()
			migrateBesideTheFleet(t, version, "SELECT count(*) FROM {s}.tasks", "SELECT count(*) FROM {s}.workers")
		})

		t.Run(fmt.Sprintf("from version %d, a run of a schedule", version), func(t *testing.T) {
			t.Parallel()
			migrateBesideTheFleet(t, version, "UPDATE {s}.schedules SET last_run_at = now()",
				"INSERT INTO {s}.tasks (queue, kind, state, run_at, payload, max_attempts) VALUES ('q', 'k', 'available', now(), '{}', 1)")
		})
	}

	t.Run("from version 6, a batch of a bulk delete", func(t *testing.T) {
		t.Parallel()
		migrateBesideTheFleet(t, 6, `WITH picked AS (SELECT id FROM {s}.tasks FOR UPDATE SKIP LOCKED)
		                             DELETE FROM {s}.task_history WHERE id IN (SELECT id FROM picked)`,
			"DELETE FROM {s}.tasks WHERE id = 0")
	})

	t.Run("from version 6, a completion", func(t *testing.T) {
		t.Parallel()
		migrateBesideTheFleet(t, 6, "DELETE FROM {s}.tasks WHERE id = 0",
			`INSERT INTO {s}.task_history (id, finalized_at, queue, created_at, kind, run_at, attempted_by, attempted_at,
			                              priority, attempt, max_attempts, payload, metadata, errors)
			 SELECT 1, now(), 'q', now(), 'k', now(), 'w', now(), 2, 1, 1, '{}', '{}', '[]' WHERE false`)
	})
}

// migrateBesideTheFleet migrates a schema at version while a transaction of
// the fleet is between its statements first and then. The migration must
// end without an error, and must hold up neither the fleet's next statement
// nor a worker's check-in.
func migrateBesideTheFleet(t *testing.T, version int, first, then string) {
	t.Helper()
	pool := testdb.Pool(t)
	e := &env{pool: pool, schema: testdb.Schema(t, pool)}
	applyUpTo(t, pool, e.schema, version)

	fleet := e.begin(t)
	e.txExec(t, fleet, first)

	migrated := make(chan error, 1)
	go func() {
		migrated <- taskmill.Migrate(context.Background(), pool, &taskmill.MigrateConfig{Schema: e.schema})
	}()
	const waiting = `SELECT count(*) FROM pg_locks l JOIN pg_class c ON c.oid = l.relation
	                 WHERE NOT l.granted AND c.relnamespace = $1::regnamespace`
	waitFor(t, "the migration to end, or to wait for a lock", func() bool {
		return len(migrated) == 1 || e.count(t, waiting, e.schema) > 0
	})

	// A statement that waited for the migration would time out here.
	worker := e.begin(t)
	for _, tx := range []pgx.Tx{fleet, worker} {
		e.txExec(t, tx, "SET LOCAL lock_timeout = '2s'")
	}
	e.txExec(t, fleet, then)
	e.txExec(t, worker, "UPDATE {s}.workers SET heartbeat_at = now() WHERE id = 'worker'")

	for _, tx := range []pgx.Tx{worker, fleet} {
		if err := tx.Commit(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
	if err := recv(t, migrated, "the migration to end"); err != nil {
		t.Fatalf("migrate beside the fleet: %v", err)
	}
}

// A schema that any earlier release left upgrades in one call, with the
// rows of a running fleet in the tables that its migrations change, and the
// fleet's statements work on those rows afterwards. Rows that hold what no
// release writes, as an UPDATE by hand leaves them, are repaired. The tasks
// that the release left, in the history too, are in the indexes of creation
// time.
func TestMigrateUpgradesEveryEarlierVersion(t *testing.T) {
	t.Parallel()
	all, err := taskmill.Migrations("taskmill")
	if err != nil {
		t.Fatal(err)
	}

	latest := len(all) - 1 // without the bootstrap
	for from := 1; from < latest; from++ {
		t.Run(fmt.Sprintf("from version %d", from), func(t *testing.T) {
			t.Parallel()
			// The one row of the settings holds one pair of periods, so the
			// versions take turns with the pairs.
			upgradeWithRows(t, from, brokenRetentions[from%len(brokenRetentions)])
		})
	}
}

// refusingVersion is the migration from which the small tables refuse the
// rows of writeRefusedRows.
const refusingVersion = 6

// brokenRetention is a pair of retention periods as SQL by hand can leave
// them, written as its SET clause, and what an upgrade makes of them, as a
// condition on the row. PostgreSQL compares periods by their totals.
type brokenRetention struct{ set, repaired string }

var brokenRetentions = []brokenRetention{
	// Outside the range: the nearest periods inside it.
	{
		"history_retention = '-30 days', dead_retention = '1000000 years'",
		"history_retention = interval '1 hour' AND dead_retention = interval '300 years'",
	},
	// In the range, with a negative field: the same periods without one.
	// The first moves a time 3,600 years ahead, not 1 hour back.
	{
		"history_retention = '-3000000 mons 90000000 days 1 hour', dead_retention = '7 days -1 hour'",
		"history_retention = interval '1 hour' AND dead_retention = interval '6 days 23 hours'",
	},
	// A good period beside a bad one stays as it is stored: 30 days, not
	// the month that equals them.
	{
		"history_retention = '2 mons -20 days'",
		"history_retention = interval '40 days' AND extract(day FROM dead_retention) = 30",
	},
}

// noNegativeField is the condition that the period in column has no
// negative field: PostgreSQL stores its months, its days, and the rest
// apart, each with a sign of its own.
func noNegativeField(column string) string {
	return fmt.Sprintf(`least(extract(year FROM %[1]s), extract(month FROM %[1]s), extract(day FROM %[1]s),
		extract(hour FROM %[1]s), extract(minute FROM %[1]s), extract(second FROM %[1]s)) >= 0`, column)
}

// upgradeWithRows migrates a schema at the version from, which holds what a
// fleet of its release left: a worker, a schedule, statistics, a FIFO group
// that a dead task halts, and a completed task in the history of an earlier
// day. A schema from before refusingVersion also holds every kind of row
// that the tables refuse afterwards. Then the current code must read the
// fleet's rows, find the others repaired, list the tasks by creation time
// from their indexes, and run a task beside them.
func upgradeWithRows(t *testing.T, from int, retention brokenRetention) {
	t.Helper()
	pool := testdb.Pool(t)
	e := &env{pool: pool, schema: testdb.Schema(t, pool)}
	applyUpTo(t, pool, e.schema, from)

	e.exec(t, `CREATE TABLE {s}.task_history_20200101 PARTITION OF {s}.task_history
	           FOR VALUES FROM ('2020-01-01 00:00:00+00') TO ('2020-01-02 00:00:00+00')`)
	e.exec(t, `INSERT INTO {s}.task_history (id, finalized_at, queue, created_at, kind, run_at, attempted_by, attempted_at,
	                                         priority, attempt, max_attempts, payload, metadata, errors)
	           VALUES (1000, '2020-01-01 12:00:00+00', 'default', '2020-01-01 11:00:00+00', 'email.send', now(), 'w', now(),
	                   2, 1, 1, '{}', '{}', '[]')`)
	e.exec(t, `INSERT INTO {s}.workers (id, queues, kinds, hostname, version)
	           VALUES ('old-worker', '{default}', '{email.send}', 'host', 'v0.1.0')`)
	e.exec(t, `INSERT INTO {s}.schedules (name, queue, kind, cron, next_run_at)
	           VALUES ('nightly', 'default', 'email.send', '0 3 * * *', now() + interval '1 day')`)
	e.exec(t, `INSERT INTO {s}.queue_stats (queue, state, tasks, capped, refreshed_at)
	           VALUES ('default', 'scheduled', 1, false, now())`)
	e.exec(t, `INSERT INTO {s}.tasks (queue, kind, state, run_at, payload, max_attempts, group_key, finalized_at)
	           VALUES ('default', 'email.send', 'dead', now(), '{}', 1, 'customer', now())`)
	e.exec(t, `INSERT INTO {s}.groups (queue, group_key, tasks, halted_task_id, halted_at)
	           SELECT queue, group_key, 1, id, now() FROM {s}.tasks`)

	refused := from < refusingVersion
	if refused {
		e.writeRefusedRows(t, retention.set)
	}

	if err := taskmill.Migrate(t.Context(), pool, &taskmill.MigrateConfig{Schema: e.schema}); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	e = e.withLogger(t, testLogger(t))
	ctx := t.Context()
	workers, errWorkers := e.client.Workers(ctx)
	schedules, errSchedules := e.client.Schedules(ctx)
	queues, errQueues := e.client.Queues(ctx)
	groups, errGroups := e.client.Groups(ctx, &taskmill.GroupParams{Halted: true})
	_, errSettings := e.client.Settings(ctx)
	if err := errors.Join(errWorkers, errSchedules, errQueues, errGroups, errSettings); err != nil {
		t.Fatalf("read what the old release left: %v", err)
	}

	// The schedules come in the order of their names: the fleet's, between
	// the two that were repaired and a third.
	fleets := slices.IndexFunc(schedules, func(s taskmill.ScheduleInfo) bool { return s.Name == "nightly" })
	for what, ok := range map[string]bool{
		"worker":       len(workers) == 1 && workers[0].ID == "old-worker" && !workers[0].Stale,
		"schedule":     fleets >= 0 && !schedules[fleets].Paused && !schedules[fleets].NextRunAt.IsZero(),
		"statistics":   len(queues) == 1 && queues[0].Counts[taskmill.StateScheduled].N == 1,
		"halted group": len(groups.Groups) == 1,
	} {
		if !ok {
			t.Errorf("the %s from before the upgrade: %+v, %+v, %+v, %+v", what, workers, schedules, queues, groups)
		}
	}

	// The tasks from before the upgrade are in the indexes of creation time:
	// the dead one, and the completed one, whose partition got the index of
	// the history, as the seven that Migrate prepared did.
	created, err := e.client.List(ctx, &taskmill.ListParams{CreatedSince: time.Date(2020, time.January, 1, 0, 0, 0, 0, time.UTC)})
	if err != nil || len(created.Tasks) != 2 || created.Tasks[1].ID != 1000 || created.Tasks[1].State != taskmill.StateCompleted {
		t.Errorf("the tasks from before the upgrade, by creation time: %+v, %v; want the dead one, and then the completed one", created, err)
	}
	const indexed = `SELECT count(*) FROM pg_index i
	                 WHERE i.indisvalid AND (i.indexrelid = '{s}.tasks_created_idx'::regclass OR i.indexrelid IN (
	                     SELECT inhrelid FROM pg_inherits WHERE inhparent = '{s}.task_history_created_idx'::regclass))`
	if got := e.count(t, indexed); got != 1+8 {
		t.Errorf("%d valid indexes of creation time, want that of tasks, and one for each of the 8 partitions of the history", got)
	}

	// The workers that the tables refuse are gone: the list above holds the
	// fleet's worker alone. The other rows are repaired.
	repairs := map[string]string{
		"a schedule whose next run had no end is paused, and due": `
			SELECT count(*) FROM {s}.schedules WHERE name = 'never' AND paused_at IS NOT NULL AND next_run_at <= now()`,
		"a paused schedule whose next run had no end keeps its pause": `
			SELECT count(*) FROM {s}.schedules WHERE name = 'never-paused' AND paused_at = '2026-01-01' AND next_run_at <= now()`,
		"a schedule whose last run had no end has none, and still runs": `
			SELECT count(*) FROM {s}.schedules WHERE name = 'ran-forever' AND last_run_at IS NULL AND paused_at IS NULL AND next_run_at > now()`,
		"the retention periods are repaired, to " + retention.repaired: `
			SELECT count(*) FROM {s}.settings WHERE ` + retention.repaired + `
			AND ` + noNegativeField("history_retention") + ` AND ` + noNegativeField("dead_retention"),
	}
	if !refused {
		repairs = nil
	}
	for what, repaired := range repairs {
		if e.count(t, repaired) != 1 {
			t.Errorf("after the upgrade, want that %s", what)
		}
	}

	// The new code runs beside what the old release left.
	id := e.enqueue(t, sendEmail{}, nil)
	runWorker(t, e, &taskmill.WorkerConfig{Handlers: noopHandlers(), Queues: defaultQueue(1)})
	e.waitState(t, id, taskmill.StateCompleted)
}

// writeRefusedRows writes by hand, into a schema of an earlier release, a
// row of every kind that the tables refuse once they are migrated: times
// without end, names that are NULL or in two dimensions, leases that no
// worker has, and the retention periods of the SET clause retention.
func (e *env) writeRefusedRows(t *testing.T, retention string) {
	t.Helper()
	e.exec(t, `INSERT INTO {s}.workers (id, queues, kinds, hostname, version, started_at, heartbeat_at) VALUES
	           ('check-in-without-end', '{default}', '{}', 'host', 'v', now(), 'infinity'),
	           ('start-before-all-time', '{default}', '{}', 'host', 'v', '-infinity', now()),
	           ('null-queue', '{default,NULL}', '{}', 'host', 'v', now(), now()),
	           ('null-kind', '{default}', '{NULL}', 'host', 'v', now(), now()),
	           ('queues-in-two-dimensions', '{{a,b},{c,d}}', '{}', 'host', 'v', now(), now()),
	           ('kinds-in-two-dimensions', '{}', '{{a,b},{c,NULL}}', 'host', 'v', now(), now())`)

	// The lease's column came with migration 5.
	if e.count(t, "SELECT max(version) FROM {s}.migrations") >= 5 {
		e.exec(t, `INSERT INTO {s}.workers (id, queues, hostname, version, lease_duration) VALUES
		           ('lease-too-long', '{}', 'host', 'v', '1000000 years'),
		           ('lease-below-zero', '{}', 'host', 'v', '-1 second'),
		           ('lease-of-nothing-with-negative-months', '{}', 'host', 'v', '-4000000 mons 120000000 days'),
		           ('lease-with-negative-days', '{}', 'host', 'v', '2 mons -20 days'),
		           ('lease-with-negative-time', '{}', 'host', 'v', '7 days -1 hour')`)
	}

	e.exec(t, `INSERT INTO {s}.schedules (name, queue, kind, cron, next_run_at, last_run_at, paused_at) VALUES
	           ('never', 'default', 'email.send', '0 3 * * *', 'infinity', NULL, NULL),
	           ('never-paused', 'default', 'email.send', '0 3 * * *', '-infinity', NULL, '2026-01-01'),
	           ('ran-forever', 'default', 'email.send', '0 3 * * *', now() + interval '1 day', 'infinity', NULL)`)

	// Where the database stores a period without end, the period that is
	// too long has none.
	if e.infinitePeriods(t) {
		retention = strings.ReplaceAll(retention, "1000000 years", "infinity")
	}
	e.exec(t, "UPDATE {s}.settings SET "+retention)
}

// infinitePeriods reports whether the database stores a period without end,
// as PostgreSQL does from version 17.
func (e *env) infinitePeriods(t *testing.T) bool {
	t.Helper()
	return e.count(t, "SELECT current_setting('server_version_num')::integer") >= 170000
}

// Every statement finds its schedules by name: the maintainer loads the ones
// that it defines, and checks in Go which are due. An index on the next run
// time served no statement, and made each run of a schedule write an index
// entry. A migration drops it, also from a schema where an operator already
// did.
func TestSchedulesAreIndexedByNameOnly(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name   string
		before string // what ran on the schema that has the index
	}{
		{"a schema with the index", "SELECT 1"},
		{"a schema whose index an operator dropped", "DROP INDEX {s}.schedules_due_idx"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			pool := testdb.Pool(t)
			e := &env{pool: pool, schema: testdb.Schema(t, pool)}
			applyUpTo(t, pool, e.schema, 4) // migration 5 drops the index
			e.exec(t, tt.before)

			if err := taskmill.Migrate(t.Context(), pool, &taskmill.MigrateConfig{Schema: e.schema}); err != nil {
				t.Fatalf("migrate: %v", err)
			}

			r, err := pool.Query(t.Context(),
				"SELECT indexname FROM pg_indexes WHERE schemaname = $1 AND tablename = 'schedules'", e.schema)
			if err != nil {
				t.Fatal(err)
			}
			indexes, err := pgx.CollectRows(r, pgx.RowTo[string])
			if err != nil || !slices.Equal(indexes, []string{"schedules_pkey"}) {
				t.Errorf("schedules has the indexes %v, %v; want its primary key alone", indexes, err)
			}
		})
	}
}

// Migrate runs before NewClient checks the pool's isolation level. A process
// that waited for another's migration must see what that one committed,
// whatever the pool's level.
func TestConcurrentMigratesAtRepeatableRead(t *testing.T) {
	t.Parallel()
	cfg, err := pgxpool.ParseConfig(testdb.DirectURL(t)) // a proxy refuses the parameter
	if err != nil {
		t.Fatal(err)
	}
	cfg.MaxConns = 8
	cfg.ConnConfig.RuntimeParams["default_transaction_isolation"] = "repeatable read"

	pool, err := pgxpool.NewWithConfig(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	migrateAtOnce(t, pool, testdb.Schema(t, pool), 8)
}

// migrateAtOnce runs Migrate on schema from several processes at once, and
// fails the test if one of them fails.
func migrateAtOnce(t *testing.T, pool *pgxpool.Pool, schema string, processes int) {
	t.Helper()
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
	applyWithoutMigrate(t, pool, schema, 0)
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
	applyWithoutMigrate(t, admin, schema, 0)

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
// the record of its version. It leaves the newest pending migrations out.
func applyWithoutMigrate(t *testing.T, pool *pgxpool.Pool, schema string, pending int) {
	t.Helper()
	ms, err := taskmill.Migrations(schema)
	if err != nil {
		t.Fatal(err)
	}

	for _, m := range ms[:len(ms)-pending] {
		sql := m.SQL
		if m.Version > 0 {
			sql += fmt.Sprintf("\nINSERT INTO %s.migrations (version, name) VALUES (%d, '%s');", schema, m.Version, m.Name)
		}
		// Statements of a string run in one transaction: an online
		// migration runs them one at a time, as goose's NO TRANSACTION does.
		stmts := []string{sql}
		if m.Online {
			stmts = strings.Split(sql, ";")
		}
		for _, stmt := range stmts {
			if _, err := pool.Exec(t.Context(), stmt); err != nil {
				t.Fatalf("apply migration %d: %v", m.Version, err)
			}
		}
	}
}

// applyUpTo applies the schema as applyWithoutMigrate does, up to the
// migration version: the schema that the release with that migration left.
func applyUpTo(t *testing.T, pool *pgxpool.Pool, schema string, version int) {
	t.Helper()
	ms, err := taskmill.Migrations(schema)
	if err != nil {
		t.Fatal(err)
	}
	applyWithoutMigrate(t, pool, schema, len(ms)-1-version) // without the bootstrap
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

// onlineEnv returns an env whose schema is migrated, with tasks whose index
// builds slowly, and the files of three migrations after taskmill's own: an
// online step that builds that index and a quick one, a migration in a
// transaction that comments the slow one, and an online step that drops the
// quick one.
func onlineEnv(t *testing.T, pool *pgxpool.Pool) (*env, []string) {
	t.Helper()
	e := &env{pool: pool, schema: testdb.Schema(t, pool)}
	if err := taskmill.Migrate(t.Context(), pool, &taskmill.MigrateConfig{Schema: e.schema}); err != nil {
		t.Fatal(err)
	}
	// About a second for a build, which reads each task twice.
	e.exec(t, `CREATE FUNCTION {s}.slow(bigint) RETURNS boolean IMMUTABLE LANGUAGE sql AS 'SELECT pg_sleep(0.002) IS NOT NULL'`)
	e.exec(t, `INSERT INTO {s}.tasks (queue, kind, state, run_at, payload, max_attempts)
	           SELECT 'q', 'k', 'available', now(), '{}', 1 FROM generate_series(1, 250)`)

	latest := e.count(t, "SELECT max(version) FROM {s}.migrations")
	return e, []string{
		fmt.Sprintf("%04d_build.sql", latest+1), `-- online
CREATE INDEX CONCURRENTLY tasks_slow_idx ON __SCHEMA__.tasks (id) WHERE __SCHEMA__.slow(id);
CREATE INDEX CONCURRENTLY tasks_kind_idx ON __SCHEMA__.tasks (kind);`,
		fmt.Sprintf("%04d_comment.sql", latest+2), `SET LOCAL lock_timeout = '5s';
COMMENT ON INDEX __SCHEMA__.tasks_slow_idx IS 'built online';`,
		fmt.Sprintf("%04d_drop.sql", latest+3), `-- online
DROP INDEX CONCURRENTLY __SCHEMA__.tasks_kind_idx;`,
	}
}

// migrateWith runs taskmill.MigrateWith with files on e's schema in the
// background, and returns its result.
func (e *env) migrateWith(ctx context.Context, files []string) <-chan error {
	done := make(chan error, 1)
	go func() { done <- taskmill.MigrateWith(ctx, e.pool, &taskmill.MigrateConfig{Schema: e.schema}, files...) }()
	return done
}

// buildSlowIndex builds, in the background, the slow index of onlineEnv's
// first migration, as a Migrate does whose process is gone, and returns the
// build's result.
func (e *env) buildSlowIndex() <-chan error {
	done := make(chan error, 1)
	go func() {
		_, err := e.pool.Exec(context.Background(),
			replaceSchema("CREATE INDEX CONCURRENTLY tasks_slow_idx ON {s}.tasks (id) WHERE {s}.slow(id)", e.schema))
		done <- err
	}()
	return done
}

// lockHolders returns how many sessions hold the lock of e's migrations
// table.
func (e *env) lockHolders(t *testing.T) int {
	t.Helper()
	return e.count(t, `SELECT count(*) FROM pg_locks
	                   WHERE relation = '{s}.migrations'::regclass AND mode = 'ShareRowExclusiveLock' AND granted`)
}

// waitBuilding waits until a session builds an index of e's tasks.
func (e *env) waitBuilding(t *testing.T) {
	t.Helper()
	waitFor(t, "an index of tasks to build", func() bool {
		return e.count(t, `SELECT count(*) FROM pg_stat_progress_create_index
		                   WHERE relid = '{s}.tasks'::regclass AND phase LIKE 'building index%'`) > 0
	})
}

// wantOnlineSteps fails the test unless the three migrations of onlineEnv
// are applied, each once, in order.
func (e *env) wantOnlineSteps(t *testing.T) {
	t.Helper()
	if n := e.count(t, "SELECT count(*) FROM pg_index WHERE indexrelid = '{s}.tasks_slow_idx'::regclass AND indisvalid"); n != 1 {
		t.Errorf("the slow index is valid %d times, want once", n)
	}
	if n := e.count(t, "SELECT count(*) FROM pg_description WHERE objoid = '{s}.tasks_slow_idx'::regclass AND description = 'built online'"); n != 1 {
		t.Error("the migration after the online step did not comment the index")
	}
	if n := e.count(t, "SELECT count(*) FROM pg_class WHERE oid = to_regclass('{s}.tasks_kind_idx')"); n != 0 {
		t.Error("the second online step did not drop the quick index")
	}
	if n := e.count(t, "SELECT count(*) FROM {s}.migrations WHERE version > (SELECT max(version) - 3 FROM {s}.migrations)"); n != 3 {
		t.Errorf("recorded %d of the 3 migrations", n)
	}
}

// A migration that builds an index on a busy table must not make its writes
// wait: they would block enqueues, and the renewals of leases, past a
// lease. An online step builds it with CONCURRENTLY, outside a transaction,
// and the migrations after it run when it is done.
func TestMigrateBuildsIndexesOnline(t *testing.T) {
	t.Parallel()
	e, files := onlineEnv(t, testdb.Pool(t))
	migrated := e.migrateWith(t.Context(), files)
	e.waitBuilding(t)

	// A write that waited for the build would time out.
	write := e.begin(t)
	e.txExec(t, write, "SET LOCAL lock_timeout = '500ms'")
	e.txExec(t, write, `INSERT INTO {s}.tasks (queue, kind, state, run_at, payload, max_attempts)
	                    VALUES ('q', 'k', 'available', now(), '{}', 1)`)
	if err := write.Commit(t.Context()); err != nil {
		t.Fatal(err)
	}

	if err := recv(t, migrated, "Migrate"); err != nil {
		t.Fatal(err)
	}
	e.wantOnlineSteps(t)
}

// Processes that migrate at the same time take turns at the lock of the
// migrations table, also while an online step builds. One that waited in the
// lock's queue, on a connection that had not read the table before, would
// keep a snapshot, which the build waits for, while the build's process
// waits for the build: they would stop each other until a context ended.
// Each statement of this pool runs on a new connection, as through a proxy
// that pools transactions it may.
func TestConcurrentMigratesBuildAnIndexOnline(t *testing.T) {
	t.Parallel()
	cfg, err := pgxpool.ParseConfig(testdb.URL(t))
	if err != nil {
		t.Fatal(err)
	}
	cfg.MaxConns = 16
	cfg.AfterRelease = func(*pgx.Conn) bool { return false } // close it
	pool, err := pgxpool.NewWithConfig(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	e, files := onlineEnv(t, pool)
	migrated := make([]<-chan error, 6)
	for i := range migrated {
		migrated[i] = e.migrateWith(t.Context(), files)
	}

	for i, done := range migrated {
		if err := recv(t, done, "Migrate"); err != nil {
			t.Errorf("Migrate %d: %v", i, err)
		}
	}
	e.wantOnlineSteps(t)
}

// A build that fails, or is canceled, leaves its index invalid: PostgreSQL
// maintains it on every write, and no statement uses it. The next Migrate
// drops it and builds it again.
func TestMigrateBuildsAgainAnIndexThatACanceledBuildLeft(t *testing.T) {
	t.Parallel()
	e, files := onlineEnv(t, testdb.Pool(t))
	ctx, cancel := context.WithCancel(t.Context())
	migrated := e.migrateWith(ctx, files)
	e.waitBuilding(t)
	cancel()

	if err := recv(t, migrated, "the canceled Migrate"); err == nil {
		t.Fatal("a canceled Migrate returned no error")
	}
	if n := e.count(t, "SELECT count(*) FROM pg_index WHERE indexrelid = '{s}.tasks_slow_idx'::regclass AND NOT indisvalid"); n != 1 {
		t.Fatal("the canceled build left no invalid index")
	}

	if err := recv(t, e.migrateWith(t.Context(), files), "Migrate"); err != nil {
		t.Fatal(err)
	}
	e.wantOnlineSteps(t)
}

// A process can stop while its build goes on in the database: the server
// notices a client that is gone only when it answers. The next Migrate then
// takes the lock of the migrations table, and finds an invalid index that is
// still being built. It must neither drop that index nor build another of
// the same table: PostgreSQL would end one of the two builds as a deadlock.
// It waits for the build, says so once, and finds the index valid.
func TestMigrateWaitsForABuildWhoseProcessIsGone(t *testing.T) {
	t.Parallel()
	e, files := onlineEnv(t, testdb.Pool(t))
	orphan := e.buildSlowIndex()
	e.waitBuilding(t)

	logs := &logRecorder{}
	cfg := &taskmill.MigrateConfig{Schema: e.schema, Logger: slog.New(logs)}
	if err := taskmill.MigrateWith(t.Context(), e.pool, cfg, files...); err != nil {
		t.Fatal(err)
	}
	if err := recv(t, orphan, "the build of the process that is gone"); err != nil {
		t.Fatalf("the build of the process that is gone: %v", err)
	}
	e.wantOnlineSteps(t)
	if n := logs.count("waiting for an index build"); n != 1 {
		t.Errorf("logged the wait %d times, want once", n)
	}
}

// The lock of an online step can end while its build runs: an operator ends
// the session, or a proxy or the database ends a transaction that stayed
// open too long. Another process then takes the lock, waits for the build,
// and applies the migrations after it, which here drop the quick index. The
// first process must not go on: it would build that index again.
//
// The first process waits for the build of a process that is gone, as the
// second does, so the second may get ahead of it.
func TestMigrateStopsAnOnlineStepWhoseLockEnded(t *testing.T) {
	t.Parallel()
	e, files := onlineEnv(t, testdb.Pool(t))
	orphan := e.buildSlowIndex()
	e.waitBuilding(t)
	first := e.migrateWith(t.Context(), files)
	waitFor(t, "the first Migrate to take the lock", func() bool { return e.lockHolders(t) == 1 })
	time.Sleep(300 * time.Millisecond) // its pauses grow while it waits for the build
	if n := e.count(t, `SELECT count(pg_terminate_backend(pid)) FROM pg_locks
	                    WHERE relation = '{s}.migrations'::regclass AND mode = 'ShareRowExclusiveLock' AND granted`); n != 1 {
		t.Fatalf("ended %d sessions that held the lock, want 1", n)
	}

	second := e.migrateWith(t.Context(), files)
	if err := recv(t, orphan, "the build of the process that is gone"); err != nil {
		t.Fatal(err)
	}
	if err := recv(t, second, "the second Migrate"); err != nil {
		t.Fatalf("the second Migrate: %v", err)
	}
	if err := recv(t, first, "the first Migrate"); err == nil {
		t.Fatal("the Migrate whose lock ended returned no error")
	}
	e.wantOnlineSteps(t)
}

// An online step runs on a second connection while the first holds the lock
// of the migrations table. A pool of one connection would wait for the
// second forever.
func TestMigrateNeedsTwoConnectionsForAnOnlineStep(t *testing.T) {
	t.Parallel()
	e, files := onlineEnv(t, testdb.PoolSize(t, 1))
	err := recv(t, e.migrateWith(t.Context(), files), "Migrate")
	if err == nil || !strings.Contains(err.Error(), "at least 2") {
		t.Fatalf("got %v, want an error that asks for 2 connections", err)
	}
}

// A build waits for every older transaction in the database, and reads the
// table twice: a timeout of its connection would cancel it, and leave an
// invalid index at each try. Migrate refuses the step before it starts.
func TestMigrateRefusesAnOnlineStepThatATimeoutWouldCancel(t *testing.T) {
	t.Parallel()
	cfg, err := pgxpool.ParseConfig(testdb.DirectURL(t)) // a proxy refuses the parameter
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams["statement_timeout"] = "30s"
	pool, err := pgxpool.NewWithConfig(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	e, files := onlineEnv(t, pool)

	err = recv(t, e.migrateWith(t.Context(), files), "Migrate")
	if err == nil || !strings.Contains(err.Error(), "statement_timeout = 30s") {
		t.Fatalf("got %v, want an error that names the timeout", err)
	}
	if n := e.count(t, "SELECT count(*) FROM pg_class WHERE oid = to_regclass('{s}.tasks_slow_idx')"); n != 0 {
		t.Fatal("Migrate began the build")
	}
}

// Migrations 7 and 9 build indexes that block the writes to tasks while they
// read it, about half a second for each million tasks. Teams that upgrade
// from a release before them have them pending. Migrate applies them only on
// tables whose rows MigrateConfig.MaxBlockingIndexRows allows, and changes
// nothing otherwise.
func TestMigrateRefusesABlockingIndexOnALargeTable(t *testing.T) {
	t.Parallel()
	pool := testdb.Pool(t)
	e := &env{pool: pool, schema: testdb.Schema(t, pool)}
	applyUpTo(t, pool, e.schema, 5)
	e.exec(t, `INSERT INTO {s}.tasks (queue, kind, state, run_at, payload, max_attempts)
	           SELECT 'q', 'k', 'available', now(), '{}', 1 FROM generate_series(1, 100)`)
	e.exec(t, "ANALYZE {s}.tasks")

	err := taskmill.Migrate(t.Context(), pool, &taskmill.MigrateConfig{Schema: e.schema, MaxBlockingIndexRows: 99})
	if err == nil || !strings.Contains(err.Error(), "0007_created_order builds an index that blocks writes") ||
		!strings.Contains(err.Error(), "MaxBlockingIndexRows") {
		t.Fatalf("got %v, want an error that names migration 7 and the limit", err)
	}
	if v := e.count(t, "SELECT max(version) FROM {s}.migrations"); v != 5 {
		t.Fatalf("a refused Migrate left version %d, want 5", v)
	}

	if err := taskmill.Migrate(t.Context(), pool, &taskmill.MigrateConfig{Schema: e.schema, MaxBlockingIndexRows: 100}); err != nil {
		t.Fatalf("migrate within the limit: %v", err)
	}
}
