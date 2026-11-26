package taskmill_test

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"

	"github.com/code19m/taskmill"
	"github.com/code19m/taskmill/internal/testdb"
	"github.com/code19m/taskmill/outbox"
)

// The recipe for two services lists the grants of a role that only enqueues
// into another service's schema. The test takes the list from the page, so
// that the page stays true: with these grants every enqueue call works, and
// the role can write none of the columns that taskmill keeps for itself,
// such as a task's lease.
func TestRoleThatOnlyEnqueues(t *testing.T) {
	t.Parallel()
	admin := testdb.Pool(t)
	schema := testdb.Schema(t, admin)
	if err := taskmill.Migrate(t.Context(), admin, &taskmill.MigrateConfig{Schema: schema}); err != nil {
		t.Fatal(err)
	}

	pool := enqueueOnlyRole(t, admin, schema)
	client, err := taskmill.NewClient(t.Context(), pool, &taskmill.ClientConfig{Schema: schema})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	if err := enqueueEveryWay(t.Context(), client, pool); err != nil {
		t.Errorf("enqueue as the role: %v", err)
	}
	e := &env{pool: admin, schema: schema}
	if n := e.count(t, "SELECT count(*) FROM {s}.tasks"); n != 14 {
		t.Errorf("the role enqueued %d tasks, want 14", n)
	}

	// What an enqueue does not need, the role cannot do. It cannot write a
	// task that looks leased: a row that says it runs needs a lease, and the
	// role may not write that column.
	const insert = "INSERT INTO {s}.tasks (queue, kind, state, run_at, payload, max_attempts"
	const values = ") VALUES ('default', 'email.send', 'running', now(), '{}', 1"
	for what, tt := range map[string]struct{ sql, code string }{
		"read a payload":          {"SELECT payload FROM {s}.tasks", "42501"},
		"change a task":           {"UPDATE {s}.tasks SET priority = 4", "42501"},
		"delete a task":           {"DELETE FROM {s}.tasks", "42501"},
		"write a task's lease":    {insert + ", lease_expires_at" + values + ", 'infinity')", "42501"},
		"write a task's attempts": {insert + ", attempt, lease_token" + values + ", 1, 2147483647)", "42501"},
		"write a task's errors":   {insert + ", errors" + values + ", '{}')", "42501"},
		"write a freed key":       {insert + ", freed_key, freed_key_expires_at" + values + ", sha256('k'), 'infinity')", "42501"},
		"write a task that runs":  {insert + values + ")", "23514"}, // the table's rule: only a leased task runs
	} {
		_, err := pool.Exec(t.Context(), replaceSchema(tt.sql, schema))
		if pgErr, ok := errors.AsType[*pgconn.PgError](err); !ok || pgErr.Code != tt.code {
			t.Errorf("%s as the role: got %v, want SQLSTATE %s", what, err, tt.code)
		}
	}
}

// enqueueOnlyRole creates a role with the grants that the recipe for two
// services lists, with the names of schema in place of the recipe's, and
// returns a pool that connects as the role.
func enqueueOnlyRole(t *testing.T, admin *pgxpool.Pool, schema string) *pgxpool.Pool {
	t.Helper()
	page, err := os.ReadFile("docs/recipes/split-across-services.md")
	if err != nil {
		t.Fatal(err)
	}
	grants := regexp.MustCompile(`(?m)^ *GRANT [^;]+;`).FindAllString(string(page), -1)
	if len(grants) == 0 {
		t.Fatal("the recipe lists no grants")
	}

	role := schema + "_orders"
	return newRole(t, admin, role, replaced(strings.NewReplacer("billing", schema, "orders", role), grants)...)
}

// enqueueEveryWay enqueues 14 tasks through every enqueue call, with each
// option that changes the statement: a delay, a FIFO group, a dedupe key,
// with and without a transaction of pgx or database/sql, and the outbox.
func enqueueEveryWay(ctx context.Context, client *taskmill.Client, pool *pgxpool.Pool) error {
	delayed := &taskmill.EnqueueOptions{RunAt: time.Now().Add(time.Hour), Priority: taskmill.PriorityHigh, MaxAttempts: 3}
	tagged := &taskmill.EnqueueOptions{Metadata: map[string]string{"tenant": "shop"}, GroupKey: "customer"}
	deduped := &taskmill.EnqueueOptions{DedupeKey: "order-1"}
	many := []taskmill.EnqueueParams{{Args: sendEmail{}}, {Args: sendEmail{}, Options: tagged}, {Args: sendEmail{}, Options: delayed}}

	var errs []error
	for _, opts := range []*taskmill.EnqueueOptions{nil, delayed, tagged, tagged, deduped} {
		_, err := client.Enqueue(ctx, sendEmail{}, opts)
		errs = append(errs, err)
	}

	if res, err := client.Enqueue(ctx, sendEmail{}, deduped); err != nil || !res.Duplicate {
		errs = append(errs, errors.New("a second task with the dedupe key is no duplicate"), err)
	}

	_, err := client.EnqueueMany(ctx, many)
	errs = append(errs, err, pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		_, one := client.EnqueueTx(ctx, taskmill.Pgx(tx), sendEmail{}, tagged)
		_, more := client.EnqueueManyTx(ctx, taskmill.Pgx(tx), many)
		message := outbox.Message{Topic: "orders", Key: []byte("customer"), Value: []byte("placed")}
		_, stored := outbox.New(client).StoreTx(ctx, taskmill.Pgx(tx), message)
		return errors.Join(one, more, stored)
	}))

	db := stdlib.OpenDB(*pool.Config().ConnConfig)
	tx, err := db.BeginTx(ctx, &sql.TxOptions{})
	if err == nil {
		_, err = client.EnqueueTx(ctx, taskmill.SQL(tx), sendEmail{}, &taskmill.EnqueueOptions{GroupKey: "customer"})
		err = errors.Join(err, tx.Commit())
	}
	return errors.Join(append(errs, err, db.Close())...)
}

// migrationsTable and plantedView create a migrations table, and a view
// that Migrate would read as the version, which runs code of its owner's,
// in schema {s}, readable by role {r} and owned by role {o}.
const migrationsTable = "CREATE TABLE {s}.migrations (version integer PRIMARY KEY, name text NOT NULL, applied_at timestamptz NOT NULL DEFAULT now())"

var plantedView = []string{
	"CREATE FUNCTION {s}.ran() RETURNS boolean LANGUAGE plpgsql AS $$BEGIN RAISE EXCEPTION 'ran as the runner'; END$$",
	"CREATE VIEW {s}.migrations AS SELECT 1 AS version WHERE {s}.ran()", "GRANT SELECT ON {s}.migrations TO {r}",
	"ALTER FUNCTION {s}.ran() OWNER TO {o}", "ALTER VIEW {s}.migrations OWNER TO {o}",
}

// Migrate, and the bootstrap of Migrations, fill a schema only if they
// trust its owner, the owner of its migrations table, and the current role:
// a superuser, or a role that has the runner's rights. Any other role could
// run code as the runner there, also one that the runner is a member of,
// and a role set on the runner's connection would own what they create.
// The other role's name is the runner's in capitals, which is another
// role, if SQL does not fold it. Planted code raises an error if it runs,
// which is no refusal.
func TestMigrateRefusesASchemaThatAnotherRoleOwns(t *testing.T) {
	t.Parallel()
	admin := testdb.Pool(t) // a superuser
	for name, tt := range map[string]struct {
		setup   []string // as the superuser, with {s} the schema, {r} the runner, and {o} the other role
		refused bool
	}{
		"another role":                          {[]string{"CREATE SCHEMA {s} AUTHORIZATION {o}", "GRANT USAGE, CREATE ON SCHEMA {s} TO {r}"}, true},
		"another role, without usage":           {[]string{"CREATE SCHEMA {s} AUTHORIZATION {o}"}, true},
		"another role's tables, without usage":  {[]string{"CREATE SCHEMA {s} AUTHORIZATION {o}", migrationsTable, "ALTER TABLE {s}.migrations OWNER TO {o}"}, true},
		"another role's tables, without select": {[]string{"CREATE SCHEMA {s} AUTHORIZATION {o}", migrationsTable, "ALTER TABLE {s}.migrations OWNER TO {o}", "GRANT USAGE ON SCHEMA {s} TO {r}"}, true},
		"another role's tables, set without usage": {[]string{ // a role that reads only what the runner may read
			"CREATE SCHEMA {s} AUTHORIZATION {o}", migrationsTable, "ALTER TABLE {s}.migrations OWNER TO {o}", "GRANT USAGE ON SCHEMA {s} TO {r}",
			"GRANT SELECT ON {s}.migrations TO {r}, pg_monitor", "GRANT pg_monitor TO {r}", "ALTER ROLE {r} SET role = pg_monitor",
		}, true},
		"another role's tables, set without select": {[]string{
			"CREATE SCHEMA {s} AUTHORIZATION {o}", migrationsTable, "ALTER TABLE {s}.migrations OWNER TO {o}", "GRANT USAGE ON SCHEMA {s} TO {r}, pg_monitor",
			"GRANT SELECT ON {s}.migrations TO {r}", "GRANT pg_monitor TO {r}", "ALTER ROLE {r} SET role = pg_monitor",
		}, true},
		"another role's tables":           {[]string{"CREATE SCHEMA {s} AUTHORIZATION {o}", migrationsTable, "ALTER TABLE {s}.migrations OWNER TO {o}", "GRANT USAGE, CREATE ON SCHEMA {s} TO {r}", "GRANT ALL ON {s}.migrations TO {r}"}, true},
		"another role's migrations table": {[]string{"CREATE SCHEMA {s}", "GRANT USAGE, CREATE ON SCHEMA {s} TO {r}", migrationsTable, "ALTER TABLE {s}.migrations OWNER TO {o}", "GRANT ALL ON {s}.migrations TO {r}"}, true},
		"another role's view":             {append([]string{"CREATE SCHEMA {s}", "GRANT USAGE, CREATE ON SCHEMA {s} TO {r}"}, plantedView...), true},
		"another role's operator": {[]string{ // which the runner's search path finds before pg_catalog's, where its table makes the read compare OIDs
			"CREATE SCHEMA {s} AUTHORIZATION {o}", "GRANT USAGE, CREATE ON SCHEMA {s} TO {r}", "ALTER ROLE {r} SET search_path = {s}, pg_catalog",
			migrationsTable, "ALTER TABLE {s}.migrations OWNER TO {r}",
			"CREATE FUNCTION {s}.yes(oid, oid) RETURNS boolean LANGUAGE plpgsql AS $$BEGIN RAISE EXCEPTION 'ran as the runner'; END$$",
			"CREATE OPERATOR {s}.= (LEFTARG = oid, RIGHTARG = oid, FUNCTION = {s}.yes)",
			"ALTER FUNCTION {s}.yes(oid, oid) OWNER TO {o}", "ALTER OPERATOR {s}.=(oid, oid) OWNER TO {o}",
		}, true},
		"the runner, before, for a superuser": {[]string{ // up to date, so Migrate would read it
			"ALTER ROLE {r} SUPERUSER", "CREATE SCHEMA {s} AUTHORIZATION {r}", migrationsTable, "INSERT INTO {s}.migrations VALUES (1000, 'later')",
			"ALTER TABLE {s}.migrations OWNER TO {r}", "ALTER SCHEMA {s} OWNER TO {o}",
		}, true},
		"the runner, before":   {[]string{"CREATE SCHEMA {s} AUTHORIZATION {r}", migrationsTable, "ALTER TABLE {s}.migrations OWNER TO {r}", "ALTER SCHEMA {s} OWNER TO {o}", "GRANT USAGE, CREATE ON SCHEMA {s} TO {r}"}, true},
		"a role of the runner": {[]string{"GRANT {o} TO {r}", "CREATE SCHEMA {s} AUTHORIZATION {o}"}, true},
		"a role of the runner, set": { // which would own the schema that Migrate creates
			[]string{"GRANT {o} TO {r}", "DO $$BEGIN EXECUTE format('GRANT CREATE ON DATABASE %I TO {o}', current_database()); END$$", "ALTER ROLE {r} SET role = {o}"}, true,
		},
		"another role's view, set": { // which the set role may not create, but the runner may fill
			append([]string{
				"CREATE SCHEMA {s}", "GRANT USAGE, CREATE ON SCHEMA {s} TO {r}, {o}", "GRANT USAGE ON SCHEMA {s} TO pg_monitor",
				"GRANT pg_monitor TO {r}", "ALTER ROLE {r} SET role = pg_monitor",
			}, append(plantedView, "GRANT SELECT ON {s}.migrations TO pg_monitor")...), true,
		},
		"a member that does not inherit its rights": {[]string{"ALTER ROLE {o} NOINHERIT", "GRANT {r} TO {o}", "CREATE SCHEMA {s} AUTHORIZATION {o}", "GRANT USAGE, CREATE ON SCHEMA {s} TO {r}"}, true},
		"another role, for a superuser":             {[]string{"ALTER ROLE {r} SUPERUSER", "CREATE SCHEMA {s} AUTHORIZATION {o}"}, true},
		"another role, set, for a superuser":        {[]string{"ALTER ROLE {r} SUPERUSER", "CREATE SCHEMA {s} AUTHORIZATION {r}", "ALTER ROLE {r} SET role = {o}"}, true},
		"the runner, set without usage, for a superuser": {[]string{
			"ALTER ROLE {r} SUPERUSER", "CREATE SCHEMA {s} AUTHORIZATION {r}", migrationsTable, "ALTER TABLE {s}.migrations OWNER TO {r}", "ALTER ROLE {r} SET role = pg_monitor",
		}, true},
		"another role's view, for a superuser": {append([]string{"ALTER ROLE {r} SUPERUSER", "CREATE SCHEMA {s} AUTHORIZATION {o}"}, plantedView...), true},
		"another role's view, for a member of a superuser": {append([]string{
			"DO $$BEGIN EXECUTE format('GRANT %I TO {r}', current_user); END$$", "CREATE SCHEMA {s} AUTHORIZATION {o}", "GRANT USAGE ON SCHEMA {s} TO {r}",
		}, plantedView...), true},
		"a superuser":            {[]string{"CREATE SCHEMA {s}", "GRANT USAGE, CREATE ON SCHEMA {s} TO {r}"}, false},
		"a member of the runner": {[]string{"GRANT {r} TO {o}", "CREATE SCHEMA {s} AUTHORIZATION {o}", "GRANT USAGE, CREATE ON SCHEMA {s} TO {r}"}, false},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			schema := testdb.Schema(t, admin)
			other := strings.ToUpper(schema + "_run")
			names := strings.NewReplacer("{s}", schema, "{r}", schema+"_run", "{o}", pgx.Identifier{other}.Sanitize())
			newRole(t, admin, other) // dropped after the runner, whose tables its schema may hold
			pool := newRole(t, admin, schema+"_run", replaced(names, tt.setup)...)

			ms, err := taskmill.Migrations(schema)
			if err != nil {
				t.Fatal(err)
			}
			_, bootstrap := pool.Exec(t.Context(), ms[0].SQL)
			migrate := taskmill.Migrate(t.Context(), pool, &taskmill.MigrateConfig{Schema: schema})
			for how, err := range map[string]error{"the bootstrap": bootstrap, "Migrate": migrate} {
				if refused := refusedOwner(err); refused != tt.refused || !refused && err != nil {
					t.Errorf("%s: %v; want it refused: %t", how, err, tt.refused)
				}
			}
		})
	}
}

// A superuser's connection that runs SET SESSION AUTHORIZATION has a role
// set, as with SET ROLE: code that runs in it can set the authorization
// back to the superuser. So Migrate refuses that role, and runs neither
// the view of another role nor a trigger of the role set.
func TestMigrateRefusesARoleSetBySessionAuthorization(t *testing.T) {
	t.Parallel()
	admin := testdb.Pool(t) // a superuser
	for name, setup := range map[string][]string{
		"another role's view": append([]string{"CREATE SCHEMA {s} AUTHORIZATION {o}", "GRANT USAGE ON SCHEMA {s} TO {r}"}, plantedView...),
		"the role's trigger": {
			"CREATE SCHEMA {s} AUTHORIZATION {r}", migrationsTable, "ALTER TABLE {s}.migrations OWNER TO {r}",
			"CREATE FUNCTION {s}.ran() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN RAISE EXCEPTION 'ran as the runner'; END$$",
			"CREATE TRIGGER ran BEFORE INSERT ON {s}.migrations FOR EACH ROW EXECUTE FUNCTION {s}.ran()", "ALTER FUNCTION {s}.ran() OWNER TO {r}",
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			schema := testdb.Schema(t, admin)
			role := schema + "_run"
			newRole(t, admin, schema+"_other")
			newRole(t, admin, role, replaced(strings.NewReplacer("{s}", schema, "{r}", role, "{o}", schema+"_other"), setup)...)

			cfg, err := pgxpool.ParseConfig(testdb.DirectURL(t)) // a proxy would not keep the setting
			if err != nil {
				t.Fatal(err)
			}
			cfg.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
				_, err := conn.Exec(ctx, "SET SESSION AUTHORIZATION "+role)
				return err
			}
			pool, err := pgxpool.NewWithConfig(t.Context(), cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer pool.Close()
			err = taskmill.Migrate(t.Context(), pool, &taskmill.MigrateConfig{Schema: schema})
			if !refusedOwner(err) || !strings.Contains(err.Error(), "connect as role "+role+" ") {
				t.Errorf("Migrate: %v; want it refused, naming role %s", err, role)
			}
		})
	}
}

// pg_database_owner, which owns the schema public, stands for the owner of
// the database, its only member. So the owner of a database, a superuser or
// not, fills its schema public, and so does a superuser if another
// superuser owns the database. Another superuser refuses it, and names the
// owner of the database, as whom it can connect. A role that owns nothing
// then reads the version of public, as of any schema whose owner filled it,
// and so does the runner on a replica.
func TestMigrateFillsPublicForTheOwnerOfItsDatabase(t *testing.T) {
	t.Parallel()
	admin := testdb.Pool(t)
	for name, tt := range map[string]struct {
		setup   []string
		refused bool
	}{
		"the owner":                  {[]string{"CREATE DATABASE {d} OWNER {r}"}, false},
		"an owner, superuser":        {[]string{"ALTER ROLE {r} SUPERUSER", "CREATE DATABASE {d} OWNER {r}"}, false},
		"another, a superuser":       {[]string{"ALTER ROLE {r} SUPERUSER", "CREATE DATABASE {d} OWNER {o}"}, true},
		"a superuser's, a superuser": {[]string{"ALTER ROLE {r} SUPERUSER", "ALTER ROLE {o} SUPERUSER", "CREATE DATABASE {d} OWNER {o}"}, false},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			migratePublic(t, admin, tt.setup, tt.refused)
		})
	}
}

// migratePublic runs setup as the superuser, with {d} a new database, {r}
// the runner, and {o} the other role, and then Migrate on public of {d} as
// the runner. Unless it is refused, a role that owns nothing, and the
// runner, then only read the version: they take no lock of the migrations
// table, which a replica would refuse, while another transaction holds a
// lock that conflicts with it.
func migratePublic(t *testing.T, admin *pgxpool.Pool, setup []string, refused bool) {
	t.Helper()
	db := testdb.Schema(t, admin) // a fresh name
	newRole(t, admin, db+"_other")
	newRole(t, admin, db+"_run", replaced(strings.NewReplacer("{d}", db, "{r}", db+"_run", "{o}", db+"_other"), setup)...)
	newRole(t, admin, db+"_web")
	t.Cleanup(func() { // before the roles
		if _, err := admin.Exec(context.Background(), "DROP DATABASE "+db+" WITH (FORCE)"); err != nil {
			t.Errorf("drop database %s: %v", db, err)
		}
	})

	runner := connectAs(t, db+"_run", db)
	err := taskmill.Migrate(t.Context(), runner, &taskmill.MigrateConfig{Schema: "public"})
	if refused {
		if !refusedOwner(err) || !strings.Contains(err.Error(), "connect as role "+db+"_other to migrate it (") { // with no hint to make the superuser its owner
			t.Errorf("Migrate: %v; want it refused, naming role %s_other", err, db)
		}
		return
	}
	if err != nil {
		t.Fatalf("Migrate: %v", err)
	}

	if _, err := runner.Exec(t.Context(), "GRANT USAGE, CREATE ON SCHEMA public TO "+db+"_web; GRANT SELECT ON public.migrations TO "+db+"_web"); err != nil {
		t.Fatal(err)
	}
	lock, err := runner.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = lock.Rollback(context.Background()) }()
	if _, err := lock.Exec(t.Context(), "LOCK TABLE public.migrations IN SHARE MODE"); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second) // Migrate tries the lock until then
	defer cancel()
	for _, role := range []string{db + "_web", db + "_run"} {
		if err := taskmill.Migrate(ctx, connectAs(t, role, db), &taskmill.MigrateConfig{Schema: "public"}); err != nil {
			t.Errorf("Migrate as %s, on an up-to-date public: %v", role, err)
		}
	}
}

// replaced returns the statements with the names that r replaces.
func replaced(r *strings.Replacer, statements []string) []string {
	out := make([]string, len(statements))
	for i, sql := range statements {
		out[i] = r.Replace(sql)
	}
	return out
}

// refusedOwner reports whether err is the refusal of a role that Migrate
// does not trust, which names the role to connect as.
func refusedOwner(err error) bool {
	pgErr, ok := errors.AsType[*pgconn.PgError](err)
	return ok && pgErr.Code == "42501" && strings.Contains(pgErr.Message, ": connect as role ")
}
