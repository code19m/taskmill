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
	names := strings.NewReplacer("billing", schema, "orders", role)
	for i, grant := range grants {
		grants[i] = names.Replace(grant)
	}
	return newRole(t, admin, role, grants...)
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
		return errors.Join(one, more, outbox.New(client).Store(ctx, taskmill.Pgx(tx), message))
	}))

	db := stdlib.OpenDB(*pool.Config().ConnConfig)
	tx, err := db.BeginTx(ctx, &sql.TxOptions{})
	if err == nil {
		_, err = client.EnqueueTx(ctx, taskmill.SQL(tx), sendEmail{}, &taskmill.EnqueueOptions{GroupKey: "customer"})
		err = errors.Join(err, tx.Commit())
	}
	return errors.Join(append(errs, err, db.Close())...)
}
