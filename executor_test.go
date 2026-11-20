package taskmill_test

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/code19m/taskmill"
	"github.com/code19m/taskmill/internal/testdb"
)

// A statement can fail after it returned a row: a later row fails, a
// deferred constraint fails at the commit, or the statement is canceled. The
// read of a statement's one row reports that, on every kind of executor. A
// worker that took the row for the result would count an attempt's failure
// as recorded after the database rolled it back.
func TestStatementThatFailsAfterItsFirstRowIsReported(t *testing.T) {
	t.Parallel()
	pool, db := testdb.Pool(t), openSQL(t)

	tx, err := pool.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tx.Rollback(context.Background()) }) // t.Context is canceled before cleanups run

	sqlTx, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sqlTx.Rollback() })

	for name, e := range map[string]taskmill.Executor{
		"a pgx pool":                 taskmill.Pgx(pool),
		"a pgx transaction":          taskmill.Pgx(tx),
		"a database/sql pool":        taskmill.SQL(db),
		"a database/sql transaction": taskmill.SQL(sqlTx),
	} {
		// The first row is 1, and the second divides by zero.
		var n int
		found, err := taskmill.QueryRow(t.Context(), e, "SELECT 1 / (2 - g) FROM generate_series(1, 2) g", &n)
		if pgErr, ok := errors.AsType[*pgconn.PgError](err); !ok || pgErr.Code != "22012" { // division_by_zero
			t.Errorf("on %s: got a row (%t) and the error %v, want SQLSTATE 22012", name, found, err)
		}
	}
}
