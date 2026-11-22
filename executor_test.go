package taskmill_test

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

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

// A transaction variable left nil on an error path is a caller's mistake,
// which every call that takes an executor reports as an invalid argument,
// whatever the executor wraps: it must not panic.
func TestANilExecutorIsAnInvalidArgument(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	id := e.enqueue(t, sendEmail{}, nil)
	calls := map[string]func(ex taskmill.Executor) error{
		"EnqueueTx": func(ex taskmill.Executor) error {
			_, err := e.client.EnqueueTx(t.Context(), ex, sendEmail{}, nil)
			return err
		},
		"EnqueueManyTx": func(ex taskmill.Executor) error {
			_, err := e.client.EnqueueManyTx(t.Context(), ex, []taskmill.EnqueueParams{{Args: sendEmail{}}})
			return err
		},
		"CancelTx": func(ex taskmill.Executor) error { return e.client.CancelTx(t.Context(), ex, id) },
	}
	executors := map[string]taskmill.Executor{
		"nil":                    nil,
		"Pgx(nil)":               taskmill.Pgx(nil),
		"Pgx(nil pgx.Tx)":        taskmill.Pgx(pgx.Tx(nil)),
		"Pgx(nil *pgxpool.Tx)":   taskmill.Pgx((*pgxpool.Tx)(nil)),
		"Pgx(nil *pgxpool.Pool)": taskmill.Pgx((*pgxpool.Pool)(nil)),
		"Pgx(nil *pgx.Conn)":     taskmill.Pgx((*pgx.Conn)(nil)),
		"SQL(nil)":               taskmill.SQL(nil),
		"SQL(nil *sql.Tx)":       taskmill.SQL((*sql.Tx)(nil)),
		"SQL(nil *sql.DB)":       taskmill.SQL((*sql.DB)(nil)),
	}

	for callName, call := range calls {
		for exName, ex := range executors {
			if err := call(ex); !errors.Is(err, taskmill.ErrInvalidArgument) {
				t.Errorf("%s on %s: got %v, want ErrInvalidArgument", callName, exName, err)
			}
		}
	}
	e.wantState(t, id, taskmill.StateAvailable)
}
