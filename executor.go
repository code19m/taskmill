package taskmill

import (
	"context"
	"database/sql"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// Executor runs taskmill's statements on a transaction or connection that the
// caller owns, so that they commit or roll back with the caller's own work.
// Create one with [Pgx] or [SQL].
//
// The enqueue methods accept a transaction, a connection, or a pool.
// [Client.CancelTx] and [Task.CompleteTx] run several statements that must
// commit together, so they need a transaction: a [pgx.Tx], a [*sql.Tx], or a
// type with Commit and Rollback methods, such as *sqlx.Tx. They return
// [ErrInvalidArgument] for anything else. An enqueue counts every other
// executor as outside a transaction, also a connection on which the caller
// ran BEGIN, and a type that wraps a pgx.Tx with only Exec and Query. There,
// a failed second read of a dedupe key returns an error that wraps
// [ErrStoredWithoutIDs] (see [EnqueueResult.ID]), and an [EnqueueMiddleware]
// that fails after next has its error logged, not returned.
//
// Executor is sealed: only this package implements it, which lets its methods
// change without breaking callers.
//
// The [transactions guide] shows an enqueue, a completion, and a cancel in
// the caller's transaction.
//
// [transactions guide]: https://github.com/code19m/taskmill/blob/main/docs/guides/transactions.md
type Executor interface {
	querier() querier
}

// PgxQuerier is the part of pgx that taskmill uses: these two methods, and
// no other. [pgx.Tx], [*pgx.Conn], and [*pgxpool.Pool] implement it. An
// application's own type that wraps one of them, for example to trace
// statements, needs only these two methods to enqueue. For [Client.CancelTx]
// and [Task.CompleteTx], it must also be a [pgx.Tx], as [Executor] says.
type PgxQuerier interface {
	Exec(ctx context.Context, query string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, query string, args ...any) (pgx.Rows, error)
}

// SQLQuerier is the part of database/sql that taskmill uses: these two
// methods, and no other. [*sql.Tx], [*sql.Conn], and [*sql.DB] implement it.
// An application's own type that wraps one of them needs only these two
// methods to enqueue. For [Client.CancelTx] and [Task.CompleteTx], it must
// also have Commit and Rollback methods, as [Executor] says. The connection
// must use pgx's database/sql driver, github.com/jackc/pgx/v5/stdlib.
type SQLQuerier interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

// Pgx returns an [Executor] that runs statements on q, typically a [pgx.Tx].
func Pgx(q PgxQuerier) Executor {
	return pgxExecutor{q: q}
}

// SQL returns an [Executor] that runs statements on q, typically a [*sql.Tx].
//
// Libraries built on database/sql work through their underlying transaction:
// with bun, pass tx.Tx, because bun rewrites query placeholders itself.
func SQL(q SQLQuerier) Executor {
	return sqlExecutor{q: q}
}

// querier is what taskmill's statements run on: a pool, a connection, or a
// transaction, from pgx or database/sql.
type querier interface {
	exec(ctx context.Context, query string, args ...any) (int64, error)
	query(ctx context.Context, query string, args ...any) (rows, error)

	// inTx reports whether statements run inside one transaction, which
	// operations made of several statements require.
	inTx() bool
}

// rows is the part of a result set that taskmill reads. [pgx.Rows]
// implements it.
type rows interface {
	Next() bool
	Scan(dest ...any) error
	Err() error
	Close()
}

// pgxExecutor runs statements on pgx. It is the querier of the client's own
// pool too.
type pgxExecutor struct {
	q PgxQuerier
}

func (e pgxExecutor) querier() querier {
	if isNil(e.q) {
		return nil
	}
	return e
}

func (e pgxExecutor) exec(ctx context.Context, query string, args ...any) (int64, error) {
	tag, err := e.q.Exec(ctx, query, args...)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

func (e pgxExecutor) query(ctx context.Context, query string, args ...any) (rows, error) {
	return e.q.Query(ctx, query, args...) //nolint:sqlclosecheck // the caller closes the rows
}

func (e pgxExecutor) inTx() bool {
	_, ok := e.q.(pgx.Tx)
	return ok
}

// sqlExecutor runs statements on database/sql.
type sqlExecutor struct {
	q SQLQuerier
}

func (e sqlExecutor) querier() querier {
	if isNil(e.q) {
		return nil
	}
	return e
}

func (e sqlExecutor) exec(ctx context.Context, query string, args ...any) (int64, error) {
	res, err := e.q.ExecContext(ctx, query, args...)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func (e sqlExecutor) query(ctx context.Context, query string, args ...any) (rows, error) {
	r, err := e.q.QueryContext(ctx, query, args...) //nolint:rowserrcheck // the caller checks Err
	if err != nil {
		return nil, err
	}
	return sqlRows{Rows: r}, nil
}

// inTx reports whether q is a transaction: a *sql.Tx, or a type that wraps
// one, such as *sqlx.Tx.
func (e sqlExecutor) inTx() bool {
	_, ok := e.q.(interface {
		Commit() error
		Rollback() error
	})
	return ok
}

// sqlRows adapts [*sql.Rows] to rows.
type sqlRows struct {
	*sql.Rows
}

func (r sqlRows) Close() {
	_ = r.Rows.Close() // Err reports the error that matters
}

// queryRow runs a statement that returns at most one row and scans it into
// dest. It returns false if there was no row.
func queryRow(ctx context.Context, q querier, query string, args []any, dest ...any) (bool, error) {
	r, err := q.query(ctx, query, args...)
	if err != nil {
		return false, err
	}
	defer r.Close()

	if !r.Next() {
		return false, r.Err()
	}
	if err := r.Scan(dest...); err != nil {
		return false, err
	}

	// A statement can still fail after its first row, as a deferred
	// constraint does at the commit, and rows report that only once they are
	// closed. Closing skips any further rows without decoding them.
	r.Close()
	return true, r.Err()
}

// executorQuerier returns the querier of an Executor for a single statement,
// which may run on a pool or connection as well as in a transaction.
func executorQuerier(tx Executor) (querier, error) {
	if tx == nil {
		return nil, invalidArgument("executor is nil")
	}
	q := tx.querier()
	if q == nil {
		return nil, invalidArgument("executor wraps nil, not a transaction, connection, or pool")
	}
	return q, nil
}

// txQuerier returns the querier of an Executor that must be a transaction,
// because the operation runs several statements that must commit together.
func txQuerier(tx Executor) (querier, error) {
	q, err := executorQuerier(tx)
	if err == nil && !q.inTx() {
		return nil, invalidArgument("executor must wrap a transaction (pgx.Tx or *sql.Tx)")
	}
	return q, err
}
