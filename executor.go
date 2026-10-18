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
// Executor is sealed: only this package implements it, which lets its methods
// change without breaking callers.
type Executor interface {
	querier() querier
}

// PgxQuerier is the part of pgx that taskmill uses. [pgx.Tx], [*pgx.Conn],
// and [*pgxpool.Pool] implement it.
type PgxQuerier interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// SQLQuerier is the part of database/sql that taskmill uses. [*sql.Tx],
// [*sql.Conn], and [*sql.DB] implement it. The connection must use pgx's
// database/sql driver, github.com/jackc/pgx/v5/stdlib.
type SQLQuerier interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// PgxExecutor runs statements through pgx. Create one with [Pgx].
type PgxExecutor struct {
	q PgxQuerier
}

// SQLExecutor runs statements through database/sql. Create one with [SQL].
type SQLExecutor struct {
	q SQLQuerier
}

// Pgx returns an [Executor] that runs statements on q, typically a [pgx.Tx].
func Pgx(q PgxQuerier) PgxExecutor {
	return PgxExecutor{q: q}
}

// SQL returns an [Executor] that runs statements on q, typically a [*sql.Tx].
//
// Libraries built on database/sql work through their underlying transaction:
// with bun, pass tx.Tx, because bun rewrites query placeholders itself.
func SQL(q SQLQuerier) SQLExecutor {
	return SQLExecutor{q: q}
}

func (e PgxExecutor) querier() querier {
	if e.q == nil {
		return nil
	}
	return pgxQuerier(e)
}

func (e SQLExecutor) querier() querier {
	if e.q == nil {
		return nil
	}
	return sqlQuerier(e)
}

// querier is what taskmill's statements run on: a pool, a connection, or a
// transaction, from pgx or database/sql.
type querier interface {
	exec(ctx context.Context, sql string, args ...any) (int64, error)
	query(ctx context.Context, sql string, args ...any) (rows, error)

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

type pgxQuerier struct {
	q PgxQuerier
}

func (p pgxQuerier) exec(ctx context.Context, sql string, args ...any) (int64, error) {
	tag, err := p.q.Exec(ctx, sql, args...)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

func (p pgxQuerier) query(ctx context.Context, sql string, args ...any) (rows, error) {
	return p.q.Query(ctx, sql, args...) //nolint:sqlclosecheck // the caller closes the rows
}

func (p pgxQuerier) inTx() bool {
	_, ok := p.q.(pgx.Tx)
	return ok
}

type sqlQuerier struct {
	q SQLQuerier
}

func (s sqlQuerier) exec(ctx context.Context, query string, args ...any) (int64, error) {
	res, err := s.q.ExecContext(ctx, query, args...)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func (s sqlQuerier) query(ctx context.Context, query string, args ...any) (rows, error) {
	r, err := s.q.QueryContext(ctx, query, args...) //nolint:rowserrcheck // the caller checks Err
	if err != nil {
		return nil, err
	}
	return sqlRows{Rows: r}, nil
}

func (s sqlQuerier) inTx() bool {
	_, ok := s.q.(*sql.Tx)
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
func queryRow(ctx context.Context, q querier, sql string, args []any, dest ...any) (bool, error) {
	r, err := q.query(ctx, sql, args...)
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
	return true, r.Err()
}
