// Package teardown drops the schema that a test or a benchmark worked in.
package teardown

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// closeLimit bounds the wait for the pool to close its connections. The pool
// closes the connection of a statement that was given up in milliseconds,
// unless the server is far behind. One that stays acquired for longer is most
// likely in use, by somebody that DropSchema must not wait for.
const closeLimit = time.Second

// retries bounds the drops that DropSchema tries again after a deadlock. The
// server ends each after its deadlock_timeout, a second by default.
const retries = 5

// DropSchema drops schema, if it exists, on a connection of pool, once the
// statements that the pool's users gave up have ended.
//
// A statement whose context ended can go on in the database. Its caller has
// returned, so a worker that was told to stop has stopped, and the pool
// closes the statement's connection in the background, until the server has
// ended the statement. Until then the statement may take locks, in another
// order than a drop of its schema does, and PostgreSQL ends one of the two as
// a deadlock. So DropSchema first waits until the pool has closed every such
// connection.
//
// The wait cannot always tell that the statements have ended: not on a pool
// that others use meanwhile, not after closeLimit, and not through a proxy,
// which closes the connection at once while the server works on. Only then,
// DropSchema tries a drop again that PostgreSQL ended as a deadlock, a few
// times. Otherwise such a deadlock is a mistake in the order of the caller's
// clean-up, and DropSchema returns it.
func DropSchema(ctx context.Context, pool *pgxpool.Pool, schema string) error {
	closed := waitClosed(ctx, pool)
	for try := 0; ; try++ {
		_, err := pool.Exec(ctx, "DROP SCHEMA IF EXISTS "+pgx.Identifier{schema}.Sanitize()+" CASCADE")
		pgErr, ok := errors.AsType[*pgconn.PgError](err)
		if !ok || pgErr.Code != "40P01" || try == retries || closed && direct(ctx, pool) { // deadlock_detected
			return err
		}
	}
}

// waitClosed waits until pool has closed the connections that it is closing,
// and reports whether it has. The pool counts a connection as acquired until
// it is closed, and also while somebody uses it: so the wait ends without an
// answer when a connection is acquired meanwhile, which only another user of
// the pool does, after closeLimit, and with ctx.
func waitClosed(ctx context.Context, pool *pgxpool.Pool) bool {
	acquires := pool.Stat().AcquireCount()
	for start := time.Now(); ; time.Sleep(time.Millisecond) {
		s := pool.Stat()
		if s.AcquiredConns() == 0 {
			return true
		}
		if s.AcquireCount() != acquires || time.Since(start) > closeLimit || ctx.Err() != nil {
			return false
		}
	}
}

// direct reports whether pool reaches the server without a proxy that hands
// its statements to server processes of its own, as PgBouncer does: then the
// process that answers is the one that the connection was opened to.
func direct(ctx context.Context, pool *pgxpool.Pool) bool {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return false
	}
	defer conn.Release()
	var pid uint32
	err = conn.QueryRow(ctx, "SELECT pg_backend_pid()").Scan(&pid)
	return err == nil && pid == conn.Conn().PgConn().PID()
}
