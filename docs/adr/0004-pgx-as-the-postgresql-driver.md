# 0004. Use pgx as the PostgreSQL driver

- Status: Accepted
- Date: 2026-10-18

## Context

The engine needs a PostgreSQL driver with a connection pool, `LISTEN` and
`NOTIFY` for low-latency wake-ups, and efficient array parameters, which batch
statements use to send many tasks in one round trip. Applications must also be
able to enqueue tasks inside their own transactions, whether they use pgx
directly or `database/sql` through libraries such as bun or sqlx.

The options were:

1. **pgx v5**, used directly for the engine, with its `database/sql` driver
   (`pgx/v5/stdlib`) for applications built on `database/sql`.
2. **lib/pq** through `database/sql`. It is in maintenance mode, needs
   wrappers for array parameters, and `database/sql` offers no API for
   notifications.
3. **`database/sql` only,** with no driver chosen by taskmill. The engine could
   not use notifications or pgx's binary protocol and batching.

## Decision

The engine uses pgx v5 and its connection pool, `pgxpool`. Applications pass
their transactions through two adapters, `taskmill.Pgx` for pgx and
`taskmill.SQL` for `database/sql`. The `database/sql` connection must use pgx's
driver.

The core module's allowlist in `.golangci.yml` gains `github.com/jackc/pgx/v5`.
Measured with `go list -deps`, pgx brings four more modules into the build:
`github.com/jackc/pgpassfile`, `github.com/jackc/pgservicefile`,
`github.com/jackc/puddle/v2`, and `golang.org/x/text` with `golang.org/x/sync`
from the Go project.

## Consequences

- The engine gets notifications, binary encoding, and batching, and both
  pgx and `database/sql` applications can enqueue in their own transactions.
  Both paths were tested against PostgreSQL with the batch statements' array
  parameters.
- Applications on `database/sql` must use pgx's driver; lib/pq is not
  supported. Libraries that rewrite query placeholders, such as bun, must pass
  their underlying `*sql.Tx`.
- pgx types appear in taskmill's public API (`PgxQuerier`). A future pgx major
  version would therefore require a taskmill major version.
