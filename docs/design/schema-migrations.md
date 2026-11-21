# Schema migrations

This page explains how `Migrate` changes a schema that a fleet uses all the
time: one process at a time, indexes built without blocking writes, and a
guard for the older migrations that block them. The files are in
[`internal/migrations`](../../internal/migrations), and
[CONTRIBUTING.md](../../CONTRIBUTING.md#schema-migrations) has the rules for
a new one.

## One process at a time

`Migrate` first creates the schema and the `migrations` table, in a
transaction of their own. Then it takes steps, each in a transaction that
locks `migrations` in `SHARE ROW EXCLUSIVE` mode:

| Next pending migration | The step |
|---|---|
| Not online | Applies it, and the ones after it up to the next online step, in the transaction |
| An [online step](#online-steps) | Runs its statements on other connections, while the transaction keeps the lock |

Each step reads the version again under the lock, and records each
migration that it applied. So processes that migrate at the same time take
turns, and the last ones find nothing left to do. A migration that is not
online sets a lock timeout of 5 seconds as its first statement, so it never
blocks the fleet for long.

**A waiting process never waits in the lock's queue.** It tries the lock
with `NOWAIT`, and tries again after a pause that doubles from 20
milliseconds up to 1 second, until its context ends. A session that waits
for a table's lock, on a connection that had not read the table before,
keeps a snapshot while it waits. A build with `CONCURRENTLY` waits for every
older snapshot in the database before it marks its index valid, and the
holder of the lock waits for its build. The three would wait for each other
until a context ended, and PostgreSQL sees no deadlock: the holder waits in
the client. Measured on PostgreSQL 18, with a pool that opens a new
connection for each statement.

## Online steps

An online step is a migration whose first line is `-- online`. It holds
only `CREATE INDEX CONCURRENTLY` and `DROP INDEX CONCURRENTLY` statements,
which build or drop an index while writes continue. PostgreSQL runs each
only outside a transaction, alone.

- **The lock stays held during the build.** The step's transaction holds
  the lock and nothing else. It sets `idle_in_transaction_session_timeout`,
  and from PostgreSQL 17 `transaction_timeout`, to 0 for itself, since a
  build takes minutes. Each statement runs on another connection of the
  pool, so the pool needs 2. Then the step records the version, and
  commits. An idle transaction at READ COMMITTED keeps no snapshot, so the
  build does not wait for it: measured.
- **It needs no session.** Through PgBouncer in transaction mode, the
  transaction keeps one server connection, and each statement gets
  another. PgBouncer's own pool must allow both.
- **No timeout may cancel a build.** A `lock_timeout` cancels it too: a
  build waits for every older transaction in the database. A canceled build
  leaves an invalid index at each try, so the step fails before it starts
  while the pool's connections have one of these timeouts. We rejected
  setting them to 0 for the build's session: through PgBouncer, that
  setting would reach other clients, and the build might not get it.
- **Before each statement, it waits for builds of the table.** A `Migrate`
  whose process stopped can leave its build running: the server notices a
  client that is gone only when it answers. A second build of the same
  table, waiting for the table's `SHARE UPDATE EXCLUSIVE` lock, would keep a
  snapshot that the first waits for, and PostgreSQL ends one of the two as
  a deadlock. Measured: it ended the first, after it had read the table. So
  the step looks in `pg_locks` for a session that holds a lock of the table
  that a build waits for, and looks again after a pause, and logs once that
  it waits. It does not wait for autovacuum, which ends its work when a
  build waits for it.
- **An invalid index is dropped and built again.** Once no such session is
  left, the step reads the index. A valid one is done, and a missing one is
  built. An invalid one is left by a build that failed or was canceled: the
  step drops it with `DROP INDEX CONCURRENTLY`, then builds it. No session
  builds it meanwhile: other processes wait for the lock of `migrations`,
  and stopped ones run no new statement.
- **A step whose lock ended stops.** A proxy or an operator can end the
  lock's session during a build, and another process then takes the lock.
  So before each build or drop, the step pings the lock's connection: a
  ping takes no snapshot, which a build could wait for. Each look at the
  index also reads the version, and fails if another process recorded the
  step.
- **A build never has `IF NOT EXISTS`.** It would skip an invalid index,
  and succeed. Measured: of two builds with it at the same time, the first
  was ended as a deadlock, and the second skipped its invalid index.
- **A partitioned table, such as the history, has no online step yet.**
  PostgreSQL cannot build an index of a partitioned table concurrently. Such
  a step would build the index on each partition, and attach them.

| Rejected option | Why not |
|---|---|
| A session's advisory lock, held during the build, as most migration tools do | Any role may take it ([Storage schema](schema.md#operational-notes)), and PgBouncer in transaction mode keeps no session |
| No lock during the build, with checks in its place | A process that read the version before another applied later migrations could still build an index that a later migration dropped |
| One transaction for all pending migrations, as `Migrate` had before | An index can then only be built with a lock that blocks writes, about half a second for each million rows |

## The guard for migrations that block writes

Migrations up to 9 build their indexes in their transaction, with a lock
that makes writes wait while the table is read: migration 3 on `groups`, 7
on `tasks` and the history, and 9 on `tasks`. Teams that upgrade from an
older release still run them. So before a step changes anything, it
estimates the rows of the tables of each pending migration that blocks, and
fails if a migration would read more than `MaxBlockingIndexRows`.

- **What blocks.** When it loads the files, the code marks each `CREATE
  INDEX`, and each unique or primary key constraint without `USING INDEX`,
  except on a table that the same migration creates: that table is empty.
- **The estimate.** As the planner does, it takes the rows for each page
  that VACUUM and ANALYZE last counted, times the pages that the table has
  now, summed over the partitions. A table that neither has seen yet
  counts as rows of 100 bytes, fewer than a task takes.
- **The error is `ErrBlockingMigration`,** so that a deploy job can tell a
  refusal, which needs a person, from a failure that a retry fixes.
- **The default is 1,000,000 rows.** Migration 7 then makes writes wait
  about half a second: well under the 5 seconds for which every other
  migration may wait for a lock, and far under a lease of 30 seconds. A
  renewal that waits longer than a lease loses its task's attempt. A small
  schema never meets the limit: a million rows are a week of history at
  1.6 tasks per second.
- **Rows, not seconds.** The time depends on the disks, while an operator
  knows the rows, and the docs give the time for each million.

## See also

- [Deploys](../production/deploys.md#run-migrations): how to run `Migrate`, and what to do when the guard refuses
- [Lists by creation time](schema-creation-order.md#what-the-creation-order-costs): what migration 7 costs

**Next:** [The API's shape](api-shape.md)
