# Database

This page helps you set up PostgreSQL for taskmill: which version, which
role, how many connections, and what to watch so the queue stays fast.

## Use PostgreSQL 15 or newer, and its primary

taskmill needs PostgreSQL 15 or newer, and nothing more: no extension, and no
superuser. So it runs on managed services too, such as Amazon RDS and Aurora,
Google Cloud SQL and AlloyDB, Azure Database for PostgreSQL, Neon, and
Supabase.

Connect every process to the **primary**. taskmill writes on every fetch, so
use the writer endpoint, such as Aurora's cluster endpoint, never a read
replica.

## Run workers as the owner of taskmill's tables

**Run your workers as the owner of taskmill's tables,** or as a member of the
owner role.

Why: taskmill keeps completed tasks in a history table with one partition per
day. Each worker creates the missing partitions of the next 7 days when it
starts. The [maintainer](../concepts/glossary.md#maintainer) then creates each day's partition a
week ahead, and drops old ones. PostgreSQL allows that only to the owner.

If the role is wrong, tasks still complete, but you lose their history:

1. Within about a day, the log shows `operation still failing` for
   `apply retention`, at Error level.
2. Up to 7 days later, a day starts without a partition. The log says
   `task history has no partition for today`, and completed tasks skip the
   history.

If your own migration tool applied a new schema, it has no partitions yet.
With the wrong role, each worker then logs `prepare history partitions
failed` at Warn level when it starts, and completed tasks skip the history
from the first one.

To create the schema, `Migrate` needs the CREATE privilege on the database.
If your role does not have it, create the schema yourself, owned by that
role. `Migrate` then only fills it.

| Who | Role |
|---|---|
| Workers, and whatever runs `Migrate` | The owner of taskmill's tables, or a member of the owner role |
| Web processes that only enqueue | The same role is simplest |
| A read-only dashboard | A role that may only read: see [Security](security.md#give-the-dashboard-a-read-only-role) |

## Keep the isolation level at READ COMMITTED

taskmill's own statements need READ COMMITTED, PostgreSQL's default. If your
database or role sets another default, `NewClient` fails. Set the level back
for taskmill's pool:

```go
cfg, err := pgxpool.ParseConfig(os.Getenv("DATABASE_URL"))
if err != nil {
    return err
}
cfg.ConnConfig.RuntimeParams["default_transaction_isolation"] = "read committed"
pool, err := pgxpool.NewWithConfig(ctx, cfg)
```

Your own transactions, which you pass to the `...Tx` methods, may use any
level.

## Size the connection pool

Give each worker's pool its **total `Concurrency` plus 5** connections. Add up
`Concurrency` over all the worker's queues. Set the size with
`pool_max_conns` in the connection URL:

```text
postgres://shop@db.internal:5432/shop?pool_max_conns=15
```

Why: a running task holds no connection of its own. But if your handlers use
the same pool, they can take every connection. Then the worker cannot renew
its tasks' [leases](../concepts/glossary.md#lease) in time, and tasks run twice.
The 5 extra connections are for the worker's own work. The worker logs a
warning at start when the pool is smaller.

| Process | `pool_max_conns` | Connections it opens, at most |
|---|---|---|
| One `Worker` | its total `Concurrency` + 5 | the pool, plus 1 for notifications |
| Two `Worker`s that share one pool | both totals + 10 | the pool, plus 2 for notifications |
| A process that only enqueues | what your own code needs | the pool |
| `taskmill-ui` | 4, for a few people at a time | the pool |

Always set `pool_max_conns`. pgx's default is the larger of 4 and the number
of CPUs it sees, and in a container that is the node's CPUs, not your limit.

To add up the connections of all your pods, see
[Count the database connections](kubernetes.md#count-the-database-connections).

### Which should I use?

| Situation | What to use | Why |
|---|---|---|
| Handlers use the database for short queries | **One pool** for taskmill and your handlers, sized as above | Simple, and the warning watches it for you |
| Handlers hold connections for a long time | A pool of their own for the handlers | taskmill's work never waits for them. If taskmill's own pool is then smaller than `Concurrency` plus 5, ignore the start-up warning |

## Keep long transactions from slowing the queue

A *snapshot* is a transaction's view of the data. While an old snapshot stays
open anywhere in the database, vacuum cannot clean taskmill's busy `tasks`
table. Every fetch then steps over more dead rows, so throughput falls and
tasks wait longer.

Common causes are sessions left idle in a transaction, `pg_dump`, long
reports, replicas with `hot_standby_feedback`, and replication slots that
nobody reads.

- Set `idle_in_transaction_session_timeout`, and a `statement_timeout` for
  the roles that run reports.
- Run long reports on a replica without `hot_standby_feedback`.
- Set `autovacuum_naptime` to 10 seconds or less, and keep autovacuum on.
  taskmill's migrations already tune autovacuum for the `tasks` table.
- Watch these ages. When one keeps growing, an old snapshot is open
  ([Monitoring](monitoring.md) has the alert):

  ```sql
  SELECT max(age(backend_xmin)) FROM pg_stat_activity;    -- open transactions
  SELECT max(age(xmin)) FROM pg_replication_slots;         -- replication slots
  SELECT max(age(backend_xmin)) FROM pg_stat_replication;  -- standbys with hot_standby_feedback
  ```

## When the database fails

| What happens | What taskmill does | What you do |
|---|---|---|
| A failover or outage of a few seconds | Workers reconnect at their next try. Running tasks continue, and their results are saved. Nothing runs twice. | Nothing |
| A failover or outage of more than about 20 seconds | Workers cannot renew their leases, so they cancel their running tasks. The tasks run again once the database is back. | Make handlers safe to run twice. Or raise `LeaseDuration`: see the note below |
| The database is down when a process starts | `Migrate` and `NewClient` return an error, so your process exits. Kubernetes restarts it until the database is back. | Fix the database. The pods recover by themselves |
| A failover with asynchronous replication | The last commits can be lost, and with them tasks enqueued in them, like any other row | Use synchronous replication if that matters |
| You restore a backup | Tasks go back to their state at that time. Tasks that finished later run again. Tasks enqueued later are gone. | Make handlers safe to run twice |

> [!NOTE]
> A worker renews its leases every third of `LeaseDuration` (30 s by
> default). It gives a task up once it could not renew the lease for a whole
> `LeaseDuration`. To ride out longer failovers, set `LeaseDuration` to more
> than 1.5 times your provider's failover time. The cost: when a worker
> crashes, its tasks wait that long before another worker runs them.

## Keep clocks in sync

taskmill takes most times from the database, but not all. A producer sets
`RunAt` with its own clock, and workers name the daily history partitions by
their own clock. Keep every server's clock in sync with NTP.

## See also

- [Connection poolers](connection-poolers.md): PgBouncer, RDS Proxy, Supabase, and Neon
- [Monitoring](monitoring.md): alerts for old snapshots and more
- [Schema](../design/schema.md): taskmill's tables, for contributors
- [`NewClient`](https://pkg.go.dev/github.com/code19m/taskmill#NewClient) and [`Migrate`](https://pkg.go.dev/github.com/code19m/taskmill#Migrate) on pkg.go.dev

**Next:** [Connection poolers](connection-poolers.md)
