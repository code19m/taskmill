# Database

This page helps you set up PostgreSQL for taskmill: which version, which
role, how many connections, and what to watch so the queue stays fast.

## Use PostgreSQL 15 or newer, and its primary

taskmill needs PostgreSQL 15 or newer, and nothing more: no extension, and no
superuser. CI tests community PostgreSQL 15 to 18. Managed services are
expected to work, but CI does not test them: Amazon RDS and Aurora, Google
Cloud SQL and AlloyDB, Azure Database for PostgreSQL, Neon, and Supabase.

Connect every process to the **primary**. taskmill writes on every fetch, so
use the writer endpoint, such as Aurora's cluster endpoint, never a read
replica. [Keep taskmill away from read replicas](backups-and-failover.md#keep-taskmill-away-from-read-replicas)
shows how to make pgx refuse one.

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
role. `Migrate` then only fills it:

```sql
CREATE ROLE shop LOGIN PASSWORD '...';
CREATE SCHEMA taskmill AUTHORIZATION shop;
```

| Who | Role |
|---|---|
| Workers, and whatever runs `Migrate` | The owner of taskmill's tables, or a member of the owner role |
| Web processes that only enqueue, and a dashboard with actions | The same role is simplest. Or a role that reads and writes, but owns nothing |
| A read-only dashboard | A role that may only read |

[Give each process the smallest role](security.md#give-each-process-the-smallest-role)
has the SQL for the last two rows.

## Keep the isolation level at READ COMMITTED

taskmill's own statements need READ COMMITTED, PostgreSQL's default. If your
database or role sets another default, `Migrate` still works, but
`NewClient` fails. Set the level back for taskmill's pool:

```go
cfg, err := pgxpool.ParseConfig(os.Getenv("DATABASE_URL"))
if err != nil {
    return err
}
cfg.ConnConfig.RuntimeParams["default_transaction_isolation"] = "read committed"
pool, err := pgxpool.NewWithConfig(ctx, cfg)
```

| Situation | What to use |
|---|---|
| Your own Go process | The code above |
| `taskmill-ui`, which has no code to change | `default_transaction_isolation=read%20committed` at the end of `TASKMILL_DATABASE_URL` |
| The pool goes through PgBouncer, which by default rejects this connection parameter | `ALTER ROLE shop SET default_transaction_isolation = 'read committed'`. It applies to the role's new connections |

Your own transactions, which you pass to the `...Tx` methods, may use any
level. At REPEATABLE READ and SERIALIZABLE, `CompleteTx` has one rule:
[Use REPEATABLE READ or SERIALIZABLE](../guides/transactions.md#use-repeatable-read-or-serializable).

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

The 5 extra connections are for the worker's own work. If the pool is full
and a lease renewal has to wait for a connection, the worker logs the warning
`the pool was full, so renewing leases waited`, at most once a minute.

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
| Handlers use the database for short queries | **One pool** for taskmill and your handlers, sized as above | Simple. The worker logs a warning if the pool is too small |
| Handlers hold connections for a long time | A pool of their own for the handlers | taskmill's work never waits for them. taskmill's pool can then be smaller than `Concurrency` plus 5, and the warning tells you if it is too small |

## Keep long transactions from slowing the queue

A *snapshot* is a transaction's view of the data. While an old snapshot stays
open anywhere in the database, vacuum cannot clean taskmill's busy `tasks`
table. Every fetch then reads past more dead rows (old row versions that
vacuum has not removed), so throughput falls and tasks wait longer.

Common causes are sessions left idle in a transaction, `pg_dump`, long
reports, prepared transactions that nobody commits, replicas with
`hot_standby_feedback`, and replication slots that nobody reads.

- Set `idle_in_transaction_session_timeout`, and a `statement_timeout` for
  the roles that run reports.
- Don't set `idle_session_timeout` for the role that taskmill uses. A
  worker's notification connection waits without sending queries, so
  PostgreSQL would close it again and again.
- Run long reports on a replica without `hot_standby_feedback`.
- Keep autovacuum on. taskmill's migrations already tune it for the `tasks`
  table. If that table still grows at a high throughput, lower
  `autovacuum_naptime`, so that autovacuum starts more often. It is 1 minute
  by default, for the whole server, and taskmill's benchmarks did not need
  less ([Performance](../reference/performance.md#resource-use)).
- Watch what blocks vacuum. [Monitoring](monitoring.md#what-to-alert-on)
  has the thresholds:

  ```sql
  -- Seconds since the oldest transaction that blocks vacuum began
  SELECT coalesce(max(extract(epoch FROM now() - xact_start)), 0)
  FROM pg_stat_activity WHERE backend_xmin IS NOT NULL OR backend_xid IS NOT NULL;
  -- Seconds since the oldest prepared transaction was prepared
  SELECT coalesce(max(extract(epoch FROM now() - prepared)), 0) FROM pg_prepared_xacts;
  -- Replication slots and standbys: an age in transactions, not in seconds
  SELECT max(age(xmin)) FROM pg_replication_slots;
  SELECT max(age(backend_xmin)) FROM pg_stat_replication;
  ```

## When the database fails

| What happens | What taskmill does | What you do |
|---|---|---|
| A failover or outage of a few seconds | Workers reconnect at their next try. Running tasks continue, and their results are saved. Nothing runs twice. | Nothing |
| A failover or outage of more than about 20 seconds | Workers cannot renew their leases, so they cancel their running tasks. The tasks run again once the database is back. | Make handlers safe to run twice. Or raise `LeaseDuration`: see below |
| The database is down when a process starts | `Migrate` and `NewClient` return an error, so your process exits. Kubernetes restarts it until the database is back. | Fix the database. The pods recover by themselves |
| A failover with asynchronous replication | The last commits can be lost, and with them tasks enqueued in them, like any other row | Use synchronous replication if that matters. Then [check these points](backups-and-failover.md#check-these-points-after-a-failover) |
| You restore a backup | Tasks go back to their state at that time, and task IDs repeat | Follow [Restore a backup](backups-and-failover.md#restore-a-backup) |

### Raise `LeaseDuration` for a slow failover

`LeaseDuration` is 30 seconds by default, and a worker renews its leases
every third of it: every 10 seconds. It stops a task's handler once it could
not renew the lease for a whole `LeaseDuration`. To keep tasks running
through a longer failover, set `LeaseDuration` to more than 1.5 times your
provider's failover time.

A longer lease has costs, because other timings follow it:

| `LeaseDuration` | What it costs |
|---|---|
| Any longer value | When a worker crashes, its tasks wait that long before another worker runs them. `Worker.Health` also fails that much later |
| 90 seconds or more | The dashboard marks healthy workers as stale. It expects a check-in every 30 seconds, and a worker checks in every third of its lease |
| 15 minutes or more | The maintainer removes healthy workers from the list after 5 minutes without a check-in. Each one logs `worker was removed as stale; registering again` |

So keep `LeaseDuration` under 90 seconds if you can, and always under 15
minutes.

## Keep clocks in sync

taskmill takes most times from the database, but not all. Keep every
server's clock in sync with NTP, because these use the app's clock:

- A [producer](../concepts/glossary.md#producer), the code that enqueues,
  sets `RunAt`.
- `taskmill_stats_age_seconds` compares the app's clock with the time that
  the database stored. A clock that is a minute ahead fires its alert for no
  reason.

The daily history partitions follow the database's clock, in UTC: the
database says which days need a partition, and which partitions have
expired. So a worker whose clock is wrong cannot drop history too early.

## See also

- [Backups, failover, and replicas](backups-and-failover.md): restore, fail over, and what to check after
- [Capacity planning](capacity.md): how much disk and how many connections
- [Storage schema](../design/schema.md): taskmill's tables, for contributors
- [`NewClient`](https://pkg.go.dev/github.com/code19m/taskmill#NewClient) and [`Migrate`](https://pkg.go.dev/github.com/code19m/taskmill#Migrate) on pkg.go.dev

**Next:** [Connection poolers](connection-poolers.md)
