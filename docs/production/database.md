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
role. Then `Migrate`, or [your own tool](../recipes/use-your-migration-tool.md), only fills it:

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

Why: a running task holds no connection of its own, but handlers that use the
same pool can take every connection. In a pool of this size, the worker keeps
one in reserve while `Run` runs, so its tasks'
[leases](../concepts/glossary.md#lease) still renew, while fetches wait. For
that, the idle limit of taskmill's role, such as `idle_session_timeout` or a
proxy's, must be longer than a third of the lease. A smaller pool spares no
reserve: renewals wait too, leases can run out, and tasks run twice.

The 5 extra connections are for the worker's own work. `Run` warns at start
if the pool is smaller: `the pool has fewer connections than the worker's
total Concurrency plus 5`. A wait for a connection later is logged as `the
pool was full, so the worker waited for a connection`, and counted in
`taskmill_worker_pool_waits_total`.

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
| Handlers use the database for short queries | **One pool** for taskmill and your handlers, sized as above | Simple. The worker warns if the pool is too small |
| Handlers hold connections for a long time | A pool of their own for the handlers | taskmill's work never waits for them. Keep taskmill's pool at `Concurrency` plus 5 anyway: `pool_max_conns` is only a cap, since pgx opens only the connections that it uses |

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
| A failover or outage of more than about 20 seconds | Workers cannot renew their leases, so they cancel their running tasks. The tasks run again once the database is back, or become dead if that was their last attempt. | Make handlers safe to run twice. Or raise `LeaseDuration`: see below |
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

- When a worker crashes, its tasks wait that long before another worker runs
  them.
- `Worker.Health` fails that much later, and the dashboard marks the worker
  as stale that much later.
- A crashed worker stays in the list of workers for twice its lease, and for
  at least 5 minutes.

So keep `LeaseDuration` as short as your failover allows.

## Keep or remove a day of old history

The maintainer drops a history partition once its whole day is older than
`HistoryRetention`. It marks the partition with a comment, detaches it, and
drops it. If the drop fails, a later pass finishes it.

- **To keep a day,** detach its partition yourself before it expires, or
  copy it. taskmill drops a table that is no partition only if the table
  has taskmill's mark, so yours stays. If `\dt+
  taskmill.task_history_20261001` shows a comment of taskmill, a pass has
  marked the partition already: clear the mark first, with `COMMENT ON
  TABLE taskmill.task_history_20261001 IS NULL;`.
- **A detached table without the mark stays for good.** A taskmill release
  that set no mark yet may have left one, when a drop failed. `\d+
  taskmill.task_history` lists the partitions: a table named
  `task_history_` and a day that is not in that list is detached. Remove it
  by hand:

  ```sql
  DROP TABLE taskmill.task_history_20261001;
  ```

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

## Use any time zone

Set the `TimeZone` of your database, of its roles, and of your servers as
you like: taskmill works the same in each. Every time that it stores is a
`timestamptz`, a moment that is the same in every time zone.

| Where a time zone shows | Which one |
|---|---|
| A day of history, and a day of a retention period | UTC, and always 24 hours |
| The expression of a [schedule](../guides/schedules.md) | The schedule's `Location`: UTC unless you set it |
| A time that the Go API returns | A moment: compare two with `Equal`, and choose a zone with `In` to show one |
| The dashboard | The reader's, named beside each exact time. UTC without JavaScript |

## See also

- [Backups, failover, and replicas](backups-and-failover.md): restore, fail over, and what to check after
- [Capacity planning](capacity.md): how much disk and how many connections
- [Storage schema](../design/schema.md): taskmill's tables, for contributors
- [`NewClient`](https://pkg.go.dev/github.com/code19m/taskmill#NewClient) and [`Migrate`](https://pkg.go.dev/github.com/code19m/taskmill#Migrate) on pkg.go.dev

**Next:** [Connection poolers](connection-poolers.md)
