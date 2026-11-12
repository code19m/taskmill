# Storage schema

This page maps the [task lifecycle](task-lifecycle.md) onto PostgreSQL: the
tables and the indexes, with the reasoning behind each choice. Three more
pages cover the rest:

| Page | What it covers |
|---|---|
| [Key statements](schema-statements.md) | The statements that enqueue, fetch, complete, list, and change tasks |
| [FIFO groups and deduplication](schema-groups.md) | How a group keeps its order, and how a dedupe key admits one task |
| [Maintenance and notifications](schema-maintenance.md) | What the maintainer runs, how it counts the queues, and how workers are woken |

The exact definitions are in
[`internal/migrations`](../../internal/migrations), starting with
`0001_initial.sql`, and the exact statements are in [`sql.go`](../../sql.go).
The statements were measured on PostgreSQL 15 and 18 with 1.7 million tasks.

All objects live in one schema, `taskmill` by default. The name is
configurable, so several schemas can share a database. It must match
`^[a-z_][a-z0-9_]{0,62}$`, which makes it safe to use unquoted.

## Principles

1. **The hot table holds only unfinished work.** `tasks` holds tasks that can
   still run, plus dead and canceled tasks waiting for an operator. Completed
   tasks move to a history table that is partitioned by day, and retention
   drops whole partitions. The hot table stays small, so vacuum keeps up.
2. **One partial index per access path.** Each index holds only the rows in the
   states its query needs. Fetching never reads future, pending, or finished
   tasks, however many there are.
3. **Most transitions are one statement.** A few take several statements in
   one transaction: those that involve a FIFO group, `Cancel`, `Retry`, and
   `Delete`, which lock the task before they change it, and fetches from a
   queue with a `MaxRunning` limit. No statement depends on session state, so
   all of them work inside the caller's transaction and behind poolers in
   transaction mode, such as PgBouncer.
4. **The database enforces the lifecycle's invariants** with `CHECK`
   constraints, as a second line of defense behind the code.
5. **Frequent updates touch no indexed column.** Lease renewals change only
   `lease_expires_at`, which no index contains, so PostgreSQL can update the
   row in place (a HOT update) without writing to any index.
6. **Every timestamp that taskmill sets comes from the database clock**
   (`now()`). Only a run time that the application passes in comes from the
   application's clock.

## Tables

| Table | Purpose | Write rate |
|---|---|---|
| `tasks` | Unfinished, dead, and canceled tasks | Hot: every enqueue, fetch, lease renewal, and result |
| `task_history` | Completed tasks, one partition per day | Hot: one insert per completion; append-only |
| `groups` | One row per FIFO group with unfinished tasks or a dead task that halts it | Warm: grouped enqueues and completions only |
| `dedupe_keys` | Dedupe keys and their windows | Warm: deduplicated enqueues only |
| `queues` | Per-queue state: paused, and the `MaxRunning` limit | Cold |
| `settings` | Retention for the whole schema, in a single row | Cold |
| `schedules` | Recurring schedules and their next run times | Cold |
| `workers` | Running worker processes | Cold: a check-in per process every third of `LeaseDuration`, 10 s by default |
| `leader` | The elected maintainer | Cold |
| `queue_stats` | Task counts per queue and state, with each count's oldest due task, and each queue's halted groups and most failures in a row, refreshed by the maintainer | Cold |
| `activity` | Task counts and ended attempts per minute and queue, for a day | Cold: a few rows every 10 s |
| `migrations` | Applied schema migrations | Cold |

### `tasks`

- **Columns are ordered for compact rows:** fixed-width columns first, from
  the widest to the narrowest, then variable-width ones.
- **`id` is the task's stable identity.** It is unique within a schema and
  is kept when a task moves to history. Messages that cross service
  boundaries need globally unique IDs. The outbox generates those (a UUIDv7
  in the message) rather than widening every task row.
- **`lease_token` is a counter, not a random value.** It grows on every fetch
  and is never reset, not even by an operator retry, so a stale attempt's
  token can never match again. Unlike `attempt`, it never goes down, so
  snoozes and releases, which give attempts back, are safe.
- **`errors` is bounded.** Each failure appends an entry with the attempt
  number, the time, the message, and, for failures that taskmill detects
  itself, a code such as `lease_lost`. Beyond 20 entries the oldest is
  dropped. The Go side cuts each message to 4 KiB before storing it.
- **`fillfactor = 80`** leaves room in each page for updated row versions,
  so lease renewals can be HOT updates. Measured: renewing 5,000 leases on
  densely packed pages was 25% HOT in the first round, limited by the free
  space, and 97% HOT after PostgreSQL pruned old row versions.
- **Autovacuum is aggressive on `tasks`:** a scale factor of 1%, a threshold
  of 1,000 rows, and no cost delay. Every fetch and result leaves a dead row
  version.
- **`queue` and `kind` are stored as text,** not as IDs into lookup tables.
  The rows are slightly larger, but there are no joins, no lookups, and no
  registration step before the first enqueue.

### `task_history`

Completed tasks, partitioned by `finalized_at`, one partition per UTC day. It
is append-only: rows are never updated, and retention drops whole partitions,
which is instant and leaves nothing for vacuum to clean up. Two indexes serve
the list pages: `(queue, id)` for one queue, and `(id)` for `Get` and for all
queues.

History retention is one setting for the whole schema, stored in
`settings`, because dropping a day's partition removes every queue's rows for
that day. Per-queue periods would need row deletes, which the partitions exist
to avoid. A kind that needs no history skips it with
`HandlerOptions.DiscardCompleted`.

### `groups`

One row per FIFO group that has unfinished tasks or a dead task that halts
it. Its row lock serializes the enqueues and completions of a group. Its
`tasks` column counts the group's unfinished tasks and the dead head that
halts it, if any, which decides the head. A group halted by its only task
counts 1. `halted_task_id` names that dead head.

Two partial indexes cover the halted groups only, so they stay small:

- One on `halted_task_id` lets retention skip the dead heads that keep
  their groups halted, without scanning every group.
- One on `(queue, group_key)` lets `Client.Groups` page through the halted
  groups in its order, and lets the maintainer count them per queue.

### `dedupe_keys`

The key is a SHA-256 hash, computed in Go, of the queue, the kind, and the
caller's key, so index entries stay small whatever the caller's key looks
like. A key blocks duplicates until `expires_at`, unless its task is canceled
or deleted. The index on `expires_at` serves retention, and the index on
`task_id` lets `Cancel` and `Delete` release a task's key.

### Settings, coordination, and bookkeeping

- `queues` holds `paused_at` and `max_running` for the queues that have them.
- `settings` has exactly one row, enforced by a `CHECK` on its key. Settings
  live in the database rather than in each process's configuration, so every
  worker that becomes the maintainer applies the same retention.
- `schedules` holds each schedule's definition, next run time, and pause
  state. The maintainer loads the schedules that it defines by name, and
  checks in Go which are due. No statement uses the partial index on
  `next_run_at` today.
- `workers` gets a check-in from every worker process; `fillfactor = 50`
  keeps those updates HOT. A check-in sets the column `heartbeat_at`. The
  column keeps that name: renaming it needs a migration, and the workers of
  an older release would fail on it during a rolling deploy.
- `leader` elects the maintainer with a lease row rather than an advisory
  lock. Session advisory locks need a dedicated connection and do not work
  behind poolers in transaction mode. A lease row works everywhere.

### `activity`

One row per minute and queue, kept for 24 hours, behind `Client.Activity`
and the dashboard's charts. Two writers share a row, each with its own
columns:

- **The maintainer** copies the counts it stores in `queue_stats` into the
  current minute, every 10 seconds, with zeros for known queues that have no
  tasks. A minute with `NULL` counts had no maintainer, which a chart shows
  as a gap rather than as an empty queue.
- **Each worker** counts the attempts it ends, completed and failed, in
  memory, and adds them to their minutes every 10 seconds, in a loop of its
  own so that a slow write never delays a check-in. Counting in the
  statements that record attempts would make every worker update the same
  row with every result; adding them every 10 seconds costs one small upsert
  per worker. A worker that crashes loses the counts of its last few
  seconds, and one that stops writes them before it returns.

Both writers lock rows in the order of minute, then queue name in byte
order, so they cannot deadlock. `fillfactor = 70` keeps the repeated updates
HOT, and the primary key leads with the minute, which serves both reading a
period and deleting old rows.

## Indexes on `tasks`

| Index | Key | Rows it holds | Used by |
|---|---|---|---|
| `tasks_pkey` | `id` | All | Every update by ID; lease renewals; completions; listing all tasks |
| `tasks_fetch_idx` | `queue, priority DESC, run_at, id` | `available` | Fetch; filling batches; counting available tasks |
| `tasks_due_idx` | `run_at`, including `queue` and `state` | `scheduled`, `retryable` | Making due tasks available; finding when the next task is due; counting scheduled and retryable tasks |
| `tasks_running_idx` | `queue` | `running` | Lease rescue; `MaxRunning` limits; counting running tasks |
| `tasks_pending_idx` | `queue, group_key, id` | `pending` | Promoting the next task of a group; counting pending tasks |
| `tasks_finished_idx` | `queue, state, finalized_at` | `dead`, `canceled` | Retention; counting dead and canceled tasks |

The fetch index's key matches the fetch query's `ORDER BY` exactly. PostgreSQL
reads the first rows of the index in order and stops at the `LIMIT`, with no
sort, however long the backlog. Measured: fetching 10 tasks from a queue with
300,000 due and 1,000,000 future tasks took under 1 ms.

`tasks_running_idx` deliberately does not contain `lease_expires_at`: if it
did, every lease renewal would write an index entry. `INCLUDE` adds columns
to an index's entries without making them part of its key. With `queue` and
`state` in `tasks_due_idx`, and `state` in `tasks_finished_idx`, statistics
count those tasks from the index alone, without reading the table (an
index-only scan).

## Operational notes

- **Long transactions slow the queue down.** A snapshot that stays open,
  anywhere in the database, stops vacuum from removing dead row versions. The
  fetch index then fills with dead entries and fetches slow down. The
  [database guide](../production/database.md) says what to set and what to
  alert on.
- **History partitions are created ahead of time.** `Migrate`, each worker
  when it starts, and the maintainer keep partitions for today and the next
  6 days, by UTC day. The worker does it because a schema applied with
  `Migrations` has none, and its first tasks may complete before the
  maintainer's first pass.
- **A partition is created apart, then attached.** It is created with
  `CREATE TABLE ... (LIKE ...)` and then attached with
  `ALTER TABLE ... ATTACH PARTITION` under a 5-second lock timeout, because
  `CREATE TABLE ... PARTITION OF` locks the whole history table and blocks
  completions. Measured: with a completion's transaction open,
  `PARTITION OF` timed out on its lock, while `LIKE` with `ATTACH` finished
  in 0.07 seconds.
- **Old partitions are detached, then dropped.** The detach is
  `DETACH PARTITION ... CONCURRENTLY`, bounded by 30 seconds. An interrupted
  detach is finished next time with `DETACH PARTITION ... FINALIZE`. These
  commands need ownership of `task_history`.
- **There is no default partition.** Attaching a partition would then have to
  scan and lock it. If a day's partition is missing, completions skip history
  and the worker logs an error, so the gap shows up in alerts.
- **Schema changes are versioned.** Each migration is recorded in
  `migrations`. `Migrate` applies all pending migrations in one transaction,
  under a transaction-scoped advisory lock, so only one process migrates at a
  time. A migration therefore cannot use `CREATE INDEX CONCURRENTLY`; a
  future migration that indexes a large existing table will need a step of
  its own outside the transaction.

## See also

- [Task lifecycle and guarantees](task-lifecycle.md): the contract that this schema keeps
- [Benchmarks](benchmarks.md): what the statements cost under load
- [Database](../production/database.md): what to set in production

**Next:** [Key statements](schema-statements.md)
