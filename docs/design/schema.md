# Storage schema

This document maps the [task lifecycle](task-lifecycle.md) onto PostgreSQL:
the tables, the indexes, and the statements that move tasks between states,
with the reasoning behind each choice. The exact definitions are in
[`internal/migrations/0001_initial.sql`](../../internal/migrations/0001_initial.sql),
and the exact statements are in [`sql.go`](../../sql.go). The statements were
measured on PostgreSQL 15 and 18 with 1.7 million tasks.

All objects live in one schema, `taskmill` by default. The name is
configurable, so several installations can share a database. It must match
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
   queue with a concurrency limit. No statement depends on session state, so
   all of them work inside the caller's transaction and behind
   transaction-pooling proxies such as PgBouncer.
4. **The database enforces the lifecycle's invariants** with `CHECK`
   constraints, as a second line of defense behind the code.
5. **Frequent updates touch no indexed column.** Heartbeats change only
   `lease_expires_at`, which no index contains, so PostgreSQL can update the
   row in place (a HOT update) without writing to any index.
6. **Every timestamp that taskmill sets comes from the database clock**
   (`now()`). Only a run time that the application passes in comes from the
   application's clock.

## Tables

| Table | Purpose | Write rate |
|---|---|---|
| `tasks` | Unfinished, dead, and canceled tasks | Hot: every enqueue, fetch, heartbeat, and result |
| `task_history` | Completed tasks, one partition per day | Hot: one insert per completion; append-only |
| `groups` | One row per FIFO group with unfinished tasks | Warm: grouped enqueues and completions only |
| `dedupe_keys` | Dedupe keys and their windows | Warm: deduplicated enqueues only |
| `queues` | Per-queue state: paused, and the concurrency limit | Cold |
| `settings` | Installation-wide retention, in a single row | Cold |
| `schedules` | Recurring schedules and their next run times | Cold |
| `workers` | Running worker processes | Cold: a heartbeat per process every 10 s |
| `leader` | The elected maintainer | Cold |
| `queue_stats` | Task counts per queue and state, with each count's oldest due task, refreshed by the maintainer | Cold |
| `activity` | Task counts and ended attempts per minute and queue, for a day | Cold: a few rows every 10 s |
| `migrations` | Applied schema migrations | Cold |

### `tasks`

- **Columns are ordered for compact rows:** fixed-width columns first, from
  the widest to the narrowest, then variable-width ones.
- **`id` is the task's stable identity.** It is unique within an
  installation and is kept when a task moves to history. Messages that cross
  service boundaries need globally unique IDs; the outbox generates those (a
  UUIDv7 in the message) rather than widening every task row.
- **`lease_token` is a counter, not a random value.** It grows on every fetch
  and is never reset, not even by an operator retry, so a stale attempt's
  token can never match again. Unlike `attempt`, it never goes down, so
  snoozes and releases, which give attempts back, are safe.
- **`errors` is bounded.** Each failure appends an entry with the attempt
  number, the time, the message, and, for failures that taskmill detects
  itself, a code such as `lease_expired`. Beyond 20 entries the oldest is
  dropped. The Go side cuts each message to 4 KiB before storing it.
- **`fillfactor = 80`** leaves room in each page for updated row versions,
  so heartbeats can be HOT updates. Measured: renewing 5,000 leases on
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

History retention is one setting for the whole installation, stored in
`settings`, because dropping a day's partition removes every queue's rows for
that day. Per-queue periods would need row deletes, which the partitions exist
to avoid. A kind that needs no history skips it with
`HandlerOptions.DiscardCompleted`.

### `groups`

One row per FIFO group that has unfinished tasks. Its row lock serializes the
enqueues and completions of a group. Its `tasks` column counts the group's
unfinished tasks, which decides the head, and `halted_task_id` names the dead
head that halts it. A partial index on `halted_task_id` lets retention skip
the dead heads that keep their groups halted, without scanning every group.

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
  state. A partial index on `next_run_at` finds the due ones.
- `workers` gets a heartbeat from every worker process; `fillfactor = 50`
  keeps those updates HOT.
- `leader` elects the maintainer with a lease row rather than an advisory
  lock. Session advisory locks need a dedicated connection and do not work
  behind transaction-pooling proxies; a lease row works everywhere.

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
  own so that a slow write never delays a lease's heartbeat. Counting in the
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
| `tasks_pkey` | `id` | All | Every update by ID; heartbeats; completions; listing all tasks |
| `tasks_fetch_idx` | `queue, priority DESC, run_at, id` | `available` | Fetch; filling batches; counting available tasks |
| `tasks_due_idx` | `run_at`, including `queue` and `state` | `scheduled`, `retryable` | Making due tasks available; the next wake-up time; counting scheduled and retryable tasks |
| `tasks_running_idx` | `queue` | `running` | Lease rescue; concurrency limits; counting running tasks |
| `tasks_pending_idx` | `queue, group_key, id` | `pending` | Promoting the next task of a group; counting pending tasks |
| `tasks_finished_idx` | `queue, state, finalized_at` | `dead`, `canceled` | Retention; counting dead and canceled tasks |

The fetch index's key matches the fetch query's `ORDER BY` exactly. PostgreSQL
reads the first rows of the index in order and stops at the `LIMIT`, with no
sort, however long the backlog. Measured: fetching 10 tasks from a queue with
300,000 due and 1,000,000 future tasks took under 1 ms.

`tasks_running_idx` deliberately does not contain `lease_expires_at`: if it
did, every heartbeat would write an index entry. `INCLUDE` adds columns to an
index's entries without making them part of its key. With `queue` and
`state` in `tasks_due_idx`, and `state` in `tasks_finished_idx`, statistics
count those tasks from the index alone, without reading the table (an
index-only scan).

## Key statements

The statements send batches as arrays and use `unnest`, so a batch of any size
is one round trip with fixed SQL text, which lets prepared statements be
reused.

### Enqueue

Every batch goes through one of two statements. Batches with dedupe keys or
FIFO groups go through one that also claims the keys and counts the groups
(see [FIFO groups](#fifo-groups) and [Deduplication](#deduplication)). All
other batches go through a lean one. Both reserve IDs from the
`tasks_id_seq` identity sequence first, so results map back to their inputs.

### Fetch

```sql
WITH picked AS (
    SELECT id
    FROM taskmill.tasks
    WHERE state = 'available' AND queue = $1
    ORDER BY priority DESC, run_at, id
    LIMIT $2
    FOR UPDATE SKIP LOCKED
)
UPDATE taskmill.tasks t
SET state            = 'running',
    attempt          = t.attempt + 1,
    lease_token      = t.lease_token + 1,
    lease_expires_at = now() + $3 * interval '1 microsecond',
    attempted_at     = now(),
    attempted_by     = $4
WHERE t.id = ANY(ARRAY(SELECT id FROM picked))
RETURNING t.*;
```

`FOR UPDATE SKIP LOCKED` lets many workers fetch at once: each one skips the
rows another is taking instead of waiting for them. A worker that serves
several queues fetches from each queue separately, so every fetch uses the
index order.

**The fetch must stay on its index,** and two things keep it there:

- **Sorts and bitmap scans are off for the fetch.** A queue's table changes
  so fast that its statistics are often far off. Measured: after a backlog of
  200,000 tasks arrived faster than autovacuum analyzed the table, the
  planner estimated 3 available tasks, and read and sorted all of them on
  every fetch, 15 ms instead of 1 ms. So each fetch runs in a transaction of
  its own that first turns sorts and bitmap scans off with `set_config(...,
  true)`. The worker sends `BEGIN`, the settings, the fetch, and `COMMIT` as
  one batch, in one round trip.
- **The update finds the picked tasks through an array of their IDs.** A
  join with `picked` lets a generic plan, which cannot know how few rows
  `LIMIT $2` returns, hash-join against the whole table.

For a queue with a concurrency limit, the fetch runs in a transaction that
first locks the queue's row in `queues`, then counts its running tasks
through `tasks_running_idx`, and fetches at most the remaining capacity. This
serializes fetches for limited queues only.

A worker fetches one task per free slot. If the fetched tasks include kinds
that have a batch handler, a second statement fills each batch up to its
size. It is the same fetch, limited to one kind, and it looks at no more than
the first 1,000 available tasks of the queue, so it stays cheap in a queue of
many kinds. A batch takes one slot, so every fetched task starts at once.

### Heartbeat

One statement renews every lease a worker holds. It returns each task that
the worker's attempts still own, and marks the ones it renewed. A task
missing from the result lost its lease, and the worker cancels that attempt's
context. A task with a cancellation request is canceled the same way.

The statement locks with `SKIP LOCKED`, so one locked row, such as a task that
an application transaction is canceling, cannot hold up the renewal of all
the others. A skipped task is still owned: the worker keeps its attempt
running and renews it at the next heartbeat. An attempt that completed its
task with `CompleteTx` is never canceled as lost: its own transaction holds
the row until it commits.

### Complete

One statement completes a batch: it deletes the tasks whose attempts still
hold their lease tokens, and inserts them into history, except those of kinds
that discard completed tasks. A task missing from the result had lost its
lease, and its result is discarded. The same statement, run inside the
handler's transaction, is transactional completion.

Ungrouped tasks complete with this one statement, without a transaction of
its own. Grouped tasks complete in one transaction per batch, which also
updates their groups (see [FIFO groups](#fifo-groups)). If the database is
unreachable, the worker retries a completion with backoff until the
attempt's lease would expire.

### Fail, snooze, and release

A failure is one `UPDATE` that checks the lease token, sets the state that
the kind's retry policy chose (`retryable` with a new run time, or `dead`),
and appends the error, dropping the oldest once there are 20. A snooze and a
release are similar updates that also give the attempt back
(`attempt = attempt - 1`). All three cancel the task instead if its
cancellation was requested, so a request whose notification was missed still
takes effect when the attempt ends. For a grouped task, the update runs in a
transaction that also updates the group if the task finished.

### Maintenance

| Step | What it does |
|---|---|
| Promote | Makes up to 1,000 due `scheduled` and `retryable` tasks `available` per statement, through `tasks_due_idx`, and notifies their queues in the same statement. It runs on its own, so a slow step never delays it. |
| Rescue | Every second, fails up to 1,000 attempts per statement whose leases expired. It uses the default backoff, because the maintainer does not know each kind's retry policy, and caps the exponent so that a task with thousands of attempts cannot overflow it. |
| Retention | Every minute, deletes old dead and canceled tasks, expired dedupe keys, and activity older than a day in batches of 1,000, skipping locked rows. Dead heads that halt their groups are kept. Also deletes workers without a heartbeat for 5 minutes, and schedules that the maintainer does not define and that did not run or change for a day. |
| History | Keeps partitions for today and the next 6 days, and drops a partition once its whole day is older than the retention period (see [Operational notes](#operational-notes)). |
| Statistics | Every 10 seconds, counts each queue's tasks per state, finds each count's oldest due task, and stores both in `queue_stats` and the counts in the current minute of `activity`. |
| Election | An upsert on `leader` that succeeds only for the current leader, or when the lease has expired. |

**Statistics read indexes only, and every count is capped,** so the cost
stays bounded however large the backlog: up to 10,000 tasks per queue and
state, and 100,000 scheduled and retryable tasks in all. A recursive query
finds the next queue name with one index probe each, a technique called a
loose index scan, so finding the queues costs one probe per queue, not one
row per task. Scheduled and retryable tasks are read from `tasks_due_idx`,
which is ordered by run time, not by queue, so they are counted together. A
count that reaches its cap is stored with `capped = true`: it is a lower
bound.

**Lag comes with the counts.** Each row of `queue_stats` keeps, in
`oldest_due_at`, the run time of the oldest due task it counted: for
`available`, the first entry of `tasks_fetch_idx` per priority, one more
probe per queue and priority; for `scheduled` and `retryable`, the oldest
task whose run time has passed, which the count meets first because it reads
`tasks_due_idx` in run time order. A queue's lag is then the age of its
oldest `oldest_due_at` when it was counted, which `Client.Queues` reads
without touching `tasks`. Only if more than 100,000 scheduled and retryable
tasks are overdue can a queue's overdue tasks fall past the cap; the oldest
ones, which set the largest lag, are always read.

### Listing

`Client.List` and the web interface page through tasks by `id`, newest
first, with keyset pagination: each page starts below the last `id` of the
previous one. Unfinished tasks and history are read in two branches, each
limited on its own, and merged. Each page runs with custom query plans
(`plan_cache_mode = force_custom_plan`), so the planner sees which filters are
set and uses the partial index that matches them, and with a 5-second
statement timeout.

### Operator actions

- **Cancel** cancels a waiting task at once, and flags a running one with
  `cancel_requested_at`. A `pending` task's group row is locked first.
- **Retry** moves a `dead` or `canceled` task back to `available` with
  `attempt = 0`, keeping its error history, or to `pending` if its group has
  other unfinished tasks.
- **Cancel and Delete** release the task's dedupe key in the same
  transaction.

## FIFO groups

**The problem.** In a FIFO group only the head may run. If the rest of the
group were `available`, every fetch would have to skip over them, and a group
with a large backlog would slow every fetch down: the same problem that
separate `scheduled` tasks avoid.

**The design.** Tasks behind the head are in a separate state, `pending`, which
no fetch reads. Measured: fetching 100 heads from a queue with 100,000 pending
tasks read no pending task and took under 0.2 ms. The `groups` row counts
the group's unfinished tasks.

- **Enqueue** first upserts the group's row, which locks it, and then inserts
  the task. The task is the head (`available` or `scheduled`) if the count is
  now 1, and `pending` otherwise. Both steps are one statement (simplified
  here to one task; the real statement takes a batch):

  ```sql
  WITH g AS (
      INSERT INTO taskmill.groups AS g (queue, group_key, tasks) VALUES ($1, $2, 1)
      ON CONFLICT (queue, group_key) DO UPDATE SET tasks = g.tasks + 1, updated_at = now()
      RETURNING tasks
  )
  INSERT INTO taskmill.tasks (queue, kind, state, run_at, group_key, payload, max_attempts)
  SELECT $1, $3, CASE WHEN g.tasks = 1 THEN 'available' ELSE 'pending' END::taskmill.task_state,
         now(), $2, $4, $5
  FROM g;
  ```

- **When the head finishes**, one transaction finishes it, decrements the
  group's count, which locks the group's row, and then, in a statement of its
  own, promotes the oldest `pending` task through `tasks_pending_idx`, or
  deletes the group's row if the count reached zero.

**Why a separate statement.** The promotion must see tasks that were enqueued
while the decrement waited for the group's lock. At READ COMMITTED, each
statement sees the data as of its own start. One combined statement therefore
misses a task whose enqueue committed while it waited, and that task stays
`pending` forever. A second statement starts after the lock was acquired, so
it sees the task. We reproduced the failure of the one-statement version, and
the correctness of the two-statement version, on both PostgreSQL versions.
This is one reason taskmill's own transactions require READ COMMITTED.

**Completing a batch.** A worker completes the grouped tasks of a batch in one
transaction, with a few statements for the whole batch rather than a few per
task. First it locks the rows of the batch's groups that no other
transaction holds:

```sql
SELECT g.queue, g.group_key FROM taskmill.groups g
JOIN unnest($1::text[], $2::text[]) AS u(queue, group_key)
  ON g.queue = u.queue AND g.group_key = u.group_key
FOR UPDATE OF g SKIP LOCKED;
```

Then it completes the tasks of the locked groups with the Complete statement,
decrements each group's count, deletes the groups whose count reached zero,
and promotes the oldest `pending` task of every other group, with a
`LATERAL` subquery per group through `tasks_pending_idx`.

`SKIP LOCKED` here means a batch never waits for, or deadlocks with,
another transaction. A task whose group row another transaction holds, for
example an open `EnqueueTx` into that group, stays with the worker, which
keeps renewing its lease and tries it again in a later batch. So one busy
group never holds up the batch, and a waiting completion never ties up a
database connection. Maintenance works the same way: it skips group rows
that other transactions hold and retries them in its next cycle, so a long
application transaction cannot stall it.

**Lock order.** Group rows are always locked in bytewise order of queue and
group key: `COLLATE "C"` in SQL, and Go's string order in code, which is the
same. A `pending` task's row is locked only after its group's row: promotion
takes them in that order, and `Cancel` and `Delete` lock a pending task's
group first. Locking in one order prevents deadlocks, for example between a
`Cancel` and the promotion of the same task.

**Halting.** When a group's head becomes dead, the count is not decremented
and `halted_task_id` is set. The group waits until an operator retries the
dead head, which resumes the group, or deletes it, which promotes the next
task. Halting is always on: groups exist to keep an order, and skipping a
dead task would break it. A canceled head leaves its group like a completed
one, because cancellation is an explicit choice.

**Costs.** A grouped enqueue writes two rows instead of one, and finishing a
grouped task writes up to two more: the group's row, and the next task it
promotes. Ungrouped tasks pay nothing. Producers that enqueue into the same
group wait for each other's transactions to commit, so transactions that
enqueue grouped tasks should be short. Two transactions that enqueue into the
same groups in opposite orders can deadlock; PostgreSQL detects this and
aborts one of them, and batch enqueues sort their groups to avoid it.

## Deduplication

A deduplicated enqueue reserves the task's ID, claims the key with that ID,
and inserts the task only if the claim succeeded, all in one statement. An
expired key that the maintainer has not deleted yet is claimed again.
Simplified to one task:

```sql
WITH next AS (
    SELECT nextval('taskmill.tasks_id_seq') AS id
), k AS (
    INSERT INTO taskmill.dedupe_keys AS d (key, task_id, expires_at)
    SELECT $1, next.id, now() + $2::interval FROM next
    ON CONFLICT (key) DO UPDATE SET task_id = EXCLUDED.task_id, expires_at = EXCLUDED.expires_at
        WHERE d.expires_at <= now()
    RETURNING task_id
)
INSERT INTO taskmill.tasks (id, queue, kind, state, run_at, payload, max_attempts)
OVERRIDING SYSTEM VALUE
SELECT k.task_id, $3, $4, 'available', now(), $5, $6 FROM k
RETURNING id;
```

The ID is reserved first because the parts of one statement cannot see each
other's writes: a final `UPDATE` that tries to store the new task's ID in the
key finds no key row, which leaves the key without its task.

A duplicate's row reports the task that holds the key, found in the same
statement. If that task's transaction committed while the statement waited
for the key, the statement cannot see it, and the client reads the key again
in a second statement. Two concurrent enqueues of the same key cannot both
succeed: the second waits on the first's key and then finds it taken. A
duplicate wastes one sequence value, which is harmless.

`Cancel` and `Delete` delete the task's key, through the index on `task_id`,
so the same key can be enqueued again at once. A dead task keeps its key
until the window ends: an inbox relies on that, so that a redelivered message
does not run again after its task failed. Uniqueness among unfinished tasks
only ("at most one queued refresh per product") can be added later as a new
option, without a disruptive migration.

## Notifications

When enqueue or promotion makes tasks available, the client sends
`pg_notify` on a channel named after the schema, with the payload
`a|<queue>`, so idle workers fetch at once instead of waiting for their next
poll. The same channel carries `c|<id>`, which asks the worker running task
`<id>` to cancel it, and `q|<queue>`, which tells workers that the queue's
settings changed. Notifications are delivered only when the transaction
commits.

Each notifying transaction takes a global lock at commit. At high commit
rates this lock becomes a bottleneck: in a pgbench test before taskmill had
any Go code, a `NOTIFY` in every enqueue capped throughput near 10,000 tasks
per second. So a client sends at most one notification per queue per 50 ms,
and workers also poll as a fallback. `LISTEN` needs a session, so each
worker holds one dedicated connection for it, made with the pool's settings
unless `WorkerConfig.ListenConfig` says otherwise. Behind a
transaction-pooling proxy, `ListenConfig` must point at the database
directly. All other traffic works through such proxies.

## Operational notes

- **Long transactions slow the queue down.** A snapshot that stays open,
  anywhere in the database, stops vacuum from removing dead row versions. The
  fetch index then fills with dead entries and fetches slow down. The
  [guide](../guide.md#running-in-production) says what to set and what to
  alert on.
- **History partitions are created ahead of time.** `Migrate` and the
  maintainer keep partitions for today and the next 6 days, by UTC day. Each
  one is created with `CREATE TABLE ... (LIKE ...)` and then attached with
  `ALTER TABLE ... ATTACH PARTITION` under a 5-second lock timeout, because
  `CREATE TABLE ... PARTITION OF` locks the whole history table and blocks
  completions. Measured: with a completion's transaction open,
  `PARTITION OF` timed out on its lock, while `LIKE` with `ATTACH` finished in
  0.07 seconds. Old partitions are detached with `DETACH PARTITION ...
  CONCURRENTLY`, bounded by 30 seconds, and dropped; an interrupted detach is
  finished next time with `DETACH PARTITION ... FINALIZE`. These commands need
  ownership of `task_history`.
- **There is no default partition.** Attaching a partition would then have to
  scan and lock it. If a day's partition is missing, completions skip history
  and the worker logs an error, so the gap shows up in alerts.
- **Schema changes are versioned.** Each migration is recorded in
  `migrations`. `Migrate` applies all pending migrations in one transaction,
  under a transaction-scoped advisory lock, so only one process migrates at a
  time. A migration therefore cannot use `CREATE INDEX CONCURRENTLY`; a
  future migration that indexes a large existing table will need a step of
  its own outside the transaction.
