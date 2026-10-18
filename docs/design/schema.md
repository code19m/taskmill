# Storage schema

- Status: Accepted
- Milestone: M1

This document maps the [task lifecycle](task-lifecycle.md) and the
[requirements](scope.md) onto PostgreSQL: the tables, the indexes, and the
statements that move tasks between states. Every statement below was run on
PostgreSQL 15 and 18, and the query plans were measured with 1.7 million tasks
(see [Evidence](#evidence)).

All objects live in one schema, `taskmill` by default. The name is
configurable (R1), so several installations can share a database. It must
match `^[a-z_][a-z0-9_]{0,62}$`, which makes it safe to use unquoted.

## Principles

1. **The hot table holds only unfinished work.** `tasks` holds tasks that can
   still run, plus dead and canceled tasks waiting for an operator. Completed
   tasks move to a history table that is partitioned by day, and retention
   drops whole partitions. The hot table stays small, so vacuum keeps up.
2. **One partial index per access path.** Each index holds only the rows in the
   states its query needs. Fetching never reads future, pending, or finished
   tasks, however many there are.
3. **Most transitions are one statement.** A few take several statements in
   one transaction: those that involve a FIFO group (see
   [FIFO groups](#fifo-groups)), `Cancel`, `Retry`, and `Delete`, which lock
   the task before they change it, and fetches from a queue with a
   concurrency limit. No statement depends on session state, so all of them
   work inside the caller's transaction (R12) and behind transaction-pooling
   proxies such as PgBouncer.
4. **The database enforces the lifecycle's invariants** with `CHECK`
   constraints, as a second line of defense behind the code.
5. **Frequent updates touch no indexed column.** Heartbeats change only
   `lease_expires_at`, which no index contains, so PostgreSQL can update the
   row in place (a HOT update) without writing to any index.
6. **Every timestamp that taskmill sets comes from the database clock**
   (`now()`). Only times that the application passes in, such as a run time
   or a deadline, come from the application's clock.

## Tables

| Table | Purpose | Write rate |
|---|---|---|
| `tasks` | Unfinished, dead, and canceled tasks | Hot: every enqueue, fetch, heartbeat, and result |
| `task_history` | Completed tasks, one partition per day | Hot: one insert per completion; append-only |
| `groups` | One row per active FIFO group | Warm: grouped enqueues and completions only |
| `dedupe_keys` | Deduplication keys and their windows | Warm: deduplicated enqueues only |
| `queues` | Per-queue settings | Cold |
| `settings` | Installation-wide settings, in a single row | Cold |
| `schedules` | Recurring schedules | Cold |
| `workers` | Live worker processes | Cold: a heartbeat per process every third of the lease duration (10 s by default) |
| `leader` | The elected maintainer | Cold |
| `queue_stats` | Task counts per queue and state, refreshed by the maintainer | Cold |
| `migrations` | Applied schema migrations | Cold |

### `tasks`

Columns are ordered for compact rows: fixed-width columns first, from the
widest to the narrowest, then variable-width ones.

```sql
CREATE TYPE taskmill.task_state AS ENUM (
    'scheduled', 'pending', 'available', 'running',
    'retryable', 'completed', 'dead', 'canceled'
);

CREATE TABLE taskmill.tasks (
    id                  bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_at              timestamptz NOT NULL,         -- earliest start; also the retry time
    created_at          timestamptz NOT NULL DEFAULT now(),
    deadline            timestamptz,                  -- latest start
    lease_expires_at    timestamptz,                  -- set only while running
    attempted_at        timestamptz,                  -- start of the latest attempt
    cancel_requested_at timestamptz,
    finalized_at        timestamptz,                  -- when it became dead or canceled
    lease_token         integer NOT NULL DEFAULT 0,   -- grows by one on every fetch
    state               taskmill.task_state NOT NULL,
    priority            smallint NOT NULL DEFAULT 2,  -- 1 to 4, higher runs first
    attempt             smallint NOT NULL DEFAULT 0,  -- attempts used
    max_attempts        smallint NOT NULL,
    queue               text NOT NULL,
    kind                text NOT NULL,
    group_key           text,                         -- FIFO group, if any
    attempted_by        text,                         -- worker ID of the latest attempt
    payload             jsonb NOT NULL,
    metadata            jsonb NOT NULL DEFAULT '{}',  -- trace context and user tags
    errors              jsonb NOT NULL DEFAULT '[]',  -- the 20 most recent attempt errors, newest last

    CONSTRAINT tasks_queue_check     CHECK (char_length(queue) BETWEEN 1 AND 128),
    CONSTRAINT tasks_kind_check      CHECK (char_length(kind) BETWEEN 1 AND 128),
    CONSTRAINT tasks_group_key_check CHECK (char_length(group_key) BETWEEN 1 AND 256),
    CONSTRAINT tasks_attempts_check  CHECK (max_attempts >= 1 AND attempt BETWEEN 0 AND max_attempts),
    CONSTRAINT tasks_state_check     CHECK (state <> 'completed'),
    CONSTRAINT tasks_pending_check   CHECK (state <> 'pending' OR group_key IS NOT NULL),
    CONSTRAINT tasks_lease_check     CHECK ((state = 'running') = (lease_expires_at IS NOT NULL)),
    CONSTRAINT tasks_finalized_check CHECK ((state IN ('dead', 'canceled')) = (finalized_at IS NOT NULL))
) WITH (
    fillfactor = 80,
    autovacuum_vacuum_scale_factor = 0.01,
    autovacuum_vacuum_threshold = 1000,
    autovacuum_vacuum_cost_delay = 0
);
```

Design notes:

- **`id` is the task's stable identity** (R11). It is unique within an
  installation and is kept when a task moves to history. Messages that cross
  service boundaries need globally unique IDs; the outbox package generates
  those (a UUIDv7 in the message) rather than widening every task row.
- **`lease_token` is a counter, not a random value.** It grows on every fetch
  and is never reset, not even by an operator retry, so a stale attempt's token
  can never match again. Unlike `attempt`, it never goes down, so snoozes and
  graceful releases, which give attempts back, are safe.
- **`errors` is bounded.** Each failure appends an entry with the attempt
  number, the time, the message, and, for failures that taskmill detected
  itself, a code such as `lease_expired`. The first snooze of a task whose
  kind has no handler also appends one, a note with the code `no_handler`;
  the error of a task that then waits too long has the code `unknown_kind`.
  Beyond
  20 entries the oldest is dropped, so a task keeps its 20 most recent
  errors. The Go side truncates each message to 4 KiB before storing it.
- **`fillfactor = 80`** leaves room in each page for updated row versions, so
  heartbeats can be HOT updates (see [Evidence](#evidence)).
- **`queue` and `kind` are stored as text,** not as IDs into lookup tables. The
  rows are slightly larger, but there are no joins, no lookups, and no
  registration step before the first enqueue (R2).

### `task_history`

Completed tasks, partitioned by `finalized_at`, one partition per UTC day. It
is append-only: rows are never updated, and retention drops whole partitions,
which is instant and leaves nothing for vacuum to clean up.

```sql
CREATE TABLE taskmill.task_history (
    id           bigint NOT NULL,
    finalized_at timestamptz NOT NULL,   -- completion time; the partition key
    created_at   timestamptz NOT NULL,
    run_at       timestamptz NOT NULL,
    attempted_at timestamptz NOT NULL,
    priority     smallint NOT NULL,
    attempt      smallint NOT NULL,
    max_attempts smallint NOT NULL,
    queue        text NOT NULL,
    kind         text NOT NULL,
    group_key    text,
    attempted_by text NOT NULL,
    payload      jsonb NOT NULL,
    metadata     jsonb NOT NULL,
    errors       jsonb NOT NULL          -- errors of earlier, failed attempts
) PARTITION BY RANGE (finalized_at);

CREATE INDEX task_history_queue_idx ON taskmill.task_history (queue, id);
CREATE INDEX task_history_id_idx    ON taskmill.task_history (id);
```

Lists page through history by `id`, newest first (see
[Listing](#listing)): `task_history_queue_idx` serves a list of one queue,
and `task_history_id_idx` serves `Get` and a list of all queues.

The history retention period is one setting for the whole installation
(7 days by default), stored in `settings`, because dropping a day's partition
removes every queue's rows for that day. A queue can still opt out of history
entirely (R10).

### `groups`

One row per FIFO group that has unfinished tasks. Its row lock serializes the
enqueues and completions of a group, and its counter decides which task is the
head. See [FIFO groups](#fifo-groups).

```sql
CREATE TABLE taskmill.groups (
    queue          text NOT NULL,
    group_key      text NOT NULL,
    tasks          integer NOT NULL,  -- unfinished tasks; a halted group also counts its dead head
    halted_task_id bigint,            -- the dead head that halted the group
    halted_at      timestamptz,
    updated_at     timestamptz NOT NULL DEFAULT now(),

    PRIMARY KEY (queue, group_key),
    CONSTRAINT groups_tasks_check  CHECK (tasks >= 0),
    CONSTRAINT groups_halted_check CHECK ((halted_task_id IS NULL) = (halted_at IS NULL))
) WITH (fillfactor = 80);

CREATE INDEX groups_halted_task_idx ON taskmill.groups (halted_task_id) WHERE halted_task_id IS NOT NULL;
```

`groups_halted_task_idx` lets retention skip the dead heads that keep their
groups halted without scanning every group.

### `dedupe_keys`

Deduplication keys for R9. The key is a SHA-256 hash, computed in Go, of the
queue, the kind, and the caller's key, so index entries stay small whatever the
caller's key looks like. A key blocks duplicates until `expires_at`, whatever
happens to the task. That makes the window independent of retention and of
the task's state.

```sql
CREATE TABLE taskmill.dedupe_keys (
    key        bytea PRIMARY KEY,
    task_id    bigint NOT NULL,       -- the task the key admitted
    expires_at timestamptz NOT NULL,

    CONSTRAINT dedupe_keys_key_check CHECK (octet_length(key) = 32)
);

CREATE INDEX dedupe_keys_expires_idx ON taskmill.dedupe_keys (expires_at);
```

### Settings, coordination, and bookkeeping

```sql
CREATE TABLE taskmill.queues (
    name               text PRIMARY KEY,
    paused_at          timestamptz,                    -- R17
    max_running        integer,                        -- R8; NULL means no limit
    keep_history       boolean NOT NULL DEFAULT true,  -- R10
    halt_group_on_dead boolean NOT NULL DEFAULT false, -- R6
    created_at         timestamptz NOT NULL DEFAULT now(),
    updated_at         timestamptz NOT NULL DEFAULT now(),

    CONSTRAINT queues_name_check        CHECK (char_length(name) BETWEEN 1 AND 128),
    CONSTRAINT queues_max_running_check CHECK (max_running > 0)
);

CREATE TABLE taskmill.settings (
    id                boolean PRIMARY KEY DEFAULT true,       -- always true: one row
    history_retention interval NOT NULL DEFAULT '7 days',     -- completed tasks
    dead_retention    interval NOT NULL DEFAULT '30 days',    -- dead and canceled tasks
    updated_at        timestamptz NOT NULL DEFAULT now(),

    CONSTRAINT settings_single_row_check CHECK (id)
);

CREATE TABLE taskmill.schedules (                      -- R14
    name        text PRIMARY KEY,
    queue       text NOT NULL,
    kind        text NOT NULL,
    cron        text NOT NULL,
    timezone    text NOT NULL DEFAULT 'UTC',
    payload     jsonb NOT NULL DEFAULT '{}',
    next_run_at timestamptz NOT NULL,
    last_run_at timestamptz,
    paused_at   timestamptz,
    created_at  timestamptz NOT NULL DEFAULT now(),
    updated_at  timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX schedules_due_idx ON taskmill.schedules (next_run_at) WHERE paused_at IS NULL;

CREATE TABLE taskmill.workers (                        -- R15
    id           text PRIMARY KEY,
    queues       text[] NOT NULL,
    hostname     text NOT NULL,
    version      text NOT NULL,
    started_at   timestamptz NOT NULL DEFAULT now(),
    heartbeat_at timestamptz NOT NULL DEFAULT now()
) WITH (fillfactor = 50);

CREATE TABLE taskmill.leader (
    name       text PRIMARY KEY,                       -- 'maintainer'
    worker_id  text NOT NULL,
    elected_at timestamptz NOT NULL,
    expires_at timestamptz NOT NULL
);

CREATE TABLE taskmill.queue_stats (                    -- R15
    queue        text NOT NULL,
    state        taskmill.task_state NOT NULL,
    tasks        bigint NOT NULL,
    capped       boolean NOT NULL,                     -- true when the count hit its cap
    refreshed_at timestamptz NOT NULL,

    PRIMARY KEY (queue, state)
);

CREATE TABLE taskmill.migrations (
    version    integer PRIMARY KEY,
    name       text NOT NULL,
    applied_at timestamptz NOT NULL DEFAULT now()
);
```

Installation-wide settings live in the database rather than in each
process's configuration, so every worker that becomes the maintainer applies
the same retention.

The leader is elected with a lease row rather than an advisory lock. Session
advisory locks need a dedicated connection and do not work behind
transaction-pooling proxies; a lease row works everywhere.

## Indexes on `tasks`

| Index | Key | Rows it holds | Used by |
|---|---|---|---|
| `tasks_pkey` | `id` | All | Every update by ID; heartbeats; completions; listing all tasks |
| `tasks_fetch_idx` | `queue, priority DESC, run_at, id` | `available` | Fetch; filling batches; counting available tasks |
| `tasks_due_idx` | `run_at`, including `queue` and `state` | `scheduled`, `retryable` | Promotion; the maintainer's next wake-up time; counting scheduled and retryable tasks |
| `tasks_running_idx` | `queue` | `running` | Lease rescue; concurrency limits (R8); counting running tasks |
| `tasks_pending_idx` | `queue, group_key, id` | `pending` | Promoting the next task of a group; counting pending tasks |
| `tasks_deadline_idx` | `deadline` | `scheduled`, `available`, `retryable`, with a deadline | The deadline sweep |
| `tasks_finished_idx` | `queue, state, finalized_at` | `dead`, `canceled` | Retention; counting dead and canceled tasks per queue and state |

```sql
CREATE INDEX tasks_fetch_idx    ON taskmill.tasks (queue, priority DESC, run_at, id) WHERE state = 'available';
CREATE INDEX tasks_due_idx      ON taskmill.tasks (run_at) INCLUDE (queue, state) WHERE state IN ('scheduled', 'retryable');
CREATE INDEX tasks_running_idx  ON taskmill.tasks (queue) WHERE state = 'running';
CREATE INDEX tasks_pending_idx  ON taskmill.tasks (queue, group_key, id) WHERE state = 'pending';
CREATE INDEX tasks_deadline_idx ON taskmill.tasks (deadline)
    WHERE deadline IS NOT NULL AND state IN ('scheduled', 'available', 'retryable');
CREATE INDEX tasks_finished_idx ON taskmill.tasks (queue, state, finalized_at) WHERE state IN ('dead', 'canceled');
```

The fetch index's key matches the fetch query's `ORDER BY` exactly. PostgreSQL
reads the first rows of the index in order and stops at the `LIMIT`, with no
sort, however long the backlog. `tasks_running_idx` deliberately does not
contain `lease_expires_at`: if it did, every heartbeat would write an index
entry.

`INCLUDE` adds columns to an index's entries without making them part of its
key. With `queue` and `state` in `tasks_due_idx`, and `state` in
`tasks_finished_idx`, statistics can count those tasks from the index alone,
without reading the table (an index-only scan). `tasks_deadline_idx` leaves
out `pending` tasks, because the deadline sweep never touches them (see
[FIFO groups](#fifo-groups)).

A list of tasks pages by `id`, not by `finalized_at`, so it does not use
`tasks_finished_idx` for its order. It can still use the partial index that
matches its filters (see [Listing](#listing)).

## Key statements

The statements below are simplified for reading: they show each statement's
shape, and leave out details such as some returned columns and the exact
parameter types (durations, for example, are passed as microseconds). The
exact text is in `sql.go`. Batch versions pass arrays and use `unnest`, so a
batch of any size is one round trip with fixed SQL text, which lets prepared
statements be reused.

### Enqueue

```sql
INSERT INTO taskmill.tasks (queue, kind, state, priority, run_at, deadline, payload, metadata, max_attempts)
SELECT t.queue, t.kind,
       CASE WHEN coalesce(t.run_at, now()) <= now() THEN 'available' ELSE 'scheduled' END::taskmill.task_state,
       t.priority, coalesce(t.run_at, now()), t.deadline, t.payload, t.metadata, t.max_attempts
FROM unnest($1::text[], $2::text[], $3::smallint[], $4::timestamptz[],
            $5::timestamptz[], $6::jsonb[], $7::jsonb[], $8::smallint[])
     AS t(queue, kind, priority, run_at, deadline, payload, metadata, max_attempts)
RETURNING id;
```

The implementation sends every batch through one of two statements. Batches
with dedupe keys or FIFO groups go through a statement that also claims the
keys and counts the groups, as described in [FIFO groups](#fifo-groups) and
[Deduplication](#deduplication); all other batches go through a lean one.
Both reserve IDs from the `tasks_id_seq` identity sequence first, so results
map back to their inputs.

### Fetch

```sql
WITH picked AS (
    SELECT id
    FROM taskmill.tasks
    WHERE state = 'available' AND queue = $1
      AND (deadline IS NULL OR deadline > now())
    ORDER BY priority DESC, run_at, id
    LIMIT $2
    FOR UPDATE SKIP LOCKED
)
UPDATE taskmill.tasks t
SET state            = 'running',
    attempt          = t.attempt + 1,
    lease_token      = t.lease_token + 1,
    lease_expires_at = now() + $3::interval,
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

For a queue with a concurrency limit (R8), the fetch runs in a transaction that
first locks the queue's row in `queues`, then counts its running tasks through
`tasks_running_idx`, and fetches at most the remaining capacity. This
serializes fetches for limited queues only.

A worker fetches one task per free slot. If the fetched tasks include kinds
that have a batch handler (R7), a second statement fills each such batch up
to its batch size. It is the same fetch, limited to one kind, and it looks at
no more than the first 1,000 available tasks of the queue, in the same order,
so it stays cheap in a queue of many kinds:

```sql
WITH candidates AS (
    SELECT id, kind FROM taskmill.tasks
    WHERE state = 'available' AND queue = $1 AND (deadline IS NULL OR deadline > now())
    ORDER BY priority DESC, run_at, id
    LIMIT 1000
), picked AS (
    SELECT t.id FROM taskmill.tasks t JOIN candidates c ON c.id = t.id
    WHERE c.kind = $5 AND t.state = 'available'
    ORDER BY t.priority DESC, t.run_at, t.id
    LIMIT $2
    FOR UPDATE OF t SKIP LOCKED
)
UPDATE taskmill.tasks t SET state = 'running' -- and the other columns, as in the fetch above
FROM picked WHERE t.id = picked.id
RETURNING t.*;
```

A batch takes one slot, so every fetched task starts at once, and batch kinds
can share a queue with other kinds.

`Worker.RunTask` leases one task by ID with a similar `UPDATE`. It accepts a
`scheduled`, `available`, or `retryable` task whose deadline has not passed,
whatever its run time.

### Heartbeat

```sql
WITH attempts AS (
    SELECT * FROM unnest($1::bigint[], $2::integer[]) AS u(id, token)
), locked AS (
    SELECT id FROM taskmill.tasks
    WHERE (id, lease_token) IN (SELECT * FROM attempts) AND state = 'running'
    FOR UPDATE SKIP LOCKED
), renewed AS (
    UPDATE taskmill.tasks t
    SET lease_expires_at = now() + $3::interval
    FROM locked
    WHERE t.id = locked.id
    RETURNING t.id, t.cancel_requested_at IS NOT NULL AS cancel_requested
)
SELECT id, true AS renewed, cancel_requested FROM renewed
UNION ALL
SELECT id, false, cancel_requested_at IS NOT NULL FROM taskmill.tasks
WHERE (id, lease_token) IN (SELECT * FROM attempts) AND state = 'running';
```

One statement renews every lease a worker holds, and its cost grows linearly
with their number. The last branch returns every task that the attempts
still own, so a renewed task appears twice: once as renewed, and once as
owned. A task missing from the result has lost its lease, and the worker
cancels that attempt's context. A task returned with `cancel_requested` is
canceled the same way (transition 16 of the lifecycle).

`SKIP LOCKED` keeps one locked row, such as a task that an application
transaction is canceling, from holding up the renewal of all the others. A
skipped task is returned only as owned: the worker keeps its attempt running
until its lease could have expired, and renews it at the next heartbeat. An
attempt that completed its task with `CompleteTx` is never canceled as lost:
its own transaction holds the row until it commits.

### Complete

One statement completes a batch of tasks. It moves each row to history,
unless its queue keeps none (`keep_history = false`):

```sql
WITH done AS (
    DELETE FROM taskmill.tasks
    WHERE (id, lease_token) IN (SELECT * FROM unnest($1::bigint[], $2::integer[]))
      AND state = 'running'
    RETURNING *
), kept AS (
    INSERT INTO taskmill.task_history (id, finalized_at, created_at, run_at, attempted_at, priority, attempt,
                                       max_attempts, queue, kind, group_key, attempted_by, payload, metadata, errors)
    SELECT id, now(), created_at, run_at, attempted_at, priority, attempt,
           max_attempts, queue, kind, group_key, attempted_by, payload, metadata, errors
    FROM done
    WHERE NOT EXISTS (SELECT 1 FROM taskmill.queues q WHERE q.name = done.queue AND NOT q.keep_history)
)
SELECT id, queue, group_key FROM done;
```

A task missing from the result had lost its lease, and its result is
discarded (guarantee 4). The same statement, run inside the handler's
transaction, is transactional completion (R12).

A worker records the completions that are ready together. Ungrouped tasks
complete with this one statement per batch, without a transaction of its
own. Grouped tasks complete in one transaction per batch, which also updates
their groups (see [FIFO groups](#fifo-groups)).

### Fail

```sql
UPDATE taskmill.tasks
SET state            = CASE WHEN cancel_requested_at IS NOT NULL THEN 'canceled'
                            ELSE $3::taskmill.task_state END,  -- 'retryable' or 'dead', from the kind's retry policy
    run_at           = CASE WHEN cancel_requested_at IS NULL AND $3::taskmill.task_state = 'retryable'
                            THEN now() + $4::interval ELSE run_at END,
    finalized_at     = CASE WHEN cancel_requested_at IS NOT NULL OR $3::taskmill.task_state = 'dead' THEN now() END,
    lease_expires_at = NULL,
    errors           = (CASE WHEN jsonb_array_length(errors) >= 20 THEN errors - 0 ELSE errors END)
                       || jsonb_build_array(jsonb_build_object('attempt', attempt, 'at', now()) || $5::jsonb)
WHERE id = $1 AND lease_token = $2 AND state = 'running'
RETURNING queue, group_key, state;
```

`$5` is the error as a JSON object, `{"message": ..., "code": ...}`. The
`CASE` drops the oldest entry once there are 20, so a task keeps its 20 most
recent errors.

Snooze and graceful release are similar updates that also give the attempt
back (`attempt = attempt - 1`). A snooze can record an error too, as the
first snooze of a task with no handler does. All three cancel the task
instead if its cancellation was requested, so a request whose notification
was missed still takes effect when the attempt ends.

For an ungrouped task, the statement is all there is: it runs alone, without
a transaction of its own. For a grouped task, it runs in a transaction, which
also updates the group if the task ended finished (see
[FIFO groups](#fifo-groups)).

### Rescue expired leases

The maintainer runs this every second. A lost lease counts as a failed
attempt. The retry delay is a fixed exponential backoff, from 1 second up to
1 hour, because the maintainer does not know each kind's retry policy, nor
the workers' `DefaultRetry`.

```sql
WITH expired AS (
    SELECT id FROM taskmill.tasks
    WHERE state = 'running' AND lease_expires_at < now()
    ORDER BY id
    LIMIT 1000
    FOR UPDATE SKIP LOCKED
)
UPDATE taskmill.tasks t
SET state = CASE
            WHEN t.cancel_requested_at IS NOT NULL THEN 'canceled'
            WHEN t.attempt >= t.max_attempts THEN 'dead'
            ELSE 'retryable'
        END::taskmill.task_state,
    run_at = CASE
            WHEN t.cancel_requested_at IS NULL AND t.attempt < t.max_attempts
            THEN now() + make_interval(secs => least($1 * power(2, least(t.attempt - 1, 30)), $2) * (0.9 + random() * 0.2))
            ELSE t.run_at
        END,
    finalized_at = CASE WHEN t.cancel_requested_at IS NOT NULL OR t.attempt >= t.max_attempts THEN now() END,
    lease_expires_at = NULL,
    errors = (CASE WHEN jsonb_array_length(t.errors) >= 20 THEN t.errors - 0 ELSE t.errors END)
             || jsonb_build_array(jsonb_build_object('attempt', t.attempt, 'at', now(),
                    'message', 'the worker lost its lease: it crashed, stopped, or lost the database',
                    'code', 'lease_expired'))
FROM expired
WHERE t.id = expired.id
RETURNING t.id, t.queue, t.group_key, t.state;
```

The backoff is exponential with ±10% jitter: `$1` is the base delay in seconds
and `$2` the maximum. The exponent is capped, so that a task with thousands of
attempts cannot overflow it. The statement runs in a transaction that also
updates the groups of the grouped tasks it finished. It repeats until it
rescues fewer than 1,000 tasks.

### Retention

Every minute, the maintainer applies retention, in a loop of its own so that
a slow step never delays promotion or rescue. Old dead and canceled tasks are
deleted in batches:

```sql
WITH old AS (
    SELECT t.id FROM taskmill.tasks t
    WHERE t.state IN ('dead', 'canceled') AND t.finalized_at < now() - $1::interval
      AND NOT EXISTS (SELECT 1 FROM taskmill.groups g WHERE g.halted_task_id = t.id)
    LIMIT 1000
    FOR UPDATE SKIP LOCKED
)
DELETE FROM taskmill.tasks t USING old WHERE t.id = old.id;
```

- **Locked rows are skipped.** A task that an operator is retrying is locked
  by that transaction, so retention never deletes it. Expired dedupe keys are
  deleted the same way, so a key that an enqueue is claiming again is kept.
- **Dead heads that halt their groups are kept,** found through
  `groups_halted_task_idx`, until an operator retries or deletes them.
- **History is dropped by the day.** A partition whose whole day is older
  than `settings.history_retention` is detached with `DETACH PARTITION ...
  CONCURRENTLY` and dropped. Each detach is bounded by 30 seconds. A detach
  that was interrupted leaves the partition pending; the next run finishes it
  with `DETACH PARTITION ... FINALIZE`.
- Workers that stopped without unregistering are deleted after 5 minutes
  without heartbeats.

### Statistics

Every 10 seconds, in the same loop as retention, the maintainer counts each
queue's tasks per state, except completed ones, and stores the counts in
`queue_stats` for `Client.Queues`. Every count reads indexes only, and every
count is capped, so the cost stays bounded however large the backlog is:

```sql
-- available, running, pending, dead, and canceled: per queue, up to 10,000
-- each. Shown for available; the others use their own index the same way.
WITH RECURSIVE queues AS (
    (SELECT queue FROM taskmill.tasks WHERE state = 'available' ORDER BY queue LIMIT 1)
    UNION ALL
    SELECT (SELECT t.queue FROM taskmill.tasks t
            WHERE t.state = 'available' AND t.queue > q.queue ORDER BY t.queue LIMIT 1)
    FROM queues q WHERE q.queue IS NOT NULL
)
SELECT 'available', q.queue,
       (SELECT count(*) FROM (SELECT FROM taskmill.tasks t
                              WHERE t.state = 'available' AND t.queue = q.queue LIMIT 10000) c)
FROM queues q WHERE q.queue IS NOT NULL
UNION ALL
-- scheduled and retryable: across all queues, up to 100,000 in all.
SELECT state, queue, count(*) FROM (
    SELECT state, queue FROM taskmill.tasks WHERE state IN ('scheduled', 'retryable') LIMIT 100000
) due GROUP BY state, queue;
```

The recursive part finds the next queue name with one index probe each, a
technique called a loose index scan, so finding the queues costs one probe
per queue, not one row per task. Scheduled and retryable tasks are read from
`tasks_due_idx`, which is ordered by run time, not by queue, so they are
counted together. A count that reaches its cap is stored with
`capped = true`: it is a lower bound.

### Listing

`Client.List` and the web interface page through tasks by `id`, newest
first, with keyset pagination: each page starts below the last `id` of the
previous one. Live tasks and history are read in two branches, each limited
on its own, and merged. Each page runs with custom query plans
(`plan_cache_mode = force_custom_plan`), so the planner sees which filters are
set and uses the partial index that matches them, and with a 5-second
statement timeout.

### Other statements

| Statement | What it does |
|---|---|
| Promote | Moves up to 1,000 `scheduled` and `retryable` tasks whose run time has passed to `available`, through `tasks_due_idx`, and notifies their queues in the same statement. It runs on its own timer, set to the next due task, at most 20 times a second and at least once a second. |
| Deadline sweep | Every second, moves `scheduled`, `available`, and `retryable` tasks whose deadline has passed to `dead`, through `tasks_deadline_idx`. `pending` tasks are left to their groups (see [FIFO groups](#fifo-groups)). |
| Cancel | Cancels waiting tasks at once, and flags running ones with `cancel_requested_at`. A `pending` task's group row is locked first. |
| Retry | Moves a `dead` or `canceled` task back to `available` with `attempt = 0`, keeping its error history, or to `pending` if its group has other unfinished tasks. |
| Leader election | An upsert on `leader` that succeeds only for the current leader or when the lease has expired. |

Statements that end a grouped task (complete, dead, cancel, rescue, deadline)
also run the group bookkeeping described next.

## FIFO groups

**The problem.** In a FIFO group only the head may run. If the rest of the
group were `available`, every fetch would have to skip over them, and a group
with a large backlog would slow every fetch down: the same problem that
separate `scheduled` tasks avoid.

**The design.** Tasks behind the head are in a separate state, `pending`, which
no fetch reads. The `groups` row counts the group's unfinished tasks.

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
while the decrement waited for the group's lock. In PostgreSQL's default
isolation level, each statement sees the data as of its own start. One
combined statement therefore misses a task whose enqueue committed while it
waited, and that task stays `pending` forever. A second statement starts
after the lock was acquired, so it sees the task. We reproduced the failure of
the one-statement version, and the correctness of the two-statement version,
on both PostgreSQL versions (see [Evidence](#evidence)).

**Completing a batch.** A worker completes the grouped tasks of a batch in one
transaction, with a few statements for the whole batch rather than a few per
task:

```sql
BEGIN;
-- 1. Lock the rows of the batch's groups that no other transaction holds.
SELECT g.queue, g.group_key FROM taskmill.groups g
JOIN unnest($1::text[], $2::text[]) AS u(queue, group_key)
  ON g.queue = u.queue AND g.group_key = u.group_key
FOR UPDATE OF g SKIP LOCKED;

-- 2. Complete the tasks of the locked groups, with the Complete statement above.

-- 3. Count each finished head out of its group.
UPDATE taskmill.groups g
SET tasks = g.tasks - 1, updated_at = now()
FROM unnest($1::text[], $2::text[], $3::bigint[]) AS u(queue, group_key, id)
WHERE g.queue = u.queue AND g.group_key = u.group_key
RETURNING g.queue, g.group_key, g.tasks;

-- 4. Delete the groups whose count reached zero.
DELETE FROM taskmill.groups g USING unnest($1::text[], $2::text[]) AS u(queue, group_key)
WHERE g.queue = u.queue AND g.group_key = u.group_key AND g.tasks = 0;

-- 5. Promote the oldest pending task of every other group.
WITH next AS (
    SELECT p.id FROM unnest($1::text[], $2::text[]) AS u(queue, group_key)
    CROSS JOIN LATERAL (
        SELECT id FROM taskmill.tasks
        WHERE queue = u.queue AND group_key = u.group_key AND state = 'pending'
        ORDER BY id
        LIMIT 1
        FOR UPDATE
    ) p
)
UPDATE taskmill.tasks t
SET state = CASE
            WHEN t.deadline <= now() THEN 'dead'  -- also sets finalized_at and records an error
            WHEN t.run_at > now() THEN 'scheduled'
            ELSE 'available'
        END::taskmill.task_state
FROM next
WHERE t.id = next.id;
COMMIT;
```

`SKIP LOCKED` in step 1 means a batch never waits for, or deadlocks with,
another transaction. A task whose group row another transaction holds, for
example an open `EnqueueTx` into that group, stays with the worker, which
renews its lease and tries it again in the next batch, about every 20 ms. So
one busy group never holds up the batch, and a waiting completion never ties
up a database connection.

**Deadlines in a group.** The deadline sweep never touches a `pending` task.
When the group promotes a task whose deadline has passed, the promotion makes
it `dead` instead, as step 5 shows. The task is then the group's head, so it
leaves the group as a dead head does: it halts the group if the queue halts
groups, and otherwise the next task is promoted.

**Lock order.** Group rows are always locked in bytewise order of queue and
group key: `COLLATE "C"` in SQL, and Go's string order in code, which is the
same. A `pending` task's row is locked only after its group's row: promotion
takes them in that order, and `Cancel` and `Delete` lock a pending task's
group first. Locking in one order prevents deadlocks, for example between a
`Cancel` and the promotion of the same task.

**Halting.** When a group's head becomes dead and its queue has
`halt_group_on_dead`, the count is not decremented and `halted_at` is set. The
group waits until an operator retries the dead head or deletes it.

**Costs.** A grouped enqueue writes two rows instead of one, and finishing a
grouped task writes up to two more: the group's row, and the next task it
promotes. Ungrouped tasks pay nothing. Producers that
enqueue into the same group wait for each other's transactions to commit, so
transactions that enqueue grouped tasks should be short. Two transactions that
enqueue into the same groups in opposite orders can deadlock; PostgreSQL
detects this and aborts one of them, and batch enqueues sort their groups to
avoid it.

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
key finds no key row, which leaves the key without its task. We found this
while testing an earlier draft.

A duplicate's row reports the task that holds the key, found in the same
statement. If that task's transaction committed while the statement waited
for the key, the statement cannot see it, and the client reads the key again
in a second statement, `dedupeLookup`. Two concurrent enqueues of the same key
cannot both succeed: the second waits on the first's key and then finds it
taken. A duplicate wastes one sequence value, which is harmless.

Uniqueness among unfinished tasks only ("at most one pending rebuild per
user") is not needed by any requirement yet. It can be added later with
nullable columns and a partial unique index, without a disruptive migration.

## Notifications

When enqueue or promotion makes tasks available, the client sends
`pg_notify` on a channel named after the schema, with the payload
`a|<queue>`, so idle workers fetch at once instead of waiting for their next
poll. The same channel carries `c|<id>`, which asks the worker running task
`<id>` to cancel it, and `q|<queue>`, which tells workers that the queue's
settings changed. Notifications are delivered only when the transaction
commits.

Each notifying transaction takes a global lock at commit. At very high commit
rates this lock becomes a bottleneck, so clients send at most one notification
per queue per short interval, and workers also poll as a fallback. The
maintainer's promote statement sends its own notifications, one per queue
that it made tasks available in. `LISTEN` needs a session, so each worker
holds one dedicated connection for it, made with the pool's settings unless
`WorkerConfig.ListenConfig` says otherwise; behind a transaction-pooling
proxy, `ListenConfig` must point at the database directly. All other traffic
works through such proxies.

## Operational notes

- **Vacuum must keep up with `tasks`.** Every fetch and result leaves a dead
  row version. Migrations set aggressive autovacuum settings on `tasks`; the
  exact values are tuned by the benchmarks in M7.
- **Long transactions slow the queue down.** A transaction that stays open,
  anywhere in the database, stops vacuum from removing dead row versions. The
  fetch index then fills with dead entries and fetches slow down. The
  [guide](../guide.md#running-in-production) tells users to set
  `idle_in_transaction_session_timeout` and to watch for old transactions.
- **History partitions are created ahead of time.** The maintainer keeps
  partitions for today and the next 6 days, by UTC day. It creates each one with `CREATE TABLE ...
  (LIKE ...)` followed by `ALTER TABLE ... ATTACH PARTITION`, because `CREATE
  TABLE ... PARTITION OF` locks the whole history table and blocks completions.
  Old partitions are removed with `DETACH PARTITION ... CONCURRENTLY` and then
  dropped (see [Retention](#retention)). There is no default partition. Attaching a partition would have to
  scan and lock it, and a missing partition should show up as an alert rather
  than be hidden.
- **Schema changes are versioned.** Each migration is recorded in
  `migrations`. `Migrate` applies all pending migrations in one transaction,
  under a transaction-scoped advisory lock, so only one process migrates at a
  time. A migration therefore cannot use `CREATE INDEX CONCURRENTLY`; a
  future migration that indexes a large existing table will need a step of
  its own outside the transaction.

## Evidence

Measured in single sessions on PostgreSQL 15.19 and 18.6 in Docker on a
laptop. The numbers show the shape of each plan, not throughput; M7 measures
throughput.

The test data had 1.7 million tasks: 300,000 due and 1,000,000 scheduled in one
queue, 200,000 due in 9 other queues, 5,000 running, 70,000 dead or canceled,
and 10,000 FIFO groups of 11 tasks each. The heap was 243 MB and the fetch
index 39 MB.

| Query | Plan | Time, PG 15 / 18 |
|---|---|---|
| Fetch 10 tasks: 300,000 due and 1,000,000 future in the same queue | Index scan of `tasks_fetch_idx`; no sort | 0.29 / 0.82 ms |
| Fetch 100 heads from a FIFO queue with 100,000 pending tasks | Index scan of `tasks_fetch_idx`; pending tasks not read | 0.06 / 0.14 ms |
| Find due tasks among 1,000,000 scheduled | Range scan of `tasks_due_idx` | 0.03 / 0.08 ms |
| Find expired leases among 5,000 running | Scan of `tasks_running_idx` | 0.37 / 0.75 ms |
| One page of 50 dead tasks, newest first | Backward scan of `tasks_finished_idx`; 5 pages read | 0.03 / 0.05 ms |
| Next pending task of a group | Index-only scan of `tasks_pending_idx` | 0.02 / 0.04 ms |
| Count 300,000 available tasks, capped at 1,000,000 | Index-only scan of `tasks_fetch_idx` | 38 / 57 ms |

Two rows of this table measured designs that have changed since. Lists now
page by `id`, not by `finalized_at` (see [Listing](#listing)), and statistics
now cap their counts at 10,000 per queue and state, and 100,000 for scheduled
and retryable tasks (see [Statistics](#statistics)).

Other results, identical on both versions:

- **Heartbeats become HOT updates.** We renewed 5,000 leases on densely packed
  pages, 12 times. The first round was 25% HOT, limited by the 20% of free
  space that `fillfactor` leaves. As PostgreSQL pruned old row versions, the
  ratio rose to 97% by round 12.
- **Attaching a partition does not block completions.** With a completion's
  transaction open on the history table, `CREATE TABLE ... PARTITION OF` timed
  out on its lock after 2 seconds, while `CREATE TABLE (LIKE ...)` with `ATTACH
  PARTITION` finished in 0.07 seconds.
- **The FIFO race is real, and two statements fix it.** A head finished while
  another transaction was enqueueing into its group. With one statement, the
  new task stayed `pending` with nothing left to promote it. With two
  statements, it became `available`.
- **The `CHECK` constraints reject invalid rows,** such as a running task
  without a lease or a pending task without a group.
- **Deduplication admits a key once per window.** A second enqueue of the same
  key inserted nothing, the key pointed at the first task, and after the key
  expired a new enqueue claimed it again.
- **Large integers in payloads are stored exactly**, for example
  `1234567890123456789`.

## Requirements

| Requirement | How the schema meets it |
|---|---|
| R1 Configurable schema | Every object lives in one schema whose name is a setting. |
| R2 Independent producers and consumers | Tasks carry `kind` and `payload` only; nothing references handlers or requires registration. |
| R3 No global state | Nothing in the schema is per-process; several installations can share a database. |
| R4 Queues route work | `queue` leads the fetch index; each fetch reads one queue. |
| R5 Unknown kinds | A snooze (`scheduled`, attempt given back) until the limit passes, counted from the first snooze, which records a `no_handler` note; then `dead`, with an `unknown_kind` error. |
| R6 FIFO groups | `group_key`, the `pending` state, `groups`, `tasks_pending_idx`, and `halt_group_on_dead`. |
| R7 Batch handlers | Fetch takes a `LIMIT`, and a second fetch fills batches by kind; complete and heartbeat take arrays. |
| R8 Concurrency limits | `queues.max_running`, the queue row lock, and `tasks_running_idx`. |
| R9 Deduplicated enqueue | `dedupe_keys`, claimed atomically in the enqueue statement. |
| R10 Completion policy | `queues.keep_history`; `task_history` partitions; `settings.history_retention`; complete with or without history. |
| R11 Stable identity | `tasks.id`, kept in history. Global message IDs are the outbox's job. |
| R12 Caller's transactions | All statements are self-contained; complete works in the handler's transaction. |
| R13 Error history | `tasks.errors`, bounded to 20 entries. |
| R14 Recurring schedules | `schedules`; each run is enqueued with a dedupe key for its scheduled time. |
| R15 Cheap monitoring | `queues`, `workers`, and `queue_stats`, refreshed by the maintainer with capped counts. |
| R16 Inspect API | Every list follows an index above, with keyset pagination. |
| R17 Pausing | `queues.paused_at`, announced to workers by notification. |

## Changes to other documents

This design changed two earlier documents:

- **The lifecycle gains a `pending` state** for tasks waiting behind the head
  of their FIFO group, with its transitions. The lease token is defined as a
  counter that only grows. Finishing a grouped task is atomic in one
  transaction, not one statement.
- **The scope's R9 is now "deduplicated enqueue within a time window",** and
  R6's halting is configured per queue.

## Decisions

These questions were settled while drafting this document.

1. **Halting is a queue setting.** `halt_group_on_dead` applies to all groups
   of a queue, because groups are created on the fly and can number in the
   millions. An outbox queue halts its groups; a background-jobs queue lets
   them continue.
2. **History retention is one setting for the installation.** Dropping a day's
   partition removes every queue's rows for that day, and per-queue periods
   would need row deletes, which the partitions exist to avoid. A queue that
   needs no history turns it off with `keep_history`.
