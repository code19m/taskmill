# Key statements

This page explains the statements that enqueue tasks and that workers run,
and why each is written the way it is.
[Lists and counts](schema-lists.md) and
[Operator actions](schema-operations.md) have the statements of the
operations API. [Storage schema](schema.md) has the tables and indexes that
they use, and [`sql_enqueue.go`](../../sql_enqueue.go) and
[`sql_attempts.go`](../../sql_attempts.go) have their exact text.

The statements send batches as arrays and use `unnest`, so a batch, up to the
10,000 tasks that one enqueue call takes, is one round trip with fixed SQL
text, which lets prepared statements be reused.

## Enqueue

Every batch goes through one of two statements. Batches with dedupe keys or
FIFO groups go through one that also claims the keys and counts the groups
(see [FIFO groups](schema-groups.md#fifo-groups) and
[Deduplication](schema-groups.md#deduplication)). All
other batches go through a lean one. Both reserve IDs from the
`tasks_id_seq` identity sequence first, so results map back to their inputs.

**A second statement finds a duplicate's task.** It reads the task of a
dedupe key that another transaction took while the first statement waited
for it. If that task was canceled or deleted meanwhile, the key is free,
and the client enqueues the duplicate's row again: it gets a new task, or
the task that took the key since. So a duplicate never reports the ID 0,
which a caller would store as a task that does not exist. Outside a
transaction the batch is committed by then, so the read outlives the
caller's context, for up to 5 seconds: an error would say that nothing was
stored, and the caller's next try would store the tasks without a key a
second time. Only a database that fails still returns one, as a lost answer
to a commit does. In the caller's transaction, the caller's context bounds
the read, and an error lets the caller roll back.

We rejected the ID 0 for a duplicate whose task the read could not find,
which the client did until v0.14: every caller had to handle a task
without an ID.

## Fetch

The fetch statement, with its list of returned columns shortened. The `least`
calls keep [one malformed row](schema-rows.md) from failing it:

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
    attempt          = least(t.attempt + 1, t.max_attempts),
    lease_token      = least(t.lease_token, 2147483646) + 1,
    lease_expires_at = clock_timestamp() + $3 * interval '1 microsecond',
    attempted_at     = now(),
    attempted_by     = $4
WHERE t.id = ANY(ARRAY(SELECT id FROM picked))
RETURNING t.id, t.queue, t.kind /* and the other columns that the worker reads */;
```

`FOR UPDATE SKIP LOCKED` lets many workers fetch at the same time: each one
skips the rows another is taking instead of waiting for them. A worker that
reads several queues fetches from each queue separately, so every fetch uses
the index order.

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

**A lease counts from the moment the task is taken:** `clock_timestamp()`,
not `now()`, which is the start of the transaction. A fetch can wait for a
row first, and that wait must not shorten the lease. `Worker.RunTask` locks
its task's row before it updates it, for the same reason.

**A queue with a `MaxRunning` limit** first asks, in one statement without a
lock, whether it already runs as many tasks as its limit allows. A limited
queue with a backlog is at its limit most of the time, and the fetch then
ends there, with one read. Otherwise a transaction locks the queue's row in
`queues`, counts the running tasks again through `tasks_running_idx`, and
fetches at most the remaining capacity. This serializes fetches for limited
queues only. The worker sends the transaction in one round trip, up to its
commit, so the row is held while the database works, not while the
network carries answers: with four round trips under the lock, a network
of 4 ms bounded a queue to about 500 tasks a second. So the fetch cannot
wait for the count's answer: it counts again in its own `LIMIT`. Each runs
as a statement of its own after the lock, and sees what the fetches before
committed. Only to fill batches does the commit wait for a second round
trip. The transaction waits at most 1 second for the queue's row.
Then the fetch gives up, with only a Debug line, and the worker fetches
again when the queue is next woken or polled: the queue's fetches take the
row in turn, and with many workers the turns alone can take a second.

A worker fetches one task per free slot. If the fetched tasks include kinds
that have a batch handler, a second statement fills each batch up to its
size. It is the same fetch, limited to one kind. It reads the queue's
available tasks in fetch order, the first 1,000 at most, locks those of its
kind one by one, and stops when the batch is full. So it never sorts, and
stays cheap in a queue of many kinds. A batch takes one slot, so every
fetched task starts right away.

## Renew leases

At each check-in, one statement renews every lease a worker holds. It
returns each task that the worker's attempts still own, and marks the ones it
renewed. A task missing from the result lost its lease, and the worker
cancels that attempt's context. A task with a cancellation request is
canceled the same way. The statement joins from the attempts to the tasks,
with one lookup by ID per attempt, so its time grows with the number of
leases, not with their square.

A worker can also fetch a task while its own older attempt of that task
still runs: it was out of touch for longer than the lease, and the task was
rescued and fetched again before a check-in found out. The new lease proves
that the older attempt lost its own. So the worker cancels the older
attempt's context right away, with `ErrLeaseLost`, and ignores its result.

The statement locks with `SKIP LOCKED`, so one locked row, such as a task that
an application transaction is canceling, cannot block the renewal of all
the others. A skipped task is still owned: the worker keeps its attempt
running and renews it at the next check-in. An attempt that completed its
task with `CompleteTx` is never canceled as lost: its own transaction holds
the row until it commits.

A worker cancels an attempt once its lease may have ended. So after a
renewal fails, the next check-in can come too late: as the lease ends, if
an outage began just before the failed one. The worker tries a failed
renewal again every thirtieth of the lease, until one succeeds. A try that
gets no answer in its time is followed by one with twice as long, up to a
third of the lease: on a slow database, each try that runs out of time
costs a connection, which pgx holds in the pool while it closes it.

The check-ins continue meanwhile with a third of the lease, which a slow
database may need. So two renewals of a lease can overlap, and the one that
started first can commit last: the statement never moves a lease's end
back, and neither does the worker.

Handlers that share the worker's pool can hold every connection for
longer than a lease, and renewals that waited for one would let leases run
out: tasks would run twice. So while `Run` runs, the worker keeps one
connection of the pool idle in reserve. A renewal or a check-in takes a
connection of the pool first, and only if none comes within a thirtieth of
the lease does it run on the reserve. The reserve is pinged before use
once it was idle for more than a second, as the pool pings its own,
because a server or a firewall may have ended its session meanwhile. At
each check-in it is pinged, and replaced once older than the pool's
`MaxConnLifetime`, while the pool has connections to spare. When it broke,
the pool closes it, and the statement runs again, once, on a new
connection, if one comes within the renewal's time: at a check-in, a third
of the lease. The pool gives the broken one's place to whoever waited
first, often a handler, so while handlers hold the pool a broken reserve
may not be replaced. A server or a proxy that ends sessions idle for less than a third
of the lease, as `idle_session_timeout` can, ends the reserve between
check-ins every time, and leaves the worker none when it is needed.

Completions, fetches, and the records of failed attempts use the pool
alone. A completion that waits for a connection keeps its lease, because
renewals continue. It tries until the lease that it began with could have
run out. A try without an answer, as on a connection that a failover left
behind, moves that end to the lease as renewed by then, for a few tries
([Defaults and limits](../reference/defaults-and-limits.md#workers)).

The worker keeps a reserve only if the pool has its total `Concurrency`
plus 5 connections, the size that
[Database](../production/database.md#size-the-connection-pool) asks for,
and warns otherwise. A smaller pool cannot spare one: workers that share
it, each keeping one, could hold all of it, and then none could fetch, and
the application could not enqueue. Through a pooler in transaction mode,
the reserve is a connection to the pooler, not to PostgreSQL: transactions
of handlers that hold every server connection of the pooler still make
renewals wait. We rejected a second pool of the worker's own: it would
open connections that the application did not count in its pool's size.

## Complete

One statement completes a batch: it deletes the tasks whose attempts still
hold their lease tokens, and inserts them into history, except those of kinds
that discard completed tasks. A task missing from the result had lost its
lease, and its result is discarded. The same statement, run inside the
handler's transaction, is transactional completion.

The statement first locks its tasks in ID order, and then deletes the tasks
it holds. A delete alone takes the rows as its plan meets them, which is not
always the order of their IDs: a renewed lease can move an older task's row
behind a newer one's. Locked in ID order, a completion that waits for a task
holds only tasks with lower IDs. So it cannot deadlock with a transaction
that takes tasks in that order too, such as one that cancels several.

Ungrouped tasks complete with this one statement, without a transaction of
its own. Grouped tasks complete in one transaction per batch, which also
updates their groups (see [FIFO groups](schema-groups.md#fifo-groups)). If
the database is unreachable, the worker retries a completion with backoff
until the attempt's lease would expire.

## Fail, snooze, and release

A failure is one `UPDATE` that checks the lease token, sets the state that
the kind's retry policy chose (`retryable` with a new run time, or `dead`),
and appends the error, dropping the oldest once there are 20. A snooze and a
release are similar updates that also give the attempt back
(`attempt = attempt - 1`). From a worker that has a handler for the task,
they also remove the note of a wait for one. All three cancel the task
instead if its row has a cancellation request, and only then: a request
whose notification was missed still takes effect when the attempt ends, and
a notification alone cancels nothing. For a grouped task, the update runs in
a transaction that also updates the group if the task finished.

## See also

- [Storage schema](schema.md): the tables and indexes that these statements use
- [Lists and counts](schema-lists.md) and [Operator actions](schema-operations.md): the statements that list, count, retry, cancel, and delete tasks
- [FIFO groups and deduplication](schema-groups.md): the statements of grouped and deduplicated tasks
- [Maintenance and notifications](schema-maintenance.md): the statements that the maintainer runs

**Next:** [Lists and counts](schema-lists.md)
