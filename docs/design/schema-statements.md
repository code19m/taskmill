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

**A second statement only fills in an ID.** It reads the task of a dedupe
key that another transaction took while the first statement waited for it.
By then the batch is stored, and on the client's pool it is committed. So
`Enqueue` and `EnqueueMany` log a read that fails, and return the duplicate
with the ID 0, as they do when the task that took the key was canceled or
deleted meanwhile. An error would say that nothing was stored, and the
caller's next try would store the tasks without a key a second time. In the
caller's transaction the enqueue returns the error, and the caller rolls
back.

We rejected a read that outlives the caller's context, and a second try of
the read. Both make a call that is done wait for an ID, and a caller must
handle the ID 0 anyway.

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

While `Run` runs, the worker keeps one connection of its pool for the
renewals, the check-ins, and the completions. Handlers that share the pool
can hold every other connection for longer than a lease. Renewals that
waited for one would let leases run out, and completions that waited could
give up once the lease might have run out: either way, tasks would run
twice. The statements take turns on the connection, each for a moment, and
none holds it for more than a third of the lease, or a second if that is
longer: a statement that hangs delays the next renewal by about one
check-in, which a lease outlasts. Fetches and failed attempts take
connections of the pool, and may wait. We rejected a second pool of the
worker's own: it would open connections that the application did not
size, and skip the hooks of its pool.

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
