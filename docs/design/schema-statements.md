# Key statements

This page explains the statements that move tasks between states, and why
each is written the way it is. [Storage schema](schema.md) has the tables
and indexes that they use, and [`sql.go`](../../sql.go) has their exact
text.

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

## Fetch

The fetch statement, with its list of returned columns shortened:

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
queues only. The transaction waits at most 1 second for the queue's row.
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

## Listing

`Client.List` and the dashboard page through tasks by `id`, newest
first, with keyset pagination: each page starts below the last `id` of the
previous one. Unfinished tasks and history are read in two branches, each
limited on its own, and merged. Each page runs with a 5-second statement
timeout, and with custom query plans (`plan_cache_mode = force_custom_plan`),
so the planner sees which filters are set. Two more things let it pick the
index that gives a page in ID order:

- **A single queue, kind, or group is compared with `=`,** not with a list
  of one value. Only then does the planner see that an index which leads
  with that column gives its rows in the order of the next column: the
  history's `(queue, id)` for one queue's completed tasks, and
  `tasks_pending_idx` for the waiting tasks of one queue's group.
- **Each selected state is tested as a literal,** so the planner sees which
  states are left, and which partial indexes hold them.

A list of one state across all queues, such as every dead task, has no
index in ID order: it still reads the newer tasks of other states first.

`Client.Count` has one branch per state, and custom plans leave out the
branches of the states not counted. Sequential and bitmap scans are off for
it, so each branch reads the index of its state and stops at its cap. A
count by kind or FIFO group alone still reads every task of each state.

## Operator actions

- **Cancel** cancels a waiting task right away, and flags a running one with
  `cancel_requested_at`. A `pending` task's group row is locked first.
- **Retry** moves a `dead` or `canceled` task back to `available` with
  `attempt = 0`, keeping its error history, or to `pending` if its group has
  other unfinished tasks.
- **Cancel and Delete** release the task's dedupe key in the same
  transaction.
- **Bulk actions** (`RetryMatching`, `CancelMatching`, `DeleteMatching`) do
  the same to every task that matches a filter, in batches of 1,000, each in
  a transaction of its own.
  - **Keyset by ID.** A batch takes the next 1,000 matching tasks in ID
    order after the last one of the previous batch, up to the newest task
    when the action started. So each batch starts where the last one ended,
    through the primary key or a state's partial index, and the work grows
    with the tasks the action reads, not with their square. On 2 million
    tasks, canceling 500,000 of one queue kept a steady pace of about 40,000
    per second, measured before the windows below existed.
    It also reads each task once, so a retry never retries a task twice, and
    it ends however fast tasks arrive. And a retry larger than a batch keeps
    a group in order: the group's oldest task becomes its head.
  - **Windows, when the matching tasks lie far apart.** A batch first reads
    without a window. With a filter that an index serves, it reads only the
    tasks that match. Without one, it reads every task before the next 1,000
    that match, so the database ends a statement of such a batch after 1
    second. The action then reads in windows, starting with that batch: a
    batch reads at most the next 100,000 tasks of each table, whether they
    match or not, and moves to the window's end once it finds less than a
    full batch before it. It still reads each task once, in ID order.
  - **No waiting while holding rows.** The batch's one statement that picks
    tasks reads the next 1,000 without locks, then locks those it can, the
    rows of their groups, and the rows of their dedupe keys, all with
    `SKIP LOCKED`, as rescue does, and checks again that they match. It
    returns all 1,000, so that the batch knows which ones it skipped. Then
    the batch changes only rows it holds: the tasks, their groups in a few
    statements for all of them, and the next task of each group whose head
    left, promoted in a statement of its own, as completions do.
  - **Groups stay in order.** Once a retry skips a task of a group, it skips
    the group's later tasks too, until it ends, and counts them as skipped.
    A later run then retries the group's tasks oldest first.
  - **Two bounded waits.** A retry into a group that has no row creates the
    row. A concurrent enqueue may be creating the same row, uncommitted, and
    an insert must wait for it to decide. The batch inserts under a
    savepoint with a lock timeout of 100 ms, far below the deadlock timeout
    of 1 s, and on timeout leaves those groups' tasks for a later run. So it
    never stalls an application transaction for long, and never makes one
    fail with a deadlock. A transaction per new group would avoid the wait
    too, but cost a transaction per group when every group is new, as after
    canceling a whole queue. The promotion of a group's next task waits for
    that task's row, under the same lock timeout. The batch holds the
    group's row, so only two callers can hold the task's row (see
    [Lock order](schema-groups.md#fifo-groups)): a `Cancel` or a `Delete`
    that is about to start again, which makes the wait short, and a
    `CancelTx` that locked the task after another transaction made it
    pending. If the wait times out, the batch rolls back, and the action
    stops with a cursor to continue.
  - **Exact counts, and continuing.** A batch whose statements ran commits
    even if the caller's context ends meanwhile, so the number of changed
    tasks is exact. An action that stops early returns a cursor to continue:
    at the first task it skipped, if any, or else after the last task it went
    through. Continuing at the first skipped task keeps no state between
    calls, and still brings a group that the action left back in order: the
    tasks that the action changed since no longer match, so reading past
    them again is cheap.
  - **Delete goes through two tables.** It reads the next batch of both
    tasks and history, and goes up to the end of the shorter one, so that
    one position covers both.
  - **The remaining limit.** A filter by kind or FIFO group alone has no
    index. A retry with one reads the dead and canceled entries of
    `tasks_finished_idx` after its position in each batch, and where a
    batch needs more than the second, a window bounds what it reads.

## See also

- [Storage schema](schema.md): the tables and indexes that these statements use
- [FIFO groups and deduplication](schema-groups.md): the statements of grouped and deduplicated tasks
- [Maintenance and notifications](schema-maintenance.md): the statements that the maintainer runs

**Next:** [FIFO groups and deduplication](schema-groups.md)
