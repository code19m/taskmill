# Lists and operator actions

This page explains the statements behind the operations API and the
dashboard: those that list and count tasks, and those that retry, cancel,
and delete them, one task or every task that matches a filter.
[Key statements](schema-statements.md) has the statements that workers run,
and [`sql.go`](../../sql.go) has the exact text of both.

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

**Dead and canceled tasks have no index in ID order,** for one queue or for
all: `tasks_finished_idx` holds them by queue, state, and the time they
finished. So the planner has two ways to read such a page, and both can be
far from the tasks it shows:

| Way | It reads | Fast when |
|---|---|---|
| The primary key, backward | Every newer task of any queue and state, until the page is full | The page's tasks are among the newest |
| `tasks_finished_idx`, then a sort | Every dead or canceled task of the queue, or of all queues | Those are few |

The planner takes the index only where it expects few matches. For one
queue, it multiplies the queue's part of all tasks by the part of all tasks
that is dead. So a small queue gets the index, and a large queue with no
dead task of its own does not: its page reads the whole table. Measured on
341,000 tasks of which 50,000 were dead, older than 51,000 other tasks: the
dead tasks of all queues, and of the queue with most of them, read 51,000
tasks for a page of 50. Those of a queue with 6% of the tasks and none dead
read all 341,000. The time limit bounds every such read. An index on `id`
for dead tasks would give the page 51 rows, but a migration that builds it
must read a busy table under a lock, which is a design of its own.

**A summary leaves out what makes a task large.** With
`ListParams.Summary`, the statement returns no payload and no metadata, and
of each task's errors only the newest. The planner sees the parameter, so
it drops those columns from the plan, and the database does not read their
values from TOAST. The dashboard's lists ask for summaries: one page of 50
tasks with payloads of 1 MiB took 54 MiB of memory to show 50 KB, at every
refresh of every open tab. A task's own page reads the task in full, with
`Get`. For a dead task that halts its group, that page counts the tasks
behind it with `Count`, up to its cap, and does not list them.

`Client.Count` has one branch per state, and custom plans leave out the
branches of the states not counted. Sequential and bitmap scans are off for
it, so each branch reads the index of its state and stops at its cap. A
count by kind or FIFO group alone still reads every task of each state.

**Each read is one round trip.** `List`, `Count`, `Groups`, and `Activity`
send their settings and their statement as one batch. PostgreSQL runs a
batch as one transaction, so the time limit and the plan settings end with
it. The batch has no `BEGIN`: with one, a read that runs out of time would
leave a failed transaction open, and the pool closes such a connection.
`Queues` sends its two statements the same way. A dashboard page makes one
round trip for each thing it shows: the overview 5, and the task list 2. A
connection that sends a statement for the first time prepares it in a round
trip before the batch, outside the time limit: only a lock on a whole table
makes that step wait.

## Operator actions

- **Cancel** cancels a waiting task right away, and flags a running one with
  `cancel_requested_at`. A `pending` task's group row is locked first.
- **Retry** moves a `dead` or `canceled` task back to `available` with
  `attempt = 0`, keeping its error history, or to `pending` if its group has
  other unfinished tasks. Before that, it takes back the dedupe key that the
  task's cancel released, or stops with `ErrConflict` if another task holds
  it (see [Deduplication](schema-groups.md#deduplication)).
- **Cancel and Delete** release the task's dedupe key in the same
  transaction. A cancel stores the key with the task, in the statement
  that cancels it: one statement for a waiting and for a running task.
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
  - **Keys come back with their tasks.** A retry takes back the dedupe
    keys of the canceled tasks in its batch, with one statement, before it
    counts the tasks into their groups. It leaves a task whose key another
    task holds, and counts it in `BulkResult.Conflicts`, apart from the
    skipped tasks: a later run leaves it again. Such a task does not make
    the action leave its group. If the batch then leaves a task after all,
    because its group has no row yet, it gives that task's key away again:
    a canceled task holds no key.
  - **Three bounded waits.** A retry that takes a key back inserts the
    key's row, or changes it. An enqueue that claims the same key, or a
    transaction that holds its row, makes that statement wait. A retry
    into a group that has no row creates the row. A concurrent enqueue may
    be creating the same row, uncommitted, and an insert must wait for it
    to decide. The batch runs each of the two under a savepoint with a
    lock timeout of 100 ms, far below the deadlock timeout of 1 s. On
    timeout it takes no key, or creates no group, and leaves those tasks
    for a later run. So it never stalls an application transaction for
    long, and never makes one fail with a deadlock. A transaction per new group would avoid the wait
    too, but cost a transaction per group when every group is new, as after
    canceling a whole queue. The promotion of a group's next task waits for
    that task's row, under the same lock timeout. The batch holds the
    group's row, so only two callers can hold the task's row (see
    [Lock order](schema-groups.md#fifo-groups)): a `Cancel` or a `Delete`
    that is about to start again, which makes the wait short, and a
    `CancelTx` that locked the task after another transaction made it
    pending. If the wait times out, the batch rolls back, and the action
    stops with a cursor to continue.
  - **No lock timeout at the commit.** The timeout is for the batch's
    statements, and the batch lifts it as its last one. A transaction that
    sent a notification takes one more lock when it commits: the lock that
    every notifying transaction of the server takes in turn, and holds
    until its commit is done. With synchronous replication, that includes
    the wait for the standby. Under the timeout, a batch whose work was
    done failed there. A schedule's run, which waits at most a second for
    a group's row, lifts its timeout the same way.
  - **Counts, and continuing.** A batch whose statements ran commits even
    if the caller's context ends meanwhile, so the action counts it. The
    count is exact for a call that returns no error. A commit is bounded by
    the batch's 5 seconds in the client only: past them, the client closes
    the connection, and the database may still commit that batch. So a call
    that returns an error may have changed up to 1,000 tasks more than it
    reports. An action that stops early returns a cursor to continue:
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

- [Key statements](schema-statements.md): the statements that enqueue, fetch, and complete tasks
- [FIFO groups and deduplication](schema-groups.md): the lock order that the operator actions keep
- [Manage tasks from code](../guides/operations-api.md): the user's view

**Next:** [FIFO groups and deduplication](schema-groups.md)
