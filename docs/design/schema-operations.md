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

- [Key statements](schema-statements.md): the statements that enqueue, fetch, and complete tasks
- [FIFO groups and deduplication](schema-groups.md): the lock order that the operator actions keep
- [Manage tasks from code](../guides/operations-api.md): the user's view

**Next:** [FIFO groups and deduplication](schema-groups.md)
