# Operator actions

This page explains the statements that retry, cancel, and delete tasks: one
task, or every task that matches a filter. The operations API and the
dashboard run them. [Lists and counts](schema-lists.md) has the statements
that read tasks, [Key statements](schema-statements.md) has those that
workers run, and [`sql.go`](../../sql.go) has the exact text.

## One task

- **Cancel** cancels a waiting task right away, and flags a running one with
  `cancel_requested_at`. A `pending` task's group row is locked first.
- **Retry** moves a `dead` or `canceled` task back to `available` with
  `attempt = 0`, keeping its error history, or to `pending` if its group has
  other unfinished tasks. Before that, it takes back the dedupe key that the
  task's cancel released, or stops with `ErrConflict` if another task holds
  it (see [below](#a-retry-takes-its-dedupe-key-back)). One statement writes
  the retry of one task and of a batch, and notifies the queue.
- **Cancel and Delete** release the task's dedupe key in the same
  transaction. A cancel stores the key with the task, in the statement
  that cancels it: one statement for a waiting and for a running task.

## Every task that matches a filter

The bulk actions (`RetryMatching`, `CancelMatching`, `DeleteMatching`) do
the same to every task that matches a filter, in batches of 1,000, each in a
transaction of its own.

- **Keyset by ID.** A batch takes the next 1,000 matching tasks in ID order
  after the last one of the previous batch, up to the newest task when the
  action started. So each batch starts where the last one ended, through the
  primary key or a state's partial index, and the work grows with the tasks
  the action reads, not with their square. On 2 million tasks, canceling
  500,000 of one queue kept a steady pace of about 40,000 per second,
  measured before the windows below existed. It also reads each task once,
  so a retry never retries a task twice, and it ends however fast tasks
  arrive. And a retry larger than a batch keeps a group in order: the
  group's oldest task becomes its head.
- **Windows, when the matching tasks lie far apart.** A batch first reads
  without a window. With a filter that an index serves, it reads only the
  tasks that match. Without one, it reads every task before the next 1,000
  that match, so the database ends a statement of such a batch after 1
  second. The action then reads in windows, starting with that batch: a
  batch reads at most the next 100,000 tasks of each table, whether they
  match or not, and moves to the window's end once it finds less than a full
  batch before it. It still reads each task once, in ID order.
- **No waiting while holding rows.** The batch's one statement that picks
  tasks reads the next 1,000 without locks, then locks those it can, the
  rows of their groups, and the rows of their dedupe keys, all with
  `SKIP LOCKED`, as rescue does, and checks again that they match. It
  returns all 1,000, so that the batch knows which ones it skipped. Then the
  batch changes only rows it holds: the tasks, their groups in a few
  statements for all of them, and the next task of each group whose head
  left, promoted in a statement of its own, as completions do.
- **Groups stay in order.** Once a retry skips a task of a group, it skips
  the group's later tasks too, until it ends, and counts them as skipped. A
  later run then retries the group's tasks oldest first.
- **Keys come back with their tasks.** A retry takes back the dedupe keys of
  the canceled tasks in its batch, with one statement, before it counts the
  tasks into their groups. It leaves a task whose key another task holds,
  and counts it in `BulkResult.Conflicts`, apart from the skipped tasks: a
  later run leaves it again. Such a task does not make the action leave its
  group. If the batch then leaves a task after all, because its group has no
  row yet, it gives that task's key away again: a canceled task holds no
  key.
- **Three bounded waits.** A retry that takes a key back inserts the key's
  row, or changes it. An enqueue that claims the same key, or a transaction
  that holds its row, makes that statement wait. A retry into a group that
  has no row creates the row. A concurrent enqueue may be creating the same
  row, uncommitted, and an insert must wait for it to decide. The batch runs
  each of the two under a savepoint with a lock timeout of 100 ms, far below
  the deadlock timeout of 1 s. On timeout it takes no key, or creates no
  group, and leaves those tasks for a later run. So it never stalls an
  application transaction for long, and never makes one fail with a
  deadlock. A transaction per new group would avoid the wait too, but cost a
  transaction per group when every group is new, as after canceling a whole
  queue. The promotion of a group's next task waits for that task's row,
  under the same lock timeout. The batch holds the group's row, so only two
  callers can hold the task's row (see
  [Lock order](schema-groups.md#fifo-groups)): a `Cancel` or a `Delete` that
  is about to start again, which makes the wait short, and a `CancelTx` that
  locked the task after another transaction made it pending. If the wait
  times out, the batch rolls back, and the action stops with a cursor to
  continue.
- **No lock timeout at the commit.** The timeout is for the batch's
  statements, and the batch lifts it as its last one. A transaction that
  sent a notification takes one more lock when it commits: the lock that
  every notifying transaction of the server takes in turn, and holds until
  its commit is done. With synchronous replication, that includes the wait
  for the standby. Under the timeout, a batch whose work was done failed
  there. A schedule's run, which waits at most a second for a group's row,
  lifts its timeout the same way.
- **Counts, and continuing.** A batch whose statements ran commits even if
  the caller's context ends meanwhile, so the action counts it. The count is
  exact for a call that returns no error. A commit is bounded by the batch's
  5 seconds in the client only: past them, the client closes the connection,
  and the database may still commit that batch. So a call that returns an
  error may have changed up to 1,000 tasks more than it reports. An action
  that stops early returns a cursor to continue: at the first task it
  skipped, if any, or else after the last task it went through. Continuing
  at the first skipped task keeps no state between calls, and still brings a
  group that the action left back in order: the tasks that the action
  changed since no longer match, so reading past them again is cheap.
- **Delete goes through two tables.** It reads the next batch of both tasks
  and history, and goes up to the end of the shorter one, so that one
  position covers both.
- **The remaining limit.** A filter by kind or FIFO group alone has no
  index. A retry with one reads the dead and canceled entries of
  `tasks_finished_idx` after its position in each batch, and where a batch
  needs more than the second, a window bounds what it reads.

## A retry takes its dedupe key back

A cancel frees the task's dedupe key (see
[Deduplication](schema-groups.md#deduplication)). If the retry of the
canceled task did not take the key back, a cancel, an enqueue of the same
key, and a retry would leave two unfinished tasks for one key.
`dedupe_keys` holds one row per key, so a canceled task cannot keep a row
there. The statement that cancels a task and deletes its key therefore
stores the key and the end of its window in the task's own row: `freed_key`
and `freed_key_expires_at`. A running task that is asked to stop gets them
then, with the key's release.

A retry claims the stored key as an enqueue does, with the window's old
end: it gets the key if no row has it, or if the row's window has ended.
Then it clears the two columns. If another task holds the key, a `Retry`
rolls back and returns `ErrConflict`, and a bulk retry leaves the task and
counts it. A stored key whose window has ended claims nothing: the window
is fixed at the enqueue, and a task past it has no claim, canceled or not.

| Decision | Why |
|---|---|
| Two columns, not the task's metadata | Producers write metadata, and handlers and tools copy it. A key there could be forged, or travel to another task. Only a role with `UPDATE` on `tasks` writes the columns: the grants of a role that only enqueues leave them out |
| The retry is refused whatever state the holder is in | A completed or dead holder still stands for the work until its window ends, as it does for an enqueue |
| The refusal names the holder only if that task exists | A completed task keeps its key for its window, also when no history is kept of it. Nobody can cancel or delete a task that is gone, so the error gives the end of the window |
| A stored key that is not 32 bytes counts as none | `dedupe_keys` would refuse it, and one such row would fail every batch of a bulk retry |
| The columns are not copied to the history | A task that completes after its cancel was requested needs no key back |

Two things follow from the claim. A transaction of the caller that enqueues
a key and then calls `CancelTx` for a task, while a `Retry` of that task
waits for the key, makes the two wait for each other: the retry runs again,
or the caller's transaction gets the deadlock error. And while a
transaction that claimed a key stays open, a bulk retry leaves the tasks of
a batch that need a key, and counts them as skipped.

The previous release names the columns that it writes, so it works on the
new schema as before: its cancel stores no key, and a retry of that task
takes none. Its retry leaves what a cancel of this release stored, on a
task that runs on without its key. Only a canceled task takes a key back,
so that task asks for none if it dies. After another cancel, a retry of
this release takes the stored key back if it is free.

## See also

- [Lists and counts](schema-lists.md): the statements that list and count tasks
- [Key statements](schema-statements.md): the statements that enqueue, fetch, and complete tasks
- [FIFO groups and deduplication](schema-groups.md): the lock order that the operator actions keep
- [Manage tasks from code](../guides/operations-api.md): the user's view

**Next:** [FIFO groups and deduplication](schema-groups.md)
