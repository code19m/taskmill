# FIFO groups and deduplication

This page explains how a FIFO group keeps its order without slowing fetches
down, and how a dedupe key admits one task. [Storage schema](schema.md) has
the tables `groups` and `dedupe_keys`.

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

**One way out.** A counted task leaves its group when it completes, is
canceled, or is deleted, alone or in a batch. Every such change runs the
same three statements, in one function (`leaveGroups`): the decrement, the
delete of the empty groups, and the promotion. A `pending` task that leaves
was not the head, so nothing is promoted for it. A group that has no row,
which should never happen, is logged as `fifo group missing`, and the task
still ends.

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

Then it completes the tasks of the locked groups with the
[Complete](schema-statements.md#complete) statement,
decrements each group's count, deletes the groups whose count reached zero,
and promotes the oldest `pending` task of every other group, with a
`LATERAL` subquery per group through `tasks_pending_idx`.

`SKIP LOCKED` here means a batch never waits for, or deadlocks with,
another transaction. A task whose group row another transaction holds, for
example an open `EnqueueTx` into that group, stays with the worker, which
keeps renewing its lease and tries it again in a later batch. So one busy
group never blocks the batch, and a waiting completion never keeps a
database connection busy. Maintenance works the same way: it skips group rows
that other transactions hold and retries them in its next cycle, so a long
application transaction cannot stall it.

**The next task starts without a notification.** The promotion notifies the
queue, but a client sends at most one notification per queue per 50 ms (see
[Notifications](schema-maintenance.md#notifications)). A group whose every
task waited for one ran 20 tasks a second. So the worker that completed the
head fetches the queue again when its batch commits. Only that worker does:
the others still wake at the pace of the notifications. Measured with one
worker and tasks that take no time, 200 tasks of one group ran at about 220
a second. What remains is the 2 ms for which the completer gathers a batch,
and the statements of the completion and of the fetch.

The wake-up comes with the commit because the fetch that follows a freed
slot comes too early. A slot is free as soon as its handler returns, and the
completer records the result a few milliseconds later, in a batch. A fetch
in between finds the group's next task still `pending`. It also still counts
the task as running, so a queue at its `MaxRunning` limit looks full: a
committed batch wakes the fetch of such a queue too, and the queue runs at
its limit without a poll. The worker's own notification of the promotion
comes back to it, and its fetch finds nothing. The 20 ms that follow hold
back notifications only, not these wake-ups.

**Lock order.** Group rows are always locked in bytewise order of queue and
group key: `COLLATE "C"` in SQL, and Go's string order in code, which is the
same. A `pending` task's row is locked only after its group's row: promotion
takes them in that order, and `Cancel` and `Delete` lock a pending task's
group first. Locking in one order prevents deadlocks, for example between a
`Cancel` and the promotion of the same task. The row of any other task is
locked before its group's row, as a worker that records a failure and a
`Retry` do, or only if it is free, as completions and bulk actions do. So a
transaction that holds a group's row never waits for the row of a task that
is not pending.

A dedupe key's row comes before a group's row. An enqueue claims its keys
first and counts its groups after, in one statement. A `Retry` locks its
task, then takes the task's key back, and only then takes the group's row.
So a retry that waits for a transaction that claims the same key holds no
group, and that transaction can continue into the group.

`Cancel` and `Delete` cannot know which order applies before they hold a
lock: a `Retry`, a promotion, or the task's own enqueue may commit between
the read of the task's state and the lock. So they check again under the
lock. With the group's row, which every change into and out of `pending`
takes, they lock the task only if it is still pending. With the task's row
alone, they read the state that the lock protects. If the task changed
sides, they roll their transaction back, which releases what they hold, and
start again: the new read locks in the other order. After five tries they
continue with the locks they hold, and PostgreSQL's deadlock detection is
the last resort.

`CancelTx` cannot do this. It runs in the caller's transaction, which it
may not roll back, and a savepoint there costs more than it saves. Each
savepoint that writes is a subtransaction, and past 64 of them in one
transaction, every session's visibility checks slow down until that
transaction ends. A released savepoint also makes whoever waits for a row
that it locked wait for the caller's whole transaction. So `CancelTx`
continues with the locks as it took them, which may be the wrong order. Then
it can deadlock with another transaction that changes the same task or its
group at the same moment: a retry of the task, the end of the group's head,
which promotes the task while a cancel or a delete of it comes in, or an
enqueue that commits between the two statements of `CancelTx`. PostgreSQL
ends one of the transactions with a deadlock error (SQLSTATE 40P01), which
may hit the other one, or a third that only waits between the two, such
as an enqueue into the group. `Cancel`, `Delete`, and `Retry` run again
when it hits them, within their five tries. So do `Enqueue` and
`EnqueueMany`: on the client's pool, the enqueue statement is a transaction
of its own, so nothing of a try that PostgreSQL ended was saved. Any other
transaction that got the error can be run again by its caller.

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
in a second statement. It does the same when the holder it sees is one
whose window ended: the statement did not get that key, so another enqueue
claimed it meanwhile, and its task is the one to report. Two concurrent
enqueues of the same key cannot both succeed: the second waits on the
first's key and then finds it taken.

**A duplicate writes nothing.** A claim that fails still locks the key's
row: it takes a transaction ID and writes WAL, and its transaction commits
as a write. So the real statement first looks: a task whose key it sees as
held gets no ID from the sequence, and is left out of the claim. It is
reported as a duplicate of the holder it saw. One duplicate enqueue read 43
pages and wrote 1 to 3 WAL records before, and reads 17 pages and writes
nothing now. A new key costs one more index lookup, 4 pages.

Deduplication stays exact, because the claim alone still decides who gets a
key: every task that is created went through it. Leaving a key out only
gives the claim's answer early, and the two answers can differ in one case.
The row of a held key changes in one way only: the cancel or the delete of
its task removes it. An enqueue and a retry claim only a key whose window
has ended. If such a removal is not committed when the statement begins,
the claim would wait for it, and then take the key. The statement reports
the duplicate instead: the answer of an enqueue that ran just before the
removal, which is a correct order of the two.

This holds only if the statement sees the keys as they are when it begins,
as at READ COMMITTED. A caller's transaction at REPEATABLE READ or
SERIALIZABLE sees them as they were when it began, so a key that it sees as
held may be free. There the statement leaves no key out, and the claim,
which looks at the rows as they are now, decides alone.

`Cancel` and `Delete` delete the task's key, through the index on `task_id`,
so the same key can be enqueued again right away. A completed or dead task
keeps its key until the window ends: an inbox relies on that, so that a
redelivered message does not run again after its task failed. Uniqueness
among unfinished tasks only ("at most one queued refresh per product") can be
added later as a new option, without a disruptive migration.

**A retry takes the key back.** A cancel stores the key that it frees with
the task, and a retry of the canceled task claims it again: see
[Lists and operator actions](schema-operations.md#a-retry-takes-its-dedupe-key-back).

## See also

- [Key statements](schema-statements.md): the Complete statement
- [Lists and operator actions](schema-operations.md): the bulk actions
- [Task lifecycle and guarantees](task-lifecycle.md): invariant 6, and guarantees 8 and 9
- [Task order and FIFO groups](../guides/ordering.md) and [Avoid duplicate tasks](../guides/deduplication.md): the user's view

**Next:** [Maintenance and notifications](schema-maintenance.md)
