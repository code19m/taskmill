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
statement sees the data as of its own start, so one combined statement misses
a task whose enqueue committed while it waited, and that task stays `pending`
forever. A second statement starts after the lock was acquired, and sees it:
one reason why taskmill's own transactions require READ COMMITTED.

**One way out.** A counted task leaves its group when it completes, is
canceled, or is deleted. Every such change runs the same statements, built
in one place (`groupLeaving`): the decrement, which also ends a halt that
names a leaving task, then the settling, which deletes the groups that
count no task and promotes the next task of each group whose head left.
The settling reads the counts that the decrement left, not what it
returned, so a worker sends both in one round trip. Other callers read the
decrement first, and skip the settling when it has nothing to do, as when
a `pending` task leaves. A group that has no row, which should never
happen, is logged as `fifo group missing`, and the task still ends.

**Completing a batch.** A worker completes the grouped tasks of a batch in
one transaction of two round trips. The first begins it, and locks the
rows of the batch's groups that no other transaction holds:

```sql
SELECT g.queue, g.group_key FROM taskmill.groups g
JOIN unnest($1::text[], $2::text[]) AS u(queue, group_key)
  ON g.queue = u.queue AND g.group_key = u.group_key
FOR UPDATE OF g SKIP LOCKED;
```

and completes, with the [Complete](schema-statements.md#complete)
statement, the tasks whose groups' rows it holds and whose own rows are
free. The second decrements the groups' counts, settles them, and commits.
So a batch holds its groups' rows, which an enqueue into one of those
groups waits for, for about one round trip: it held them for six before.

`SKIP LOCKED` here means a batch never waits for, or deadlocks with,
another transaction. A task whose group row another transaction holds, as an
open `EnqueueTx` into that group does, stays with the worker, which renews
its lease and tries it again in a later batch. Maintenance skips held group
rows until its next cycle too, so a long transaction stalls neither.

**A busy group's head waits for the row.** Retries that skip a held row
rarely find it free when short transactions enqueue into the group one after
another. Measured: with 70 enqueues a second, each holding the row for 5 ms,
2 to 4 of 100 tasks completed meanwhile. So up to 4 completions of a worker
also wait for their groups' rows, up to 100 ms, each in a transaction that
writes nothing and holds no other row before; it then locks the task's row
if it is free. PostgreSQL hands the row to those who wait in turn: 1 to 4 of
the 100 were left, and 3 to 9 of 300 in 3 such groups. Each wait costs a
transaction, so a worker keeps up with a few busy groups, and more take more
workers. A wait that ran out is tried again a second later at the soonest,
and only while no other waits: a long transaction soon costs one connection.

**The next task starts without a notification.** The promotion's
notification goes out with the client's next batch, at most one per 50 ms
(see [Notifications](schema-maintenance.md#notifications)): a group whose
every task waited for one ran 20 tasks a second. So the worker that
completed the head fetches the queue again when its batch commits. With one
worker and tasks that take no time, 200 tasks of one group ran at about 220
a second: what remains is the completer's 2 ms, the completion, and the
fetch.

The fetch that follows a freed slot comes too early: a slot is free as soon
as its handler returns, a few milliseconds before the completer records the
result. That fetch finds the group's next task still `pending`, and counts
the task as running, so a queue at its `MaxRunning` limit looks full: a
committed batch wakes such a queue's fetch too. The worker's own
notification of the promotion comes back to it later, and finds nothing.

**Lock order.** An enqueue, and a bulk retry that creates groups, take the
rows of several groups in bytewise order of queue and group key
(`COLLATE "C"`). The decrement takes rows in the order of its plan. So
batches of completions, bulk actions, and rescues lock their groups' rows
first and skip the held ones, and a busy group's head waits for one group.
Only a stopping worker's last completions wait for several groups, and run
again after a deadlock.

A `pending` task's row is locked only after its group's row: promotion
takes them in that order, and `Cancel` and `Delete` lock a pending task's
group first. Locking in one order prevents deadlocks, for example between a
`Cancel` and the promotion of the same task. The row of any other task is
locked before its group's row, as a worker that records a failure and a
`Retry` do, or only if it is free, as completions and bulk actions do. So a
transaction that holds a group's row never waits for the row of a task that
is not pending.

A dedupe key's row comes before a group's row. An enqueue claims its keys,
then counts its groups. `Retry` and `CompleteTx` lock the task, take back
the key that a cancel freed, then take the group's row: while they wait for
a transaction that claims the key, they hold no group. A claim skips a key
that another task holds, whose pending holder's cancel takes group, then key.

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
may not roll back, and a savepoint there costs more than it saves: past 64
subtransactions in one transaction, every session's visibility checks slow
down until it ends, and a released savepoint makes whoever waits for a row
that it locked wait for the caller's whole transaction. So `CancelTx` keeps
the locks as it took them, perhaps in the wrong order, and can deadlock with
a transaction that changes the same task or its group at that moment: a
retry of the task, the end of the group's head, which promotes the task as a
cancel or a delete of it comes in, or an enqueue that commits between the
two statements of `CancelTx`. PostgreSQL ends one of them with a deadlock
error (SQLSTATE 40P01), which may also hit a third that only waits between
the two, such as an enqueue into the group. `Cancel`, `Delete`, and `Retry`
run again when it hits them, within their five tries, and so do `Enqueue`
and `EnqueueMany`, whose statement on the client's pool is a transaction of
its own. A caller can run any other such transaction again.

**Halting.** When a group's head becomes dead, the count is not decremented
and `halted_task_id` is set. The group waits until an operator retries the
dead head, which resumes the group, or deletes it, which promotes the next
task. Halting is always on: groups exist to keep an order, which skipping a
dead task breaks. A canceled head leaves like a completed one: a cancel is
an explicit choice.

**Costs.** A grouped enqueue writes two rows instead of one, and finishing a
grouped task writes up to two more: the group's row, and the next task it
promotes. Ungrouped tasks pay nothing. Producers that enqueue into the same
group wait for each other's transactions to commit, and so does its head's
completion: keep transactions that enqueue grouped tasks short. Two
transactions that enqueue into the same groups in opposite orders can
deadlock; PostgreSQL aborts one of them, and batch enqueues sort their
groups to avoid it.

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
statement. The client reads the key again in a
[second statement](schema-statements.md#enqueue) if the statement cannot
see that task, whose transaction committed while it waited for the key, or
sees a holder whose window ended, so that another enqueue claimed the key
meanwhile. Two concurrent enqueues of the same key cannot both succeed: the
second waits on the first's key and then finds it taken.

**A duplicate writes nothing.** A claim that fails still locks the key's
row: it takes a transaction ID and writes WAL, and its transaction commits
as a write. So the real statement first looks: a task whose key it sees as
held gets no ID, is left out of the claim, and is reported as a duplicate
of the holder it saw. One duplicate enqueue read 43 pages and wrote 1 to 3
WAL records before, and reads 17 pages and writes nothing now. A new key
costs one more index lookup, 4 pages.

Deduplication stays exact, because the claim alone still decides who gets a
key: every task that is created went through it. Leaving a key out only
gives the claim's answer early, and the two can differ in one case. A held
key's row changes only when the cancel or the delete of its task removes it:
enqueues and retries claim only keys whose window ended. If such a removal
is not committed when the statement begins, the claim would wait for it and
take the key, while the statement reports the duplicate: the answer of an
enqueue that ran just before the removal, a correct order of the two.

This holds only at READ COMMITTED, where the statement sees the keys as
they are when it begins. A caller's transaction at REPEATABLE READ or
SERIALIZABLE sees them as they were when it began, so there the statement
leaves no key out, and the claim, which reads the rows as they are now,
decides alone.

`Cancel` and `Delete` delete the task's key, through the index on `task_id`,
so the same key can be enqueued again right away. A completed or dead task
keeps its key until the window ends: an inbox relies on that, so that a
redelivered message does not run again after its task failed.

**A retry takes the key back.** A cancel stores the key that it frees with
the task, and a retry of the canceled task claims it again: see
[Operator actions](schema-operations.md#a-retry-takes-its-dedupe-key-back).

## See also

- [Key statements](schema-statements.md): the Complete statement
- [Operator actions](schema-operations.md): the bulk actions
- [Task lifecycle and guarantees](task-lifecycle.md): invariant 6, and guarantees 8 and 9
- [Task order and FIFO groups](../guides/ordering.md) and [Avoid duplicate tasks](../guides/deduplication.md): the user's view

**Next:** [Maintenance and notifications](schema-maintenance.md)
