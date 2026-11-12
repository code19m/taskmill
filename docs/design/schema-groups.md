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
so the same key can be enqueued again right away. A completed or dead task
keeps its key until the window ends: an inbox relies on that, so that a
redelivered message does not run again after its task failed. Uniqueness
among unfinished tasks only ("at most one queued refresh per product") can be
added later as a new option, without a disruptive migration.

## See also

- [Key statements](schema-statements.md): the Complete statement, and the bulk actions
- [Task lifecycle and guarantees](task-lifecycle.md): invariant 6, and guarantees 8 and 9
- [Task order and FIFO groups](../guides/ordering.md) and [Avoid duplicate tasks](../guides/deduplication.md): the user's view

**Next:** [Maintenance and notifications](schema-maintenance.md)
