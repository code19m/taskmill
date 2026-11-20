package taskmill

// This file holds the statements that change the rows of FIFO groups. The
// invariants that they keep are in
// https://github.com/code19m/taskmill/blob/main/docs/design/schema-groups.md.

// groupFreeSQL matches the tasks t that have no FIFO group, whose group's row it
// locks, or whose group has no row. It skips group rows that other
// transactions hold, so a statement never waits for them.
const groupFreeSQL = `(t.group_key IS NULL
       OR EXISTS (SELECT FROM {s}.groups g WHERE g.queue = t.queue AND g.group_key = t.group_key
                  FOR UPDATE SKIP LOCKED)
       OR NOT EXISTS (SELECT FROM {s}.groups g WHERE g.queue = t.queue AND g.group_key = t.group_key))`

// $1 queue, $2 group key, $3 the dead head's task ID.
//
// haltGroupSQL marks a group halted by its dead head, which stays
// counted.
const haltGroupSQL = `
UPDATE {s}.groups SET halted_task_id = $3, halted_at = now(), updated_at = now()
WHERE queue = $1 AND group_key = $2`

// $1 queues, $2 group keys.
//
// lockGroupsSQL locks the rows of groups ($1, $2) that no other
// transaction holds. It returns each locked group with true, and each
// existing group with false, so a group it does not return has no row.
// Skipping held rows means a batch never waits for another
// transaction.
const lockGroupsSQL = `
WITH wanted AS (
    SELECT DISTINCT queue, group_key FROM unnest($1::text[], $2::text[]) AS u(queue, group_key)
), locked AS (
    SELECT g.queue, g.group_key FROM {s}.groups g
    JOIN wanted w ON g.queue = w.queue AND g.group_key = w.group_key
    FOR UPDATE OF g SKIP LOCKED
)
SELECT queue, group_key, true FROM locked
UNION ALL
SELECT g.queue, g.group_key, false FROM {s}.groups g
JOIN wanted w ON g.queue = w.queue AND g.group_key = w.group_key`

// $1 queues, $2 group keys, $3 changes of their counts, $4 task IDs that left.
//
// recountGroupsSQL adds $3, which is negative for tasks that left, to the
// counts of groups ($1, $2), and returns each group that has a row,
// with its count and whether it is still halted. A group is named
// once: an UPDATE changes a row once.
//
// $4 are the tasks that left, or none. If one of them halts its
// group, the halt ends here, with the task's place in the count. A
// halt that stayed would name a task that is no longer counted, or
// no longer exists: a retry of that task would end the halt and
// count the task as still there, and nothing could end the halt of
// a task that is gone. A task that taskmill halted a group by is
// dead, and leaves only when it is deleted, but a row written by
// hand can name any task.
//
// It locks the rows, and waits for one that another transaction
// holds. The plan decides in which order it takes the rows of
// several groups: a nested loop takes them in the order of the
// arrays, and a hash join can take them as they lie in the table.
//
// A count stops at zero. Only a row that taskmill did not write
// counts fewer tasks than its group holds, and a count below zero
// breaks the table's rule: the error would fail the caller's
// transaction, which in a rescue ends the expired leases of every
// queue, and in a completion the tasks of every other group in the
// batch.
const recountGroupsSQL = `
UPDATE {s}.groups g
SET tasks = greatest(g.tasks + u.n, 0),
    halted_task_id = CASE WHEN g.halted_task_id = ANY($4::bigint[]) THEN NULL ELSE g.halted_task_id END,
    halted_at = CASE WHEN g.halted_task_id = ANY($4::bigint[]) THEN NULL ELSE g.halted_at END,
    updated_at = now()
FROM unnest($1::text[], $2::text[], $3::integer[]) AS u(queue, group_key, n)
WHERE g.queue = u.queue AND g.group_key = u.group_key
RETURNING g.queue, g.group_key, g.tasks, g.halted_task_id IS NOT NULL`

// $1 queues, $2 group keys.
//
// deleteEmptyGroupsSQL deletes the rows of the groups ($1, $2) that count no
// task.
const deleteEmptyGroupsSQL = `
DELETE FROM {s}.groups g USING unnest($1::text[], $2::text[]) AS u(queue, group_key)
WHERE g.queue = u.queue AND g.group_key = u.group_key AND g.tasks = 0`

// $1 queues, $2 group keys, $3 channel, $4 queues to notify.
//
// promoteGroupsSQL makes the oldest pending task of each group ($1, $2)
// its head, and notifies channel $3 of the queues in $4 that got an
// available task. It must run as its own statement after
// recountGroups, so that it sees tasks enqueued while recountGroups
// waited for a group's lock.
const promoteGroupsSQL = `
WITH next AS (
    SELECT p.id FROM unnest($1::text[], $2::text[]) AS u(queue, group_key)
    CROSS JOIN LATERAL (
        SELECT id FROM {s}.tasks
        WHERE queue = u.queue AND group_key = u.group_key AND state = 'pending'
        ORDER BY id
        LIMIT 1
        FOR UPDATE
    ) p
), promoted AS (
    UPDATE {s}.tasks t
    SET state = CASE WHEN t.run_at > now() THEN 'scheduled' ELSE 'available' END::{s}.task_state
    FROM next
    WHERE t.id = next.id
    RETURNING t.queue, t.state
)
SELECT count(pg_notify($3, '{available}' || queue))
FROM (SELECT DISTINCT queue FROM promoted WHERE state = 'available' AND queue = ANY($4)) q`

// $1 task ID.
//
// lockPendingGroupSQL locks the group of task $1 if the task is pending,
// and returns a row if it did. A pending task's row is locked only
// after its group's, the order in which promotion takes them, so the
// two cannot deadlock. The statement reads the task before it waits
// for the group's row, so the task may no longer be pending once it
// holds that row: takeTask checks, where it can start again.
const lockPendingGroupSQL = `
SELECT true FROM {s}.groups g JOIN {s}.tasks t ON t.queue = g.queue AND t.group_key = g.group_key
WHERE t.id = $1 AND t.state = 'pending'
FOR UPDATE OF g`

// $1 queue, $2 group key.
//
// enterGroupSQL counts one more task in group ($1, $2), creating its row if
// it has none, and returns the group's count.
const enterGroupSQL = `
INSERT INTO {s}.groups AS g (queue, group_key, tasks) VALUES ($1, $2, 1)
ON CONFLICT (queue, group_key) DO UPDATE SET tasks = g.tasks + 1, updated_at = now()
RETURNING tasks`

// $1 queue, $2 group key, $3 task ID.
//
// resumeHaltedHeadSQL ends the halt of group ($1, $2) if task $3 halts
// it: it changes a row only then. Callers run it as its own statement
// after locking the task, so that it sees the latest halt: only
// statements that hold the task's lock change whether the task halts
// its group.
const resumeHaltedHeadSQL = `
UPDATE {s}.groups SET halted_task_id = NULL, halted_at = NULL, updated_at = now()
WHERE queue = $1 AND group_key = $2 AND halted_task_id = $3`

// $1 task IDs.
//
// releaseHaltsSQL ends the halts of the groups that tasks $1 halt, and
// returns those tasks. The caller holds the tasks' and the groups'
// locks.
const releaseHaltsSQL = `
WITH halting AS (
    SELECT queue, group_key, halted_task_id AS id FROM {s}.groups WHERE halted_task_id = ANY($1::bigint[])
)
UPDATE {s}.groups g SET halted_task_id = NULL, halted_at = NULL, updated_at = now()
FROM halting h
WHERE g.queue = h.queue AND g.group_key = h.group_key
RETURNING h.id`

// $1 queues, $2 group keys, $3 task counts.
//
// createGroupsSQL creates the rows of groups ($1, $2) with $3 tasks each,
// and returns those it created: a group whose row another transaction
// created meanwhile is left out.
const createGroupsSQL = `
INSERT INTO {s}.groups (queue, group_key, tasks)
SELECT queue, group_key, n FROM unnest($1::text[], $2::text[], $3::integer[]) AS u(queue, group_key, n)
ORDER BY queue COLLATE "C", group_key COLLATE "C"
ON CONFLICT DO NOTHING
RETURNING queue, group_key`
