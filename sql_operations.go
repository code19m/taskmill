package taskmill

// This file holds the statements of what an operator changes: tasks, one or
// many, queues, settings, and schedules, and the read of the queues'
// settings that workers follow. The reasons are in
// https://github.com/code19m/taskmill/blob/main/docs/design/schema-operations.md.

// keyedSQL matches the tasks t that must take a dedupe key back when they
// are retried: they are canceled, their cancel freed a key, and stored it
// with them, and the key's window still lasts. Only a canceled task gave its
// key away. A task in another state that stores one was retried by the
// previous release, which leaves the columns as they are: it ran on without
// its key, and so asks for none when it is dead. A stored key that is not
// one, which only a write by hand leaves, counts as none: the table of the
// keys would refuse it, and fail the retry.
const keyedSQL = `(t.state = 'canceled' AND t.freed_key_expires_at > now() AND octet_length(t.freed_key) = 32)`

// keyFreeSQL matches the tasks t that hold no dedupe key, or whose key's row it
// locks. It skips key rows that other transactions hold, such as an enqueue
// that tried to claim the key in an open transaction, so a statement never
// waits for them.
const keyFreeSQL = `(NOT EXISTS (SELECT FROM {s}.dedupe_keys d WHERE d.task_id = t.id)
       OR EXISTS (SELECT FROM {s}.dedupe_keys d WHERE d.task_id = t.id FOR UPDATE SKIP LOCKED))`

// $1 task ID, $2 only if pending.
//
// lockTaskSQL locks a task's row and returns its state; if $2, only if
// the task is pending, and without waiting for the row of a task that
// is not. Statements that change the task run after it, as statements
// of their own, so that they read the row as it is once locked.
const lockTaskSQL = `
SELECT state::text, queue, coalesce(group_key, '')
FROM {s}.tasks WHERE id = $1 AND (NOT $2 OR state = 'pending') FOR UPDATE`

// $1 task IDs.
//
// takeKeysSQL gives the tasks $1, which a retry holds, the dedupe keys
// that their cancels freed, for the rest of each key's window. It
// claims a key as an enqueue does: only if no task holds it, or its
// holder's window has ended. A task whose key's window has ended
// asks for no key. It returns the tasks that did not get their key:
// another task holds it, which may be an older task of $1.
//
// Like an enqueue, it waits for a transaction that claims or holds
// the same key, so a retry runs it before it takes a FIFO group's
// row, the order of an enqueue.
const takeKeysSQL = `
WITH wanted AS (
    SELECT t.id, t.freed_key AS key, t.freed_key_expires_at AS expires_at
    FROM {s}.tasks t
    WHERE t.id = ANY($1::bigint[]) AND ` + keyedSQL + `
), taken AS (
    INSERT INTO {s}.dedupe_keys AS d (key, task_id, expires_at)
    SELECT DISTINCT ON (key) key, id, expires_at FROM wanted ORDER BY key, id
    ON CONFLICT (key) DO UPDATE SET task_id = EXCLUDED.task_id, expires_at = EXCLUDED.expires_at
        WHERE d.expires_at <= now()
    RETURNING d.task_id
)
SELECT id FROM wanted WHERE id NOT IN (SELECT task_id FROM taken)`

// $1 task ID.
//
// keyHolderSQL reads who holds the dedupe key that task $1 stores, for
// the error of a retry that takeKeys refused: the task that holds
// it, and the end of that task's window, if it ends. The task is 0
// if it no longer exists: a completed task keeps its key for its
// window, also after its history is gone. It returns no row if no
// task holds the key any more.
const keyHolderSQL = `
SELECT coalesce(h.id, 0), CASE WHEN isfinite(d.expires_at) THEN d.expires_at END
FROM {s}.tasks t
JOIN {s}.dedupe_keys d ON d.key = t.freed_key AND d.expires_at > now()
LEFT JOIN LATERAL (
    SELECT id FROM {s}.tasks WHERE id = d.task_id
    UNION ALL
    SELECT id FROM {s}.task_history WHERE id = d.task_id
    LIMIT 1
) h ON true
WHERE t.id = $1`

// $1 task IDs.
//
// releaseKeysSQL frees the dedupe keys that tasks $1 hold, so that the
// keys can be enqueued again.
const releaseKeysSQL = `DELETE FROM {s}.dedupe_keys WHERE task_id = ANY($1::bigint[])`

// $1 task ID, $2 only if pending.
//
// deleteTaskSQL deletes task $1 unless it is running, and returns the
// state it was in; if $2, only if the task is pending, as lockTask
// does.
const deleteTaskSQL = `
DELETE FROM {s}.tasks WHERE id = $1 AND state <> 'running' AND (NOT $2 OR state = 'pending')
RETURNING state::text, queue, coalesce(group_key, '')`

// $1 task ID.
//
// deleteHistorySQL deletes completed task $1 from history.
const deleteHistorySQL = `DELETE FROM {s}.task_history WHERE id = $1`

// $1 task ID.
//
// taskExistsSQL returns the state of task $1, unfinished or in history, or
// no row if it does not exist.
const taskExistsSQL = `
SELECT state::text FROM {s}.tasks WHERE id = $1
UNION ALL
SELECT 'completed' FROM {s}.task_history WHERE id = $1
LIMIT 1`

// lastTaskSQL returns the ID of the newest task, unfinished or in history.
const lastTaskSQL = `
SELECT greatest((SELECT coalesce(max(id), 0) FROM {s}.tasks), (SELECT coalesce(max(id), 0) FROM {s}.task_history))`

// $1 after task ID, $2 up to task ID, $3 read the tasks, $4 read the history.
//
// windowEndSQL returns where the window of a bulk action's next batches
// ends: the ID of the {window}th task after $1, in the tasks table
// if $3 and in the history if $4, whichever comes first, and at most
// $2. With windowSettings, it reads that many index entries of each
// table, in ID order, and no row beyond them.
const windowEndSQL = `
SELECT least($2::bigint,
    (SELECT id FROM {s}.tasks WHERE $3 AND id > $1 AND id <= $2 ORDER BY id OFFSET {window} - 1 LIMIT 1),
    (SELECT id FROM {s}.task_history WHERE $4 AND id > $1 AND id <= $2 ORDER BY id OFFSET {window} - 1 LIMIT 1))`

// $1 queues, $2 kinds, $3 group keys, $4 states, $5 after task ID, $6 up to task ID,
// $7 and $8 queues and group keys of the groups that the action left.
//
// pickSQL returns a pick statement, whose tasks t meet cond, and whose
// locked tasks meet gates too. The pick statements read the next batch of
// the tasks that a bulk action changes, in ID order after $5 up to $6, the
// action's last task or the end of the batch's window: those of states $4
// that match the filters. Each batch starts where the last one ended, so a
// bulk action reads each task once, and the oldest tasks of a FIFO group
// come first. They return each of them, and whether they hold its lock: they
// skip the tasks, the FIFO groups, and the dedupe keys that other
// transactions hold, the groups ($7, $8) that the action left before, and
// tasks that changed meanwhile. They report whether each task's group had a
// row, which they then hold. A cancel skips running tasks already asked to
// stop. They also report the tasks that must take a dedupe key back if they
// are retried.
func pickSQL(cond, gates string) string {
	return `
WITH candidates AS (
    SELECT t.id, t.queue, coalesce(t.group_key, '') AS group_key FROM {s}.tasks t
    WHERE ` + cond + ` AND t.id > $5 AND t.id <= $6
      AND t.state = ANY($4::text[]::{s}.task_state[]) AND ` + taskFilterSQL + `
    ORDER BY t.id
    LIMIT {batch}
), picked AS (
    SELECT t.id, t.state::text AS state,
           EXISTS (SELECT FROM {s}.groups g WHERE g.queue = t.queue AND g.group_key = t.group_key) AS in_group,
           coalesce(` + keyedSQL + `, false) AS keyed
    FROM {s}.tasks t
    WHERE t.id = ANY(ARRAY(SELECT id FROM candidates)) AND ` + cond + `
      AND t.state = ANY($4::text[]::{s}.task_state[])
      AND NOT EXISTS (SELECT FROM unnest($7::text[], $8::text[]) AS u(queue, group_key)
                      WHERE u.queue = t.queue AND u.group_key = t.group_key)
      AND ` + groupFreeSQL + gates + `
    FOR UPDATE OF t SKIP LOCKED
)
SELECT c.id, c.queue, c.group_key, p.id IS NOT NULL, coalesce(p.state, ''), coalesce(p.in_group, false),
       coalesce(p.keyed, false)
FROM candidates c LEFT JOIN picked p ON p.id = c.id
ORDER BY c.id`
}

// pickRetrySQL picks the dead and canceled tasks that a retry changes.
var pickRetrySQL = pickSQL(`t.state IN ('dead', 'canceled')`, ``)

// pickCancelSQL picks the unfinished tasks that a cancel changes, except
// running tasks already asked to stop.
var pickCancelSQL = pickSQL(`t.state IN ('scheduled', 'pending', 'available', 'running', 'retryable')
      AND t.cancel_requested_at IS NULL`, ` AND `+keyFreeSQL)

// pickDeleteSQL picks the tasks that a delete changes: all but the running
// ones.
var pickDeleteSQL = pickSQL(`t.state <> 'running'`, ` AND `+keyFreeSQL)

// $1 task IDs, $2 task IDs that become pending, $3 channel, $4 queues to notify.
//
// retryTasksSQL makes the tasks $1 run again, one task or a batch, those
// of $2 as pending, and notifies channel $3 of queues $4. The caller
// holds the tasks' rows, and has seen that they are dead or canceled.
// It forgets the dedupe keys that the tasks' cancels freed: takeKeys
// ran before.
const retryTasksSQL = `
WITH retried AS (
    UPDATE {s}.tasks
    SET state = CASE WHEN id = ANY($2::bigint[]) THEN 'pending' ELSE 'available' END::{s}.task_state,
        attempt = 0, run_at = now(), finalized_at = NULL, cancel_requested_at = NULL, errors = ` + errorsUnnotedSQL + `,
        freed_key = NULL, freed_key_expires_at = NULL
    WHERE id = ANY($1::bigint[])
), notified AS (
    SELECT count(pg_notify($3, '{available}' || q)) AS n FROM unnest($4::text[]) AS q
)
SELECT n FROM notified`

// $1 task IDs, $2 channel.
//
// cancelTasksSQL cancels the tasks $1, whose rows the caller holds, and
// frees their dedupe keys, so that the keys can be enqueued again. It
// stores each freed key with its task, and the end of the key's
// window, so that a retry can take the key back: see takeKeys. It
// flags the running tasks, and asks their workers on channel $2 to
// stop them. A task that holds no key keeps what an earlier cancel
// stored: a running task can be flagged twice.
const cancelTasksSQL = `
WITH released AS (
    DELETE FROM {s}.dedupe_keys d WHERE d.task_id = ANY($1::bigint[]) RETURNING d.task_id, d.key, d.expires_at
), canceled AS (
    UPDATE {s}.tasks t
    SET state = CASE WHEN t.state = 'running' THEN t.state ELSE 'canceled' END,
        finalized_at = CASE WHEN t.state = 'running' THEN NULL ELSE now() END,
        cancel_requested_at = now(),
        freed_key = coalesce(r.key, t.freed_key),
        freed_key_expires_at = coalesce(r.expires_at, t.freed_key_expires_at)
    FROM unnest($1::bigint[]) AS u(id) LEFT JOIN released r ON r.task_id = u.id
    WHERE t.id = u.id
    RETURNING t.id, t.state
)
SELECT count(pg_notify($2, '{cancel}' || id)) FROM canceled WHERE state = 'running'`

// $1 task IDs.
//
// deleteTasksSQL deletes the picked tasks $1 and releases their dedupe
// keys.
const deleteTasksSQL = `
WITH released AS (
    DELETE FROM {s}.dedupe_keys WHERE task_id = ANY($1::bigint[])
)
DELETE FROM {s}.tasks WHERE id = ANY($1::bigint[])`

// $1 queues, $2 kinds, $3 group keys, $4 after task ID, $5 up to task ID.
//
// pickCompletedSQL is a pick statement for the completed tasks in history,
// after $4 up to $5.
const pickCompletedSQL = `
WITH candidates AS (
    SELECT t.id FROM {s}.task_history t
    WHERE t.id > $4 AND t.id <= $5 AND ` + taskFilterSQL + `
    ORDER BY t.id
    LIMIT {batch}
), picked AS (
    SELECT t.id FROM {s}.task_history t
    WHERE t.id = ANY(ARRAY(SELECT id FROM candidates)) AND ` + keyFreeSQL + `
    FOR UPDATE SKIP LOCKED
)
SELECT c.id, p.id IS NOT NULL FROM candidates c LEFT JOIN picked p ON p.id = c.id ORDER BY c.id`

// $1 task IDs.
//
// deleteCompletedSQL deletes the picked completed tasks $1, and releases
// their dedupe keys.
const deleteCompletedSQL = `
WITH released AS (
    DELETE FROM {s}.dedupe_keys WHERE task_id = ANY($1::bigint[])
)
DELETE FROM {s}.task_history WHERE id = ANY($1::bigint[])`

// $1 queues.
//
// queueSettingsSQL reads the settings of the queues $1 that have a row, for
// the workers that read them.
const queueSettingsSQL = `SELECT name, paused_at IS NOT NULL, coalesce(max_running, 0) FROM {s}.queues WHERE name = ANY($1)`

// $1 queue.
//
// readQueueSQL reads queue $1's settings, which are NULL when it has no
// row.
const readQueueSQL = `SELECT (SELECT max_running FROM {s}.queues WHERE name = $1)`

// $1 queue, $2 old MaxRunning, $3 new MaxRunning, $4 channel, $5 payload.
//
// swapQueueSQL writes queue $1's settings $3 only if they are still $2,
// as readQueue read them, creating its row if it has none, and tells
// the workers on channel $4 with payload $5. It returns 1 if it
// wrote, 0 if the settings changed meanwhile.
const swapQueueSQL = `
WITH swapped AS (
    INSERT INTO {s}.queues AS q (name, max_running) VALUES ($1, $3)
    ON CONFLICT (name) DO UPDATE SET max_running = $3, updated_at = now()
    WHERE q.max_running IS NOT DISTINCT FROM $2
    RETURNING 1
)
SELECT count(pg_notify($4, $5)) FROM swapped`

// $1 queue, $2 paused.
//
// pauseQueueSQL pauses queue $1, or resumes it, creating its row if it has
// none. A paused queue keeps the time it was first paused.
const pauseQueueSQL = `
INSERT INTO {s}.queues AS q (name, paused_at) VALUES ($1, CASE WHEN $2 THEN now() END)
ON CONFLICT (name) DO UPDATE
SET paused_at = CASE WHEN $2 THEN coalesce(q.paused_at, now()) END, updated_at = now()`

// readSettingsSQL reads the retention periods in microseconds, which
// swapSettings compares and writes.
const readSettingsSQL = `
SELECT (extract(epoch FROM history_retention) * 1000000)::bigint, (extract(epoch FROM dead_retention) * 1000000)::bigint
FROM {s}.settings`

// $1 old history retention, $2 old dead retention, $3 new history retention,
// $4 new dead retention, all in microseconds.
//
// swapSettingsSQL writes the retention periods $3 and $4 only if they
// are still $1 and $2, as readSettings read them. It changes no row
// if they changed meanwhile.
const swapSettingsSQL = `
UPDATE {s}.settings
SET history_retention = $3 * interval '1 microsecond', dead_retention = $4 * interval '1 microsecond',
    updated_at = now()
WHERE (extract(epoch FROM history_retention) * 1000000)::bigint = $1
  AND (extract(epoch FROM dead_retention) * 1000000)::bigint = $2`

// $1 schedule.
//
// runScheduleSQL makes schedule $1 due now, unless it is paused or due
// already, and reports whether it exists and is paused. The time of
// the request takes the place of the next regular run's time: the
// maintainer tells the two apart, runs the request as it runs any due
// schedule, and computes the regular run again from the expression. A
// run that is due already stays, since its time cannot be computed
// again. The statement decides on the row it locked, not on the one it
// first read, which the maintainer may be moving on.
const runScheduleSQL = `
WITH s AS (
    SELECT name, paused_at IS NOT NULL AS paused, next_run_at <= now() AS due
    FROM {s}.schedules WHERE name = $1 FOR UPDATE
), requested AS (
    UPDATE {s}.schedules t SET next_run_at = now(), updated_at = now()
    FROM s WHERE t.name = s.name AND NOT s.paused AND NOT s.due
)
SELECT paused FROM s`

// $1 schedule, $2 paused.
//
// pauseScheduleSQL pauses schedule $1, or resumes it. A paused schedule
// keeps the time it was first paused.
const pauseScheduleSQL = `
UPDATE {s}.schedules SET paused_at = CASE WHEN $2 THEN coalesce(paused_at, now()) END, updated_at = now()
WHERE name = $1`
