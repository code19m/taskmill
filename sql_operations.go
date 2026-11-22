package taskmill

// This file holds the statements of what an operator changes: tasks, one or
// many, queues, settings, and schedules, and the read of the queues'
// settings that workers follow. The reasons are in
// https://github.com/code19m/taskmill/blob/main/docs/design/schema-operations.md.

import "strconv"

// timeRangesSQL matches the tasks t created and finished in the ranges whose
// bounds are the parameters from $first on: created since, created before,
// finished since, and finished before, as timeRanges.args gives them. A bound
// is NULL where its range has none. The statements that take it run with
// custom plans, so a range that is set is a condition that an index can
// serve, and one that is not costs nothing.
//
// The conditions on the states that come with a range let the planner use
// the indexes that hold such times: a range of finish times selects only
// dead and canceled tasks, which their index holds, and a range of creation
// times reads the tasks that are not running in their index of creation
// times, and the running ones in theirs.
func timeRangesSQL(first int) string {
	created, finished := param(first), param(first+2)
	return `(` + created(0) + ` IS NULL AND ` + created(1) + ` IS NULL
           OR (t.state <> 'running' OR t.state = 'running') AND ` + bounds(`t.created_at`, created) + `)
      AND (` + finished(0) + ` IS NULL AND ` + finished(1) + ` IS NULL
           OR t.state IN ('dead', 'canceled') AND ` + bounds(`t.finalized_at`, finished) + `)`
}

// historyRangeSQL is timeRangesSQL for the completed tasks t in history,
// which a range of finish times never selects: it takes the range of
// creation times alone, $first and the one after.
func historyRangeSQL(first int) string {
	return bounds(`t.created_at`, param(first))
}

// bounds matches the rows whose column is in the range that starts at p(0)
// and ends before p(1), either of which may be NULL.
func bounds(column string, p func(int) string) string {
	return `(` + p(0) + ` IS NULL OR ` + column + ` >= ` + p(0) + `) AND (` + p(1) + ` IS NULL OR ` + column + ` < ` + p(1) + `)`
}

// param returns the parameters from $first on, by their offset from it, as
// times.
func param(first int) func(int) string {
	return func(i int) string { return "$" + strconv.Itoa(first+i) + "::timestamptz" }
}

// keyedSQL matches the tasks t that must take a dedupe key back when they
// are retried: they are canceled, their cancel freed a key, and stored it
// with them, and the key's window still lasts. Only a canceled task gave its
// key away. A task in another state that stores one was retried by the
// previous release, which leaves the columns as they are: it ran on without
// its key, and so asks for none when it is dead.
const keyedSQL = `(t.state = 'canceled' AND t.freed_key_expires_at > now())`

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
// it, and the end of that task's window. The task is 0
// if it no longer exists: a completed task keeps its key for its
// window, also after its history is gone. It returns no row if no
// task holds the key any more.
const keyHolderSQL = `
SELECT coalesce(h.id, 0), d.expires_at
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
// $7 and $8 queues and group keys of the groups that the action left,
// $9 to $12 the ranges of times.
//
// pickSQL returns a pick statement, whose tasks t meet cond, and whose
// locked tasks meet gates too. The pick statements read the next batch of
// the tasks that a bulk action changes, in ID order after $5 up to $6, the
// action's last task or the end of the batch's window: those of states $4
// that match the filters and the ranges. If fromQueues, the statement
// reads them queue by queue, from the queues that hold tasks of states $4,
// and returns with each task how many of them it found: see rangeWalk. The
// pick statements return each task, whether they hold its lock, whether its
// group had a row, which they then hold, and whether it must take a dedupe
// key back if it is retried. They skip what other transactions hold, and
// the groups ($7, $8) that the action left before: the page on operator
// actions says why.
func pickSQL(cond, gates string, fromQueues bool) string {
	read := `SELECT t.id, t.queue, coalesce(t.group_key, '') AS group_key FROM {s}.tasks t
    WHERE ` + cond + ` AND t.id > $5 AND t.id <= $6
      AND t.state = ANY($4::text[]::{s}.task_state[]) AND ` + taskFilterSQL + `
      AND ` + timeRangesSQL(9)
	candidates, found := read+`
    ORDER BY t.id`, ``
	if fromQueues {
		candidates, found = `SELECT c.id, c.queue, c.group_key, count(*) OVER () AS found FROM `+
			wholeIn(read, queuesOfStates(`$4::text[]`))+`
    ORDER BY c.id`, `, c.found`
	}
	return `
WITH candidates AS (
    ` + candidates + `
    LIMIT {batch}
), picked AS (
    SELECT t.id, t.state::text AS state,
           EXISTS (SELECT FROM {s}.groups g WHERE g.queue = t.queue AND g.group_key = t.group_key) AS in_group,
           coalesce(` + keyedSQL + `, false) AS keyed
    FROM {s}.tasks t
    WHERE t.id = ANY(ARRAY(SELECT id FROM candidates)) AND ` + cond + `
      AND t.state = ANY($4::text[]::{s}.task_state[]) AND ` + timeRangesSQL(9) + `
      AND NOT EXISTS (SELECT FROM unnest($7::text[], $8::text[]) AS u(queue, group_key)
                      WHERE u.queue = t.queue AND u.group_key = t.group_key)
      AND ` + groupFreeSQL + gates + `
    FOR UPDATE OF t SKIP LOCKED
)
SELECT c.id, c.queue, c.group_key, p.id IS NOT NULL, coalesce(p.state, ''), coalesce(p.in_group, false),
       coalesce(p.keyed, false)` + found + `
FROM candidates c LEFT JOIN picked p ON p.id = c.id
ORDER BY c.id`
}

// The tasks t that each action picks: a retry the dead and canceled tasks, a
// cancel the unfinished tasks except running tasks already asked to stop,
// and a delete all but the running ones.
const (
	retryPickSQL  = `t.state IN ('dead', 'canceled')`
	cancelPickSQL = `t.state IN ('scheduled', 'pending', 'available', 'running', 'retryable')
      AND t.cancel_requested_at IS NULL`
	deletePickSQL = `t.state <> 'running'`
)

// The pick statements of each action, in their two forms.
var (
	pickRetrySQL        = pickSQL(retryPickSQL, ``, false)
	pickRetryQueuesSQL  = pickSQL(retryPickSQL, ``, true)
	pickCancelSQL       = pickSQL(cancelPickSQL, ` AND `+keyFreeSQL, false)
	pickCancelQueuesSQL = pickSQL(cancelPickSQL, ` AND `+keyFreeSQL, true)
	pickDeleteSQL       = pickSQL(deletePickSQL, ` AND `+keyFreeSQL, false)
	pickDeleteQueuesSQL = pickSQL(deletePickSQL, ` AND `+keyFreeSQL, true)
)

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

// $1 queues, $2 kinds, $3 group keys, $4 after task ID, $5 up to task ID,
// $6 created since, $7 created before.
//
// pickCompletedSQL returns the pick statement for the completed tasks in
// history, after $4 up to $5, with the time each was finalized. If
// fromQueues, it reads them queue by queue, from the queues that history
// holds, in the partitions that the range of creation times can reach, and
// returns with each task how many it found.
//
// History has a partition per day of finalized_at. A search by the IDs
// alone would read every partition's index with all of them, which
// PostgreSQL 15 and 16 do one ID at a time. So the batch locks its tasks
// by the range of IDs and of finalized_at that the candidates span: in one
// snapshot, that range with the filters holds exactly the candidates, and
// picked reads only the partitions from their first day to their last.
func pickCompletedSQL(fromQueues bool) string {
	read := `SELECT t.id, t.finalized_at FROM {s}.task_history t
    WHERE t.id > $4 AND t.id <= $5 AND ` + taskFilterSQL + ` AND ` + historyRangeSQL(6)
	candidates, found := read+`
    ORDER BY t.id`, ``
	if fromQueues {
		since := `$6::timestamptz`
		candidates, found = `SELECT c.id, c.finalized_at, count(*) OVER () AS found FROM `+
			wholeIn(read+` AND `+finishedSinceSQL(`t.finalized_at`, since), historyQueues(since))+`
    ORDER BY c.id`, `, c.found`
	}
	return `
WITH candidates AS (
    ` + candidates + `
    LIMIT {batch}
), picked AS (
    SELECT t.id FROM {s}.task_history t
    WHERE t.id BETWEEN (SELECT min(id) FROM candidates) AND (SELECT max(id) FROM candidates)
      AND t.finalized_at BETWEEN (SELECT min(finalized_at) FROM candidates) AND (SELECT max(finalized_at) FROM candidates)
      AND ` + taskFilterSQL + ` AND ` + historyRangeSQL(6) + ` AND ` + keyFreeSQL + `
    FOR UPDATE SKIP LOCKED
)
SELECT c.id, c.finalized_at, p.id IS NOT NULL` + found + ` FROM candidates c LEFT JOIN picked p ON p.id = c.id ORDER BY c.id`
}

// $1 task IDs, $2 and $3 the first and the last time that they were
// finalized.
//
// deleteCompletedSQL deletes the picked completed tasks $1, and releases
// their dedupe keys. The times keep the delete to the history partitions
// from their first day to their last, as in pickCompleted.
const deleteCompletedSQL = `
WITH released AS (
    DELETE FROM {s}.dedupe_keys WHERE task_id = ANY($1::bigint[])
)
DELETE FROM {s}.task_history WHERE id = ANY($1::bigint[]) AND finalized_at BETWEEN $2 AND $3`

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
// as readQueue read them, and tells the workers on channel $4 with
// payload $5. It returns 1 if it wrote, 0 if the settings changed
// meanwhile. Only a caller that read no limit may create the row: one
// that read a limit finds no row if a concurrent update deleted it,
// and must read again.
const swapQueueSQL = `
WITH inserted AS (
    INSERT INTO {s}.queues AS q (name, max_running) SELECT $1::text, $3::integer WHERE $2::integer IS NULL
    ON CONFLICT (name) DO UPDATE SET max_running = $3, updated_at = now()
    WHERE q.max_running IS NULL
    RETURNING 1
), updated AS (
    UPDATE {s}.queues SET max_running = $3, updated_at = now()
    WHERE name = $1 AND max_running = $2
    RETURNING 1
)
SELECT count(pg_notify($4, $5)) FROM (TABLE inserted UNION ALL TABLE updated) swapped`

// $1 queue, $2 paused.
//
// pauseQueueSQL pauses queue $1, or resumes it, creating its row if it has
// none. A paused queue keeps the time it was first paused.
const pauseQueueSQL = `
INSERT INTO {s}.queues AS q (name, paused_at) VALUES ($1, CASE WHEN $2 THEN now() END)
ON CONFLICT (name) DO UPDATE
SET paused_at = CASE WHEN $2 THEN coalesce(q.paused_at, now()) END, updated_at = now()`

// $1 queue.
//
// deleteEmptyQueueSQL deletes queue $1's row if it holds no settings:
// the queue is not paused, and has no MaxRunning limit. A queue with a
// row is listed for good, so one that a pause or a limit named by
// mistake would stay. It runs in the transaction that changed the row
// and holds it. A column that a later release adds holds a setting
// that this one does not know, which a delete would lose: the row is
// deleted only while every such column is NULL.
const deleteEmptyQueueSQL = `
DELETE FROM {s}.queues q
WHERE name = $1 AND paused_at IS NULL AND max_running IS NULL
  AND jsonb_strip_nulls(to_jsonb(q) - 'name' - 'paused_at' - 'max_running' - 'created_at' - 'updated_at') = '{}'::jsonb`

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
