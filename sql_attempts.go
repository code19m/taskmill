package taskmill

import (
	"math"
	"strconv"
)

// This file holds the statements that fetch tasks, renew their leases, and
// record how their attempts end. The reasons are in
// https://github.com/code19m/taskmill/blob/main/docs/design/schema-statements.md.

// leasedColumns are the columns that scanLeased reads, in its order.
const leasedColumns = `t.id, t.queue, t.kind, t.priority, t.attempt, t.max_attempts, t.lease_token,
          coalesce(t.group_key, ''), t.metadata, t.payload, t.errors, t.created_at, t.run_at, t.attempted_at`

// $1 queue, $2 limit, $3 lease in microseconds, $4 worker ID.
//
// fetchSQL leases up to $2 available tasks of queue $1 for $3
// microseconds, in priority, run time, and enqueue order. It updates
// the picked tasks through an array of their IDs: a join with the
// picked rows can make a generic plan scan the whole table, as the
// planner cannot know how few rows LIMIT $2 returns.
//
// The lease counts from clock_timestamp(), the moment the task is
// taken, not from now(), the start of the transaction: the fetch of
// a queue with a MaxRunning limit waits for the queue's row first,
// and that wait must not be taken off the lease.
var fetchSQL = fetchUpTo(`$2`)

// $1 queue, $2 limit, $3 lease in microseconds, $4 worker ID.
//
// fetchLimitedSQL is fetchSQL for a queue with a MaxRunning limit, after
// lockQueueSQL in the same transaction. It leases no more tasks than the
// limit leaves room for, and counts the running tasks itself. It runs
// as a statement of its own after the lock, so it sees the tasks of the
// fetches that held the row before.
var fetchLimitedSQL = fetchUpTo(`least($2::bigint, greatest(0,
          coalesce((SELECT max_running FROM {s}.queues WHERE name = $1), $2::bigint)
          - (SELECT count(*) FROM {s}.tasks WHERE queue = $1 AND state = 'running')))`)

// fetchUpTo returns fetchSQL with limit as its LIMIT.
func fetchUpTo(limit string) string {
	return `
WITH picked AS (
    SELECT id FROM {s}.tasks
    WHERE state = 'available' AND queue = $1
    ORDER BY priority DESC, run_at, id
    LIMIT ` + limit + `
    FOR UPDATE SKIP LOCKED
)
UPDATE {s}.tasks t
SET ` + leaseSet(`$3`, `$4`) + `
WHERE t.id = ANY(ARRAY(SELECT id FROM picked))
RETURNING ` + leasedColumns
}

// $1 queue, $2 limit, $3 lease in microseconds, $4 worker ID, $5 kind,
// $6 most candidates to look at.
//
// fetchKindSQL is fetch for tasks of kind $5 only, to fill a batch. It
// looks at no more than the first $6 available tasks of the queue,
// so filling a batch in a queue of many kinds stays cheap. It locks
// the candidates of its kind one by one, in their order, and stops
// at $2 of them: ordered by the columns of the locked rows, they
// would all be read and sorted first.
var fetchKindSQL = `
WITH candidates AS (
    SELECT id, kind, priority, run_at FROM {s}.tasks
    WHERE state = 'available' AND queue = $1
    ORDER BY priority DESC, run_at, id
    LIMIT $6
), picked AS (
    SELECT t.id FROM candidates c
    CROSS JOIN LATERAL (
        SELECT id FROM {s}.tasks WHERE id = c.id AND state = 'available' FOR UPDATE SKIP LOCKED
    ) t
    WHERE c.kind = $5
    ORDER BY c.priority DESC, c.run_at, c.id
    LIMIT $2
)
UPDATE {s}.tasks t
SET ` + leaseSet(`$3`, `$4`) + `
WHERE t.id = ANY(ARRAY(SELECT id FROM picked))
RETURNING ` + leasedColumns

// $1 task ID, $2 lease in microseconds, $3 worker ID.
//
// leaseTaskSQL leases task $1 for $2 microseconds, whatever its run time,
// unless it is pending, running, or finished. It locks the task's row
// before it updates it, so that the lease counts from the end of a
// wait for a row that another transaction holds: an UPDATE alone
// computes the row's new values before it waits.
var leaseTaskSQL = `
WITH picked AS (
    SELECT id FROM {s}.tasks
    WHERE id = $1 AND state IN ('scheduled', 'available', 'retryable')
    FOR UPDATE
)
UPDATE {s}.tasks t
SET ` + leaseSet(`$2`, `$3`) + `
WHERE t.id = (SELECT id FROM picked)
RETURNING ` + leasedColumns

// $1 queue.
//
// atLimitSQL reports whether queue $1 runs as many tasks as its limit
// allows, or more; a queue without a limit never does. It locks
// nothing, so its answer may be out of date right away: a fetch uses it
// only to skip the transaction below, never to lease.
const atLimitSQL = `
SELECT coalesce((
    SELECT (SELECT count(*) FROM {s}.tasks WHERE queue = $1 AND state = 'running') >= q.max_running
    FROM {s}.queues q WHERE q.name = $1), false)`

// $1 queue.
//
// lockQueueSQL serializes fetches from a queue with a MaxRunning limit.
// countRunning and fetchLimited then run as statements of their own, so
// they see the tasks that fetches committed while lockQueue waited.
const lockQueueSQL = `SELECT max_running FROM {s}.queues WHERE name = $1 FOR UPDATE`

// $1 queue.
//
// countRunningSQL counts the queue's running tasks, after lockQueueSQL.
const countRunningSQL = `SELECT count(*) FROM {s}.tasks WHERE queue = $1 AND state = 'running'`

// $1 task IDs, $2 lease tokens, $3 lease in microseconds.
//
// renewLeasesSQL renews the leases of attempts that still own their
// tasks. It skips rows that other transactions lock, so that one
// locked task cannot block the renewal of all the others. It returns
// each renewed task with true, and each task the attempts still own
// with false, both with whether its cancellation was requested; a task
// it does not return lost its lease.
const renewLeasesSQL = `
WITH attempts AS (
    SELECT DISTINCT id, lease_token FROM unnest($1::bigint[], $2::integer[]) AS u(id, lease_token)
), locked AS (
    SELECT t.id FROM attempts JOIN {s}.tasks t USING (id, lease_token)
    WHERE ` + leasedSQL + `
    FOR UPDATE OF t SKIP LOCKED
), renewed AS (
    UPDATE {s}.tasks t
    SET lease_expires_at = now() + $3 * interval '1 microsecond'
    FROM locked
    WHERE t.id = locked.id
    RETURNING t.id, t.cancel_requested_at IS NOT NULL AS cancel_requested
)
SELECT id, true, cancel_requested FROM renewed
UNION ALL
SELECT t.id, false, t.cancel_requested_at IS NOT NULL FROM attempts JOIN {s}.tasks t USING (id, lease_token)
WHERE ` + leasedSQL

// $1 task IDs, $2 lease tokens.
//
// cancelRequestSQL returns, of the tasks $1 that the attempts with the
// lease tokens $2 still own, those whose cancellation was requested.
const cancelRequestSQL = `
WITH attempts AS (
    SELECT DISTINCT id, lease_token FROM unnest($1::bigint[], $2::integer[]) AS u(id, lease_token)
)
SELECT t.id FROM attempts JOIN {s}.tasks t USING (id, lease_token)
WHERE ` + leasedSQL + ` AND t.cancel_requested_at IS NOT NULL`

// $1 task IDs, $2 lease tokens, $3 task IDs to delete without history.
//
// completeSQL finishes running tasks whose attempts still hold their
// leases: it moves them to history, except tasks $3, which it only
// deletes. It returns each task it finished, with true.
//
// It waits for a row that another transaction holds. So it first
// locks the tasks that the attempts own, in the order of their IDs,
// and then deletes the tasks it holds. While it waits for a task, it
// holds only tasks with lower IDs, and cannot deadlock with a
// transaction that takes tasks in that order too, such as one that
// cancels several. A delete alone takes the rows as its plan meets
// them: in the order of the attempts, or in that of the table, where
// a renewed lease moves a row behind newer ones. A read that locks
// sorts first, whatever its plan.
const completeSQL = `
WITH held AS (
    SELECT t.id FROM unnest($1::bigint[], $2::integer[]) AS u(id, lease_token)
    JOIN {s}.tasks t USING (id, lease_token)
    WHERE ` + leasedSQL + `
    ORDER BY t.id
    FOR UPDATE OF t
), ` + completeHeldSQL + `
SELECT id, queue, coalesce(group_key, ''), true FROM done`

// $1 task IDs, $2 lease tokens, $3 task IDs to delete without history.
//
// completeSkippingSQL is completeSQL for tasks outside FIFO groups,
// without the wait: it skips the rows that other transactions hold,
// such as one that cancels a task, and returns each such task with
// false, after the tasks it finished. A batch of completions thus
// never waits for one row, and the worker tries the skipped ones
// again later. Only the attempts that it skipped are looked up again.
const completeSkippingSQL = `
WITH attempts AS (
    SELECT DISTINCT id, lease_token FROM unnest($1::bigint[], $2::integer[]) AS u(id, lease_token)
), held AS (
    SELECT t.id FROM attempts JOIN {s}.tasks t USING (id, lease_token)
    WHERE ` + leasedSQL + `
    FOR UPDATE OF t SKIP LOCKED
), ` + completeHeldSQL + `
SELECT id, queue, coalesce(group_key, ''), true FROM done
UNION ALL
SELECT t.id, '', '', false FROM attempts a JOIN {s}.tasks t USING (id, lease_token)
WHERE NOT EXISTS (SELECT FROM held WHERE held.id = a.id) AND ` + leasedSQL

// completeHeldSQL is the part of completeSQL and completeSkippingSQL
// that finishes the tasks held: it deletes them, and copies those that
// keep their history, all but tasks $3, into history.
const completeHeldSQL = `done AS (
    DELETE FROM {s}.tasks
    WHERE id = ANY(ARRAY(SELECT id FROM held))
    RETURNING id, created_at, run_at, attempted_at, priority, attempt, max_attempts,
              queue, kind, group_key, attempted_by, payload, metadata, errors
), kept AS (
    INSERT INTO {s}.task_history (id, finalized_at, created_at, run_at, attempted_at, priority, attempt,
                                  max_attempts, queue, kind, group_key, attempted_by, payload, metadata, errors)
    SELECT id, now(), created_at, run_at, attempted_at, priority, attempt,
           max_attempts, queue, kind, group_key, attempted_by, payload, metadata, errors
    FROM done
    WHERE done.id <> ALL($3::bigint[])
)`

// $1 task IDs, $2 lease tokens.
//
// lockLeasedSQL locks the rows of the attempts ($1, $2) that still own
// their tasks, skipping rows that other transactions hold. It returns
// each locked task with true, and each owned task with false. Taken
// after lockGroups, it keeps a batch from ever waiting while it holds
// group rows.
const lockLeasedSQL = `
WITH attempts AS (
    SELECT DISTINCT id, lease_token FROM unnest($1::bigint[], $2::integer[]) AS u(id, lease_token)
), locked AS (
    SELECT t.id FROM attempts JOIN {s}.tasks t USING (id, lease_token)
    WHERE ` + leasedSQL + `
    FOR UPDATE OF t SKIP LOCKED
)
SELECT id, true FROM locked
UNION ALL
SELECT t.id, false FROM attempts JOIN {s}.tasks t USING (id, lease_token)
WHERE ` + leasedSQL

// $1 task ID, $2 lease token, $3 retryable or dead, $4 delay in microseconds,
// $5 error.
//
// failSQL records a failed attempt: retryable after a delay of $4
// microseconds, or dead. $5 is the error as a JSON object. A task
// whose cancellation was requested is canceled instead.
const failSQL = `
UPDATE {s}.tasks
SET state = CASE WHEN cancel_requested_at IS NOT NULL THEN 'canceled' ELSE $3::{s}.task_state END,
    run_at = CASE WHEN cancel_requested_at IS NULL AND $3::{s}.task_state = 'retryable'
                  THEN now() + $4 * interval '1 microsecond' ELSE run_at END,
    finalized_at = CASE WHEN cancel_requested_at IS NOT NULL OR $3::{s}.task_state = 'dead' THEN now() END,
    lease_expires_at = NULL,
    errors = ` + errorsTrimmedSQL + ` || jsonb_build_array(jsonb_build_object('attempt', attempt, 'at', ` + nowRFC3339SQL + `) || $5::jsonb)
WHERE id = $1 AND lease_token = $2 AND ` + leasedSQL + `
RETURNING queue, coalesce(group_key, ''), state::text`

// $1 task ID, $2 lease token, $3 delay in microseconds, $4 note or NULL,
// $5 the worker has a handler.
//
// snoozeSQL runs the task again after $3 microseconds and gives the
// attempt back, recording $4 as a note if it is not null; the note
// carries the number of attempts used. If $5 is set, the worker has
// a handler for the task, so the task's wait for one is over: its
// errors lose the note of that wait. A task whose cancellation was
// requested is canceled instead. The cast makes $3 a bigint: compared
// with 0 alone, it would be an integer, which holds only 35 minutes.
const snoozeSQL = `
UPDATE {s}.tasks
SET state = CASE
            WHEN cancel_requested_at IS NOT NULL THEN 'canceled'
            WHEN $3::bigint > 0 THEN 'scheduled'
            ELSE 'available'
        END::{s}.task_state,
    run_at = CASE WHEN cancel_requested_at IS NULL THEN now() + $3 * interval '1 microsecond' ELSE run_at END,
    attempt = CASE WHEN cancel_requested_at IS NULL THEN attempt - 1 ELSE attempt END,
    finalized_at = CASE WHEN cancel_requested_at IS NOT NULL THEN now() END,
    lease_expires_at = NULL,
    errors = CASE WHEN $4::jsonb IS NOT NULL
                  THEN ` + errorsTrimmedSQL + ` || jsonb_build_array(jsonb_build_object('attempt', attempt - 1, 'at', ` + nowRFC3339SQL + `) || $4::jsonb)
                  WHEN $5 THEN ` + errorsUnnotedSQL + `
                  ELSE errors
             END
WHERE id = $1 AND lease_token = $2 AND ` + leasedSQL + `
RETURNING queue, coalesce(group_key, ''), state::text`

// $1 task ID, $2 lease token, $3 the worker has a handler.
//
// releaseSQL ends an attempt that the worker stopped: it cancels the
// task if its cancellation was requested, and otherwise gives it
// back, attempt included. It records a shutdown, and an attempt that
// ended after the worker read a cancel request. So the task's row
// decides, as in fail and snooze: no statement cancels a running
// task whose row has no request. If $3 is set, the worker has a
// handler for the task, and its errors lose the note of a wait for
// one, as in snooze.
const releaseSQL = `
UPDATE {s}.tasks
SET state = CASE WHEN cancel_requested_at IS NULL THEN 'available' ELSE 'canceled' END::{s}.task_state,
    attempt = CASE WHEN cancel_requested_at IS NULL THEN attempt - 1 ELSE attempt END,
    finalized_at = CASE WHEN cancel_requested_at IS NOT NULL THEN now() END,
    lease_expires_at = NULL,
    errors = CASE WHEN $3 THEN ` + errorsUnnotedSQL + ` ELSE errors END
WHERE id = $1 AND lease_token = $2 AND ` + leasedSQL + `
RETURNING queue, coalesce(group_key, ''), state::text`

// leaseSet is what taking a task writes: worker by runs the task, its attempt
// and its lease token count up, and its lease lasts us microseconds from the
// moment it is taken. us and by are parameters, such as "$3".
//
// Every statement that takes a task writes this text. The statements that
// renew a lease, end an attempt, or rescue an expired lease read what it
// wrote, and cannot know which statement took the task.
//
// The attempt stops at the task's maximum, so that a row written by hand
// with no attempt left cannot fail the fetch of every task taken with it.
// The token stops at the largest integer, where the worker makes the task
// dead: a token that cannot grow cannot tell the next attempt from this one.
func leaseSet(us, by string) string {
	return `state = 'running',
    attempt = least(t.attempt + 1, t.max_attempts),
    lease_token = least(t.lease_token, ` + strconv.Itoa(math.MaxInt32-1) + `) + 1,
    lease_expires_at = clock_timestamp() + ` + us + ` * interval '1 microsecond',
    attempted_at = now(),
    attempted_by = ` + by
}
