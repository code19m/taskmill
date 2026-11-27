package taskmill

import (
	"math"
	"strconv"
	"strings"
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
// locked task cannot block the renewal of all the others. It never
// moves a lease's end back: of two renewals that overlap, as a
// check-in's and a try again after one failed, the one that started
// first can commit last. It returns each renewed task with true, and
// each task the attempts still own with false, both with whether its
// cancellation was requested; a task it does not return lost its
// lease.
const renewLeasesSQL = `
WITH attempts AS (
    SELECT DISTINCT id, lease_token FROM unnest($1::bigint[], $2::integer[]) AS u(id, lease_token)
), locked AS (
    SELECT t.id FROM attempts JOIN {s}.tasks t USING (id, lease_token)
    WHERE ` + leasedSQL + `
    FOR UPDATE OF t SKIP LOCKED
), renewed AS (
    UPDATE {s}.tasks t
    SET lease_expires_at = greatest(t.lease_expires_at, now() + $3 * interval '1 microsecond')
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
// deletes. It returns each task it finished, with true and the dedupe
// key that its cancel freed, if any: see regainKeysSQL.
//
// It waits for a row that another transaction holds, so it first locks
// the tasks in the order of their IDs, and then deletes them: the page on
// key statements says why.
const completeSQL = `
WITH held AS (
    SELECT t.id FROM unnest($1::bigint[], $2::integer[]) AS u(id, lease_token)
    JOIN {s}.tasks t USING (id, lease_token)
    WHERE ` + leasedSQL + `
    ORDER BY t.id
    FOR UPDATE OF t
), ` + completeHeldSQL + `
SELECT id, queue, coalesce(group_key, ''), true, freed_key, freed_key_expires_at FROM done`

// $1 task IDs, $2 lease tokens, $3 task IDs to delete without history.
//
// completeSkippingSQL is completeSQL for tasks outside FIFO groups,
// without the wait: it skips the rows that other transactions hold,
// such as one that cancels a task, and returns each such task with
// false, after the tasks it finished. A batch of completions thus
// never waits for one row, and the worker tries the skipped ones
// again later. Only the attempts that it skipped are looked up again.
var completeSkippingSQL = strings.ReplaceAll(completeSkippingTemplate, "{group}", "")

// $1 task IDs, $2 lease tokens, $3 task IDs to delete without history.
//
// completeGroupedSQL is completeSkippingSQL for tasks in FIFO groups,
// after lockGroupsSQL in the same transaction: it completes a task only if
// the transaction holds its group's row, or its group has no row, and
// returns each other task with false too. So a completion never takes a
// task's row while another transaction holds its group's.
var completeGroupedSQL = strings.ReplaceAll(completeSkippingTemplate, "{group}", " AND "+groupFreeSQL)

// completeSkippingTemplate is completeSkippingSQL, with {group} for what
// else a task must match to complete. NOT IN reads held into a hash table:
// NOT EXISTS can plan as a loop over held for each attempt, which took
// twice as long for a batch of 500.
const completeSkippingTemplate = `
WITH attempts AS (
    SELECT DISTINCT id, lease_token FROM unnest($1::bigint[], $2::integer[]) AS u(id, lease_token)
), held AS (
    SELECT t.id FROM attempts JOIN {s}.tasks t USING (id, lease_token)
    WHERE ` + leasedSQL + `{group}
    FOR UPDATE OF t SKIP LOCKED
), ` + completeHeldSQL + `
SELECT id, queue, coalesce(group_key, ''), true, freed_key, freed_key_expires_at FROM done
UNION ALL
SELECT t.id, '', '', false, NULL, NULL FROM attempts a JOIN {s}.tasks t USING (id, lease_token)
WHERE a.id NOT IN (SELECT id FROM held) AND ` + leasedSQL

// completeHeldSQL is the part of completeSQL and completeSkippingSQL
// that finishes the tasks held: it deletes them, and copies those that
// keep their history, all but tasks $3, into history.
const completeHeldSQL = `done AS (
    DELETE FROM {s}.tasks
    WHERE id = ANY(ARRAY(SELECT id FROM held))
    RETURNING id, created_at, run_at, attempted_at, priority, attempt, max_attempts,
              queue, kind, group_key, attempted_by, payload, metadata, errors, freed_key, freed_key_expires_at
), kept AS (
    INSERT INTO {s}.task_history (id, finalized_at, created_at, run_at, attempted_at, priority, attempt,
                                  max_attempts, queue, kind, group_key, attempted_by, payload, metadata, errors)
    SELECT id, now(), created_at, run_at, attempted_at, priority, attempt,
           max_attempts, queue, kind, group_key, attempted_by, payload, metadata, errors
    FROM done
    WHERE done.id <> ALL($3::bigint[])
)`

// $1 dedupe keys, $2 task IDs, $3 ends of the keys' windows.
//
// regainKeysSQL gives the completed tasks $2 the dedupe keys $1 that their
// cancels freed while they ran: they did their work all the same. It
// claims a key as takeKeys does for a retry: only if no task holds it, or
// its holder's window has ended, and only for the rest of its window. A
// task that took the key during the run keeps it. Like an enqueue, it
// waits for a transaction that claims or holds the same key, so a worker
// runs it once the completion has committed: see Worker.regainKeys.
//
// Like an enqueue, it skips a key that it sees held, at READ COMMITTED:
// ON CONFLICT DO UPDATE would lock the holder's row although it claims
// nothing. CompleteTx claims before it takes its group's row, and that
// lock would make it wait in the group for a Cancel or a Delete of the
// pending holder, which takes the group's row, then the key's.
const regainKeysSQL = `
INSERT INTO {s}.dedupe_keys AS d (key, task_id, expires_at)
SELECT DISTINCT ON (key) key, id, expires_at
FROM unnest($1::bytea[], $2::bigint[], $3::timestamptz[]) AS u(key, id, expires_at)
WHERE expires_at > now()
  AND (current_setting('transaction_isolation') IN ('repeatable read', 'serializable')
       OR NOT EXISTS (SELECT FROM {s}.dedupe_keys k WHERE k.key = u.key AND k.expires_at > now()))
ORDER BY key, id
ON CONFLICT (key) DO UPDATE SET task_id = EXCLUDED.task_id, expires_at = EXCLUDED.expires_at
    WHERE d.expires_at <= now()`

// $1 task IDs, $2 lease tokens, $3 outcomes, $4 delays in microseconds,
// $5 errors or notes, each NULL or a JSON object, $6 whether the worker
// has a handler for each task.
//
// recordSQL records the outcomes other than completion of the attempts
// ($1, $2) that still own their tasks. Outcome $3 is one of:
//
//   - retryable: run the task again after $4 microseconds, recording
//     error $5;
//   - dead: the task ran out of attempts, or failed for good, recording
//     error $5;
//   - snooze: run the task again after $4 microseconds, and give the
//     attempt back, recording $5 as a note if it is not NULL; the note
//     carries the number of attempts used;
//   - release: give the task back, attempt included: the worker stopped
//     the attempt, at a shutdown, or after it read a cancel request.
//
// A task whose cancellation was requested is canceled instead: the row
// decides, so no statement cancels a running task whose row has no
// request. If $6, the worker has a handler for the task, so a snooze or
// a release ends its wait for one: its errors lose the note of that
// wait. The cast makes $4 a bigint: compared with 0 alone, a delay would
// be an integer, which holds only 35 minutes.
//
// It waits for the rows that other transactions hold, and takes them in
// the order of their IDs. It returns each attempt that it recorded, with
// its task's queue, FIFO group, and new state, and true.
var recordSQL = strings.ReplaceAll(recordOutcomesSQL, "{wait}", "") + `
SELECT id, lease_token, queue, group_key, state, true FROM recorded`

// $1 to $6 as in recordSQL.
//
// recordSkippingSQL is recordSQL without the wait: it skips the rows
// that other transactions hold, and returns the attempt of each such task
// with false, after the attempts that it recorded. So the outcomes of a
// batch never wait for one row, and the worker records the skipped ones
// one by one.
var recordSkippingSQL = strings.ReplaceAll(recordOutcomesSQL, "{wait}", "SKIP LOCKED") + `
SELECT id, lease_token, queue, group_key, state, true FROM recorded
UNION ALL
SELECT t.id, t.lease_token, '', '', '', false FROM outcomes o JOIN {s}.tasks t USING (id, lease_token)
WHERE NOT EXISTS (SELECT FROM locked l WHERE l.id = o.id) AND ` + leasedSQL

// recordOutcomesSQL is the part of recordSQL and recordSkippingSQL that
// locks the rows and records the outcomes. {wait} says how the lock
// treats a row that another transaction holds. The update joins each
// task to the outcome of its attempt by the lease token too: a batch can
// hold the outcome of an attempt that lost the task, besides the outcome
// of the attempt that took it after.
const recordOutcomesSQL = `
WITH outcomes AS (
    SELECT u.id, u.lease_token, u.outcome, u.delay_us, u.note, u.handled
    FROM unnest($1::bigint[], $2::integer[], $3::text[], $4::bigint[], $5::jsonb[], $6::boolean[])
        AS u(id, lease_token, outcome, delay_us, note, handled)
), locked AS (
    SELECT t.id FROM outcomes JOIN {s}.tasks t USING (id, lease_token)
    WHERE ` + leasedSQL + `
    ORDER BY t.id
    FOR UPDATE OF t {wait}
), recorded AS (
    UPDATE {s}.tasks t
    SET state = CASE
            WHEN t.cancel_requested_at IS NOT NULL THEN 'canceled'
            WHEN o.outcome IN ('retryable', 'dead') THEN o.outcome
            WHEN o.outcome = 'snooze' AND o.delay_us > 0 THEN 'scheduled'
            ELSE 'available'
        END::{s}.task_state,
        run_at = CASE WHEN t.cancel_requested_at IS NULL AND o.outcome IN ('retryable', 'snooze')
                      THEN now() + o.delay_us * interval '1 microsecond' ELSE t.run_at END,
        attempt = CASE WHEN t.cancel_requested_at IS NULL AND o.outcome IN ('snooze', 'release')
                       THEN t.attempt - 1 ELSE t.attempt END,
        finalized_at = CASE WHEN t.cancel_requested_at IS NOT NULL OR o.outcome = 'dead' THEN now() END,
        lease_expires_at = NULL,
        errors = CASE
            WHEN o.outcome IN ('retryable', 'dead')
            THEN ` + errorsTrimmedSQL + ` || jsonb_build_array(jsonb_build_object('attempt', t.attempt, 'at', ` + nowRFC3339SQL + `) || o.note)
            WHEN o.note IS NOT NULL
            THEN ` + errorsTrimmedSQL + ` || jsonb_build_array(jsonb_build_object('attempt', t.attempt - 1, 'at', ` + nowRFC3339SQL + `) || o.note)
            WHEN o.handled THEN ` + errorsUnnotedSQL + `
            ELSE t.errors
        END
    FROM outcomes o
    WHERE t.id = o.id AND t.lease_token = o.lease_token AND t.id = ANY(ARRAY(SELECT id FROM locked))
    RETURNING t.id, t.lease_token, t.queue, coalesce(t.group_key, '') AS group_key, t.state::text AS state
)`

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
