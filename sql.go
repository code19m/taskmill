package taskmill

import (
	"regexp"
	"strconv"
	"strings"
)

// keptErrors is how many of a task's most recent attempt errors are kept.
const keptErrors = 20

// schemaPattern is what a schema name must match, so that it can be used in
// statements and as a notification channel without quoting.
var schemaPattern = regexp.MustCompile(`^[a-z_][a-z0-9_]{0,62}$`)

// validateSchema checks a schema name and applies the default.
func validateSchema(schema string) (string, error) {
	if schema == "" {
		return defaultSchema, nil
	}
	if !schemaPattern.MatchString(schema) {
		return "", invalidArgument("schema %q must match %s", schema, schemaPattern)
	}
	return schema, nil
}

// statements holds the statements of the task lifecycle and of the
// operations API, with the schema name filled in, so each has fixed text and
// can be prepared once per connection. Statements that build names at run
// time, such as those for history partitions, live with their callers.
type statements struct {
	schemaVersion string

	enqueue      string
	enqueuePlain string
	dedupeLookup string

	complete string

	getTask       string
	lockTask      string
	flagCancel    string
	cancelWaiting string
	retry         string
	releaseKey    string
	deleteTask    string
	deleteHistory string
	taskExists    string
	haltedBy      string

	haltGroup         string
	decrementGroup    string
	deleteEmptyGroup  string
	promoteGroup      string
	lockGroups        string
	lockLeased        string
	recountGroups     string
	deleteEmptyGroups string
	promoteGroups     string
	lockPendingGroup  string
	enterGroup        string
	resumeHaltedHead  string

	lastTask        string
	pickRetry       string
	pickCancel      string
	pickDelete      string
	releaseHalts    string
	createGroups    string
	retryTasks      string
	cancelTasks     string
	deleteTasks     string
	pickCompleted   string
	deleteCompleted string
	countTasks      string

	fetch         string
	fetchKind     string
	leaseTask     string
	lockQueue     string
	countRunning  string
	renewLeases   string
	fail          string
	snooze        string
	release       string
	cancelRunning string
	queueSettings string
	upsertWorker  string
	checkInWorker string
	deleteWorker  string
	electLeader   string
	resignLeader  string
	promoteDue    string
	nextDue       string
	wakeQueues    string
	rescue        string

	pendingDetach   string
	retention       string
	deleteFinished  string
	pruneSchedules  string
	loadSchedules   string
	upsertSchedule  string
	advanceSchedule string
	listSchedules   string
	pauseSchedule   string
	runSchedule     string

	readQueue        string
	swapQueue        string
	pauseQueue       string
	listQueues       string
	queueStats       string
	readSettings     string
	swapSettings     string
	listWorkers      string
	listTasks        string
	listGroups       string
	listQueueGroups  string
	countStats       string
	clearStats       string
	storeStats       string
	storeActivity    string
	addActivity      string
	deleteActivity   string
	activity         string
	deleteDedupeKeys string
	deleteWorkers    string
}

// leasedColumns are the columns that scanLeased reads, in its order.
const leasedColumns = `t.id, t.queue, t.kind, t.priority, t.attempt, t.max_attempts, t.lease_token,
          coalesce(t.group_key, ''), t.metadata, t.payload, t.errors, t.created_at, t.run_at, t.attempted_at`

// taskFilter matches the tasks t of queues $1, kinds $2, and FIFO groups $3;
// an empty filter matches every task.
const taskFilter = `(cardinality($1::text[]) = 0 OR t.queue = ANY($1::text[]))
  AND (cardinality($2::text[]) = 0 OR t.kind = ANY($2::text[]))
  AND (cardinality($3::text[]) = 0 OR t.group_key = ANY($3::text[]))`

// groupFree matches the tasks t that have no FIFO group, whose group's row it
// locks, or whose group has no row. It skips group rows that other
// transactions hold, so a statement never waits for them.
const groupFree = `(t.group_key IS NULL
       OR EXISTS (SELECT FROM {s}.groups g WHERE g.queue = t.queue AND g.group_key = t.group_key
                  FOR UPDATE SKIP LOCKED)
       OR NOT EXISTS (SELECT FROM {s}.groups g WHERE g.queue = t.queue AND g.group_key = t.group_key))`

// keyFree matches the tasks t that hold no dedupe key, or whose key's row it
// locks. It skips key rows that other transactions hold, such as a duplicate
// enqueue in an open transaction, so a statement never waits for them.
const keyFree = `(NOT EXISTS (SELECT FROM {s}.dedupe_keys d WHERE d.task_id = t.id)
       OR EXISTS (SELECT FROM {s}.dedupe_keys d WHERE d.task_id = t.id FOR UPDATE SKIP LOCKED))`

// taskColumns are the columns that scanTaskInfo reads, in its order.
const taskColumns = `id, queue, kind, state::text, priority, attempt, max_attempts, coalesce(group_key, ''),
       metadata, payload, errors, created_at, run_at, attempted_at, finalized_at`

func newStatements(schema string) *statements {
	r := strings.NewReplacer(
		"{s}", schema,
		"{batch}", strconv.Itoa(maintenanceBatch),
		"{cap}", strconv.Itoa(countCap),
		// {available} and {cancel} start the payloads of notifications.
		"{available}", notifyAvailable+"|",
		"{cancel}", notifyCancel+"|",
		"{heartbeat}", notifyHeartbeat+"|",
		"{resigned}", notifyResigned+"|",
		// {trimmed} is a task's errors without the oldest entry once it holds
		// keptErrors, so that appending one keeps at most keptErrors.
		"{trimmed}", "(CASE WHEN jsonb_array_length(errors) >= "+strconv.Itoa(keptErrors)+" THEN errors - 0 ELSE errors END)",
		// {leased} checks that a task is leased. The statements that find
		// tasks by their attempts look each one up by ID, and compare the
		// state as text, so that the planner cannot read tasks_running_idx
		// instead: statistics taken while few tasks ran make that index look
		// small, but until vacuum it keeps an entry for every task that ran.
		"{leased}", "state::text = 'running'",
	)
	q := func(sql string) string { return r.Replace(sql) }
	pick := func(cond, gates string) string {
		return q(`
WITH candidates AS (
    SELECT t.id, t.queue, coalesce(t.group_key, '') AS group_key FROM {s}.tasks t
    WHERE ` + cond + ` AND t.id > $5 AND t.id <= $6
      AND t.state = ANY($4::text[]::{s}.task_state[]) AND ` + taskFilter + `
    ORDER BY t.id
    LIMIT {batch}
), picked AS (
    SELECT t.id, t.state::text AS state,
           EXISTS (SELECT FROM {s}.groups g WHERE g.queue = t.queue AND g.group_key = t.group_key) AS in_group
    FROM {s}.tasks t
    WHERE t.id = ANY(ARRAY(SELECT id FROM candidates)) AND ` + cond + `
      AND t.state = ANY($4::text[]::{s}.task_state[])
      AND NOT EXISTS (SELECT FROM unnest($7::text[], $8::text[]) AS u(queue, group_key)
                      WHERE u.queue = t.queue AND u.group_key = t.group_key)
      AND ` + groupFree + gates + `
    FOR UPDATE OF t SKIP LOCKED
)
SELECT c.id, c.queue, c.group_key, p.id IS NOT NULL, coalesce(p.state, ''), coalesce(p.in_group, false)
FROM candidates c LEFT JOIN picked p ON p.id = c.id
ORDER BY c.id`)
	}

	return &statements{
		schemaVersion: q(`
SELECT coalesce(max(version), 0), current_setting('default_transaction_isolation') FROM {s}.migrations`),

		// enqueue inserts a batch of tasks in one round trip. It claims dedupe
		// keys first, counts admitted tasks into their FIFO groups, places each
		// task in its starting state, and notifies the listed queues.
		enqueue: q(`
WITH input AS (
    SELECT queue, kind, priority, run_at, max_attempts, group_key,
           dedupe_key, dedupe_window_us, payload, metadata, ord
    FROM unnest($1::text[], $2::text[], $3::smallint[], $4::timestamptz[], $5::smallint[],
                $6::text[], $7::bytea[], $8::bigint[], $9::jsonb[], $10::jsonb[])
        WITH ORDINALITY AS t(queue, kind, priority, run_at, max_attempts, group_key,
                             dedupe_key, dedupe_window_us, payload, metadata, ord)
), ids AS (
    SELECT input.*, nextval('{s}.tasks_id_seq') AS id
    FROM input
), claimed AS (
    INSERT INTO {s}.dedupe_keys AS d (key, task_id, expires_at)
    SELECT dedupe_key, id, now() + dedupe_window_us * interval '1 microsecond'
    FROM ids
    WHERE dedupe_key IS NOT NULL
    ORDER BY dedupe_key
    ON CONFLICT (key) DO UPDATE SET task_id = EXCLUDED.task_id, expires_at = EXCLUDED.expires_at
        WHERE d.expires_at <= now()
    RETURNING task_id
), admitted AS (
    SELECT ids.*
    FROM ids
    WHERE ids.dedupe_key IS NULL OR ids.id IN (SELECT task_id FROM claimed)
), added AS (
    SELECT queue, group_key, count(*) AS n
    FROM admitted
    WHERE group_key IS NOT NULL
    GROUP BY queue, group_key
), grouped AS (
    INSERT INTO {s}.groups AS g (queue, group_key, tasks)
    SELECT queue, group_key, n FROM added
    ORDER BY queue COLLATE "C", group_key COLLATE "C"
    ON CONFLICT (queue, group_key) DO UPDATE SET tasks = g.tasks + EXCLUDED.tasks, updated_at = now()
    RETURNING queue, group_key, tasks
), placed AS (
    SELECT a.*,
           CASE
               WHEN a.group_key IS NOT NULL AND gr.tasks - ad.n > 0 THEN 'pending'
               WHEN a.group_key IS NOT NULL
                    AND row_number() OVER (PARTITION BY a.queue, a.group_key ORDER BY a.ord) > 1 THEN 'pending'
               WHEN coalesce(a.run_at, now()) > now() THEN 'scheduled'
               ELSE 'available'
           END::{s}.task_state AS state
    FROM admitted a
    LEFT JOIN grouped gr ON gr.queue = a.queue AND gr.group_key = a.group_key
    LEFT JOIN added ad ON ad.queue = a.queue AND ad.group_key = a.group_key
), inserted AS (
    INSERT INTO {s}.tasks (id, queue, kind, state, priority, run_at, group_key, payload, metadata, max_attempts)
    OVERRIDING SYSTEM VALUE
    SELECT id, queue, kind, state, priority, coalesce(run_at, now()), group_key, payload, metadata, max_attempts
    FROM placed
    ORDER BY ord
    RETURNING id
), notified AS (
    SELECT count(pg_notify($11, '{available}' || q)) AS n FROM unnest($12::text[]) AS q
)
SELECT ids.ord, coalesce(i.id, d.task_id, 0), i.id IS NULL
FROM ids
LEFT JOIN inserted i ON i.id = ids.id
LEFT JOIN {s}.dedupe_keys d ON i.id IS NULL AND d.key = ids.dedupe_key
CROSS JOIN notified
ORDER BY ids.ord`),

		// enqueuePlain is enqueue for batches without dedupe keys or FIFO
		// groups, the common case: it skips their bookkeeping. It takes the
		// same parameters and returns the same rows.
		enqueuePlain: q(`
WITH ids AS (
    SELECT queue, kind, priority, run_at, max_attempts, payload, metadata, ord,
           nextval('{s}.tasks_id_seq') AS id
    FROM unnest($1::text[], $2::text[], $3::smallint[], $4::timestamptz[], $5::smallint[],
                $6::text[], $7::bytea[], $8::bigint[], $9::jsonb[], $10::jsonb[])
        WITH ORDINALITY AS t(queue, kind, priority, run_at, max_attempts, group_key,
                             dedupe_key, dedupe_window_us, payload, metadata, ord)
), inserted AS (
    INSERT INTO {s}.tasks (id, queue, kind, state, priority, run_at, payload, metadata, max_attempts)
    OVERRIDING SYSTEM VALUE
    SELECT id, queue, kind,
           CASE WHEN coalesce(run_at, now()) > now() THEN 'scheduled' ELSE 'available' END::{s}.task_state,
           priority, coalesce(run_at, now()), payload, metadata, max_attempts
    FROM ids
    ORDER BY ord
), notified AS (
    SELECT count(pg_notify($11, '{available}' || q)) AS n FROM unnest($12::text[]) AS q
)
SELECT ids.ord, ids.id, false FROM ids CROSS JOIN notified ORDER BY ids.ord`),

		dedupeLookup: q(`SELECT key, task_id FROM {s}.dedupe_keys WHERE key = ANY($1::bytea[])`),

		// complete finishes running tasks whose attempts still hold their
		// leases: it moves them to history, except tasks $3, which it only
		// deletes.
		complete: q(`
WITH done AS (
    DELETE FROM {s}.tasks
    WHERE id = ANY($1::bigint[])
      AND (id, lease_token) IN (SELECT i, t FROM unnest($1::bigint[], $2::integer[]) AS u(i, t))
      AND {leased}
    RETURNING id, created_at, run_at, attempted_at, priority, attempt, max_attempts,
              queue, kind, group_key, attempted_by, payload, metadata, errors
), kept AS (
    INSERT INTO {s}.task_history (id, finalized_at, created_at, run_at, attempted_at, priority, attempt,
                                  max_attempts, queue, kind, group_key, attempted_by, payload, metadata, errors)
    SELECT id, now(), created_at, run_at, attempted_at, priority, attempt,
           max_attempts, queue, kind, group_key, attempted_by, payload, metadata, errors
    FROM done
    WHERE done.id <> ALL($3::bigint[])
)
SELECT id, queue, coalesce(group_key, '') FROM done`),

		getTask: q(`
SELECT ` + taskColumns + ` FROM {s}.tasks WHERE id = $1
UNION ALL
SELECT id, queue, kind, 'completed', priority, attempt, max_attempts, coalesce(group_key, ''),
       metadata, payload, errors, created_at, run_at, attempted_at, finalized_at
FROM {s}.task_history WHERE id = $1
LIMIT 1`),

		// lockTask locks a task's row and returns its state. Statements that
		// change the task run after it, as statements of their own, so that
		// they read the row as it is once locked.
		lockTask: q(`SELECT state::text, queue, coalesce(group_key, '') FROM {s}.tasks WHERE id = $1 FOR UPDATE`),

		// flagCancel asks the worker running task $1 to cancel it.
		flagCancel: q(`UPDATE {s}.tasks SET cancel_requested_at = now() WHERE id = $1`),

		// cancelWaiting cancels task $1, which is not running.
		cancelWaiting: q(`
UPDATE {s}.tasks SET state = 'canceled', finalized_at = now(), cancel_requested_at = now() WHERE id = $1`),

		retry: q(`
UPDATE {s}.tasks
SET state = CASE WHEN $2 THEN 'pending' ELSE 'available' END::{s}.task_state,
    attempt = 0, run_at = now(), finalized_at = NULL, cancel_requested_at = NULL
WHERE id = $1 AND state IN ('dead', 'canceled')
RETURNING queue`),

		// releaseKey frees the dedupe key that task $1 holds, so that the key
		// can be enqueued again.
		releaseKey: q(`DELETE FROM {s}.dedupe_keys WHERE task_id = $1`),

		deleteTask: q(`
DELETE FROM {s}.tasks WHERE id = $1 AND state <> 'running'
RETURNING state::text, queue, coalesce(group_key, '')`),

		deleteHistory: q(`DELETE FROM {s}.task_history WHERE id = $1`),

		taskExists: q(`
SELECT state::text FROM {s}.tasks WHERE id = $1
UNION ALL
SELECT 'completed' FROM {s}.task_history WHERE id = $1
LIMIT 1`),

		// haltedBy reports whether task $3 halts its group. Callers run it as
		// its own statement after locking the task, so that it sees the latest
		// halt: only statements that hold the task's lock change whether the
		// task halts its group.
		haltedBy: q(`
SELECT EXISTS (SELECT 1 FROM {s}.groups WHERE queue = $1 AND group_key = $2 AND halted_task_id = $3)`),

		// haltGroup marks a group halted by its dead head, which stays
		// counted.
		haltGroup: q(`
UPDATE {s}.groups SET halted_task_id = $3, halted_at = now(), updated_at = now()
WHERE queue = $1 AND group_key = $2`),

		// decrementGroup removes one task from a group's count, locking the
		// group's row; if that task was the halting head, the halt ends.
		decrementGroup: q(`
UPDATE {s}.groups
SET tasks = tasks - 1,
    halted_task_id = CASE WHEN halted_task_id = $3 THEN NULL ELSE halted_task_id END,
    halted_at = CASE WHEN halted_task_id = $3 THEN NULL ELSE halted_at END,
    updated_at = now()
WHERE queue = $1 AND group_key = $2
RETURNING tasks, halted_task_id IS NOT NULL`),

		deleteEmptyGroup: q(`DELETE FROM {s}.groups WHERE queue = $1 AND group_key = $2 AND tasks = 0`),

		// promoteGroup makes the oldest pending task of group ($1, $2) its
		// head, notifying channel $3 if $4 and the task is available. It must
		// run as its own statement after decrementGroup, so that it sees tasks
		// enqueued while decrementGroup waited for the group's lock.
		promoteGroup: q(`
WITH next AS (
    SELECT id FROM {s}.tasks
    WHERE queue = $1 AND group_key = $2 AND state = 'pending'
    ORDER BY id
    LIMIT 1
    FOR UPDATE
), promoted AS (
    UPDATE {s}.tasks t
    SET state = CASE WHEN t.run_at > now() THEN 'scheduled' ELSE 'available' END::{s}.task_state
    FROM next
    WHERE t.id = next.id
    RETURNING t.queue, t.state
)
SELECT count(pg_notify($3, '{available}' || queue)) FROM promoted WHERE $4 AND state = 'available'`),

		// lockGroups locks the rows of groups ($1, $2) that no other
		// transaction holds. It returns each locked group with true, and each
		// existing group with false, so a group it does not return has no row.
		// Skipping held rows means a batch never waits for another
		// transaction.
		lockGroups: q(`
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
JOIN wanted w ON g.queue = w.queue AND g.group_key = w.group_key`),

		// lockLeased locks the rows of the attempts ($1, $2) that still own
		// their tasks, skipping rows that other transactions hold. It returns
		// each locked task with true, and each owned task with false. Taken
		// after lockGroups, it keeps a batch from ever waiting while it holds
		// group rows.
		lockLeased: q(`
WITH attempts AS (
    SELECT i, t FROM unnest($1::bigint[], $2::integer[]) AS u(i, t)
), locked AS (
    SELECT id FROM {s}.tasks
    WHERE id = ANY($1::bigint[]) AND (id, lease_token) IN (SELECT i, t FROM attempts) AND {leased}
    FOR UPDATE SKIP LOCKED
)
SELECT id, true FROM locked
UNION ALL
SELECT id, false FROM {s}.tasks
WHERE id = ANY($1::bigint[]) AND (id, lease_token) IN (SELECT i, t FROM attempts) AND {leased}`),

		// recountGroups adds $3, which is negative for tasks that left, to the
		// counts of groups ($1, $2). The caller holds the groups' locks, and
		// has ended the halts of halting heads that left.
		recountGroups: q(`
UPDATE {s}.groups g
SET tasks = g.tasks + u.n, updated_at = now()
FROM unnest($1::text[], $2::text[], $3::integer[]) AS u(queue, group_key, n)
WHERE g.queue = u.queue AND g.group_key = u.group_key
RETURNING g.queue, g.group_key, g.tasks, g.halted_task_id IS NOT NULL`),

		deleteEmptyGroups: q(`
DELETE FROM {s}.groups g USING unnest($1::text[], $2::text[]) AS u(queue, group_key)
WHERE g.queue = u.queue AND g.group_key = u.group_key AND g.tasks = 0`),

		// promoteGroups is promoteGroup for groups ($1, $2), notifying the
		// queues in $4.
		promoteGroups: q(`
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
FROM (SELECT DISTINCT queue FROM promoted WHERE state = 'available' AND queue = ANY($4)) q`),

		// lockPendingGroup locks the group of task $1 if the task is pending.
		// A pending task's row is locked only after its group's, the order
		// in which promotion takes them, so the two cannot deadlock.
		lockPendingGroup: q(`
SELECT FROM {s}.groups g JOIN {s}.tasks t ON t.queue = g.queue AND t.group_key = g.group_key
WHERE t.id = $1 AND t.state = 'pending'
FOR UPDATE OF g`),

		enterGroup: q(`
INSERT INTO {s}.groups AS g (queue, group_key, tasks) VALUES ($1, $2, 1)
ON CONFLICT (queue, group_key) DO UPDATE SET tasks = g.tasks + 1, updated_at = now()
RETURNING tasks`),

		resumeHaltedHead: q(`
UPDATE {s}.groups SET halted_task_id = NULL, halted_at = NULL, updated_at = now()
WHERE queue = $1 AND group_key = $2 AND halted_task_id = $3`),

		// lastTask returns the ID of the newest task, unfinished or in history.
		lastTask: q(`
SELECT greatest((SELECT coalesce(max(id), 0) FROM {s}.tasks), (SELECT coalesce(max(id), 0) FROM {s}.task_history))`),

		// The pick statements read the next batch of the tasks that a bulk
		// action changes, in ID order after $5 up to $6: those of states $4
		// that match the filters. Each batch starts where the last one ended,
		// so a bulk action reads each task once, and the oldest tasks of a
		// FIFO group come first. They return each of them, and whether they
		// hold its lock: they skip the tasks, the FIFO groups, and the dedupe
		// keys that other transactions hold, the groups ($7, $8) that the
		// action left before, and tasks that changed meanwhile. They report
		// whether each task's group had a row, which they then hold. A cancel
		// skips running tasks already asked to stop.
		pickRetry: pick(`t.state IN ('dead', 'canceled')`, ``),
		pickCancel: pick(`t.state IN ('scheduled', 'pending', 'available', 'running', 'retryable')
      AND t.cancel_requested_at IS NULL`, ` AND `+keyFree),
		pickDelete: pick(`t.state <> 'running'`, ` AND `+keyFree),

		// releaseHalts ends the halts of the groups that tasks $1 halt, and
		// returns those tasks. The caller holds the tasks' and the groups'
		// locks.
		releaseHalts: q(`
WITH halting AS (
    SELECT queue, group_key, halted_task_id AS id FROM {s}.groups WHERE halted_task_id = ANY($1::bigint[])
)
UPDATE {s}.groups g SET halted_task_id = NULL, halted_at = NULL, updated_at = now()
FROM halting h
WHERE g.queue = h.queue AND g.group_key = h.group_key
RETURNING h.id`),

		// createGroups creates the rows of groups ($1, $2) with $3 tasks each,
		// and returns those it created: a group whose row another transaction
		// created meanwhile is left out.
		createGroups: q(`
INSERT INTO {s}.groups (queue, group_key, tasks)
SELECT queue, group_key, n FROM unnest($1::text[], $2::text[], $3::integer[]) AS u(queue, group_key, n)
ORDER BY queue COLLATE "C", group_key COLLATE "C"
ON CONFLICT DO NOTHING
RETURNING queue, group_key`),

		// retryTasks retries the picked tasks $1, those of $2 into pending,
		// and notifies channel $3 of queues $4.
		retryTasks: q(`
WITH retried AS (
    UPDATE {s}.tasks
    SET state = CASE WHEN id = ANY($2::bigint[]) THEN 'pending' ELSE 'available' END::{s}.task_state,
        attempt = 0, run_at = now(), finalized_at = NULL, cancel_requested_at = NULL
    WHERE id = ANY($1::bigint[])
), notified AS (
    SELECT count(pg_notify($3, '{available}' || q)) AS n FROM unnest($4::text[]) AS q
)
SELECT n FROM notified`),

		// cancelTasks cancels the picked tasks $1 and releases their dedupe
		// keys. It flags the running ones, and asks their workers on channel
		// $2 to stop them.
		cancelTasks: q(`
WITH released AS (
    DELETE FROM {s}.dedupe_keys WHERE task_id = ANY($1::bigint[])
), canceled AS (
    UPDATE {s}.tasks SET state = 'canceled', finalized_at = now(), cancel_requested_at = now()
    WHERE id = ANY($1::bigint[]) AND state <> 'running'
), flagged AS (
    UPDATE {s}.tasks SET cancel_requested_at = now()
    WHERE id = ANY($1::bigint[]) AND state = 'running'
    RETURNING id
)
SELECT count(pg_notify($2, '{cancel}' || id)) FROM flagged`),

		// deleteTasks deletes the picked tasks $1 and releases their dedupe
		// keys.
		deleteTasks: q(`
WITH released AS (
    DELETE FROM {s}.dedupe_keys WHERE task_id = ANY($1::bigint[])
)
DELETE FROM {s}.tasks WHERE id = ANY($1::bigint[])`),

		// pickCompleted is a pick statement for the completed tasks in history,
		// after $4 up to $5.
		pickCompleted: q(`
WITH candidates AS (
    SELECT t.id FROM {s}.task_history t
    WHERE t.id > $4 AND t.id <= $5 AND ` + taskFilter + `
    ORDER BY t.id
    LIMIT {batch}
), picked AS (
    SELECT t.id FROM {s}.task_history t
    WHERE t.id = ANY(ARRAY(SELECT id FROM candidates)) AND ` + keyFree + `
    FOR UPDATE SKIP LOCKED
)
SELECT c.id, p.id IS NOT NULL FROM candidates c LEFT JOIN picked p ON p.id = c.id ORDER BY c.id`),

		// deleteCompleted deletes the picked completed tasks $1, and releases
		// their dedupe keys.
		deleteCompleted: q(`
WITH released AS (
    DELETE FROM {s}.dedupe_keys WHERE task_id = ANY($1::bigint[])
)
DELETE FROM {s}.task_history WHERE id = ANY($1::bigint[])`),

		// countTasks counts the tasks in each state of $4 that match the
		// filters, up to {cap} per state. Each state has a branch of its own,
		// which reads that state's index.
		countTasks: q(countTasks()),

		// fetch leases up to $2 available tasks of queue $1 for $3
		// microseconds, in priority, run time, and enqueue order. It updates
		// the picked tasks through an array of their IDs: a join with the
		// picked rows can make a generic plan scan the whole table, as the
		// planner cannot know how few rows LIMIT $2 returns.
		fetch: q(`
WITH picked AS (
    SELECT id FROM {s}.tasks
    WHERE state = 'available' AND queue = $1
    ORDER BY priority DESC, run_at, id
    LIMIT $2
    FOR UPDATE SKIP LOCKED
)
UPDATE {s}.tasks t
SET state = 'running',
    attempt = t.attempt + 1,
    lease_token = t.lease_token + 1,
    lease_expires_at = now() + $3 * interval '1 microsecond',
    attempted_at = now(),
    attempted_by = $4
WHERE t.id = ANY(ARRAY(SELECT id FROM picked))
RETURNING ` + leasedColumns),

		// fetchKind is fetch for tasks of kind $5 only, to fill a batch. It
		// looks at no more than the first $6 available tasks of the queue,
		// so filling a batch in a queue of many kinds stays cheap.
		fetchKind: q(`
WITH candidates AS (
    SELECT id, kind FROM {s}.tasks
    WHERE state = 'available' AND queue = $1
    ORDER BY priority DESC, run_at, id
    LIMIT $6
), picked AS (
    SELECT t.id FROM {s}.tasks t JOIN candidates c ON c.id = t.id
    WHERE c.kind = $5 AND t.state = 'available'
    ORDER BY t.priority DESC, t.run_at, t.id
    LIMIT $2
    FOR UPDATE OF t SKIP LOCKED
)
UPDATE {s}.tasks t
SET state = 'running',
    attempt = t.attempt + 1,
    lease_token = t.lease_token + 1,
    lease_expires_at = now() + $3 * interval '1 microsecond',
    attempted_at = now(),
    attempted_by = $4
WHERE t.id = ANY(ARRAY(SELECT id FROM picked))
RETURNING ` + leasedColumns),

		// leaseTask leases task $1 for $2 microseconds, whatever its run time,
		// unless it is pending, running, or finished.
		leaseTask: q(`
UPDATE {s}.tasks t
SET state = 'running',
    attempt = t.attempt + 1,
    lease_token = t.lease_token + 1,
    lease_expires_at = now() + $2 * interval '1 microsecond',
    attempted_at = now(),
    attempted_by = $3
WHERE t.id = $1 AND t.state IN ('scheduled', 'available', 'retryable')
RETURNING ` + leasedColumns),

		// lockQueue serializes fetches from a queue with a concurrency limit.
		// countRunning then runs as its own statement, so it sees the tasks
		// that fetches committed while lockQueue waited.
		lockQueue:    q(`SELECT max_running FROM {s}.queues WHERE name = $1 FOR UPDATE`),
		countRunning: q(`SELECT count(*) FROM {s}.tasks WHERE queue = $1 AND state = 'running'`),

		// renewLeases renews the leases of attempts that still own their
		// tasks. It skips rows that other transactions lock, so that one
		// locked task cannot hold up the renewal of all the others. It returns
		// each renewed task with true, and each task the attempts still own
		// with false, both with whether its cancellation was requested; a task
		// it does not return lost its lease.
		renewLeases: q(`
WITH attempts AS (
    SELECT i, t FROM unnest($1::bigint[], $2::integer[]) AS u(i, t)
), locked AS (
    SELECT id FROM {s}.tasks
    WHERE id = ANY($1::bigint[]) AND (id, lease_token) IN (SELECT i, t FROM attempts) AND {leased}
    FOR UPDATE SKIP LOCKED
), renewed AS (
    UPDATE {s}.tasks t
    SET lease_expires_at = now() + $3 * interval '1 microsecond'
    FROM locked
    WHERE t.id = locked.id
    RETURNING t.id, t.cancel_requested_at IS NOT NULL AS cancel_requested
)
SELECT id, true, cancel_requested FROM renewed
UNION ALL
SELECT id, false, cancel_requested_at IS NOT NULL FROM {s}.tasks
WHERE id = ANY($1::bigint[]) AND (id, lease_token) IN (SELECT i, t FROM attempts) AND {leased}`),

		// fail records a failed attempt: retryable after a delay of $4
		// microseconds, or dead. $5 is the error as a JSON object. A task
		// whose cancellation was requested is canceled instead.
		fail: q(`
UPDATE {s}.tasks
SET state = CASE WHEN cancel_requested_at IS NOT NULL THEN 'canceled' ELSE $3::{s}.task_state END,
    run_at = CASE WHEN cancel_requested_at IS NULL AND $3::{s}.task_state = 'retryable'
                  THEN now() + $4 * interval '1 microsecond' ELSE run_at END,
    finalized_at = CASE WHEN cancel_requested_at IS NOT NULL OR $3::{s}.task_state = 'dead' THEN now() END,
    lease_expires_at = NULL,
    errors = {trimmed} || jsonb_build_array(jsonb_build_object('attempt', attempt, 'at', now()) || $5::jsonb)
WHERE id = $1 AND lease_token = $2 AND {leased}
RETURNING queue, coalesce(group_key, ''), state::text`),

		// snooze runs the task again after $3 microseconds and gives the
		// attempt back, recording $4 as a note if it is not null; the note
		// carries the number of attempts used. A task whose cancellation was
		// requested is canceled instead.
		snooze: q(`
UPDATE {s}.tasks
SET state = CASE
            WHEN cancel_requested_at IS NOT NULL THEN 'canceled'
            WHEN $3 > 0 THEN 'scheduled'
            ELSE 'available'
        END::{s}.task_state,
    run_at = CASE WHEN cancel_requested_at IS NULL THEN now() + $3 * interval '1 microsecond' ELSE run_at END,
    attempt = CASE WHEN cancel_requested_at IS NULL THEN attempt - 1 ELSE attempt END,
    finalized_at = CASE WHEN cancel_requested_at IS NOT NULL THEN now() END,
    lease_expires_at = NULL,
    errors = CASE WHEN $4::jsonb IS NULL THEN errors
                  ELSE {trimmed} || jsonb_build_array(jsonb_build_object('attempt', attempt - 1, 'at', now()) || $4::jsonb)
             END
WHERE id = $1 AND lease_token = $2 AND {leased}
RETURNING queue, coalesce(group_key, ''), state::text`),

		// release gives a task back during shutdown, attempt included. A task
		// whose cancellation was requested is canceled instead.
		release: q(`
UPDATE {s}.tasks
SET state = CASE WHEN cancel_requested_at IS NULL THEN 'available' ELSE 'canceled' END::{s}.task_state,
    attempt = CASE WHEN cancel_requested_at IS NULL THEN attempt - 1 ELSE attempt END,
    finalized_at = CASE WHEN cancel_requested_at IS NOT NULL THEN now() END,
    lease_expires_at = NULL
WHERE id = $1 AND lease_token = $2 AND {leased}
RETURNING queue, coalesce(group_key, ''), state::text`),

		cancelRunning: q(`
UPDATE {s}.tasks
SET state = 'canceled', finalized_at = now(), lease_expires_at = NULL
WHERE id = $1 AND lease_token = $2 AND {leased}
RETURNING queue, coalesce(group_key, '')`),

		queueSettings: q(`SELECT name, paused_at IS NOT NULL, coalesce(max_running, 0) FROM {s}.queues WHERE name = ANY($1)`),

		upsertWorker: q(`
INSERT INTO {s}.workers (id, queues, kinds, hostname, version) VALUES ($1, $2, $5, $3, $4)
ON CONFLICT (id) DO UPDATE
SET queues = EXCLUDED.queues, kinds = EXCLUDED.kinds, hostname = EXCLUDED.hostname, version = EXCLUDED.version,
    started_at = now(), heartbeat_at = now()`),
		// The column heartbeat_at holds the worker's last check-in. It keeps
		// its name: renaming it needs a migration, and the workers of an older
		// release would fail on the renamed column during a rolling deploy.
		checkInWorker: q(`UPDATE {s}.workers SET heartbeat_at = now() WHERE id = $1`),
		deleteWorker:  q(`DELETE FROM {s}.workers WHERE id = $1`),

		// electLeader takes or renews the maintainer's lease for $2
		// microseconds. It returns a row only if $1 is the leader.
		electLeader: q(`
INSERT INTO {s}.leader AS l (name, worker_id, elected_at, expires_at)
VALUES ('maintainer', $1, now(), now() + $2 * interval '1 microsecond')
ON CONFLICT (name) DO UPDATE
SET worker_id = EXCLUDED.worker_id,
    elected_at = CASE WHEN l.worker_id = EXCLUDED.worker_id THEN l.elected_at ELSE now() END,
    expires_at = EXCLUDED.expires_at
WHERE l.worker_id = EXCLUDED.worker_id OR l.expires_at < now()
RETURNING worker_id`),
		// resignLeader gives up $1's lease, and tells the workers on channel
		// $2, so that another takes over at once.
		resignLeader: q(`
WITH resigned AS (
    DELETE FROM {s}.leader WHERE name = 'maintainer' AND worker_id = $1 RETURNING 1
)
SELECT pg_notify($2, '{resigned}') FROM resigned`),

		// promoteDue makes a batch of due scheduled and retryable tasks
		// available, notifies their queues on channel $1, and returns how
		// many it promoted. It reads the notified CTE, so that it runs. Batch
		// limits are part of the text, so that a generic plan stays bounded
		// too. Other maintenance statements have no ORDER BY: they repeat
		// until they handle less than a batch, so order does not matter, and
		// without one the planner never sorts. This one orders by run time,
		// the order of its index, to keep the planner on that index: past run
		// times belong to available tasks too, so the statistics, which see
		// state and run time apart, can suggest that many tasks are due, and a
		// scan of the whole table looks cheaper. Like the fetch, it updates
		// the tasks through an array of their IDs.
		promoteDue: q(`
WITH due AS (
    SELECT id FROM {s}.tasks
    WHERE state IN ('scheduled', 'retryable') AND run_at <= now()
    ORDER BY run_at
    LIMIT {batch}
    FOR UPDATE SKIP LOCKED
), promoted AS (
    UPDATE {s}.tasks t SET state = 'available' WHERE t.id = ANY(ARRAY(SELECT id FROM due)) RETURNING t.queue
), notified AS (
    SELECT count(pg_notify($1, '{available}' || queue)) AS n FROM (SELECT DISTINCT queue FROM promoted) q
)
SELECT (SELECT count(*) FROM promoted) FROM notified`),

		nextDue: q(`
SELECT extract(epoch FROM min(run_at) - now()) FROM {s}.tasks WHERE state IN ('scheduled', 'retryable')`),

		// wakeQueues notifies, on channel $1, every queue that is not paused
		// and whose next task waited longer than $2 microseconds, then, if $3,
		// sends the maintainer's heartbeat, which shows workers that this ran
		// and that notifications reach them. Its queues and their next tasks
		// are found with one index probe each. It reads the woken CTE, so that
		// it runs.
		wakeQueues: q(`
WITH RECURSIVE available AS (
    (SELECT queue FROM {s}.tasks WHERE state = 'available' ORDER BY queue LIMIT 1)
    UNION ALL
    SELECT (SELECT t.queue FROM {s}.tasks t WHERE t.state = 'available' AND t.queue > x.queue ORDER BY t.queue LIMIT 1)
    FROM available x WHERE x.queue IS NOT NULL
), woken AS (
    SELECT count(pg_notify($1, '{available}' || x.queue)) FROM available x
    WHERE x.queue IS NOT NULL
      AND (SELECT t.run_at FROM {s}.tasks t WHERE t.state = 'available' AND t.queue = x.queue
           ORDER BY t.priority DESC, t.run_at, t.id LIMIT 1) < now() - $2 * interval '1 microsecond'
      AND NOT EXISTS (SELECT FROM {s}.queues q WHERE q.name = x.queue AND q.paused_at IS NOT NULL)
)
SELECT CASE WHEN $3 THEN pg_notify($1, '{heartbeat}') END FROM woken`),

		// rescue fails the attempts whose leases expired: their workers
		// crashed or lost the database. $1 and $2 are the default backoff's
		// base and maximum in seconds; the exponent is capped so that tasks
		// with thousands of attempts cannot overflow it. It locks a grouped
		// task's group first, skipping groups that other transactions hold,
		// such as one that enqueues into the group: it never waits for them,
		// and a later run rescues those tasks.
		rescue: q(`
WITH expired AS (
    SELECT t.id FROM {s}.tasks t
    WHERE t.state = 'running' AND t.lease_expires_at < now() AND ` + groupFree + `
    LIMIT {batch}
    FOR UPDATE OF t SKIP LOCKED
)
UPDATE {s}.tasks t
SET state = CASE
            WHEN t.cancel_requested_at IS NOT NULL THEN 'canceled'
            WHEN t.attempt >= t.max_attempts THEN 'dead'
            ELSE 'retryable'
        END::{s}.task_state,
    run_at = CASE
            WHEN t.cancel_requested_at IS NULL AND t.attempt < t.max_attempts
            THEN now() + make_interval(secs => least($1 * power(2, least(t.attempt - 1, 30)), $2) * (0.9 + random() * 0.2))
            ELSE t.run_at
        END,
    finalized_at = CASE WHEN t.cancel_requested_at IS NOT NULL OR t.attempt >= t.max_attempts THEN now() END,
    lease_expires_at = NULL,
    errors = {trimmed} || jsonb_build_array(jsonb_build_object('attempt', t.attempt, 'at', now(),
                 'message', $3::text, 'code', 'lease_lost'))
FROM expired
WHERE t.id = expired.id
RETURNING t.id, t.queue, coalesce(t.group_key, ''), t.state::text, t.kind, t.attempt`),

		// pendingDetach lists the history partitions whose detach was
		// interrupted, which must be finalized before they can be dropped.
		pendingDetach: q(`
SELECT c.relname FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid
WHERE i.inhparent = '{s}.task_history'::regclass AND i.inhdetachpending`),

		retention: q(`
SELECT extract(epoch FROM history_retention)::bigint, extract(epoch FROM dead_retention)::bigint FROM {s}.settings`),

		// deleteFinished deletes dead and canceled tasks older than $1
		// seconds, except the dead heads that keep their groups halted. It
		// locks the rows it picks and skips locked ones, so a task that an
		// operator is retrying is never deleted.
		deleteFinished: q(`
WITH old AS (
    SELECT t.id FROM {s}.tasks t
    WHERE t.state IN ('dead', 'canceled') AND t.finalized_at < now() - $1 * interval '1 second'
      AND NOT EXISTS (SELECT 1 FROM {s}.groups g WHERE g.halted_task_id = t.id)
    LIMIT {batch}
    FOR UPDATE SKIP LOCKED
)
DELETE FROM {s}.tasks t USING old WHERE t.id = old.id`),

		// deleteDedupeKeys deletes expired dedupe keys, skipping keys that an
		// enqueue is claiming again.
		deleteDedupeKeys: q(`
WITH expired AS (
    SELECT key FROM {s}.dedupe_keys WHERE expires_at < now() LIMIT {batch} FOR UPDATE SKIP LOCKED
)
DELETE FROM {s}.dedupe_keys d USING expired WHERE d.key = expired.key`),

		deleteWorkers: q(`DELETE FROM {s}.workers WHERE heartbeat_at < now() - $1 * interval '1 second'`),

		// pruneSchedules deletes the schedules that are not among $1 and did
		// not run or change for a day: they were removed from the code. The day
		// keeps a rolling deploy, or a rollback, from losing their state.
		pruneSchedules: q(`
DELETE FROM {s}.schedules WHERE NOT (name = ANY($1)) AND updated_at < now() - interval '1 day'`),

		loadSchedules: q(`
SELECT name, queue, kind, cron, timezone, payload, next_run_at, paused_at IS NOT NULL
FROM {s}.schedules WHERE name = ANY($1)`),

		// upsertSchedule stores a schedule's definition. Its next run time is
		// replaced only when $8 says the timing changed.
		upsertSchedule: q(`
INSERT INTO {s}.schedules AS s (name, queue, kind, cron, timezone, payload, next_run_at)
VALUES ($1, $2, $3, $4, $5, $6, $7)
ON CONFLICT (name) DO UPDATE
SET queue = EXCLUDED.queue, kind = EXCLUDED.kind, cron = EXCLUDED.cron, timezone = EXCLUDED.timezone,
    payload = EXCLUDED.payload,
    next_run_at = CASE WHEN $8 THEN EXCLUDED.next_run_at ELSE s.next_run_at END,
    updated_at = now()`),

		// advanceSchedule claims the run due at $3; it changes nothing if
		// another maintainer claimed it first or the schedule was paused.
		advanceSchedule: q(`
UPDATE {s}.schedules SET next_run_at = $2, last_run_at = now(), updated_at = now()
WHERE name = $1 AND next_run_at = $3 AND paused_at IS NULL`),

		listSchedules: q(`
SELECT name, queue, kind, cron, timezone, next_run_at, last_run_at, paused_at IS NOT NULL
FROM {s}.schedules ORDER BY name`),

		// readQueue reads queue $1's settings, which are NULL when it has no
		// row.
		readQueue: q(`SELECT (SELECT max_running FROM {s}.queues WHERE name = $1)`),

		// swapQueue writes queue $1's settings $3 only if they are still $2,
		// as readQueue read them, creating its row if it has none, and tells
		// the workers on channel $4 with payload $5. It returns 1 if it
		// wrote, 0 if the settings changed meanwhile.
		swapQueue: q(`
WITH swapped AS (
    INSERT INTO {s}.queues AS q (name, max_running) VALUES ($1, $3)
    ON CONFLICT (name) DO UPDATE SET max_running = $3, updated_at = now()
    WHERE q.max_running IS NOT DISTINCT FROM $2
    RETURNING 1
)
SELECT count(pg_notify($4, $5)) FROM swapped`),

		pauseQueue: q(`
INSERT INTO {s}.queues AS q (name, paused_at) VALUES ($1, CASE WHEN $2 THEN now() END)
ON CONFLICT (name) DO UPDATE
SET paused_at = CASE WHEN $2 THEN coalesce(q.paused_at, now()) END, updated_at = now()`),

		listQueues: q(`
WITH served AS (
    SELECT q AS name, count(*)::int AS workers FROM {s}.workers, unnest(queues) AS q GROUP BY q
), names AS (
    SELECT name FROM {s}.queues
    UNION SELECT queue FROM {s}.queue_stats
    UNION SELECT name FROM served
)
SELECT n.name, coalesce(q.max_running, 0), q.paused_at IS NOT NULL, coalesce(s.workers, 0)
FROM names n
LEFT JOIN {s}.queues q ON q.name = n.name
LEFT JOIN served s ON s.name = n.name
ORDER BY n.name`),

		// queueStats reads the statistics, with the lag of each count in
		// microseconds by the database's clock now: 0 for a count without a
		// due task.
		queueStats: q(`
SELECT queue, state::text, tasks, capped, refreshed_at,
       greatest(0, (extract(epoch FROM now() - oldest_due_at) * 1000000)::bigint)
FROM {s}.queue_stats`),

		// readSettings reads the retention periods in microseconds, which
		// swapSettings compares and writes.
		readSettings: q(`
SELECT (extract(epoch FROM history_retention) * 1000000)::bigint, (extract(epoch FROM dead_retention) * 1000000)::bigint
FROM {s}.settings`),

		// swapSettings writes the retention periods $3 and $4 only if they
		// are still $1 and $2, as readSettings read them. It changes no row
		// if they changed meanwhile.
		swapSettings: q(`
UPDATE {s}.settings
SET history_retention = $3 * interval '1 microsecond', dead_retention = $4 * interval '1 microsecond',
    updated_at = now()
WHERE (extract(epoch FROM history_retention) * 1000000)::bigint = $1
  AND (extract(epoch FROM dead_retention) * 1000000)::bigint = $2`),

		listWorkers: q(`
SELECT w.id, w.hostname, w.version, w.queues, w.kinds, w.started_at, w.heartbeat_at, l.worker_id IS NOT NULL
FROM {s}.workers w
LEFT JOIN {s}.leader l ON l.name = 'maintainer' AND l.worker_id = w.id AND l.expires_at > now()
ORDER BY w.id`),

		// listTasks pages through live tasks and history, newest first. Each
		// branch is limited on its own, so both can use their indexes. It runs
		// with custom plans, so that the planner sees which filters are set.
		listTasks: q(`
SELECT ` + taskColumns + ` FROM (
    (SELECT ` + taskColumns + ` FROM {s}.tasks
     WHERE $6 AND (cardinality($1::text[]) = 0 OR queue = ANY($1::text[]))
       AND (cardinality($2::text[]) = 0 OR state = ANY($2::text[]::{s}.task_state[]))
       AND (cardinality($3::text[]) = 0 OR kind = ANY($3::text[]))
       AND (cardinality($8::text[]) = 0 OR group_key = ANY($8::text[])) AND id < $4
     ORDER BY id DESC LIMIT $5)
    UNION ALL
    (SELECT id, queue, kind, 'completed', priority, attempt, max_attempts, coalesce(group_key, ''),
            metadata, payload, errors, created_at, run_at, attempted_at, finalized_at
     FROM {s}.task_history
     WHERE $7 AND (cardinality($1::text[]) = 0 OR queue = ANY($1::text[]))
       AND (cardinality($3::text[]) = 0 OR kind = ANY($3::text[]))
       AND (cardinality($8::text[]) = 0 OR group_key = ANY($8::text[])) AND id < $4
     ORDER BY id DESC LIMIT $5)
) page(id, queue, kind, state, priority, attempt, max_attempts, group_key, metadata, payload, errors,
       created_at, run_at, attempted_at, finalized_at)
ORDER BY id DESC LIMIT $5`),

		// listGroups reads a page of FIFO groups in the order of the primary
		// key, after group ($4, $5) where the last page ended, so a page with
		// no search reads only its own rows. Halted groups only ($3) read the
		// index of halted groups, in the same order.
		listGroups: q(`
SELECT queue, group_key, tasks, coalesce(halted_task_id, 0) FROM {s}.groups
WHERE tasks > 0
  AND ($1 = '' OR strpos(lower(group_key), lower($1)) > 0)
  AND (NOT $3 OR halted_task_id IS NOT NULL)
  AND (queue, group_key) > ($4, $5)
ORDER BY queue, group_key
LIMIT $2`),

		// listQueueGroups reads a page of listGroups in queues $6 only. Each
		// queue from the cursor's on reads its own groups in key order, up to
		// a page, from where the last page ended, so a page reads no group
		// of an earlier one.
		listQueueGroups: q(`
SELECT g.queue, g.group_key, g.tasks, coalesce(g.halted_task_id, 0)
FROM (SELECT DISTINCT unnest($6::text[])) AS u(q),
LATERAL (
    SELECT g.queue, g.group_key, g.tasks, g.halted_task_id FROM {s}.groups g
    WHERE g.queue = u.q AND g.group_key > CASE WHEN u.q = $4 THEN $5 ELSE '' END
      AND g.tasks > 0
      AND ($1 = '' OR strpos(lower(g.group_key), lower($1)) > 0)
      AND (NOT $3 OR g.halted_task_id IS NOT NULL)
    ORDER BY g.group_key
    LIMIT $2
) g
WHERE u.q >= $4
ORDER BY g.queue, g.group_key
LIMIT $2`),

		// countStats counts each queue's tasks per state, except completed
		// ones, with the run time of the oldest due task of each count. The
		// states whose indexes lead with the queue are counted per queue, up
		// to $1 tasks each: their queues are found with one index probe per
		// queue, and the oldest available task with one more per priority.
		// Scheduled and retryable tasks are counted across all queues, oldest
		// first, up to $2 in all, so the due ones come first. Every branch
		// reads only an index.
		countStats: q(`
WITH RECURSIVE available AS (
    (SELECT queue FROM {s}.tasks WHERE state = 'available' ORDER BY queue LIMIT 1)
    UNION ALL
    SELECT (SELECT t.queue FROM {s}.tasks t WHERE t.state = 'available' AND t.queue > x.queue ORDER BY t.queue LIMIT 1)
    FROM available x WHERE x.queue IS NOT NULL
), running AS (
    (SELECT queue FROM {s}.tasks WHERE state = 'running' ORDER BY queue LIMIT 1)
    UNION ALL
    SELECT (SELECT t.queue FROM {s}.tasks t WHERE t.state = 'running' AND t.queue > x.queue ORDER BY t.queue LIMIT 1)
    FROM running x WHERE x.queue IS NOT NULL
), pending AS (
    (SELECT queue FROM {s}.tasks WHERE state = 'pending' ORDER BY queue LIMIT 1)
    UNION ALL
    SELECT (SELECT t.queue FROM {s}.tasks t WHERE t.state = 'pending' AND t.queue > x.queue ORDER BY t.queue LIMIT 1)
    FROM pending x WHERE x.queue IS NOT NULL
), finished AS (
    (SELECT queue FROM {s}.tasks WHERE state IN ('dead', 'canceled') ORDER BY queue LIMIT 1)
    UNION ALL
    SELECT (SELECT t.queue FROM {s}.tasks t WHERE t.state IN ('dead', 'canceled') AND t.queue > x.queue ORDER BY t.queue LIMIT 1)
    FROM finished x WHERE x.queue IS NOT NULL
)
SELECT 'available', x.queue,
       (SELECT count(*) FROM (SELECT FROM {s}.tasks t WHERE t.state = 'available' AND t.queue = x.queue LIMIT $1) c),
       (SELECT min(o.run_at) FROM unnest('{1,2,3,4}'::smallint[]) AS p, -- every priority
        LATERAL (SELECT t.run_at FROM {s}.tasks t WHERE t.state = 'available' AND t.queue = x.queue AND t.priority = p
                 ORDER BY t.run_at LIMIT 1) o)
FROM available x WHERE x.queue IS NOT NULL
UNION ALL
SELECT 'running', x.queue,
       (SELECT count(*) FROM (SELECT FROM {s}.tasks t WHERE t.state = 'running' AND t.queue = x.queue LIMIT $1) c), NULL
FROM running x WHERE x.queue IS NOT NULL
UNION ALL
SELECT 'pending', x.queue,
       (SELECT count(*) FROM (SELECT FROM {s}.tasks t WHERE t.state = 'pending' AND t.queue = x.queue LIMIT $1) c), NULL
FROM pending x WHERE x.queue IS NOT NULL
UNION ALL
SELECT 'dead', x.queue,
       (SELECT count(*) FROM (SELECT FROM {s}.tasks t WHERE t.state = 'dead' AND t.queue = x.queue LIMIT $1) c), NULL
FROM finished x WHERE x.queue IS NOT NULL
UNION ALL
SELECT 'canceled', x.queue,
       (SELECT count(*) FROM (SELECT FROM {s}.tasks t WHERE t.state = 'canceled' AND t.queue = x.queue LIMIT $1) c), NULL
FROM finished x WHERE x.queue IS NOT NULL
UNION ALL
SELECT state::text, queue, count(*), min(run_at) FILTER (WHERE run_at <= now()) FROM (
    SELECT state, queue, run_at FROM {s}.tasks WHERE state IN ('scheduled', 'retryable') ORDER BY run_at LIMIT $2
) due GROUP BY state, queue`),

		clearStats: q(`DELETE FROM {s}.queue_stats`),
		// storeStats stores the counts, and a zero available count for every
		// other known queue, so that each one shows when it was counted, even
		// an empty installation. Once the shared cap was hit ($6), any queue
		// may have scheduled and retryable tasks that were not counted, so
		// each one missing those counts gets them as capped zeros.
		storeStats: q(`
WITH u AS (
    SELECT q, s, n, c, o FROM unnest($1::text[], $2::text[], $3::bigint[], $4::boolean[], $5::timestamptz[]) AS u(q, s, n, c, o)
), k AS (
    SELECT name FROM {s}.queues UNION SELECT unnest(queues) FROM {s}.workers UNION SELECT q FROM u
)
INSERT INTO {s}.queue_stats (queue, state, tasks, capped, refreshed_at, oldest_due_at)
SELECT q, s::{s}.task_state, n, c, now(), o FROM u
UNION ALL
SELECT k.name, 'available', 0, false, now(), NULL
FROM k WHERE NOT EXISTS (SELECT FROM u WHERE u.q = k.name)
UNION ALL
SELECT k.name, d.s::{s}.task_state, 0, true, now(), NULL
FROM k, unnest('{scheduled,retryable}'::text[]) AS d(s)
WHERE $6 AND NOT EXISTS (SELECT FROM u WHERE u.q = k.name AND u.s = d.s)`),

		// storeActivity copies the counts just stored in queue_stats into the
		// current minute's activity, with zeros for every other known queue,
		// so that an empty queue reads as empty rather than as not counted.
		// A later refresh in the same minute overwrites them. Rows are locked
		// in the byte order of their queue names, the order workers use too.
		storeActivity: q(`
INSERT INTO {s}.activity AS a (minute, queue, scheduled, pending, available, running, retryable, dead, canceled)
SELECT date_trunc('minute', now()), n.queue,
       coalesce(sum(s.tasks) FILTER (WHERE s.state = 'scheduled'), 0),
       coalesce(sum(s.tasks) FILTER (WHERE s.state = 'pending'), 0),
       coalesce(sum(s.tasks) FILTER (WHERE s.state = 'available'), 0),
       coalesce(sum(s.tasks) FILTER (WHERE s.state = 'running'), 0),
       coalesce(sum(s.tasks) FILTER (WHERE s.state = 'retryable'), 0),
       coalesce(sum(s.tasks) FILTER (WHERE s.state = 'dead'), 0),
       coalesce(sum(s.tasks) FILTER (WHERE s.state = 'canceled'), 0)
FROM (SELECT name FROM {s}.queues
      UNION SELECT queue FROM {s}.queue_stats
      UNION SELECT unnest(queues) FROM {s}.workers) AS n(queue)
LEFT JOIN {s}.queue_stats s ON s.queue = n.queue
GROUP BY n.queue
ORDER BY n.queue COLLATE "C"
ON CONFLICT (minute, queue) DO UPDATE
SET scheduled = excluded.scheduled, pending = excluded.pending, available = excluded.available,
    running = excluded.running, retryable = excluded.retryable, dead = excluded.dead,
    canceled = excluded.canceled`),

		// addActivity adds a worker's counts of ended attempts to their
		// minutes. The caller sorts the rows by minute, then by the bytes of
		// the queue name, so that concurrent flushes lock rows in one order.
		addActivity: q(`
INSERT INTO {s}.activity AS a (minute, queue, completed, failed)
SELECT m, q, c, f FROM unnest($1::timestamptz[], $2::text[], $3::integer[], $4::integer[]) AS u(m, q, c, f)
ON CONFLICT (minute, queue) DO UPDATE
SET completed = a.completed + excluded.completed, failed = a.failed + excluded.failed`),

		deleteActivity: q(`
WITH old AS (
    SELECT minute, queue FROM {s}.activity WHERE minute < now() - $1 * interval '1 second'
    LIMIT {batch} FOR UPDATE SKIP LOCKED
)
DELETE FROM {s}.activity a USING old WHERE a.minute = old.minute AND a.queue = old.queue`),

		// activity sums the selected queues' activity per minute, then
		// combines the minutes of each step of $3 seconds: the most tasks
		// counted in each state, and the sum of ended attempts. A minute in
		// which no maintainer counted has NULL counts.
		activity: q(`
WITH minutes AS (
    SELECT minute, sum(scheduled) AS scheduled, sum(pending) AS pending, sum(available) AS available,
           sum(running) AS running, sum(retryable) AS retryable, sum(dead) AS dead, sum(canceled) AS canceled,
           sum(completed) AS completed, sum(failed) AS failed
    FROM {s}.activity
    WHERE minute >= $1 AND minute < $2 AND (cardinality($4::text[]) = 0 OR queue = ANY($4::text[]))
    GROUP BY minute
)
SELECT date_bin($3 * interval '1 second', minute, '2000-01-01 00:00:00+00') AS at,
       max(scheduled), max(pending), max(available), max(running), max(retryable), max(dead), max(canceled),
       sum(completed), sum(failed)
FROM minutes
GROUP BY at
ORDER BY at`),

		// runSchedule makes schedule $1 due now, unless it is paused, and
		// reports whether it exists and is paused. The maintainer then runs
		// it as it runs any due schedule, and computes the next regular run
		// from the expression as usual.
		runSchedule: q(`
WITH s AS (
    SELECT name, paused_at IS NOT NULL AS paused FROM {s}.schedules WHERE name = $1 FOR UPDATE
), due AS (
    UPDATE {s}.schedules t SET next_run_at = now(), updated_at = now()
    FROM s WHERE t.name = s.name AND NOT s.paused
)
SELECT paused FROM s`),

		pauseSchedule: q(`
UPDATE {s}.schedules SET paused_at = CASE WHEN $2 THEN coalesce(paused_at, now()) END, updated_at = now()
WHERE name = $1`),
	}
}

// countTasks returns the statement that counts tasks per state, up to one
// more than {cap} each, with how many of them were not asked to stop: a
// branch for each state, run only if $4 lists it, so that each reads the
// index of its state, or the history.
func countTasks() string {
	var b strings.Builder
	for _, s := range allStates {
		from := `SELECT t.cancel_requested_at FROM {s}.tasks t WHERE t.state = '` + string(s) + `' AND `
		if s == StateCompleted {
			from = `SELECT NULL AS cancel_requested_at FROM {s}.task_history t WHERE `
		}
		if b.Len() > 0 {
			b.WriteString("\nUNION ALL")
		}
		b.WriteString(`
SELECT '` + string(s) + `', n, unflagged FROM (
    SELECT count(*) AS n, count(*) FILTER (WHERE c.cancel_requested_at IS NULL) AS unflagged
    FROM (` + from + taskFilter + ` LIMIT {cap} + 1) c) x
WHERE '` + string(s) + `' = ANY($4::text[])`)
	}
	return b.String()
}
