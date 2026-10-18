package taskmill

import (
	"regexp"
	"strings"
)

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

// statements holds every SQL statement with the schema name filled in, so each
// has fixed text and can be prepared once per connection.
type statements struct {
	schemaVersion string

	enqueue      string
	enqueuePlain string
	dedupeLookup string

	complete string

	getTask       string
	cancel        string
	lockForRetry  string
	retry         string
	deleteTask    string
	deleteHistory string
	taskExists    string

	haltGroup        string
	decrementGroup   string
	deleteEmptyGroup string
	promoteGroup     string
	enterGroup       string
	resumeHaltedHead string

	fetch          string
	lockQueue      string
	countRunning   string
	heartbeat      string
	fail           string
	snooze         string
	release        string
	cancelRunning  string
	queueSettings  string
	upsertWorker   string
	beatWorker     string
	deleteWorker   string
	electLeader    string
	resignLeader   string
	promoteDue     string
	nextDue        string
	rescue         string
	expireDeadline string

	retention       string
	deleteFinished  string
	loadSchedules   string
	upsertSchedule  string
	advanceSchedule string
	listSchedules   string
	pauseSchedule   string

	updateQueue      string
	pauseQueue       string
	listQueues       string
	queueStats       string
	updateSettings   string
	listWorkers      string
	listTasks        string
	countState       string
	clearStats       string
	storeStats       string
	deleteDedupeKeys string
	deleteWorkers    string
}

// taskColumns are the columns that scanTaskInfo reads, in its order.
const taskColumns = `id, queue, kind, state::text, priority, attempt, max_attempts, coalesce(group_key, ''),
       metadata, payload, errors, created_at, run_at, deadline, attempted_at, finalized_at`

func newStatements(schema string) *statements {
	r := strings.NewReplacer("{s}", schema)
	q := func(sql string) string { return r.Replace(sql) }

	return &statements{
		schemaVersion: q(`SELECT coalesce(max(version), 0) FROM {s}.migrations`),

		// enqueue inserts a batch of tasks in one round trip. It claims dedupe
		// keys first, counts admitted tasks into their FIFO groups, places each
		// task in its starting state, and notifies the listed queues.
		enqueue: q(`
WITH input AS (
    SELECT queue, kind, priority, run_at, deadline, max_attempts, group_key,
           dedupe_key, dedupe_window_us, payload, metadata, ord
    FROM unnest($1::text[], $2::text[], $3::smallint[], $4::timestamptz[], $5::timestamptz[],
                $6::smallint[], $7::text[], $8::bytea[], $9::bigint[], $10::jsonb[], $11::jsonb[])
        WITH ORDINALITY AS t(queue, kind, priority, run_at, deadline, max_attempts, group_key,
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
    ORDER BY queue, group_key
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
    INSERT INTO {s}.tasks (id, queue, kind, state, priority, run_at, deadline, group_key, payload, metadata, max_attempts)
    OVERRIDING SYSTEM VALUE
    SELECT id, queue, kind, state, priority, coalesce(run_at, now()), deadline, group_key, payload, metadata, max_attempts
    FROM placed
    ORDER BY ord
    RETURNING id
), notified AS (
    SELECT count(pg_notify($12, 'a|' || q)) AS n FROM unnest($13::text[]) AS q
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
    SELECT queue, kind, priority, run_at, deadline, max_attempts, payload, metadata, ord,
           nextval('{s}.tasks_id_seq') AS id
    FROM unnest($1::text[], $2::text[], $3::smallint[], $4::timestamptz[], $5::timestamptz[],
                $6::smallint[], $7::text[], $8::bytea[], $9::bigint[], $10::jsonb[], $11::jsonb[])
        WITH ORDINALITY AS t(queue, kind, priority, run_at, deadline, max_attempts, group_key,
                             dedupe_key, dedupe_window_us, payload, metadata, ord)
), inserted AS (
    INSERT INTO {s}.tasks (id, queue, kind, state, priority, run_at, deadline, payload, metadata, max_attempts)
    OVERRIDING SYSTEM VALUE
    SELECT id, queue, kind,
           CASE WHEN coalesce(run_at, now()) > now() THEN 'scheduled' ELSE 'available' END::{s}.task_state,
           priority, coalesce(run_at, now()), deadline, payload, metadata, max_attempts
    FROM ids
    ORDER BY ord
), notified AS (
    SELECT count(pg_notify($12, 'a|' || q)) AS n FROM unnest($13::text[]) AS q
)
SELECT ids.ord, ids.id, false FROM ids CROSS JOIN notified ORDER BY ids.ord`),

		dedupeLookup: q(`SELECT key, task_id FROM {s}.dedupe_keys WHERE key = ANY($1::bytea[])`),

		// complete finishes running tasks whose attempts still hold their
		// leases: it moves them to history, unless their queue keeps none.
		complete: q(`
WITH done AS (
    DELETE FROM {s}.tasks
    WHERE (id, lease_token) IN (SELECT i, t FROM unnest($1::bigint[], $2::integer[]) AS u(i, t))
      AND state = 'running'
    RETURNING id, created_at, run_at, attempted_at, priority, attempt, max_attempts,
              queue, kind, group_key, attempted_by, payload, metadata, errors
), kept AS (
    INSERT INTO {s}.task_history (id, finalized_at, created_at, run_at, attempted_at, priority, attempt,
                                  max_attempts, queue, kind, group_key, attempted_by, payload, metadata, errors)
    SELECT id, now(), created_at, run_at, attempted_at, priority, attempt,
           max_attempts, queue, kind, group_key, attempted_by, payload, metadata, errors
    FROM done
    WHERE NOT EXISTS (SELECT 1 FROM {s}.queues q WHERE q.name = done.queue AND NOT q.keep_history)
)
SELECT id, queue, coalesce(group_key, '') FROM done`),

		getTask: q(`
SELECT ` + taskColumns + ` FROM {s}.tasks WHERE id = $1
UNION ALL
SELECT id, queue, kind, 'completed', priority, attempt, max_attempts, coalesce(group_key, ''),
       metadata, payload, errors, created_at, run_at, NULL, attempted_at, finalized_at
FROM {s}.task_history WHERE id = $1
LIMIT 1`),

		// cancel finishes a waiting task at once, and flags a running one for
		// its worker. It returns the state before and after.
		cancel: q(`
WITH old AS (
    SELECT id, state FROM {s}.tasks WHERE id = $1 FOR UPDATE
)
UPDATE {s}.tasks t
SET cancel_requested_at = now(),
    state = CASE WHEN old.state = 'running' THEN t.state ELSE 'canceled' END,
    finalized_at = CASE WHEN old.state = 'running' THEN NULL ELSE now() END
FROM old
WHERE t.id = old.id AND old.state IN ('scheduled', 'pending', 'available', 'running', 'retryable')
RETURNING old.state::text, t.queue, coalesce(t.group_key, '')`),

		lockForRetry: q(`
SELECT t.state::text, t.queue, coalesce(t.group_key, ''),
       coalesce((SELECT g.halted_task_id = t.id FROM {s}.groups g
                 WHERE g.queue = t.queue AND g.group_key = t.group_key), false)
FROM {s}.tasks t WHERE t.id = $1 FOR UPDATE`),

		retry: q(`
UPDATE {s}.tasks
SET state = CASE WHEN $2 THEN 'pending' ELSE 'available' END::{s}.task_state,
    attempt = 0, run_at = now(), finalized_at = NULL, cancel_requested_at = NULL
WHERE id = $1 AND state IN ('dead', 'canceled')
RETURNING queue`),

		deleteTask: q(`
DELETE FROM {s}.tasks WHERE id = $1 AND state <> 'running'
RETURNING state::text, queue, coalesce(group_key, ''),
          coalesce((SELECT g.halted_task_id = $1 FROM {s}.groups g
                    WHERE g.queue = tasks.queue AND g.group_key = tasks.group_key), false)`),

		deleteHistory: q(`DELETE FROM {s}.task_history WHERE id = $1`),

		taskExists: q(`SELECT state::text FROM {s}.tasks WHERE id = $1`),

		// haltGroup marks a group halted by its dead head, if the group's queue
		// halts groups. The dead head stays counted.
		haltGroup: q(`
UPDATE {s}.groups g SET halted_task_id = $3, halted_at = now(), updated_at = now()
FROM {s}.queues qs
WHERE g.queue = $1 AND g.group_key = $2 AND qs.name = g.queue AND qs.halt_group_on_dead`),

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

		// promoteGroup makes the oldest pending task the group's head. It must
		// run as its own statement after decrementGroup, so that it sees tasks
		// enqueued while decrementGroup waited for the group's lock.
		promoteGroup: q(`
WITH promoted AS (
    UPDATE {s}.tasks
    SET state = CASE WHEN run_at > now() THEN 'scheduled' ELSE 'available' END::{s}.task_state
    WHERE id = (
        SELECT id FROM {s}.tasks
        WHERE queue = $1 AND group_key = $2 AND state = 'pending'
        ORDER BY id
        LIMIT 1
        FOR UPDATE
    )
    RETURNING queue, state
), notified AS (
    SELECT count(pg_notify($3, 'a|' || queue)) AS n FROM promoted WHERE $4 AND state = 'available'
)
SELECT promoted.state::text, notified.n > 0
FROM promoted CROSS JOIN notified`),

		enterGroup: q(`
INSERT INTO {s}.groups AS g (queue, group_key, tasks) VALUES ($1, $2, 1)
ON CONFLICT (queue, group_key) DO UPDATE SET tasks = g.tasks + 1, updated_at = now()
RETURNING tasks`),

		resumeHaltedHead: q(`
UPDATE {s}.groups SET halted_task_id = NULL, halted_at = NULL, updated_at = now()
WHERE queue = $1 AND group_key = $2 AND halted_task_id = $3`),

		// fetch leases up to $2 available tasks of queue $1 for $3
		// microseconds, in priority, run time, and enqueue order.
		fetch: q(`
WITH picked AS (
    SELECT id FROM {s}.tasks
    WHERE state = 'available' AND queue = $1 AND (deadline IS NULL OR deadline > now())
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
FROM picked
WHERE t.id = picked.id
RETURNING t.id, t.kind, t.priority, t.attempt, t.max_attempts, t.lease_token, coalesce(t.group_key, ''),
          t.metadata, t.payload, t.errors, t.created_at, t.run_at, t.deadline, t.attempted_at`),

		// lockQueue serializes fetches from a queue with a concurrency limit.
		// countRunning then runs as its own statement, so it sees the tasks
		// that fetches committed while lockQueue waited.
		lockQueue:    q(`SELECT max_running FROM {s}.queues WHERE name = $1 FOR UPDATE`),
		countRunning: q(`SELECT count(*) FROM {s}.tasks WHERE queue = $1 AND state = 'running'`),

		// heartbeat renews the leases of attempts that still own their tasks.
		heartbeat: q(`
UPDATE {s}.tasks
SET lease_expires_at = now() + $3 * interval '1 microsecond'
WHERE (id, lease_token) IN (SELECT i, t FROM unnest($1::bigint[], $2::integer[]) AS u(i, t))
  AND state = 'running'
RETURNING id, cancel_requested_at IS NOT NULL`),

		// fail records a failed attempt: retryable after a delay of $4
		// microseconds, or dead. $5 is the error as a JSON object.
		fail: q(`
UPDATE {s}.tasks
SET state = $3::{s}.task_state,
    run_at = CASE WHEN $3::{s}.task_state = 'retryable' THEN now() + $4 * interval '1 microsecond' ELSE run_at END,
    finalized_at = CASE WHEN $3::{s}.task_state = 'dead' THEN now() END,
    lease_expires_at = NULL,
    errors = (CASE WHEN jsonb_array_length(errors) >= 20 THEN errors - 0 ELSE errors END)
             || jsonb_build_array(jsonb_build_object('attempt', attempt, 'at', now()) || $5::jsonb)
WHERE id = $1 AND lease_token = $2 AND state = 'running'
RETURNING queue, coalesce(group_key, '')`),

		// snooze runs the task again after $3 microseconds and gives the
		// attempt back.
		snooze: q(`
UPDATE {s}.tasks
SET state = CASE WHEN $3 > 0 THEN 'scheduled' ELSE 'available' END::{s}.task_state,
    run_at = now() + $3 * interval '1 microsecond',
    attempt = attempt - 1,
    lease_expires_at = NULL
WHERE id = $1 AND lease_token = $2 AND state = 'running'
RETURNING queue`),

		// release gives a task back during shutdown, attempt included.
		release: q(`
UPDATE {s}.tasks
SET state = 'available', attempt = attempt - 1, lease_expires_at = NULL
WHERE id = $1 AND lease_token = $2 AND state = 'running'
RETURNING queue`),

		cancelRunning: q(`
UPDATE {s}.tasks
SET state = 'canceled', finalized_at = now(), lease_expires_at = NULL
WHERE id = $1 AND lease_token = $2 AND state = 'running'
RETURNING queue, coalesce(group_key, '')`),

		queueSettings: q(`SELECT name, paused_at IS NOT NULL, coalesce(max_running, 0) FROM {s}.queues WHERE name = ANY($1)`),

		upsertWorker: q(`
INSERT INTO {s}.workers (id, queues, hostname, version) VALUES ($1, $2, $3, $4)
ON CONFLICT (id) DO UPDATE
SET queues = EXCLUDED.queues, hostname = EXCLUDED.hostname, version = EXCLUDED.version,
    started_at = now(), heartbeat_at = now()`),
		beatWorker:   q(`UPDATE {s}.workers SET heartbeat_at = now() WHERE id = $1`),
		deleteWorker: q(`DELETE FROM {s}.workers WHERE id = $1`),

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
		resignLeader: q(`DELETE FROM {s}.leader WHERE name = 'maintainer' AND worker_id = $1`),

		promoteDue: q(`
WITH due AS (
    SELECT id FROM {s}.tasks
    WHERE state IN ('scheduled', 'retryable') AND run_at <= now()
    ORDER BY run_at
    LIMIT $1
    FOR UPDATE SKIP LOCKED
), promoted AS (
    UPDATE {s}.tasks t SET state = 'available' FROM due WHERE t.id = due.id RETURNING t.queue
)
SELECT queue, count(*) FROM promoted GROUP BY queue`),

		nextDue: q(`
SELECT extract(epoch FROM min(run_at) - now()) FROM {s}.tasks WHERE state IN ('scheduled', 'retryable')`),

		// rescue fails the attempts whose leases expired: their workers
		// crashed or lost the database. $1 and $2 are the default backoff's
		// base and maximum in seconds.
		rescue: q(`
WITH expired AS (
    SELECT id FROM {s}.tasks
    WHERE state = 'running' AND lease_expires_at < now()
    ORDER BY id
    LIMIT $3
    FOR UPDATE SKIP LOCKED
)
UPDATE {s}.tasks t
SET state = CASE
            WHEN t.cancel_requested_at IS NOT NULL THEN 'canceled'
            WHEN t.attempt >= t.max_attempts THEN 'dead'
            ELSE 'retryable'
        END::{s}.task_state,
    run_at = CASE
            WHEN t.cancel_requested_at IS NULL AND t.attempt < t.max_attempts
            THEN now() + make_interval(secs => least($1 * power(2, t.attempt - 1), $2) * (0.9 + random() * 0.2))
            ELSE t.run_at
        END,
    finalized_at = CASE WHEN t.cancel_requested_at IS NOT NULL OR t.attempt >= t.max_attempts THEN now() END,
    lease_expires_at = NULL,
    errors = (CASE WHEN jsonb_array_length(t.errors) >= 20 THEN t.errors - 0 ELSE t.errors END)
             || jsonb_build_array(jsonb_build_object('attempt', t.attempt, 'at', now(),
                    'message', 'the worker lost its lease: it crashed, stopped, or lost the database',
                    'code', 'lease_expired'))
FROM expired
WHERE t.id = expired.id
RETURNING t.id, t.queue, coalesce(t.group_key, ''), t.state::text`),

		// expireDeadline makes waiting tasks dead once their deadline passed.
		expireDeadline: q(`
WITH expired AS (
    SELECT id, state FROM {s}.tasks
    WHERE deadline < now() AND deadline IS NOT NULL
      AND state IN ('scheduled', 'pending', 'available', 'retryable')
    ORDER BY deadline
    LIMIT $1
    FOR UPDATE SKIP LOCKED
)
UPDATE {s}.tasks t
SET state = 'dead', finalized_at = now(),
    errors = (CASE WHEN jsonb_array_length(t.errors) >= 20 THEN t.errors - 0 ELSE t.errors END)
             || jsonb_build_array(jsonb_build_object('attempt', t.attempt, 'at', now(),
                    'message', 'the deadline passed before the task started', 'code', 'deadline_exceeded'))
FROM expired
WHERE t.id = expired.id
RETURNING t.id, t.queue, coalesce(t.group_key, ''), expired.state::text`),

		retention: q(`
SELECT extract(epoch FROM history_retention)::bigint, extract(epoch FROM dead_retention)::bigint FROM {s}.settings`),

		// deleteFinished deletes dead and canceled tasks older than $1
		// seconds, except the dead heads that keep their groups halted.
		deleteFinished: q(`
DELETE FROM {s}.tasks WHERE id IN (
    SELECT t.id FROM {s}.tasks t
    WHERE t.state IN ('dead', 'canceled') AND t.finalized_at < now() - $1 * interval '1 second'
      AND NOT EXISTS (SELECT 1 FROM {s}.groups g WHERE g.halted_task_id = t.id)
    LIMIT $2
)`),

		deleteDedupeKeys: q(`
DELETE FROM {s}.dedupe_keys WHERE key IN (
    SELECT key FROM {s}.dedupe_keys WHERE expires_at < now() LIMIT $1
)`),

		deleteWorkers: q(`DELETE FROM {s}.workers WHERE heartbeat_at < now() - $1 * interval '1 second'`),

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

		updateQueue: q(`
INSERT INTO {s}.queues AS q (name, max_running, keep_history, halt_group_on_dead) VALUES ($1, $2, $3, $4)
ON CONFLICT (name) DO UPDATE
SET max_running = EXCLUDED.max_running, keep_history = EXCLUDED.keep_history,
    halt_group_on_dead = EXCLUDED.halt_group_on_dead, updated_at = now()`),

		pauseQueue: q(`
INSERT INTO {s}.queues AS q (name, paused_at) VALUES ($1, CASE WHEN $2 THEN now() END)
ON CONFLICT (name) DO UPDATE
SET paused_at = CASE WHEN $2 THEN coalesce(q.paused_at, now()) END, updated_at = now()`),

		listQueues: q(`
WITH names AS (
    SELECT name FROM {s}.queues
    UNION SELECT queue FROM {s}.queue_stats
    UNION SELECT unnest(queues) FROM {s}.workers
)
SELECT n.name, coalesce(q.max_running, 0), NOT coalesce(q.keep_history, true),
       coalesce(q.halt_group_on_dead, false), q.paused_at IS NOT NULL,
       (SELECT count(*) FROM {s}.workers w WHERE n.name = ANY(w.queues))::int
FROM names n LEFT JOIN {s}.queues q ON q.name = n.name
ORDER BY n.name`),

		queueStats: q(`SELECT queue, state::text, tasks, capped, refreshed_at FROM {s}.queue_stats`),

		updateSettings: q(`
UPDATE {s}.settings
SET history_retention = $1 * interval '1 microsecond', dead_retention = $2 * interval '1 microsecond',
    updated_at = now()`),

		listWorkers: q(`SELECT id, hostname, version, queues, started_at, heartbeat_at FROM {s}.workers ORDER BY id`),

		// listTasks pages through live tasks and history, newest first. Each
		// branch is limited on its own, so both can use their ID indexes.
		listTasks: q(`
SELECT ` + taskColumns + ` FROM (
    (SELECT ` + taskColumns + ` FROM {s}.tasks
     WHERE $6 AND ($1 = '' OR queue = $1) AND (cardinality($2::text[]) = 0 OR state::text = ANY($2))
       AND ($3 = '' OR kind = $3) AND id < $4
     ORDER BY id DESC LIMIT $5)
    UNION ALL
    (SELECT id, queue, kind, 'completed', priority, attempt, max_attempts, coalesce(group_key, ''),
            metadata, payload, errors, created_at, run_at, NULL::timestamptz, attempted_at, finalized_at
     FROM {s}.task_history
     WHERE $7 AND ($1 = '' OR queue = $1) AND ($3 = '' OR kind = $3) AND id < $4
     ORDER BY id DESC LIMIT $5)
) page(id, queue, kind, state, priority, attempt, max_attempts, group_key, metadata, payload, errors,
       created_at, run_at, deadline, attempted_at, finalized_at)
ORDER BY id DESC LIMIT $5`),

		// countState counts each queue's tasks in one state, reading at most
		// $2 index entries in all.
		countState: q(`
SELECT queue, count(*) FROM (
    SELECT queue FROM {s}.tasks WHERE state = $1::{s}.task_state LIMIT $2
) capped GROUP BY queue`),

		clearStats: q(`DELETE FROM {s}.queue_stats`),
		storeStats: q(`
INSERT INTO {s}.queue_stats (queue, state, tasks, capped, refreshed_at)
SELECT q, s::{s}.task_state, n, c, now()
FROM unnest($1::text[], $2::text[], $3::bigint[], $4::boolean[]) AS u(q, s, n, c)`),

		pauseSchedule: q(`
UPDATE {s}.schedules SET paused_at = CASE WHEN $2 THEN coalesce(paused_at, now()) END, updated_at = now()
WHERE name = $1`),
	}
}
