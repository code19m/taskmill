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
    SELECT input.*, nextval(pg_get_serial_sequence('{s}.tasks', 'id')) AS id
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
	}
}
