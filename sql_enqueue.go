package taskmill

// This file holds the statements that enqueue tasks. The reasons are in
// https://github.com/code19m/taskmill/blob/main/docs/design/schema-statements.md,
// and those of deduplication in
// https://github.com/code19m/taskmill/blob/main/docs/design/schema-groups.md.

// $1 queues, $2 kinds, $3 priorities, $4 run times, $5 max attempts, $6 group keys,
// $7 dedupe keys, $8 dedupe windows in microseconds, $9 payloads, $10 metadata,
// $11 channel, $12 queues to notify.
//
// enqueueSQL inserts a batch of tasks in one round trip: the arrays $1 to
// $10 hold one task per element. It claims dedupe keys first, counts
// admitted tasks into their FIFO groups, places each task in its starting
// state, and notifies the queues $12. For a duplicate, it returns the task
// that holds the key, or 0 if it cannot see that task: the holder committed
// while the statement waited for the key. A holder whose window ended is
// such a case: the statement would have claimed its key, had another enqueue
// not done so meanwhile. The client then reads the key again.
//
// A task whose key the statement sees as held is a duplicate right away, and
// skips the claim, so that a duplicate writes nothing. That is exact only at
// READ COMMITTED, where the statement sees the keys as they are when it
// begins: in a caller's transaction at REPEATABLE READ or SERIALIZABLE,
// every key goes through the claim. The page on deduplication says why.
const enqueueSQL = `
WITH input AS (
    SELECT queue, kind, priority, run_at, max_attempts, group_key,
           dedupe_key, dedupe_window_us, payload, metadata, ord
    FROM unnest($1::text[], $2::text[], $3::smallint[], $4::timestamptz[], $5::smallint[],
                $6::text[], $7::bytea[], $8::bigint[], $9::jsonb[], $10::jsonb[])
        WITH ORDINALITY AS t(queue, kind, priority, run_at, max_attempts, group_key,
                             dedupe_key, dedupe_window_us, payload, metadata, ord)
), ids AS (
    SELECT input.*,
           CASE WHEN dedupe_key IS NOT NULL
                     AND current_setting('transaction_isolation') NOT IN ('repeatable read', 'serializable')
                     AND EXISTS (SELECT FROM {s}.dedupe_keys k
                                 WHERE k.key = input.dedupe_key AND k.expires_at > now())
                THEN NULL
                ELSE nextval('{s}.tasks_id_seq') END AS id
    FROM input
), claimed AS (
    INSERT INTO {s}.dedupe_keys AS d (key, task_id, expires_at)
    SELECT dedupe_key, id, now() + dedupe_window_us * interval '1 microsecond'
    FROM ids
    WHERE dedupe_key IS NOT NULL AND id IS NOT NULL
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
LEFT JOIN {s}.dedupe_keys d ON i.id IS NULL AND d.key = ids.dedupe_key AND d.expires_at > now()
CROSS JOIN notified
ORDER BY ids.ord`

// $1 to $12 as in enqueueSQL.
//
// enqueuePlainSQL is enqueue for batches without dedupe keys or FIFO groups,
// the common case: it skips their bookkeeping. It takes the same parameters
// and returns the same rows.
const enqueuePlainSQL = `
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
SELECT ids.ord, ids.id, false FROM ids CROSS JOIN notified ORDER BY ids.ord`

// $1 dedupe keys.
//
// dedupeLookupSQL reads the tasks that hold the keys $1, for the duplicates
// whose holder the enqueue could not see.
const dedupeLookupSQL = `SELECT key, task_id FROM {s}.dedupe_keys WHERE key = ANY($1::bytea[])`
