package taskmill

// This file holds the statements that count the queues, and keep their
// activity per minute. The reasons are in
// https://github.com/code19m/taskmill/blob/main/docs/design/schema-statistics.md.

// $1 cap per queue and state, $2 cap of the scheduled and retryable tasks in all,
// $3 most retryable tasks whose attempts are read.
//
// countStatsSQL counts each queue's tasks per state, except completed ones,
// with the run time of the oldest due task of each count, the queue's halted
// groups with its dead count, and the most failures of its retryable tasks
// with its retryable count. The caps are read one past, so that a count that
// stops at its cap with more tasks left shows. Every branch reads an index,
// and finds the queues of its state with one probe each (queuesWith).
//
// Scheduled and retryable tasks are counted across all queues, oldest first,
// so the due ones come first. The halted groups stop at the cap itself ($1 -
// 1), as they have no capped flag. The most failures are the highest attempt
// among the $3 counted tasks that retry last, and each queue's own task that
// retries last, read backward from the last one counted. A run time written
// by hand that is infinite is no oldest task, so that it cannot fail the
// counts of every queue.
var countStatsSQL = `
WITH RECURSIVE ` + queuesWith(`available`, `= 'available'`) + `, ` +
	queuesWith(`running`, `= 'running'`) + `, ` +
	queuesWith(`pending`, `= 'pending'`) + `, ` +
	queuesWith(`finished`, `IN ('dead', 'canceled')`) + `, due AS (
    SELECT state, queue, count(*) AS tasks,
           min(run_at) FILTER (WHERE run_at <= now() AND run_at > '-infinity') AS oldest, max(run_at) AS last
    FROM (
        SELECT state, queue, run_at FROM {s}.tasks WHERE state IN ('scheduled', 'retryable') ORDER BY run_at LIMIT $2
    ) d GROUP BY state, queue
), failures AS (
    SELECT r.queue, max(a.attempt) AS most FROM (
        (SELECT queue, run_at FROM {s}.tasks
         WHERE state = 'retryable' AND run_at <= (SELECT max(last) FROM due WHERE state = 'retryable')
         ORDER BY run_at DESC LIMIT $3)
        UNION ALL
        SELECT queue, last FROM due WHERE state = 'retryable'
    ) r, LATERAL (
        SELECT t.attempt FROM {s}.tasks t
        WHERE t.state = 'retryable' AND t.run_at = r.run_at AND t.queue = r.queue LIMIT 1
    ) a GROUP BY r.queue
)
SELECT 'available', x.queue,
       (SELECT count(*) FROM (SELECT FROM {s}.tasks t WHERE t.state = 'available' AND t.queue = x.queue LIMIT $1) c),
       (SELECT min(o.run_at) FROM unnest('{1,2,3,4}'::smallint[]) AS p, -- every priority
        LATERAL (SELECT t.run_at FROM {s}.tasks t WHERE t.state = 'available' AND t.queue = x.queue AND t.priority = p
                   AND t.run_at > '-infinity' AND t.run_at < 'infinity'
                 ORDER BY t.run_at LIMIT 1) o),
       NULL::bigint, NULL::integer
FROM available x WHERE x.queue IS NOT NULL
UNION ALL
SELECT 'running', x.queue,
       (SELECT count(*) FROM (SELECT FROM {s}.tasks t WHERE t.state = 'running' AND t.queue = x.queue LIMIT $1) c), NULL, NULL::bigint, NULL::integer
FROM running x WHERE x.queue IS NOT NULL
UNION ALL
SELECT 'pending', x.queue,
       (SELECT count(*) FROM (SELECT FROM {s}.tasks t WHERE t.state = 'pending' AND t.queue = x.queue LIMIT $1) c), NULL, NULL::bigint, NULL::integer
FROM pending x WHERE x.queue IS NOT NULL
UNION ALL
SELECT 'dead', x.queue,
       (SELECT count(*) FROM (SELECT FROM {s}.tasks t WHERE t.state = 'dead' AND t.queue = x.queue LIMIT $1) c), NULL,
       (SELECT count(*) FROM (SELECT FROM {s}.groups g WHERE g.halted_task_id IS NOT NULL AND g.queue = x.queue LIMIT $1 - 1) c),
       NULL
FROM finished x WHERE x.queue IS NOT NULL
UNION ALL
SELECT 'canceled', x.queue,
       (SELECT count(*) FROM (SELECT FROM {s}.tasks t WHERE t.state = 'canceled' AND t.queue = x.queue LIMIT $1) c), NULL, NULL::bigint, NULL::integer
FROM finished x WHERE x.queue IS NOT NULL
UNION ALL
SELECT d.state::text, d.queue, d.tasks, d.oldest, NULL, f.most
FROM due d LEFT JOIN failures f ON d.state = 'retryable' AND f.queue = d.queue`

// lockStatsSQL lets one refresh at a time replace the statistics, while
// everyone reads them, so that the table always holds the counts of exactly
// one refresh, also when two maintainers' terms overlap. The page on
// statistics says why a lock on the table. A maintainer of a release without
// the lock takes its turn too: its delete waits for the lock, and the lock
// for its transaction.
const lockStatsSQL = `LOCK TABLE {s}.queue_stats IN SHARE ROW EXCLUSIVE MODE`

// clearStatsSQL deletes the stored statistics, before storeStatsSQL stores
// new ones.
const clearStatsSQL = `DELETE FROM {s}.queue_stats`

// $1 queues, $2 states, $3 counts, $4 capped, $5 oldest due run times, $6 shared cap passed,
// $7 halted groups, $8 most failures.
//
// storeStatsSQL stores the counts, with their halted groups ($7) and most
// failures ($8), and a zero available count for every other known queue, so
// that each one shows when it was counted, even an empty installation. Once
// the shared cap was hit ($6), any queue may have scheduled and retryable
// tasks that were not counted, so each one missing those counts gets them as
// capped zeros.
const storeStatsSQL = `
WITH u AS (
    SELECT q, s, n, c, o, h, f
    FROM unnest($1::text[], $2::text[], $3::bigint[], $4::boolean[], $5::timestamptz[], $7::bigint[], $8::integer[])
         AS u(q, s, n, c, o, h, f)
), k AS (
    SELECT name FROM {s}.queues UNION SELECT unnest(queues) FROM {s}.workers UNION SELECT q FROM u
)
INSERT INTO {s}.queue_stats (queue, state, tasks, capped, refreshed_at, oldest_due_at, halted_groups, most_failures)
SELECT q, s::{s}.task_state, n, c, now(), o, h, f FROM u
UNION ALL
SELECT k.name, 'available', 0, false, now(), NULL, NULL::bigint, NULL::integer
FROM k WHERE NOT EXISTS (SELECT FROM u WHERE u.q = k.name)
UNION ALL
SELECT k.name, d.s::{s}.task_state, 0, true, now(), NULL, NULL::bigint, NULL::integer
FROM k, unnest('{scheduled,retryable}'::text[]) AS d(s)
WHERE $6 AND NOT EXISTS (SELECT FROM u WHERE u.q = k.name AND u.s = d.s)`

// queueStatsSQL reads the statistics, with the lag of each count in
// microseconds by the database's clock now: 0 for a count without a
// due task. Halted groups and most failures read 0 where not
// stored: with the other states, and from a maintainer that does
// not count them.
const queueStatsSQL = `
SELECT queue, state::text, tasks, capped, refreshed_at,
       greatest(0, (extract(epoch FROM now() - oldest_due_at) * 1000000)::bigint),
       coalesce(halted_groups, 0), coalesce(most_failures, 0)
FROM {s}.queue_stats`

// storeActivitySQL copies the counts just stored in queue_stats into the
// current minute's activity, with zeros for every other known queue,
// so that an empty queue reads as empty rather than as not counted.
// A later refresh in the same minute keeps the higher count of each
// state, so the minute holds the most tasks counted in it: a queue
// that fills and drains within a minute shows. A count is NULL in a
// row that only a worker's attempts made so far, and greatest skips
// it. Rows are locked in the byte order of their queue names, the
// order workers use too.
const storeActivitySQL = `
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
SET scheduled = greatest(a.scheduled, excluded.scheduled), pending = greatest(a.pending, excluded.pending),
    available = greatest(a.available, excluded.available), running = greatest(a.running, excluded.running),
    retryable = greatest(a.retryable, excluded.retryable), dead = greatest(a.dead, excluded.dead),
    canceled = greatest(a.canceled, excluded.canceled)`

// $1 minutes, $2 queues, $3 completed attempts, $4 failed attempts, $5 the
// worker's ID, $6 the write's number.
//
// addActivitySQL adds a worker's counts of ended attempts to their minutes,
// if write $6 raises the number of the worker's last write: see
// flushActivity. It locks the worker's row of activity_writes first, so a
// write tried again waits for a first try that is still under way. The
// caller sorts the rows by minute, then by the bytes of the queue name, so
// that concurrent flushes lock rows in one order.
const addActivitySQL = `
WITH claimed AS (
    INSERT INTO {s}.activity_writes AS w (worker, last_write) VALUES ($5, $6)
    ON CONFLICT (worker) DO UPDATE SET last_write = excluded.last_write, written_at = now()
    WHERE w.last_write < excluded.last_write
    RETURNING 1
)
INSERT INTO {s}.activity AS a (minute, queue, completed, failed)
SELECT m, q, c, f FROM unnest($1::timestamptz[], $2::text[], $3::integer[], $4::integer[]) AS u(m, q, c, f)
WHERE EXISTS (SELECT FROM claimed)
ON CONFLICT (minute, queue) DO UPDATE
SET completed = a.completed + excluded.completed, failed = a.failed + excluded.failed`

// $1 since, $2 before, $3 step in seconds, $4 queues.
//
// activitySQL sums the selected queues' activity per minute, then combines
// the minutes of each step of $3 seconds: the most tasks counted in each
// state, and the sum of ended attempts. A minute in which no maintainer
// counted has NULL counts.
const activitySQL = `
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
ORDER BY at`
