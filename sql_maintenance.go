package taskmill

// This file holds the statements of the maintainer's work, and of the
// workers' own rows. The reasons are in
// https://github.com/code19m/taskmill/blob/main/docs/design/schema-maintenance.md.

// $1 worker ID, $2 queues, $3 kinds, $4 host name, $5 version, $6 lease in microseconds.
//
// upsertWorkerSQL registers worker $1, with its lease of $6 microseconds:
// the worker checks in every third of it, so listWorkers and
// deleteWorkers judge by it how long the worker may be silent.
const upsertWorkerSQL = `
INSERT INTO {s}.workers (id, queues, kinds, hostname, version, lease_duration)
VALUES ($1, $2, $3, $4, $5, $6 * interval '1 microsecond')
ON CONFLICT (id) DO UPDATE
SET queues = EXCLUDED.queues, kinds = EXCLUDED.kinds, hostname = EXCLUDED.hostname, version = EXCLUDED.version,
    lease_duration = EXCLUDED.lease_duration, started_at = now(), heartbeat_at = now()`

// $1 worker ID.
//
// checkInWorkerSQL records a worker's check-in. The column heartbeat_at
// holds the worker's last check-in. It keeps its name: renaming it needs a
// migration, and the workers of an older release would fail on the renamed
// column during a rolling deploy.
const checkInWorkerSQL = `UPDATE {s}.workers SET heartbeat_at = now() WHERE id = $1`

// $1 worker ID.
//
// deleteWorkerSQL unregisters worker $1 when it stops.
const deleteWorkerSQL = `DELETE FROM {s}.workers WHERE id = $1`

// $1 worker ID, $2 lease in microseconds.
//
// electMaintainerSQL takes or renews the maintainer's lease for $2
// microseconds. It returns a row only if $1 is the maintainer. It proposes
// its row only if no other worker's lease lasts, so that a failed try writes
// nothing. Another worker's lease lasts only if it ends within two leases
// from now: a later one is no worker's, but a row written by hand, and
// obeyed it would keep every worker from leading. Why two leases:
// https://github.com/code19m/taskmill/blob/main/docs/design/schema-rows.md.
const electMaintainerSQL = `
INSERT INTO {s}.leader AS l (name, worker_id, elected_at, expires_at)
SELECT 'maintainer', $1, now(), now() + $2 * interval '1 microsecond'
WHERE NOT EXISTS (SELECT FROM {s}.leader WHERE name = 'maintainer' AND worker_id <> $1
                  AND expires_at BETWEEN now() AND now() + $2 * interval '2 microseconds')
ON CONFLICT (name) DO UPDATE
SET worker_id = EXCLUDED.worker_id,
    elected_at = CASE WHEN l.worker_id = EXCLUDED.worker_id THEN l.elected_at ELSE now() END,
    expires_at = EXCLUDED.expires_at
WHERE l.worker_id = EXCLUDED.worker_id
   OR l.expires_at NOT BETWEEN now() AND now() + $2 * interval '2 microseconds'
RETURNING worker_id`

// $1 worker ID, $2 channel.
//
// resignMaintainerSQL gives up $1's lease, and tells the workers on channel
// $2, so that another takes over right away.
const resignMaintainerSQL = `
WITH resigned AS (
    DELETE FROM {s}.leader WHERE name = 'maintainer' AND worker_id = $1 RETURNING 1
)
SELECT pg_notify($2, '{resigned}') FROM resigned`

// $1 channel.
//
// promoteDueSQL makes a batch of due scheduled and retryable tasks
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
const promoteDueSQL = `
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
SELECT (SELECT count(*) FROM promoted) FROM notified`

// nextDueSQL returns the seconds until the next scheduled or retryable task
// is due, negative if one is due, or NULL if there is none.
const nextDueSQL = `
SELECT extract(epoch FROM min(run_at) - now()) FROM {s}.tasks WHERE state IN ('scheduled', 'retryable')`

// $1 channel, $2 longest wait in microseconds, $3 send the heartbeat.
//
// wakeQueuesSQL notifies, on channel $1, every queue that is not paused
// and whose next task waited longer than $2 microseconds, then, if $3,
// sends the maintainer's heartbeat, which shows workers that this ran
// and that notifications reach them. Its queues and their next tasks
// are found with one index probe each. It reads the woken CTE, so that
// it runs.
var wakeQueuesSQL = `
WITH RECURSIVE ` + queuesWith(`available`, `= 'available'`) + `, woken AS (
    SELECT count(pg_notify($1, '{available}' || x.queue)) FROM available x
    WHERE x.queue IS NOT NULL
      AND (SELECT t.run_at FROM {s}.tasks t WHERE t.state = 'available' AND t.queue = x.queue
           ORDER BY t.priority DESC, t.run_at, t.id LIMIT 1) < now() - $2 * interval '1 microsecond'
      AND NOT EXISTS (SELECT FROM {s}.queues q WHERE q.name = x.queue AND q.paused_at IS NOT NULL)
)
SELECT CASE WHEN $3 THEN pg_notify($1, '{heartbeat}') END FROM woken`

// $1 backoff base in seconds, $2 backoff maximum in seconds, $3 error message.
//
// rescueSQL fails the attempts whose leases expired: their workers
// crashed or lost the database. $1 and $2 are the default backoff's
// base and maximum in seconds; the exponent is capped so that tasks
// with thousands of attempts cannot overflow it. It locks a grouped
// task's group first, skipping groups that other transactions hold,
// such as one that enqueues into the group: it never waits for them,
// and a later run rescues those tasks.
var rescueSQL = `
WITH expired AS (
    SELECT t.id FROM {s}.tasks t
    WHERE t.state = 'running' AND t.lease_expires_at < now() AND ` + groupFreeSQL + `
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
    errors = ` + errorsTrimmedSQL + ` || jsonb_build_array(jsonb_build_object('attempt', t.attempt, 'at', ` + nowRFC3339SQL + `,
                 'message', $3::text, 'code', 'lease_lost'))
FROM expired
WHERE t.id = expired.id
RETURNING t.id, t.queue, coalesce(t.group_key, ''), t.state::text, t.kind, t.attempt`

// deleteFinishedSQL deletes dead and canceled tasks older than the dead
// retention, except the dead heads that keep their groups halted. It
// locks the rows it picks and skips locked ones, so a task that an
// operator is retrying is never deleted.
//
// The index of finished tasks leads with the queue, so old tasks are
// a range of it only within one queue. The statement finds the
// queues with one index probe each, as countStats does, and reads
// each queue's range, up to a batch: that limit lets the planner read
// the range in order and stop, not collect all of it first. With a
// filter by age alone, the planner, which cannot know the cutoff that
// the statement reads from the settings, expects old tasks everywhere,
// and scans the whole table for them.
//
// A dead task halts only its own group, so its halt is found by the
// group's key, with one lookup per old task. OFFSET 0 keeps it a
// lookup: joined to the groups instead, the statement can read every
// halted group for each queue, or every old task before it deletes
// the first.
var deleteFinishedSQL = `
WITH RECURSIVE ` + queuesWith(`finished`, `IN ('dead', 'canceled')`) + `, old AS (
    SELECT o.id FROM finished x
    CROSS JOIN LATERAL (
        SELECT t.id FROM {s}.tasks t
        WHERE t.queue = x.queue AND t.state IN ('dead', 'canceled')
          AND t.finalized_at < (` + nowUTCSQL + ` - (SELECT dead_retention FROM {s}.settings)) AT TIME ZONE 'UTC'
          AND NOT EXISTS (SELECT FROM {s}.groups g
                          WHERE g.queue = t.queue AND g.group_key = t.group_key AND g.halted_task_id = t.id
                          OFFSET 0)
        LIMIT {batch}
        FOR UPDATE SKIP LOCKED
    ) o
    LIMIT {batch}
)
DELETE FROM {s}.tasks t USING old WHERE t.id = old.id`

// deleteDedupeKeysSQL deletes expired dedupe keys, skipping keys that an
// enqueue is claiming again.
const deleteDedupeKeysSQL = `
WITH expired AS (
    SELECT key FROM {s}.dedupe_keys WHERE expires_at < now() LIMIT {batch} FOR UPDATE SKIP LOCKED
)
DELETE FROM {s}.dedupe_keys d USING expired WHERE d.key = expired.key`

// $1 seconds without a check-in.
//
// deleteWorkersSQL deletes the workers that stopped without
// unregistering: those that made no check-in for $1 seconds, or for
// twice their lease if that is longer. A worker checks in every third
// of its lease, so one with a long lease is silent for long while it
// is healthy. A worker of a release that stores no lease has only
// the $1 seconds. The table refuses a lease of more than 300 years,
// and one with a negative field, so a time can always be moved back
// by twice a lease.
const deleteWorkersSQL = `
DELETE FROM {s}.workers WHERE heartbeat_at < now() - greatest($1 * interval '1 second', 2 * lease_duration)`

// $1 age in seconds.
//
// deleteActivitySQL deletes a batch of the minutes of activity older than $1
// seconds, skipping locked rows.
const deleteActivitySQL = `
WITH old AS (
    SELECT minute, queue FROM {s}.activity WHERE minute < now() - $1 * interval '1 second'
    LIMIT {batch} FOR UPDATE SKIP LOCKED
)
DELETE FROM {s}.activity a USING old WHERE a.minute = old.minute AND a.queue = old.queue`

// $1 most partitions, $2 the comment that marks a detached partition to drop.
//
// expiredHistorySQL lists the history partitions whose whole UTC day is
// older than the history retention, by the database's clock. Each comes with
// whether it is still attached, and whether its detach was interrupted,
// which must be finalized before it can be dropped. It also lists the
// detached tables with the comment $2, which a pass could not drop. An
// interrupted detach comes first, then the attached partitions, the oldest
// first, and the detached tables last, in random order: the maintenance page
// says why that order cannot get stuck. The pattern keeps to names that are
// safe in the text of a statement.
const expiredHistorySQL = `
SELECT c.relname, i.inhrelid IS NOT NULL, coalesce(i.inhdetachpending, false)
FROM pg_class c
LEFT JOIN pg_inherits i ON i.inhrelid = c.oid AND i.inhparent = '{s}.task_history'::regclass
WHERE c.relnamespace = '{s}'::regnamespace
  AND c.relkind = 'r'
  AND (i.inhrelid IS NOT NULL OR NOT c.relispartition AND obj_description(c.oid, 'pg_class') = $2)
  AND c.relname ~ '^task_history_[0-9]{8}$'
  AND c.relname < 'task_history_' || to_char(` + nowUTCSQL + ` - (SELECT history_retention FROM {s}.settings), 'YYYYMMDD')
ORDER BY coalesce(i.inhdetachpending, false) DESC, i.inhrelid IS NULL,
         CASE WHEN i.inhrelid IS NOT NULL THEN c.relname END, random()
LIMIT $1`

// $1 the caller's schedules.
//
// pruneSchedulesSQL deletes the schedules that no maintainer has defined
// for a day: they were removed from the code. The day keeps a rolling
// deploy, a canary, or a rollback, in which some workers do not define
// a schedule, from losing its state, such as its pause.
//
// A schedule's updated_at says when a maintainer last defined it, if
// nothing changed it later: the statement sets it for the schedules
// among $1, which are the caller's. It does so only once it is an
// hour old, so that most passes write nothing. So a schedule is
// deleted 23 to 24 hours after a maintainer last defined it. The day
// is written as 24 hours: see {utc}.
//
// The statement first locks the rows that it changes, and skips the
// ones that another transaction holds, such as one that runs, pauses,
// marks, or deletes the schedule: the next pass finds what is left to
// do. So it waits for no row, and two maintainers that prune at the
// same time cannot deadlock, whatever each one defines.
const pruneSchedulesSQL = `
WITH old AS (
    SELECT name, name = ANY($1) AS defined FROM {s}.schedules
    WHERE updated_at < now() - CASE WHEN name = ANY($1) THEN interval '1 hour' ELSE interval '24 hours' END
    FOR UPDATE SKIP LOCKED
), marked AS (
    UPDATE {s}.schedules s SET updated_at = now() FROM old WHERE s.name = old.name AND old.defined
)
DELETE FROM {s}.schedules s USING old WHERE s.name = old.name AND NOT old.defined`

// $1 schedules.
//
// loadSchedulesSQL reads the stored state of the schedules $1.
const loadSchedulesSQL = `
SELECT name, queue, kind, cron, timezone, payload, next_run_at, paused_at IS NOT NULL
FROM {s}.schedules WHERE name = ANY($1)`

// $1 name, $2 queue, $3 kind, $4 cron expression, $5 time zone, $6 payload,
// $7 next run time, $8 the timing changed.
//
// upsertScheduleSQL stores a schedule's definition. Its next run time is
// replaced only when $8 says the timing changed.
const upsertScheduleSQL = `
INSERT INTO {s}.schedules AS s (name, queue, kind, cron, timezone, payload, next_run_at)
VALUES ($1, $2, $3, $4, $5, $6, $7)
ON CONFLICT (name) DO UPDATE
SET queue = EXCLUDED.queue, kind = EXCLUDED.kind, cron = EXCLUDED.cron, timezone = EXCLUDED.timezone,
    payload = EXCLUDED.payload,
    next_run_at = CASE WHEN $8 THEN EXCLUDED.next_run_at ELSE s.next_run_at END,
    updated_at = now()`

// $1 name, $2 next run time, $3 due run time.
//
// advanceScheduleSQL claims the run due at $3; it changes nothing if
// another maintainer claimed it first or the schedule was paused.
const advanceScheduleSQL = `
UPDATE {s}.schedules SET next_run_at = $2, last_run_at = now(), updated_at = now()
WHERE name = $1 AND next_run_at = $3 AND paused_at IS NULL`
