-- Refuses, in three small tables, the values that no release writes and that
-- the code cannot read: a time that is infinite, a list of names with a NULL
-- or with more than one dimension, and a period outside those that the API
-- accepts. Written by hand, such a value failed where it was read: a page of
-- the dashboard, every statistics refresh, every schedule. A negative
-- retention was obeyed, and deleted dead tasks. Now it fails where it is
-- written, with the name of its constraint. See docs/design/schema-rows.md.
--
-- A period, the type interval, needs two rules. PostgreSQL stores its
-- months, its days, and the rest, in microseconds, apart, each with a sign of
-- its own, and compares two periods by their totals, with months of 30 days.
-- So a range alone accepts '-3000000 mons 90000000 days 1 hour' as 1 hour.
-- But on the calendar, a month is not 30 days: a time moved back by that
-- period is 3,600 years ahead, and a retention of it deletes what just
-- finished. So no field of a period may be negative either. date_trunc
-- keeps the larger fields of a period, so the two lines of that rule say:
-- its months are not negative, and it does not get shorter with its days,
-- nor with the rest. A time that is moved back by such a period is never a
-- later time.
--
-- Each table gets the same three steps:
--
--   1. LOCK TABLE takes the ACCESS EXCLUSIVE lock that ADD CONSTRAINT needs,
--      waits at most 5 seconds for it, and holds it until the migration
--      commits, a moment later. Taking it first keeps a writer from adding a
--      bad row between the repair and the constraint. It also keeps the
--      migration from asking for a stronger lock on a table whose rows it
--      has changed: a writer that waits for one of those rows holds the
--      table against that lock, and the two deadlock.
--   2. The repair changes the rows that break the new rule, which takes no
--      further lock.
--   3. ADD CONSTRAINT reads the table once, to validate every row of it: a
--      few pages, since these tables are small.
--
-- The order of the locks is safe beside a running fleet: settings, then
-- schedules, then workers. Two lock orders can deadlock only if a transaction
-- holds one of the tables while it asks for another. No statement of
-- taskmill names two of the three. Of its transactions with several
-- statements, a run of a schedule writes schedules, a statistics refresh
-- reads workers, and neither touches another of the three. The migration
-- locks workers last, because every worker writes that table at each
-- check-in.
--
-- Pending migrations run in one transaction, so after 0005 this one asks for
-- settings while it holds schedules, tasks, and workers. One statement reads
-- settings beside another of those: retention's delete of old tasks. It
-- locks tasks first. So either it waits there for the migration, and holds
-- nothing, or the migration waits there for it, before it locks settings.

SET LOCAL lock_timeout = '5s';

-- settings: each retention period from 1 hour, the least that
-- UpdateSettings accepts, to 300 years, which no Go duration reaches, and
-- without a negative field. A period outside the range becomes the nearest
-- one inside: a negative one, which deleted every dead task, becomes 1 hour.
-- A period inside it with a negative field becomes the same period without
-- one, as justify_interval writes it: '7 days -1 hour' becomes '6 days 23
-- hours'. Each statement repairs one of the two periods, so that a good
-- period beside a bad one stays as it is stored.
-- Lock: settings, which retention reads once a minute.
LOCK TABLE __SCHEMA__.settings IN ACCESS EXCLUSIVE MODE;
UPDATE __SCHEMA__.settings
SET history_retention = CASE WHEN history_retention < interval '1 hour' THEN interval '1 hour'
                             WHEN history_retention > interval '300 years' THEN interval '300 years'
                             ELSE justify_interval(history_retention) END,
    updated_at = now()
WHERE NOT (history_retention BETWEEN interval '1 hour' AND interval '300 years'
           AND date_trunc('month', history_retention) >= interval '0'
           AND date_trunc('day', history_retention) BETWEEN date_trunc('month', history_retention) AND history_retention);
UPDATE __SCHEMA__.settings
SET dead_retention = CASE WHEN dead_retention < interval '1 hour' THEN interval '1 hour'
                          WHEN dead_retention > interval '300 years' THEN interval '300 years'
                          ELSE justify_interval(dead_retention) END,
    updated_at = now()
WHERE NOT (dead_retention BETWEEN interval '1 hour' AND interval '300 years'
           AND date_trunc('month', dead_retention) >= interval '0'
           AND date_trunc('day', dead_retention) BETWEEN date_trunc('month', dead_retention) AND dead_retention);
ALTER TABLE __SCHEMA__.settings ADD CONSTRAINT settings_retention_check CHECK (
    history_retention BETWEEN interval '1 hour' AND interval '300 years'
    AND date_trunc('month', history_retention) >= interval '0'
    AND date_trunc('day', history_retention) BETWEEN date_trunc('month', history_retention) AND history_retention
    AND dead_retention BETWEEN interval '1 hour' AND interval '300 years'
    AND date_trunc('month', dead_retention) >= interval '0'
    AND date_trunc('day', dead_retention) BETWEEN date_trunc('month', dead_retention) AND dead_retention);

-- schedules: finite times. An infinite next run time failed the read of
-- every schedule, and SQL cannot compute the next run from the schedule's
-- expression. So such a schedule becomes a paused one that is due: the
-- dashboard shows it, and ResumeSchedule makes it run. An infinite last run
-- time becomes none, as for a schedule that never ran: a CHECK accepts a
-- NULL.
-- Lock: schedules, which only the maintainer and the dashboard use.
LOCK TABLE __SCHEMA__.schedules IN ACCESS EXCLUSIVE MODE;
UPDATE __SCHEMA__.schedules SET last_run_at = NULL, updated_at = now() WHERE NOT isfinite(last_run_at);
UPDATE __SCHEMA__.schedules
SET next_run_at = now(), paused_at = coalesce(paused_at, now()), updated_at = now()
WHERE NOT isfinite(next_run_at);
ALTER TABLE __SCHEMA__.schedules ADD CONSTRAINT schedules_times_check
    CHECK (isfinite(next_run_at) AND isfinite(last_run_at));

-- workers: finite times, names without a NULL in a list of one dimension,
-- and a lease from zero to 300 years without a negative field, or none, as
-- a worker of a release without the column leaves it. A row that breaks the
-- rule is deleted: it was no worker's, or its worker registers again at its
-- next check-in. array_position fails on a list of several dimensions, so
-- the CASE answers for such a list first.
-- Lock: workers.
LOCK TABLE __SCHEMA__.workers IN ACCESS EXCLUSIVE MODE;
DELETE FROM __SCHEMA__.workers
WHERE NOT (isfinite(started_at) AND isfinite(heartbeat_at))
   OR NOT (lease_duration BETWEEN interval '0' AND interval '300 years'
           AND date_trunc('month', lease_duration) >= interval '0'
           AND date_trunc('day', lease_duration) BETWEEN date_trunc('month', lease_duration) AND lease_duration)
   OR CASE WHEN array_ndims(queues) > 1 OR array_ndims(kinds) > 1 THEN true
           ELSE array_position(queues, NULL) IS NOT NULL OR array_position(kinds, NULL) IS NOT NULL END;
ALTER TABLE __SCHEMA__.workers
    ADD CONSTRAINT workers_times_check CHECK (isfinite(started_at) AND isfinite(heartbeat_at)),
    ADD CONSTRAINT workers_lease_check CHECK (
        lease_duration BETWEEN interval '0' AND interval '300 years'
        AND date_trunc('month', lease_duration) >= interval '0'
        AND date_trunc('day', lease_duration) BETWEEN date_trunc('month', lease_duration) AND lease_duration),
    ADD CONSTRAINT workers_names_check CHECK (
        CASE WHEN array_ndims(queues) > 1 OR array_ndims(kinds) > 1 THEN false
             ELSE array_position(queues, NULL) IS NULL AND array_position(kinds, NULL) IS NULL END);
