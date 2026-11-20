-- Stores, with each worker that takes part in the maintainer election, the
-- names of its schedules, sorted, and a hash of each one's definition, in
-- the same order. The maintainer compares them with its own, and logs the
-- workers whose schedules differ. Both are NULL for a worker that takes no
-- part, and for a worker of a release that does not store them: neither is
-- compared. Nullable columns without a default change only the catalog. See
-- docs/design/schema-maintenance.md.
--
-- The constraint refuses what no release writes, as workers_names_check
-- does: a NULL in a list, a list of more than one dimension, and two lists
-- of different lengths, or one without the other. Adding it reads the table
-- once: a few pages.
--
-- The migration locks only workers, which every worker writes at each
-- check-in. Pending migrations run in one transaction, so on a schema from
-- before 0007 it asks for workers while it holds tasks and the history. That
-- order is safe: no transaction holds workers while it asks for tasks or the
-- history, and a statistics refresh reads tasks and then workers.

SET LOCAL lock_timeout = '5s';

-- Lock: workers.
ALTER TABLE __SCHEMA__.workers
    ADD COLUMN schedules text[],
    ADD COLUMN schedule_hashes text[],
    ADD CONSTRAINT workers_schedules_check CHECK (
        CASE WHEN array_ndims(schedules) > 1 OR array_ndims(schedule_hashes) > 1 THEN false
             ELSE array_position(schedules, NULL) IS NULL AND array_position(schedule_hashes, NULL) IS NULL
                  AND coalesce(cardinality(schedules) = cardinality(schedule_hashes),
                               schedules IS NULL AND schedule_hashes IS NULL) END);
