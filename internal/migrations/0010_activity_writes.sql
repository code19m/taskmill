-- Stores the number of each worker's last write to activity that added its
-- counts. A worker numbers its writes, and a write adds its counts only if
-- it raises its worker's number, in the same statement: so a write that the
-- worker tries again, because it could not tell whether the first try
-- committed, adds them once. The number commits with the counts, so it
-- holds on a standby that a failover promotes too. See
-- docs/design/schema.md.
--
-- One row per worker, updated with each of its writes, every 10 seconds at
-- most. Retention deletes the rows of workers that wrote nothing for a day.
--
-- The migration creates a table, and locks no other.

SET LOCAL lock_timeout = '5s';

CREATE TABLE __SCHEMA__.activity_writes (
    worker     text PRIMARY KEY,
    last_write bigint NOT NULL,
    written_at timestamptz NOT NULL DEFAULT now()
) WITH (fillfactor = 50);
