-- Stores the number of each worker's last write to activity that added its
-- counts. A worker numbers its writes, and a write adds its counts only if
-- it raises its worker's number, in the same statement: so a write that the
-- worker tries again, because it could not tell whether the first try
-- committed, adds them once. The number commits with the counts, so it
-- holds on a standby that a failover promotes too. See
-- docs/design/schema-statistics.md.
--
-- A row belongs to one worker, under a random key of its own: a worker's
-- ID, which only 3 random bytes keep apart from an earlier worker's on the
-- same host, would refuse the writes of a worker that got the ID of one
-- from the last day. It is updated with each of the worker's writes, every
-- 10 seconds at most. Retention deletes the rows that no write updated for
-- a day.
--
-- The migration creates a table, and locks no other.

SET LOCAL lock_timeout = '5s';

CREATE TABLE __SCHEMA__.activity_writes (
    writer     uuid PRIMARY KEY,
    last_write bigint NOT NULL,
    written_at timestamptz NOT NULL DEFAULT now()
) WITH (fillfactor = 50);
