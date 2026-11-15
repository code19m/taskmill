-- One change to the catalog: the statement reads and writes no row. It takes
-- an ACCESS EXCLUSIVE lock on workers, waits at most 5 seconds for it, and
-- holds it until the migration commits, a moment later. See
-- docs/design/schema.md.
--
-- The lock is safe beside a running fleet: the migration locks no other
-- table, so it never waits for a table while it holds one.

SET LOCAL lock_timeout = '5s';

-- Stores each worker's LeaseDuration with its row. A worker checks in every
-- third of it, so the dashboard and the maintainer judge by it how long the
-- worker may be silent. NULL for a worker of a release that does not store
-- it, which they judge by fixed times, as before. A nullable column without
-- a default changes only the catalog.
-- Lock: workers.
ALTER TABLE __SCHEMA__.workers ADD COLUMN lease_duration interval;
