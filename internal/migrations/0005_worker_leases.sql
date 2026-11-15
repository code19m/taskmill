-- Two changes to the catalog: neither statement reads or writes a row. Each
-- takes an ACCESS EXCLUSIVE lock on its table, waits at most 5 seconds for
-- it, and holds it until the migration commits, a moment later. See
-- docs/design/schema.md.
--
-- The order of the locks is safe beside a running fleet. Two lock orders can
-- deadlock only if a transaction holds one of the tables while it asks for
-- the other, and nothing in taskmill touches both schedules and workers: a
-- run of a schedule writes schedules and then the tables of its task, a
-- statistics refresh reads tasks and then workers, and every other
-- statement on either table is alone in its transaction. The migration
-- locks neither tasks nor any other table of those transactions. It locks
-- workers last, because every worker writes that table at each check-in:
-- while the migration waits for schedules, it holds no lock.

SET LOCAL lock_timeout = '5s';

-- Drops an index that no statement reads: each one finds its schedules by
-- name. The index made every run of a schedule write an index entry. An
-- operator may have dropped it already, hence IF EXISTS.
-- Lock: schedules, which only the maintainer and the dashboard use.
DROP INDEX IF EXISTS __SCHEMA__.schedules_due_idx;

-- Stores each worker's LeaseDuration with its row. A worker checks in every
-- third of it, so the dashboard and the maintainer judge by it how long the
-- worker may be silent. NULL for a worker of a release that does not store
-- it, which they judge by fixed times, as before. A nullable column without
-- a default changes only the catalog.
-- Lock: workers.
ALTER TABLE __SCHEMA__.workers ADD COLUMN lease_duration interval;
