-- Three changes to the catalog: no statement reads or writes a row. Each
-- takes an ACCESS EXCLUSIVE lock on its table, waits for it at most 5
-- seconds, or 1 second from tasks on, and holds it until the migration
-- commits, a moment later. See docs/design/schema.md.
--
-- The order of the locks is safe beside a running fleet: schedules, then
-- tasks, then workers. Two lock orders can deadlock only if a transaction
-- holds one of the tables while it asks for another. A run of a schedule
-- writes schedules and then tasks, and a statistics refresh reads tasks and
-- then workers: the migration takes each pair in their order. Nothing in
-- taskmill touches both schedules and workers, and every other statement on
-- one of the three is alone in its transaction, or takes no other of the
-- three after it. The migration locks workers last, because every worker
-- writes that table at each check-in.

SET LOCAL lock_timeout = '5s';

-- Drops an index that no statement reads: each one finds its schedules by
-- name. The index made every run of a schedule write an index entry. An
-- operator may have dropped it already, hence IF EXISTS.
-- Lock: schedules, which only the maintainer and the dashboard use.
DROP INDEX IF EXISTS __SCHEMA__.schedules_due_idx;

-- From here on, the migration waits at most 1 second for a lock. Every
-- statement on a table queues behind a migration that waits for that table,
-- and tasks is the table that every fetch, enqueue, and completion uses:
-- with three processes that migrated in turn behind one long reader of
-- tasks, inserts and check-ins waited 15 of 15.1 seconds. Once the
-- migration holds tasks, its wait for workers holds those statements up
-- too, so that wait is as short. A migration that gives up changes nothing,
-- and the next try runs it again.
SET LOCAL lock_timeout = '1s';

-- Stores, with a canceled task, the dedupe key that its cancel freed, as
-- dedupe_keys stores it, and the end of the key's window, so that a retry
-- of the task takes the key back. NULL for every task whose cancel freed no
-- key, and after the retry. Nullable columns without a default change only
-- the catalog.
-- Lock: tasks.
ALTER TABLE __SCHEMA__.tasks ADD COLUMN freed_key bytea, ADD COLUMN freed_key_expires_at timestamptz;

-- Stores each worker's LeaseDuration with its row. A worker checks in every
-- third of it, so the dashboard and the maintainer judge by it how long the
-- worker may be silent. NULL for a worker of a release that does not store
-- it, which they judge by fixed times, as before. A nullable column without
-- a default changes only the catalog.
-- Lock: workers.
ALTER TABLE __SCHEMA__.workers ADD COLUMN lease_duration interval;
