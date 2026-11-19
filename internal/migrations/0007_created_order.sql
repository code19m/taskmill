-- Indexes every task by its queue and by when it was created, so that
-- Client.List reads a range of creation times in the order of that time: a
-- page of a queue reads its own tasks, wherever the range lies and however
-- long it is. No other index holds tasks by creation time, and IDs follow it
-- only roughly: a task of a long transaction has an old creation time and a
-- new ID. See docs/design/schema-creation-order.md.
--
-- The index of tasks leaves out the running ones. A fetch then writes no
-- entry to it, and a list reads the running tasks, which are few, from their
-- own index. A task costs one entry when it is enqueued, and one in the
-- history when it completes.
--
-- Both indexes are built inside the migration's transaction, with a plain
-- CREATE INDEX: reads continue, and every write to the table waits while
-- the index is built, about half a second for each million tasks. The
-- history's index is built on every partition, and a partition that is
-- attached later gets it from the table, as it gets the other two.
--
-- The order of the locks is safe beside a running fleet: tasks, then the
-- history. A completion and Delete take the two in that order. A batch of
-- DeleteMatching writes the history and then tasks, but it locks rows of
-- tasks first, to pick them. So the migration takes tasks in EXCLUSIVE mode,
-- which waits for such a batch to end, where the SHARE lock of CREATE INDEX
-- would not: the batch would then wait for tasks, and the migration for the
-- history. The mode blocks nothing more that matters: only statements that
-- lock rows without writing, and no statement of taskmill does that outside
-- a transaction that writes.
--
-- Pending migrations run in one transaction. On a schema from before 0006,
-- this one asks for tasks while it holds settings, schedules, and workers,
-- and holds them while it builds: check-ins wait too. One statement takes
-- two of these the other way round: retention's delete of old tasks locks
-- tasks, and then reads settings. If the two meet, PostgreSQL ends one of
-- them, and retention, or the next Migrate, runs it again.

SET LOCAL lock_timeout = '5s';

-- Lock: tasks, against every statement but a plain read.
LOCK TABLE __SCHEMA__.tasks IN EXCLUSIVE MODE;
CREATE INDEX tasks_created_idx ON __SCHEMA__.tasks (queue, created_at, id) WHERE state <> 'running';

-- Lock: task_history and each of its partitions, against writes.
CREATE INDEX task_history_created_idx ON __SCHEMA__.task_history (queue, created_at, id);
