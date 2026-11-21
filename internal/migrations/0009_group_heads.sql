-- Indexes the head of each FIFO group: its one task that is neither pending
-- nor canceled, which the group runs, or which halts it as a dead task. The
-- page of a pending task in the dashboard shows the task that it waits
-- behind. tasks_pending_idx is the only other index with group keys, so
-- that read looked at every task of the queue: 70 to 120 ms for a queue of
-- 126,000 tasks, on every update of the page. With this index it reads the
-- head alone. See docs/design/schema.md.
--
-- Only grouped tasks have entries: one when a task becomes the head, and
-- one when a fetch takes it, about 170 bytes of WAL per grouped task. An
-- ungrouped task writes none, and a renewal of a lease writes none either:
-- the index holds no column that it changes.
--
-- The index is built inside the migration's transaction, with a plain
-- CREATE INDEX: reads continue, and every write to tasks waits while the
-- table is read, about a tenth of a second for each million tasks.
--
-- The migration locks only tasks, in the SHARE mode of CREATE INDEX. A batch
-- of DeleteMatching that holds rows of tasks waits for it, and holds nothing
-- that it asks for. Pending migrations run in one transaction, so on a
-- schema from before 0008 it asks for tasks while it holds workers. That
-- order is safe: no transaction writes tasks and then asks for workers.

SET LOCAL lock_timeout = '5s';

-- Lock: tasks, against writes.
CREATE INDEX tasks_group_head_idx ON __SCHEMA__.tasks (queue, group_key)
    WHERE group_key IS NOT NULL AND state NOT IN ('pending', 'canceled');
