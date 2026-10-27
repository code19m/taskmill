-- Indexes the halted groups in the order Client.Groups pages them, so that a
-- page of halted groups reads only its own rows, however many groups run.
-- See docs/design/schema.md. Only halted groups are indexed, so the index
-- stays small; the lock timeout keeps a migration that waits behind grouped
-- enqueues from holding up other work on the table.

SET LOCAL lock_timeout = '5s';

CREATE INDEX groups_halted_idx ON __SCHEMA__.groups (queue, group_key) WHERE halted_task_id IS NOT NULL;
