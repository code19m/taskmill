-- Stores, with each count of queue_stats, the run time of the oldest due task
-- that it counted, so that Client.Queues reads a queue's lag instead of
-- measuring it. See docs/design/schema.md. A nullable column without a
-- default changes only the catalog; the lock timeout keeps a migration that
-- waits behind the maintainer from holding up other work on the table.

SET LOCAL lock_timeout = '5s';

ALTER TABLE __SCHEMA__.queue_stats ADD COLUMN oldest_due_at timestamptz;
