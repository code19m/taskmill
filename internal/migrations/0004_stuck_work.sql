-- Stores, with the counts of queue_stats, two signs of work that is stuck
-- while the queue's lag stays near zero: on each dead count, how many FIFO
-- groups a dead task halts; on each retryable count, the most attempts that
-- one of those tasks has failed. See docs/design/schema.md and ADR 0007.
-- Nullable columns without a default change only the catalog; the lock
-- timeout keeps a migration that waits behind the maintainer from holding up
-- other work on the table.

SET LOCAL lock_timeout = '5s';

ALTER TABLE __SCHEMA__.queue_stats ADD COLUMN halted_groups bigint, ADD COLUMN failed_attempts integer;
