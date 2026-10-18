# Changelog

All notable changes to taskmill are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and taskmill uses
[Semantic Versioning](https://semver.org/): before v1.0.0, a minor release may
change the API, and its notes say how to migrate.

## [Unreleased]

The first release, to be tagged v0.1.0.

### Added

- Transactional enqueue of single tasks and batches, through pgx or
  `database/sql`, with priorities, run times, deadlines, attempt limits, and
  metadata.
- Deduplicated enqueue within a time window, safe against concurrent
  producers.
- FIFO groups that run one task at a time per key, for any number of keys,
  and can halt when a task becomes dead.
- Workers with typed handlers, batch handlers, middleware, per-process and
  cluster-wide concurrency limits, notifications with polling as a fallback,
  heartbeats, lease-loss detection, and graceful drain.
- Retries with exponential backoff and jitter, permanent errors, snoozing,
  timeouts, panic recovery, and a dead-letter queue with the error of every
  attempt.
- Transactional completion, for exactly-once effects in the same database.
- An elected maintainer that promotes due tasks, rescues expired leases,
  expires deadlines, runs cron schedules, refreshes statistics, and applies
  retention.
- Cron schedules, exactly once per scheduled time across workers.
- An inspection and operations API: listing, retrying, canceling, and
  deleting tasks; queue settings and pausing; retention settings; workers and
  queue statistics.
- The `outbox` package: a transactional outbox for message brokers, in order
  per key.
- The `ui` package: a web interface as an `http.Handler`.
- Versioned migrations, with the SQL exported for external migration tools.

[Unreleased]: https://github.com/code19m/taskmill/commits/main
