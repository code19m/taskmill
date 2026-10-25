# Changelog

All notable changes to taskmill are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and taskmill uses
[Semantic Versioning](https://semver.org/) as described in
[Upgrading](docs/guide.md#upgrading).

## [Unreleased]

### Added

- Bulk actions by filter: `Client.RetryMatching`, `CancelMatching`, and
  `DeleteMatching` act on every task that matches the queues, kinds, states,
  and FIFO groups of a `TaskFilter`, in short batches that skip rows other
  transactions hold, and return how many tasks they changed. `Client.Count`
  counts the matching tasks per state. The dashboard's task list offers them
  when filters are set, with the number of tasks each one changes.

## [0.1.0] - 2026-10-25

Initial release.

### Added

- Transactional enqueue, alone or in batches, inside your own pgx or
  `database/sql` transaction, with priorities, delays, deduplication, and
  metadata.
- Workers with typed handlers, batch handlers, middleware, retries with
  backoff, timeouts, snoozing, cancellation, and graceful shutdown. Dead
  tasks are kept for inspection and retry.
- FIFO groups that run tasks one at a time per key, and halt on a dead task.
- Concurrency limits per worker and across all workers, and pausing of
  queues.
- Cron schedules, enqueued at most once per scheduled time across workers,
  which operators can also run on demand.
- Transactional completion, for exactly-once effects in the same database.
- An operations API, and a live web dashboard: the `ui` package, an
  `http.Handler` with optional basic authentication that can also be shown in
  another application's frame, and the `taskmill-ui` command that serves it on
  its own.
- A day of activity history per minute: task counts and completed and failed
  attempts, from `Client.Activity` and in the dashboard's charts.
- The `outbox` package, a transactional outbox that publishes in order per
  key, and the `taskmilltest` package, helpers to test your code against a
  real PostgreSQL.

[Unreleased]: https://github.com/code19m/taskmill/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/code19m/taskmill/releases/tag/v0.1.0
