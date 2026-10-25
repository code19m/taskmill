# Changelog

All notable changes to taskmill are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and taskmill uses
[Semantic Versioning](https://semver.org/) as described in
[Upgrading](docs/guide.md#upgrading).

## [Unreleased]

### Fixed

- Through PgBouncer in transaction mode with `max_prepared_statements` set,
  `Migrate` failed on a new schema, and `NewClient` could fail right after it
  found a schema not migrated. PgBouncer keeps the name of a prepared
  statement whose parse failed, and disconnects the client that prepares it
  again ([pgbouncer#1630](https://github.com/pgbouncer/pgbouncer/issues/1630)).
  taskmill now reads the schema version without a prepared statement.
- `Migrate` called by several processes at once on a new schema could fail
  with a duplicate key on the schema's name, when their connections were
  already open, as a running application's are. A migration that waited for
  another's lock trusted its connection's stale view that the schema did not
  exist yet.
- Promoting due tasks could scan the whole `tasks` table every second, when
  many tasks were scheduled far ahead while others waited available: with 10
  million scheduled tasks, each promotion took 1.2 seconds instead of under a
  millisecond. It now stays on its index.

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
