# Changelog

All notable changes to taskmill are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and taskmill uses
[Semantic Versioning](https://semver.org/) as described in
[Upgrading](docs/guide.md#upgrading).

## [Unreleased]

### Added

- `ClientConfig.EnqueueMiddleware`, which wraps every enqueue, for tracing
  and metrics: it sees the tasks' queues and kinds, may add to their
  metadata, and sees their results.
- `QueueInfo.Lag`: how long a queue's oldest due task has waited since its
  run time. The maintainer finds that task when it counts, in migration 2's
  new column, which `Migrate` adds, and `Client.Queues` measures its age.
- `outbox.Delivery.Metadata`: the metadata stored with a message, such as
  its trace context, for the publisher.
- taskmill's errors name their class, such as `invalid_argument` or
  `timeout`, which OpenTelemetry reports as `error.type`.
- The `github.com/code19m/taskmill/otel` module, released on its own: traces
  and metrics through OpenTelemetry. See its [changelog](otel/CHANGELOG.md).
- Releases publish `taskmill-ui` binaries for Linux, macOS, and Windows, and
  a container image, `ghcr.io/code19m/taskmill-ui`, for amd64 and arm64, with
  checksums, SBOMs, and build provenance attestations.
- `taskmill-ui` logs its version when it starts.
- The shop example, `examples/shop`: a small shop that uses most features,
  with the dashboard. Run it with `go run ./examples/shop`.
- `Worker.Health`, for readiness probes: nil while `Run` runs and its last
  heartbeat reached the database within `LeaseDuration`. It reads only
  memory.
- A guide section, [Running on Kubernetes](docs/guide.md#running-on-kubernetes):
  shutdown and probes, Deployments for workers and `taskmill-ui`,
  migrations, connection budgets, PgBouncer, autoscaling with KEDA, and
  managed PostgreSQL.

### Changed

- Logs follow one level policy: Error when a person must act, Warn when
  taskmill recovers on its own, Info for lifecycle, Debug for routine events
  of single tasks. A dead task now logs at Error, a retry at Debug, and a
  failed dashboard request at Warn. The [log catalog](docs/guide.md#log-catalog)
  lists every message.
- An operation that a worker repeats, such as a fetch or a heartbeat, no
  longer logs at every failed try while the database is down. It logs
  `operation failed` once, `operation still failing` at most once a minute,
  and `operation recovered` when it works again. These replace messages such
  as `fetch failed` and `maintenance step failed`.
- A task that dies because no worker has a handler for its kind, or because
  its last attempt was rescued, is now logged, and a maintainer logs when it
  loses the role.
- The maintainer counts every known queue, also one without tasks, so
  `QueueInfo.RefreshedAt` shows when it last counted an empty queue too.
- `Client.List`, `Client.Groups`, `Client.Activity`, and the schedule methods
  return `ErrInvalidArgument` for a filter, search, or name longer than any
  task or schedule can have. They returned an empty result, or `ErrNotFound`
  for a schedule.
- The 1 MiB payload limit counts each number at the length PostgreSQL writes
  it out, as it stores `1e6` as `1000000`. A short payload of numbers with
  large exponents no longer stores a value that is too large to fetch, which
  blocked its queue.

### Deprecated

- `ClientConfig.ContextMetadata`, which v0.3.0 removes. It now runs as the
  outermost enqueue middleware, once per enqueue call. Enqueue middleware does
  the same; this one keeps the metadata that the enqueue sets, as before:

  ```go
  EnqueueMiddleware: []taskmill.EnqueueMiddleware{
      func(ctx context.Context, tasks []*taskmill.EnqueueTask, next func(context.Context) error) error {
          for _, task := range tasks {
              for k, v := range contextMetadata(ctx) { // your former ContextMetadata function
                  if _, set := task.Metadata[k]; !set {
                      task.Metadata[k] = v
                  }
              }
          }
          return next(ctx)
      },
  },
  ```

### Fixed

- A worker that the maintainer removed as stale, after a pause such as its
  machine's sleep, lists itself again at its next heartbeat. It went on
  running tasks but stayed missing from `Client.Workers` and the dashboard,
  which could also mean no maintainer was shown.
- `Client.List`, `Client.Groups`, `Client.Activity`, and the schedule methods
  return `ErrInvalidArgument` for a filter, search, or name that is not valid
  UTF-8 or holds a NUL character. They returned a database error, and the
  dashboard showed a server error.
- Enqueueing returns `ErrInvalidArgument` for a payload that is not valid
  UTF-8, such as a `json.RawMessage` that holds a request body in another
  encoding, or that holds a number beyond the range of PostgreSQL's
  `numeric` type, before the query, so the caller's transaction stays
  usable. A payload that PostgreSQL itself rejects, such as a string with an
  unpaired surrogate escape, returns `ErrInvalidArgument` too. All returned
  a database error.
- `Migrate` works for a role that owns its schema but may not create schemas
  in the database. It ran `CREATE SCHEMA IF NOT EXISTS` every time, and
  PostgreSQL checks that privilege before it checks whether the schema
  exists, so the role could not install or upgrade taskmill.
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
- Recording an attempt's result and renewing leases could read the whole
  index of running tasks, which holds an entry for every task that ran since
  the last vacuum, when the statistics had been taken while few tasks ran.
  With millions of tasks waiting, a completion read about 4,000 pages instead
  of 240, and a heartbeat 8,600. They now look each task up by its ID.
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
