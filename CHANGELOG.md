# Changelog

All notable changes to taskmill are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and taskmill uses
[Semantic Versioning](https://semver.org/) as described in
[Compatibility](docs/reference/compatibility.md).

## [Unreleased]

### Added

- `examples/quickstart`, the program that the new quickstart runs.

### Changed

- The docs are rebuilt as short pages, by what you want to do: get started,
  concepts, guides, recipes ("How do I…?"), production, and reference. They
  draw flows as diagrams, and recommend one way wherever there is a choice.
  Start at [docs/README.md](docs/README.md). The old `docs/guide.md` is
  gone.

### Fixed

- The worker no longer warns at every start that its pool has fewer
  connections than its concurrency plus 5. That check could not see which
  pool the handlers use, so it also warned when they had a pool of their
  own, as the docs advise. Now the worker warns when the pool was full and
  renewing leases had to wait for a connection, at most once a minute:
  `the pool was full, so renewing leases waited; …`.
- The docs of `BatchHandler` and `Client.Cancel` said that a task canceled
  during its batch becomes canceled. It becomes completed if the batch
  returns nil for it, because the work was done, as for other handlers.

## [0.3.0] - 2026-10-28

### Added

- `ErrConflict`, of class `conflict`: `Client.UpdateQueue` and
  `Client.UpdateSettings` return an error that wraps it when each of their
  10 tries lost to a concurrent update of the same settings. Between tries
  they pause a short random time, up to 50 ms, so that colliding updates
  spread out and nearly all land.
- `ErrTaskCanceled`, `ErrAttemptTimeout`, and `ErrShutdown`: with
  `ErrLeaseLost`, the causes that `context.Cause` gives for a handler's
  context that ended. The error classes that `ErrorType()` returns, such as
  `invalid_argument`, are now part of the API.

### Changed

- The guide's [Upgrading](docs/reference/compatibility.md) section states the
  compatibility promise from v1.0.0 on: breaking changes only in a new major
  version, deprecated names kept until then, interfaces that never grow, and
  names that never change, such as the `State` values and error classes. It
  names the `migrations` table as the one public table, and what the promise
  leaves out: log and error messages, and the `otel` module's names.
- **Breaking:** `Client.UpdateQueue` and `Client.UpdateSettings` take a
  function that changes the current settings, instead of settings that
  replace them all. A setting that your code leaves alone, or that a later
  version adds, keeps its value. They hold no lock while the function runs:
  they write the settings back only if no other update changed them
  meanwhile, and otherwise run the function again on the new settings, up to
  10 times before they fail with `ErrConflict`. So concurrent updates no
  longer lose each other's changes, and the function must only change the
  settings it gets, with no I/O. To migrate, set the fields you mean to:
  `client.UpdateQueue(ctx, name, func(s *taskmill.QueueSettings) { s.MaxRunning = 4 })`.
  Calls that pass nil still compile, but now fail with `ErrInvalidArgument`.
  Replace `UpdateQueue(ctx, name, nil)`, which removed the limit, with
  `UpdateQueue(ctx, name, func(s *taskmill.QueueSettings) { s.MaxRunning = 0 })`,
  and `UpdateSettings(ctx, nil)`, which reset retention to its defaults, with
  `UpdateSettings(ctx, func(s *taskmill.Settings) { *s = taskmill.Settings{} })`.
- **Breaking:** `Client.Groups` pages through every group, like `List`: it
  returns a `*GroupsResult` with `Groups` and a `Next` cursor, which
  `GroupParams.Cursor` takes, and `GroupParams.Halted` finds the halted
  groups. Before, it returned at most 1,000 groups. To migrate, read
  `res.Groups`, and follow `res.Next` for more. `GroupParams.Queues` takes at
  most 100 names, since each named queue reads up to a page. Each page reads
  only its own groups, also from a few queues or of halted groups only, with
  migration 3's index on the halted groups, which `Migrate` adds. Its
  documentation now says exactly what it lists: the groups with unfinished
  tasks or a dead task that halts them, which `GroupInfo.Tasks` counts too.
- **Breaking:** `QueueInfo.Counts` holds a `TaskCount` per state, as
  `Client.Count` does, with its own `Capped` flag, and `QueueInfo.Capped` is
  gone. Every count stops at 10,000 per queue and state, scheduled and
  retryable ones too, and is capped only when more tasks are left: a count
  of exactly 10,000 is not. Once the shared cap of 100,000 scheduled and
  retryable tasks is passed, only those counts are capped, in every queue,
  even at zero. The dashboard marks only the counts that stopped at their
  cap with "+"; before, every count of a queue with one capped count had
  it. To migrate, read `Counts[state].N`, and `Counts[state].Capped` for
  whether that count stopped at its cap.
- **Breaking:** `ui.New(client, cfg)` replaces `ui.Handler`, and returns an
  error that wraps `taskmill.ErrInvalidArgument` instead of panicking on a
  configuration it refuses: a nil client, a `Prefix` without a leading slash,
  an empty `BasicAuth` username or password, or a bad `FrameAncestors` entry.
  To migrate, check the error: `dashboard, err := ui.New(client, cfg)`.
- **Breaking:** attempt middleware that returns nil without calling `next`
  fails the attempt, which is retried with backoff. Before, the task was
  completed although its handler never ran. Middleware that skips a task on
  purpose returns an error, such as `taskmill.Snooze(d)` or
  `taskmill.Permanent(err)`. Such a failure, and an enqueue middleware's that
  returns nil without calling `next`, now reaches the middleware outside it
  as an error from its `next`, of the class `middleware`, so tracing and
  metrics record it.
- **Breaking:** limits on one enqueue call, checked before any statement, so
  that the caller's transaction stays usable: at most 64 KiB of encoded
  metadata per task, after enqueue middleware, and at most 10,000 tasks and
  64 MiB of payload and metadata per call. They apply to `EnqueueMany`,
  `EnqueueManyTx`, and `outbox.Store`, and to the metadata of schedules,
  which `NewWorker` checks. Calls beyond them fail with `ErrInvalidArgument`.
  To migrate, split larger calls into chunks within both limits, and keep
  each task's metadata within 64 KiB. Sizes now count `&`, `<`, and `>` as
  one byte each, as stored, not as JSON escapes.
- **Breaking:** `CodeLeaseExpired` is now `CodeLeaseLost`, with the value
  `lease_lost`, like `ErrLeaseLost`. Error history entries recorded before
  keep `lease_expired`: code that reads them checks for both.
- **Breaking:** `NewWorker` rejects a `Schedule` whose `Options` set `RunAt`,
  `DedupeKey`, or `DedupeWindow`, with `ErrInvalidArgument`, instead of
  silently replacing them: the schedule sets them itself. Remove them from the
  options.
- `Worker.Run` on a running worker fails with `ErrInvalidState`, and
  `NewClient` with a pool that does not use READ COMMITTED with
  `ErrInvalidArgument`. Code that matched their messages can use `errors.Is`.
- `Worker.Run` returns nil when its context ends before the worker
  registered, as when a process is stopped while it starts, instead of a
  "register worker" error.
- **Breaking:** `outbox.Outbox.AddRelay` takes `*outbox.RelayOptions`, whose
  `BatchSize` and `Timeout` set the most deliveries per `Publish` call and
  the time each call has, as `HandlerOptions` do for other batch handlers.
  nil or zero values keep the defaults, 100 deliveries and 1 minute, and
  values out of range make `NewWorker` fail with `ErrInvalidArgument`. To
  migrate, pass nil: `ob.AddRelay(&handlers, publisher, nil)`.
- An idle queue polls once a minute instead of every `PollInterval` while
  the maintainer's heartbeats reach its worker. Every second, the maintainer
  notifies each queue whose next task has waited longer than a second, which
  finds tasks whose notification came before their transaction committed or
  never came, and every 5 seconds it sends a heartbeat with it. A worker
  polls every `PollInterval` until its first heartbeat, and again once none
  came for 15 seconds, starting at once: no maintainer may run, an older one
  may during an upgrade, or notifications may not reach the worker. It then
  replaces its notification connection if that does not answer, since a
  connection can die without a word. So such a task starts within about 2
  seconds while heartbeats arrive, within about 16 seconds if the maintainer
  or the notification connection fails without a word, and within
  `PollInterval` while no maintainer of this version runs. A queue whose
  installation-wide concurrency limit holds tasks back still polls every
  `PollInterval`. 50 idle worker processes serving 100 queues sent the
  database about 5,600 transactions a second, and now send about 150 on
  average, and 460 in the busiest 10 seconds. A worker logs when
  heartbeats start or stop arriving, and gives up opening its notification
  connection after 10 seconds, to try again.
- A maintainer that stops hands over at once: it tells the workers, and
  another takes over and sends its heartbeat. One that crashes is still
  replaced within about 12 seconds.

### Removed

- **Breaking:** `ClientConfig.ContextMetadata`, deprecated in v0.2.0. Use
  enqueue middleware, as the v0.2.0 entry below shows.

### Fixed

- A bulk action whose context ended during a batch closed that batch's
  connection, and so left the batch's transaction running on the server,
  holding up to 1,000 tasks and their FIFO groups, until its statement
  ended. A run that went on at once skipped those tasks as held by other
  work. The batch now finishes, for at most 5 seconds, before the action
  returns, and counts what it did.
- A bulk action whose context ended, or whose database call failed, while it
  read the newest task ID returned no `BulkResult.Next`, so going on started
  over from the first task. It now returns `Next` whenever it stops early.
- The module no longer includes a 20 MB binary of the example shop, which
  v0.2.0 shipped by mistake and every download of it carried.
- A worker no longer listens through a proxy that pools transactions, such
  as PgBouncer in transaction mode when `ListenConfig` is not set. `LISTEN`
  stayed on a server connection that went back to the proxy's pool, so
  notifications reached no worker, and PgBouncer logged a warning for each.
  The worker now finds that out when it starts, logs `notifications
  disabled; polling only` at Error, and polls every `PollInterval`.

## [0.2.0] - 2026-10-27

### Added

- Bulk actions by filter: `Client.RetryMatching`, `CancelMatching`, and
  `DeleteMatching` act on every task that matches the queues, kinds, states,
  and FIFO groups of a `TaskFilter`, oldest first, in short batches that skip
  rows other transactions hold. They take `BulkParams`, a filter and a
  cursor, and their `BulkResult` says how many tasks they changed and
  skipped, and, if they stopped early, gives the cursor to go on.
  `Client.Count` counts the matching tasks per state, and those each bulk
  action would change. `State.CanRetry`, `CanCancel`, and `CanDelete` say
  which actions a state allows.
- The dashboard's task list offers the bulk actions when filters are set.
  Each leads to a page that shows the filters and the number of tasks it
  changes, and does it.
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
- A guide section, [Running on Kubernetes](docs/production/kubernetes.md):
  shutdown and probes, Deployments for workers and `taskmill-ui`,
  migrations, connection budgets, PgBouncer, autoscaling with KEDA, and
  managed PostgreSQL.

### Changed

- **Breaking:** `ListParams` holds its filters in a `Filter TaskFilter`
  field, which the bulk actions and `Count` take too, instead of its own
  `Queues`, `States`, `Kinds`, and `GroupKeys` fields. To migrate, move them
  into `Filter`: `&taskmill.ListParams{Filter: taskmill.TaskFilter{Queues:
  q}, Limit: 100}`.
- Logs follow one level policy: Error when a person must act, Warn when
  taskmill recovers on its own, Info for lifecycle, Debug for routine events
  of single tasks. A dead task now logs at Error, a retry at Debug, and a
  failed dashboard request at Warn. The [log catalog](docs/reference/log-messages.md)
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

[Unreleased]: https://github.com/code19m/taskmill/compare/v0.3.0...HEAD
[0.3.0]: https://github.com/code19m/taskmill/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/code19m/taskmill/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/code19m/taskmill/releases/tag/v0.1.0
