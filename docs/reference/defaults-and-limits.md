# Defaults and limits

Every default and limit in taskmill, with its exact value. The guides round
these numbers. Each setting's godoc on
[pkg.go.dev](https://pkg.go.dev/github.com/code19m/taskmill) has the full
rules.

"Fixed" means you can't change the value.

## Tasks

| Setting | Default | Limit |
|---|---|---|
| Schema (`ClientConfig.Schema`, `MigrateConfig.Schema`) | `taskmill` | Matches `^[a-z_][a-z0-9_]{0,62}$` |
| Queue, kind, and schedule names | — | 1 to 128 characters |
| `EnqueueOptions.Queue` | `"default"` | — |
| `EnqueueOptions.Priority` | `PriorityNormal` | `PriorityLow` to `PriorityCritical` |
| `EnqueueOptions.RunAt` | Now | — |
| `EnqueueOptions.MaxAttempts` | 25 | 1 to 10,000 |
| `EnqueueOptions.GroupKey` | None | 1 to 256 characters |
| `EnqueueOptions.DedupeWindow` | 24 hours, counted from the enqueue | Not negative |
| Payload | — | 1 MiB of JSON, as PostgreSQL stores it; valid UTF-8, with no NUL character |
| Metadata | None | 64 KiB of JSON per task, after enqueue middleware |
| One enqueue call (`EnqueueMany`, `EnqueueManyTx`, `outbox.Store`) | — | 10,000 tasks, and 64 MiB of payload and metadata |
| Start of a delayed task, a retry, or a snooze | — | Within about 1 second of its run time |

An option set in the enqueue call is used first. If it isn't set, the
kind's `EnqueueDefaults` are used, and then the defaults above.

## Handlers

| Setting | Default | Limit |
|---|---|---|
| `HandlerOptions.Timeout` | 1 minute per attempt, or per batch | Not negative |
| `HandlerOptions.Retry` | `ExponentialBackoff{}`: 1 second, doubling up to 1 hour. Each wait varies at random by ±10% | — |
| Time from the first failure to dead, with the defaults | About 13 hours (25 attempts) | — |
| `HandlerOptions.BatchSize` | 100 tasks per call | 1 to 1,000 |
| `HandlerOptions.DiscardCompleted` | `false`: completed tasks go to history | — |
| Retry delay after a lost lease | The default backoff, whatever `Retry` says | Fixed |
| A task whose kind has no handler | Snoozed 1 minute at a time, without using an attempt | Dead with `unknown_kind` after 1 hour; fixed |
| Error history (`TaskInfo.Errors`) | The 20 most recent entries | 4 KiB per message; fixed |

## Workers

| Setting | Default | Limit |
|---|---|---|
| `WorkerConfig.Queues` | The default queue of each registered kind | — |
| `QueueOptions.Concurrency` | 10 tasks per queue, per worker; a batch counts as one | Not negative |
| `WorkerConfig.LeaseDuration` | 30 seconds | At least 100 ms |
| The worker's [check-in](../concepts/glossary.md#check-in): it renews its leases and reports that it is alive | Every third of `LeaseDuration`: 10 seconds | Fixed ratio |
| `WorkerConfig.PollInterval` | 1 second | Used only while the maintainer's heartbeats don't arrive |
| Idle poll while the heartbeats arrive | Once a minute per queue | Fixed |
| `WorkerConfig.DrainTimeout` | 20 seconds | Then 5 more seconds for handlers to return; fixed |
| `WorkerConfig.NoMaintainer` | `false`: the worker can become the maintainer | At least one worker must take part |
| Database connections | The pool, plus 1 for notifications | The worker warns when the pool is full and renewing leases waits `LeaseDuration` / 30 or longer (1 second by default) for a connection; at most once a minute |
| `Worker.Health` | Fails while `Run` is not running, or when the worker has not reached the database for longer than `LeaseDuration` | — |

## The maintainer

The [maintainer](../concepts/glossary.md) is one worker, chosen by
election, that does the shared work. All these timings are fixed.

| What | When |
|---|---|
| Wake queues whose next task waited over 1 second | Every second |
| Send its heartbeat to the workers | Every 5 seconds, and right away when a new maintainer starts |
| Workers notice that the heartbeats stopped | After 15 seconds without one; idle queues then poll every `PollInterval` |
| Promote due tasks (delayed, retries, snoozes) | As they come due, at most 20 times a second |
| Rescue tasks whose leases expired | Every second |
| Enqueue due schedule runs | Every second |
| Refresh queue statistics | Every 10 seconds |
| Apply retention | Every minute |
| Prepare history partitions | 7 days ahead: today and the next 6 days, by UTC day |
| Other workers try to take over | Every 2 seconds |
| A new maintainer after a crash | Within about 12 seconds (its lease is 10 seconds) |
| A new maintainer after a clean stop | Right away, once the old one has drained |
| Remove a worker that stopped reporting | After 5 minutes without a report |
| Delete a schedule removed from the code | After 24 hours without a run or a change |

Every worker also reloads queue settings and writes its activity every
10 seconds. Its notification connection reconnects after 1 second, then
waits longer each time, up to 30 seconds.

## Retention

| Setting | Default | Limit |
|---|---|---|
| `Settings.HistoryRetention` (completed tasks) | 7 days | At least 1 hour. History is dropped a whole UTC day at a time, so a task can stay up to a day longer |
| `Settings.DeadRetention` (dead and canceled tasks) | 30 days | At least 1 hour |
| A dead task that halts its FIFO group | Kept until you retry or delete it | — |
| Activity (`Client.Activity`) | 24 hours, one point per minute | Fixed |
| Published outbox messages | Not kept | — |

Change retention with `Client.UpdateSettings`. A setting set to zero goes
back to its default.

## Operations API

| Setting | Default | Limit |
|---|---|---|
| `ListParams.Limit` | 100 tasks | 1 to 1,000 |
| `GroupParams.Limit` | 100 groups | 1 to 1,000; at most 100 names in `GroupParams.Queues` |
| Query time of `List`, `Groups`, `Count`, and `Activity` | — | 5 seconds, then an error |
| `Count` | — | Stops at 10,000 tasks per state |
| Queue statistics (`Client.Queues`) | Refreshed every 10 seconds | Count stops at 10,000 tasks per queue and state, and at 100,000 scheduled and retryable tasks in all |
| `QueueInfo.Lag` | — | No cap; may read high for up to 10 seconds |
| `ActivityParams.Since` | An hour ago | Activity is kept 24 hours |
| `ActivityParams.Step` | 1 minute | Whole minutes, up to 1 day |
| Bulk actions (`RetryMatching`, `CancelMatching`, `DeleteMatching`) | Batches of 1,000 tasks | When the context ends, the current batch finishes within 5 seconds |
| `QueueSettings.MaxRunning` | 0: no limit | Counts running tasks, also the tasks inside batches |
| `UpdateQueue`, `UpdateSettings` | Up to 10 tries, with random pauses of up to 50 ms | Then `ErrConflict` |
| Queue changes reach the workers | Right away, by notification | Within 10 seconds without notifications |

## Outbox

| Setting | Default | Limit |
|---|---|---|
| `outbox.Queue` | `"outbox"` | Fixed |
| `RelayOptions.BatchSize` | 100 deliveries per `Publish` call | 1 to 1,000 |
| `RelayOptions.Timeout` | 1 minute per `Publish` call | — |
| Retries of a failed delivery | 1 second, doubling up to 5 minutes. Each wait varies at random by ±10% | 10,000 tries, about a month; then the message is dead. Fixed |
| `Message.Topic` | — | Required |
| `Message.Value` | — | About 750 KiB: it is stored base64-encoded in a 1 MiB payload |
| Messages per `Store` call | — | 10,000, and 64 MiB |

## Dashboard

| What | Value |
|---|---|
| Time limit of one request | 10 seconds; then the page says "Taking too long" |
| Live updates | About every 5 seconds while the browser tab is visible. Up to 1 minute apart while the server is out of reach |
| Tasks per page of the task list | 50 |
| One bulk action request | Works for about 5 seconds, then offers to go on |
| A worker shows as stale | After 30 seconds without a report from it |
| Values per filter | Up to 100 |
| Charts | The last hour, 6 hours, or day |

The `taskmill-ui` command listens on `localhost:8080` by default: set
`TASKMILL_UI_ADDR=:8080` in a container. Its login user is `admin` unless you
set `TASKMILL_UI_USERNAME`. Its HTTP server waits up to 10 seconds for a
request's headers, 30 seconds to write an answer, and keeps an idle
connection for 2 minutes. At shutdown, it waits up to 10 seconds for open
requests. The
[command's godoc](https://pkg.go.dev/github.com/code19m/taskmill/cmd/taskmill-ui)
lists every environment variable.

## See also

- [Errors](errors.md): what each error means, and what to do
- [Workers and queues](../guides/workers-and-queues.md): how concurrency, leases, and shutdown work
- [Retries and errors](../guides/retries-and-errors.md): the retry table
- [`WorkerConfig`](https://pkg.go.dev/github.com/code19m/taskmill#WorkerConfig) on pkg.go.dev, and the godoc of every other setting

**Next:** [Errors](errors.md)
