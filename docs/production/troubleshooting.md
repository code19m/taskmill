# Troubleshooting

Find what you see. Each entry lists the likely causes, most likely first,
with what to check and what to do.

> [!NOTE]
> Many failures fix themselves. A crashed worker's tasks run again about 30
> seconds later, when their [leases](../concepts/glossary.md#lease) run out.
> Another worker replaces a crashed [maintainer](../concepts/glossary.md#maintainer)
> within about 12 seconds. Workers reconnect after a database outage. Look
> for a cause when a problem lasts longer than a minute.

## Tasks start late or not at all

### Tasks wait longer and longer

**You see:** `taskmill_queue_lag_seconds` keeps rising, and the dashboard
shows many available tasks.

| Likely cause | Check | Do |
|---|---|---|
| The queue is paused | The queues on the dashboard's Overview, or `QueueInfo.Paused` | Resume it |
| No worker reads the queue | `QueueInfo.Workers` is 0, or a worker logs `the worker does not serve the default queue of these registered kinds` | Add the queue to `WorkerConfig.Queues`, or run a worker for it |
| The workers are full | Running tasks are about replicas × `Concurrency` | Add replicas, or raise `Concurrency` and the pool with it |
| A queue limit holds tasks back | `QueueInfo.Settings.MaxRunning` is set, and running tasks equal it | Raise or remove it with `Client.UpdateQueue` |
| No maintainer runs | `taskmill_stats_age_seconds` is over 60 | See [the next entry](#retries-delayed-tasks-or-schedules-dont-run) |
| Fetches got slow | Snapshot ages grow | See [Throughput falls over hours](#throughput-falls-over-hours) |

### Retries, delayed tasks, or schedules don't run

**You see:** tasks stay `scheduled` or `retryable` after their time,
schedules don't run, or `taskmill_stats_age_seconds` is over 60. All of these
need the maintainer, which another worker replaces within about 12 seconds.

| Likely cause | Check | Do |
|---|---|---|
| No worker can become the maintainer: all have `NoMaintainer: true`, or KEDA stopped them all | Dashboard › Workers: no worker has the *maintainer* label | Keep one such worker running: [Scale to zero](autoscaling.md#scale-to-zero) |
| Workers restart again and again | Pod restarts, and [start-up errors](#start-up-errors) | Fix the start-up error |
| The database is out of reach | `operation still failing` for `elect maintainer` | Fix the database or the network |
| Only schedules: the maintainer does not define them | Dashboard › Workers shows the maintainer. Check its `Schedules` | Define the same `Schedules` on every worker that can become the maintainer |
| Only one schedule: it is paused | Dashboard › Schedules | Resume it |

### Tasks start seconds late

**You see:** new tasks start a second or more after they are enqueued, not
within milliseconds. (Delayed tasks and retries start within about a second
of their time: that is normal.)

| Likely cause | Check | Do |
|---|---|---|
| Notifications go through a pooler in transaction mode | The Error line `notifications disabled; polling only` | Point `ListenConfig` at a direct connection: [Connection poolers](connection-poolers.md) |
| The maintainer's [heartbeats](../concepts/glossary.md#heartbeat) don't arrive | The Warn line `maintainer heartbeats do not arrive` | See [the next entry](#the-log-says-maintainer-heartbeats-do-not-arrive) |

### The log says `maintainer heartbeats do not arrive`

**You see:** the Warn line `maintainer heartbeats do not arrive; idle queues
poll every PollInterval`. Until heartbeats arrive again, the worker checks
its idle queues every `PollInterval` (1 second by default), which adds load.

| Likely cause | Check | Do |
|---|---|---|
| No maintainer runs | As in [Retries, delayed tasks, or schedules don't run](#retries-delayed-tasks-or-schedules-dont-run) | As there |
| During an upgrade, the maintainer still runs an older release that sends no heartbeats | Dashboard › Workers: the versions | Nothing: it ends with the rollout |
| Notifications don't reach this worker | Where `ListenConfig` points | Point it at the primary of the same database |

### Tasks wait in `pending`

**You see:** tasks in a [FIFO group](../concepts/glossary.md#fifo-group) stay
`pending`. They wait for the tasks before them in their group, so they don't
count in lag.

| Likely cause | Check | Do |
|---|---|---|
| The group's first task is dead, so the group is [halted](../concepts/glossary.md#halted-group) | Open a pending task in the dashboard: it shows the task it waits behind. Or call `Client.Groups` with `Halted: true` | Fix the cause, then retry the dead task: the group continues in order. Delete it only to skip that step for good |
| The group's first task still runs, waits for a retry, or is snoozed | The same page shows that task's state | Wait, or fix why it fails |

## Tasks run twice or die

### A task ran twice

**You see:** a handler ran twice for one task. The logs show `attempt lost
its lease; canceling it`, or `rescued tasks whose workers lost their leases`.
Some repeats are expected: [Guarantees](../concepts/guarantees.md).

| Likely cause | Check | Do |
|---|---|---|
| Handlers use up the pool's connections | The warning `the pool was full, so renewing leases waited` | [Size the pool](database.md#size-the-connection-pool) |
| A pod was killed before it drained | `terminationGracePeriodSeconds` below `DrainTimeout` plus 10, or pods killed for their memory | Raise the grace period, or the memory limit |
| A handler ignores its context | The log `handler ignored shutdown; releasing its task` | Make the handler return when `ctx` ends |
| A failover or network cut of more than about 20 s | `operation still failing` during the incident | Expected: [When the database fails](database.md#when-the-database-fails) |
| The handler finished, but its result was not saved | The log `complete tasks failed; they are rescued when their leases expire` | Expected after a database outage |

In the long run, [make handlers safe to run twice](../recipes/do-work-once.md).

### Tasks die with `unknown_kind`, or wait with a `no_handler` note

**You see:** after a deploy, tasks have `no_handler` notes, and the log says
`no handler for task kind; snoozing`. An hour later, they are dead with the
code `unknown_kind`.

| Likely cause | Check | Do |
|---|---|---|
| Code that enqueues a new kind went out before the workers that handle it | Your deploy order | Deploy the workers, then retry the dead tasks |
| A kind was renamed | The history of the `Kind()` method | Handle the old name too, until no old tasks are left |
| Workers with different handlers read the same queue | `Client.Workers` lists each worker's `Kinds` | Give each set of kinds its own queue |

To retry them, filter the dead tasks by kind and choose *Retry all*, or call
`Client.RetryMatching`.

### Tasks die with `invalid_payload`

- **Cause:** a deploy changed the `Args` struct, such as a field's type, so
  old payloads do not decode. Retries cannot fix that.
- **Do:** deploy a handler that also decodes the old payloads, then retry the
  dead tasks. Next time, [add a new kind](../recipes/change-a-task-safely.md).

### Throughput falls over hours

- **Cause:** an old snapshot keeps vacuum from cleaning the `tasks` table.
- **Check:** the [snapshot ages](database.md#keep-long-transactions-from-slowing-the-queue),
  and `idle in transaction` sessions in `pg_stat_activity`.
- **Do:** end the long transaction, or drop the unused replication slot. Then
  set the timeouts that the same page lists.

## Start-up errors

### `NewClient` fails with `ErrSchemaOutdated`

**You see:** `taskmill: database schema is missing or outdated`, followed by
`schema "taskmill" is not migrated` or `schema "taskmill" is at version 2,
need 3`. `taskmill-ui` exits with the same error.

| Likely cause | Do |
|---|---|
| `Migrate` did not run yet | Run it: at process start, or in a Job before the rollout |
| The code and `Migrate` use different schema names | Compare `ClientConfig.Schema`, `MigrateConfig.Schema`, and `TASKMILL_SCHEMA` |
| `taskmill-ui` is newer than your apps | Pin its tag to your apps' taskmill version |

### `NewClient` fails because of the isolation level

- **You see:** `the pool's default_transaction_isolation is "repeatable
  read", but taskmill needs "read committed"`.
- **Do:** [set the level](database.md#keep-the-isolation-level-at-read-committed)
  for taskmill's pool.

### `Migrate` fails with a lock timeout

- **You see:** `canceling statement due to lock timeout`.
- **Cause:** a long transaction held a lock for more than 5 seconds.
- **Do:** let a pod restart or the Job's next try run it again. If it keeps
  failing, find the long transaction in `pg_stat_activity`.

### `taskmill-ui` restarts again and again

**You see:** the Error line `taskmill-ui stopped`, and the pod restarts. Its
`error` attribute names the cause: a missing `TASKMILL_DATABASE_URL`, a
database out of reach, `ErrSchemaOutdated`, or a wrong `TASKMILL_UI_*`
setting. Fix what it names.

## Other log lines and pages

### The log says `task history has no partition for today`

- **Cause:** the role that runs your workers does not own taskmill's tables.
  Tasks still complete, but without history.
- **Check:** `operation still failing` for `apply retention`, with
  `the role that runs workers must own the taskmill tables` in its error.
- **Do:** [make that role the owner](database.md#run-workers-as-the-owner-of-taskmills-tables).
  The maintainer creates the missing partitions within a minute.

### Kubernetes marks workers as not ready

**You see:** worker pods are not ready, and a rollout stops. `/healthz`
returns 503, with `the worker has not reached the database for …` or
`the worker is not running`.

| Likely cause | Check | Do |
|---|---|---|
| The database is out of reach: an outage, a failover, a network policy, or a changed password | `operation still failing` for `record heartbeat` (the worker updates its own row) or `renew leases` | Fix the database or the network. Pods are ready again about 20 s after it is back |
| Handlers hold every connection of the pool | The warning `the pool was full, so renewing leases waited` | Raise `pool_max_conns`, or give handlers their own pool |
| `Run` returned, or never started | `the worker is not running`, and the error that `Run` returned | Fix the start-up error |

Don't restart the workers: that does not fix a database outage
([Kubernetes](kubernetes.md#tell-kubernetes-when-a-worker-is-ready)).

### Counts in the dashboard stop at 10,000

This is on purpose, so that counting stays cheap on a large backlog: the
dashboard shows "10,000+". To watch a backlog, use lag: it has no cap
([Monitoring](monitoring.md#know-the-catches)).

### The dashboard says "Taking too long" or "Something went wrong"

- **Cause:** the database did not answer a page within 10 seconds, or a task
  filter read for more than 5 seconds (the log says `statement timeout`).
- **Do:** try again. Add a queue or a state to the task filter: a filter by
  kind or group alone reads more. Then check the database's load.

## See also

- [Log messages](../reference/log-messages.md): every log line, and what to do
- [Errors](../reference/errors.md): every error and error code
- [Recover from an incident](../recipes/recover-from-an-incident.md): retry many tasks safely

**Next:** [Upgrading](upgrading.md)
