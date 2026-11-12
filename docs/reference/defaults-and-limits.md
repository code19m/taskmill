# Defaults and limits

The timings and limits that are not a field you set, with their exact
values. The guides round these numbers. For every field's default and
limit, see [Configuration](configuration.md).

Every value on this page is fixed, unless its row names the setting that
changes it.

## Tasks

| What | Value |
|---|---|
| Queue names, kinds, and schedule names | 1 to 128 characters |
| Payload | At most 1 MiB of JSON. The godoc of [`Args`](https://pkg.go.dev/github.com/code19m/taskmill#Args) has the exact rules |
| One enqueue call (`EnqueueMany`, `EnqueueManyTx`, `outbox.Store`) | At most 10,000 tasks, and 64 MiB of payload and metadata |
| A delayed task, a retry, or a snoozed task becomes available | Within about 1 second of its run time |

## Handlers

| What | Value |
|---|---|
| Time from the first failure to dead, with the default `MaxAttempts` and `Retry` | About 13 hours (25 attempts) |
| Retry wait after a lost [lease](../concepts/glossary.md#lease) | The default [backoff](../concepts/glossary.md#backoff), whatever the kind's `Retry` says |
| A task whose kind has no handler on the worker that fetched it | Snoozed 1 minute at a time, without using an attempt. Dead with `unknown_kind` after 1 hour |
| Error history (`TaskInfo.Errors`) | The 20 most recent entries, with 4 KiB per message |

## Workers

| What | Value |
|---|---|
| The worker's [check-in](../concepts/glossary.md#check-in): it renews its leases and notes that it is alive | Every third of `LeaseDuration`: 10 seconds by default |
| Poll of an idle queue while the maintainer's [heartbeats](../concepts/glossary.md#heartbeat) arrive | Once a minute, ±10%. Every `PollInterval`, if that is longer |
| A task whose [notification](../concepts/glossary.md#notification) was lost starts | Within about 2 seconds while heartbeats arrive. Within about 16 seconds if the maintainer or the notification connection fails silently |
| Handlers that still run when `DrainTimeout` ends | Their contexts are canceled. 5 seconds later, their tasks go back to their queues |
| Database connections of one worker | Its pool, plus 1 for notifications |
| Warning that the pool is full | Logged when the pool is full, and a lease renewal waits 1 second or longer for a connection (`LeaseDuration` / 30). At most once a minute |
| `Worker.Health` returns an error | While `Run` is not running, or when the last successful check-in is older than `LeaseDuration` |
| A result that the worker cannot record | Tried again after 0.1 to 2 seconds, until the task's lease would run out |
| Activity counts, for the charts | Written every 10 seconds. Counts that a worker cannot write are dropped after 1 hour |
| "Tasks available" notifications | At most one per queue per 50 ms from each client |
| Notification connection: time to connect, and to answer when heartbeats stop | 10 seconds each |
| Notification connection: reconnect | After 1 second, then twice as long each time, up to 30 seconds |
| No history partition for today | Completions skip history for 1 minute, then try again |

## The maintainer

The [maintainer](../concepts/glossary.md#maintainer) is one worker, chosen
by election, that does the shared work.

| What | When |
|---|---|
| Wake queues whose next task waited over 1 second | Every second |
| Send its heartbeat to the workers | Every 5 seconds, and right away when a new maintainer starts |
| Workers notice that the heartbeats stopped | After 15 seconds without one. Idle queues then poll every `PollInterval` |
| Promote due tasks (delayed tasks, retries, snoozed tasks): make them available | As they come due, at most 20 times a second |
| Rescue tasks whose leases ran out: fail their attempts, so that they retry | Every second |
| Enqueue due schedule runs | Every second |
| `Client.RunSchedule` | The run is enqueued within about 1 second, if the maintainer defines the schedule |
| Dedupe window of a schedule run | 24 hours, or twice the time to the schedule's next run if that is longer |
| Refresh queue statistics | Every 10 seconds |
| Apply retention | Every minute |
| Prepare history partitions | 7 days ahead: today and the next 6 days, by UTC day |
| Drop an old history partition | Waits up to 30 seconds for transactions that use the history. Tried again every minute |
| Other workers try to take over | Every 2 seconds |
| A new maintainer after a crash | Within about 12 seconds (the maintainer's lease is 10 seconds) |
| A new maintainer after a clean stop | Right away, when the old one starts to stop. It gives the role up before it waits for its running tasks |
| Remove a worker that stopped checking in | After 5 minutes without a check-in |
| Delete a schedule removed from the code | After 24 hours without a run or a change |

## Retention

| What | Value |
|---|---|
| Completed tasks, and dead and canceled tasks | Set in [`Settings`](configuration.md#settings): 7 and 30 days by default |
| A dead task that halts its [FIFO group](../concepts/glossary.md#fifo-group) | Kept until you retry or delete it |
| Activity (`Client.Activity`) | Kept 24 hours, one point per minute |
| Published outbox messages | Not kept |

## Operations API

| What | Value |
|---|---|
| Query time of `List`, `Groups`, `Count`, and `Activity` | 5 seconds, then an error |
| `Count` | Stops at 10,000 tasks per state |
| Queue statistics (`Client.Queues`) | Refreshed every 10 seconds. A count stops at 10,000 tasks per queue and state, and at 100,000 scheduled and retryable tasks in all |
| `QueueInfo.Lag` | No cap. It can be too high for up to 10 seconds: a task that finished after the last count still counts |
| `QueueInfo.HaltedGroups` | Stops at 10,000 groups per queue |
| `QueueInfo.MostFailures` | The maintainer looks at 10,000 retryable tasks across all queues: the ones whose next retry is furthest away. It also looks at one such task per queue. With more retryable tasks than that, the number can be too low |
| Bulk actions (`RetryMatching`, `CancelMatching`, `DeleteMatching`) | Batches of 1,000 tasks. When the context ends, the current batch finishes within 5 seconds |
| `UpdateQueue`, `UpdateSettings` | Up to 10 tries, with random pauses of up to 50 ms. Then `ErrConflict` |
| A change of a queue's settings reaches the workers | Right away, by notification. Within 10 seconds without notifications |

## Outbox

| What | Value |
|---|---|
| Queue (`outbox.Queue`) | `outbox` |
| Task kind | `taskmill.outbox.message` |
| Size of one message | Its topic, key, value, and headers share the 1 MiB payload limit, base64-encoded. That allows about 750 KiB of `Message.Value` |
| Messages per `Store` call | At most 10,000, and 64 MiB |
| Retries of a failed delivery | After 1 second, doubling up to 5 minutes, ±10%. 10,000 tries, about a month. Then the message is dead |

## Dashboard

| What | Value |
|---|---|
| Time limit of one request | 10 seconds. Then the page says "Taking too long" |
| Live updates | About every 5 seconds while the browser tab is visible. Up to 1 minute apart while the server is out of reach |
| Tasks per page of the task list | 50 |
| One bulk action request | Works for about 5 seconds, then offers to continue |
| The dashboard marks a worker as stale | After 30 seconds without a check-in |
| Values per filter | Up to 100 |
| FIFO groups that a filter lists | The first 100. Its search finds the others |
| Filter choices (kinds and FIFO groups) | Read again at most every 30 seconds |
| Page of a dead task in a FIFO group | Counts up to 100 tasks that wait behind it |
| Charts | The last hour, 6 hours, or day, with one point per 1, 5, or 15 minutes |

The values of the `taskmill-ui` command, such as its HTTP timeouts, are in
[The `taskmill-ui` command](taskmill-ui.md).

## See also

- [Configuration](configuration.md): every field, with its default and limits
- [Errors](errors.md): what each error means, and what to do
- [Workers and queues](../guides/workers-and-queues.md): how concurrency, leases, and shutdown work
- [Retries and errors](../guides/retries-and-errors.md): the retry table

**Next:** [The `taskmill-ui` command](taskmill-ui.md)
