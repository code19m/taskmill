# Fixed timings and limits

The timings and limits of the workers, the maintainer, and the dashboard
that belong to no function, type, or field, with their exact values. The
guides round these numbers. Every value here is fixed, unless its row names
the setting that changes it.

A default or a limit of a function, a type, or a field is not on this page.
It is in the doc comment of that identifier.

## Limits in the doc comments

| You look for | Read the doc comment of |
|---|---|
| The default or the limits of a field | The field's type: [Configuration](configuration.md) names the types |
| The size of a payload, and what it may hold | [`Args`](https://pkg.go.dev/github.com/code19m/taskmill#Args) |
| The length of a queue name, a kind, a schedule name, or a group key | [`EnqueueOptions`](https://pkg.go.dev/github.com/code19m/taskmill#EnqueueOptions), [`Args`](https://pkg.go.dev/github.com/code19m/taskmill#Args), and [`Schedule`](https://pkg.go.dev/github.com/code19m/taskmill#Schedule) |
| The tasks and bytes of one enqueue call | [`Client.EnqueueMany`](https://pkg.go.dev/github.com/code19m/taskmill#Client.EnqueueMany) |
| The waits between retries, and the number of attempts | [`ExponentialBackoff`](https://pkg.go.dev/github.com/code19m/taskmill#ExponentialBackoff) and [`EnqueueOptions`](https://pkg.go.dev/github.com/code19m/taskmill#EnqueueOptions) |
| A task whose kind has no handler | [`CodeNoHandler`](https://pkg.go.dev/github.com/code19m/taskmill#CodeNoHandler) |
| The length of the error history | [`TaskInfo`](https://pkg.go.dev/github.com/code19m/taskmill#TaskInfo) and [`AttemptError`](https://pkg.go.dev/github.com/code19m/taskmill#AttemptError) |
| The lease, the check-in, polling, and shutdown | [`WorkerConfig`](https://pkg.go.dev/github.com/code19m/taskmill#WorkerConfig) |
| A worker's connections, and its health | [`Worker`](https://pkg.go.dev/github.com/code19m/taskmill#Worker) and [`Worker.Health`](https://pkg.go.dev/github.com/code19m/taskmill#Worker.Health) |
| The caps of the queue counts, and their age | [`QueueInfo`](https://pkg.go.dev/github.com/code19m/taskmill#QueueInfo) and [`Client.Count`](https://pkg.go.dev/github.com/code19m/taskmill#Client.Count) |
| The time limit of a list, and the size of a page | [`Client.List`](https://pkg.go.dev/github.com/code19m/taskmill#Client.List) and [`ListParams`](https://pkg.go.dev/github.com/code19m/taskmill#ListParams) |
| The batches of a bulk action | [`Client.RetryMatching`](https://pkg.go.dev/github.com/code19m/taskmill#Client.RetryMatching) |
| The tries of `Cancel`, `Retry`, `Delete`, and of an update of settings | [`Client.Cancel`](https://pkg.go.dev/github.com/code19m/taskmill#Client.Cancel) and [`Client.UpdateQueue`](https://pkg.go.dev/github.com/code19m/taskmill#Client.UpdateQueue) |
| A schedule's run on request, and a schedule removed from the code | [`Client.RunSchedule`](https://pkg.go.dev/github.com/code19m/taskmill#Client.RunSchedule) and [`Schedule`](https://pkg.go.dev/github.com/code19m/taskmill#Schedule) |
| How long tasks and activity are kept | [`Settings`](https://pkg.go.dev/github.com/code19m/taskmill#Settings) and [`ActivityParams`](https://pkg.go.dev/github.com/code19m/taskmill#ActivityParams) |
| The outbox: its queue, the size of a message, and its retries | [`outbox.Queue`](https://pkg.go.dev/github.com/code19m/taskmill/outbox#Queue), [`outbox.Message`](https://pkg.go.dev/github.com/code19m/taskmill/outbox#Message), and [`outbox.Publisher`](https://pkg.go.dev/github.com/code19m/taskmill/outbox#Publisher) |

## Workers

| What | Value |
|---|---|
| A fetch from a queue with `MaxRunning` waits for the queue's row | At most 1 second. Then the worker fetches again when the queue is next woken or polled |
| Reads that notifications ask for: cancel requests, and queue settings | At most one of each kind per 20 ms |
| Warning that the pool is full | Logged when the pool is full, and a lease renewal waits 1 second or longer for a connection (`LeaseDuration` / 30). At most once a minute |
| A result that the worker cannot record | Tried again after 0.1 to 2 seconds, until the task's lease would run out |
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
| Send its [heartbeat](../concepts/glossary.md#heartbeat) to the workers | Every 5 seconds, and right away when a new maintainer starts |
| Workers notice that the heartbeats stopped | After 15 seconds without one. Idle queues then poll every `PollInterval` |
| [Promote](../concepts/glossary.md#promote) due tasks: delayed tasks, retries, and snoozed tasks | As they come due, at most 20 times a second. So a task becomes available within about 1 second of its [run time](../concepts/glossary.md#run-time) |
| [Rescue](../concepts/glossary.md#rescue) tasks whose leases ran out | Every second |
| Enqueue due schedule runs | Every second |
| Dedupe window of a schedule run | 24 hours, or twice the time to the schedule's next run if that is longer |
| Refresh queue statistics | Every 10 seconds |
| Apply retention | Every minute |
| Prepare history partitions | 7 days ahead: today and the next 6 days, by UTC day on the database's clock |
| Drop an old history partition | Waits up to 30 seconds for transactions that use the history. Tried again every minute |
| Other workers try to take over | Every 2 to 3 seconds, with a statement that writes nothing while the maintainer's lease lasts |
| A new maintainer after a crash | Within about 12 seconds (the maintainer's lease is 10 seconds) |
| A new maintainer after a clean stop | Right away, when the old one starts to stop. It gives the role up before it waits for its running tasks |
| Remove a worker that stopped checking in | After 5 minutes without a [check-in](../concepts/glossary.md#check-in) |

## Dashboard

| What | Value |
|---|---|
| Time limit of one request | 10 seconds. Then the page says "Taking too long" |
| Live updates | About every 5 seconds while the browser tab is visible. Up to 1 minute apart while the server is out of reach |
| Tasks per page of the task list | 50 |
| One bulk action request | Works for about 5 seconds, then offers to continue |
| The dashboard marks a worker as stale | After 30 seconds without a check-in. So with a `LeaseDuration` above 90 seconds, a healthy worker shows as stale between check-ins |
| Values per filter | Up to 100 |
| FIFO groups that a filter lists | The first 100. Its search finds the others |
| Filter choices (kinds and FIFO groups) | Read again at most every 30 seconds |
| Page of a dead task in a FIFO group | Counts up to 100 tasks that wait behind it |
| Charts | The last hour, 6 hours, or day, with one point per 1, 5, or 15 minutes |

The values of the `taskmill-ui` command, such as its HTTP timeouts, are in
[The `taskmill-ui` command](taskmill-ui.md).

## See also

- [Configuration](configuration.md): the type that holds each setting
- [Workers and queues](../guides/workers-and-queues.md): how concurrency, leases, and shutdown work
- [Maintenance and notifications](../design/schema-maintenance.md): why the maintainer works this way
- [API on pkg.go.dev](https://pkg.go.dev/github.com/code19m/taskmill): every default and limit of the API

**Next:** [Compatibility](compatibility.md)
