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
| A task whose kind has no handler | [`CodeNoHandler`](https://pkg.go.dev/github.com/code19m/taskmill#CodeNoHandler) and [`WorkerConfig`](https://pkg.go.dev/github.com/code19m/taskmill#WorkerConfig) |
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
| A worker's first read of its queues' pauses and limits | Before its first fetch. If it fails: again after 0.1 seconds, then twice as long each time, up to 10 seconds. A read ends after 10 seconds |
| Reads that notifications ask for: cancel requests, and queue settings | At most one of each kind per 20 ms |
| Warning that the pool is full | Logged when every connection of the pool is in use, and a statement of the worker waits 1 second or longer for one (`LeaseDuration` / 30). At most once a minute |
| A fetch | Two thirds of `LeaseDuration` in all, at least 5 seconds and at most 30: 20 seconds by default. A fetch cut short at that time may have leased tasks that never reached the worker: they are [rescued](../concepts/glossary.md#rescue) when their leases run out, at the cost of an attempt. After a fetch that was cut short, or whose tasks took longer than a third of `LeaseDuration` to arrive, not counting a wait for a connection of the pool, the worker renews their leases right away, and the next fetch takes at most half as many tasks. A fetch that brought that many within a sixth of `LeaseDuration` doubles it |
| A fetch under a backlog | After a fetch that took as many tasks as the queue had free slots, a freed slot starts the next fetch once a quarter of the slots are free, or after a twentieth of the time that a task holds its slot, and 2 ms at most. Room that a recorded completion makes starts it right away |
| Results that a worker records together | Completions, and the failures, snoozes, and releases of tasks outside FIFO groups: those ready, and those that end within 2 ms more, up to 500. They take one statement, and the completions of FIFO groups' tasks one transaction |
| A try to take an idle connection of the pool | A thirtieth of `LeaseDuration`, 1 second by default. The pool pings a connection that was idle for over a second, and closes one that does not answer by then. The worker then pings all of the pool's idle connections at the same time, for as long, and the pool closes those that do not answer either: the next try takes one that did, or opens one. So keep `LeaseDuration` above 30 round trips to the database, or the pool closes healthy connections, and opens new ones |
| A renewal of leases that failed | Tried again every thirtieth of `LeaseDuration`, 1 second by default, until a try succeeds or no lease is left to renew. A try has that much time, or twice as long as the one before if that one got no answer, up to a third of `LeaseDuration`. The check-ins continue every third of `LeaseDuration` meanwhile |
| A result that the worker cannot record | Tried again after 0.1 to 2 seconds, until the task's lease would run out. The first try has the time left on the lease, and at least a third of `LeaseDuration`. A try without an answer is followed by one with twice as long, and moves that end to the lease's end as check-ins renewed it by then. A try of 30 seconds, the longest, moves it no more |
| The failure, snooze, or release of a grouped task waits for a row that another transaction holds | At most 1 second at a time, then tried again, for as long as the row is held |
| A completion whose task's row, or its FIFO group's, another transaction holds | Tried again with the worker's next batch, or after 20 ms, then twice as long each time while rows stay held, up to 1 second. Up to 4 completions of a worker at a time also wait for their groups' rows, up to 100 ms each. One whose wait ran out, or whose task's row was held, waits again a second later at the soonest, and only while no other does. A warning once it waited longer than `LeaseDuration` |
| A handler that ignores its context | Abandoned 5 seconds after its context was canceled by its `Timeout`, a cancel, or a lost lease; a batch handler only after its `Timeout`, which alone cancels its context. Its goroutine keeps its slot until it returns. In a queue with `MaxRunning`, not abandoned: its task stays running until it returns, and the worker warns |
| What a stopping worker does after the drain | 5 seconds in all, to record what is left, give back the tasks whose handlers ignored the shutdown, and sign off. 5 seconds also for the tasks of a fetch that it could not start. A task that is not given back by then is [rescued](../concepts/glossary.md#rescue) when its lease runs out |
| "Tasks available" notifications | From a client's own writes: one batch per 50 ms, for all queues. From writes in your transaction: at most one per queue per 50 ms from each client |
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
| Workers notice that the heartbeats stopped | After 15 seconds without one. Idle queues then poll every `PollInterval`, ±10% |
| [Promote](../concepts/glossary.md#promote) due tasks: delayed tasks, retries, and snoozed tasks | As they come due, at most 20 times a second. So a task becomes available within about 1 second of its [run time](../concepts/glossary.md#run-time) |
| [Rescue](../concepts/glossary.md#rescue) tasks whose leases ran out | Every second |
| One statement of the maintainer's work, or one transaction | Ends after 10 seconds (the maintainer's lease), so a connection that stops answering holds it no longer. The detach and the drop of a history partition have 30 seconds |
| Enqueue due schedule runs | Every second |
| Compare the schedules of the workers that can become the maintainer | Every minute. A difference is logged once it has lasted 10 minutes, then at most once an hour |
| Dedupe window of a schedule run | 24 hours, or twice the time to the schedule's next run if that is longer |
| Refresh queue statistics | Every 10 seconds |
| Apply retention | Every minute |
| Prepare history partitions | 7 days ahead: today and the next 6 days, by UTC day on the database's clock |
| Drop old history partitions | At most 10 in a pass of retention, so 10 a minute: a much shorter `HistoryRetention` takes a while to free its disk space. A pass waits up to 30 seconds for transactions that use the history, and tries again a minute later |
| Other workers try to take over | Every 2 to 3 seconds, with a statement that writes nothing while the maintainer's lease lasts |
| A new maintainer after a crash | Within about 12 seconds (the maintainer's lease is 10 seconds) |
| A new maintainer after a clean stop | Right away. The old maintainer gives the role up as soon as it starts to stop, before it waits for its running tasks. If an election of its own still waits for an answer, it first waits for that, up to 5 seconds |
| Remove a worker that stopped checking in | After 5 minutes without a [check-in](../concepts/glossary.md#check-in), or after twice its `LeaseDuration` if that is longer |

## Dashboard

| What | Value |
|---|---|
| Time limit of one request | 10 seconds, or less if your app sets an earlier deadline in the request's context. Then the page says "Taking too long". It says the same when a list of tasks reads for more than its 5 seconds |
| Live updates | About every 5 seconds while the browser tab is visible. While the server fails or is out of reach, 5, 10, 20, and 40 seconds apart, then 1 minute |
| A page or an action that the page's script asks for | Given up after 15 seconds, more than the server's 10. Then the page says that no answer came, and tries again by itself, or a notice says that the action may have applied |
| Pages kept for the back and forward buttons | The last 10 of a browser tab that update themselves, in memory, so that they show right away. The browser loads an older one again, and a page that does not update itself, such as one that says why an action did not apply |
| Tasks that the task list shows | 50 at first. "Load more" adds 50, up to 250, and every live update reads all that are shown. After 250, the list continues from its last task on a page of its own |
| One bulk action request | Works for about 5 seconds, then offers to continue |
| The dashboard marks a worker as stale | After one `LeaseDuration` of that worker without a check-in: 30 seconds by default |
| Values per filter | Up to 100 |
| FIFO groups that a filter lists | The first 100. Its search finds the others |
| Filter choices (kinds and FIFO groups) | Read again at most every 30 seconds, or 5 seconds after a read that failed |
| Charts | The last hour, 6 hours, or day, with one point per 1, 5, or 15 minutes. The 6 hours and the day read their history again at most every 30 seconds, and when a point becomes complete |

The values of the `taskmill-ui` command, such as its HTTP timeouts, are in
[The `taskmill-ui` command](taskmill-ui.md).

## See also

- [Configuration](configuration.md): the type that holds each setting
- [Workers and queues](../guides/workers-and-queues.md): how concurrency, leases, and shutdown work
- [Maintenance and notifications](../design/schema-maintenance.md): why the maintainer works this way
- [API on pkg.go.dev](https://pkg.go.dev/github.com/code19m/taskmill): every default and limit of the API

**Next:** [Compatibility](compatibility.md)
