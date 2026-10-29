# Glossary

The words that taskmill uses, each in one plain line. Other pages link here
the first time they use a word.

## Terms from A to Z

### Args
The Go type of a task: a struct with a `Kind` method. Its fields are the task's [payload](#payload).

### At least once
taskmill's promise: every task runs one or more times, until it finishes. So a task can run twice ([guarantees](guarantees.md)).

### Attempt
One run of a [handler](#handler) on a task. A task gets 25 attempts by default.

### Backoff
The wait before a failed task runs again. By default, it doubles after each failure, from 1 second up to 1 hour.

### Batch handler
A [handler](#handler) that gets many tasks of one [kind](#kind) in one call, for work that is cheaper in bulk.

### Cancel
Stop a task for good, but keep it as `canceled`, so you can still see it and [retry](#retry) it. A running task is asked to stop through its context.

### Check-in
A worker's report to the database, every 10 seconds by default: it renews the [leases](#lease) of its running tasks, and notes that it is alive. The dashboard's **Checked in** column shows the last one. Not the same as the maintainer's [heartbeat](#heartbeat).

### Client
The object your code uses to [enqueue](#enqueue) tasks, and to find, retry, cancel, or delete them: `taskmill.Client`.

### Concurrency
How many tasks one [worker](#worker) runs at the same time from one [queue](#queue). The default is 10.

### Dashboard
The web page where you watch queues, tasks, and workers, and retry, cancel, or delete tasks. It is the `ui` package, or the `taskmill-ui` command.

### Dead task
A task that will not run again unless you retry it: it used all its attempts, returned a [permanent error](#permanent-error), or could not run at all. It waits for you, 30 days by default.

### Dedupe key
A key you choose, like `receipt-42`. For 24 hours after the enqueue (by default), a second task with the same key, kind, and queue is not added: you get the first task back.

### Drain
What a worker does when it stops: it takes no new tasks, and waits for its running tasks to finish, for up to 20 seconds by default.

### Enqueue
Add a task. `Enqueue` adds it on its own; `EnqueueTx` adds it inside your own database transaction.

### FIFO group
The tasks of one [queue](#queue) that share a `GroupKey`, such as one customer's. They run one at a time, in the order you added them (first in, first out).

### Finished
A task that is `completed`, `dead`, or `canceled`. It does not run again, unless you retry a dead or canceled one. All other [states](#state) are unfinished.

### Halted group
A [FIFO group](#fifo-group) whose first task is dead. The tasks behind it wait until you retry or delete that task.

### Handler
Your code that runs one [kind](#kind) of task. It returns nil when the work is done, or an error to try again later.

### Head
The first unfinished task of a [FIFO group](#fifo-group): the only task of the group that can run.

### Heartbeat
The [maintainer](#maintainer)'s signal to all workers, every few seconds, that it is alive. While workers hear it, they check idle queues only rarely. A worker's own report to the database is its [check-in](#check-in).

### Idempotent
Safe to run twice: two runs have the same effect as one. Write handlers this way, because a task can run more than once.

### Inbox
A way to take messages from a broker without doing the work twice: enqueue one task per message, with the message ID as its [dedupe key](#dedupe-key).

### Installation
The word that the API reference (godoc) uses for a [schema](#schema).

### Kind
The name of a type of task, like `email.send_receipt`. It tells taskmill which [handler](#handler) runs the task.

### Lag
How long the oldest due task of a [queue](#queue) has waited since its run time. It grows when workers fall behind.

### Lease
A worker's claim on a running task, for a limited time (30 seconds by default). The worker renews it while the handler runs. If the worker dies, the lease runs out, and another worker runs the task.

### Maintainer
The one worker that taskmill chooses to do shared work: wake queues, make due tasks ready, give the tasks of crashed workers back, and run [schedules](#schedule). If it stops, another worker takes over.

### `MaxRunning`
A limit on how many tasks of one [queue](#queue) run at the same time on all workers together. It counts tasks, not batches.

### Metadata
Small text tags stored with a task, such as a trace ID.

### Middleware
Code that wraps every attempt, or every enqueue, for logs, metrics, or tracing.

### Migrate
Create or update taskmill's tables with `taskmill.Migrate`. It is safe to run every time your app starts.

### Notification
A message from PostgreSQL (`LISTEN` and `NOTIFY`) that wakes workers when a task is ready.

### Outbox
A safe way to send messages to a broker such as Kafka: you store the message in your transaction, and a [relay](#relay) publishes it after the commit.

### Pause
Stop workers from taking new tasks from a [queue](#queue) for a while. Running tasks finish, and waiting tasks keep their place.

### Payload
A task's data, stored as JSON. In Go, it is the task's [Args](#args) struct.

### Permanent error
An error that you wrap with `taskmill.Permanent`. It means "a retry cannot help", so the task becomes [dead](#dead-task) right away.

### Polling
Checking the database for tasks on a timer. Workers poll as a backup, so a lost [notification](#notification) never loses a task.

### Priority
Which ready task of a queue starts first: `low`, `normal` (the default), `high`, or `critical`.

### Queue
A named line of tasks, such as `default` or `emails`. Each worker chooses the queues it takes tasks from. Limits and pauses work per queue.

### Relay
The [batch handler](#batch-handler) that publishes [outbox](#outbox) messages to your broker. It runs on a worker.

### Release
Give a running task back to its queue when the worker stops, without using an attempt. Another worker runs it again.

### Retention
How long taskmill keeps [finished](#finished) tasks: completed tasks for 7 days, and dead and canceled tasks for 30 days, by default.

### Retry
Run a task again. taskmill retries a failed task by itself, after a [backoff](#backoff). You can also retry a dead or canceled task, in the dashboard or with `Client.Retry`.

### Schedule
A task that runs on a cron timetable, such as every night at 03:00. Not the same as the `scheduled` [state](#state), which means "waiting for its run time".

### Schema
One taskmill schema, with its own tasks, queues, and workers. Its tables live in one PostgreSQL schema, `taskmill` by default. Two schemas never see each other's tasks.

### Slot
A worker's place for one running task, or one batch, in a queue. A worker has [Concurrency](#concurrency) slots per queue.

### Snooze
A handler's way to say "try again later": the task runs again after the delay you choose, without using an attempt.

### State
Where a task is in its life: one of eight, such as `available`, `running`, or `dead` ([task lifecycle](task-lifecycle.md)).

### Task
One piece of work to do in the background, like "email this receipt". It is a row in PostgreSQL.

### Worker
The part of your program that takes tasks from the database and runs their handlers: `taskmill.Worker`.

## See also

- [How it works](how-it-works.md): the parts, in one picture
- [Task lifecycle](task-lifecycle.md): every state, and what moves a task
- [API reference](https://pkg.go.dev/github.com/code19m/taskmill) on pkg.go.dev

**Next:** [Guides](../guides/README.md)
