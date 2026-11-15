// Package taskmill is a task queue and transactional outbox for Go, on
// PostgreSQL.
//
// Tasks are rows in PostgreSQL. An enqueue commits or rolls back with the
// application's own writes, and a worker that crashes never loses a task.
// Every task runs at least once: handlers must be idempotent, so that running
// a task twice has the same effect as running it once.
//
// # Where to start
//
// Most programs take five steps, and the package's example shows them
// together:
//
//   - [Migrate] creates the schema, and [NewClient] returns the [Client].
//   - [Args] is a task: a struct with a kind.
//   - [Client.EnqueueTx] adds a task in the caller's transaction.
//   - [Handler] runs the tasks of one kind, registered with [AddHandler].
//   - [Worker] fetches tasks and runs their handlers, as its [WorkerConfig]
//     says.
//
// These comments are the reference: the comment of each identifier states
// its contract, its defaults, its limits, and its errors. The [docs] teach
// how to use them, from a [quickstart] to running in production. They ship
// with the module, in its docs directory, at the version in use, and
// llms.txt in the module's root lists every page in one line.
//
// # Defining tasks
//
// A task is a struct that implements [Args]: its Kind names the handler that
// runs it, and its fields are stored as JSON. Implement [ArgsWithDefaults] to
// give a kind default [EnqueueOptions], such as its queue.
//
// # Enqueueing
//
// [Client.Enqueue] enqueues a task in its own transaction, and
// [Client.EnqueueTx] in the caller's, through a [Pgx] or [SQL] executor, so
// that the task exists only if the caller's transaction commits.
// [Client.EnqueueMany] enqueues thousands of tasks in one round trip.
// [EnqueueOptions] delay a task, prioritize it, deduplicate it with a key, or
// put it in a FIFO group, whose tasks run one at a time, in order.
//
// # Handling tasks
//
// Register a [Handler] or a [BatchHandler] for each kind with [AddHandler]
// or [AddBatchHandler], with [HandlerOptions] such as a timeout. Returning
// nil completes a task. An error retries it with backoff, 25 attempts by
// default, after which it is dead: kept for an operator to inspect and
// [Client.Retry]. [Permanent] makes it dead right away, and [Snooze] runs it
// again later without using an attempt. [Task.CompleteTx] completes a task in
// the handler's own transaction, so its effects apply exactly once.
//
// # Running workers
//
// A [Worker] runs the handlers until its context ends, then stops cleanly.
// Its [WorkerConfig] names its queues, each with a [QueueOptions], its
// [Middleware], and its [Schedule] values, which enqueue tasks on a cron
// timetable. One worker of an installation is elected maintainer, and does
// the shared work: delayed tasks, retries, lost leases, schedules, and
// retention. [Worker.Health] answers a readiness probe.
//
// # Operating
//
// The [Client] lists, cancels, retries, and deletes tasks, one at a time or
// all that match a [TaskFilter]. It pauses queues, limits how many of a
// queue's tasks run at the same time with [QueueSettings], and sets
// retention with [Settings]. [Client.Queues] reports each queue's counts and
// lag.
//
// # Other packages
//
//   - github.com/code19m/taskmill/outbox publishes messages to a broker with
//     the transactional outbox pattern.
//   - github.com/code19m/taskmill/ui serves a dashboard of the queues and
//     tasks, as a [net/http.Handler].
//   - github.com/code19m/taskmill/cmd/taskmill-ui is that dashboard as a
//     program of its own.
//   - github.com/code19m/taskmill/taskmilltest runs tests against a real
//     database, each in a schema of its own.
//   - github.com/code19m/taskmill/otel, a module of its own, adds
//     OpenTelemetry traces and metrics.
//
// [docs]: https://github.com/code19m/taskmill/blob/main/docs/README.md
// [quickstart]: https://github.com/code19m/taskmill/blob/main/docs/start/quickstart.md
package taskmill
