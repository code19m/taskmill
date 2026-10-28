// Package taskmill is a PostgreSQL-backed task queue for Go.
//
// Tasks are rows in PostgreSQL. An enqueue commits or rolls back with the
// application's own writes, and a worker that crashes never loses a task.
// Every task runs at least once: handlers must be idempotent, so that running
// a task twice has the same effect as running it once.
//
// The guide at https://github.com/code19m/taskmill/blob/main/docs/guide.md
// covers every feature, testing, and running in production.
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
// that the task exists only if the caller's transaction commits. Options
// delay a task, prioritize it, deduplicate it with a key, or put it in a FIFO
// group, whose tasks run one at a time, in order.
//
// # Handling tasks
//
// Register a [Handler] or a [BatchHandler] for each kind with [AddHandler]
// or [AddBatchHandler], and run them with a [Worker]. Returning nil completes
// a task. An error retries it with backoff, 25 attempts by default, after
// which it is dead: kept for an operator to inspect and [Client.Retry].
// [Permanent] makes it dead at once, and [Snooze] runs it again later without
// using an attempt. [Task.CompleteTx] completes a task in the handler's own
// transaction, so its effects apply exactly once.
//
// # Operating
//
// The [Client] lists, cancels, retries, and deletes tasks, pauses queues,
// and limits how many of a queue's tasks run at once. Package
// github.com/code19m/taskmill/ui serves these operations, except the
// limits, as a web interface, and package
// github.com/code19m/taskmill/outbox publishes messages to a broker with the
// transactional outbox pattern.
//
// # Testing
//
// Package github.com/code19m/taskmill/taskmilltest runs tests against a real
// database, each in a schema of its own: it checks what code enqueues, and
// runs handlers the way a worker does.
package taskmill
