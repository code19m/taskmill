// Package taskmill is a PostgreSQL-backed task queue for Go.
//
// An application defines a task as a struct that implements [Args], enqueues
// it with a [Client], usually inside its own database transaction, and runs it
// with a [Handler] registered on a [Worker]. Tasks are stored as rows in
// PostgreSQL, so an enqueue commits or rolls back with the application's own
// writes, and a worker that crashes never loses a task.
//
// Every task runs at least once. Failed attempts are retried with backoff
// until the task runs out of attempts and becomes dead, where operators can
// inspect and retry it. Tasks can be delayed, prioritized, given deadlines,
// deduplicated, and ordered one at a time per key in FIFO groups.
//
// See https://github.com/code19m/taskmill for guides and design documents.
package taskmill
