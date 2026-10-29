# Guarantees

This page says what taskmill promises, what it does not, and the one rule
your handlers must follow.

> [!WARNING]
> **Your handler may run more than once.** taskmill runs every task
> [at least once](glossary.md#at-least-once), not exactly once. Write each
> handler so that a second run does no harm: make it
> [idempotent](glossary.md#idempotent). [Below](#make-your-handler-safe-to-run-twice)
> are three ways to do that.

## What taskmill promises

| Promise | What it means for you |
|---|---|
| **A task commits with your data.** | A task added with `EnqueueTx` exists only if your transaction commits. No worker sees it before the commit. |
| **No task is lost.** | A committed task runs until it is completed or dead, unless you cancel or delete it. Dead tasks stay, so you can see them. |
| **A crashed worker loses nothing.** | Its tasks run again on another worker, about 30 seconds after the crash. |
| **One result per task.** | If two attempts overlap, only the one that holds the [lease](glossary.md#lease) can save its result. A late result is refused. |
| **Database writes can happen exactly once.** | Write your data and complete the task in one transaction, with `CompleteTx`. |
| **Retries stop.** | A failing task waits longer after each failure, then becomes [dead](glossary.md#dead-task) when its attempts run out. It keeps its last 20 errors. |
| **Tasks start in order.** | In a queue, a higher [priority](glossary.md#priority) starts first, then an earlier run time, then the order you added them. Tasks may finish in any order. |
| **FIFO groups keep their order.** | The tasks of a [FIFO group](glossary.md#fifo-group) run one at a time, in order. A dead task is never skipped: the group waits for you. |
| **A dedupe key gives one task.** | Within its window, a [dedupe key](glossary.md#dedupe-key) makes one task, even when two producers race. The window starts at the enqueue. |
| **Delayed tasks start on time.** | They start within about a second of their run time, while a [maintainer](glossary.md#maintainer) runs. This is not real time. |
| **Payloads arrive as you sent them.** | The JSON is decoded straight into your struct, so large integers stay exact. |

## What taskmill does not promise

- **Exactly once in general.** A worker can finish the work, then crash
  before it saves the result. The task then runs again.
- **No overlap.** If a worker loses the database for longer than its lease,
  another worker can start the same task while the first one still runs.
  Only one of them can save a result.
- **One order across workers.** Tasks start in order, but they run at the
  same time, so they can finish in any order.
- **Your last commits after an unsafe failover.** With asynchronous
  replication, a PostgreSQL failover can lose the last commits, and the
  tasks in them. That depends on your database setup, not on taskmill.
- **Exactly-once delivery through a broker.** The
  [outbox](glossary.md#outbox) publishes each message at least once.
  Consumers drop duplicates by message ID.

## When can a task run twice?

| What happens | Why the task runs again | How to make it rare |
|---|---|---|
| A worker crashes after the work, before it saves the result | The lease runs out, and another worker runs the task | You cannot prevent it: make the handler safe to run twice |
| A worker loses the database for longer than its lease | Another worker takes the task over | If your database's failover takes longer than 30 seconds, raise `LeaseDuration` ([Database](../production/database.md)) |
| Handlers use every connection of the pool | The worker cannot renew its leases in time | Give the pool your total concurrency plus 5 connections |
| A handler ignores the end of its context at shutdown | The worker gives the task back, and it starts again elsewhere | Return quickly when `ctx` ends |
| The platform kills the worker before it drains | The same as a crash | Give the process at least `DrainTimeout` plus 10 seconds to stop ([Deploys](../production/deploys.md)) |

## Make your handler safe to run twice

### Which should I use?

| Situation | What to use | Why |
|---|---|---|
| The handler writes only to the same database | `CompleteTx`, in the same transaction as the writes | The writes and the completion commit together: exactly once |
| The handler calls another service, such as a payment API or a mailer | An idempotency key, sent with the call | The other service drops the second call |
| A second run does no harm, such as an upsert or a cache refresh | Nothing | The handler is already idempotent |

With `CompleteTx`, the update and the completion commit together, or not at
all:

```go
func (h *PaymentHandler) Handle(ctx context.Context, t *taskmill.Task[MarkPaid]) error {
	return pgx.BeginFunc(ctx, h.pool, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, "UPDATE orders SET paid = true WHERE id = $1", t.Args.OrderID)
		if err != nil {
			return err
		}
		// Complete the task in the same transaction: both commit, or neither.
		return t.CompleteTx(ctx, taskmill.Pgx(tx))
	})
}
```

If another attempt took the task over, `CompleteTx` returns
`taskmill.ErrLeaseLost`. The transaction then rolls back, so the update is
undone.

For an idempotency key, use your service's name, the taskmill schema, and
the task ID, like `shop/taskmill/42`. The task ID alone is not enough: two
schemas can have the same task ID. [Do work once](../recipes/do-work-once.md)
shows both ways in full.

## Follow the handler rules

taskmill's promises hold only if your handlers follow these rules:

1. **Be idempotent,** or use `CompleteTx`.
2. **Stop when the context ends.** It ends when the task is canceled, its
   lease is lost, its time runs out, or the worker stops.
   `context.Cause(ctx)` says which.
3. **Return errors; don't panic.** taskmill turns a panic into a failed
   attempt, but an error tells you more.
4. **Mark failures that a retry cannot fix** with `taskmill.Permanent(err)`,
   so the task does not use up its attempts for nothing.

## See also

- [Do work once](../recipes/do-work-once.md): idempotency keys and `CompleteTx`, step by step
- [Handle tasks](../guides/handle-tasks.md): what a handler returns, and why its context ends
- [Design: task lifecycle](../design/task-lifecycle.md): how taskmill keeps each promise
- [`Task.CompleteTx`](https://pkg.go.dev/github.com/code19m/taskmill#Task.CompleteTx) on pkg.go.dev

**Next:** [Define tasks](../guides/define-tasks.md)
