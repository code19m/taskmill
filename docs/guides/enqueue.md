# Enqueue tasks

Add tasks to a queue: one or many, to run now, later, or before others.

## Quick example

```go
res, err := client.Enqueue(ctx, SendReceipt{OrderID: 42}, nil)
if err != nil {
    return err
}
log.Printf("enqueued task %d", res.ID)
```

When `Enqueue` returns, the task is saved in PostgreSQL. A worker that
reads its [queue](../concepts/glossary.md#queue) then picks it up.

## Which should I use?

| Situation | What to use | Why |
|---|---|---|
| A database write causes the task: an order, a payment, a sign-up | `EnqueueTx`, in the same transaction | The task exists only if your write commits. A rolled-back order never gets a receipt |
| Nothing is written, or the write is in another database | `Enqueue` | It saves the task in a transaction of its own |
| Many tasks: one per customer, or an import | `EnqueueManyTx` or `EnqueueMany` | One database call for up to 10,000 tasks |

We recommend `EnqueueTx` whenever your code also writes data. See
[Use your own transactions](transactions.md).

## Run a task later

```go
_, err := client.Enqueue(ctx, RemindCustomer{OrderID: 42}, &taskmill.EnqueueOptions{
    RunAt: time.Now().Add(48 * time.Hour),
})
```

- The task waits as `scheduled` until `RunAt`, its
  [run time](../concepts/glossary.md#run-time). It can start up to about a
  second late. A `RunAt` in the past means "now".
- Use `RunAt` for times that depend on your data, such as two days after
  each order. For a fixed timetable, such as every night at 03:00, use a
  [schedule](schedules.md).

## Run a task before others

```go
_, err := client.Enqueue(ctx, SendReceipt{OrderID: 42}, &taskmill.EnqueueOptions{
    Priority: taskmill.PriorityHigh,
})
```

- In a queue, a waiting task with a higher
  [priority](../concepts/glossary.md#priority) starts first. There are four:
  `PriorityLow`, `PriorityNormal` (the default), `PriorityHigh`, and
  `PriorityCritical`.
- A priority does not keep [slots](../concepts/glossary.md#slot) free for
  urgent work: while every slot is busy, an urgent task waits too. For work
  that must never wait, use a queue of its own.
- [Task order and FIFO groups](ordering.md) explains the full start order.

## Tag a task with metadata

`Metadata` holds small text tags that describe a task, such as the customer
or the request that it belongs to:

```go
_, err := client.Enqueue(ctx, SendReceipt{OrderID: 42}, &taskmill.EnqueueOptions{
    Metadata: map[string]string{"tenant": "acme"},
})
```

The handler reads them from `t.Metadata`, and the dashboard shows them on
the task's page:

```go
func (s *ReceiptSender) Handle(ctx context.Context, t *taskmill.Task[SendReceipt]) error {
    log.Printf("send the receipt of order %d for tenant %s", t.Args.OrderID, t.Metadata["tenant"])
    return s.mailer.SendReceipt(ctx, t.Args.OrderID)
}
```

- Put what the work needs in the payload. Use metadata for tags that many
  kinds share, such as a tenant, a request ID, or a trace ID.
- To add the same tag to every task, use enqueue middleware: see
  [Add middleware](middleware.md).
- All tags of one task can take up to 64 KiB.

## Options

| Option | What it does | Default |
|---|---|---|
| `Queue` | The queue to add the task to | `"default"` |
| `Priority` | Which waiting task of the queue starts first: `PriorityLow`, `PriorityNormal`, `PriorityHigh`, or `PriorityCritical` | `PriorityNormal` |
| `RunAt` | The earliest time the task may start | Now |
| `MaxAttempts` | How many times the task may run: 1 to 10,000 | 25 |
| `GroupKey` | Runs the tasks of one key in one queue one at a time, in order ([ordering](ordering.md)) | None |
| `DedupeKey` | Creates no new task if this kind already used this key in this queue within `DedupeWindow` ([deduplication](deduplication.md)) | None |
| `DedupeWindow` | How long the key blocks duplicates, from the enqueue | 24 hours |
| `Metadata` | Text tags saved with the task and passed to its handler, such as a trace ID | None |

A zero value means "not set". For each option, the first value that is set
wins:

1. the option in the enqueue call;
2. the kind's `EnqueueDefaults` ([Define tasks](define-tasks.md#set-defaults-per-kind));
3. taskmill's default.

`Metadata` merges key by key: the call's keys win, and the other keys of
both are kept. An invalid option fails with `ErrInvalidArgument`. The exact
limits are in [Defaults and limits](../reference/defaults-and-limits.md).

## Read the result

Each call returns an `EnqueueResult` per task:

- `ID` is the task's ID. Keep it if you want to cancel the task later, or
  to find it with `client.Get`.
- `Duplicate` is true when a dedupe key was already taken. Then no task was
  created, and `ID` names the task that took the key:
  [Avoid duplicate tasks](deduplication.md) says more.

## Enqueue many tasks in one call

```go
params := make([]taskmill.EnqueueParams, 0, len(orders))
for _, o := range orders {
    params = append(params, taskmill.EnqueueParams{Args: SendReceipt{OrderID: o.ID}})
}
for chunk := range slices.Chunk(params, 10_000) { // each chunk commits on its own
    if _, err := client.EnqueueMany(ctx, chunk); err != nil {
        return err
    }
}
```

- One call is one transaction: it saves all of its tasks, or none. Results
  come back in the same order as `params`.
- One call takes up to 10,000 tasks and 64 MiB of payload and metadata. A
  bigger call fails with `ErrInvalidArgument`, so split big sets into chunks.
- If the chunks must commit together, pass the same transaction to
  `EnqueueManyTx` for each chunk.
- One call for many tasks is much faster than one call per task. See
  [Performance](../reference/performance.md).

## See also

- [Use your own transactions](transactions.md): `EnqueueTx` with pgx, `database/sql`, bun, or sqlx
- [Avoid duplicate tasks](deduplication.md) and [Task order and FIFO groups](ordering.md)
- [`EnqueueOptions`](https://pkg.go.dev/github.com/code19m/taskmill#EnqueueOptions) on pkg.go.dev

**Next:** [Use your own transactions](transactions.md)
