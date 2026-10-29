# Avoid duplicate tasks

A dedupe key stops the same task from being enqueued twice. Use one when a
client retries a request, a broker delivers a message again, or some work
must happen only once a day.

## Quick example

```go
res, err := client.Enqueue(ctx, SendReceipt{OrderID: 42}, &taskmill.EnqueueOptions{
    DedupeKey: "receipt-42",
})
if err != nil {
    return err
}
if res.Duplicate {
    log.Printf("receipt 42 is already enqueued as task %d", res.ID)
}
```

A duplicate is **not** an error. `Duplicate` is true, no task is created,
and `ID` is the task that already holds the key. So you can enqueue again
without checking first: a retried request or a redelivered message does no
harm.

## How it works

- A [dedupe key](../concepts/glossary.md#dedupe-key) allows one task while
  its *window* lasts: 24 hours by default. Set `DedupeWindow` to change it.
- A key belongs to one queue and one kind. The same key in another queue,
  or with another kind, is a different key.
- Name the work in the key, not the moment: `receipt-42`, a message ID, or
  `sales-report-2026-10-29`.
- It works in every enqueue call, batches included. In one batch, a key
  that comes again is a duplicate of its first use.
- Two producers can enqueue the same key at the same time. The second waits
  for the first one's transaction, so only one task is created. Keep these
  transactions short.

## Set the window for a task that runs later

The window starts when you **enqueue** the task, not at its `RunAt`.

> [!WARNING]
> For a task that runs later, the key can be free again before the task
> even runs. Then a second enqueue creates a second task, and the work
> happens twice. Set `DedupeWindow` to at least the delay, plus the time the
> task may need to run and retry.

```go
_, err := client.EnqueueTx(ctx, taskmill.Pgx(tx), RemindCustomer{OrderID: id}, &taskmill.EnqueueOptions{
    RunAt:        time.Now().Add(48 * time.Hour),
    DedupeKey:    "remind-" + strconv.FormatInt(id, 10),
    DedupeWindow: 72 * time.Hour, // counts from now, so it must cover the 48 hours of delay
})
```

## When is a key free again?

| The task that holds the key is… | The same key can be enqueued again… |
|---|---|
| Waiting, running, completed, or dead | When the window ends, even if the task has not run yet |
| Canceled or deleted | Right away |

Do you want "at most one waiting task per product", such as for a cache
refresh? Put a time bucket in the key, such as
`refresh-product-7-2026-10-29T10`, with a window of one hour.

## See also

- [Consume from a broker](../recipes/consume-from-a-broker.md): a dedupe key per message
- [Task order and FIFO groups](ordering.md): combine a dedupe key with a group
- [`EnqueueOptions`](https://pkg.go.dev/github.com/code19m/taskmill#EnqueueOptions) and [`EnqueueResult`](https://pkg.go.dev/github.com/code19m/taskmill#EnqueueResult) on pkg.go.dev

**Next:** [Run tasks on a schedule](schedules.md)
