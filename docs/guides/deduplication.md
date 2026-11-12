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
and `ID` names the task that took the key. So you can enqueue again without
checking first: a retried request or a redelivered message does no harm.

## How it works

- A [dedupe key](../concepts/glossary.md#dedupe-key) allows one task while
  its *window* lasts: 24 hours by default. Set `DedupeWindow` to change it.
- A key belongs to one queue and one kind. The same key in another queue,
  or with another kind, is a different key.
- It works in every enqueue call, batches included. In one batch, a key
  that comes again is a duplicate of its first use.
- Two [producers](../concepts/glossary.md#producer) can enqueue the same key
  at the same time. The second waits for the first one's transaction, so
  only one task is created. Keep these transactions short.
- The task that took the key can be gone when you look for it. A completed
  task is deleted when its history is dropped, or right away if its kind
  uses `DiscardCompleted`. Then `client.Get` returns `ErrNotFound` for that
  `ID`.

## Choose the key and the window

Name the work in the key, not the moment of the call:

| You want… | Key | Window |
|---|---|---|
| One task per request, even if the client sends it twice | The ID of what the request creates: `receipt-42` | The default |
| One task per message from a broker | The message ID ([Consume from a broker](../recipes/consume-from-a-broker.md)) | Longer than the broker may deliver it again |
| Work once per period, such as a daily report | The period: `sales-report-2026-10-29` | As long as a repeat can still arrive: 48 hours for a day |
| At most one refresh per product and hour | A time bucket: `refresh-product-7-2026-10-29T10` | 1 hour |
| One delayed task per thing, such as a reminder | The thing: `remind-42` | The delay, plus the time to run: see below |

A key lasts for its window, also after its task completed. So a key can't
mean "one task until it is done". A time bucket, as in the fourth row, comes
close: the next bucket allows the next task.

But a change that comes after the task ran, in the same bucket, gets no
task. If the last change must not be lost, follow
[Keep at most one waiting task per key](../recipes/one-waiting-task-per-key.md).

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

> [!WARNING]
> A canceled task gave its key away, and a retry does not take the key
> back. So if you cancel a task, enqueue its key again, and then retry the
> canceled task, both tasks run. To run canceled work again, enqueue it
> again with the same key, and leave the canceled task alone. `Duplicate`
> then tells you if another task already took the key.

The same can happen when you retry a dead task after its window ended.

## Replace a waiting task

Because a cancel frees the key, one transaction can replace a task. Here, a
reminder moves to a new time:

```go
err := pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
    if err := client.CancelTx(ctx, taskmill.Pgx(tx), oldReminderID); err != nil {
        return err
    }
    _, err := client.EnqueueTx(ctx, taskmill.Pgx(tx), RemindCustomer{OrderID: id}, &taskmill.EnqueueOptions{
        RunAt:        newTime,
        DedupeKey:    "remind-" + strconv.FormatInt(id, 10), // free again after the cancel
        DedupeWindow: time.Until(newTime) + 24*time.Hour,
    })
    return err
})
```

If the old reminder already finished, `CancelTx` returns `ErrInvalidState`,
and nothing changes.

## See also

- [Consume from a broker](../recipes/consume-from-a-broker.md): a dedupe key per message
- [Use your own transactions](transactions.md#cancel-a-task-in-the-same-transaction): `CancelTx`
- [Task order and FIFO groups](ordering.md): combine a dedupe key with a group
- [`EnqueueOptions`](https://pkg.go.dev/github.com/code19m/taskmill#EnqueueOptions) and [`EnqueueResult`](https://pkg.go.dev/github.com/code19m/taskmill#EnqueueResult) on pkg.go.dev

**Next:** [Run tasks on a schedule](schedules.md)
