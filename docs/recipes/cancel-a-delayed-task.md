# Cancel or replace a delayed task

**Situation.** "We remind a customer 2 days after an order. If the customer
pays first, the reminder must not go out. If the customer asks for more
time, the reminder must move to a later day."

## Which should I use?

| Situation | What to use | Why |
|---|---|---|
| The task can check by itself that it is no longer needed | A check in the handler, which then returns nil | Nothing to store. It is right even when a cancel comes too late |
| The task must stop right away, or another task replaces it | `CancelTx` in the transaction that changes your data | The cancel commits with your change, or not at all |

Use both: cancel the task when your data changes, and still check in the
handler.

## The recommended way

When you enqueue the delayed task, keep its ID next to your data:

```go
res, err := client.EnqueueTx(ctx, taskmill.Pgx(tx), RemindCustomer{OrderID: orderID}, &taskmill.EnqueueOptions{
    RunAt: time.Now().Add(48 * time.Hour),
})
if err != nil {
    return err
}
_, err = tx.Exec(ctx, "UPDATE orders SET reminder_task_id = $1 WHERE id = $2", res.ID, orderID)
```

Cancel the task in the transaction that makes it useless:

```go
// markPaid marks the order paid, and cancels its reminder with it.
func markPaid(ctx context.Context, pool *pgxpool.Pool, client *taskmill.Client, orderID int64) error {
    return pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
        var reminderID int64 // 0 if the order has no reminder
        err := tx.QueryRow(ctx, `UPDATE orders SET paid_at = now() WHERE id = $1
            RETURNING coalesce(reminder_task_id, 0)`, orderID).Scan(&reminderID)
        if err != nil {
            return err
        }
        err = client.CancelTx(ctx, taskmill.Pgx(tx), reminderID)
        if errors.Is(err, taskmill.ErrInvalidState) || errors.Is(err, taskmill.ErrNotFound) {
            return nil // the reminder already ran, or is gone: nothing to cancel
        }
        return err
    })
}
```

In the handler, check that the work is still needed:

```go
func (r *Reminder) Handle(ctx context.Context, t *taskmill.Task[RemindCustomer]) error {
    var paid bool
    err := r.pool.QueryRow(ctx, "SELECT paid_at IS NOT NULL FROM orders WHERE id = $1", t.Args.OrderID).Scan(&paid)
    if err != nil || paid {
        return err // nil if the order is paid: no reminder
    }
    return r.mailer.SendReminder(ctx, t.Args.OrderID)
}
```

## Why

- **`CancelTx` is part of your transaction.** If the payment rolls back, the
  reminder stays. If it commits, the reminder is canceled at the same
  moment.
- **A canceled task is kept as a record,** 30 days by default. You can see
  in the [dashboard](../concepts/glossary.md#dashboard) that the reminder
  existed, and when it was canceled.
- **A cancel can come too late.** For a task that already finished,
  `CancelTx` returns `ErrInvalidState`, or `ErrNotFound` once its history
  is gone. A task that runs right now gets a canceled context, but its
  email may be out already. So the handler checks too.

## Replace the task

To move the reminder, cancel the old task and enqueue a new one in one
transaction:

```go
// moveReminder replaces the order's reminder with one that runs at the new time.
func moveReminder(ctx context.Context, tx pgx.Tx, client *taskmill.Client, orderID int64, at time.Time) error {
    var oldID int64 // FOR UPDATE: two moves of one order wait for each other
    err := tx.QueryRow(ctx, `SELECT coalesce(reminder_task_id, 0) FROM orders
        WHERE id = $1 FOR UPDATE`, orderID).Scan(&oldID)
    if err != nil {
        return err
    }
    err = client.CancelTx(ctx, taskmill.Pgx(tx), oldID)
    if err != nil && !errors.Is(err, taskmill.ErrInvalidState) && !errors.Is(err, taskmill.ErrNotFound) {
        return err
    }
    res, err := client.EnqueueTx(ctx, taskmill.Pgx(tx), RemindCustomer{OrderID: orderID}, &taskmill.EnqueueOptions{RunAt: at})
    if err != nil {
        return err
    }
    _, err = tx.Exec(ctx, "UPDATE orders SET reminder_task_id = $1 WHERE id = $2", res.ID, orderID)
    return err
}
```

If the reminder has a [dedupe key](../concepts/glossary.md#dedupe-key), the
new task can use the same key. A cancel frees the key right away, also
inside your transaction.

## Other options

| Option | When it fits | The cost |
|---|---|---|
| Only the check in the handler | A task that waits until its time does no harm | The task stays in the queue until then. To move it, the handler must compare times, and [snooze](../concepts/glossary.md#snooze) |
| `Cancel`, with no transaction | Admin tools, and code with no transaction of its own | Your change can commit while the cancel fails, or the other way around |
| `CancelMatching` with a `GroupKey` per order | You can't store the task's ID | It runs in its own transactions. And tasks with the same key run one at a time |

## Watch out

- **Don't return `ErrInvalidState` or `ErrNotFound` from your transaction.**
  They mean that nothing was left to cancel. Your transaction is still
  usable after them, so commit your own change.
- **Don't retry a canceled reminder.** A retried task runs right away, not
  at its old time. And if a newer task took its dedupe key, both run.
- **Keep the transaction short.** `CancelTx` locks the task's row until you
  commit ([Use your own transactions](../guides/transactions.md)).
- **Run the transaction again after a deadlock.** When another transaction
  changes the same task, or its FIFO group, at the same moment, PostgreSQL
  may end yours with `deadlock detected` (SQLSTATE `40P01`). Nothing was
  saved. `Cancel` runs again by itself, but `CancelTx` is in your
  transaction, so only your code can.

## See also

- [Manage tasks from code](../guides/operations-api.md): what `Cancel` does in each state
- [Avoid duplicate tasks](../guides/deduplication.md): a dedupe key for a task that runs later
- [Handle tasks](../guides/handle-tasks.md): what a handler sees when its task is canceled
- [`Client.CancelTx`](https://pkg.go.dev/github.com/code19m/taskmill#Client.CancelTx) on pkg.go.dev

**Next:** [Run a task at each customer's local time](run-at-each-customers-local-time.md)
