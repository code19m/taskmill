# Keep at most one waiting task per key

**Situation.** "Every change to a product enqueues a task that rebuilds its
search entry. An import changes one product 50 times, and we rebuild it 50
times. One rebuild that sees the last change is enough."

## Which should I use?

| Situation | What to use | Why |
|---|---|---|
| The same request or message can arrive twice | A `DedupeKey` | [Avoid duplicate tasks](../guides/deduplication.md) shows how |
| Many changes to one thing need one run, soon, that sees the last change | **A flag in your own row** | It is exact, and the run starts right away |
| The run may wait a minute, and the changes come in bursts | [A delayed task with a `DedupeKey`](#wait-for-a-burst-to-end) | No new column |

A [dedupe key](../concepts/glossary.md#dedupe-key) alone can't do this. The
key stays taken until its window ends, also after the task completed. A
change that comes after the run, but inside the window, gets no task, and
the search entry stays old.

## The recommended way

Add a flag to your own table. It says that a refresh waits:

```sql
ALTER TABLE products ADD COLUMN refresh_queued boolean NOT NULL DEFAULT false;
```

In the transaction that changes the product, enqueue a refresh only if none
waits:

```go
// queueRefresh enqueues a refresh of the product, unless one already waits.
func queueRefresh(ctx context.Context, tx pgx.Tx, client *taskmill.Client, productID int64) error {
    var queued bool // FOR UPDATE: the handler waits for this transaction before it reads
    err := tx.QueryRow(ctx, "SELECT refresh_queued FROM products WHERE id = $1 FOR UPDATE", productID).Scan(&queued)
    if err != nil || queued {
        return err // a refresh already waits: it will see this change
    }
    if _, err := tx.Exec(ctx, "UPDATE products SET refresh_queued = true WHERE id = $1", productID); err != nil {
        return err
    }
    _, err = client.EnqueueTx(ctx, taskmill.Pgx(tx), RefreshProduct{ProductID: productID}, &taskmill.EnqueueOptions{
        GroupKey: "product-" + strconv.FormatInt(productID, 10), // one refresh of a product at a time
    })
    return err
}
```

The handler clears the flag first, and then reads the product:

```go
func (r *Refresher) Handle(ctx context.Context, t *taskmill.Task[RefreshProduct]) error {
    // From here on, a change enqueues a new refresh.
    _, err := r.pool.Exec(ctx, "UPDATE products SET refresh_queued = false WHERE id = $1", t.Args.ProductID)
    if err != nil {
        return err
    }
    return r.rebuild(ctx, t.Args.ProductID) // reads the product as it is now
}
```

## Why

- **The flag and the task commit with your change.** So a set flag always
  has a waiting task behind it, and a rollback leaves neither.
- **No change is lost.** A change that commits before the handler clears
  the flag is in the data that the handler reads. A change after that finds
  the flag cleared, and enqueues the next refresh.
- **The `GroupKey` keeps refreshes of one product apart.** They form a
  [FIFO group](../concepts/glossary.md#fifo-group), and run one at a time.
  Without it, a slow old refresh could finish after a newer one, and write
  older data.
- **At most two unfinished tasks exist per product:** one that runs, and
  one that waits.

In a test, 4 writers made 400 changes to one product while a worker ran.
That made about 20 tasks, and the search entry had the last change.

## Wait for a burst to end

If the run may wait, you need no flag. Enqueue a delayed task with a
`DedupeKey`, and make the key's window a little shorter than the delay:

```go
const quiet = time.Minute // the refresh runs this long after the first change

_, err := client.EnqueueTx(ctx, taskmill.Pgx(tx), RefreshProduct{ProductID: productID}, &taskmill.EnqueueOptions{
    RunAt:        time.Now().Add(quiet),
    DedupeKey:    "refresh-product-" + strconv.FormatInt(productID, 10),
    DedupeWindow: quiet - 5*time.Second, // the key is free before the task can start
})
```

Every change in the window shares one task. That task starts after the
window, so it reads all of those changes. The 5 seconds are a safety margin.
Make it longer than your transactions take, plus the difference between
your app's clock and the database's clock.

## Other options

| Option | When it fits | The cost |
|---|---|---|
| A `DedupeKey` with a time in it, such as `refresh-7-2026-10-29T10`, and no delay | The work may happen at most once an hour, and may miss the last changes | A change after the run, in the same hour, gets no task |
| One task per change | Changes are rare, or the work is cheap | Every change runs the work again |

## Watch out

- **Keep the flag and the task together.** If you cancel or delete a
  waiting refresh, also set `refresh_queued` back to false. Otherwise no
  change enqueues a refresh for that product again.
- **A dead refresh [halts](../concepts/glossary.md#halted-group) its
  product's group.** Later refreshes of that product wait behind it until
  you retry it ([Keep order per key](keep-order-per-key.md#fix-a-halted-group)).
- **The refresh can still run twice,** like every task. Make the rebuild
  safe to repeat ([Do work once](do-work-once.md)).

## See also

- [Avoid duplicate tasks](../guides/deduplication.md): dedupe keys and their windows
- [Use your own transactions](../guides/transactions.md): enqueue in the transaction that changes your data
- [Keep order per key](keep-order-per-key.md): FIFO groups
- [`EnqueueOptions`](https://pkg.go.dev/github.com/code19m/taskmill#EnqueueOptions) on pkg.go.dev

**Next:** [Cancel or replace a delayed task](cancel-a-delayed-task.md)
