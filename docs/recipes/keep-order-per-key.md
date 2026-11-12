# Keep order per key

**Situation.** "Each order posts a ledger entry for its account. The entries
of one account must post in order, and never two at the same time. Different
accounts may run side by side."

## The recommended way

Give each account's tasks the same `GroupKey`. Tasks with the same key in a
queue form a [FIFO group](../concepts/glossary.md#fifo-group):

```go
_, err := client.EnqueueTx(ctx, taskmill.Pgx(tx), entry, &taskmill.EnqueueOptions{
    GroupKey: "account-" + strconv.FormatInt(entry.AccountID, 10), // one group per account
})
```

In the handler, return `Permanent` for an entry that can never post. This
stops the group, so that no later entry posts before it:

```go
func postEntry(ctx context.Context, t *taskmill.Task[PostLedgerEntry]) error {
    if t.Args.Cents < 0 {
        // Dead right away. The account's later entries wait behind it.
        return taskmill.Permanent(fmt.Errorf("order %d: negative amount", t.Args.OrderID))
    }
    return book(ctx, t.Args)
}
```

## Why

- Only the group's first unfinished task, its
  [head](../concepts/glossary.md#head), can run. The next task starts when
  the head finishes. Groups need no setup, and the benchmarks run 100,000
  of them in one queue ([Performance](../reference/performance.md)).
- A failed head is retried before anything behind it runs. So a short outage
  doesn't change the order.
- If the head becomes dead, the group stops and waits for a person: it is a
  [halted group](../concepts/glossary.md#halted-group). taskmill never skips
  a dead task, because that would break the order.

## Other options

| Option | When it fits | The cost |
|---|---|---|
| A higher `Priority` | Some tasks only need to *start* first | Tasks still run at the same time, and can finish in any order |
| A queue with `MaxRunning: 1` | Every task in the queue must wait for the one before | No two accounts run at the same time. And a failed task waits for its retry while later tasks run, so the order breaks anyway |
| `Concurrency: 1` for the queue | Never, for order | It limits one process: two replicas run two tasks at the same time |
| No group | A stuck account is worse than entries out of order | Entries can post out of order, and two can run at the same time |

## Fix a halted group

1. Find the cause in the dead task's errors. In the dashboard, open the dead
   task, and choose **View group** to see what waits behind it.
2. Fix the cause: deploy a fix, or correct the data.
3. Retry the dead task. It runs again as the head, and the group continues
   in order. Delete it instead only if that step must never run.

[Fix a halted group](../guides/ordering.md#fix-a-halted-group) shows how to
do this from code. Did one bug halt many groups? See
[Recover from an incident](recover-from-an-incident.md). To try it, run the
[shop example](../../examples/shop/README.md): its 13th order halts a group
on purpose.

## Watch out

- **Alert on halted groups.** A halted group waits for a person, and its
  waiting tasks don't count in the queue's
  [lag](../concepts/glossary.md#lag). Alert when
  `taskmill_queue_halted_groups` is above 0
  ([Monitoring](../production/monitoring.md)).
- **One group runs slowly.** Its tasks run one after the other. Even tasks
  that take no time run at only about 20 a second with one worker. With
  thousands of tasks waiting for one key, expect minutes, not seconds.
- **Keep the transactions that enqueue into a group short.**
  [Producers](../concepts/glossary.md#producer) of one group wait for each
  other's transactions to commit.
- **A group lives in one queue.** The same key in another queue is another
  group. And a snoozed head blocks its group until it runs again.

## See also

- [Task order and FIFO groups](../guides/ordering.md): priorities and groups in detail
- [The shop example](../../examples/shop/README.md): a halted group to try in the dashboard
- [`EnqueueOptions.GroupKey`](https://pkg.go.dev/github.com/code19m/taskmill#EnqueueOptions) on pkg.go.dev

**Next:** [Share a queue fairly between customers](share-a-queue-between-customers.md)
