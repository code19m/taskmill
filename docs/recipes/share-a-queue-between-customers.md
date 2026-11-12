# Share a queue fairly between customers

**Situation.** "One customer imports 200,000 rows, and we enqueue a task for
every 1,000 rows. Now every other customer's import waits behind those 200
tasks. Each customer should get a fair share of the workers."

## Which should I use?

A queue starts its tasks by priority, and then the task that has waited
longest goes first ([Task order and FIFO groups](../guides/ordering.md)). It
doesn't know your customers, and taskmill has no setting for fairness. You
build it from queues and groups:

| Situation | What to use | Why |
|---|---|---|
| Batch work delays other work, such as receipts | A queue of its own for the batch work | Each queue has its own [slots](../concepts/glossary.md#slot) |
| In one queue, one customer must not take every slot | A `GroupKey` per customer | Only one task per customer runs at a time |
| Customers must take turns, however many are busy | [A task that enqueues the customer's next task](#let-customers-take-turns) | Each new task goes to the end of the line |
| Some customers must go first | A higher `Priority` for their tasks | They start before the others, who can then wait for a long time |

## The recommended way

Put the batch work in a queue of its own:

```go
func (ImportRows) Kind() string { return "import.rows" }

func (ImportRows) EnqueueDefaults() taskmill.EnqueueOptions {
    return taskmill.EnqueueOptions{Queue: "imports"} // away from receipts and other urgent work
}
```

Give all tasks of one customer the same `GroupKey`. They form a
[FIFO group](../concepts/glossary.md#fifo-group):

```go
// enqueueImport enqueues one task per page of 1,000 rows, in the customer's group.
func enqueueImport(ctx context.Context, tx pgx.Tx, client *taskmill.Client, customerID, importID int64, pages int) error {
    params := make([]taskmill.EnqueueParams, pages)
    for page := range params {
        params[page] = taskmill.EnqueueParams{
            Args:    ImportRows{ImportID: importID, Page: page},
            Options: &taskmill.EnqueueOptions{GroupKey: "customer-" + strconv.FormatInt(customerID, 10)},
        }
    }
    _, err := client.EnqueueManyTx(ctx, taskmill.Pgx(tx), params)
    return err
}
```

Then choose how many slots the worker gives to each queue:

```go
worker, err := taskmill.NewWorker(client, &taskmill.WorkerConfig{
    Handlers: &handlers,
    Queues: map[string]taskmill.QueueOptions{
        "default": {Concurrency: 10},
        "imports": {Concurrency: 4}, // in this process, 4 customers import at the same time
    },
})
```

## Why

- **Each queue has its own slots.** Imports can fill the 4 slots of
  `imports`, but never the 10 slots that receipts use.
- **Only one task of a group runs at a time.** So one customer takes one
  slot, however many tasks it has waiting. The other slots stay free for
  other customers.
- **Groups need no setup.** A new customer is a new key.

We tested this with 3 slots. Customer A enqueued 60 tasks, then customer B
enqueued 3:

| Setup | Customer B's first task started |
|---|---|
| One queue, no groups | After all 60 tasks of customer A |
| A group per customer | Right away, next to customer A's first task |

## What taskmill can't do

- **Customers don't take turns.** When more customers are busy than the
  queue has slots, the customers who came first share the slots. In a test
  with 2 slots and 5 busy customers, the fifth customer started only after
  the other four had finished.
- **A group runs one task at a time, never 2 or 5.** For more, use several
  groups per customer, as the next section shows.
- **A worker's queues are fixed when it starts.** So a queue per customer
  fits only a few customers that you know when you deploy.

## Let a customer run several tasks at a time

Spread one customer's tasks over a few groups:

```go
const lanes = 3 // how many tasks of one customer may run at the same time

key := fmt.Sprintf("customer-%d/%d", customerID, page%lanes)
```

Tasks keep their order only inside one of these groups.

## Let customers take turns

Enqueue only the customer's first page. Let each task enqueue the next page
in the transaction that completes it, as
[Split work that takes hours](run-long-tasks.md#split-work-that-takes-hours)
shows. The new task goes to the end of the line, behind every customer who
already waits. So the customers take turns, and you need no group: a
customer has only one task at a time.

In the same test with 2 slots and 5 customers, every customer started
within the first 5 tasks, and all five finished at about the same time.

## Other options

| Option | When it fits | The cost |
|---|---|---|
| A lower `Priority` for batch tasks, in the same queue | Batch tasks are short, and urgent work is rare | A priority keeps no slot free. And a steady stream of urgent tasks makes the batch tasks wait for a long time |
| A queue per plan, such as `imports-paid` and `imports-free` | Paying customers must never wait for the others | Each queue needs slots of its own, also when it is empty |
| `MaxRunning` on the batch queue | The batch work loads something that all workers share, such as your database | It limits all customers together, not each one |

## Watch out

- **A dead task [halts](../concepts/glossary.md#halted-group) its
  customer's group.** The customer's later tasks wait until you retry or
  delete it ([Keep order per key](keep-order-per-key.md#fix-a-halted-group)).
  If one bad page must not block the rest, save the failure in your own
  table and return nil.
- **One group runs slowly,** about 20 short tasks a second with one worker.
  So give each task real work: a page of rows, not a single row.
- **Lag doesn't show a long group.** The tasks that wait behind the
  [head](../concepts/glossary.md#head) of their group don't count in the
  queue's [lag](../concepts/glossary.md#lag). To see how much a customer has
  waiting, call `client.Groups`, or filter the dashboard's task list by the
  group.

## See also

- [Task order and FIFO groups](../guides/ordering.md): the order in which tasks start
- [Workers and queues](../guides/workers-and-queues.md): queues, `Concurrency`, and `MaxRunning`
- [Keep order per key](keep-order-per-key.md): FIFO groups, and how to fix a halted one
- [`EnqueueOptions`](https://pkg.go.dev/github.com/code19m/taskmill#EnqueueOptions) on pkg.go.dev

**Next:** [Keep at most one waiting task per key](one-waiting-task-per-key.md)
