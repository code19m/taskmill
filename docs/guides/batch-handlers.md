# Handle tasks in batches

A [batch handler](../concepts/glossary.md#batch-handler) gets many tasks of one kind in
one call. Use it for work that is much cheaper in bulk.

## Quick example

```go
type ProductIndexer struct{ search SearchClient }

func (p *ProductIndexer) HandleBatch(ctx context.Context, tasks []*taskmill.Task[IndexProduct]) []error {
    ids := make([]int64, len(tasks))
    for i, t := range tasks {
        ids[i] = t.Args.ProductID
    }
    err := p.search.IndexProducts(ctx, ids) // one request for the whole batch
    errs := make([]error, len(tasks))      // one result per task, in order
    for i := range errs {
        errs[i] = err
    }
    return errs
}

taskmill.AddBatchHandler[IndexProduct](&handlers, &ProductIndexer{search: search},
    &taskmill.HandlerOptions{BatchSize: 200})
```

Return one result per task, in the same order as `tasks`. Each result works
like a normal handler's: nil completes that task, an error retries it, and
`Permanent` and `Snooze` work too.

## Which should I use?

| Situation | Use | Why |
|---|---|---|
| One call for many tasks is much cheaper: a bulk insert, a bulk API | `AddBatchHandler` | Far fewer calls to the other system |
| Each task is slow, or calls a different service | `AddHandler` | Each task gets its own time limit and context, and can be canceled |
| You are not sure | `AddHandler` | It is simpler |

## How it works

- When a worker fetches a task of a batch kind, it adds more waiting tasks
  of that kind, up to `BatchSize`: 100 by default, at most 1,000.
- It doesn't wait for more tasks to arrive. A batch holds what is waiting
  now, so when tasks are few, batches are small.
- A batch counts as one task toward its queue's `Concurrency`.
- `Timeout` is the time limit of the whole batch.
- If the handler returns fewer results than tasks, the tasks without a
  result fail. If it panics, every task of the batch fails.
- If a task's payload does not decode, the task never reaches the
  handler. It becomes dead, and the rest of the batch runs.
- Middleware runs once per task. But the context that middleware passes on
  does not reach the batch handler.
- A batch kind can share a queue with other kinds. Give a busy batch kind a
  queue of its own if it must not take places from other work.

## Cancel a task that runs in a batch

The batch's context ends only when the batch times out, or the worker
stops. When someone cancels one task of a running batch, the batch's
context goes on, and the handler can't see the request. The task's result
decides:

| The batch returns for the canceled task | The task becomes |
|---|---|
| nil | `completed`, because the work was done |
| An error | `canceled` |

So a canceled task in a batch usually completes. If a cancel must stop the
work, use `AddHandler`.

A task that loses its lease during the batch works the same way: the
handler can't see it. taskmill then ignores that task's result.

## MaxRunning also limits the batch size

`MaxRunning` is a limit for all workers on how many of a queue's tasks run
at the same time ([Workers and queues](workers-and-queues.md#limit-how-many-tasks-run)).
It counts tasks, not batches. So a batch takes only as many tasks as the
limit has room for.

> [!NOTE]
> With `MaxRunning: 4` and `BatchSize: 100`, a batch holds at most 4 tasks.
> If other tasks of the queue are running, it holds fewer.

## See also

- [Handle tasks](handle-tasks.md): what each result does
- [Workers and queues](workers-and-queues.md): `Concurrency` and
  `MaxRunning`
- [`BatchHandler`](https://pkg.go.dev/github.com/code19m/taskmill#BatchHandler)
  and [`AddBatchHandler`](https://pkg.go.dev/github.com/code19m/taskmill#AddBatchHandler)
  on pkg.go.dev

**Next:** [Workers and queues](workers-and-queues.md)
