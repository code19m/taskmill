# Handle tasks in batches

A [batch handler](../concepts/glossary.md#batch-handler) gets many tasks of
one kind in one call. Use it for work that is much cheaper in bulk.

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

| Situation | What to use | Why |
|---|---|---|
| One call for many tasks is much cheaper: a bulk insert, a bulk API | `AddBatchHandler` | Far fewer calls to the other system |
| Each task is slow, or calls a different service | `AddHandler` | Each task gets its own time limit, and can be canceled |
| You are not sure | `AddHandler` | It is simpler |

## How it works

- When a worker fetches a task of a batch kind, it adds other waiting tasks
  of that kind, up to `BatchSize`: 100 by default. It doesn't wait for more
  to arrive, so when tasks are few, batches are small.
- A batch takes one place in its queue's `Concurrency`. `Timeout` limits the
  whole batch.
- A task without a result fails. If the handler panics, every task fails.
  If a task's payload does not decode, that task becomes dead, and the
  rest run.
- `MaxRunning` counts tasks, not batches. So it also limits how full a batch
  gets: with `MaxRunning: 4`, a batch holds at most 4 tasks.

## Cancel a task that runs in a batch

The batch's context ends only when the batch times out, or the worker
stops. A batch handler can't see a cancel request for one of its tasks.
The result it returns for that task decides:

| The batch returns for the canceled task | The task becomes |
|---|---|
| nil | `completed`, because the work was done |
| An error | `canceled` |

If a cancel must stop the work, use `AddHandler`.

## See also

- [Handle tasks](handle-tasks.md): what each result does
- [Workers and queues](workers-and-queues.md): `Concurrency` and `MaxRunning`
- [`BatchHandler`](https://pkg.go.dev/github.com/code19m/taskmill#BatchHandler) and [`Middleware`](https://pkg.go.dev/github.com/code19m/taskmill#Middleware) on pkg.go.dev: lost leases and middleware in a batch

**Next:** [Workers and queues](workers-and-queues.md)
