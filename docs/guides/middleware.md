# Add middleware

[Middleware](../concepts/glossary.md#middleware) is code of yours that runs
around every attempt, or around every enqueue. Use it for what all your
tasks share: metrics, log lines, tracing, or a tag on each task.

## Quick example

```go
// Timing records how long each attempt ran, and if it worked.
func Timing(ctx context.Context, task *taskmill.TaskInfo, next func(context.Context) error) error {
    start := time.Now()
    err := next(ctx) // runs the next middleware, and then the handler
    recordAttempt(task.Queue, task.Kind, time.Since(start), err == nil)
    return err
}

worker, err := taskmill.NewWorker(client, &taskmill.WorkerConfig{
    Handlers:   &handlers,
    Middleware: []taskmill.Middleware{Timing},
})
```

## Which should I use?

| Situation | What to use | Why |
|---|---|---|
| Traces and metrics with OpenTelemetry | The `otel` module: see [Logs, traces, and metrics](observability.md) | Its middleware is written and tested |
| Code around every attempt: your own metrics, a log line, a value in the context | `Middleware`, in `WorkerConfig` | It runs for every kind on that worker |
| A tag on every task you enqueue: a tenant, a request ID | `EnqueueMiddleware`, in `ClientConfig` | It runs for every enqueue of that client |
| Logic for one kind | That kind's handler | Middleware runs for all kinds |

## Wrap every attempt

Attempt middleware gets the task as a `TaskInfo`, and `next`. The error that
`next` returns is the handler's result. A handler's panic arrives as an
error, too.

The context that you pass to `next` is the handler's context. So middleware
can hand values to every handler. This one passes on the tenant from the
task's [metadata](enqueue.md#tag-a-task-with-metadata):

```go
type tenantKey struct{}

// WithTenant puts the task's tenant in the handler's context.
func WithTenant(ctx context.Context, task *taskmill.TaskInfo, next func(context.Context) error) error {
    return next(context.WithValue(ctx, tenantKey{}, task.Metadata["tenant"]))
}
```

`Worker.RunTask` runs the middleware too, so you can
[test it](testing.md#test-retries) without a running worker.

## Tag every task you enqueue

Enqueue middleware gets the tasks of one enqueue call, and may add to their
metadata. This one stores the tenant of the current request in each task:

```go
// TagTenant stores the request's tenant in the metadata of each task.
func TagTenant(ctx context.Context, tasks []*taskmill.EnqueueTask, next func(context.Context) error) error {
    if tenant, ok := ctx.Value(tenantKey{}).(string); ok {
        for _, t := range tasks {
            t.Metadata["tenant"] = tenant
        }
    }
    return next(ctx) // stores the tasks
}

client, err := taskmill.NewClient(ctx, pool, &taskmill.ClientConfig{
    Logger:            logger,
    EnqueueMiddleware: []taskmill.EnqueueMiddleware{TagTenant},
})
```

- With `TagTenant` on the client and `WithTenant` on the worker, the tenant
  travels from the request to the handler.
- The middleware also runs for the tasks that
  [schedules](schedules.md) enqueue.
- After `next` returns nil, each task's `Result` holds its ID, and
  `Duplicate`.
- Inside your own transaction, `next` returns before you commit. The tasks
  exist only if you commit.

## Follow the rules

- **Call `next` once, and return its error.** Attempt middleware that
  returns nil without calling `next` fails the attempt, and the task is
  retried. Enqueue middleware that does so fails the call, and stores
  nothing.
- **To skip a task on purpose,** return `taskmill.Snooze(d)` or
  `taskmill.Permanent(err)` without calling `next`. The handler does not
  run.
- **Don't change the `TaskInfo`.** In enqueue middleware, change only
  `Metadata`: taskmill ignores changes to the other fields.
- **The order matters.** The first middleware in a list is the outermost:
  it starts first and ends last.

## Middleware and batch handlers

For a [batch handler](batch-handlers.md), attempt middleware runs once per
task, not once per batch:

- `next` returns that task's result when the batch ends.
- The context that you pass to `next` does not reach the batch handler. So
  `WithTenant` above has no effect there: read `t.Metadata` in the handler.
- A task whose middleware returns without calling `next` is left out of the
  batch.

## See also

- [Logs, traces, and metrics](observability.md): the `otel` module, which is built on these two hooks
- [Enqueue tasks](enqueue.md#tag-a-task-with-metadata): set metadata in one enqueue call
- [`Middleware`](https://pkg.go.dev/github.com/code19m/taskmill#Middleware) and [`EnqueueMiddleware`](https://pkg.go.dev/github.com/code19m/taskmill#EnqueueMiddleware) on pkg.go.dev

**Next:** [Workers and queues](workers-and-queues.md)
