# Logs, traces, and metrics

This page shows how to see what taskmill does: logs through `log/slog`,
traces and metrics through OpenTelemetry, and queue numbers from the
`Client`. For the alerts to set on them, see
[Monitoring](../production/monitoring.md).

## Which should I use?

| Situation | Use | Why |
|---|---|---|
| Every app | Your `*slog.Logger`, passed to taskmill | By default, taskmill writes no logs at all. |
| You use OpenTelemetry | The `otel` module | You get one trace from the request through every attempt, and standard metrics, in a few lines. |
| You use another tracing or metrics system | Your own middleware, and `Client.Queues` | They are the same hooks that the `otel` module uses. |

## Send taskmill's logs to your logger

Pass your logger everywhere taskmill takes one:

```go
logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))

err := taskmill.Migrate(ctx, pool, &taskmill.MigrateConfig{Logger: logger})
// ...
client, err := taskmill.NewClient(ctx, pool, &taskmill.ClientConfig{Logger: logger}) // its workers log here too
// ...
dashboard, err := ui.New(client, &ui.Config{Logger: logger})
```

The `taskmill-ui` command writes its own logs to standard error.

Each level means one thing:

| Level | What it means | What to do |
|---|---|---|
| **Error** | A person must act: a task is dead, an operation has failed for a minute, or a privilege is missing. | Alert a person. |
| **Warn** | Something failed or looks wrong, but taskmill recovers by itself. | Look if it repeats or lasts. |
| **Info** | A normal event: a worker starts or stops, the maintainer changes, migrations run, someone acts in the dashboard. | Nothing. |
| **Debug** | Routine events of single tasks, such as a retry. | Turn it on to follow one kind's retries. |

Run at Info. Watch Warn lines that repeat, such as
`rescued tasks whose workers lost their leases`.
[Log messages](../reference/log-messages.md) lists every line, and what to
do about it.

Every line uses the same keys for the same things, such as `task_id`,
`kind`, `queue`, and `error`. No line shows a task's payload or metadata.
But a handler's error is logged as the handler returned it, so keep secrets
out of your errors.

## Add traces and metrics with OpenTelemetry

The `otel` module is a Go module of its own, so apps without OpenTelemetry
download none of it:

```sh
go get github.com/code19m/taskmill/otel
```

Add its middleware to the client and to the worker:

```go
import (
    "go.opentelemetry.io/otel"
    "go.opentelemetry.io/otel/propagation"

    tmotel "github.com/code19m/taskmill/otel"
)

otel.SetTextMapPropagator(propagation.TraceContext{}) // without it, tasks don't join the request's trace
tel := tmotel.New(nil)                                // OpenTelemetry's global providers

client, err := taskmill.NewClient(ctx, pool, &taskmill.ClientConfig{
    Logger:            logger,
    EnqueueMiddleware: []taskmill.EnqueueMiddleware{tel.EnqueueMiddleware},
})
// ...
outbox.New(client).AddRelay(&handlers, tel.Publisher("kafka", publisher), nil) // if you use the outbox
worker, err := taskmill.NewWorker(client, &taskmill.WorkerConfig{
    Handlers:   &handlers,
    Middleware: []taskmill.Middleware{tel.Middleware}, // first, so it also measures the middleware after it
})
// ...
_, err = tel.ObserveQueues(client) // the queue gauges
```

`tmotel.New` uses OpenTelemetry's global providers. Pass a `tmotel.Config`
to use others.

What you get:

| Signal | What it shows |
|---|---|
| Span `send {queue}` | Each enqueue call. It stores its trace context in the tasks' metadata. |
| Span `process {queue}` | Each attempt, as a child of the `send` span. So a task's work shows in the trace of the request that enqueued it, even a retry hours later. |
| Span `send {topic}` | Each outbox message sent to the broker. The message's headers carry the trace on to the consumers. |
| Counters and histograms | Tasks enqueued, and how long enqueues and attempts take, per queue and kind. Failures carry an `error.type`, such as `timeout`. |
| Queue gauges | Tasks per queue and state, the queue's lag, and how old the counts are. See [below](#watch-queue-depth-and-lag). |

The metrics never carry task IDs, so they make few series.
[Metrics and traces](../reference/metrics.md) lists every name and attribute.

> [!NOTE]
> Inside your transaction, the `send` span ends before you commit. So a
> transaction that rolls back leaves a span for a task that never existed.

## Write your own middleware

For another tracing or metrics system, use the same two
[middleware](../concepts/glossary.md#middleware) hooks that the `otel`
module uses:

| Hook | What it wraps | Where you set it |
|---|---|---|
| `Middleware` | Every attempt | `WorkerConfig.Middleware` |
| `EnqueueMiddleware` | Every enqueue, the schedules' runs included | `ClientConfig.EnqueueMiddleware` |

The first middleware in the list is the outermost. Here is one of each:

```go
// Metrics records each attempt: whether it worked, how long it ran, and how
// long the task waited before it started.
func Metrics(ctx context.Context, task *taskmill.TaskInfo, next func(context.Context) error) error {
    start := time.Now()
    err := next(ctx)
    record(task.Queue, task.Kind, err == nil, time.Since(start), task.AttemptedAt.Sub(task.RunAt))
    return err
}

// TagRequests stores the ID of the request that enqueued each task in the
// task's metadata.
func TagRequests(ctx context.Context, tasks []*taskmill.EnqueueTask, next func(context.Context) error) error {
    if id, ok := ctx.Value(requestIDKey{}).(string); ok {
        for _, t := range tasks {
            t.Metadata["request_id"] = id
        }
    }
    return next(ctx)
}
```

Follow these rules:

| Rule | Why |
|---|---|
| Call `next` once, and return its error. | Middleware that returns `nil` without calling `next` fails the attempt, which is then retried with backoff. Enqueue middleware that does so fails the call, and nothing is stored. |
| To skip a task on purpose, return `taskmill.Snooze(d)` or `taskmill.Permanent(err)`. | Never return `nil` without calling `next`. |
| Don't change the `TaskInfo` that attempt middleware gets. | It is taskmill's own record of the attempt. |
| In enqueue middleware, change only `Metadata`. | taskmill ignores changes to the other fields. |

A handler's panic reaches the middleware as an error. For a batch kind, the
middleware runs once per task, but the context it passes to `next` doesn't
reach the batch handler.

## Watch queue depth and lag

`Client.Queues` returns each queue with its numbers. The maintainer counts
them every 10 seconds.

```go
queues, err := client.Queues(ctx)
if err != nil {
    return err
}
for _, q := range queues {
    log.Printf("queue %s: %d available, the oldest has waited %s", q.Name, q.Counts[taskmill.StateAvailable].N, q.Lag)
}
```

| Field | What it means | Good to know |
|---|---|---|
| `Counts` | Tasks per state, except completed ones | Counting stops at 10,000 per queue and state, so a big backlog shows as 10,000. `Capped` says so. |
| `Lag` | How long the queue's oldest due task has waited | It has no cap, so it suits backlog alerts. Tasks that wait behind their FIFO group don't count. |
| `RefreshedAt` | When the maintainer last counted | It stops moving when no maintainer runs. |

With the `otel` module, `ObserveQueues` reports the same numbers as gauges.
Every process reports the same values, so aggregate them with `max`, not
`sum`.

For history, `Client.Activity` returns task counts, and the attempts that
completed and failed, per minute for the last 24 hours. The dashboard's
charts use it:

```go
points, err := client.Activity(ctx, &taskmill.ActivityParams{
    Since: time.Now().Add(-6 * time.Hour),
    Step:  5 * time.Minute, // one point per 5 minutes
})
```

## See also

- [Monitoring](../production/monitoring.md): what to alert on
- [Log messages](../reference/log-messages.md) and [Metrics and traces](../reference/metrics.md): every line and every signal
- [Package `otel`](https://pkg.go.dev/github.com/code19m/taskmill/otel) and [`Middleware`](https://pkg.go.dev/github.com/code19m/taskmill#Middleware) on pkg.go.dev

**Next:** [Test your code](testing.md)
