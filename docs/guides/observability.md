# Logs, traces, and metrics

This page shows how to see what taskmill does: logs through `log/slog`,
traces and metrics through OpenTelemetry, and queue numbers from the
`Client`. For the alerts to set on them, see
[Monitoring](../production/monitoring.md).

## Quick example

Pass your logger everywhere taskmill takes one:

```go
logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))

err := taskmill.Migrate(ctx, pool, &taskmill.MigrateConfig{Logger: logger})
// ...
client, err := taskmill.NewClient(ctx, pool, &taskmill.ClientConfig{Logger: logger}) // its workers log here too
// ...
dashboard, err := ui.New(client, &ui.Config{Logger: logger})
```

## Which should I use?

| Situation | What to use | Why |
|---|---|---|
| Every app | Your `*slog.Logger`, passed to taskmill | By default, taskmill writes no logs at all |
| You use OpenTelemetry | The `otel` module | You get one trace from the request through every attempt, and standard metrics, in a few lines |
| You use another tracing or metrics system | Your own middleware, and `Client.Queues` | They are the same hooks that the `otel` module uses |

## Send taskmill's logs to your logger

Run at Info. Send Error lines to a person, and watch Warn lines that repeat.
One example is `rescued tasks whose workers lost their leases`: the
[maintainer](../concepts/glossary.md#maintainer)
[rescues](../concepts/glossary.md#rescue) the tasks of a worker that
crashed. [Log messages](../reference/log-messages.md#levels) says what each
level means, and lists every line with what to do about it.

Every line uses the same keys for the same things, such as `task_id`,
`kind`, `queue`, and `error`. No line shows a task's payload or metadata.
But a handler's error is logged as the handler returned it, so keep secrets
out of your errors.

## Add traces and metrics with OpenTelemetry

The `otel` module is a Go module of its own, so apps without OpenTelemetry
download none of it: `go get github.com/code19m/taskmill/otel`. Add its
middleware to the client and to the worker:

```go
import (
    "go.opentelemetry.io/otel"
    "go.opentelemetry.io/otel/propagation"

    tmotel "github.com/code19m/taskmill/otel"
)

otel.SetTextMapPropagator(propagation.TraceContext{}) // without it, tasks don't join the request's trace
tel := tmotel.New(nil)                                // nil: OpenTelemetry's global providers

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

What you get:

| Signal | What it shows |
|---|---|
| Span `send {queue}` | Each enqueue call. It stores its trace context in the tasks' metadata |
| Span `process {queue}` | Each attempt, as a child of the `send` span. So a task's work shows in the trace of the request that enqueued it, even a retry hours later |
| Span `send {topic}` | Each outbox message sent to the broker. The message's headers carry the trace on to the consumers |
| Counters and histograms | Tasks enqueued, and how long enqueues and attempts take, per queue and kind. Failures carry an `error.type`, such as `timeout` |
| Queue gauges | Tasks per queue and state, the queue's [lag](../concepts/glossary.md#lag), and how old the counts are. See [below](#watch-queue-depth-and-lag) |

The metrics never carry task IDs, so they create few time series.
[Metrics and traces](../reference/metrics.md) lists every name and
attribute.

> [!NOTE]
> Inside your transaction, the `send` span ends before you commit. So a
> transaction that rolls back leaves a span for a task that never existed.

## Export the metrics and traces

`tmotel.New(nil)` uses OpenTelemetry's global providers. Until your app sets
them, they record nothing. This is the smallest setup for metrics: it serves
them to Prometheus on `/metrics`.

```go
import (
    "github.com/prometheus/client_golang/prometheus/promhttp"
    "go.opentelemetry.io/otel"
    "go.opentelemetry.io/otel/exporters/prometheus"
    sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

exporter, err := prometheus.New() // registers with Prometheus' default registry
if err != nil {
    return err
}
otel.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(exporter)))
mux.Handle("GET /metrics", promhttp.Handler())
```

- **The names follow the exporter's defaults.** With them, you get the
  names that [Monitoring](../production/monitoring.md#rules-for-prometheus)
  uses, such as `taskmill_queue_lag_seconds`. If you change how the exporter
  or your collector makes names, see
  [Prometheus names](../reference/metrics.md#prometheus-names).
- **Every pod reports the same queue gauges.** Prometheus tells the pods
  apart by its `instance` label, and its `job` label names your service in
  the alert rules. If you push with OTLP instead, give each pod its own
  `service.instance.id`, such as the pod's name.
- **Report the gauges from a process that stays up.** While no process
  reports them, alerts on them see nothing.
- **For traces,** set a tracer provider with `otel.SetTracerProvider` in the
  same way. OpenTelemetry's
  [exporter guide](https://opentelemetry.io/docs/languages/go/exporters/)
  shows the OTLP exporters for traces and metrics.

## Write your own middleware

For another tracing or metrics system, use the same two
[middleware](../concepts/glossary.md#middleware) hooks that the `otel`
module uses. `Middleware` wraps every attempt, and `EnqueueMiddleware` wraps
every enqueue. [Add middleware](middleware.md) shows how to write both, and
the rules that they must follow.

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

| You want to know | Read | Why |
|---|---|---|
| Is there a backlog? | `Lag` | It has no cap. `Counts` stop at 10,000 per queue and state |
| Is work stuck while lag stays low? | `HaltedGroups` and `MostFailures` | Tasks behind a [halted group](../concepts/glossary.md#halted-group), or between retries, don't count in lag |
| Does a maintainer run? | `RefreshedAt` | It stops moving when no maintainer counts |

The godoc of [`QueueInfo`](https://pkg.go.dev/github.com/code19m/taskmill#QueueInfo)
says exactly what each field counts.

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
- [Add middleware](middleware.md): write your own tracing or metrics
- [Package `otel`](https://pkg.go.dev/github.com/code19m/taskmill/otel) and [`Client.Queues`](https://pkg.go.dev/github.com/code19m/taskmill#Client.Queues) on pkg.go.dev

**Next:** [Test your code](testing.md)
