# Configuration

This page names the type that holds each setting, and where you set it. The
doc comment of the type, on pkg.go.dev, has every field with its default,
its limits, and its zero value. A value outside its limits makes the call
return an error that wraps
[`ErrInvalidArgument`](https://pkg.go.dev/github.com/code19m/taskmill#ErrInvalidArgument).

## What you set in your code, for one process

| You set | Type | You pass it to |
|---|---|---|
| The schema to create or upgrade, and the logger | [`MigrateConfig`](https://pkg.go.dev/github.com/code19m/taskmill#MigrateConfig) | `Migrate` |
| A client's schema, logger, and enqueue middleware | [`ClientConfig`](https://pkg.go.dev/github.com/code19m/taskmill#ClientConfig) | `NewClient` |
| A kind's timeout, retry policy, batch size, and history | [`HandlerOptions`](https://pkg.go.dev/github.com/code19m/taskmill#HandlerOptions) | `AddHandler`, `AddBatchHandler` |
| The waits between retries | [`ExponentialBackoff`](https://pkg.go.dev/github.com/code19m/taskmill#ExponentialBackoff) | `HandlerOptions.Retry` |
| A worker's queues, lease, polling, shutdown, middleware, schedules, and notifications | [`WorkerConfig`](https://pkg.go.dev/github.com/code19m/taskmill#WorkerConfig) | `NewWorker` |
| How many tasks of a queue one process runs | [`QueueOptions`](https://pkg.go.dev/github.com/code19m/taskmill#QueueOptions) | `WorkerConfig.Queues` |
| A recurring task | [`Schedule`](https://pkg.go.dev/github.com/code19m/taskmill#Schedule) | `WorkerConfig.Schedules` |
| The dashboard's path, login, and frames | [`ui.Config`](https://pkg.go.dev/github.com/code19m/taskmill/ui#Config), [`ui.BasicAuth`](https://pkg.go.dev/github.com/code19m/taskmill/ui#BasicAuth) | `ui.New` |
| A relay's batch size and timeout | [`outbox.RelayOptions`](https://pkg.go.dev/github.com/code19m/taskmill/outbox#RelayOptions) | `Outbox.AddRelay` |
| The providers and the propagator of OpenTelemetry | [`otel.Config`](https://pkg.go.dev/github.com/code19m/taskmill/otel#Config) | `otel.New` |

The `taskmill-ui` command has no Go configuration. It reads
[environment variables](taskmill-ui.md#environment-variables).

## What you store in the database, for every worker

These settings bind every worker of the
[schema](../concepts/glossary.md#schema), so you change them once.

| You set | Type | You pass it to |
|---|---|---|
| How many tasks of a queue run across all workers | [`QueueSettings`](https://pkg.go.dev/github.com/code19m/taskmill#QueueSettings) | `Client.UpdateQueue` |
| How long finished tasks are kept | [`Settings`](https://pkg.go.dev/github.com/code19m/taskmill#Settings) | `Client.UpdateSettings` |

## What you pass with one call

| You set | Type | You pass it to |
|---|---|---|
| A task's queue, priority, run time, attempts, FIFO group, dedupe key, and metadata | [`EnqueueOptions`](https://pkg.go.dev/github.com/code19m/taskmill#EnqueueOptions) | The enqueue calls. Also a kind's `EnqueueDefaults`, and `Schedule.Options` |
| One task of a batch enqueue | [`EnqueueParams`](https://pkg.go.dev/github.com/code19m/taskmill#EnqueueParams) | `Client.EnqueueMany`, `Client.EnqueueManyTx` |
| A message's topic, key, value, and headers | [`outbox.Message`](https://pkg.go.dev/github.com/code19m/taskmill/outbox#Message) | `Outbox.Store` |
| Which tasks a list, a count, or a bulk action selects | [`TaskFilter`](https://pkg.go.dev/github.com/code19m/taskmill#TaskFilter) | `ListParams`, `CountParams`, and `BulkParams` |
| A page of tasks | [`ListParams`](https://pkg.go.dev/github.com/code19m/taskmill#ListParams) | `Client.List` |
| The tasks that a count counts | [`CountParams`](https://pkg.go.dev/github.com/code19m/taskmill#CountParams) | `Client.Count` |
| A page of FIFO groups | [`GroupParams`](https://pkg.go.dev/github.com/code19m/taskmill#GroupParams) | `Client.Groups` |
| The tasks of a bulk action | [`BulkParams`](https://pkg.go.dev/github.com/code19m/taskmill#BulkParams) | `Client.RetryMatching`, `Client.CancelMatching`, `Client.DeleteMatching` |
| The period and the step of the activity | [`ActivityParams`](https://pkg.go.dev/github.com/code19m/taskmill#ActivityParams) | `Client.Activity` |

## See also

- [Fixed timings and limits](defaults-and-limits.md): the timings that no setting changes
- [Workers and queues](../guides/workers-and-queues.md): which setting to use for which limit
- [API on pkg.go.dev](https://pkg.go.dev/github.com/code19m/taskmill): the contract of every field

**Next:** [Errors](errors.md)
