# Metrics and traces

Every span, metric, and gauge that the `otel` module reports, with its
attributes, its Prometheus name, and queries for a dashboard. To set the
module up, see [Logs, traces, and metrics](../guides/observability.md). To
choose alerts and thresholds, see [Monitoring](../production/monitoring.md).

The module follows OpenTelemetry's semantic conventions for messaging, with
`messaging.system` set to `taskmill` and the queue as the destination. Its
instrumentation scope is `github.com/code19m/taskmill/otel`.

## Spans

| Span | Kind | Made by | One span per |
|---|---|---|---|
| `send {queue}` | Producer | `Instrumentation.EnqueueMiddleware` | Enqueue call. A batch has one span. Its context goes into each task's metadata. |
| `process {queue}` | Consumer | `Instrumentation.Middleware` | Attempt. Its parent is the `send` span, so a task's attempts show in the trace of the request that enqueued it, even a retry hours later. |
| `send {topic}` | Producer | `Instrumentation.Publisher` | Outbox delivery to the broker. Its parent is the span that stored the message. Its context goes into the message's headers, so consumers continue the trace. |

A span's name leaves out the queue when a batch holds tasks of several
queues: it is then `send`.

Span attributes:

| Attribute | On | Value |
|---|---|---|
| `messaging.system` | All | `taskmill`; on `send {topic}`, the broker name you pass to `Publisher`, such as `kafka` |
| `messaging.operation.name`, `messaging.operation.type` | All | `send` or `process` |
| `messaging.destination.name` | All | The queue; on `send {topic}`, the topic. Left out when a batch has several |
| `taskmill.task.kind` | `send {queue}`, `process {queue}` | The kind. Left out when a batch has several |
| `messaging.message.id` | All | The task ID; on `send {topic}`, the delivery ID. On `send {queue}`, only for one task |
| `messaging.batch.message_count` | `send {queue}` | How many tasks the call enqueued, when more than one |
| `taskmill.task.attempt` | `process {queue}` | The attempt number, from 1 |
| `error.type` | All | Why it failed. See [error classes](errors.md#error-classes) |

A `process` span also links to the `send` span, as the conventions ask. A
snooze is not an error. When the attempt's context ended, `error.type` names
why, such as `timeout`.

> [!NOTE]
> Inside your transaction, the `send` span ends before the commit. If you
> roll back, the trace keeps a span for a task that never existed.

## Metrics

The second name in each row is the Prometheus name.

| Metric | Type | Unit | What it measures |
|---|---|---|---|
| `messaging.client.sent.messages`<br/>`messaging_client_sent_messages_total` | Counter | `{message}` | The tasks of each enqueue call, and each outbox delivery to the broker. A failed call or delivery counts too, with `error.type` |
| `messaging.client.operation.duration`<br/>`messaging_client_operation_duration_seconds` | Histogram | `s` | How long each enqueue call takes. A batch is one value |
| `messaging.process.duration`<br/>`messaging_process_duration_seconds` | Histogram | `s` | How long each attempt takes. So its count is the number of attempts. A snooze counts, without `error.type` |

| Attribute | On | Value |
|---|---|---|
| `messaging.system` | All | `taskmill`; for an outbox delivery, the broker name |
| `messaging.operation.name` | All | `send` or `process` |
| `messaging.operation.type` | The enqueue duration | `send` |
| `messaging.destination.name` | All | The queue, or the topic of an outbox delivery. The enqueue duration of a batch with several queues has none |
| `taskmill.task.kind` | All but outbox deliveries | The kind. The enqueue duration of a batch with several kinds has none |
| `error.type` | Failures only | Why it failed. See [error classes](errors.md#error-classes) |

The metrics never carry a task ID or an attempt number, so the number of
time series stays small. The histograms' buckets run from 5 ms to 10
seconds, as the conventions advise. A task whose kind has no handler on the
worker that fetched it has no span and is not measured.

## Queue gauges

`Instrumentation.ObserveQueues` reports these. The
[maintainer](../concepts/glossary.md#maintainer) counts the queues every 10
seconds, and the gauges show its latest count.

| Gauge | Unit | Attributes | What it reports |
|---|---|---|---|
| `taskmill.queue.tasks`<br/>`taskmill_queue_tasks` | `{task}` | `messaging.destination.name`, `taskmill.task.state` | Tasks per queue and state, except completed. Counting stops at 10,000 per queue and state, and at 100,000 scheduled and retryable tasks in all |
| `taskmill.queue.capped`<br/>`taskmill_queue_capped` | — | `messaging.destination.name`, `taskmill.task.state` | 1 when that count stopped at its cap with more tasks left, else 0 |
| `taskmill.queue.lag`<br/>`taskmill_queue_lag_seconds` | `s` | `messaging.destination.name` | The [lag](../concepts/glossary.md#lag): how long the queue's oldest due task has waited since its run time. No cap. It can be too high for up to 10 seconds |
| `taskmill.queue.halted_groups`<br/>`taskmill_queue_halted_groups` | `{group}` | `messaging.destination.name` | The queue's [halted groups](../concepts/glossary.md#halted-group): FIFO groups that a dead task halts. Counting stops at 10,000 |
| `taskmill.queue.most_failures`<br/>`taskmill_queue_most_failures` | `{failure}` | `messaging.destination.name` | The most times in a row that one task of the queue, waiting to retry, has failed. A task does not count while it runs its next attempt |
| `taskmill.stats.age`<br/>`taskmill_stats_age_seconds` | `s` | — | How long ago the maintainer last counted, by the clock of the process that reports it. Absent until the first count |

- **The states** are `scheduled`, `pending`, `available`, `running`,
  `retryable`, `dead`, and `canceled`. A queue reports 0 for a state in
  which it has no task.
- **Lag leaves out** the tasks that wait behind their FIFO group, and the
  tasks that wait for their next retry. Halted groups and most failures
  show that work instead: it is stuck while the lag stays near zero.
- **Each collection costs two small queries** in every process that calls
  `ObserveQueues`, and stops after 5 seconds. If the read fails, the gauges
  report nothing for that collection, and the error goes to OpenTelemetry's
  error handler, `otel.Handle`.

## Aggregate the gauges with `max`

The queue gauges describe the whole
[schema](../concepts/glossary.md#schema), not the process that reports them.
Every process reads the same stored count, so every process reports the
same values. If 5 pods report them, `sum` shows 5 times the real number. Use
`max` to merge the pods, then `sum` across queues if you need a total.

The counters and histograms are per process, so `sum` is right for them.

## Dashboard queries

These PromQL queries build a dashboard. For alert rules, see
[Monitoring](../production/monitoring.md).

> [!TIP]
> Select one service in every query, as `job="shop-worker"` does here. The
> default queue of every service is called `default`, so without a selector
> a query mixes the queues of all your services.

```promql
# Attempts per second, by queue and kind
sum by (messaging_destination_name, taskmill_task_kind) (
  rate(messaging_process_duration_seconds_count{job="shop-worker"}[5m]))

# Failed attempts per second, by error type
sum by (error_type) (
  rate(messaging_process_duration_seconds_count{job="shop-worker", error_type!=""}[5m]))

# Share of attempts that fail
sum(rate(messaging_process_duration_seconds_count{job="shop-worker", error_type!=""}[5m]))
  / sum(rate(messaging_process_duration_seconds_count{job="shop-worker"}[5m]))

# 95th percentile of the attempt time, by queue
histogram_quantile(0.95, sum by (le, messaging_destination_name) (
  rate(messaging_process_duration_seconds_bucket{job="shop-worker"}[5m])))

# Tasks enqueued per second, by queue. Use error_type!="" for failed enqueues
sum by (messaging_destination_name) (
  rate(messaging_client_sent_messages_total{job="shop-api", messaging_system="taskmill", error_type=""}[5m]))

# Tasks per queue and state
max by (messaging_destination_name, taskmill_task_state) (taskmill_queue_tasks{job="shop-worker"})

# Counts that stopped at their cap: the real number is higher
max by (messaging_destination_name, taskmill_task_state) (taskmill_queue_capped{job="shop-worker"}) == 1

# Lag of each queue, in seconds
max by (messaging_destination_name) (taskmill_queue_lag_seconds{job="shop-worker"})

# Seconds since the maintainer last counted
max(taskmill_stats_age_seconds{job="shop-worker"})
```

`shop-api` stands for the service that enqueues, and `shop-worker` for the
one that runs the workers and calls `ObserveQueues`.

## Prometheus names

The Prometheus names above follow the usual translation of OpenTelemetry
names: dots become underscores, the unit becomes a suffix such as `_seconds`,
and counters get `_total`. Attributes become labels the same way, such as
`messaging_destination_name` and `taskmill_task_state`. If your setup keeps
OpenTelemetry's names as they are, use the names with dots.

The module is below v1. A minor release may rename a span, attribute, or
metric to follow OpenTelemetry's conventions, and its
[changelog](../../otel/CHANGELOG.md) says so.

## See also

- [Logs, traces, and metrics](../guides/observability.md): set up the module, and write your own middleware
- [Monitoring](../production/monitoring.md): what to alert on, and the limits of each signal
- [Autoscaling](../production/autoscaling.md): scale workers on these gauges
- [`otel` on pkg.go.dev](https://pkg.go.dev/github.com/code19m/taskmill/otel)

**Next:** [Compatibility](compatibility.md)
