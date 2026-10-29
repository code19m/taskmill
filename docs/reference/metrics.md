# Metrics and traces

Every span, metric, and gauge that the `otel` module reports, with its
attributes and its Prometheus name. To set the module up, see
[Observability](../guides/observability.md).

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

| Metric | Type | Unit | What it measures | Prometheus name |
|---|---|---|---|---|
| `messaging.client.sent.messages` | Counter | `{message}` | Tasks enqueued, and outbox deliveries sent to the broker | `messaging_client_sent_messages_total` |
| `messaging.client.operation.duration` | Histogram | `s` | How long enqueue calls take | `messaging_client_operation_duration_seconds` |
| `messaging.process.duration` | Histogram | `s` | How long attempts take | `messaging_process_duration_seconds` |

Their attributes are `messaging.system`, `messaging.operation.name`, the
queue or topic as `messaging.destination.name`, `taskmill.task.kind` for
tasks, and `error.type` on failures. The enqueue duration also has
`messaging.operation.type`. They never carry a task ID or an attempt number,
so the number of series stays small. The histograms' buckets run
from 5 ms to 10 seconds, as the conventions advise.

## Queue gauges

`Instrumentation.ObserveQueues` reports these. The maintainer counts the
queues every 10 seconds, and the gauges read its latest count.

| Gauge | Unit | Attributes | What it reports | Prometheus name |
|---|---|---|---|---|
| `taskmill.queue.tasks` | `{task}` | `messaging.destination.name`, `taskmill.task.state` | Tasks per queue and state, except completed. Counting stops at 10,000 per queue and state, and at 100,000 scheduled and retryable tasks in all | `taskmill_queue_tasks` |
| `taskmill.queue.capped` | — | `messaging.destination.name`, `taskmill.task.state` | 1 when that count stopped at its cap with more tasks left, else 0 | `taskmill_queue_capped` |
| `taskmill.queue.lag` | `s` | `messaging.destination.name` | How long the queue's oldest due task has waited since its run time. No cap. May read high for up to 10 seconds | `taskmill_queue_lag_seconds` |
| `taskmill.queue.halted_groups` | `{group}` | `messaging.destination.name` | FIFO groups of the queue that a dead task halts. Counting stops at 10,000 | `taskmill_queue_halted_groups` |
| `taskmill.queue.failed_attempts` | `{attempt}` | `messaging.destination.name` | How many attempts the queue's most failed task has failed, among those that wait to retry. A task drops out while it runs its next attempt | `taskmill_queue_failed_attempts` |
| `taskmill.stats.age` | `s` | — | How long ago the maintainer last counted. Absent until the first count | `taskmill_stats_age_seconds` |

The states are `scheduled`, `pending`, `available`, `running`, `retryable`,
`dead`, and `canceled`. Tasks that wait behind their FIFO group are not in
the lag, and neither are tasks that wait for their next retry. Halted groups
and failed attempts show that work instead: it is stuck while the lag stays
near zero.

Alert on `taskmill.queue.lag` for a backlog: it has no cap, and it keeps
growing when workers stop. Alert on `taskmill.stats.age` over a minute: then
no maintainer runs, and a queue that was empty at the last count shows no
lag. Alert on `taskmill.queue.halted_groups` above 0, and on
`taskmill.queue.failed_attempts` reaching 10 within 15 minutes.
[Monitoring](../production/monitoring.md) has the full list of alerts.

At each collection, every process that calls `ObserveQueues` runs two small
queries, and stops after 5 seconds. A read that fails goes to
OpenTelemetry's error handler, `otel.Handle`.

## Aggregate the gauges with `max`

The queue gauges describe the whole [schema](../concepts/glossary.md), not
the process that reports them. Every process reads the same stored count, so
every process reports the same values. If 5 pods report them, `sum` shows 5
times the real number. Use `max` to merge the pods, then `sum` across queues
if you need a total:

```promql
# Lag of each queue
max by (messaging_destination_name) (taskmill_queue_lag_seconds)

# Available tasks across all queues
sum(max by (messaging_destination_name) (taskmill_queue_tasks{taskmill_task_state="available"}))

# No maintainer: fire after a minute
max(taskmill_stats_age_seconds) > 60
```

The counters and histograms are per process, so `sum` is right for them.

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

- [Observability](../guides/observability.md): set up the module, and write your own middleware
- [Autoscaling](../production/autoscaling.md): scale workers on these gauges
- [`otel` on pkg.go.dev](https://pkg.go.dev/github.com/code19m/taskmill/otel)
- [ADR 0005](../adr/0005-opentelemetry-in-a-module-of-its-own.md): why the module looks like this

**Next:** [Compatibility](compatibility.md)
