# 0005. OpenTelemetry in a module of its own

- Status: Accepted
- Date: 2026-10-25

## Context

Cloud-native applications expect traces and metrics through OpenTelemetry. A
task queue sits in the middle of their traces: a request enqueues a task, and
a worker runs it seconds or hours later. Operators also watch how deep the
queues are and how long tasks wait.

[ADR 0001](0001-dependency-policy.md) keeps pgx the core's only dependency,
so OpenTelemetry must live in a module of its own. Before this decision the
core offered logs, worker middleware, and `ClientConfig.ContextMetadata`, and
the guide showed how to trace by hand with them. That covers the consumer
side, but not the rest:

- **No producer span.** `ContextMetadata` can inject the caller's context into
  a task's metadata, but it cannot wrap the enqueue: it never learns the
  task's ID or whether the enqueue failed, so nothing can end a producer span.
- **No lag.** `Client.Queues` counts tasks, but not how long the oldest one
  has waited. The counts are capped, so on a large backlog they cannot say
  how far behind a queue is.
- **No path to the broker.** The outbox's publisher does not see the metadata
  of the task that holds a message, so the trace stops at the outbox.

## Decision

### Where it lives

The integration is the module `github.com/code19m/taskmill/otel`, in the
`otel/` directory of this repository, with its own go.mod. Its releases are
tagged `otel/vX.Y.Z`.

We considered a separate repository. It would let the module release on its
own schedule, but every change that needs a new building block in the core
would take two pull requests, and a core release before the module could use
it. In one repository, a building block and its first user are reviewed,
tested, and documented together. `make check` and CI lint, test, and scan
every module.

The module's go.mod replaces the core with `../`, so that every command in
the repository builds it against the core at the same commit; users of the
module ignore that line. A workspace (go.work) would do the same, but
`go mod tidy` ignores workspaces, so it would fail whenever the module waits
for a core release. Because the replace line hides the required version, a
separate check, `make otel-release-check`, copies the module, drops the line,
and builds and tests the copy against the core version that go.mod requires,
as users build it. Until that version is released, the check only says so;
when the module is released, it must pass.

### Versioning

The module has its own semantic version. Each release requires the oldest
core release that has the building blocks it uses. When the module starts to
use a core change, its go.mod requires the next core version in the same pull
request, and the release check fails if it does not. The core is then
released before the module is tagged.

The module stays below v1 while OpenTelemetry's messaging conventions are in
development. Its Go API follows the core's compatibility policy, but the names
of its spans, attributes, and metrics follow the conventions, and may change
in a minor release that its changelog announces.

### What the core provides

Only what the module cannot build on its own, with no dependency:

1. **Enqueue middleware.** `ClientConfig.EnqueueMiddleware` wraps every
   enqueue: alone, in batches, inside the caller's transaction, and by
   schedules. It sees each task's resolved queue and kind, may change its
   metadata before the tasks are stored, and sees their results. It covers
   `ClientConfig.ContextMetadata` in a few lines, so that field is
   deprecated: it runs as the outermost enqueue middleware until v0.3.0
   removes it, as the compatibility policy asks. A
   middleware that fails after the tasks were stored outside the caller's
   transaction does not fail the enqueue, whose tasks have committed: a retry
   would duplicate them. The worker's `Middleware` already covers attempts.
2. **Lag.** `QueueInfo.Lag` is how long a queue's oldest due task had
   waited since its run time: an available task, or a scheduled or retryable
   one whose run time has passed, so overdue retries and a maintainer that
   stopped promoting show too. FIFO tasks that wait behind their group are
   excluded: they wait by design. The maintainer finds each queue's oldest
   due task while it counts, every 10 seconds, and stores its run time with
   the counts (migration 2). `Client.Queues`, which the dashboard reads every
   few seconds, then measures its age by the database's clock without reading
   a task. So lag is not capped like the counts, and keeps growing when no
   maintainer runs, at the cost of reading high for up to one refresh. The
   maintainer also counts every known queue, also an empty one, so the time of
   the last count is always known.
3. **Metadata in deliveries.** `outbox.Delivery.Metadata` carries the stored
   task's metadata, and with it the trace context, to the publisher.
4. **Error classes.** taskmill's errors name their class, such as
   `invalid_argument`, `panic`, or `timeout`, through an `ErrorType() string`
   method, which OpenTelemetry's `semconv.ErrorType` reads.

### What the module instruments

The module follows OpenTelemetry's semantic conventions for messaging, with
`messaging.system` set to `taskmill` and the queue as the destination. Its
own attributes use the `taskmill.` namespace: `taskmill.task.kind`,
`taskmill.task.attempt`, and `taskmill.task.state`.

- **Enqueue.** A producer span, `send {queue}`, wraps each enqueue call, and
  its context is stored in the metadata of every task the call enqueues. A
  batch has one span with `messaging.batch.message_count`, which leaves out
  the queue when its tasks go to several. The conventions also allow a
  `create` span per task of a batch; we leave them out, because an outbox
  batch of hundreds of messages would cost hundreds of spans, and the batch's
  span already gives each task a creation context.
- **Attempts.** A consumer span, `process {queue}`, wraps each attempt. Its
  parent is the producer span, so a task's work shows in the trace of the
  request that enqueued it; the conventions allow this parent for a single
  message. The span also links to the producer span, as the conventions ask
  of every consumer span, and to a span already in the worker's context.
  Spans carry the task's ID, kind, and attempt. When the attempt's context
  ended, `error.type` names why, such as `timeout`.
- **The outbox.** A wrapper around the user's publisher traces each delivery
  with a producer span, `send {topic}`, a child of the span that stored the
  message. Its context goes into the message's headers, so consumers behind
  the broker continue the same trace. The user names the broker, such as
  `kafka`, because taskmill does not know it.
- **Metrics.** `messaging.client.sent.messages` for enqueues and publishes,
  `messaging.client.operation.duration` for enqueues, and
  `messaging.process.duration` for attempts, with `error.type` on failures.
  Gauges describe the queues: `taskmill.queue.tasks`, per state;
  `taskmill.queue.capped`, which says that a count stopped at its cap; and
  `taskmill.queue.lag`, which backlog alerts use because it has no cap. A
  fourth, `taskmill.stats.age`, says how long ago the maintainer counted: a
  queue that was empty then shows no lag, so this gauge is the alert for "no
  maintainer runs".
  Metric attributes are bounded: queue, kind, state, and error type, never a
  task ID or an attempt number.

The queue gauges are turned on by a call of their own, because they describe
the whole installation rather than the process. Only the maintainer measures
them, and it stores them, so every process that reports them reads the same
small table and reports the same values: dashboards aggregate them with max.
We considered reporting them only from the maintainer's process, but the
maintainer moves between processes, and a gauge that moves with it breaks its
series; reading the stored values costs each process one small query per
collection. Everything else is turned on with one value, created by
`otel.New`, whose middleware goes into the client's and the worker's
configuration. It uses OpenTelemetry's global providers and propagator
unless it is given others.

## Consequences

- Users who do not use OpenTelemetry download nothing new. Those who do get
  connected traces from a request through its tasks and outbox messages to
  the consumers behind the broker, and standard metrics, with a few lines of
  setup.
- The core gains small extension points that other integrations can use
  too, such as metrics for another system. Applications that use
  `ContextMetadata` move to enqueue middleware before v0.3.0.
- Releasing takes one more step when the module needs new core features, and
  the repository has two modules to keep tidy, linted, and scanned.
- Traces of tasks that retry for hours stay open for hours. Backends that
  limit a trace's duration may split them.
- Every process that reports the queue gauges reports the same values, so
  dashboards aggregate them with max, not sum.
