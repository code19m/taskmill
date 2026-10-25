# 0005. OpenTelemetry in a module of its own

- Status: Proposed
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
  has waited.
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
tested, and documented together. go.mod replaces the core with `../`, so CI
tests the module against the core at the same commit; users of the module
ignore that line. `make check` and CI lint, test, and scan every module.

### Versioning

The module has its own semantic version. Each release requires the oldest
core release that has the building blocks it uses. When a module release
needs core changes that are not released yet, the core is released first,
then the module's go.mod requires that version, then the module is tagged.

The module stays below v1 while OpenTelemetry's messaging conventions are in
development. Its Go API follows the core's compatibility policy, but the names
of its spans, attributes, and metrics follow the conventions, and may change
in a minor release that its changelog announces.

### What the core provides

Only what the module cannot build on its own, with no dependency:

1. **Enqueue middleware.** `ClientConfig.EnqueueMiddleware` wraps every
   enqueue: alone, in batches, inside the caller's transaction, and by
   schedules. It sees each task's resolved queue and kind, may change its
   metadata before the tasks are stored, and sees their results. The worker's
   `Middleware` already covers attempts.
2. **Lag.** `QueueInfo.Lag` is how long a queue's oldest available task has
   waited since its run time. `Client.Queues` measures it in the same query
   that lists the queues, by reading one entry of the fetch index per queue
   and priority.
3. **Metadata in deliveries.** `outbox.Delivery.Metadata` carries the stored
   task's metadata, and with it the trace context, to the publisher.
4. **Failure codes.** Failures that taskmill classifies itself report their
   code, such as `panic`, through an `ErrorType() string` method, which
   OpenTelemetry's `semconv.ErrorType` reads.

### What the module instruments

The module follows OpenTelemetry's semantic conventions for messaging, with
`messaging.system` set to `taskmill` and the queue as the destination:

- **Enqueue.** A producer span, `send {queue}`, wraps each enqueue call, and
  its context is stored in the metadata of every task the call enqueues. A
  batch has one span with `messaging.batch.message_count`, which leaves out
  the queue when its tasks go to several. The conventions also allow a
  `create` span per task of a batch; we leave them out, because an outbox
  batch of hundreds of messages would cost hundreds of spans, and the batch's
  span already gives each task a creation context.
- **Attempts.** A consumer span, `process {queue}`, wraps each attempt. Its
  parent is the producer span, so a task's work shows in the trace of the
  request that enqueued it. The conventions allow this parent for a single
  message, and link to the creation context otherwise; a span already in the
  worker's context becomes a link instead. Spans carry the task's ID, kind,
  and attempt.
- **The outbox.** A wrapper around the user's publisher traces each delivery
  with a producer span, `send {topic}`, a child of the span that stored the
  message. Its context goes into the message's headers, so consumers behind
  the broker continue the same trace. The user names the broker, such as
  `kafka`, because taskmill does not know it.
- **Metrics.** `messaging.client.sent.messages` for enqueues and publishes,
  `messaging.client.operation.duration` for enqueues, and
  `messaging.process.duration` for attempts, with `error.type` on failures.
  Two gauges describe the queues: `taskmill.queue.tasks`, per state, and
  `taskmill.queue.lag`. Metric attributes are bounded: queue, kind, state,
  and error type, never a task ID or an attempt number.

The queue gauges are separate, because they describe the whole installation
and cost two queries per collection: an application turns them on where it
wants them reported. Everything else is turned on with one value, created by
`otel.New`, whose middleware goes into the client's and the worker's
configuration. It uses OpenTelemetry's global providers and propagator
unless it is given others.

## Consequences

- Users who do not use OpenTelemetry download nothing new. Those who do get
  connected traces from a request through its tasks and outbox messages to
  the consumers behind the broker, and standard metrics, with a few lines of
  setup.
- The core gains three small extension points that other integrations can
  use too, such as metrics for another system.
- Releasing takes one more step when the module needs new core features, and
  the repository has two modules to keep tidy, linted, and scanned.
- Traces of tasks that retry for hours stay open for hours. Backends that
  limit a trace's duration may split them.
- Every process that reports the queue gauges reports the same values, so
  dashboards aggregate them with max, not sum.
- `ContextMetadata` overlaps with enqueue middleware, which can do what it
  does. It stays for now, as the simpler tool; the API review before v1.0.0
  decides whether to keep both.
