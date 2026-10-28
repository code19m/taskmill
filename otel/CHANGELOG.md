# Changelog

All notable changes to the `github.com/code19m/taskmill/otel` module are
recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/). The module has its
own versions, tagged `otel/vX.Y.Z`; see
[ADR 0005](../docs/adr/0005-opentelemetry-in-a-module-of-its-own.md). While
OpenTelemetry's messaging conventions are in development, a minor release may
rename spans, attributes, or metrics to follow them, and says so here.

## [Unreleased]

### Changed

- Requires taskmill v0.3.0, whose `outbox.Outbox.AddRelay` takes relay
  options, and whose queue counts each say whether they stopped at their
  cap; the examples pass nil relay options.
- **Breaking:** `taskmill.queue.capped` has the attribute
  `taskmill.task.state`, as `taskmill.queue.tasks` has: it is 1 when the
  count of the queue's tasks in that state stopped at its cap, so each count
  says whether it is a lower bound. Dashboards and alerts that read it per
  queue take the maximum over the states.
- With taskmill v0.3.0, an attempt or an enqueue that a middleware after
  this module's skips, by returning nil without calling `next`, is recorded
  as failed: its span has an error status, and its spans and metrics have
  the `error.type` `middleware`.

## [0.1.0] - 2026-10-27

Initial release, for taskmill v0.2.0 or newer.

### Added

- Producer spans for enqueues and consumer spans for attempts, following
  OpenTelemetry's messaging conventions: an attempt is a child of the span
  that enqueued its task, and links to it.
- Metrics: `messaging.client.sent.messages`,
  `messaging.client.operation.duration`, and `messaging.process.duration`.
- Gauges of the queues: `taskmill.queue.tasks` per state,
  `taskmill.queue.capped`, `taskmill.queue.lag`, and `taskmill.stats.age`,
  which shows that no maintainer runs.
- A wrapper for the outbox's publisher, which traces each delivery and carries
  the trace to the broker in the message's headers.

[Unreleased]: https://github.com/code19m/taskmill/compare/otel/v0.1.0...HEAD
[0.1.0]: https://github.com/code19m/taskmill/tree/otel/v0.1.0/otel
