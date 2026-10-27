# Changelog

All notable changes to the `github.com/code19m/taskmill/otel` module are
recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/). The module has its
own versions, tagged `otel/vX.Y.Z`; see
[ADR 0005](../docs/adr/0005-opentelemetry-in-a-module-of-its-own.md). While
OpenTelemetry's messaging conventions are in development, a minor release may
rename spans, attributes, or metrics to follow them, and says so here.

## [Unreleased]

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
