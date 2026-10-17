# taskmill

[![CI](https://github.com/code19m/taskmill/actions/workflows/ci.yml/badge.svg)](https://github.com/code19m/taskmill/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/code19m/taskmill.svg)](https://pkg.go.dev/github.com/code19m/taskmill)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

A PostgreSQL-backed task queue for Go, built for high-throughput production systems.

> [!WARNING]
> taskmill is in early development. There is no usable API yet, and nothing is
> stable until v1.0.0.

## Why taskmill

If your service already runs on PostgreSQL, your task queue can live there too:
one less system to operate, and tasks that commit or roll back together with
your data.

taskmill aims to be:

- **Transactional.** Enqueue a task inside your own database transaction. If
  the transaction rolls back, the task never existed.
- **Reliable.** At-least-once execution with leases, retries with backoff, and
  a dead-letter queue. A crashed worker never loses a task.
- **Ordered when you need it.** Strict FIFO processing per group key, for any
  number of groups, and concurrency limits.
- **Fast.** Low pickup latency through `LISTEN/NOTIFY`, batched round trips,
  and queries verified against their query plans.
- **Observable.** Structured logging through `log/slog`, plus metrics and
  tracing.
- **Small.** The core module depends on the standard library and a PostgreSQL
  driver, nothing else.

## Requirements

- Go 1.27 or newer
- PostgreSQL 15 or newer

See [ADR 0002](docs/adr/0002-supported-versions.md) for the support policy.

## Roadmap

| Milestone | Scope |
|---|---|
| M0 Foundations | Repository, tooling, CI, project policies |
| M1 Design | Task lifecycle, schema, public API, guarantees, performance targets |
| M2 Core engine | Migrations, enqueue, fetch, complete, retry, dead-letter queue |
| M3 Worker runtime | Typed handlers, worker pool, `LISTEN/NOTIFY`, heartbeats, graceful shutdown |
| **v0.1.0** | First release |
| M4 Ordering and control | FIFO groups, concurrency limits, uniqueness, cancellation, pausing |
| M5 Scheduling and maintenance | Cron schedules, leader election, stuck-task rescue, retention |
| M6 Observability and admin | Metrics, tracing, admin API, CLI |
| M7 Hardening | Benchmarks, load and failure testing |
| **v1.0.0** | Stable API and complete documentation |

## Contributing

Contributions are welcome. Please read [CONTRIBUTING.md](CONTRIBUTING.md)
before opening a pull request, and report security issues privately as
described in [SECURITY.md](SECURITY.md).

## License

taskmill is released under the [MIT License](LICENSE).
