# Packages

Which package to import for what. Each name links to its API on pkg.go.dev.

| Package | What it is | Use it to… |
|---|---|---|
| [`taskmill`](https://pkg.go.dev/github.com/code19m/taskmill) | The client, workers, schedules, and the operations API | Enqueue, run, and manage tasks |
| [`taskmill/outbox`](https://pkg.go.dev/github.com/code19m/taskmill/outbox) | The transactional outbox | Publish messages to Kafka, NATS, RabbitMQ, or another broker, only if your transaction commits |
| [`taskmill/ui`](https://pkg.go.dev/github.com/code19m/taskmill/ui) | The dashboard, as an `http.Handler` | Add the dashboard to your own HTTP server |
| [`taskmill/cmd/taskmill-ui`](https://pkg.go.dev/github.com/code19m/taskmill/cmd/taskmill-ui) | The dashboard as a program of its own: a binary, and the image `ghcr.io/code19m/taskmill-ui` | Run the dashboard where you have no Go HTTP server |
| [`taskmill/taskmilltest`](https://pkg.go.dev/github.com/code19m/taskmill/taskmilltest) | Test helpers that use a real PostgreSQL | Test what your code enqueues, and run your handlers in tests |
| [`taskmill/otel`](https://pkg.go.dev/github.com/code19m/taskmill/otel) | OpenTelemetry traces and metrics, in a module of its own | Trace tasks from the request to the handler, and measure queues |

All packages but `otel` are in one Go module:

```sh
go get github.com/code19m/taskmill
```

Its only direct dependency is pgx, the PostgreSQL driver
([ADR 0001](../adr/0001-dependency-policy.md),
[ADR 0004](../adr/0004-pgx-as-the-postgresql-driver.md)).

## The `otel` module

`otel` is a Go module of its own, so that applications without OpenTelemetry
download none of it:

```sh
go get github.com/code19m/taskmill/otel
```

- **Its own versions.** Its releases are tagged `otel/vX.Y.Z`, and its
  [changelog](../../otel/CHANGELOG.md) is separate.
- **It needs a recent taskmill.** Its `go.mod` names the oldest taskmill
  release it works with.
- **It stays below v1** while OpenTelemetry's messaging conventions settle.
  Its Go API follows taskmill's [compatibility](compatibility.md) promise,
  but a minor release may rename a span, attribute, or metric.

[ADR 0005](../adr/0005-opentelemetry-in-a-module-of-its-own.md) explains
why.

## See also

- [Metrics and traces](metrics.md): what the `otel` module reports
- [Dashboard](../guides/dashboard.md): `ui` or `taskmill-ui`, and when to use which
- [Testing](../guides/testing.md): how to use `taskmilltest`

**Next:** [Performance](performance.md)
