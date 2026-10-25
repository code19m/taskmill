# Taskmill

[![CI](https://github.com/code19m/taskmill/actions/workflows/ci.yml/badge.svg)](https://github.com/code19m/taskmill/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/code19m/taskmill.svg)](https://pkg.go.dev/github.com/code19m/taskmill)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

A PostgreSQL-backed task queue and transactional outbox for Go.

If your service already runs on PostgreSQL, your background tasks can live
there too: one less system to operate, and tasks that commit or roll back
together with your data.

- **Transactional.** Enqueue inside your own transaction, with pgx or
  `database/sql`. If it rolls back, the task never existed.
- **Reliable.** Every task runs at least once, with leases, heartbeats, and
  retries with backoff. Dead tasks are kept for inspection and retry. A crashed
  worker never loses a task.
- **Ordered when you need it.** FIFO groups run tasks one at a time per key,
  for any number of keys, and halt on a dead task instead of skipping it.
- **Complete.** Priorities, delays, deduplication, cron schedules,
  concurrency limits across all workers, batch handlers, cancellation, and
  pausing.
- **An outbox, too.** Publish to Kafka, NATS, or RabbitMQ with the same
  transactional guarantee, in order per key.
- **Operable.** A live web dashboard that you mount in your application,
  embed in your admin panel, or run on its own; an operations API;
  structured logs through `log/slog`; and OpenTelemetry traces and metrics
  from an optional module.
- **Small and fast.** One direct dependency, pgx. One worker drained a backlog
  at over 35,000 tasks per second, and one relay published over 35,000 outbox
  messages per second. This was on an Apple M5 laptop with PostgreSQL 18 in
  Docker; see the [benchmarks](docs/benchmarks.md).

> **Status:** taskmill is new, with no known production users yet. Before
> v1.0.0, minor releases may change the API, and the
> [changelog](CHANGELOG.md) says how to migrate. See
> [Upgrading](docs/guide.md#upgrading).

## Why taskmill?

taskmill is closest to [River](https://riverqueue.com). Both keep tasks in
PostgreSQL, use pgx, and enqueue in your own transaction. taskmill also
includes FIFO ordering per key, concurrency limits across all workers, batch
handlers, a transactional outbox, and a web interface, in one MIT-licensed
module. River offers several of these only in its paid River Pro. Choose
River if you need workflows, a command-line tool, or a long production
record.

## What taskmill is not

- **A workflow engine.** Tasks do not depend on each other.
- **A message broker.** The outbox hands messages to your broker.
- **Exactly-once across systems.** Delivery is at least once, and consumers
  deduplicate.
- **For other databases.** It runs on PostgreSQL only.

The [design overview](docs/design/README.md#out-of-scope) lists the rest.

## Install

```sh
go get github.com/code19m/taskmill
```

taskmill needs Go 1.26 or newer and PostgreSQL 15 or newer
([supported versions](docs/adr/0002-supported-versions.md)).

## Quick start

```go
// A task is a struct with a kind.
type SendReceipt struct {
    OrderID int64  `json:"order_id"`
    Email   string `json:"email"`
}

func (SendReceipt) Kind() string { return "email.send_receipt" }

func main() {
    ctx := context.Background()
    pool, err := pgxpool.New(ctx, os.Getenv("DATABASE_URL"))
    if err != nil {
        log.Fatal(err)
    }

    // Create or upgrade the schema. Safe to run from every process.
    if err := taskmill.Migrate(ctx, pool, nil); err != nil {
        log.Fatal(err)
    }
    client, err := taskmill.NewClient(ctx, pool, nil)
    if err != nil {
        log.Fatal(err)
    }

    // Enqueue inside your own transaction: the order and its receipt commit
    // together, or not at all.
    err = pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
        // ... insert the order ...
        _, err := client.EnqueueTx(ctx, taskmill.Pgx(tx), SendReceipt{OrderID: 42, Email: "a@example.com"}, nil)
        return err
    })
    if err != nil {
        log.Fatal(err)
    }

    // Handle tasks of that kind.
    var handlers taskmill.Handlers
    taskmill.AddHandler(&handlers, taskmill.HandlerFunc[SendReceipt](
        func(ctx context.Context, t *taskmill.Task[SendReceipt]) error {
            return sendEmail(ctx, t.Args.Email, t.Args.OrderID)
        }), nil)

    worker, err := taskmill.NewWorker(client, &taskmill.WorkerConfig{Handlers: &handlers})
    if err != nil {
        log.Fatal(err)
    }

    // Run until SIGTERM, then drain running tasks.
    ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
    defer stop()
    if err := worker.Run(ctx); err != nil {
        log.Fatal(err)
    }
}
```

A runnable version is `Example` in [example_test.go](example_test.go). A
handler that returns an error is retried with backoff, by default for about
13 hours, and then the task becomes dead. `taskmill.Permanent(err)` makes it
dead at once, and `taskmill.Snooze(d)` runs it again later without using an
attempt.

## Documentation

- **[Guide](docs/guide.md):** how to use every feature, test your code, and
  run in production.
- **[API reference](https://pkg.go.dev/github.com/code19m/taskmill)** on
  pkg.go.dev.
- **[Design](docs/design):** the task lifecycle and its guarantees, and the
  storage schema.
- **[Benchmarks](docs/benchmarks.md):** performance targets and measured
  results.
- **[Decisions](docs/adr):** why taskmill is built the way it is.

| Package | What it is |
|---|---|
| [`taskmill`](https://pkg.go.dev/github.com/code19m/taskmill) | The client, workers, schedules, and the operations API |
| [`taskmill/outbox`](https://pkg.go.dev/github.com/code19m/taskmill/outbox) | The transactional outbox for message brokers |
| [`taskmill/ui`](https://pkg.go.dev/github.com/code19m/taskmill/ui) | The web interface, as an `http.Handler` |
| [`taskmill/cmd/taskmill-ui`](https://pkg.go.dev/github.com/code19m/taskmill/cmd/taskmill-ui) | The web interface as a dashboard of its own |
| [`taskmill/taskmilltest`](https://pkg.go.dev/github.com/code19m/taskmill/taskmilltest) | Helpers to test your code against a real PostgreSQL |
| [`taskmill/otel`](https://pkg.go.dev/github.com/code19m/taskmill/otel) | OpenTelemetry traces and metrics, in a module of its own |

## Contributing

Contributions are welcome. Please read [CONTRIBUTING.md](CONTRIBUTING.md)
before opening a pull request, and report security issues privately as
described in [SECURITY.md](SECURITY.md).

## License

taskmill is released under the [MIT License](LICENSE).
