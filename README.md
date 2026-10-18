# taskmill

[![CI](https://github.com/code19m/taskmill/actions/workflows/ci.yml/badge.svg)](https://github.com/code19m/taskmill/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/code19m/taskmill.svg)](https://pkg.go.dev/github.com/code19m/taskmill)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

A PostgreSQL-backed task queue and transactional outbox for Go.

If your service already runs on PostgreSQL, your background tasks can live
there too: one less system to operate, and tasks that commit or roll back
together with your data.

- **Transactional.** Enqueue inside your own transaction, with pgx or
  `database/sql`. If it rolls back, the task never existed.
- **Reliable.** At-least-once execution with leases, heartbeats, retries with
  backoff, and a dead-letter queue: dead tasks are kept for inspection and
  retry. A crashed worker never loses a task.
- **Ordered when you need it.** Strict FIFO groups, for any number of keys,
  that can halt on a dead task instead of skipping it.
- **Complete.** Priorities, delays, deadlines, deduplication, cron schedules,
  concurrency limits, batch handlers, cancellation, and pausing.
- **An outbox, too.** Publish to Kafka, NATS, or RabbitMQ with the same
  transactional guarantee, in order per key.
- **Operable.** A web interface, an inspection API, structured logs through
  `log/slog`, and middleware for metrics and tracing.
- **Small and fast.** One dependency, pgx. On a laptop, one worker runs over
  35,000 tasks per second, and one relay publishes over 35,000 outbox messages
  per second; see the [benchmarks](docs/benchmarks.md).

> [!NOTE]
> taskmill is pre-1.0: minor releases may change the API until v1.0.0, and
> the release notes say how to migrate.

## Install

```sh
go get github.com/code19m/taskmill
```

taskmill needs Go 1.27 or newer and PostgreSQL 15 or newer.

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

    worker, err := taskmill.NewWorker(client, &taskmill.WorkerConfig{
        Handlers: &handlers,
        Queues:   map[string]taskmill.QueueOptions{taskmill.DefaultQueue: {Concurrency: 20}},
    })
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

A handler that returns an error is retried with backoff until the task runs
out of attempts and becomes dead. `taskmill.Permanent(err)` makes it dead at
once, and `taskmill.Snooze(d)` runs it again later without using an attempt.

## Documentation

- **[Guide](docs/guide.md):** every feature with examples, testing,
  production settings, observability, and how taskmill fits monoliths,
  modular monoliths, microservices, and shared database clusters.
- **[API reference](https://pkg.go.dev/github.com/code19m/taskmill)** on
  pkg.go.dev.
- **[Design](docs/design):** the task lifecycle and its guarantees, the storage
  schema, the public API, and the performance targets.
- **[Decisions](docs/adr):** why taskmill is built the way it is.
- **[Benchmarks](docs/benchmarks.md):** measured throughput, latency, and
  behavior under faults.

| Package | What it is |
|---|---|
| [`taskmill`](https://pkg.go.dev/github.com/code19m/taskmill) | The client, workers, schedules, and the inspection API |
| [`taskmill/outbox`](https://pkg.go.dev/github.com/code19m/taskmill/outbox) | The transactional outbox for message brokers |
| [`taskmill/ui`](https://pkg.go.dev/github.com/code19m/taskmill/ui) | The web interface, as an `http.Handler` |
| [`taskmill/taskmilltest`](https://pkg.go.dev/github.com/code19m/taskmill/taskmilltest) | Helpers to test your code against a real PostgreSQL |

## Contributing

Contributions are welcome. Please read [CONTRIBUTING.md](CONTRIBUTING.md)
before opening a pull request, and report security issues privately as
described in [SECURITY.md](SECURITY.md).

## License

taskmill is released under the [MIT License](LICENSE).
