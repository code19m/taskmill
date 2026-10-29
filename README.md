# Taskmill

[![CI](https://github.com/code19m/taskmill/actions/workflows/ci.yml/badge.svg)](https://github.com/code19m/taskmill/actions/workflows/ci.yml)
[![OpenSSF Scorecard](https://api.scorecard.dev/projects/github.com/code19m/taskmill/badge)](https://scorecard.dev/viewer/?uri=github.com/code19m/taskmill)
[![Go Reference](https://pkg.go.dev/badge/github.com/code19m/taskmill.svg)](https://pkg.go.dev/github.com/code19m/taskmill)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

Background tasks for Go, kept in your PostgreSQL database.

If your service already uses PostgreSQL, your tasks can live there too. You
run one less system, and each task commits or rolls back with your data.

- **In your transaction.** Add a task in the same transaction as your data,
  with pgx or `database/sql`. If the transaction rolls back, the task never
  existed.
- **Never loses a task.** Every task runs at least once. When a worker
  crashes, another worker runs its tasks. A task that keeps failing is kept
  as a dead task, so that you can fix the cause and retry it.
- **In order when you need it.** Tasks with the same key, such as one
  customer's, run one at a time, in order.
- **Outbox included.** Publish events to Kafka, NATS, or RabbitMQ only if
  your transaction commits, in order per key.
- **Dashboard included.** Watch queues and tasks, and retry, cancel, or
  delete them. Mount it in your app, or run it on its own.

Also: delays, priorities, cron schedules, deduplication, limits across all
workers, batch handlers, logs through `log/slog`, and OpenTelemetry. One
worker runs about 30,000 tasks per second on a laptop
([performance](docs/reference/performance.md)), and the only direct
dependency is pgx.

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="docs/images/dashboard-dark.png">
  <img alt="The dashboard's overview: task counts, charts of the last hour's tasks and attempts, and the queues" src="docs/images/dashboard-light.png">
</picture>

The dashboard, showing the [shop example](examples/shop).

## A quick look

```go
// A task is a Go struct with a kind. taskmill stores it as JSON.
type SendReceipt struct {
	OrderID int64 `json:"order_id"`
}

func (SendReceipt) Kind() string { return "email.send_receipt" }

// placeOrder saves an order and its task in one transaction: both, or neither.
func placeOrder(ctx context.Context, pool *pgxpool.Pool, client *taskmill.Client, email string) error {
	return pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		var orderID int64
		err := tx.QueryRow(ctx, "INSERT INTO orders (email) VALUES ($1) RETURNING id", email).Scan(&orderID)
		if err != nil {
			return err
		}
		_, err = client.EnqueueTx(ctx, taskmill.Pgx(tx), SendReceipt{OrderID: orderID}, nil)
		return err
	})
}

// runWorker runs tasks until ctx ends. A handler returns nil when the work is
// done, or an error to try again later.
func runWorker(ctx context.Context, client *taskmill.Client) error {
	var handlers taskmill.Handlers
	taskmill.AddHandler(&handlers, taskmill.HandlerFunc[SendReceipt](
		func(ctx context.Context, t *taskmill.Task[SendReceipt]) error {
			slog.InfoContext(ctx, "sending the receipt", "order_id", t.Args.OrderID)
			return nil
		}), nil)
	worker, err := taskmill.NewWorker(client, &taskmill.WorkerConfig{Handlers: &handlers})
	if err != nil {
		return err
	}
	return worker.Run(ctx)
}
```

The [quickstart](docs/start/quickstart.md) runs this as a complete
program, with the dashboard, in about 15 minutes.

## Install

```sh
go get github.com/code19m/taskmill
```

You need Go 1.26 or newer, and PostgreSQL 15 or newer.

## Learn more

- **[Quickstart](docs/start/quickstart.md):** your first task, step by step.
- **[Documentation](docs/README.md):** concepts, guides, and running in production.
- **[How do I…?](docs/recipes/README.md):** the best way to do common things.
- **[API reference](https://pkg.go.dev/github.com/code19m/taskmill)** on pkg.go.dev.
- **[Shop example](examples/shop):** a small shop that uses most features, with its dashboard.

| Package | What it is |
|---|---|
| [`taskmill`](https://pkg.go.dev/github.com/code19m/taskmill) | The client, workers, schedules, and the operations API |
| [`taskmill/outbox`](https://pkg.go.dev/github.com/code19m/taskmill/outbox) | The outbox, to publish to a message broker |
| [`taskmill/ui`](https://pkg.go.dev/github.com/code19m/taskmill/ui) | The dashboard, as an `http.Handler` |
| [`taskmill/cmd/taskmill-ui`](https://pkg.go.dev/github.com/code19m/taskmill/cmd/taskmill-ui) | The dashboard as a program of its own |
| [`taskmill/taskmilltest`](https://pkg.go.dev/github.com/code19m/taskmill/taskmilltest) | Helpers to test your code against a real PostgreSQL |
| [`taskmill/otel`](https://pkg.go.dev/github.com/code19m/taskmill/otel) | OpenTelemetry traces and metrics, in a module of its own |

## Status

The API is final for v1.0.0, which is not tagged yet: it waits for more use
in production. [Compatibility](docs/reference/compatibility.md) says what
each release promises.

## Contributing

Contributions are welcome. Please read [CONTRIBUTING.md](CONTRIBUTING.md)
before you open a pull request. Report security issues privately, as
[SECURITY.md](SECURITY.md) describes.

## License

taskmill is released under the [MIT License](LICENSE).
