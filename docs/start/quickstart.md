# Quickstart

In about 15 minutes, you run a tiny shop. When a customer places an order,
the shop adds a task: "email the receipt". A worker runs the task, and you
watch it all in the dashboard. You need Go 1.26 or newer, Git, Docker, and
`curl`.

## Step 1. Start PostgreSQL

```sh
docker run --name taskmill-pg -e POSTGRES_PASSWORD=postgres -p 5432:5432 -d postgres:18
```

Wait until `docker exec taskmill-pg pg_isready -h 127.0.0.1` prints
`accepting connections`.

## Step 2. Get the code

```sh
git clone https://github.com/code19m/taskmill.git
cd taskmill
```

The shop is one short file,
[examples/quickstart/main.go](../../examples/quickstart/main.go). The next
step shows its key parts.

## Step 3. Read the key parts

A [task](../concepts/glossary.md#task) is a Go struct, saved as JSON. Its
[kind](../concepts/glossary.md#kind) tells taskmill which handler runs it:

```go
type SendReceipt struct {
	OrderID int64  `json:"order_id"`
	Email   string `json:"email"`
}

func (SendReceipt) Kind() string { return "email.send_receipt" }
```

A [handler](../concepts/glossary.md#handler) runs one task. It returns nil
when the work is done, and an error to try again later. Here,
`sendReceipt` returns the handler, so that the handler can log:

```go
func sendReceipt(logger *slog.Logger) taskmill.HandlerFunc[SendReceipt] {
	return func(ctx context.Context, t *taskmill.Task[SendReceipt]) error {
		if t.Args.Email == "" {
			// A retry cannot help, so give up now: the task becomes dead.
			return taskmill.Permanent(errors.New("the order has no email address"))
		}
		if rand.IntN(3) == 0 {
			// Pretend that the mail server failed: taskmill tries again soon.
			return errors.New("mail server did not answer")
		}
		logger.LogAttrs(ctx, slog.LevelInfo, "receipt sent",
			slog.Int64(logKeyTaskID, t.ID), slog.String(logKeyTo, t.Args.Email))
		return nil
	}
}
```

`placeOrder` saves an order and its task in one transaction. Both are
saved, or neither is:

```go
err := pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
	var orderID int64
	err := tx.QueryRow(ctx, "INSERT INTO orders (email) VALUES ($1) RETURNING id", email).Scan(&orderID)
	if err != nil {
		return err
	}
	res, err = client.EnqueueTx(ctx, taskmill.Pgx(tx), SendReceipt{OrderID: orderID, Email: email}, nil)
	return err
})
```

`run` puts it all together. It opens a pool of 15 connections, creates
taskmill's tables with `Migrate`, and then starts the parts:

```go
client, err := taskmill.NewClient(ctx, pool, &taskmill.ClientConfig{Logger: logger})
// …
var handlers taskmill.Handlers
taskmill.AddHandler(&handlers, sendReceipt(logger), nil)
worker, err := taskmill.NewWorker(client, &taskmill.WorkerConfig{Handlers: &handlers})
// …
dashboard, err := ui.New(client, &ui.Config{Prefix: "/taskmill"}) // an http.Handler
// …
err = worker.Run(ctx) // until Ctrl-C
```

> [!TIP]
> Use `EnqueueTx` whenever a task belongs to data that you save. If saving
> the order fails, there is no task, and no receipt goes out. Use
> `client.Enqueue` only when you have no data to save.

## Step 4. Run it

```sh
go run ./examples/quickstart
```

The log shows `worker started`, then `elected maintainer`: this worker
also does the shared work, such as retries
([maintainer](../concepts/glossary.md#maintainer)). If you see
`connection refused`, PostgreSQL is not ready yet: wait, and try again. To
use another database, set `DATABASE_URL`.

## Step 5. Place orders

In a second terminal, place 10 orders:

```sh
for i in $(seq 1 10); do curl -s -d "email=user$i@example.com" localhost:8080/orders; done
```

Each one prints a link to its task. About one attempt in three fails on
purpose. taskmill logs each failure at Debug level, which the shop turns
on, and runs the task again about a second later:

```text
time=… level=DEBUG msg="attempt failed; retrying" … task_id=7 kind=email.send_receipt attempt=1 error="mail server did not answer" … retry_in=932.733785ms
time=… level=INFO msg="receipt sent" task_id=7 to=user7@example.com
```

## Step 6. Look at the dashboard

Open <http://localhost:8080/taskmill/>. **Overview** shows your queue and
charts. **Tasks** lists every task with its state. Click a task to see its
data, its attempts, and the error of each failed attempt. **Workers** shows
your worker, marked as the maintainer.

## Step 7. Make a task fail for good

Place an order without an email address:

```sh
curl -s -d email= localhost:8080/orders
```

The handler returns `taskmill.Permanent`, so the task is dead right away.
The log shows `attempt failed; the task is dead`. Within about 10 seconds,
the dashboard's menu shows **Dead 1**. Open the task: **Retry** runs it
again, and **Delete** removes it.

## Step 8. Stop

Press Ctrl-C. The worker lets running tasks finish, logs `worker stopped`,
and exits. To remove the database too, run `docker rm -f taskmill-pg`.

## What you learned

| In the code | taskmill calls it |
|---|---|
| `type SendReceipt` with `Kind()` | a [task](../concepts/glossary.md#task) and its [kind](../concepts/glossary.md#kind) |
| the function that `sendReceipt` returns | a [handler](../concepts/glossary.md#handler) |
| `client.EnqueueTx(…)` | an enqueue in your own transaction |
| `return errors.New(…)` | a failed [attempt](../concepts/glossary.md#attempt), retried after a [backoff](../concepts/glossary.md#backoff) |
| `return taskmill.Permanent(…)` | a [dead task](../concepts/glossary.md#dead-task) |
| `cfg.MaxConns = 15` | a pool sized for the worker: its concurrency (10) plus 5 |
| `worker.Run(ctx)`, then Ctrl-C | a [worker](../concepts/glossary.md#worker) that [drains](../concepts/glossary.md#drain), then stops |

To start a project of your own, run `go get github.com/code19m/taskmill` in
your module, and copy the parts you need from `main.go`.

## See also

- [Shop example](../../examples/shop/README.md): a bigger shop that uses most features
- [Test your code](../guides/testing.md): test a handler like `sendReceipt`
- [`Example`](https://pkg.go.dev/github.com/code19m/taskmill#example-package) on pkg.go.dev: the smallest program

**Next:** [How it works](../concepts/how-it-works.md)
