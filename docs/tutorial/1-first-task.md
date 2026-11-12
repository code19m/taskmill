# Part 1: Your first task

**Goal:** a program of your own that runs one background task. **Time:**
about 15 minutes. **You start from:** an empty directory, and the
PostgreSQL from [What you need](README.md#what-you-need).

## Step 1. Create the module

```sh
mkdir shop && cd shop
go mod init shop
go get github.com/code19m/taskmill
```

Your `go.mod` now requires taskmill, and pgx, the PostgreSQL driver that
taskmill is built on.

## Step 2. Connect, and create taskmill's tables

Create `main.go`. It starts with the packages that this part uses:

<!-- grows -->
```go
package main

import (
	"cmp"
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/code19m/taskmill"
)
```

Add the constants. The log keys keep the names of the log fields in one
place. The key `logKeyURL` is for part 3.

```go
// defaultDatabaseURL is the database that the quickstart's docker command starts.
const defaultDatabaseURL = "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable"

// Log attribute keys.
const (
	logKeyError  = "error"
	logKeyTaskID = "task_id"
	logKeyTo     = "to"
	logKeyURL    = "url"
)
```

Add `main`, and the start of `run`. `run` opens a pool of connections, and
`Migrate` creates taskmill's tables, in a
[schema](../concepts/glossary.md#schema) named `taskmill`:

```go
func main() {
	// Debug level shows taskmill's line for each retry.
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
	if err := run(logger); err != nil {
		logger.LogAttrs(context.Background(), slog.LevelError, "shop stopped", slog.Any(logKeyError, err))
		os.Exit(1)
	}
}

// run starts the shop, and runs it until Ctrl-C.
func run(logger *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// 15 connections: 10 for the tasks that run at the same time, 5 for the worker.
	cfg, err := pgxpool.ParseConfig(cmp.Or(os.Getenv("DATABASE_URL"), defaultDatabaseURL))
	if err != nil {
		return err
	}
	cfg.MaxConns = 15
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return err
	}
	defer pool.Close()

	// Create taskmill's tables, or update them. It is safe at every start.
	if err := taskmill.Migrate(ctx, pool, &taskmill.MigrateConfig{Logger: logger}); err != nil {
		return err
	}
```

For now, end `run` with the line `return nil` and a closing `}`. Then run
the program:

```sh
go run .
```

**You should see** one line for each migration, and then the program ends:

```text
time=… level=INFO msg="applied migration" version=1 name=initial duration=22.577667ms
…
```

Run it again: it prints nothing, because the tables are up to date. If you
see `connection refused`, PostgreSQL is not running, or `DATABASE_URL` is
wrong.

## Step 3. Define a task, and enqueue it

A [task](../concepts/glossary.md#task) is a Go struct, saved as JSON. Its
[kind](../concepts/glossary.md#kind) names it. Add the task above `main`:

```go
// SendReceipt is a task: a Go struct, saved as JSON, with a Kind method.
type SendReceipt struct {
	OrderID int64  `json:"order_id"`
	Email   string `json:"email"`
}

// Kind names the task. It tells taskmill which handler runs it.
func (SendReceipt) Kind() string { return "email.send_receipt" }
```

In `run`, replace `return nil` with a
[client](../concepts/glossary.md#client):

```go
	// The client adds and manages tasks. Without a logger, taskmill logs nothing.
	client, err := taskmill.NewClient(ctx, pool, &taskmill.ClientConfig{Logger: logger})
	if err != nil {
		return err
	}
```

Below the client, [enqueue](../concepts/glossary.md#enqueue) one task.
These lines are temporary: part 2 replaces them.

```go
	// Add one task. Part 2 replaces these lines with a real order.
	res, err := client.Enqueue(ctx, SendReceipt{OrderID: 1, Email: "user1@example.com"}, nil)
	if err != nil {
		return err
	}
	logger.LogAttrs(ctx, slog.LevelInfo, "task enqueued", slog.Int64(logKeyTaskID, res.ID))
	return nil
```

Run the program again. **You should see** the new task's ID:

```text
time=… level=INFO msg="task enqueued" task_id=1
```

The task is now a row in PostgreSQL. It waits, because no code runs it yet.

## Step 4. Write a handler, and run a worker

A [handler](../concepts/glossary.md#handler) is the code that runs one
task. It returns nil when the work is done. This one only writes to the
log. Add it below the task:

<!-- grows -->
```go
// sendReceipt returns the handler: the code that runs one SendReceipt task.
func sendReceipt(logger *slog.Logger) taskmill.HandlerFunc[SendReceipt] {
	return func(ctx context.Context, t *taskmill.Task[SendReceipt]) error {
		logger.LogAttrs(ctx, slog.LevelInfo, "receipt sent",
			slog.Int64(logKeyTaskID, t.ID), slog.String(logKeyTo, t.Args.Email))
		return nil
	}
}
```

A [worker](../concepts/glossary.md#worker) takes tasks from the database
and runs their handlers. In `run`, replace the last `return nil` with a
worker that knows your handler:

```go
	// The worker takes tasks from the database and runs their handlers.
	var handlers taskmill.Handlers
	taskmill.AddHandler(&handlers, sendReceipt(logger), nil)
	worker, err := taskmill.NewWorker(client, &taskmill.WorkerConfig{Handlers: &handlers})
	if err != nil {
		return err
	}
```

Then end `run` with these lines:

```go
	// Run the worker until Ctrl-C. Then it lets running tasks finish.
	return worker.Run(ctx)
```

Run the program. **You should see** two receipts: one for the task that
waited since step 3, and one for the task of this start.

```text
time=… level=INFO msg="task enqueued" task_id=2
time=… level=INFO msg="worker started" worker_id=… queues=[default]
time=… level=INFO msg="receipt sent" task_id=2 to=user1@example.com
time=… level=INFO msg="receipt sent" task_id=1 to=user1@example.com
time=… level=INFO msg="elected maintainer" worker_id=…
```

`elected maintainer` means that this worker also does the shared work, such
as retries: it is the [maintainer](../concepts/glossary.md#maintainer).
[Log messages](../reference/log-messages.md#messages) explains every other
line that taskmill writes.

## Step 5. Stop the worker

Press Ctrl-C. **You should see** the worker stop cleanly:

```text
time=… level=INFO msg="worker stopping; waiting for running tasks" worker_id=… tasks=0 timeout=20s
time=… level=INFO msg="worker stopped" worker_id=… released=0
```

Start the program once more: it enqueues one more task, and the worker runs
it.

## What you learned

| In the code | taskmill calls it |
|---|---|
| `taskmill.Migrate(…)` | [migrate](../concepts/glossary.md#migrate): create or update taskmill's tables |
| `taskmill.NewClient(…)` | a [client](../concepts/glossary.md#client), which enqueues and manages tasks |
| `type SendReceipt` with `Kind()` | a [task](../concepts/glossary.md#task) and its [kind](../concepts/glossary.md#kind) |
| `client.Enqueue(…)` | [enqueue](../concepts/glossary.md#enqueue): save a task, so that a worker runs it |
| the function that `sendReceipt` returns | a [handler](../concepts/glossary.md#handler) |
| `worker.Run(ctx)`, then Ctrl-C | a [worker](../concepts/glossary.md#worker) that [drains](../concepts/glossary.md#drain), then stops |

## Go deeper

- [Set up the database and client](../guides/setup.md): the schema, the pool's size, and why `NewClient` can fail
- [Define tasks](../guides/define-tasks.md): how to name a kind, and what to put in a task
- [Handle tasks](../guides/handle-tasks.md): what a handler returns, and why its context can end

**Next:** [Part 2: Tasks with your data](2-tasks-with-your-data.md)
