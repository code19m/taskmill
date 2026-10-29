# Quickstart

In about 15 minutes, you build a tiny shop. When a customer places an order,
the shop adds a task: "email the receipt". A worker runs the task, and you
watch it all in the dashboard. You need Go 1.26 or newer, Docker, and `curl`.

## Step 1. Start PostgreSQL

```sh
docker run --name taskmill-pg -e POSTGRES_PASSWORD=postgres -p 5432:5432 -d postgres:18
```

Wait until `docker exec taskmill-pg pg_isready -h 127.0.0.1` prints
`accepting connections`. If port 5432 is already in use, use
`-p 5433:5432`, and port 5433 in step 4.

## Step 2. Create the project

```sh
mkdir shop && cd shop
go mod init example.com/shop
go get github.com/code19m/taskmill@latest
```

## Step 3. Write the code

Create `main.go`. Its comments say what each part does.

```go
// Command shop is the taskmill quickstart.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/code19m/taskmill"
	"github.com/code19m/taskmill/ui"
)

// SendReceipt is a task: a Go struct, saved as JSON, with a Kind method.
type SendReceipt struct {
	OrderID int64  `json:"order_id"`
	Email   string `json:"email"`
}

// Kind names the task. It tells taskmill which handler runs it.
func (SendReceipt) Kind() string { return "email.send_receipt" }

// sendReceipt is the handler: the code that runs one SendReceipt task.
func sendReceipt(ctx context.Context, t *taskmill.Task[SendReceipt]) error {
	if t.Args.Email == "" {
		// A retry cannot help, so give up now. The task becomes dead.
		return taskmill.Permanent(errors.New("the order has no email address"))
	}
	if rand.IntN(3) == 0 {
		// Pretend that the mail server failed. taskmill tries again soon.
		slog.WarnContext(ctx, "mail server did not answer", "task_id", t.ID, "attempt", t.Attempt)
		return errors.New("mail server did not answer")
	}
	slog.InfoContext(ctx, "receipt sent", "task_id", t.ID, "attempt", t.Attempt, "to", t.Args.Email)
	return nil
}

func main() {
	if err := run(); err != nil {
		slog.Error("shop stopped", "error", err)
		os.Exit(1)
	}
}

func run() error {
	// Stop on Ctrl-C, or when the system sends SIGTERM.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// 15 connections: 10 for the tasks that run at the same time, 5 for the worker.
	cfg, err := pgxpool.ParseConfig(os.Getenv("DATABASE_URL"))
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
	if err := taskmill.Migrate(ctx, pool, nil); err != nil {
		return err
	}
	_, err = pool.Exec(ctx, `CREATE TABLE IF NOT EXISTS orders (
		id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, email text NOT NULL)`)
	if err != nil {
		return err
	}

	// The client adds and manages tasks. Without a logger, taskmill logs nothing.
	client, err := taskmill.NewClient(ctx, pool, &taskmill.ClientConfig{Logger: slog.Default()})
	if err != nil {
		return err
	}

	// The worker takes tasks from the database and runs their handlers.
	var handlers taskmill.Handlers
	taskmill.AddHandler(&handlers, taskmill.HandlerFunc[SendReceipt](sendReceipt), nil)
	worker, err := taskmill.NewWorker(client, &taskmill.WorkerConfig{Handlers: &handlers})
	if err != nil {
		return err
	}

	// The dashboard is an http.Handler. In a real app, put your login in front of it.
	dashboard, err := ui.New(client, &ui.Config{Prefix: "/taskmill"})
	if err != nil {
		return err
	}
	mux := http.NewServeMux()
	mux.Handle("/taskmill/", dashboard)
	mux.Handle("POST /orders", placeOrder(pool, client))
	server := &http.Server{Addr: "localhost:8080", Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	go func() {
		if err := server.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			slog.Error("web server failed", "error", err)
			stop()
		}
	}()
	slog.Info("open the dashboard at http://localhost:8080/taskmill/")

	// Run the worker until Ctrl-C. Then it lets running tasks finish.
	err = worker.Run(ctx)
	_ = server.Shutdown(context.Background())
	return err
}

// placeOrder saves an order and adds its receipt task in one transaction:
// both are saved, or neither is.
func placeOrder(pool *pgxpool.Pool, client *taskmill.Client) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		email := r.FormValue("email")
		var res taskmill.EnqueueResult
		err := pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
			var orderID int64
			err := tx.QueryRow(ctx, "INSERT INTO orders (email) VALUES ($1) RETURNING id", email).Scan(&orderID)
			if err != nil {
				return err
			}
			res, err = client.EnqueueTx(ctx, taskmill.Pgx(tx), SendReceipt{OrderID: orderID, Email: email}, nil)
			return err
		})
		if err != nil {
			slog.ErrorContext(ctx, "placing the order failed", "error", err)
			http.Error(w, "placing the order failed", http.StatusInternalServerError)
			return
		}
		fmt.Fprintf(w, "task %d: http://localhost:8080/taskmill/tasks/%d\n", res.ID, res.ID)
	})
}
```

> [!TIP]
> `placeOrder` uses `EnqueueTx`, so the order and its task commit together.
> If saving the order fails, there is no task, and no receipt goes out. Use
> `client.Enqueue` only when you have no data to save.

## Step 4. Run it

```sh
go mod tidy
export DATABASE_URL='postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable'
go run .
```

The log shows `worker started`, then `elected maintainer`: this worker also
does shared work, such as retries ([maintainer](../concepts/glossary.md#maintainer)).
If you see `connection refused` instead, PostgreSQL is not ready yet: wait,
and run `go run .` again.

## Step 5. Place orders

In a second terminal, place 10 orders:

```sh
for i in $(seq 1 10); do curl -s -d "email=user$i@example.com" localhost:8080/orders; done
```

Each one prints a link to its task. About one attempt in three fails on
purpose, so the log shows taskmill trying again, about a second later:

```text
WARN mail server did not answer task_id=6 attempt=1
INFO receipt sent task_id=6 attempt=2 to=user6@example.com
```

## Step 6. Look at the dashboard

Open <http://localhost:8080/taskmill/>. **Overview** shows your queue and
charts. **Tasks** lists every task with its state. Click a task to see its
data, its attempts, and the error of each failed attempt. **Workers** shows
your worker, marked as the maintainer.

## Step 7. Make a task fail for good

Place an order without an email address:

```sh
curl -d email= localhost:8080/orders
```

The handler returns `taskmill.Permanent`, so the task is dead right away.
The log shows `ERROR attempt failed; the task is dead`, and the dashboard's
menu shows **Dead 1**. Open the task: **Retry** runs it, **Delete** removes it.

## Step 8. Stop

Press Ctrl-C. The worker lets running tasks finish, logs `worker stopped`,
and exits. To remove the database too, run `docker rm -f taskmill-pg`.

## What you learned

| You wrote | taskmill calls it |
|---|---|
| `type SendReceipt` with `Kind()` | a [task](../concepts/glossary.md#task) and its [kind](../concepts/glossary.md#kind) |
| `func sendReceipt(…)` | a [handler](../concepts/glossary.md#handler) |
| `client.EnqueueTx(…)` | an enqueue in your own transaction |
| `return errors.New(…)` | a failed [attempt](../concepts/glossary.md#attempt), retried after a [backoff](../concepts/glossary.md#backoff) |
| `return taskmill.Permanent(…)` | a [dead task](../concepts/glossary.md#dead-task) |
| `cfg.MaxConns = 15` | a pool sized for the worker: its concurrency (10) plus 5 |
| `worker.Run(ctx)`, then Ctrl-C | a [worker](../concepts/glossary.md#worker) that [drains](../concepts/glossary.md#drain), then stops |

## See also

- [Shop example](../../examples/shop/README.md): a bigger shop that uses most features
- [Test your code](../guides/testing.md): test a handler like `sendReceipt`
- [`Example`](https://pkg.go.dev/github.com/code19m/taskmill#example-package) on pkg.go.dev: the smallest program

**Next:** [How it works](../concepts/how-it-works.md)
