# Part 2: Tasks with your data

**Goal:** save an order and its task together, so that both exist or
neither does. **Time:** about 10 minutes. **You start from:** the program
of [part 1](1-first-task.md).

## Step 1. Create a table for orders

The shop needs a table of its own. Add this function at the end of
`main.go`:

```go
// createTables creates the shop's own table, unless it exists. A real app
// leaves this to its migration tool.
func createTables(ctx context.Context, pool *pgxpool.Pool) error {
	_, err := pool.Exec(ctx, `CREATE TABLE IF NOT EXISTS orders (
		id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, email text NOT NULL)`)
	return err
}
```

Call it in `run`, below the call of `Migrate`:

```go
	if err := createTables(ctx, pool); err != nil {
		return err
	}
```

Start the program with `go run .`, and stop it with Ctrl-C. Then look at
the table. With the database from the quickstart:

```sh
docker exec taskmill-pg psql -U postgres -c '\d orders'
```

**You should see** the new table. With another database, use your own
`psql`.

```text
                         Table "public.orders"
 Column |  Type  | Collation | Nullable |           Default
--------+--------+-----------+----------+------------------------------
 id     | bigint |           | not null | generated always as identity
 email  | text   |           | not null |
Indexes:
    "orders_pkey" PRIMARY KEY, btree (id)
```

## Step 2. Save an order and its task in one transaction

Add `placeOrder` at the end of `main.go`. It answers `POST /orders`. It
saves the order, and it calls `EnqueueTx` with the same transaction, `tx`:

```go
// placeOrder saves an order and adds its receipt task in one transaction:
// both are saved, or neither is.
func placeOrder(pool *pgxpool.Pool, client *taskmill.Client, logger *slog.Logger) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		email := r.FormValue("email")
		var orderID int64
		var res taskmill.EnqueueResult
		err := pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
			err := tx.QueryRow(ctx, "INSERT INTO orders (email) VALUES ($1) RETURNING id", email).Scan(&orderID)
			if err != nil {
				return err
			}
			res, err = client.EnqueueTx(ctx, taskmill.Pgx(tx), SendReceipt{OrderID: orderID, Email: email}, nil)
			return err
		})
		if err != nil {
			logger.LogAttrs(ctx, slog.LevelError, "placing the order failed", slog.Any(logKeyError, err))
			http.Error(w, "placing the order failed", http.StatusInternalServerError)
			return
		}
		_, _ = fmt.Fprintf(w, "order %d placed, task %d enqueued\n", orderID, res.ID)
	})
}
```

Now let the shop take orders over HTTP. In `run`, delete the temporary
lines of part 1: from the comment `// Add one task` to the line that logs
`task enqueued`. Then add a route above the comment `// Run the worker`:

```go
	// The shop's own server takes the orders.
	mux := http.NewServeMux()
	mux.Handle("POST /orders", placeOrder(pool, client, logger))
```

Below the route, start the server:

```go
	// Serve until run returns. Set ADDR if port 8080 is taken.
	addr := cmp.Or(os.Getenv("ADDR"), defaultAddr)
	ln, err := new(net.ListenConfig).Listen(ctx, "tcp", addr)
	if err != nil {
		return err
	}
	server := &http.Server{Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	go func() { _ = server.Serve(ln) }() // it returns when Shutdown is called
	defer func() { _ = server.Shutdown(context.WithoutCancel(ctx)) }()
```

Add the constant below the log keys:

```go
// defaultAddr is where the shop listens.
const defaultAddr = "localhost:8080"
```

Add `"fmt"`, `"net"`, `"net/http"`, `"time"`, and
`"github.com/jackc/pgx/v5"` to the imports. Start the program. In a second
terminal, place an order:

```sh
curl -s -d email=user1@example.com localhost:8080/orders
```

**You should see** the IDs of the order and of its task. Your task ID
depends on how often you started the program before.

```text
order 1 placed, task 5 enqueued
```

The log of the shop shows that the worker ran the task:

```text
time=… level=INFO msg="receipt sent" task_id=5 to=user1@example.com
```

If the shop stops with `address already in use`, port 8080 is taken. Start
it with `ADDR=localhost:9090 go run .`, and use that address in every
command.

## Step 3. Make the transaction fail

Now see what happens when the transaction fails after the enqueue. Stop the
shop. In `placeOrder`, change the line `return err` below `EnqueueTx`, and
add `"errors"` to the imports:

```go
			res, err = client.EnqueueTx(ctx, taskmill.Pgx(tx), SendReceipt{OrderID: orderID, Email: email}, nil)
			return errors.Join(err, errors.New("the payment failed"))
```

Start the shop again, and place an order:

```sh
curl -s -d email=user2@example.com localhost:8080/orders
```

**You should see** an error from `curl`, and an error in the log of the
shop, but no receipt:

```text
placing the order failed
```

```text
time=… level=ERROR msg="placing the order failed" error="the payment failed"
```

The second order is not in the table either:

```sh
docker exec taskmill-pg psql -U postgres -c 'SELECT * FROM orders'
```

```text
 id |       email
----+-------------------
  1 | user1@example.com
(1 row)
```

PostgreSQL rolled back the order and its task together. No worker saw the
task, so no customer gets a receipt for an order that does not exist.

Change the line back to `return err`, and remove `"errors"` from the
imports. The next order gets the number 3: PostgreSQL does not use a number
twice.

## What you learned

| In the code | taskmill calls it |
|---|---|
| `client.EnqueueTx(ctx, taskmill.Pgx(tx), …)` | an [enqueue](../concepts/glossary.md#enqueue) in your own transaction |
| `taskmill.Pgx(tx)` | an executor: your pgx transaction, wrapped so that taskmill can use it |
| an error from the transaction's function | a rollback: the order and the task are both gone |
| `res.ID` | the task's ID, which the logs and the dashboard show |

## Go deeper

- [Use your own transactions](../guides/transactions.md): `database/sql`, bun, and sqlx, and what happens on commit
- [Enqueue tasks](../guides/enqueue.md): `Enqueue` or `EnqueueTx`, and every option
- [How it works](../concepts/how-it-works.md): the life of a task, from your transaction to a worker

**Next:** [Part 3: When a task fails](3-when-a-task-fails.md)
