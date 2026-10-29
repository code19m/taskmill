# Test your code

This page shows how to test that your code enqueues the right tasks, and
that your handlers do the right work, retries included.

## Which should I use?

| Situation | What to use | Why |
|---|---|---|
| Code that enqueues or handles tasks | The `taskmilltest` package, with a real PostgreSQL | It behaves as production does: a rolled-back transaction enqueues nothing, and a group keeps its order. A mock can't show that |
| Business logic that a handler calls | A plain unit test of that logic, without taskmill | It is fast and needs no database |
| A test suite that truly has no database | A small interface of your own around the `Client` | `Client` is a struct, so you write the interface yourself |

Use `taskmilltest`. Each test gets its own schema, so tests can run in
parallel, and leave nothing behind.

## Quick example

This test checks that placing an order enqueues one receipt, and that the
receipt's handler works:

```go
func TestPlaceOrder(t *testing.T) {
    pool := testPool(t)
    client := taskmilltest.NewClient(t, pool, nil) // a new schema, dropped when the test ends

    err := pgx.BeginFunc(t.Context(), pool, func(tx pgx.Tx) error {
        return placeOrder(t.Context(), client, tx, 42) // the code under test
    })
    if err != nil {
        t.Fatal(err)
    }
    tasks := taskmilltest.Enqueued[SendReceipt](t, client) // committed tasks of this kind, oldest first
    if len(tasks) != 1 || tasks[0].Args.OrderID != 42 {
        t.Fatalf("enqueued %+v", tasks)
    }

    // Enqueue a task and run the handler on it once, as a worker would.
    info, err := taskmilltest.Run(t, client, &ReceiptSender{mailer: fakeMailer{}}, tasks[0].Args)
    if err != nil || info.State != taskmill.StateCompleted {
        t.Fatalf("got %s, %v", info.State, err)
    }
}
```

`testPool` connects to your test database. Write it once:

```go
// testPool connects to the test database, or skips the test without one.
func testPool(t *testing.T) *pgxpool.Pool {
    t.Helper()
    url := os.Getenv("TEST_DATABASE_URL")
    if url == "" {
        t.Skip("set TEST_DATABASE_URL to run this test")
    }
    pool, err := pgxpool.New(context.Background(), url)
    if err != nil {
        t.Fatal(err)
    }
    t.Cleanup(pool.Close)
    return pool
}
```

## Which helper should I use?

| You want to test that… | Use | Good to know |
|---|---|---|
| Your code enqueued the right task, or none after a rollback | `taskmilltest.Enqueued` | It returns the committed tasks of one kind, with their payloads decoded |
| Your handler does the right work, once | `taskmilltest.Run` | It holds a real lease, so `CompleteTx` works. It doesn't retry: a failed task stays `retryable` or `dead` |
| A task succeeds after it failed, or a task with special options works | `Worker.RunTask` | It runs the task right away, without waiting for its backoff |
| Everything your code enqueued gets done | `taskmilltest.Work` | It stops when no task is available or running. Tasks that wait for later, such as retries, stay waiting |
| Several workers work together, with real timing | `Worker.Run`, started by your test | Set a short `PollInterval` |

`Work` fails the test if tasks are still there after a minute, for example
in a queue that its worker doesn't read. To test that a rollback enqueues
nothing, roll back instead of commit, and check that `Enqueued` returns no
task.

## Test retries

Enqueue a task, and call `Worker.RunTask` in a loop. `RunTask` needs no
running worker, and runs the task right away, without waiting for its
backoff:

```go
var handlers taskmill.Handlers
taskmill.AddHandler(&handlers, &ReceiptSender{mailer: &flakyMailer{failures: 2}}, nil) // fails twice, then works
worker, err := taskmill.NewWorker(client, &taskmill.WorkerConfig{Handlers: &handlers})
// ...
res, err := client.Enqueue(t.Context(), SendReceipt{OrderID: 42}, nil)
// ...
info := &taskmill.TaskInfo{}
for i := 0; i < 5 && !info.State.Finished(); i++ {
    if info, err = worker.RunTask(t.Context(), res.ID); err != nil {
        t.Fatal(err)
    }
}
if info.State != taskmill.StateCompleted || info.Attempt != 3 {
    t.Fatalf("got %s after %d attempts", info.State, info.Attempt)
}
```

## Test code that uses `database/sql`

`taskmilltest.NewClient` takes a pgx pool. If your code uses
`database/sql`, open a `*sql.DB` on the same pool, with
`stdlib.OpenDBFromPool(pool)` from `github.com/jackc/pgx/v5/stdlib`.

## Clean up leftover schemas

A test binary that is killed can't drop its schemas. When no tests are
running, list the leftovers, and drop each one with
`DROP SCHEMA ... CASCADE`:

```sql
SELECT nspname FROM pg_namespace WHERE nspname LIKE 'taskmilltest\_%';
```

## See also

- [Handle tasks](handle-tasks.md): what your handler should return
- [Do work once](../recipes/do-work-once.md): handlers that are safe to run twice
- [Package `taskmilltest`](https://pkg.go.dev/github.com/code19m/taskmill/taskmilltest) and [`Worker.RunTask`](https://pkg.go.dev/github.com/code19m/taskmill#Worker.RunTask) on pkg.go.dev

**Next:** [Production checklist](../production/checklist.md)
