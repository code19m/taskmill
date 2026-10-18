# Guide

This guide covers everything taskmill does, with examples. For the reasoning
behind the design, see [docs/design](design); for the API in full, see
[pkg.go.dev](https://pkg.go.dev/github.com/code19m/taskmill).

- [Setting up](#setting-up)
- [Defining tasks](#defining-tasks)
- [Enqueueing](#enqueueing)
- [Handling tasks](#handling-tasks)
- [Running workers](#running-workers)
- [Ordering and deduplication](#ordering-and-deduplication)
- [Schedules](#schedules)
- [The outbox and the inbox](#the-outbox-and-the-inbox)
- [Operating taskmill](#operating-taskmill)
- [Observability](#observability)
- [Running in production](#running-in-production)
- [Architectures](#architectures)

## Setting up

taskmill keeps its tables in one PostgreSQL schema, `taskmill` by default.
Create or upgrade it with `Migrate`, which is safe to call from every process
at startup: one applies the migrations while the others wait.

```go
pool, err := pgxpool.New(ctx, os.Getenv("DATABASE_URL"))
// ...
if err := taskmill.Migrate(ctx, pool, nil); err != nil {
    return err
}
client, err := taskmill.NewClient(ctx, pool, nil)
```

`NewClient` refuses a schema that is missing or older than the code
(`ErrSchemaOutdated`), so a deploy that forgot to migrate fails fast.

Teams that apply migrations with their own tool, such as goose, Atlas, or
Flyway, take the SQL from `taskmill.Migrations(schema)` instead. Each
migration has a version and a name to record in the schema's `migrations`
table.

**Several installations can share a database.** Set `Schema` in the client,
worker, and migration configurations, for example one schema per service or
per module. Installations are fully independent.

## Defining tasks

A task's payload is a struct that implements `Args`. Its kind names the
handler, and it is the contract between the code that enqueues and the code
that handles:

```go
type SendReceipt struct {
    OrderID int64  `json:"order_id"`
    Email   string `json:"email"`
}

func (SendReceipt) Kind() string { return "email.send_receipt" }
```

- Implement `Kind` on a value receiver and return a constant. Renaming a kind
  orphans the tasks already enqueued under the old name.
- Namespace kinds by module, such as `billing.invoice.send`.
- Payloads are stored as JSON and decoded straight into the handler's type,
  so large integers stay exact. Keep payloads small: identifiers, not whole
  records. The default limit is 1 MiB (`ClientConfig.MaxPayloadBytes`).
- Add fields compatibly: tasks enqueued by an older version must still decode
  in the new one.

A kind can set its own defaults by implementing `ArgsWithDefaults`:

```go
func (SendReceipt) EnqueueDefaults() taskmill.EnqueueOptions {
    return taskmill.EnqueueOptions{Queue: "emails", MaxAttempts: 5}
}
```

## Enqueueing

```go
res, err := client.Enqueue(ctx, SendReceipt{OrderID: 42}, nil)
```

Options set in the call win over the kind's defaults, which win over
taskmill's. Every zero value means "not set":

| Option | Meaning | Default |
|---|---|---|
| `Queue` | The queue to enqueue into | `"default"` |
| `Priority` | `PriorityLow`, `PriorityNormal`, `PriorityHigh`, or `PriorityCritical` | `PriorityNormal` |
| `RunAt` | The earliest start | Now |
| `Deadline` | The latest start; afterwards the task becomes dead without running | None |
| `MaxAttempts` | How many times the task may run | 10 |
| `GroupKey` | A FIFO group within the queue | None |
| `DedupeKey` | Makes the enqueue idempotent within a window | None |
| `DedupeWindow` | How long the dedupe key blocks duplicates | 24 hours |
| `Metadata` | String tags passed to the handler | None |

### In your own transaction

This is taskmill's central feature: the task commits or rolls back with your
own writes. Wrap your transaction with the adapter for your driver:

```go
// pgx
err := pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
    if err := saveOrder(ctx, tx, order); err != nil {
        return err
    }
    _, err := client.EnqueueTx(ctx, taskmill.Pgx(tx), SendReceipt{OrderID: order.ID}, nil)
    return err
})

// database/sql, with pgx's driver (github.com/jackc/pgx/v5/stdlib)
_, err = client.EnqueueTx(ctx, taskmill.SQL(tx), SendReceipt{OrderID: order.ID}, nil)

// bun: pass the underlying *sql.Tx, because bun rewrites placeholders itself
_, err = client.EnqueueTx(ctx, taskmill.SQL(bunTx.Tx), SendReceipt{OrderID: order.ID}, nil)
```

sqlx's `*sqlx.Tx` embeds `*sql.Tx`, so `taskmill.SQL(tx)` works directly.

### In batches

`EnqueueMany` and `EnqueueManyTx` send any number of tasks in one round trip,
and return results in the same order. A batch of 100 is roughly 20 times
cheaper per task than 100 single enqueues.

```go
params := make([]taskmill.EnqueueParams, len(users))
for i, u := range users {
    params[i] = taskmill.EnqueueParams{Args: Welcome{UserID: u.ID}}
}
results, err := client.EnqueueMany(ctx, params)
```

## Handling tasks

A handler receives the task with its payload decoded:

```go
type ReceiptSender struct{ mailer Mailer }

func (s *ReceiptSender) Handle(ctx context.Context, t *taskmill.Task[SendReceipt]) error {
    return s.mailer.Send(ctx, t.Args.Email, t.Args.OrderID)
}

var handlers taskmill.Handlers
taskmill.AddHandler(&handlers, &ReceiptSender{mailer: m}, &taskmill.HandlerOptions{Timeout: 30 * time.Second})
```

`taskmill.HandlerFunc[A]` adapts a plain function. `Task` also carries the
task's ID, attempt number, metadata, and the errors of earlier attempts.

**What the handler returns decides what happens next:**

| Handler returns | The task | Uses an attempt |
|---|---|---|
| `nil` | Completes | Yes |
| An error | Is retried after a backoff, or becomes dead when no attempts are left | Yes |
| `taskmill.Permanent(err)` | Becomes dead at once | Yes |
| `taskmill.Snooze(d)` | Runs again after `d` | No |
| A panic | Is recovered and counts as an error, with its stack trace recorded | Yes |
| Too slow | Its context is canceled at the timeout, and the attempt fails | Yes |

Return `Permanent` when retrying cannot help, such as for a payload that
fails validation. Return `Snooze` when the task cannot proceed yet, such as
while waiting for a rate limit.

**Retries** wait 1 second, then double up to 1 hour, with ±10% random jitter
so that failed tasks do not retry in waves. Set `HandlerOptions.Retry` or
`WorkerConfig.DefaultRetry` to change that; `ExponentialBackoff` is the
built-in policy.

**Handlers must be idempotent.** taskmill runs every task at least once, and
a task can run twice: for example, when a worker crashes after the work was
done but before the result was recorded. Pass the task ID to external
services as an idempotency key, or use transactional completion.

**Respect the context.** A canceled context means the task was canceled, its
lease was lost, its timeout passed, or the worker is shutting down. Return
promptly.

### Transactional completion

A handler whose effects live in the same database can complete its task in
its own transaction. The effects and the completion then commit together, so
they happen exactly once even if the task runs again:

```go
func (a *PaymentApplier) Handle(ctx context.Context, t *taskmill.Task[ApplyPayment]) error {
    return pgx.BeginFunc(ctx, a.pool, func(tx pgx.Tx) error {
        if _, err := tx.Exec(ctx, "UPDATE accounts SET balance = balance + $1 WHERE id = $2",
            t.Args.Cents, t.Args.AccountID); err != nil {
            return err
        }
        return t.CompleteTx(ctx, taskmill.Pgx(tx)) // ErrLeaseLost rolls the update back
    })
}
```

`CompleteTx` fails with `ErrLeaseLost` if another attempt took the task over,
which rolls back the handler's writes.

### Batch handlers

For work that is cheaper in bulk, a batch handler receives up to `BatchSize`
tasks of its kind at once and returns one error per task:

```go
func (p *Publisher) HandleBatch(ctx context.Context, tasks []*taskmill.Task[Event]) []error {
    // publish all of them in one call, and report each one's result
}

taskmill.AddBatchHandler[Event](&handlers, p, &taskmill.HandlerOptions{BatchSize: 200})
```

A batch holds the tasks of one fetch, so give a batch kind its own queue.

## Running workers

```go
worker, err := taskmill.NewWorker(client, &taskmill.WorkerConfig{
    Handlers: &handlers,
    Queues: map[string]taskmill.QueueOptions{
        "default": {Concurrency: 20},
        "emails":  {Concurrency: 5},
    },
})
// ...
err = worker.Run(ctx)
```

`Run` blocks until its context is canceled. A worker fetches from each queue
up to its free capacity, in priority, run time, and enqueue order. It wakes
on notifications as soon as tasks are enqueued, and polls every second as a
fallback.

**Shutting down.** When the context is canceled, `Run` stops fetching, waits
up to `DrainTimeout` (30 seconds) for running tasks, cancels the contexts of
those still running, and puts their tasks back in the queue without using an
attempt. Tie the context to `SIGTERM`:

```go
ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
defer stop()
err := worker.Run(ctx)
```

Give the process's termination grace period, such as Kubernetes'
`terminationGracePeriodSeconds`, at least `DrainTimeout` plus a few seconds.

**Leases.** A fetched task belongs to its worker for `LeaseDuration`
(30 seconds), renewed by heartbeats every third of that. If a worker crashes,
its tasks run again once their leases expire. A worker cut off from the
database cancels its running attempts when it cannot renew their leases, and
their late results are rejected.

**The maintainer.** One worker per installation is elected to maintain it:
it makes due tasks available, rescues tasks whose leases expired, expires
deadlines, runs schedules, refreshes statistics, and applies retention. Every
worker takes part in the election unless `NoMaintainer` is set; if the
maintainer dies, another takes over within seconds. A worker with no queues
only maintains.

**Rolling deploys.** A worker that fetches a task whose kind it has no
handler for, such as a kind that only the new version knows, snoozes the
task without using an attempt, for up to `UnknownKindTimeout` (1 hour). Deploy
consumers before producers, and avoid renaming kinds.

**Concurrency.** `QueueOptions.Concurrency` limits one process. To limit a
queue across all workers, for example to protect a rate-limited API, set a
cluster-wide limit:

```go
client.UpdateQueue(ctx, "partner-api", &taskmill.QueueSettings{MaxRunning: 4})
```

## Ordering and deduplication

**Priorities.** Within a queue, higher priorities start first. Tasks start in
priority, run time, and enqueue order, but run concurrently, so they may
finish in any order.

**FIFO groups.** Tasks with the same `GroupKey` in a queue run one at a time,
in enqueue order. A failed head is retried before anything behind it runs.
Groups are created on the fly; millions of keys cost nothing extra.

```go
client.Enqueue(ctx, PostLedgerEntry{...}, &taskmill.EnqueueOptions{GroupKey: "account-7"})
```

When a group's head becomes dead, the group moves on to its next task by
default. For streams where skipping a task would corrupt data, make the
queue halt such groups until an operator retries or deletes the dead task:

```go
client.UpdateQueue(ctx, "ledger", &taskmill.QueueSettings{HaltGroupOnDead: true})
```

**Deduplication.** A `DedupeKey` makes an enqueue idempotent: while its
window lasts, enqueueing the same kind with the same key in the same queue
creates no new task. The result reports the existing task instead of an
error, even when producers race:

```go
res, err := client.Enqueue(ctx, ChargeInvoice{ID: 7}, &taskmill.EnqueueOptions{
    DedupeKey:    "invoice-7",
    DedupeWindow: 7 * 24 * time.Hour,
})
if res.Duplicate {
    // already enqueued as res.ID
}
```

## Schedules

Recurring tasks are defined on the workers, with standard five-field cron
expressions or macros such as `@daily`:

```go
worker, err := taskmill.NewWorker(client, &taskmill.WorkerConfig{
    // ...
    Schedules: []taskmill.Schedule{
        {Name: "nightly-report", Cron: "0 3 * * *", Location: berlin, Args: BuildReport{}},
        {Name: "cleanup", Cron: "@hourly", Args: Cleanup{}},
    },
})
```

- Each run is enqueued exactly once across all workers, even if two
  maintainers briefly overlap.
- Define the same schedules on every worker that can become the maintainer.
- Runs missed while no maintainer runs, or while a schedule is paused,
  collapse into one run.
- Expressions are read on the wall clock of `Location` (UTC by default): a
  time skipped when clocks go forward runs right after the jump, and a time
  repeated when clocks go back runs once.

`Client.Schedules`, `PauseSchedule`, and `ResumeSchedule` list and control
them.

## The outbox and the inbox

**The outbox** publishes messages to a broker with the same guarantee as the
data that produced them. Store messages in the business transaction:

```go
err := pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
    if err := markPaid(ctx, tx, orderID); err != nil {
        return err
    }
    return outbox.Store(ctx, client, taskmill.Pgx(tx), nil, outbox.Message{
        Topic: "orders",
        Key:   strconv.FormatInt(orderID, 10), // messages with one key publish in order
        Value: event,
    })
})
```

Relay them with a worker and a `Publisher` that wraps your broker client:

```go
outbox.Setup(ctx, client, nil) // once: halt keys whose message fails for good

var handlers taskmill.Handlers
outbox.AddRelay(&handlers, outbox.PublisherFunc(func(ctx context.Context, ds []outbox.Delivery) []error {
    errs := make([]error, len(ds))
    for i, d := range ds {
        errs[i] = kafka.Produce(ctx, d.Topic, d.Key, d.Value, d.ID) // one error per delivery
    }
    return errs
}), nil)
// serve outbox.DefaultQueue with a worker
```

Messages with the same topic and key are published one at a time, in the
order they were stored, and a batch never holds two of them. Delivery is at
least once: each `Delivery.ID` is a UUIDv7 that stays the same across
retries, so consumers can deduplicate by it.

**The inbox** needs no package. A consumer enqueues one task per received
message, with the message ID as the dedupe key, and commits its broker offset
afterwards. A redelivered message becomes a duplicate result instead of a
second task:

```go
_, err := client.Enqueue(ctx, HandleOrderEvent{...}, &taskmill.EnqueueOptions{
    DedupeKey:    msg.ID,
    DedupeWindow: 7 * 24 * time.Hour, // longer than the broker's redelivery horizon
})
```

## Operating taskmill

**The web interface** shows queues with their statistics, workers,
schedules, and tasks with their payloads and error histories, and can pause,
resume, retry, cancel, and delete. Mount it behind your own authentication:

```go
mux.Handle("/taskmill/", requireAdmin(ui.Handler(client, &ui.Config{Prefix: "/taskmill"})))
```

`ui.Config.ReadOnly` disables every action. Forms are protected against
cross-site request forgery.

**The client** offers the same operations to your own code and tools:

| Method | Does |
|---|---|
| `Get` | Returns a task, live or from history |
| `List` | Pages through tasks by queue, state, and kind, newest first |
| `Cancel`, `CancelTx` | Cancels a waiting task, or asks a running task's worker to stop it |
| `Retry` | Runs a dead or canceled task again with fresh attempts |
| `Delete` | Deletes a task that is not running |
| `Queues` | Every queue with its settings, workers, and task counts per state |
| `UpdateQueue`, `PauseQueue`, `ResumeQueue` | Changes a queue's cluster-wide settings |
| `Settings`, `UpdateSettings` | Retention of history and of dead and canceled tasks |
| `Workers` | The registered worker processes |

**Retention.** Completed tasks move to a history table and stay there for
7 days; dead and canceled tasks stay for 30 days. Both are installation-wide
settings. A queue whose completed tasks are not worth keeping can skip
history entirely:

```go
client.UpdateQueue(ctx, "notifications", &taskmill.QueueSettings{DiscardCompleted: true})
```

## Observability

**Logs** go to the `*slog.Logger` in `ClientConfig.Logger` and
`WorkerConfig.Logger`. By default they are discarded.

**Middleware** wraps every attempt, including each task of a batch, and sees
its kind, queue, attempt, metadata, and timestamps. It is the place for
metrics and tracing:

```go
func Metrics(ctx context.Context, task *taskmill.TaskInfo, next func(context.Context) error) error {
    start := time.Now()
    err := next(ctx)
    pickup := task.AttemptedAt.Sub(task.RunAt) // how long the task waited
    record(task.Queue, task.Kind, err == nil, time.Since(start), pickup)
    return err
}
```

**Tracing across the queue.** `ClientConfig.ContextMetadata` turns the
producer's context into task metadata on every enqueue, and middleware turns
it back into a context. With OpenTelemetry:

```go
client, err := taskmill.NewClient(ctx, pool, &taskmill.ClientConfig{
    ContextMetadata: func(ctx context.Context) map[string]string {
        carrier := propagation.MapCarrier{}
        otel.GetTextMapPropagator().Inject(ctx, carrier)
        return carrier
    },
})

tracing := func(ctx context.Context, task *taskmill.TaskInfo, next func(context.Context) error) error {
    ctx = otel.GetTextMapPropagator().Extract(ctx, propagation.MapCarrier(task.Metadata))
    ctx, span := tracer.Start(ctx, "taskmill "+task.Kind, trace.WithSpanKind(trace.SpanKindConsumer))
    defer span.End()
    err := next(ctx)
    if err != nil {
        span.RecordError(err)
        span.SetStatus(codes.Error, err.Error())
    }
    return err
}
```

**Queue depth** comes from `Client.Queues`, whose counts the maintainer
refreshes every 10 seconds.

## Running in production

- **Connections.** Running attempts hold no connection while their handlers
  run. A worker needs a few connections of its pool, for fetching each queue,
  heartbeats, recording results, and maintenance, plus one dedicated
  connection for notifications. Size the pool for that plus whatever your
  handlers use.
- **Connection poolers.** Everything works through PgBouncer in transaction
  mode except notifications, which need a session. Point
  `WorkerConfig.ListenConfig` at a direct connection; without it, workers fall
  back to polling.
- **Long transactions** anywhere in the database stop vacuum from cleaning up
  after taskmill's busy tables, and fetching slows down. Set
  `idle_in_transaction_session_timeout`, and keep an eye on old transactions.
- **Autovacuum** is tuned for taskmill's hot table by its migrations. Leave it
  on.
- **Replication.** With asynchronous replication, a failover can lose the
  last committed transactions, including enqueued tasks, as it can lose any
  other row.
- **Clocks.** taskmill takes every time from the database, so application
  servers' clocks do not matter.

## Architectures

**Monolith.** One binary enqueues and works. Run workers in the same process,
or as a separate deployment of the same binary that serves the busy queues.

**Modular monolith.** Each module registers its handlers on the shared
`Handlers` and owns its queues and its namespaced kinds, such as
`billing.invoice.send`. For stronger isolation, give each module its own
schema: installations do not interact.

**Microservices with a database each.** Each service runs taskmill in its
own database. Services never enqueue into each other's databases: one
service publishes events with the outbox, and the other receives them with an
inbox, so every step is transactional.

**A shared database cluster.** Each service gets its own schema in the shared
cluster. A service that only produces tasks for another creates a `Client`
for that service's schema and never registers handlers.
