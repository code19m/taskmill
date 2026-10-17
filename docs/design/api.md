# Public API

- Status: Draft
- Milestone: M1

This document defines the Go API that applications use: how they connect,
define tasks, enqueue them, handle them, run workers, and operate queues. It
builds on the [lifecycle](task-lifecycle.md), the [schema](schema.md), and the
[requirements](scope.md).

The API was written out as a Go sketch, with every exported identifier at its
final signature and stub bodies. The sketch compiles, passes `go vet`, and
passes the project's linters, including the rules for idiomatic APIs. The claims
about drivers were tested against PostgreSQL (see [Verification](#verification)).

## Principles

1. **Concrete types out, small interfaces in.** Constructors return `*Client`
   and `*Worker`; parameters accept small interfaces such as `Handler[A]`.
2. **Context first.** Every call that can block takes a `context.Context`
   first.
3. **No global state.** Everything hangs off a `Client`, so one process can
   run several independent installations (R3).
4. **Useful zero values.** Configuration structs need only the fields that
   differ from the defaults, and a nil configuration pointer means "all
   defaults".
5. **Errors you can test.** Sentinel errors for `errors.Is`, error types for
   `errors.As`. No panics on bad input; registration mistakes are reported by
   `NewWorker`.
6. **Typed payloads.** A handler receives its payload already decoded into its
   own struct type, through generics.
7. **Only what users need is exported.** The core package exports 90
   top-level identifiers: 39 types, 24 methods, 12 constants, 10 functions, and
   5 variables. Everything else stays unexported, so it can change without
   breaking users.

## Quick start

```go
// SendReceipt is the payload of a task. Its kind connects producers and consumers.
type SendReceipt struct {
    OrderID int64  `json:"order_id"`
    Email   string `json:"email"`
}

// Kind implements taskmill.Args.
func (SendReceipt) Kind() string { return "email.send_receipt" }

// ReceiptSender handles SendReceipt tasks.
type ReceiptSender struct{ mailer Mailer }

// Handle implements taskmill.Handler.
func (s *ReceiptSender) Handle(ctx context.Context, t *taskmill.Task[SendReceipt]) error {
    return s.mailer.Send(ctx, t.Args.Email, t.Args.OrderID)
}

// placeOrder is the producer: the order and its receipt task commit together,
// or not at all.
func placeOrder(ctx context.Context, pool *pgxpool.Pool, client *taskmill.Client, email string) error {
    return pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
        var orderID int64
        err := tx.QueryRow(ctx, "INSERT INTO orders (email) VALUES ($1) RETURNING id", email).Scan(&orderID)
        if err != nil {
            return err
        }
        _, err = client.EnqueueTx(ctx, taskmill.Pgx(tx), SendReceipt{OrderID: orderID, Email: email}, nil)
        return err
    })
}

// runWorker is the consumer: it runs handlers until the process is told to stop.
func runWorker(ctx context.Context, client *taskmill.Client, mailer Mailer) error {
    var handlers taskmill.Handlers
    taskmill.AddHandler(&handlers, &ReceiptSender{mailer: mailer}, nil)

    worker, err := taskmill.NewWorker(client, &taskmill.WorkerConfig{
        Handlers: &handlers,
        Queues:   map[string]taskmill.QueueOptions{"default": {Concurrency: 20}},
    })
    if err != nil {
        return err
    }

    ctx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
    defer stop()
    return worker.Run(ctx) // on SIGTERM: stops fetching, drains running tasks, returns
}

func run(ctx context.Context, pool *pgxpool.Pool, mailer Mailer) error {
    // Create or upgrade the schema. Safe to run from every process.
    if err := taskmill.Migrate(ctx, pool, nil); err != nil {
        return err
    }
    client, err := taskmill.NewClient(ctx, pool, nil)
    if err != nil {
        return err
    }
    if err := placeOrder(ctx, pool, client, "a@example.com"); err != nil {
        return err
    }
    return runWorker(ctx, client, mailer)
}
```

## Packages

| Package | Contents | Milestone |
|---|---|---|
| `taskmill` | Client, worker, handlers, migrations, inspect and admin API | M2, M3 |
| `taskmill/outbox` | Transactional outbox: `Store`, `Publisher`, `AddRelay` | M4 |
| `taskmill/ui` | Web interface as an `http.Handler` | M6 |
| `taskmill/cmd/taskmill` | Command-line tool and standalone web interface | M6 |
| `github.com/code19m/taskmill/otel` | OpenTelemetry metrics and tracing; a separate module | M6 |

## Connecting to the database

**The engine runs on a pgx pool.** `NewClient` takes a `*pgxpool.Pool` and
uses it for its own statements. Workers also hold one dedicated connection for
`LISTEN`. Why pgx: [ADR 0004](../adr/0004-pgx-as-the-postgresql-driver.md).

```go
func NewClient(ctx context.Context, pool *pgxpool.Pool, cfg *ClientConfig) (*Client, error)
```

`NewClient` checks the schema version and fails fast with `ErrSchemaOutdated`
if the database was not migrated.

**Applications' own transactions go through an `Executor`.** Methods that end
in `Tx` run their statements on a transaction the application owns, so they
commit or roll back with it (R12). An `Executor` comes from one of two
adapters:

```go
func Pgx(q PgxQuerier) PgxExecutor // pgx.Tx, *pgx.Conn, *pgxpool.Pool
func SQL(q SQLQuerier) SQLExecutor // *sql.Tx, *sql.Conn, *sql.DB
```

| Application uses | Pass |
|---|---|
| pgx | `taskmill.Pgx(tx)` |
| `database/sql` with pgx's driver | `taskmill.SQL(tx)` |
| sqlx | `taskmill.SQL(tx)`: `*sqlx.Tx` embeds `*sql.Tx` |
| bun | `taskmill.SQL(tx.Tx)`: bun rewrites placeholders, so it needs the underlying `*sql.Tx` |

`database/sql` connections must use pgx's driver, `pgx/v5/stdlib`; lib/pq is
not supported.

**Why adapters.** `pgx.Tx` and `*sql.Tx` have no method in common, so no
single interface can accept both. The alternative, a client that is generic
over the transaction type (`Client[TTx]`), spreads a type parameter through
every type that touches a transaction, including the tasks given to handlers.
The adapters cost one function call where a transaction is passed, and keep
every other type simple. `Executor` is sealed: only taskmill implements it, so
its methods can change without breaking anyone.

## Defining tasks

A task's payload is a struct that implements `Args`. Its kind names the
handler and is the contract between producers and consumers (R2).

```go
type Args interface {
    Kind() string
}

type ArgsWithDefaults interface {
    Args
    EnqueueDefaults() EnqueueOptions
}
```

- Implement `Kind` on a value receiver and return a constant. Changing a kind
  orphans the tasks already enqueued under the old name.
- Namespace kinds by module, such as `billing.invoice.send` (U2).
- Payloads are stored as JSON with `encoding/json` and decoded straight into
  the handler's type, so large integers stay exact.
- `ArgsWithDefaults` sets per-kind defaults, such as a queue or a priority, so
  call sites stay short.

## Enqueueing

```go
func (c *Client) Enqueue(ctx context.Context, args Args, opts *EnqueueOptions) (EnqueueResult, error)
func (c *Client) EnqueueTx(ctx context.Context, tx Executor, args Args, opts *EnqueueOptions) (EnqueueResult, error)
func (c *Client) EnqueueMany(ctx context.Context, params []EnqueueParams) ([]EnqueueResult, error)
func (c *Client) EnqueueManyTx(ctx context.Context, tx Executor, params []EnqueueParams) ([]EnqueueResult, error)
```

`EnqueueMany` sends any number of tasks in one round trip. `opts` may be nil.
A field set in the call wins over the kind's defaults, which win over
taskmill's defaults. Every field's zero value means "not set", so no option
needs a pointer:

| Option | Meaning | Default | Requirement |
|---|---|---|---|
| `Queue` | Queue to enqueue into | `"default"` | R4 |
| `Priority` | `PriorityLow`, `PriorityNormal`, `PriorityHigh`, or `PriorityCritical` | `PriorityNormal` | |
| `RunAt` | Earliest start | Now | |
| `Deadline` | Latest start; afterwards the task becomes dead without running | None | |
| `MaxAttempts` | How many times the task may run | 10 | |
| `GroupKey` | FIFO group within the queue | None | R6 |
| `DedupeKey` | Makes the enqueue idempotent within a window | None | R9 |
| `DedupeWindow` | How long the key blocks duplicates | 24 hours | R9 |
| `Metadata` | String tags passed to the handler, such as a trace context | None | |

```go
type EnqueueResult struct {
    ID        int64 // the new task, or the existing one for a duplicate
    Duplicate bool  // the DedupeKey was taken, so no task was created
}
```

A duplicate is a result, not an error: an inbox that receives a message twice
should carry on, not fail (U3). Invalid options fail with `ErrInvalidArgument`
and enqueue nothing.

## Handling tasks

```go
type Handler[A Args] interface {
    Handle(ctx context.Context, task *Task[A]) error
}

type Task[A Args] struct {
    TaskInfo // ID, Queue, Kind, Attempt, MaxAttempts, Errors, Metadata, timestamps
    Args A
}
```

Handlers are registered into a `Handlers` set, which is then given to the
worker:

```go
var handlers taskmill.Handlers
taskmill.AddHandler(&handlers, &ReceiptSender{}, nil)
taskmill.AddHandler(&handlers, taskmill.HandlerFunc[Cleanup](cleanup), &taskmill.HandlerOptions{Timeout: 10 * time.Minute})
```

`AddHandler` is a function rather than a method because Go methods cannot have
type parameters. A modular monolith lets each module add its own handlers to
the set (U2). A kind registered twice is reported by `NewWorker`.

**What a handler returns decides what happens next:**

| Handler returns | The task | Uses an attempt |
|---|---|---|
| `nil` | Completes | Yes |
| An error | Is retried after a backoff, or becomes dead if no attempts are left | Yes |
| `taskmill.Permanent(err)` | Becomes dead at once | Yes |
| `taskmill.Snooze(d)` | Runs again after `d` | No |
| A panic | Is recovered and treated as an error, with the stack trace recorded | Yes |
| After its timeout | Its context is canceled; the attempt fails as an error | Yes |

**Options per kind:**

| Option | Meaning | Default |
|---|---|---|
| `Timeout` | Bound on each attempt | 1 minute |
| `Retry` | A `RetryPolicy` that returns the delay before each retry | The worker's `DefaultRetry` |
| `BatchSize` | Most tasks per call of a batch handler | 100 |

The default retry policy is `ExponentialBackoff`: 1 second, doubling up to
1 hour, with ±10% random jitter.

**Transactional completion (opt-in).** A handler whose effects live in the same
database completes its task in its own transaction, so the effects happen
exactly once (guarantee 5):

```go
// Handle applies the payment and completes the task in one transaction.
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

**Batch handlers (M4).** For work that is cheaper in bulk, such as publishing
to a broker (R7). The handler returns one error per task, in order:

```go
type BatchHandler[A Args] interface {
    HandleBatch(ctx context.Context, tasks []*Task[A]) []error
}

func AddBatchHandler[A Args](hs *Handlers, h BatchHandler[A], opts *HandlerOptions)
```

## Running workers

```go
func NewWorker(client *Client, cfg *WorkerConfig) (*Worker, error)
func (w *Worker) Run(ctx context.Context) error
```

`Run` blocks until its context is canceled, the same way `http.Server` serves
until it is shut down. A worker:

1. Registers itself in `workers`, opens its `LISTEN` connection, and joins the
   maintainer election, unless `NoMaintainer` is set.
2. Fetches from each of its queues up to that queue's free capacity, when
   notified and at least once per `PollInterval`.
3. Runs each attempt through the middleware and the handler, with heartbeats
   every third of `LeaseDuration`.
4. When its context is canceled: stops fetching, waits up to `DrainTimeout`
   for running attempts, then cancels their contexts and releases their tasks
   without using attempts (lifecycle transition 12), and returns `nil`.

| Field | Meaning | Default |
|---|---|---|
| `Handlers` | The handler set | Required |
| `Queues` | Queues to serve, with this process's `Concurrency` for each | Required |
| `ID` | Worker identity in the database and in logs | Hostname, process ID, random suffix |
| `LeaseDuration` | How long a task stays owned without a heartbeat | 30 seconds |
| `PollInterval` | Fallback polling when no notification arrives | 1 second |
| `DrainTimeout` | Graceful shutdown limit | 30 seconds |
| `UnknownKindTimeout` | How long a task with no handler waits for one (R5) | 1 hour |
| `DefaultRetry` | Retry policy for kinds that set none | `ExponentialBackoff{}` |
| `Middleware` | Wrappers around every attempt | None |
| `Schedules` | Recurring tasks (R14, M5) | None |
| `NoMaintainer` | Keeps this process out of the maintainer election | `false` |
| `Logger` | Where logs go | The client's logger |

**Per process or per cluster.** `QueueOptions` in `WorkerConfig` are local to
one process: how many tasks it runs at once. `QueueSettings`, set through the
client and stored in the database, apply to every worker: a concurrency limit,
whether completed tasks are kept, and whether FIFO groups halt (R8, R10, R6).
Keeping the two apart means two deployments can never fight over a
cluster-wide setting.

**Middleware** wraps each attempt, for logging, metrics, tracing, or tenancy:

```go
type Middleware func(ctx context.Context, task *TaskInfo, next func(context.Context) error) error
```

## Managing tasks, queues, and the installation

All on `Client`, so that the web interface, the command-line tool, and
applications share one API (R16):

| Method | Does | Milestone |
|---|---|---|
| `Get(ctx, id)` | Returns a task from `tasks` or from history | M2 |
| `List(ctx, *ListParams)` | Pages through tasks filtered by queue, states, and kind, with a cursor | M6 |
| `Cancel(ctx, id)`, `CancelTx` | Cancels a waiting task at once, or a running task when its attempt ends | M2 |
| `Retry(ctx, id)` | Makes a dead or canceled task available, with fresh attempts | M2 |
| `Delete(ctx, id)` | Deletes a task that is not running | M2 |
| `Queues(ctx)` | Every queue with its settings and statistics | M6 |
| `UpdateQueue(ctx, name, *QueueSettings)` | Changes a queue's cluster-wide settings | M4 |
| `PauseQueue(ctx, name)`, `ResumeQueue` | Stops or resumes fetching from a queue (R17) | M6 |
| `Settings(ctx)`, `UpdateSettings` | History and dead-task retention for the installation | M5 |
| `Workers(ctx)` | Live worker processes (R15) | M6 |

## Schema migrations

```go
func Migrate(ctx context.Context, pool *pgxpool.Pool, cfg *MigrateConfig) error
func Migrations(schema string) ([]Migration, error)
```

`Migrate` applies pending migrations under an advisory lock, so every process
can call it at startup and only one does the work. Teams that apply migrations
with their own tools, such as goose or Atlas, take the SQL from `Migrations`
instead. Clients never migrate implicitly: they check the version and fail
fast.

## Errors

| Error | Meaning |
|---|---|
| `ErrNotFound` | The task does not exist, or no longer exists |
| `ErrLeaseLost` | The attempt no longer owns its task; roll back |
| `ErrInvalidState` | The operation does not apply to the task's state |
| `ErrInvalidArgument` | An argument or option is out of range |
| `ErrSchemaOutdated` | The schema is missing or older than this version; run `Migrate` |
| `*PermanentError` | Returned by `Permanent`; the task becomes dead without retrying |
| `*SnoozeError` | Returned by `Snooze`; the task runs again later |

Every error message starts with `taskmill:`, and wrapped errors keep their
cause for `errors.Is` and `errors.As`.

## Logging and observability

- **Logging** uses `log/slog`. The default logger discards everything; a
  library must not write to stdout unless asked.
- **Middleware** covers everything that happens around an attempt.
- **Metrics and tracing** come from the separate
  `github.com/code19m/taskmill/otel` module (M6), so applications that do not
  use OpenTelemetry never download it. It builds on middleware and on
  lifecycle hooks that M6 will design.

## Outbox (M4)

```go
func Store(ctx context.Context, client *taskmill.Client, tx taskmill.Executor, opts *Options, msgs ...Message) error
func AddRelay(hs *taskmill.Handlers, pub Publisher, opts *Options)

type Publisher interface {
    Publish(ctx context.Context, deliveries []Delivery) []error
}
```

- `Store` saves messages in the outbox inside the business transaction.
- `AddRelay` registers a batch handler that publishes them through the
  application's own `Publisher`: Kafka, NATS, RabbitMQ, or anything else.
- Messages with the same topic and key are published in order, through a FIFO
  group. A batch never holds two of them, so per-delivery errors cannot break
  that order.
- Each `Delivery` has an ID that is unique across services and stays the same
  on retries (a UUIDv7), so consumers can deduplicate by it.

**The inbox needs no package.** A consumer enqueues each received message with
`DedupeKey` set to the message ID, and a redelivered message becomes a
duplicate result instead of a second task.

## Web interface (M6)

```go
func Handler(client *taskmill.Client, cfg *Config) http.Handler // package taskmill/ui
```

The application mounts the handler on its own router behind its own
authentication middleware (see [scope](scope.md#the-web-interface)).

## Defaults

| Setting | Default |
|---|---|
| Queue | `"default"` |
| Priority | `PriorityNormal` |
| Max attempts | 10 |
| Retry backoff | 1 s doubling to 1 h, ±10% jitter |
| Attempt timeout | 1 minute |
| Lease duration | 30 seconds, renewed every 10 seconds |
| Poll interval | 1 second |
| Drain timeout | 30 seconds |
| Unknown kind timeout | 1 hour |
| Concurrency per queue and process | 10 |
| Batch size | 100 |
| Dedupe window | 24 hours |
| Maximum payload | 1 MiB |
| History retention | 7 days |
| Dead and canceled task retention | 30 days |

## Compatibility

- taskmill follows Semantic Versioning. Before v1.0.0, a minor release may
  break the API; release notes say how to migrate.
- **Interfaces that users implement never grow.** Adding a method to `Args`,
  `Handler`, `BatchHandler`, `RetryPolicy`, or `Publisher` would break every
  implementation. New capabilities arrive as new optional interfaces, the way
  `ArgsWithDefaults` extends `Args`.
- **Configuration structs grow only with fields whose zero value keeps the old
  behavior.**
- **`Executor` is sealed,** so its methods can change freely.

## Requirements

| Requirement | API |
|---|---|
| R1 Configurable schema | `ClientConfig.Schema`, `MigrateConfig.Schema` |
| R2 Independent producers and consumers | `Client` needs no handlers; `Args.Kind` is the contract |
| R3 No global state | Everything hangs off `Client`; several per process |
| R4 Queues route work | `EnqueueOptions.Queue`, `WorkerConfig.Queues` |
| R5 Unknown kinds | `WorkerConfig.UnknownKindTimeout` |
| R6 FIFO groups | `EnqueueOptions.GroupKey`, `QueueSettings.HaltGroupOnDead` |
| R7 Batch handlers | `BatchHandler`, `AddBatchHandler`, `HandlerOptions.BatchSize` |
| R8 Concurrency limits | `QueueSettings.MaxRunning` |
| R9 Deduplicated enqueue | `EnqueueOptions.DedupeKey`, `DedupeWindow`, `EnqueueResult.Duplicate` |
| R10 Completion policy | `QueueSettings.DiscardCompleted`, `Settings.HistoryRetention` |
| R11 Stable identity | `TaskInfo.ID`, `EnqueueResult.ID`, `outbox.Delivery.ID` |
| R12 Caller's transactions | `Executor`, the `...Tx` methods, `Task.CompleteTx` |
| R13 Error history | `TaskInfo.Errors` |
| R14 Recurring schedules | `WorkerConfig.Schedules` |
| R15 Cheap monitoring | `Queues`, `Workers` |
| R16 Inspect API | `Get`, `List`, `Cancel`, `Retry`, `Delete` |
| R17 Pausing | `PauseQueue`, `ResumeQueue` |

## Verification

- **The sketch compiles and passes `go vet` and every project linter.** That
  covers, among other rules, no interfaces returned from constructors, named
  parameters in interfaces, a doc comment on every exported identifier, and
  `slog` usage. Only stub artifacts remained, such as unused receivers.
- **The code examples in this document compile, verbatim,** and pass the same
  linters. A further usage package covers `database/sql`, snoozing,
  middleware, and the outbox.
- **Driver claims were tested against PostgreSQL 18.** The batch statement's
  text, smallint, nullable timestamp, and JSON arrays work through `pgx.Tx` and
  through `*sql.Tx` with pgx's driver, and a 19-digit integer in a payload
  stayed exact. A `bun.Tx` given `$1` placeholders failed with "expected 1
  arguments, got 0", while its underlying `tx.Tx` worked.

Two fixes came out of this review and were applied to the repository:

- The `godot` linter rejected idiomatic doc comments such as "opts may be
  nil.", so its capital-letter rule is off.
- The states use the US spelling `canceled`, matching `context.Canceled` and
  the project's US English spell check.

The review also found that the installation-wide retention decided in the
schema document had nowhere to live, so the schema gained a single-row
`settings` table.

## Decisions

1. **Separate `Client` and `Worker`.** Producers create only a client and never
   load handlers or open a `LISTEN` connection (R2).
2. **Adapters for transactions instead of a generic client.** See
   [Connecting to the database](#connecting-to-the-database).
3. **A duplicate enqueue is a result, not an error,** so inboxes and
   schedulers handle it without error checks.
4. **No public SQL enqueue function in v1.** It would freeze part of the schema
   as a public interface. The schema keeps it possible, and it can be added
   once users need it (scope open question 1).
