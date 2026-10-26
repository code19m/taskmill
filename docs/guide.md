# Guide

This guide shows how to use every part of taskmill. For the full API, see
[pkg.go.dev](https://pkg.go.dev/github.com/code19m/taskmill). For the
promises taskmill makes, see the [task lifecycle](design/task-lifecycle.md).

- [Concepts](#concepts)
- [Setting up](#setting-up)
- [Defining tasks](#defining-tasks)
- [Enqueueing](#enqueueing)
- [Handling tasks](#handling-tasks)
- [Running workers](#running-workers)
- [Ordering and deduplication](#ordering-and-deduplication)
- [Schedules](#schedules)
- [The outbox and the inbox](#the-outbox-and-the-inbox)
- [Operating taskmill](#operating-taskmill)
- [Several services](#several-services)
- [Observability](#observability)
- [Testing your code](#testing-your-code)
- [Running in production](#running-in-production)
- [Running on Kubernetes](#running-on-kubernetes)
- [Upgrading](#upgrading)
- [Defaults and limits](#defaults-and-limits)

## Concepts

| Term | Meaning |
|---|---|
| Task | One unit of work: a kind, a payload, and options such as a queue. |
| Kind | The name that selects a task's handler, such as `email.send_receipt`. |
| Payload | A task's data, stored as JSON. In Go, it is the task's `Args` value. |
| Queue | A named list of tasks. Each worker chooses the queues it serves. |
| Attempt | One run of a task's handler. Each task has a maximum number of attempts. |
| Lease | An attempt's time-limited ownership of its task. Only the attempt that holds the lease can record a result. |
| Heartbeat | The worker's renewal of its leases, every third of `LeaseDuration`. |
| Backoff | The wait before a failed task is retried. |
| Snooze | Run a task again later, without using an attempt. |
| Release | Give a running task back to its queue at shutdown, without using an attempt. |
| Dead | Will not run again unless an operator retries it. Dead tasks are kept for inspection: they are the dead-letter queue. |
| Finished | Completed, dead, or canceled. All other states are unfinished. |
| FIFO group | The tasks of one queue that share a `GroupKey`. They run one at a time, in enqueue order. |
| Head | A group's oldest unfinished task: the only one of the group that can run. |
| Halt | A group stops behind its dead head until an operator retries or deletes it. |
| Slot | One of a worker's `Concurrency` places for a queue. An attempt, or a whole batch, takes one slot. |
| Dedupe key | A key that admits at most one task per queue and kind within a time window. |
| Maintainer | The one elected worker that does the upkeep of its installation, such as rescuing expired leases and running schedules. |
| Installation | One taskmill schema, with the clients and workers that use it. Installations never see each other's tasks. |

A task is always in one of eight states: `scheduled`, `pending`, `available`,
`running`, `retryable`, `completed`, `dead`, or `canceled`. The
[task lifecycle](design/task-lifecycle.md#states) defines each state, every
transition between them, and the guarantees taskmill makes.

## Setting up

taskmill keeps its tables in one PostgreSQL schema, `taskmill` by default.
`Migrate` creates or upgrades it. It is safe to call from every process at
startup: one process applies the migrations while the others wait.

```go
pool, err := pgxpool.New(ctx, os.Getenv("DATABASE_URL"))
// ...
if err := taskmill.Migrate(ctx, pool, nil); err != nil {
    return err
}
client, err := taskmill.NewClient(ctx, pool, nil)
```

`NewClient` fails fast if the schema is missing or older than the code
(`ErrSchemaOutdated`). It also fails if the pool does not use the READ
COMMITTED isolation level (see [Running in production](#running-in-production)).

**Your own migration tool.** Teams that use goose, Atlas, or Flyway take the
SQL from `taskmill.Migrations(schema)` instead. Apply the migrations in order.
The first one, version 0, creates the schema and its `migrations` table.
Record the version and name of every later one in that table.

**Several installations can share a database.** Set `Schema` in
`ClientConfig` and `MigrateConfig`, for example one schema per service or
per module. A worker uses its client's schema. A service that only enqueues
tasks for another service creates a `Client` for that service's schema and
registers no handlers. For services with a database each, and for one admin
application over several services, see [Several services](#several-services).

## Defining tasks

A task's payload is a struct that implements `Args`. Its kind names the
handler. The kind is the contract between the code that enqueues and the
code that handles:

```go
type SendReceipt struct {
    OrderID int64  `json:"order_id"`
    Email   string `json:"email"`
}

func (SendReceipt) Kind() string { return "email.send_receipt" }
```

- Implement `Kind` on a value receiver and return a constant. If you rename
  a kind, tasks already enqueued under the old name find no handler, and
  become dead after 1 hour (see [Running in production](#running-in-production)).
- Namespace kinds by module, such as `billing.invoice.send`.
- Payloads are stored as JSON and decoded straight into the handler's type,
  so large integers stay exact. Keep payloads small: identifiers, not whole
  records. The limit is 1 MiB of JSON, counting each number at the length
  PostgreSQL writes it out: it stores `1e6` as `1000000`.
- Change payloads only in compatible ways, such as adding fields. A payload
  that does not decode makes the task dead at once, with the code
  `invalid_payload`.
- PostgreSQL cannot store the NUL character (`\u0000`) in text or JSON. A
  payload, metadata, queue name, kind, or group key that holds one fails
  with `ErrInvalidArgument`. So does a payload that is not valid UTF-8, such
  as a `json.RawMessage` with a request body in another encoding, or that
  holds a number beyond the range of PostgreSQL's `numeric` type. These
  checks run before the query, so your transaction stays usable. A payload
  that PostgreSQL itself rejects, such as a string with an unpaired
  surrogate escape (`"\ud800"`), fails with `ErrInvalidArgument` too, but
  aborts the transaction it was enqueued in.

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
| `MaxAttempts` | How many times the task may run, from 1 to 10,000 | 25 |
| `GroupKey` | A FIFO group within the queue | None |
| `DedupeKey` | Admits at most one task with this key within a window | None |
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

`database/sql` code must use pgx's driver; lib/pq is not supported.
`EnqueueTx` and `EnqueueManyTx` accept a transaction, a connection, or a
pool. `CancelTx` and `Task.CompleteTx` need a transaction, because they run
several statements that must commit together. Pass a `pgx.Tx`, a `*sql.Tx`,
or a type with `Commit` and `Rollback` methods, such as `*sqlx.Tx`; anything
else returns `ErrInvalidArgument`.

### In batches

`EnqueueMany` and `EnqueueManyTx` send any number of tasks in one round trip,
and return results in the same order. In the
[benchmarks](benchmarks.md), batches of 100 enqueue more than 20 times more
tasks per second than single enqueues.

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
fails validation. `Permanent(nil)` returns nil, so
`return taskmill.Permanent(validate(t.Args))` is safe. Return `Snooze` when
the task cannot proceed yet, such as while waiting for a rate limit. A snooze
of zero or less makes the task available at once. Snoozes use no attempts, so
a task that always snoozes never becomes dead.

**Retries** wait 1 second, then double up to 1 hour, with ±10% random jitter
so that failed tasks do not retry in waves. With the default 25 attempts, a
failing task keeps retrying for about 13 hours before it becomes dead. A
retry starts within about a second of its time, so shorter delays round up to
about a second.

**Options per kind**, in `HandlerOptions`:

| Option | Meaning | Default |
|---|---|---|
| `Timeout` | Bound on each attempt, or each batch | 1 minute |
| `Retry` | A `RetryPolicy` that returns the delay before each retry | `ExponentialBackoff{}` |
| `BatchSize` | Most tasks per call of a batch handler, up to 1,000 | 100 |
| `DiscardCompleted` | Delete completed tasks at once, instead of keeping them in history | `false` |

**Handlers must be idempotent:** running a task twice must have the same
effect as running it once. taskmill runs every task at least once, and a
task can run twice: for example, when a worker crashes after the work was
done but before the result was recorded. Send external services an
idempotency key made from the task ID and a namespace, such as your service's
name and taskmill's schema: a task ID is unique only within its schema. Or
use transactional completion.

**Respect the context.** A canceled context means the task was canceled, its
lease was lost, its timeout passed, or the worker is shutting down. Return
promptly.

### Transactional completion

A handler whose effects live in the same database can complete its task in
its own transaction. The effects and the completion then commit together, so
they happen exactly once, even if the task runs again:

```go
func (a *PaymentApplier) Handle(ctx context.Context, t *taskmill.Task[ApplyPayment]) error {
    return pgx.BeginFunc(ctx, a.pool, func(tx pgx.Tx) error {
        if _, err := tx.Exec(ctx, "UPDATE accounts SET balance = balance + $1 WHERE id = $2",
            t.Args.Cents, t.Args.AccountID); err != nil {
            return err
        }
        return t.CompleteTx(ctx, taskmill.Pgx(tx)) // an error rolls the update back
    })
}
```

`CompleteTx` returns `ErrLeaseLost` if another attempt took the task over.
Return that error, so that your transaction rolls back. If the transaction
fails for another reason, return the error: the attempt then fails as usual.
Once `CompleteTx` succeeds, the worker never cancels the attempt as lost,
even while your transaction is still open.

### Batch handlers

For work that is cheaper in bulk, a batch handler receives up to `BatchSize`
tasks of its kind at once and returns one error per task:

```go
func (p *Publisher) HandleBatch(ctx context.Context, tasks []*taskmill.Task[Event]) []error {
    // publish all of them in one call, and report each one's result
}

taskmill.AddBatchHandler[Event](&handlers, p, &taskmill.HandlerOptions{BatchSize: 200})
```

A batch uses one slot. Batch kinds can share a queue; give one its own queue
if it should not compete with other work for slots.

Canceling a task of a batch, or losing its lease, does not cancel the
batch's context. The batch runs to the end; then that task becomes canceled,
or its result is discarded.

## Running workers

```go
worker, err := taskmill.NewWorker(client, &taskmill.WorkerConfig{
    Handlers: &handlers,
    Queues:   map[string]taskmill.QueueOptions{"emails": {Concurrency: 5}}, // optional
})
// ...
err = worker.Run(ctx)
```

Without `Queues`, a worker serves the default queue of each kind it handles:
the queue from the kind's `EnqueueDefaults`, or else `"default"`, with 10
slots each. If `Queues` leaves out a handled kind's default queue, `Run`
logs a warning when it starts. A worker with no handlers and no queues only
takes part in maintenance.

`Run` blocks until its context is canceled. It returns an error only if the
worker cannot start. A worker fetches from each queue up to its free slots,
in priority, run time, and enqueue order. PostgreSQL notifications wake it as
soon as tasks are enqueued, and it polls every second as a fallback.

**Shutting down.** When the context is canceled, `Run` stops fetching and
waits up to `DrainTimeout` (20 seconds) for running tasks. Then it cancels
the contexts of the tasks still running, gives their handlers 5 seconds to
return, and releases them. Tie the context to `SIGTERM` with
`signal.NotifyContext`, as the [README](../README.md#quick-start) does.

**Leases.** A fetched task belongs to its attempt for `LeaseDuration`
(30 seconds), and heartbeats renew it every 10 seconds. If a worker crashes,
its tasks run again once their leases expire. A worker cut off from the
database cancels its attempts when it cannot renew their leases, and their
late results are rejected. A worker retries recording a result until the
lease would expire, so a short database outage or failover does not make
finished tasks run again.

**The maintainer.** One worker per installation is elected to do its upkeep:
it makes due tasks available, rescues tasks whose leases expired, runs
schedules, refreshes statistics, and deletes old tasks. Every worker takes
part in the election unless `NoMaintainer` is set; at least one must take
part. If the maintainer stops, another worker takes over within about
12 seconds. `WorkerInfo.Maintainer` shows which worker leads.

**Installation-wide concurrency limits.** `QueueOptions.Concurrency` limits
one process. To limit a queue across all workers, for example to protect a
rate-limited API, set `MaxRunning`:

```go
err := client.UpdateQueue(ctx, "partner-api", &taskmill.QueueSettings{MaxRunning: 4})
```

`UpdateQueue` replaces the queue's settings: a zero `MaxRunning`, or nil
settings, removes the limit. Workers apply the change within 10 seconds.

## Ordering and deduplication

**Priorities.** Within a queue, higher priorities start first. Tasks start in
priority, run time, and enqueue order, but run concurrently, so they may
finish in any order.

**FIFO groups.** Tasks with the same `GroupKey` in a queue run one at a time,
in enqueue order. Only the group's head, its oldest unfinished task, can run.
A failed head is retried before anything behind it runs.

```go
_, err := client.Enqueue(ctx, PostLedgerEntry{...}, &taskmill.EnqueueOptions{GroupKey: "account-7"})
```

- Groups need no setup, and millions of keys work well. Each group with
  unfinished tasks keeps one small row, and a grouped task costs one or two
  extra row writes.
- If the head becomes dead, the group halts: nothing behind it runs until an
  operator retries or deletes the dead task. Skipping it would break the
  order you asked for. A canceled head does not halt its group.
- Producers that enqueue into the same group wait for each other's
  transactions, so keep those transactions short.
- `ListParams.GroupKeys` lists the tasks of chosen groups, and
  `Client.Groups` finds the groups that have unfinished tasks and those that
  are halted. The web interface links each task to its group.

**Deduplication.** A `DedupeKey` admits at most one task: while its window
lasts, enqueueing the same kind with the same key in the same queue creates
no new task. The result reports the existing task instead of an error, even
when producers race:

```go
res, err := client.Enqueue(ctx, ChargeInvoice{ID: 7}, &taskmill.EnqueueOptions{
    DedupeKey:    "invoice-7",
    DedupeWindow: 7 * 24 * time.Hour,
})
if res.Duplicate {
    // already enqueued as res.ID
}
```

`Cancel` and `Delete` release the task's key, so the same key can be
enqueued again at once. A dead task keeps its key until the window ends.

## Schedules

Recurring tasks are defined on the workers, with five-field cron expressions
or the macros `@yearly`, `@annually`, `@monthly`, `@weekly`, `@daily`,
`@midnight`, and `@hourly`. `@every` is not supported.

```go
worker, err := taskmill.NewWorker(client, &taskmill.WorkerConfig{
    // ...
    Schedules: []taskmill.Schedule{
        {Name: "nightly-report", Cron: "0 3 * * *", Location: berlin, Args: BuildReport{}},
        {Name: "cleanup", Cron: "@hourly", Args: Cleanup{}},
    },
})
```

- Each scheduled time is enqueued at most once across all workers. Define
  the same schedules on every worker that can become the maintainer.
- A scheduled task's `RunAt` is the time it was scheduled for, so its
  handler knows which period it runs for.
- Runs missed while no maintainer runs, or while a schedule is paused,
  collapse into one run.
- `NewWorker` rejects a schedule whose expression never matches, or whose
  task options are invalid.
- Expressions are read on the wall clock of `Location` (UTC by default). A
  time skipped when clocks go forward runs right after the jump. A time
  repeated when clocks go back runs once.
- A maintainer that defines schedules deletes a stored schedule that it does
  not define and that did not run or change for 24 hours, such as one
  removed from the code.

`Client.Schedules`, `PauseSchedule`, and `ResumeSchedule` list and control
schedules. `RunSchedule` runs a schedule once now, for example after a fix,
in addition to its timetable: the maintainer enqueues the run within about a
second, with the schedule's options, and the next regular run stays as it
was. A paused schedule must be resumed first.

## The outbox and the inbox

**The outbox** publishes messages to a broker with the same guarantee as the
data that produced them. Create one `Outbox`, share it, and store messages in
the business transaction:

```go
ob := outbox.New(client)

err := pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
    if err := markPaid(ctx, tx, orderID); err != nil {
        return err
    }
    return ob.Store(ctx, taskmill.Pgx(tx), outbox.Message{
        Topic: "orders",
        Key:   []byte(strconv.FormatInt(orderID, 10)), // messages with one key publish in order
        Value: event,
    })
})
```

Relay them with a worker and a `Publisher` that wraps your broker client:

```go
var handlers taskmill.Handlers
ob.AddRelay(&handlers, outbox.PublisherFunc(func(ctx context.Context, ds []outbox.Delivery) []error {
    errs := make([]error, len(ds))
    for i, d := range ds {
        errs[i] = kafka.Produce(ctx, d.Topic, d.Key, d.Value, d.ID) // one error per delivery
    }
    return errs
}))
worker, err := taskmill.NewWorker(client, &taskmill.WorkerConfig{Handlers: &handlers}) // serves outbox.Queue
```

- Messages with the same topic and key are published one at a time, in the
  order they were stored; messages without a key, in any order. A batch
  holds up to 100 deliveries, never two with the same topic and key.
- Delivery is at least once. Each `Delivery.ID` is a UUIDv7 that stays the
  same across retries, so consumers can deduplicate by it.
- Keys and header values are `[]byte`, so binary data is safe. `Value` is
  stored base64-encoded, so the 1 MiB payload limit allows about 750 KiB.
- Each `Publish` call has 1 minute; then its context is canceled.
  Deliveries without a nil result are retried quickly at first, then every
  5 minutes, up to 10,000 times: about a month. Return
  `taskmill.Permanent(err)` for a delivery that can never succeed. A message
  that becomes dead halts its key until an operator retries or deletes it.
- `Store` rejects a message without a topic with an error that wraps
  `taskmill.ErrInvalidArgument`. Published messages are not kept in history.

**The inbox** needs no package. A consumer enqueues one task per received
message, with the message ID as the dedupe key, and commits its broker offset
afterwards. A redelivered message then becomes a duplicate result instead of
a second task:

```go
_, err := client.Enqueue(ctx, HandleOrderEvent{...}, &taskmill.EnqueueOptions{
    DedupeKey:    msg.ID,
    DedupeWindow: 7 * 24 * time.Hour, // longer than the broker's redelivery horizon
})
```

## Operating taskmill

**The web interface** is a dashboard of queues, tasks, schedules, and
workers. It shows task counts live, charts of the last hour, 6 hours, or
day, and each task's payload and error history. It filters tasks by several
states and queues at once. It can pause and resume queues and schedules, run
a schedule now, and retry, cancel, and delete tasks. It has light and dark
themes, works on phones, and works without JavaScript too.

It is light on the database and the network. Each request takes at most 10
seconds. A live update downloads a page only when it changed, compressed,
and changes only the parts of the screen that differ. While the server is
out of reach, the page keeps what it shows, says so, and tries again less
often. Use it in one of three ways:

- **In your application,** behind your own authentication:

  ```go
  mux.Handle("/taskmill/", requireAdmin(ui.Handler(client, &ui.Config{Prefix: "/taskmill"})))
  ```

  The handler strips `Prefix` itself; do not wrap it in `http.StripPrefix`.
- **On its own,** with the `taskmill-ui` command. It reads its settings from
  the environment, and asks for a password when `TASKMILL_UI_PASSWORD` is
  set:

  ```sh
  go install github.com/code19m/taskmill/cmd/taskmill-ui@latest
  TASKMILL_DATABASE_URL=postgres://... TASKMILL_UI_PASSWORD=... taskmill-ui
  ```

  In your own server, `ui.Config.BasicAuth` does the same. Serve it over
  HTTPS: basic authentication sends the password with every request. Set
  `TASKMILL_UI_PREFIX` when a proxy forwards a path such as `/admin/jobs` to
  it, and `TASKMILL_UI_FRAME_ANCESTORS` to the origins that may frame it.

  Each [release](https://github.com/code19m/taskmill/releases) also has it
  as binaries for Linux, macOS, and Windows, and as a container image for
  amd64 and arm64: static, about 16 MB, and running as an unprivileged user.
  In a container, set `TASKMILL_UI_ADDR=:8080`, since the default listens
  only inside the container:

  ```sh
  docker run -p 8080:8080 -e TASKMILL_UI_ADDR=:8080 \
    -e TASKMILL_DATABASE_URL=postgres://... -e TASKMILL_UI_PASSWORD=... \
    ghcr.io/code19m/taskmill-ui:latest
  ```

  In production, pin the tag of the taskmill version your applications run,
  such as `1.0.0`: a newer dashboard fails on an older schema with
  `ErrSchemaOutdated`. To check that a download was built by taskmill's
  release workflow, pass the archive, or
  `oci://ghcr.io/code19m/taskmill-ui:1.0.0`, to `gh attestation verify` with
  `--repo code19m/taskmill --signer-workflow
  code19m/taskmill/.github/workflows/release.yml`.
- **Inside another web application,** in an `<iframe>`. List that
  application's origin in `ui.Config.FrameAncestors`; otherwise browsers
  refuse to show the interface in a frame. Add `?theme=light`,
  `?theme=dark`, or `?theme=system` to the frame's address to match your
  application's theme.

`ui.Config.ReadOnly` disables every action. Forms are protected against
cross-site request forgery, and every action is logged to `ui.Config.Logger`.

**The operations API**, the `Client` methods below, offers the same to your
own code and tools:

| Method | Does |
|---|---|
| `Get` | Returns a task, unfinished, finished, or from history |
| `List` | Pages through tasks by queues, states, kinds, and FIFO groups, newest first |
| `Groups` | The FIFO groups that have unfinished tasks, which of them are halted, and by which task |
| `Cancel`, `CancelTx` | Cancels a waiting task, or asks a running task's worker to stop it |
| `Retry` | Runs a dead or canceled task again, with fresh attempts |
| `Delete` | Deletes a task that is not running |
| `Queues` | Every queue with its settings, workers, and task counts per state |
| `UpdateQueue`, `PauseQueue`, `ResumeQueue` | Change a queue's installation-wide settings |
| `Schedules`, `PauseSchedule`, `ResumeSchedule`, `RunSchedule` | List schedules, pause and resume them, and run one now |
| `Settings`, `UpdateSettings` | Retention of history, and of dead and canceled tasks |
| `Workers` | The running workers, the queues and kinds they serve, and which one is the maintainer |
| `Activity` | Task counts and ended attempts per minute, for the last day |

A task or schedule that does not exist returns `ErrNotFound`. An action that
does not fit the task's state returns `ErrInvalidState`: for example,
canceling or retrying a completed task. Each `List` page's query stops after
5 seconds. On a very large installation, a filter that matches few tasks can
reach that bound and return an error.

A running task's cancellation takes effect when its attempt ends, however it
ends: a failure, a snooze, or a release at shutdown. If the handler completes
the task before it notices the cancellation, the task stays completed.

**Error history.** `TaskInfo.Errors` keeps a task's 20 most recent errors.
`AttemptError.Code` is empty for an error that a handler returned. For
failures that taskmill detects itself, it is `CodeLeaseExpired`, `CodeTimeout`,
`CodePanic`, `CodeInvalidPayload`, or `CodeUnknownKind`. `CodeNoHandler`
marks a note, not a failure.

**Running one task now.** `Worker.RunTask(ctx, id)` runs one task at once,
in the calling goroutine, and returns the task afterwards. It ignores the
task's run time, queue pauses, and concurrency limits, but not FIFO order. It
needs no running worker.

**Retention.** Completed tasks move to a history table and stay there for
7 days. Dead and canceled tasks stay for 30 days. Both are installation-wide
settings, of at least 1 hour. A dead task that halts its group is kept until
an operator retries or deletes it. History grows with your completion rate,
so for busy kinds whose completed tasks are not worth keeping, skip it:

```go
taskmill.AddHandler(&handlers, &CacheWarmer{}, &taskmill.HandlerOptions{DiscardCompleted: true})
```

## Several services

Each service that runs its own tasks has its own installation. There are two
setups:

- **One database, a schema per service.** Each service migrates and works in
  its own schema. A service enqueues a task for another with a `Client` for
  that service's schema, in the same transaction as its own writes.
- **A database per service.** A service hands work to another through a
  broker: it stores a message with [the outbox](#the-outbox-and-the-inbox),
  and the other service's inbox enqueues the task.

**Dashboards.** Each service serves the dashboard of its own installation, on
an address that only your admin application reaches. The admin application
forwards a path per service to it, behind its own login:

```go
// In the orders service:
mux.Handle("/admin/jobs/orders/", ui.Handler(client, &ui.Config{Prefix: "/admin/jobs/orders"}))

// In the admin application:
orders := httputil.NewSingleHostReverseProxy(ordersURL) // such as http://orders.internal:9090
mux.Handle("/admin/jobs/orders/", requireAdmin(orders))
```

- Forward the path unchanged: the dashboard strips its `Prefix` itself and
  builds its links with it.
- Each service's database stays its own, and each dashboard runs the
  taskmill version of its service.
- An admin application in another language forwards the same way to
  `taskmill-ui`, run next to each service with `TASKMILL_UI_PREFIX` set to
  the forwarded path.
- With one database, the admin application can instead mount a handler per
  schema itself. Its taskmill version must then fit every schema: no newer
  than the oldest, since newer code fails with `ErrSchemaOutdated`, and at
  most one release older than the newest. Forwarding avoids this.

**Viewers and operators.** `ReadOnly` applies to a whole handler. To let some
people act and others only look, mount both and pick one per request:

```go
operate := ui.Handler(client, &ui.Config{Prefix: "/admin/jobs"})
view := ui.Handler(client, &ui.Config{Prefix: "/admin/jobs", ReadOnly: true})
mux.Handle("/admin/jobs/", requireAdmin(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
    if canOperate(r) {
        operate.ServeHTTP(w, r)
    } else {
        view.ServeHTTP(w, r)
    }
})))
```

When the admin application forwards, it can tell the service the role in a
header for the service to pick by. Set that header on every forwarded
request, replacing any the browser sent, and trust it only on an address
that nothing else reaches.

**Who did what.** Every action is logged to `ui.Config.Logger` with the
request's context. A log handler that reads the signed-in user from the
context, where your login middleware put it, adds them to each entry:

```go
type withUser struct{ slog.Handler }

func (h withUser) Handle(ctx context.Context, r slog.Record) error {
    if user, ok := userFrom(ctx); ok {
        r.AddAttrs(slog.String("user", user))
    }
    return h.Handler.Handle(ctx, r)
}

handler := ui.Handler(client, &ui.Config{Logger: slog.New(withUser{logger.Handler()})})
```

When the admin application forwards, every action passes through it as a
`POST` request, so it can log each one with the user itself.

**One view across services.** For a page of key numbers from every service,
have each service report what it needs from the operations API, such as
`Client.Queues` for its task counts per state, and show them together in the
admin application.

## Observability

**Logs** go to the `*slog.Logger` in `ClientConfig.Logger`, which workers use
too, and the web interface's to `ui.Config.Logger`. By default they are
discarded. The [log catalog](#log-catalog) lists every message, its level,
and what to do.

**Traces and metrics** come from the `otel` module, which instruments
taskmill with OpenTelemetry's conventions for messaging. It is a Go module
of its own, so applications without OpenTelemetry download none of it:

```sh
go get github.com/code19m/taskmill/otel
```

```go
import tmotel "github.com/code19m/taskmill/otel"

tel := tmotel.New(nil) // OpenTelemetry's global providers and propagator
client, err := taskmill.NewClient(ctx, pool, &taskmill.ClientConfig{
    EnqueueMiddleware: []taskmill.EnqueueMiddleware{tel.EnqueueMiddleware},
})
// ...
worker, err := taskmill.NewWorker(client, &taskmill.WorkerConfig{
    Handlers:   &handlers,
    Middleware: []taskmill.Middleware{tel.Middleware}, // first, to measure the rest too
})
// ...
_, err = tel.ObserveQueues(client) // the queue gauges
```

Set a propagator, such as `otel.SetTextMapPropagator(propagation.TraceContext{})`:
without one, no trace context reaches the tasks. `tmotel.New` also takes a
`Config` with other providers.

| Signal | Name | What it measures |
|---|---|---|
| Span | `send {queue}` | Each enqueue call, one span per batch. Its context is stored in the tasks' metadata. |
| Span | `process {queue}` | Each attempt, as a child of the `send` span. The handler's context carries it. |
| Span | `send {topic}` | Each outbox delivery to the broker. |
| Counter | `messaging.client.sent.messages` | Tasks enqueued, and outbox messages published. |
| Histogram | `messaging.client.operation.duration` | How long enqueue calls take. |
| Histogram | `messaging.process.duration` | How long attempts take. |
| Gauge | `taskmill.queue.tasks` | Tasks per queue and state, up to the caps below. |
| Gauge | `taskmill.queue.capped` | 1 when a count of the queue stopped at its cap. |
| Gauge | `taskmill.queue.lag` | How long each queue's oldest due task has waited. Alert on it for backlogs. |
| Gauge | `taskmill.stats.age` | How long ago the maintainer last counted. Alert when it passes a minute: no maintainer runs. |

- Metrics carry the queue as `messaging.destination.name`, the kind as
  `taskmill.task.kind`, and on failure `error.type`, such as `panic`,
  `timeout`, or `invalid_argument`. They never carry task IDs, so their
  cardinality stays bounded.
- A task's attempts show in the trace of the request that enqueued it, even
  a retry hours later.
- Inside your transaction, the `send` span ends before the commit: a
  rollback leaves a span for a task that never existed.
- The queue gauges describe the whole installation, as the maintainer last
  counted it, every 10 seconds. Every process that reports them reads the
  same stored values: aggregate them with max, not sum. Lag keeps growing
  when no maintainer runs, but a queue that was empty at the last count
  shows none: `taskmill.stats.age` catches that.
- For the outbox, wrap the publisher. Each delivery's span is a child of the
  span that stored the message, and goes to the broker in the message's
  headers, so consumers continue the trace:

  ```go
  outbox.New(client).AddRelay(&handlers, tel.Publisher("kafka", publisher))
  ```

**By hand**, the same extension points serve other tools. Worker
`Middleware` wraps every attempt and sees its kind, queue, attempt, metadata,
and timestamps; `ClientConfig.EnqueueMiddleware` wraps every enqueue, and may
add metadata, such as a trace context, to the tasks it enqueues. This one
copies values from the context into the metadata, keeping what the enqueue
set, as the deprecated `ClientConfig.ContextMetadata` does until v0.3.0
removes it:

```go
func FromContext(ctx context.Context, tasks []*taskmill.EnqueueTask, next func(context.Context) error) error {
    for _, task := range tasks {
        for k, v := range valuesOf(ctx) {
            if _, set := task.Metadata[k]; !set {
                task.Metadata[k] = v
            }
        }
    }
    return next(ctx)
}

func Metrics(ctx context.Context, task *taskmill.TaskInfo, next func(context.Context) error) error {
    start := time.Now()
    err := next(ctx)
    pickup := task.AttemptedAt.Sub(task.RunAt) // how long the task waited
    record(task.Queue, task.Kind, err == nil, time.Since(start), pickup)
    return err
}
```

Middleware must call `next` at most once. A handler's panic reaches attempt
middleware as an error. For a batch handler, middleware runs once per task,
but the context it passes to `next` does not reach the handler. Enqueue
middleware that fails after `next` stored the tasks fails an enqueue in your
transaction, so that you can roll back; any other enqueue has committed, so
it succeeds, and the error is logged.

**Queue depth** comes from `Client.Queues`, whose counts the maintainer
refreshes every 10 seconds. Counting is capped, so it stays cheap on a large
backlog: up to 10,000 tasks per queue and state, and up to 100,000 scheduled
and retryable tasks across all queues. `QueueInfo.Capped` means a count hit
its cap and is a lower bound. `QueueInfo.Lag` is how long the queue's oldest
due task has waited since its run time: an available task, or a scheduled or
retryable one whose time has come. The maintainer finds that task when it
counts, and `Client.Queues` measures its age by the database's clock, so lag
has no cap, suits backlog alerts, and keeps growing when no maintainer runs.
It may read high for up to one refresh, about 10 seconds. FIFO tasks waiting
behind their group do not count.

**History** comes from `Client.Activity`: for each minute of the last 24
hours, the most tasks counted in each state, and the attempts that completed
and failed. The web interface charts it over the last hour, 6 hours, or day.
It is recorded on the server, so a chart shows the same history to everyone
from the moment it opens. A gap means that no worker ran.

```go
points, err := client.Activity(ctx, &taskmill.ActivityParams{
    Since: time.Now().Add(-6 * time.Hour),
    Step:  5 * time.Minute, // one point per 5 minutes
})
```

### Log catalog

**Levels.** Every message has one level, chosen by what it asks of you:

- **Error**: a person must act. A task is dead, an operation has failed for
  a minute, or a privilege is missing.
- **Warn**: something failed or looks wrong, but taskmill goes on and
  recovers on its own. Act if it lasts.
- **Info**: lifecycle: a worker starts or stops, the maintainer changes,
  migrations run, an operation recovers, or someone acts in the web
  interface.
- **Debug**: routine events of single tasks, such as a retry.

**Attributes** use the same keys for the same things: `task_id`, `kind`,
`queue`, `attempt`, `worker_id` (on every line about a worker's own work),
`error`, and `duration`. No line shows a task's payload, its metadata, or a connection
string. A handler's error is logged as the handler returned it, so keep
secrets out of errors.

**Repeated failures.** A worker repeats some operations all the time, such
as fetching tasks. While the database is down, they fail at every try, but
each one logs only three kinds of line about an incident:
`operation failed` (Warn) when it starts, `operation still failing` (Error)
at most once a minute while it lasts, and `operation recovered` (Info) when
the operation works again. An incident lasts while failures come less than
a minute apart, even with successes between them: an operation that fails
at every other try escalates too, and logs no more. Without a success
between them, as for work that runs only now and then, failures may come up
to two minutes apart. A failure soon after a
line waits for the next one. `failures` counts the incident's failures, and
`duration` runs from its first failure to its last. The `operation`
attribute names it:

| Operation | What it does | How often |
| --- | --- | --- |
| `fetch tasks` | Leases tasks of the queue in `queue` | Every poll |
| `renew leases` | Renews the leases of running tasks | Every third of the lease |
| `record heartbeat` | Shows that the worker is alive | Every third of the lease |
| `record results` | Records attempts that ended | After each attempt |
| `load queue settings` | Reads pauses and concurrency limits | Every 10 seconds |
| `record activity` | Writes the counts for the charts | Every 10 seconds |
| `listen for notifications` | Keeps the notification connection; the worker polls meanwhile | Backoff, 1 to 30 seconds |
| `elect maintainer` | Takes or keeps the maintainer role | Every 1 to 2 seconds |
| `promote due tasks` | Makes scheduled and retryable tasks available | As they come due |
| `rescue expired leases` | Fails attempts whose workers lost their leases | Every second |
| `run schedules` | Enqueues due schedule runs | Every second |
| `refresh statistics` | Counts tasks for `Client.Queues` | Every 10 seconds |
| `apply retention` | Drops expired history and old rows | Every minute |
| `delete removed schedules` | Deletes schedules no worker defines | Every minute |

The last six run only on the maintainer.

**Messages.**

| Message | Level | What it means, and what to do |
| --- | --- | --- |
| `operation failed` | Warn | An operation above started failing; `error` says why. The worker tries again by itself. |
| `operation still failing` | Error | It has failed for a minute or more. Fix what `error` names, often the database or a privilege. |
| `operation recovered` | Info | It works again. |
| `worker started` | Info | `Run` started, serving `queues`. |
| `worker stopping; waiting for running tasks` | Info | `Run`'s context ended; it waits up to `timeout` for `tasks`. |
| `worker stopped` | Info | `Run` returned; `released` tasks went back to their queues. |
| `worker registered again` | Info | The worker's row was gone, as the maintainer deletes a worker without heartbeats for 5 minutes, so it registered again. It was cut off from the database for that long. |
| `unregister worker failed` | Warn | The stopped worker stays listed until the maintainer deletes it, 5 minutes after its last heartbeat. |
| `the worker does not serve the default queue of these registered kinds` | Warn | Tasks of `kinds` enqueued with their defaults wait for another worker. Add the queues, or run a worker that serves them. |
| `the pool has fewer connections than the worker's concurrency plus 5; ...` | Warn | Handlers can take every connection and starve heartbeats. Raise `MaxConns`; see [Connections](#running-in-production). |
| `elected maintainer` | Info | This worker now does the installation-wide work. |
| `no longer maintainer` | Info | Another worker took over, or the election failed. The maintainer's work stops here; its incidents end without `operation recovered`. |
| `resign maintainer failed; another worker leads when the lease expires` | Warn | The next maintainer starts within 10 seconds. |
| `rescued tasks whose workers lost their leases` | Warn | `tasks` attempts failed because their workers stopped renewing them: a worker crashed or could not reach the database. The tasks retry; those that used all their attempts die. |
| `dropped expired history` | Info | Retention dropped the history `partition`. |
| `attempt failed; retrying` | Debug | The handler failed; the task runs again in `retry_in`. |
| `attempt failed; the task is dead` | Error | The task failed for good, also when its last attempt was rescued. Fix the cause, then retry or delete it. |
| `no handler for task kind; snoozing` | Warn | No handler of this worker knows `kind`. The task waits up to an hour for a worker that does; see [Deploys](#running-in-production). Later snoozes of the task log at Debug. |
| `attempt lost its lease; canceling it` | Warn | The worker could not renew the lease in time, so the task may already run elsewhere. Check the database and the pool size. |
| `result discarded: the attempt lost its lease` | Debug | The attempt ended after it lost its lease; its result does not count. |
| `handler ignored shutdown; releasing its task` | Warn | The handler did not return after its context was canceled. The task runs again; make the handler honor its context. |
| `handler failed after completing its task; it stays completed` | Warn | The handler returned an error after `CompleteTx` committed. Fix the handler. |
| `record attempt failed; the task is rescued when its lease expires` | Warn | The attempt's result could not be written in time; the task is retried later. |
| `complete tasks failed; they are rescued when their leases expire` | Warn | `tasks` successful attempts could not be recorded; they run again. |
| `task history has no partition for today; completed tasks skip history` | Error | The role that runs workers must own the taskmill tables; see [Database privileges](#running-in-production). |
| `notifications disabled; polling only` | Error | `ListenConfig` was not created with `pgx.ParseConfig`. Fix it; until then the worker polls. |
| `notification failed` | Warn | A wake-up for `queue` was not sent; its workers find the tasks at their next poll. |
| `enqueue middleware failed after the tasks were enqueued` | Warn | An enqueue middleware returned `error` after the `tasks` committed, so the enqueue succeeded, and the middleware's work, such as a span, may be missing. Fix the middleware. |
| `fifo group missing` | Error | A task's FIFO group has no row, which should not happen. Please report it. |
| `applied migration` | Info | `Migrate` applied migration `version` (`name`) in `duration`. |
| `prepare history partitions failed` | Warn | `Migrate` could not create history partitions; the maintainer tries again. |
| `web interface action` | Info | Someone did `action` on `target` in the web interface. |
| `web interface request failed` | Warn | A page or action failed, often because the database is out of reach. |
| `web interface request timed out` | Warn | The database did not answer within 10 seconds. |
| `read activity failed`, `read filter choices failed`, `search groups failed` | Warn | A page shows less: its charts, its filter choices, or its group search. |
| `render page failed` | Error | A page template failed. Please report it. |
| `taskmill-ui listening` | Info | The command, taskmill `version`, serves the web interface on `addr`. |
| `the dashboard has no password: ...` | Warn | Set `TASKMILL_UI_PASSWORD`, unless something in front of it authenticates. |
| `taskmill-ui stopped` | Error | The command could not start or serve; `error` says why. |

## Testing your code

The `taskmilltest` package helps you test code that uses taskmill. It works
against a real PostgreSQL, by design: a fake would hide the transactional
behavior that taskmill is for. A typical test checks that your code enqueues
the right tasks, and that a handler does the right work:

```go
func TestCheckout(t *testing.T) {
    // A new schema with a random name, dropped when the test ends.
    client := taskmilltest.NewClient(t, testPool(t), nil) // testPool: your test database

    if err := checkout(t.Context(), client, 42); err != nil { // the code under test
        t.Fatal(err)
    }
    tasks := taskmilltest.Enqueued[SendReceipt](t, client) // committed tasks, oldest first
    if len(tasks) != 1 || tasks[0].Args.OrderID != 42 {
        t.Fatalf("enqueued %+v", tasks)
    }

    // Enqueue a task and run the handler on it once, the way a worker does.
    info, err := taskmilltest.Run(t, client, &ReceiptSender{mailer: fakeMailer{}}, tasks[0].Args)
    if err != nil || info.State != taskmill.StateCompleted {
        t.Fatalf("got %s, %v", info.State, err)
    }
}
```

- `NewClient` migrates a new schema, so tests can run in parallel.
- `Enqueued` returns the committed tasks of one kind, with their payloads
  decoded. Use it to check that the code under test enqueued a task when its
  transaction committed, and no task when it rolled back.
- `Run` holds a real lease, so the handler may use `CompleteTx`. It does not
  retry: a failed task stays retryable or dead.
- `Work` runs a worker until no task in any queue is available or running:
  `taskmilltest.Work(t, client, &taskmill.WorkerConfig{Handlers: &handlers})`.

**Retries.** To test that a handler succeeds after failing, enqueue the task
and call `Worker.RunTask` in a loop. `RunTask` runs a retryable task at once,
without waiting for its backoff:

```go
// handlers holds a handler whose mailer fails twice, then succeeds.
worker, err := taskmill.NewWorker(client, &taskmill.WorkerConfig{Handlers: &handlers})
// ...
res, err := client.Enqueue(t.Context(), SendReceipt{OrderID: 42}, nil)
// ...
info := &taskmill.TaskInfo{State: taskmill.StateAvailable}
for i := 0; i < 5 && !info.State.Finished(); i++ {
    if info, err = worker.RunTask(t.Context(), res.ID); err != nil {
        t.Fatal(err)
    }
}
if info.State != taskmill.StateCompleted || info.Attempt != 3 {
    t.Fatalf("got %s after %d attempts", info.State, info.Attempt)
}
```

**database/sql.** The helpers take a `*pgxpool.Pool`. Code built on
`database/sql` can use the same pool, through
`stdlib.OpenDBFromPool(pool)` from `github.com/jackc/pgx/v5/stdlib`.

**Leftover schemas.** A test binary that is killed cannot drop its schemas.
When no tests are running, find the leftovers with this query, and drop each
schema it lists with `DROP SCHEMA ... CASCADE`:

```sql
SELECT nspname FROM pg_namespace WHERE nspname LIKE 'taskmilltest\_%';
```

## Running in production

**Database privileges.** The maintainer creates and drops a daily partition
of the `task_history` table, and that needs table ownership. So the role that
runs workers must own taskmill's tables, or be a member of the role that owns
them. This holds also when your own tool applies the migrations. If a
partition is missing, completions still succeed, but without history, and the
worker logs an error. `Migrate` needs no CREATE privilege when the schema is
already up to date.

**Isolation level.** taskmill's own transactions rely on READ COMMITTED,
PostgreSQL's default. If your database or role sets another default,
`NewClient` fails. Set the level back for taskmill's pool:

```go
cfg, err := pgxpool.ParseConfig(os.Getenv("DATABASE_URL"))
// ...
cfg.ConnConfig.RuntimeParams["default_transaction_isolation"] = "read committed"
pool, err := pgxpool.NewWithConfig(ctx, cfg)
```

Your own transactions, passed to the `...Tx` methods, may use any level. At
REPEATABLE READ or SERIALIZABLE, retrying serialization failures is up to
you.

**Connections.** Running attempts hold no connection of their own. A worker
uses its pool for fetching, heartbeats, results, and maintenance, plus one
dedicated connection for notifications. If handlers use the same pool, set
the pool's `MaxConns` to at least the worker's total concurrency plus 5, or
give handlers a pool of their own. Otherwise handlers can starve the
worker's heartbeats, which then lose leases and run tasks twice. The worker
logs a warning at start when `MaxConns` is lower. pgx's default is the larger
of 4 and the number of CPUs.

**Connection poolers.** Everything works through PgBouncer in transaction
mode except notifications, which need a session. pgx caches prepared
statements, so use PgBouncer 1.21 or newer with `max_prepared_statements`
set, or set pgx's `DefaultQueryExecMode` to `QueryExecModeExec`. Point
`WorkerConfig.ListenConfig` at a direct connection, created with
`pgx.ParseConfig`. Without it, workers fall back to polling.

```go
cfg.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeExec // not needed with max_prepared_statements
listen, err := pgx.ParseConfig(os.Getenv("DIRECT_DATABASE_URL"))
// ...
worker, err := taskmill.NewWorker(client, &taskmill.WorkerConfig{Handlers: &handlers, ListenConfig: listen})
```

**Old snapshots.** A snapshot is a transaction's view of the data. While an
old one stays open anywhere in the database, vacuum cannot remove dead rows
from taskmill's busy `tasks` table. Every fetch then steps over more dead
index entries, so throughput falls and tasks wait longer. Common sources are
idle-in-transaction sessions, `pg_dump`, long analytics queries, replicas
with `hot_standby_feedback`, and stale replication slots.

- Set `idle_in_transaction_session_timeout`, and `statement_timeout` for
  analytics roles. Run long reports on a replica without
  `hot_standby_feedback`.
- Set `autovacuum_naptime` to 10 seconds or less. taskmill's migrations tune
  autovacuum for the `tasks` table; keep autovacuum on.
- Alert when one of these ages keeps growing:

  ```sql
  SELECT max(age(backend_xmin)) FROM pg_stat_activity;    -- open transactions
  SELECT max(age(xmin)) FROM pg_replication_slots;         -- replication slots
  SELECT max(age(backend_xmin)) FROM pg_stat_replication;  -- standbys with hot_standby_feedback
  ```

**Deploys.** Give the process's termination grace period, such as
Kubernetes' `terminationGracePeriodSeconds`, at least `DrainTimeout` plus
10 seconds. The defaults fit Kubernetes' 30 seconds; see
[Running on Kubernetes](#running-on-kubernetes). During a rolling deploy,
a worker may fetch a task whose kind it has no handler for, such as a kind
that only the new version knows. It snoozes the task for 1 minute at a time,
without using an attempt, and records a note with the code `no_handler`. If
no worker runs the task within 1 hour of that note, the task becomes dead
with the code `unknown_kind`. Deploy consumers before producers, and do not
rename kinds.

**What to alert on.**

- Dead tasks: the `StateDead` counts in `Client.Queues`.
- No maintainer: no worker in `Client.Workers` has `Maintainer` set.
  Schedules and retries then stop.
- Error logs: each one needs a person, as the [log catalog](#log-catalog)
  says. Also warnings about rescued tasks.
- The snapshot ages above.

**Retrying after an incident.** There is no bulk retry yet. Page through the
dead tasks with `List`, filtered by queue or kind, and retry each one, as
[the example of `Client.Retry`](https://pkg.go.dev/github.com/code19m/taskmill#example-Client.Retry)
does.

**Replication.** With asynchronous replication, a failover can lose the last
committed transactions, including enqueued tasks, as it can lose any other
row.

**Clocks.** taskmill takes most times from the database, but not all. A
producer sets `RunAt` with its own clock, and workers create the daily
history partitions by their own clocks. Keep every server's clock in sync
with NTP.

## Running on Kubernetes

This section turns [Running in production](#running-in-production) into
Kubernetes terms. Workers run in your application's image; the dashboard
runs from its own.

**Shutting down.** Kubernetes stops a pod with `SIGTERM`, and kills it when
`terminationGracePeriodSeconds` runs out. Tie `Run`'s context to `SIGTERM`
with `signal.NotifyContext`, as the [README](../README.md#quick-start) does.
`Run` then waits up to `DrainTimeout` for running tasks, cancels the rest,
and returns about 5 seconds later. So set the grace period to at least
`DrainTimeout` plus 10 seconds: 30 with the defaults. A task still running
when the drain ends starts over on another worker, so if handlers often run
longer than `DrainTimeout`, raise both. Workers need no `preStop` hook: no
Service sends them traffic.

**Health.** `Worker.Health` returns nil while `Run` runs and its last
heartbeat reached the database within `LeaseDuration`, and an error
otherwise. It reads only memory, so a probe costs the database nothing.
Serve it for a readiness probe:

```go
go func() {
    mux := http.NewServeMux()
    mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
        if err := worker.Health(); err != nil {
            http.Error(w, err.Error(), http.StatusServiceUnavailable)
            return
        }
        w.WriteHeader(http.StatusOK)
    })
    server := &http.Server{Addr: ":8081", Handler: mux, ReadHeaderTimeout: 5 * time.Second}
    log.Print(server.ListenAndServe())
}()
```

With the defaults and the probe below, a worker turns unready within about
a minute of losing the database, and ready again within seconds of its
return. A worker that is not ready holds up a rolling deploy, which is right
while the database is out of reach. Do not restart workers on `Health`: it
fails during a database outage, which a restart does not fix. If you want a
liveness probe, point it at a path that does not call `Health`.

**Workers.** A minimal Deployment, with the probe above and a pool sized for
10 slots:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: worker
spec:
  replicas: 2
  selector:
    matchLabels: {app: worker}
  template:
    metadata:
      labels: {app: worker}
    spec:
      terminationGracePeriodSeconds: 30 # DrainTimeout (20 s) plus 10 s
      containers:
        - name: worker
          image: registry.example.com/myapp:1.4.0 # your application
          args: [worker]
          env:
            - name: DATABASE_URL # ends in ?pool_max_conns=15: 10 slots plus 5
              valueFrom: {secretKeyRef: {name: myapp-db, key: url}}
          ports:
            - {name: health, containerPort: 8081}
          readinessProbe:
            httpGet: {path: /healthz, port: health}
            periodSeconds: 10
          resources:
            requests: {cpu: 250m, memory: 128Mi}
            limits: {memory: 256Mi}
```

**Connections.** A worker uses at most its pool's `MaxConns`, plus one
connection for notifications; a client, such as `taskmill-ui` or a web
server that only enqueues, uses its pool alone. Add them up against the
database's `max_connections`, less what it reserves for administrators:

```text
worker replicas × (MaxConns + 1) + taskmill-ui replicas × MaxConns + your other pools
```

Count the autoscaler's maximum of replicas, not the usual number. Set every
pool's size with `pool_max_conns` in its URL: pgx's default is the larger of
4 and the number of CPUs it sees, and in a container that is the node's CPU
count, not the pod's limit. For example, 10 workers with `pool_max_conns=15`
use up to 160 connections.

**PgBouncer** in transaction mode carries the pools, as
[Connection poolers](#running-in-production) says: give it the pools'
connections, and point `WorkerConfig.ListenConfig` and `Migrate` at the
database itself. Each worker replica then holds one direct connection.

**Migrations.** `Migrate` is safe from many pods at once: one applies the
migrations in a single transaction, while the others wait for its lock and
then find nothing to do. Call it at the start of each process, as the
README does, and there is nothing more to deploy. If your deploys apply
schema changes first, run the same image as a Job instead, for example as a
Helm `pre-upgrade` hook or an earlier Argo CD sync wave:

```yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: myapp-migrate-1-4-0
spec:
  backoffLimit: 3
  template:
    spec:
      restartPolicy: Never
      containers:
        - name: migrate
          image: registry.example.com/myapp:1.4.0
          args: [migrate] # calls taskmill.Migrate and exits
          env:
            - name: DATABASE_URL # direct, not through PgBouncer
              valueFrom: {secretKeyRef: {name: myapp-db, key: direct-url}}
```

A migration stays compatible with the previous release, so old pods keep
working while new ones roll out. `taskmill-ui` does not migrate: until the
schema is migrated, it exits at start, and Kubernetes starts it again.

**The dashboard** runs from the `ghcr.io/code19m/taskmill-ui` image, as a
Deployment of its own. Set `TASKMILL_UI_ADDR`: the default listens on
localhost only. The image holds the binary alone and runs as a non-root
user. With a password set, every page asks for it, so the probe only checks
the port. Put it behind your Ingress with TLS, or reach it with
`kubectl port-forward service/taskmill-ui 8080:80`.

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: taskmill-ui
spec:
  replicas: 1
  selector:
    matchLabels: {app: taskmill-ui}
  template:
    metadata:
      labels: {app: taskmill-ui}
    spec:
      containers:
        - name: taskmill-ui
          image: ghcr.io/code19m/taskmill-ui:0.2.0 # the release you run
          env:
            - {name: TASKMILL_UI_ADDR, value: ":8080"}
            - name: TASKMILL_DATABASE_URL # ends in ?pool_max_conns=4
              valueFrom: {secretKeyRef: {name: myapp-db, key: ui-url}}
            - name: TASKMILL_UI_PASSWORD
              valueFrom: {secretKeyRef: {name: taskmill-ui, key: password}}
          ports:
            - {name: http, containerPort: 8080}
          readinessProbe:
            tcpSocket: {port: http}
          securityContext: {readOnlyRootFilesystem: true, allowPrivilegeEscalation: false}
          resources:
            requests: {cpu: 50m, memory: 32Mi}
            limits: {memory: 128Mi}
---
apiVersion: v1
kind: Service
metadata:
  name: taskmill-ui
spec:
  selector: {app: taskmill-ui}
  ports:
    - {name: http, port: 80, targetPort: http}
```

**Autoscaling.** Scale workers on how many tasks wait, or on how long they
wait. The [queue gauges](#observability) report both; every process reports
the same values for the whole installation, so read them with `max`. With
Prometheus, they are `taskmill_queue_tasks`, labeled
`messaging_destination_name` and `taskmill_task_state`, and
`taskmill_queue_lag_seconds`. This KEDA `ScaledObject` adds a worker per 100
available tasks of the default queue:

```yaml
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: worker
spec:
  scaleTargetRef: {name: worker}
  minReplicaCount: 1 # see below
  maxReplicaCount: 10
  triggers:
    - type: prometheus
      metadata:
        serverAddress: http://prometheus.monitoring.svc:9090
        query: max(taskmill_queue_tasks{messaging_destination_name="default", taskmill_task_state="available"})
        threshold: "100"
```

- **Keep one worker.** The maintainer runs in workers. With none, due
  retries and schedules are never promoted, and a queue that was empty at
  its last count never shows a backlog, so nothing scales the workers up
  again. To scale to zero, run a worker without handlers in a Deployment of
  its own, as the maintainer, and set `NoMaintainer` on the scaled ones.
- **Lag** scales on the oldest task's wait instead: query
  `max(taskmill_queue_lag_seconds{messaging_destination_name="default"})`,
  set the trigger's `metricType: Value`, and a `threshold` in seconds.
  Unlike the counts, which stop at 10,000, lag has no cap.
- **Without OpenTelemetry,** KEDA's Metrics API scaler reads a number from
  JSON. Serve it from `Client.Queues`, next to the health check:

  ```go
  mux.HandleFunc("GET /queues", func(w http.ResponseWriter, r *http.Request) {
      queues, err := client.Queues(r.Context())
      if err != nil {
          http.Error(w, "taskmill: list queues failed", http.StatusServiceUnavailable)
          return
      }
      available := map[string]int64{}
      for _, q := range queues {
          available[q.Name] = q.Counts[taskmill.StateAvailable]
      }
      _ = json.NewEncoder(w).Encode(available)
  })
  ```

  Put a Service in front of the workers' port, and give the
  `ScaledObject` this trigger instead:

  ```yaml
  apiVersion: v1
  kind: Service
  metadata:
    name: worker
  spec:
    selector: {app: worker}
    ports:
      - {name: health, port: 8081, targetPort: health}
  ```

  ```yaml
    triggers:
      - type: metrics-api
        metadata:
          url: http://worker.myapp.svc:8081/queues # the Service, in namespace myapp
          valueLocation: default
          targetValue: "100"
  ```
- Do not point KEDA's PostgreSQL scaler at taskmill's tables. They are
  internal and change between releases; `Client.Queues` and the gauges are
  the stable way to read them ([ADR 0006](adr/0006-health-and-scaling-read-paths.md)).

**Managed PostgreSQL.** taskmill needs PostgreSQL 15 or newer and nothing
more: no extension, and no superuser. So managed services, such as Amazon
RDS and Aurora, Google Cloud SQL and AlloyDB, Azure Database for
PostgreSQL, Neon, and Supabase, can run it. Mind these points:

- **Privileges.** Give the application a role of its own that owns
  taskmill's schema, as [Database privileges](#running-in-production) says.
  To create the schema, `Migrate` needs the CREATE privilege on the
  database; or create the schema yourself, owned by that role.
- **The primary only.** taskmill writes on every fetch: use the writer
  endpoint, such as Aurora's cluster endpoint, never a reader.
- **Built-in poolers** run in transaction mode, so they follow the
  PgBouncer rules above: RDS Proxy, Azure's PgBouncer on port 6432,
  Supabase's transaction pooler on port 6543, and Neon's pooled endpoint.
  Point `ListenConfig` at the direct endpoint. The Cloud SQL and AlloyDB
  Auth Proxies are not poolers: notifications work through them.
- **Failover.** The endpoint's address moves to the new primary, and open
  connections break. Workers reconnect on their own, at each loop's next
  try, and need no setting for it. A task running during a failover longer
  than `LeaseDuration` may run again elsewhere. If that costs more
  than a crashed worker's tasks waiting longer, raise `LeaseDuration` above
  your provider's failover time.
- **Neon's scale to zero** never happens while workers run, because they
  poll every second: budget for a compute that stays on.

## Upgrading

taskmill follows [Semantic Versioning](https://semver.org/).

- **Before v1.0.0,** a minor release (v0.x.0) may break the API; a patch
  release never does. The [changelog](../CHANGELOG.md) marks each breaking
  change with **Breaking:** and says how to migrate. Only the latest minor
  release gets fixes, including security fixes.
- **Interfaces you implement never grow.** `Args`, `Handler`,
  `BatchHandler`, `RetryPolicy`, and `outbox.Publisher` keep their methods.
  New capabilities arrive as new optional interfaces, the way
  `ArgsWithDefaults` extends `Args`. Configuration structs grow only with
  fields whose zero value keeps the old behavior.
- **Deprecation comes first.** An identifier is marked `// Deprecated:` for at
  least one minor release before it is removed.
- **Migrations only move forward.** There are no down migrations. Run
  `Migrate`, or apply `Migrations`, before you roll out a new version.
- **Old code works on a newer schema.** Each release's schema works with the
  previous release's workers and clients, so old and new versions can run
  side by side during a rolling deploy; CI checks this for every change.
  Code that is newer than its schema fails in `NewClient` with
  `ErrSchemaOutdated`.
- **The tables are internal.** Their layout can change in any release. Read
  and change tasks through `Client`, not with SQL.

## Defaults and limits

| Setting | Default | Limit |
|---|---|---|
| Schema | `taskmill` | Matches `^[a-z_][a-z0-9_]{0,62}$` |
| Queue, kind, and schedule names | — | 1 to 128 characters |
| Group key | — | 1 to 256 characters |
| Payload | — | 1 MiB of JSON |
| `MaxAttempts` | 25 | 1 to 10,000 |
| Retry backoff | 1 s, doubling to 1 h, ±10% jitter; about 13 h for 25 attempts | — |
| Retry and run time accuracy | About 1 second | — |
| `DedupeWindow` | 24 hours | — |
| `HandlerOptions.Timeout` | 1 minute | — |
| `HandlerOptions.BatchSize` | 100 | 1,000 |
| `QueueOptions.Concurrency` | 10 slots per queue and worker | — |
| `LeaseDuration` | 30 seconds; heartbeats every 10 seconds | At least 100 ms |
| `PollInterval` | 1 second | — |
| `DrainTimeout` | 20 seconds, then 5 seconds for handlers to return | — |
| Unknown kind | Snoozed 1 minute at a time; dead after 1 hour | — |
| Maintainer takeover | Within about 12 seconds | — |
| History retention | 7 days | At least 1 hour |
| Dead and canceled retention | 30 days | At least 1 hour |
| Error history | 20 most recent errors per task | 4 KiB per message |
| `List` page | 100 tasks | 1,000 tasks; 5 seconds per query |
| Queue statistics | Refreshed every 10 seconds | 10,000 per queue and state; 100,000 scheduled and retryable in all |
| Removed schedules | Deleted after 24 hours without a run or change | — |
| Outbox | 100 deliveries per batch; 1 minute per `Publish` | 10,000 tries, about a month; about 750 KiB of `Value` |
