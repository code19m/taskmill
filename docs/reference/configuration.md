# Configuration

Every field of every config, options, and parameter type: its type, its
default, its limits, and what it does. Search this page for the field's
name. Values that are not fields, such as fixed timings, are in
[Defaults and limits](defaults-and-limits.md).

How to read the tables:

- **Default** is what you get when you leave the field at its zero value.
- **Limits** are checked when you pass the value. A value outside them makes
  the call return an error that wraps
  [`ErrInvalidArgument`](errors.md#errors-that-calls-return). Every name and
  key must also be valid UTF-8, with no NUL character.
- Each type's name links to its godoc, which has the full contract.

## Client and migrations

### `ClientConfig`

For `NewClient`. It may be nil. [Godoc](https://pkg.go.dev/github.com/code19m/taskmill#ClientConfig)

| Field | Type | Default | Limits | What it does |
|---|---|---|---|---|
| `Schema` | `string` | `"taskmill"` | Matches `^[a-z_][a-z0-9_]{0,62}$` | Names the PostgreSQL [schema](../concepts/glossary.md#schema) that holds taskmill's tables |
| `Logger` | `*slog.Logger` | None: nothing is logged | — | Receives the logs of the client and of its workers |
| `EnqueueMiddleware` | `[]EnqueueMiddleware` | None | — | Wraps every enqueue, in order: the first is the outermost |

### `MigrateConfig`

For `Migrate`. It may be nil. [Godoc](https://pkg.go.dev/github.com/code19m/taskmill#MigrateConfig)

| Field | Type | Default | Limits | What it does |
|---|---|---|---|---|
| `Schema` | `string` | `"taskmill"` | Matches `^[a-z_][a-z0-9_]{0,62}$` | Names the schema to create or upgrade |
| `Logger` | `*slog.Logger` | None: nothing is logged | — | Receives one line per applied migration, and a warning if history partitions cannot be prepared |

## Enqueue

### `EnqueueOptions`

For the enqueue calls and for `Schedule.Options`. It may be nil. A field
set in the call is used first, then the kind's `EnqueueDefaults`, then the
default below. `Metadata` merges key by key instead.
[Godoc](https://pkg.go.dev/github.com/code19m/taskmill#EnqueueOptions)

| Field | Type | Default | Limits | What it does |
|---|---|---|---|---|
| `Queue` | `string` | `"default"` | 1 to 128 characters | Names the queue that the task goes to |
| `Priority` | `Priority` | `PriorityNormal` | `PriorityLow` to `PriorityCritical` | Orders tasks within the queue: higher priorities start first |
| `RunAt` | `time.Time` | Now | — | Sets the earliest time the task may start |
| `MaxAttempts` | `int` | 25 | 1 to 10,000 | Sets how many times the task may run before it is dead |
| `GroupKey` | `string` | None | Up to 256 characters | Puts the task in a [FIFO group](../concepts/glossary.md#fifo-group): tasks with the same key in the same queue run one at a time, in enqueue order |
| `DedupeKey` | `string` | None | Any length | Makes the enqueue create no new task while the same queue, kind, and [key](../concepts/glossary.md#dedupe-key) are in their window |
| `DedupeWindow` | `time.Duration` | 24 hours | Not negative | Sets how long `DedupeKey` blocks duplicates, counted from the enqueue |
| `Metadata` | `map[string]string` | None | 64 KiB as JSON, with what enqueue middleware adds; no NUL character | Is stored with the task and passed to its handler, for example for tracing |

## Handlers

### `HandlerOptions`

For `AddHandler` and `AddBatchHandler`. It may be nil. `NewWorker` reports a
value outside the limits.
[Godoc](https://pkg.go.dev/github.com/code19m/taskmill#HandlerOptions)

| Field | Type | Default | Limits | What it does |
|---|---|---|---|---|
| `Timeout` | `time.Duration` | 1 minute | Not negative | Bounds each attempt, or each batch |
| `Retry` | `RetryPolicy` | `ExponentialBackoff{}` | — | Decides the wait before each retry. After a lost [lease](../concepts/glossary.md#lease), the default is used instead |
| `BatchSize` | `int` | 100 | Up to 1,000; not negative | Sets the most tasks that a batch handler receives in one call. Other handlers ignore it |
| `DiscardCompleted` | `bool` | `false` | — | Deletes the kind's completed tasks right away, instead of keeping them in history |

### `ExponentialBackoff`

The default `RetryPolicy`. Each wait also varies at random by ±10%.
[Godoc](https://pkg.go.dev/github.com/code19m/taskmill#ExponentialBackoff)

| Field | Type | Default | Limits | What it does |
|---|---|---|---|---|
| `Base` | `time.Duration` | 1 second | Zero or less means the default | Sets the wait after the first failed attempt. The wait doubles after each further failure |
| `Max` | `time.Duration` | 1 hour | Zero or less means the default | Sets the longest wait |

## Workers

### `WorkerConfig`

For `NewWorker`. It may be nil. [Godoc](https://pkg.go.dev/github.com/code19m/taskmill#WorkerConfig)

| Field | Type | Default | Limits | What it does |
|---|---|---|---|---|
| `Handlers` | `*Handlers` | None: the worker only takes part in maintenance | Required when `Queues` is set | Holds the handlers that run the tasks this worker fetches |
| `Queues` | `map[string]QueueOptions` | The default queue of every registered kind | Names of 1 to 128 characters | Lists the queues to fetch from, with the options of this process |
| `LeaseDuration` | `time.Duration` | 30 seconds | At least 100 ms | Sets how long a fetched task stays owned without a renewal. The worker makes its [check-in](../concepts/glossary.md#check-in) every third of it |
| `PollInterval` | `time.Duration` | 1 second | Not negative | Sets how often the worker checks an idle queue while the maintainer's [heartbeats](../concepts/glossary.md#heartbeat) don't arrive, and a queue that its `MaxRunning` limit blocks |
| `DrainTimeout` | `time.Duration` | 20 seconds | Not negative | Sets how long `Run` waits for running tasks after its context ends. Then it cancels their contexts, and 5 seconds later it releases them |
| `Middleware` | `[]Middleware` | None | — | Wraps every attempt, in order: the first is the outermost |
| `Schedules` | `[]Schedule` | None | Each name once | Lists the recurring tasks that the [maintainer](../concepts/glossary.md#maintainer) enqueues |
| `NoMaintainer` | `bool` | `false` | At least one worker of a schema must take part | Keeps this worker out of the maintainer election |
| `ListenConfig` | `*pgx.ConnConfig` | A copy of the pool's connection settings | Made with `pgx.ParseConfig` | Sets the connection that receives [notifications](../concepts/glossary.md#notification). Point it at the database directly when the pool goes through a pooler in transaction mode |

### `QueueOptions`

The values of `WorkerConfig.Queues`. [Godoc](https://pkg.go.dev/github.com/code19m/taskmill#QueueOptions)

| Field | Type | Default | Limits | What it does |
|---|---|---|---|---|
| `Concurrency` | `int` | 10 | Not negative | Sets how many of the queue's tasks this process runs at the same time. A batch counts as one |

### `Schedule`

The entries of `WorkerConfig.Schedules`. [Godoc](https://pkg.go.dev/github.com/code19m/taskmill#Schedule)

| Field | Type | Default | Limits | What it does |
|---|---|---|---|---|
| `Name` | `string` | Required | 1 to 128 characters | Identifies the schedule in the database. A new name is a new schedule |
| `Cron` | `string` | Required | Five fields, or a macro such as `@daily`. Must match a time in the next five years | Says when the schedule runs. The godoc has the full syntax |
| `Location` | `*time.Location` | UTC | — | Sets the time zone that `Cron` is read in |
| `Args` | `Args` | Required | The limits of any payload | Is the task to enqueue |
| `Options` | `*EnqueueOptions` | None | Must not set `RunAt`, `DedupeKey`, or `DedupeWindow` | Sets the options of that task |

## Settings stored in the database

### `QueueSettings`

Changed with `Client.UpdateQueue`, for all workers. [Godoc](https://pkg.go.dev/github.com/code19m/taskmill#QueueSettings)

| Field | Type | Default | Limits | What it does |
|---|---|---|---|---|
| `MaxRunning` | `int` | 0: no limit | Not negative | Caps how many tasks of the queue run at the same time across all workers. Tasks inside batches count too |

### `Settings`

Changed with `Client.UpdateSettings`. A field set to zero goes back to its
default. [Godoc](https://pkg.go.dev/github.com/code19m/taskmill#Settings)

| Field | Type | Default | Limits | What it does |
|---|---|---|---|---|
| `HistoryRetention` | `time.Duration` | 7 days | At least 1 hour | Sets how long completed tasks stay in history. History is dropped a whole UTC day at a time, so a task can stay up to a day longer |
| `DeadRetention` | `time.Duration` | 30 days | At least 1 hour | Sets how long dead and canceled tasks are kept. A dead task that halts its FIFO group is kept until you retry or delete it |

## Operations API parameters

### `TaskFilter`

For `List`, `Count`, and the bulk actions. A task matches when it matches
every field that is set. [Godoc](https://pkg.go.dev/github.com/code19m/taskmill#TaskFilter)

| Field | Type | Default | Limits | What it does |
|---|---|---|---|---|
| `Queues` | `[]string` | Empty: all queues | Names of 1 to 128 characters | Selects the tasks of these queues |
| `States` | `[]State` | Empty: all states | Only the eight states | Selects the tasks in these states |
| `Kinds` | `[]string` | Empty: all kinds | Kinds of 1 to 128 characters | Selects the tasks of these kinds |
| `GroupKeys` | `[]string` | Empty: all tasks | Keys of 1 to 256 characters | Selects the tasks of these FIFO groups |

### `ListParams`

For `Client.List`. It may be nil. [Godoc](https://pkg.go.dev/github.com/code19m/taskmill#ListParams)

| Field | Type | Default | Limits | What it does |
|---|---|---|---|---|
| `Filter` | `TaskFilter` | Empty: every task | See `TaskFilter` | Selects the tasks |
| `Limit` | `int` | 100 | 1 to 1,000 | Sets the page size |
| `Cursor` | `string` | Empty: the first page | A `ListResult.Next` | Continues after the previous page |

### `GroupParams`

For `Client.Groups`. It may be nil. [Godoc](https://pkg.go.dev/github.com/code19m/taskmill#GroupParams)

| Field | Type | Default | Limits | What it does |
|---|---|---|---|---|
| `Queues` | `[]string` | Empty: all queues | Up to 100 names | Selects the groups of these queues |
| `Search` | `string` | Empty: all groups | Up to 256 characters | Selects the groups whose keys contain this text, ignoring case |
| `Halted` | `bool` | `false` | — | Selects only the [halted groups](../concepts/glossary.md#halted-group) |
| `Limit` | `int` | 100 | 1 to 1,000 | Sets the page size |
| `Cursor` | `string` | Empty: the first page | A `GroupsResult.Next` | Continues after the previous page |

### `BulkParams`

For `RetryMatching`, `CancelMatching`, and `DeleteMatching`. [Godoc](https://pkg.go.dev/github.com/code19m/taskmill#BulkParams)

| Field | Type | Default | Limits | What it does |
|---|---|---|---|---|
| `Filter` | `TaskFilter` | Required | Must set at least one field | Selects the tasks to act on |
| `Cursor` | `string` | Empty: from the oldest task | A `BulkResult.Next` | Continues an action that stopped early |

### `ActivityParams`

For `Client.Activity`. It may be nil. [Godoc](https://pkg.go.dev/github.com/code19m/taskmill#ActivityParams)

| Field | Type | Default | Limits | What it does |
|---|---|---|---|---|
| `Queues` | `[]string` | Empty: all queues | Names of 1 to 128 characters | Selects the activity of these queues |
| `Since` | `time.Time` | An hour ago | Activity is kept 24 hours | Sets when the period starts |
| `Step` | `time.Duration` | 1 minute | Whole minutes, up to 1 day | Sets how much time each point covers |

## Dashboard, outbox, and OpenTelemetry

### `ui.Config`

For `ui.New`. It may be nil. [Godoc](https://pkg.go.dev/github.com/code19m/taskmill/ui#Config).
The `taskmill-ui` command sets these fields from its
[environment variables](taskmill-ui.md#environment-variables).

| Field | Type | Default | Limits | What it does |
|---|---|---|---|---|
| `Prefix` | `string` | None: the root | Starts with `/` | Names the path that the handler is mounted at. The handler strips it itself: don't wrap it in `http.StripPrefix` |
| `ReadOnly` | `bool` | `false` | — | Hides and refuses every action that changes state |
| `BasicAuth` | `*BasicAuth` | None: your app authenticates | `Username` and `Password` both set | Makes every request log in as this one user, with HTTP basic authentication |
| `FrameAncestors` | `[]string` | None: no page can frame the dashboard | No empty entry; no space, comma, or semicolon in an entry | Lists the origins of the pages that may show the dashboard in a frame, such as `https://admin.example.com`, or `'self'` |
| `Logger` | `*slog.Logger` | None: nothing is logged | — | Receives every action taken, and the errors behind a server error page |

### `outbox.RelayOptions`

For `Outbox.AddRelay`. It may be nil. [Godoc](https://pkg.go.dev/github.com/code19m/taskmill/outbox#RelayOptions)

| Field | Type | Default | Limits | What it does |
|---|---|---|---|---|
| `BatchSize` | `int` | 100 | Up to 1,000; not negative | Sets the most deliveries that one `Publish` call receives |
| `Timeout` | `time.Duration` | 1 minute | Not negative | Bounds each `Publish` call |

### `outbox.Message`

For `Outbox.Store`. [Godoc](https://pkg.go.dev/github.com/code19m/taskmill/outbox#Message)

| Field | Type | Default | Limits | What it does |
|---|---|---|---|---|
| `Topic` | `string` | Required | Not empty | Names the broker destination, such as a Kafka topic |
| `Key` | `[]byte` | None: any order | — | Orders messages: those with the same topic and key are published one at a time, in the order they were stored |
| `Value` | `[]byte` | Empty | About 750 KiB: see [the outbox's limits](defaults-and-limits.md#outbox) | Is the message body |
| `Headers` | `map[string][]byte` | None | They share the message's size limit | Are passed to the broker with the message |

### `otel.Config`

For `otel.New`. It may be nil. [Godoc](https://pkg.go.dev/github.com/code19m/taskmill/otel#Config)

| Field | Type | Default | Limits | What it does |
|---|---|---|---|---|
| `TracerProvider` | `trace.TracerProvider` | `otel.GetTracerProvider()` | — | Creates the tracer |
| `MeterProvider` | `metric.MeterProvider` | `otel.GetMeterProvider()` | — | Creates the meter |
| `Propagator` | `propagation.TextMapPropagator` | `otel.GetTextMapPropagator()` | — | Carries the trace context in task metadata and message headers. The default carries nothing until your app sets one, such as `propagation.TraceContext{}` |

## See also

- [Defaults and limits](defaults-and-limits.md): fixed timings, and limits that are not fields
- [The `taskmill-ui` command](taskmill-ui.md): its environment variables
- [API on pkg.go.dev](https://pkg.go.dev/github.com/code19m/taskmill): the full contract of every field

**Next:** [Defaults and limits](defaults-and-limits.md)
