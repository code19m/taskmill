# Dependencies and modules

taskmill's core module has one dependency, pgx. Its OpenTelemetry
integration, and the examples for GORM and ent, are modules of their own.
This page says why, which options we rejected, and what a pull request that
adds a dependency must do.

## The rule: the standard library first

taskmill is a library, so every module that it requires becomes part of
its users' builds. Each one adds code to audit, supply-chain risk, possible
version conflicts, and upgrades to track. Many companies review every new
dependency before they adopt a library. And the standard library covers
most of what a task queue needs: `database/sql`, `log/slog`, `context`,
`encoding/json`, `time`, `sync`, and `testing`.

| Rule | How it works |
|---|---|
| The standard library first | A third-party module comes in only when the standard library cannot do the job well, and this page says why |
| The allowlist is enforced | The `depguard` linter runs in strict mode on the list in [.golangci.yml](../../.golangci.yml): the standard library, this module, and pgx. The `otel` module has [lists of its own](#the-opentelemetry-module) there, and so has the module of the [ORM examples](#the-module-of-the-orm-examples) |
| An integration that needs a third-party module is a Go module of its own | Users who do not need it never download it. The core offers plain extension points, such as middleware, that such modules build on |
| Tests use only the standard library | No assertion frameworks and no container libraries. The tests connect to the PostgreSQL server that an environment variable names, and CI provides one for each supported version |
| Development tools stay out of `go.mod` | Linters and scanners are pinned in the [Makefile](../../Makefile) and installed into `./bin`. A `tool` directive in `go.mod` would add their requirements to every user's module graph, where they can raise the versions of modules that users share with them |

Users get a small module graph that is quick to audit. The rule has two
costs, and we accept both:

- We write and maintain some small helpers that a dependency would
  provide, such as the UUIDs of the outbox's messages. That is fine while
  they stay small and well tested.
- Dependabot does not update the development tools. The project team bumps
  them by hand in the Makefile: see [Releasing](releasing.md).

### How to add a dependency

Add an entry to this page, in the same pull request as the change to the
allowlist. The entry says what the module is for, why the standard library
cannot do the job, and which options you rejected. The two entries below
are the models.

## pgx, the PostgreSQL driver

The engine needs a PostgreSQL driver with a connection pool, with `LISTEN`
and `NOTIFY` to wake workers fast, and with efficient array parameters: the
batch statements use them to send many tasks in one round trip.
Applications must also be able to enqueue inside their own transactions,
whether they use pgx directly, or `database/sql` through a library such as
bun or sqlx.

| Option | Why, or why not |
|---|---|
| **pgx v5**, used directly by the engine, with its `database/sql` driver (`pgx/v5/stdlib`) for applications on `database/sql` | Chosen. The engine gets notifications, binary encoding, and batching, and both kinds of application can enqueue in their own transactions |
| lib/pq through `database/sql` | It is in maintenance mode, and needs wrappers for array parameters. `database/sql` has no API for notifications |
| `database/sql` only, with no driver chosen by taskmill | The engine could not use notifications, or pgx's binary protocol and batching |

The engine uses pgx's connection pool, `pgxpool`. Measured with
`go list -deps`, pgx brings five more modules into the build:
`github.com/jackc/pgpassfile`, `github.com/jackc/pgservicefile`, and
`github.com/jackc/puddle/v2`, and `golang.org/x/text` and
`golang.org/x/sync` from the Go project.

**A transaction passes through one of two adapters:** `taskmill.Pgx` for
pgx, and `taskmill.SQL` for `database/sql`. `pgx.Tx` and `*sql.Tx` have no
method in common, so no single interface can accept both.

| Option | Why, or why not |
|---|---|
| **Two adapters that return an `Executor`** | Chosen. They cost one function call where a transaction is passed, and keep every other type simple. `Executor` is sealed: only taskmill implements it, so its methods can change without breaking anyone |
| A client that is generic over the transaction type, `Client[TTx]` | The type parameter would spread through every type that touches a transaction, including the tasks given to handlers |

What follows from pgx:

- **An application on `database/sql` must use pgx's driver.** lib/pq is not
  supported. A library that rewrites query placeholders, such as bun, must
  pass its underlying `*sql.Tx`.
- **Both adapters are tested against PostgreSQL,** with the array
  parameters of the batch statements.
- **pgx types are part of taskmill's API.** `PgxQuerier` uses pgx's result
  types, and `NewClient`, `Migrate`, and `WorkerConfig.ListenConfig` take
  `*pgxpool.Pool` and `*pgx.ConnConfig`. So a new major version of pgx
  needs a new major version of taskmill.

## The OpenTelemetry module

Applications expect traces and metrics through OpenTelemetry, and a task
queue sits in the middle of their traces: a request enqueues a task, and a
worker runs it seconds or hours later. OpenTelemetry's modules are
third-party, so by the rule the integration is a module of its own:
`github.com/code19m/taskmill/otel`, in the `otel/` directory. It has its
own `go.mod`, and its own allowlists. The code that its users build may
import OpenTelemetry's API, `go.opentelemetry.io/otel`, but not the SDK:
the application chooses that. Only the module's tests may import the SDK,
to read what the module reports, and pgx, to connect to PostgreSQL.

An application that does not use OpenTelemetry downloads none of it. One
that does gets standard metrics, and one trace from a request through its
tasks and outbox messages to the consumers behind the broker.
[Metrics and traces](../reference/metrics.md) lists what the module
reports.

### Where the module lives, and how it gets its versions

- **In this repository, not in one of its own.** A building block in the
  core and its first user are then reviewed, tested, and documented
  together, and `make check` and CI lint, test, and scan every module. A
  repository of its own would let the module release on its own schedule.
  But every change that needs a new building block would take two pull
  requests, and a core release before the module could use it.
- **A `replace` line, not a workspace.** The module's `go.mod` replaces
  the core with `../`, so every command in the repository builds the module
  against the core at the same commit. Users of the module ignore that
  line. A workspace (`go.work`) would do the same, but `go mod tidy`
  ignores workspaces, so it would fail whenever the module waits for a core
  release.
- **A check that builds the module as its users do.** The `replace` line
  hides the core version that `go.mod` requires. So
  `make otel-release-check` copies the module, drops the line, and builds
  and tests the copy against that version. Until the version is released,
  the check only says so. When the module is released, it must pass.
- **Its own version.** Releases are tagged `otel/vX.Y.Z`. Each one requires
  the oldest core release that has the building blocks it uses. When the
  module starts to use a core change, its `go.mod` requires the next core
  version in the same pull request, and the check fails if it does not. The
  core is then released first: [Releasing](releasing.md#release-the-otel-module)
  has the steps.
- **Below v1.** OpenTelemetry's conventions for messaging are in
  development. The module's Go API follows the core's
  [compatibility](../reference/compatibility.md) promise. The names of its
  spans, attributes, and metrics follow the conventions, and may change in
  a minor release that its changelog announces.

The cost: releasing takes one more step when the module needs new core
features, and the repository has one more module to keep tidy, linted, and
scanned.

### What the core provides for it

The core provides only what the module cannot build on its own. Each
building block has no dependency, and another integration can use it too,
such as metrics for another system.

| Building block | Why the module needs it |
|---|---|
| `ClientConfig.EnqueueMiddleware` | A producer span must wrap the enqueue: the module writes the trace context into each task's metadata before the tasks are stored, and must see the result to end the span. The middleware wraps every enqueue: alone, in batches, inside the caller's transaction, and by schedules. The worker's `Middleware` covers the attempts |
| `QueueInfo.Lag` | The counts are capped, so on a large backlog they cannot say how far behind a queue is. The maintainer stores each queue's oldest due task with its counts: see [Queue statistics and health](schema-statistics.md) |
| `outbox.Delivery.Metadata` | The publisher must see the metadata of the task that holds a message, and with it the trace context. Without it, the trace stops at the outbox |
| `ErrorType() string` on taskmill's errors | Each error names its class, such as `invalid_argument`, `panic`, or `timeout`. OpenTelemetry's `semconv.ErrorType` reads that method |

### What the module instruments

The module follows OpenTelemetry's semantic conventions for messaging.
These are the choices that the conventions leave open, and what taskmill
adds:

| Decision | Why |
|---|---|
| One producer span wraps each enqueue call, also a batch. There is no `create` span per task | The conventions allow one per task of a batch. But an outbox batch of hundreds of messages would cost hundreds of spans, and the batch's span already gives each task a creation context |
| The parent of an attempt's consumer span is the producer span | A task's work then shows in the trace of the request that enqueued it. The conventions allow this parent for a single message. The span also links to the producer span, as the conventions ask of every consumer span, and to a span that is already in the worker's context |
| A wrapper around the user's publisher traces each outbox delivery, and the user names the broker, such as `kafka` | The delivery's span is a child of the span that stored the message, and its context goes into the message's headers. So the consumers behind the broker continue the same trace. taskmill does not know the broker |
| Metric attributes are bounded: queue, kind, state, and error type | A task ID or an attempt number would make the number of time series grow without limit |
| The gauge `taskmill.stats.age` says how long ago the maintainer counted | A queue that was empty at the last count shows no lag. So this gauge is the alert for "no maintainer runs" |
| A call of its own, `ObserveQueues`, turns the queue gauges on | They describe the whole schema, not the process. One value, made by `otel.New`, turns everything else on: its middleware goes into the client's and the worker's configuration |
| Every process that calls `ObserveQueues` reports the same values, and dashboards aggregate them with `max`, not `sum` | Only the maintainer measures the queues, and it stores what it finds. Each process reads the stored values at each collection, in one round trip. We rejected gauges that only the maintainer's process reports: the maintainer moves between processes, and a gauge that moves with it breaks its series |

One limit follows from the consumer span's parent. The trace of a task
that retries for hours stays open for hours, and a backend that limits how
long a trace may last can split it.

## The module of the ORM examples

[Use your own transactions](../guides/transactions.md) shows how to enqueue
in a GORM or an ent transaction, and a program that compiles checks each of
those rows. GORM and ent are third-party modules, so by the rule the
examples are a module of their own: `examples/orms`, with its own `go.mod`.
Nobody imports it, and it is never released.

| Decision | Why |
|---|---|
| A module of its own, not examples in the core | In the core, the two ORMs would add 18 modules to every user's module graph, such as ent's Atlas and HCL |
| `make check` and CI lint, test, and scan it, as they do the core | An example that no longer works would mislead its readers. Its tests run each ORM's transaction against PostgreSQL, with a commit that fails after the enqueue |
| A `replace` line to the core next to it | The examples test the core of the same commit, as the `otel` module does. They are never released, so they need no release check |
| ent's generated client is committed | The module then builds with `go build` alone, and readers see the code that ent writes. `go generate ./ent` in the module writes it again, with the `sql/execquery` feature that the example needs |
| A `depguard` list of its own | It may import GORM and ent, and nothing else new |
| Dependabot updates it together with the other modules | It shares pgx with the core, so a pgx update must change every module in one pull request |

The cost: about 3,300 lines of generated code in the repository, and one
more module for CI to download and check.

## See also

- [How taskmill is built](README.md#the-product-shape): the packages, and which one uses which
- [Logs, traces, and metrics](../guides/observability.md): how to set the `otel` module up
- [Use your own transactions](../guides/transactions.md): the two adapters, for users
- [`EnqueueMiddleware` on pkg.go.dev](https://pkg.go.dev/github.com/code19m/taskmill#EnqueueMiddleware): its contract

**Next:** [Benchmarks](benchmarks.md)
