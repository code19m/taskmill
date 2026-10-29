# Set up the database and client

Create taskmill's tables and connect a client. Every process that uses
taskmill does this once, when it starts.

## Quick example

```go
pool, err := pgxpool.New(ctx, os.Getenv("DATABASE_URL"))
if err != nil {
    return err
}
defer pool.Close()

if err := taskmill.Migrate(ctx, pool, nil); err != nil {
    return err
}
client, err := taskmill.NewClient(ctx, pool, &taskmill.ClientConfig{Logger: logger})
if err != nil {
    return err
}
```

Create one `Client` per process and share it. It is safe to use from many
goroutines.

> [!TIP]
> Always pass a `Logger`. Without one, taskmill writes no logs at all, not
> even errors.

## How it works

- taskmill keeps its tables in one PostgreSQL
  [schema](../concepts/glossary.md#schema), called `taskmill` by default.
- `Migrate` creates the schema, or updates it to your taskmill version. If
  the schema is already up to date, it leaves the tables as they are.
- `Migrate` is safe to run from many processes at the same time. One
  process applies the changes. The others wait, then find nothing to do.
- `NewClient` checks the schema and the pool. If something is wrong, it
  fails right away, before your app does any work.

## Run Migrate when each process starts

Call `Migrate` before `NewClient` in every process: web servers, workers,
and tools. Then a deploy needs no extra step. Other ways, for other needs:

- Your deploys change the schema before the new code starts: run `Migrate`
  in a Job. See [Deploys](../production/deploys.md).
- You manage all schema changes with goose, Atlas, or Flyway: see
  [Use your migration tool](../recipes/use-your-migration-tool.md).

Run your workers as the database role that owns taskmill's tables. Every
day, taskmill adds a part (a partition) to its history table, and
PostgreSQL allows that only to the owner. See
[Database](../production/database.md).

## Choose the schema name

Set the same `Schema` in `MigrateConfig` and in `ClientConfig`:

```go
err := taskmill.Migrate(ctx, pool, &taskmill.MigrateConfig{Schema: "billing_tasks"})
// ...
client, err := taskmill.NewClient(ctx, pool, &taskmill.ClientConfig{Schema: "billing_tasks"})
```

- Use lowercase letters, digits, and `_`, up to 63 characters. The name
  must match `^[a-z_][a-z0-9_]{0,62}$`.
- Tasks in different schemas never see each other.
- Give each service its own schema. All processes of one app, such as its
  web servers and its workers, share one schema. See
  [Split across services](../recipes/split-across-services.md).

## Size the pool

taskmill needs a pgx pool (`*pgxpool.Pool`). `Migrate` and `NewClient`
accept nothing else, and workers use the client's pool. Your own code may
still use `database/sql`: see [Use your own transactions](transactions.md).

Use one pool for taskmill and your handlers. Give it at least the worker's
total `Concurrency`, plus 5 connections. A worker that reads one queue with
the default concurrency of 10 needs 15:

```text
DATABASE_URL=postgres://app@db/shop?pool_max_conns=15
```

pgx's default pool is often too small: the larger of 4 and the number of
CPUs. If the pool is too small, the worker logs a warning when it starts.
Count the connections for all your pods in
[Database](../production/database.md).

## Why does NewClient fail?

| Error | Why | Fix |
|---|---|---|
| `ErrSchemaOutdated` | The schema is missing, or older than your taskmill version | Run `Migrate` first |
| `ErrInvalidArgument`, about `default_transaction_isolation` | The pool's default isolation level is not READ COMMITTED | Set it for the pool, as below |
| A connection error | The database is not reachable | Let the process exit and restart. `NewClient` needs the database when it starts |

To fix the isolation level for taskmill's pool:

```go
cfg, err := pgxpool.ParseConfig(os.Getenv("DATABASE_URL"))
if err != nil {
    return err
}
cfg.ConnConfig.RuntimeParams["default_transaction_isolation"] = "read committed"
pool, err := pgxpool.NewWithConfig(ctx, cfg)
```

Only taskmill's pool needs READ COMMITTED. Your own transactions may use
any level: see [Use your own transactions](transactions.md).

`NewClient` accepts a schema that is newer than your code. So during a
rolling deploy, old and new versions can run side by side.
[Compatibility](../reference/compatibility.md) says which versions are
supported together.

## See also

- [Quickstart](../start/quickstart.md): a full program, from nothing to a
  finished task
- [Database](../production/database.md): roles, connections, and isolation
  in production
- [`Migrate`](https://pkg.go.dev/github.com/code19m/taskmill#Migrate) and
  [`NewClient`](https://pkg.go.dev/github.com/code19m/taskmill#NewClient)
  on pkg.go.dev

**Next:** [Define tasks](define-tasks.md)
