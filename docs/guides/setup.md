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

- taskmill keeps its tables in one PostgreSQL
  [schema](../concepts/glossary.md#schema), `taskmill` by default.
- `Migrate` creates the schema, or updates it to your taskmill version. Many
  processes can run it at the same time: one applies the changes, and the
  others wait.
- Create one `Client` per process, and share it between goroutines.

> [!TIP]
> Always pass a `Logger`. Without one, taskmill writes no logs, not even
> errors.

## Which should I use?

| Situation | What to use | Why |
|---|---|---|
| Most apps | `Migrate` before `NewClient`, in every process | A deploy needs no extra step |
| Your deploys change the schema before the new code starts | `Migrate` in a Job | See [Deploys](../production/deploys.md) |
| You manage all schema changes with goose, Atlas, or Flyway | `Migrations` | See [Use your migration tool](../recipes/use-your-migration-tool.md) |

Run your workers as the database role that owns taskmill's tables. Every
day, taskmill adds a partition to its history table, and PostgreSQL allows
that only to the owner. See [Database](../production/database.md).

## Choose the schema name

To use another name, set the same `Schema` in `MigrateConfig` and in
`ClientConfig`, such as `billing_tasks`. Use lowercase letters, digits, and
`_`, up to 63 characters.

Tasks in different schemas never see each other. Give each service its own
schema, and let the web servers and workers of one app share it. See
[Split across services](../recipes/split-across-services.md).

## Size the pool

taskmill needs a pgx pool (`*pgxpool.Pool`). Your own code may still use
`database/sql`: see [Use your own transactions](transactions.md).

Use one pool for taskmill and your handlers. Give it the worker's total
`Concurrency`, plus 5 connections. A worker that reads one queue with the
default concurrency of 10 needs 15:

```text
DATABASE_URL=postgres://app@db/shop?pool_max_conns=15
```

pgx's default pool is often too small. If it is, the worker logs a warning
when it starts.

## Why does NewClient fail?

`NewClient` checks the schema and the pool, so a problem shows when your
app starts:

| Error | Why | Fix |
|---|---|---|
| `ErrSchemaOutdated` | The schema is missing, or older than your taskmill version | Run `Migrate` first |
| `ErrInvalidArgument`, about `default_transaction_isolation` | The pool's default isolation level is not READ COMMITTED | Set it for the pool, as below |
| A connection error | The database can't be reached | Let the process exit and restart |

```go
cfg, err := pgxpool.ParseConfig(os.Getenv("DATABASE_URL"))
// ...
cfg.ConnConfig.RuntimeParams["default_transaction_isolation"] = "read committed"
pool, err := pgxpool.NewWithConfig(ctx, cfg)
```

Only taskmill's pool needs READ COMMITTED. Your own transactions may use any
level.

`NewClient` accepts a schema that is newer than your code, so old and new
versions can run side by side during a rolling deploy.
[Compatibility](../reference/compatibility.md) says which versions are
supported together.

## See also

- [Quickstart](../start/quickstart.md): a full program, from nothing to a finished task
- [Database](../production/database.md): roles, connections, and isolation in production
- [`Migrate`](https://pkg.go.dev/github.com/code19m/taskmill#Migrate) and [`NewClient`](https://pkg.go.dev/github.com/code19m/taskmill#NewClient) on pkg.go.dev

**Next:** [Define tasks](define-tasks.md)
