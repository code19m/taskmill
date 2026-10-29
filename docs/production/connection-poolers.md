# Connection poolers

taskmill works through PgBouncer and through the poolers of managed services.
This page shows the one thing you must set: a direct connection for
notifications.

## Which should I use?

| Situation | What to use | Why |
|---|---|---|
| No pooler, or a pooler in session mode | Nothing to set | Each connection stays yours, so notifications work |
| A pooler in transaction mode, and you can also reach PostgreSQL directly | **The pool through the pooler, and `ListenConfig` on a direct connection** | Recommended. It costs one direct connection per worker |
| A pooler in transaction mode, and no direct access | The pool through the pooler, without `ListenConfig` | It works, but idle workers poll every second, and each worker logs an Error when it starts |

## Why notifications need a direct connection

In *transaction mode*, the pooler gives your server connection to another
client after each transaction. Everything in taskmill works that way, `Migrate`
included, except notifications. A worker waits for them with `LISTEN`, and
that needs a connection that stays yours: a *session*.

taskmill's CI runs every test through PgBouncer in transaction mode.

## Set it up

1. Point the pool at the pooler.
2. Let the pooler keep prepared statements, because pgx prepares its
   statements. Use PgBouncer 1.21 or newer, with `max_prepared_statements`
   set. If your pooler cannot keep them, set pgx's `QueryExecModeExec`.
3. Point `WorkerConfig.ListenConfig` at a direct connection to the primary.
   Create it with `pgx.ParseConfig`.

```go
cfg, err := pgxpool.ParseConfig(os.Getenv("DATABASE_URL")) // through the pooler
if err != nil {
    return err
}
cfg.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeExec // only if the pooler cannot keep prepared statements
pool, err := pgxpool.NewWithConfig(ctx, cfg)
// ...
listen, err := pgx.ParseConfig(os.Getenv("DIRECT_DATABASE_URL")) // straight to PostgreSQL
// ...
worker, err := taskmill.NewWorker(client, &taskmill.WorkerConfig{
    Handlers:     &handlers,
    ListenConfig: listen,
})
```

Each worker then holds one direct connection. Count it in the database's
`max_connections`.

## What happens without a direct connection

When a worker starts, it checks its notification connection. If that
connection goes through a pooler in transaction mode, the worker logs
`notifications disabled; polling only` at Error level. From then on, it
checks its idle queues every `PollInterval` (1 second by default) instead of
waiting for notifications.

Tasks still run. But they can start up to a second late, and idle workers
query the database more often.

## Settings per provider

| Provider | The pool goes through | `ListenConfig` points at |
|---|---|---|
| PgBouncer that you run | PgBouncer, with `pool_mode = transaction` | PostgreSQL itself |
| Amazon RDS and Aurora | RDS Proxy | The instance endpoint, or Aurora's cluster (writer) endpoint |
| Azure Database for PostgreSQL | The built-in PgBouncer, on port 6432 | The server, on port 5432 |
| Supabase | The transaction pooler, on port 6543 | The direct connection |
| Neon | The pooled endpoint, with `-pooler` in its host name | The same endpoint without `-pooler` |
| Google Cloud SQL and AlloyDB | The Auth Proxy, which is not a pooler | Nothing to set: notifications work through it |

> [!NOTE]
> Neon's compute never scales to zero while workers run, because workers
> query the database every few seconds. Plan for a compute that stays on.

## See also

- [Database](database.md): roles, pool size, and failover
- [Count the database connections](kubernetes.md#count-the-database-connections)
- [`WorkerConfig.ListenConfig`](https://pkg.go.dev/github.com/code19m/taskmill#WorkerConfig) on pkg.go.dev

**Next:** [Deploys](deploys.md)
