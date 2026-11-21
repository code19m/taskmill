# Deploys

This page shows how to ship a new version without losing work. It covers
how to split your binary, how workers stop, when migrations run, and what
happens while old and new code run together.

## One binary, two roles

Build one binary, and let its first argument choose the role. The `web` role
only enqueues. The `worker` role runs the handlers. Each role gets its own
Deployment, so workers scale on the queue, not on web traffic:

```go
// run starts one role: shop web, shop worker, or shop migrate.
func run(ctx context.Context, role string) error {
    logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))
    pool, err := pgxpool.New(ctx, os.Getenv("DATABASE_URL")) // each Deployment sets its own pool_max_conns
    if err != nil {
        return err
    }
    defer pool.Close()
    if role == "migrate" { // a Job of its own, before the rollout
        return taskmill.Migrate(ctx, pool, &taskmill.MigrateConfig{Logger: logger})
    }
    client, err := taskmill.NewClient(ctx, pool, &taskmill.ClientConfig{Logger: logger})
    if err != nil {
        return err
    }

    switch role {
    case "web": // enqueues tasks, and runs no worker
        return serveHTTP(ctx, client)
    case "worker": // runs the handlers
        worker, err := taskmill.NewWorker(client, &taskmill.WorkerConfig{Handlers: handlers()})
        if err != nil {
            return err
        }
        go serveHealth(worker) // GET /healthz, for the readiness probe
        return worker.Run(ctx) // returns after SIGTERM, once the running tasks are drained
    default:
        return fmt.Errorf("unknown role %q: use web, worker, or migrate", role)
    }
}
```

`serveHTTP`, `handlers`, and `serveHealth` are your own functions.
[Ready to ship](../tutorial/7-ready-to-ship.md) builds this binary step by
step, and [Kubernetes](kubernetes.md) has the manifests for the `worker` and
`migrate` roles.

## Stop workers without losing work

When your platform stops a worker, the worker finishes what it can, and gives
the rest back to the queue
([each step](../guides/workers-and-queues.md#stop-a-worker-cleanly)):

- It stops fetching, and waits up to `DrainTimeout` (20 s by default) for
  its running tasks.
- If it is the [maintainer](../concepts/glossary.md#maintainer), it gives
  that role up as soon as it stops fetching, so another worker takes over
  right away.
- Then it cancels the tasks that still run. A task that returns nil
  completes. Every other task goes back to its queue without using an
  attempt, and another worker runs it.
- Tasks that it fetched but did not start go back too.

Do three things:

1. End `Run`'s context on `SIGTERM`:

   ```go
   ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
   defer stop()
   // ...
   err = worker.Run(ctx) // returns once running tasks are drained
   ```

2. Give the process a grace period of at least `DrainTimeout` plus 10
   seconds: 30 seconds with the defaults, which is Kubernetes' default too.
   That is the most that `Run` takes to return
   ([each step](../guides/workers-and-queues.md#stop-a-worker-cleanly)).
3. If tasks often run longer than `DrainTimeout`, raise both, or split the
   work into shorter tasks. See [Run long tasks](../recipes/run-long-tasks.md).

## Run migrations

### Which should I use?

| Situation | What to use | Why |
|---|---|---|
| Production | **Run `Migrate` in a job of its own, before the rollout**: a Kubernetes Job, a Helm `pre-upgrade` hook, an Argo CD sync wave, or a step of your pipeline | A migration that builds an index can run for minutes on a large table. At the start of a pod, it would delay every pod. See [Migrate in a Job](kubernetes.md#migrate-in-a-job) |
| Development, or a small schema | `Migrate` at the start of every process, before `NewClient` | No extra step. One process migrates, and the others wait, then find nothing to do |
| You manage every schema change with goose, Atlas, or Flyway | Apply `taskmill.Migrations` with your tool | One tool for all changes. See [Use your own migration tool](../recipes/use-your-migration-tool.md) |

```go
ctx, cancel := context.WithTimeout(ctx, time.Hour) // an index of many tasks takes minutes
defer cancel()
if err := taskmill.Migrate(ctx, pool, &taskmill.MigrateConfig{Logger: logger}); err != nil {
    return err
}
```

- Migrations only go forward. There are no down migrations.
- Pending migrations run in one transaction, up to the next
  [online step](#indexes-are-built-online). If `Migrate` fails, the
  migrations of that transaction are not applied: fix the cause, and run
  it again. It continues after the last migration that it recorded.
- A migration waits at most 5 seconds for a lock on a busy taskmill table,
  so it never blocks your workers for long. If it gives up, the Job's next
  try, or a pod restart, runs it again.
- While one process migrates, the others wait for its lock on the
  `migrations` table, as long as their context allows: they try it again
  at least every second. Give `Migrate` a deadline that leaves time for the
  index builds. A stuck migration then fails, and you see it:
  [`Migrate` fails, or never returns](troubleshooting.md#migrate-fails-or-never-returns).
- To read the schema's version, run
  `SELECT max(version) FROM taskmill.migrations`. When it is lower than
  your code needs, `NewClient` fails with `ErrSchemaOutdated`, and the error
  names both numbers.
- `taskmill-ui` never migrates. Until the schema is migrated, it exits at
  start, and Kubernetes starts it again.

### Indexes are built online

Every migration after version 9 that adds or drops an index is an *online
step*. It uses `CREATE INDEX CONCURRENTLY`, so enqueues, fetches, and
completions continue while the index builds.

- A build reads the table twice. Before that, it waits for the
  transactions that write the table, and then for every older transaction
  in the database: a long transaction anywhere delays it.
- `Migrate` uses a second connection of the pool meanwhile, so the pool
  needs at least 2. It works through PgBouncer in transaction mode.
- A `statement_timeout` of the role that runs `Migrate` must leave time
  for the build.
- A build that fails, or whose `Migrate` stops, leaves an *invalid index*:
  PostgreSQL updates it on every write, and no query uses it. The next
  `Migrate` drops it, and builds it again.

### Migrations that block writes

Migrations 7 and 9 build their indexes inside a transaction. Enqueues,
fetches, and completions wait while they read `tasks`, and the history for
migration 7. Reads continue. A schema from before v0.14.0 has migration 9
pending, and one from before v0.12.0 has migration 7 pending too.

| Migration | Writes wait, for each million rows |
|---|---|
| 7: indexes of creation time, on `tasks` and the history | About 0.5 seconds |
| 9: an index of the heads of FIFO groups, on `tasks` | About 0.1 seconds |

So `Migrate` checks first. If such a migration would read more rows than
`MaxBlockingIndexRows` allows (1,000,000 by default, by PostgreSQL's
estimate), `Migrate` changes nothing, and fails with an error that names
the migration and the rows. Then:

1. Choose a quiet time, when a pause of writes does no harm. Find it with
   the table above.
2. Run `Migrate` as a job of its own, with the limit raised, or removed:

   ```go
   err := taskmill.Migrate(ctx, pool, &taskmill.MigrateConfig{MaxBlockingIndexRows: -1}) // no limit
   ```

3. Roll out the new version.

## Run two versions side by side

During a rolling deploy, old and new pods run together. That is safe across
one release: each release's schema still works with the previous release's
code. [Compatibility](../reference/compatibility.md#old-code-on-a-newer-schema)
has the table of what works.

So upgrade one release at a time, or stop all old pods before new ones start.
Rolling back one release is safe too ([Upgrading](upgrading.md)).

**Schedules.** The [maintainer](../concepts/glossary.md#maintainer) runs only
the schedules that it defines itself. So define the same `Schedules` on every
worker that can become the maintainer, and give other workers
`NoMaintainer: true`. Otherwise, your schedules stop while such a worker is
the maintainer. When the schedules keep differing, the maintainer logs an
error that names itself and the workers whose schedules differ from its own:
[Log messages](../reference/log-messages.md#the-maintainer) says which one
to fix.

## Add a new kind during a rollout

A worker takes any task from the queues it reads, whatever its kind. If the
worker has no handler for the kind, the task waits up to an hour for a worker
that has one. [Change a task safely](../recipes/change-a-task-safely.md) shows
the flow.

- Deploy the workers that handle a new kind before the code that enqueues it.
- Every worker that reads a queue needs a handler for every kind in it.
  Otherwise, give those kinds a queue of their own.
- Never rename a kind, and never change a payload so that old handlers
  cannot decode it: such a task is dead right away, with `invalid_payload`.
  Add a new kind instead, as the same page shows.

> [!WARNING]
> If code enqueues a new kind, and no worker with its handler runs for an
> hour, those tasks become dead. This happens, for example, when the
> workers' rollout fails. Fix the rollout, then retry the tasks: see
> [Recover from an incident](../recipes/recover-from-an-incident.md).

## Run on serverless platforms

Platforms such as AWS Lambda and Google Cloud Run start and stop your code
with the load. Functions can enqueue. A worker must keep renewing its
[leases](../concepts/glossary.md#lease) while it runs, so it needs a CPU
that never pauses:

| What runs there | What to do |
|---|---|
| A function that enqueues | Create the pool and the `Client` once per instance, outside the request handler: `NewClient` checks the schema once per cold start. Give the pool only the connections that one instance's requests use at the same time, such as 2 on Lambda. Many instances open many connections, so put RDS Proxy or PgBouncer in front of the database ([Connection poolers](connection-poolers.md)) |
| A worker that runs all the time | A process that stays up, with its CPU always on: on Cloud Run, instance-based billing, which keeps the CPU allocated, and at least one instance. A worker whose CPU slows down between requests can't renew its leases, so other workers run its tasks again |
| A worker with a time limit: a Kubernetes CronJob, Cloud Run jobs, or one Lambda invocation (15 minutes at most) | Pass `Run` a context with a deadline that leaves the grace period of [step 2 above](#stop-workers-without-losing-work) before the platform's limit. When the deadline comes, the worker stops as it does on `SIGTERM` |

Delayed tasks, retries, and schedules move only while some worker runs:
the [maintainer](../concepts/glossary.md#maintainer) makes them due. With
workers that run on a timetable only, a retry or a schedule waits for the
next run. taskmill has no "run until the queue is empty" mode: give `Run` a
deadline instead.

## See also

- [Kubernetes](kubernetes.md): grace period, probes, and a migration Job
- [Workers and queues](../guides/workers-and-queues.md): which queues a worker reads
- [`Worker.Run`](https://pkg.go.dev/github.com/code19m/taskmill#Worker.Run) and [`Migrate`](https://pkg.go.dev/github.com/code19m/taskmill#Migrate) on pkg.go.dev

**Next:** [Kubernetes](kubernetes.md)
