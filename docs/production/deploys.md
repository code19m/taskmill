# Deploys

This page shows how to ship a new version without losing work: how workers
stop, when migrations run, and what happens while old and new code run
together.

## Stop workers without losing work

When your platform stops a worker, the worker finishes what it can, and gives
the rest back to the queue
([each step](../guides/workers-and-queues.md#stop-a-worker-cleanly)):

- It stops fetching, and waits up to `DrainTimeout` (20 s by default) for
  its running tasks.
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
3. If tasks often run longer than `DrainTimeout`, raise both, or split the
   work into shorter tasks. See [Run long tasks](../recipes/run-long-tasks.md).

## Run migrations

### Which should I use?

| Situation | What to use | Why |
|---|---|---|
| Most apps | **Call `Migrate` at the start of every process**, before `NewClient` | Safe when many pods run it at the same time: one migrates, and the others wait, then find nothing to do |
| Your deploys change the schema before new code starts, such as a Helm `pre-upgrade` hook or an Argo CD sync wave | A Job that runs your image and calls `Migrate` | The schema is ready before any new pod starts. See [Migrate in a Job](kubernetes.md#migrate-in-a-job) |
| You manage every schema change with goose, Atlas, or Flyway | Apply `taskmill.Migrations` with your tool | One tool for all changes. See [Use your migration tool](../recipes/use-your-migration-tool.md) |

```go
if err := taskmill.Migrate(ctx, pool, &taskmill.MigrateConfig{Logger: logger}); err != nil {
    return err
}
client, err := taskmill.NewClient(ctx, pool, &taskmill.ClientConfig{Logger: logger})
```

- Migrations only go forward. There are no down migrations.
- A migration waits at most 5 seconds for a lock on a busy taskmill table,
  so it never blocks your workers for long. If it gives up, a pod restart or
  the Job's next try runs it again.
- `taskmill-ui` never migrates. Until the schema is migrated, it exits at
  start, and Kubernetes starts it again.

## Run two versions side by side

During a rolling deploy, old and new pods run together. That is safe across
one release: each release's schema still works with the previous release's
code.

| Your code | The schema | Result |
|---|---|---|
| Same release | Same release | Works |
| The previous release | One release newer | Works: old pods keep running during the rollout |
| Newer | Older | `NewClient` fails with `ErrSchemaOutdated`, and `taskmill-ui` exits, until you migrate |
| Two or more releases older | Newer | It starts, but nothing tests it: not supported |

So upgrade one release at a time, or stop all old pods before new ones start.
Rolling back one release is safe too ([Upgrading](upgrading.md)).

**Schedules.** The [maintainer](../concepts/glossary.md#maintainer) runs only
the schedules that it defines itself. So define the same `Schedules` on every
worker that can become the maintainer, and give other workers
`NoMaintainer: true`. Otherwise, your schedules stop silently while such a
worker is the maintainer.

## Add a new kind during a rollout

A worker takes any task from the queues it reads, whatever its kind. If it
has no handler for the kind, it waits up to an hour for a worker that has
one. [Change a task safely](../recipes/change-a-task-safely.md) shows the flow.

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

## See also

- [Kubernetes](kubernetes.md): grace period, probes, and a migration Job
- [Workers and queues](../guides/workers-and-queues.md): which queues a worker reads
- [`Worker.Run`](https://pkg.go.dev/github.com/code19m/taskmill#Worker.Run) and [`Migrate`](https://pkg.go.dev/github.com/code19m/taskmill#Migrate) on pkg.go.dev

**Next:** [Kubernetes](kubernetes.md)
