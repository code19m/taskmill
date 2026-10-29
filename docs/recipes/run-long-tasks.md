# Run long tasks

**Situation.** "Our report task takes 5 to 10 minutes. With the defaults, it
fails after a minute, and every deploy seems to start it over."

## Which should I use?

| Your tasks take | What to set | Why |
|---|---|---|
| Up to a minute | Nothing: the defaults fit | A deploy waits about 20 seconds for running tasks. A task cut off then starts over, which is cheap for short tasks |
| A few minutes | A longer `Timeout`, `DrainTimeout`, and grace period | Deploys and scale-downs wait for most tasks to finish |
| Longer than you want a deploy to wait | Smaller tasks | A deploy then loses at most one small piece |

## The recommended way

Set the handler's `Timeout` a little above the slowest normal run. Raise the
worker's `DrainTimeout`, so that most runs finish during a deploy:

```go
taskmill.AddHandler(&handlers, &ReportBuilder{}, &taskmill.HandlerOptions{
    Timeout: 15 * time.Minute, // a little above the slowest normal run
})
worker, err := taskmill.NewWorker(client, &taskmill.WorkerConfig{
    Handlers:     &handlers,
    DrainTimeout: 5 * time.Minute, // so most runs finish during a deploy
})
```

Then give the process at least `DrainTimeout` plus 10 seconds to stop. In
Kubernetes, that is `terminationGracePeriodSeconds: 310` for this worker.

## Why

- **`Timeout` stops a stuck attempt,** and frees its place in the worker.
  It is 1 minute by default, and you can't turn it off. When it passes, the
  handler's context is canceled. A returned error is a failed attempt, and
  `nil` still completes the task.
- **The lease doesn't limit how long a task runs.** The worker renews the
  [lease](../concepts/glossary.md#lease) every few seconds while the task
  runs.
- **A deploy and a scale-down stop a worker the same way.** It waits up to
  `DrainTimeout`, then cancels the tasks still running and gives them back.
  A task given back uses no attempt, and starts over on another worker. See
  [Stop a worker cleanly](../guides/workers-and-queues.md#stop-a-worker-cleanly).

## Split work that takes hours

Process one page of work per task. Each task enqueues the next page in the
same transaction that completes it:

```go
func (e *Exporter) Handle(ctx context.Context, t *taskmill.Task[ExportOrders]) error {
    lastID, err := e.exportPage(ctx, t.Args.AfterID) // 1,000 orders: about a minute
    if err != nil || lastID == 0 {
        return err // nil when no orders are left: the export is done
    }
    // The next page exists only if this one completes.
    return pgx.BeginFunc(ctx, e.pool, func(tx pgx.Tx) error {
        if _, err := e.client.EnqueueTx(ctx, taskmill.Pgx(tx), ExportOrders{AfterID: lastID}, nil); err != nil {
            return err
        }
        return t.CompleteTx(ctx, taskmill.Pgx(tx))
    })
}
```

A deploy then waits for one page at most, and a failure retries one page,
not the whole export. Make each page safe to run twice
([Do work once](do-work-once.md)).

## Other options

| Option | When it fits | The cost |
|---|---|---|
| A `DrainTimeout` as long as the longest run | Runs take up to half an hour, and you deploy rarely | Every deploy and scale-down waits that long |
| Save progress when the context's cause is `ErrShutdown`, then return an error | The work can continue from saved progress | You store the progress yourself. The task goes back without using an attempt |
| Accept restarts | The work is cheap to redo | Wasted work at each deploy |
| A queue of its own for long tasks | Long tasks must not take the places that short tasks need | One more queue to run and watch |

## Watch out

- **Respect the context.** A handler that ignores its canceled context keeps
  running after the worker gave its task back. Then another worker may run
  the same task at the same time.
- **Keep the grace period at least `DrainTimeout` plus 10 seconds.** If the
  platform kills the worker sooner, its tasks wait until their leases end,
  and each counts as a failed attempt.
- **Scale-downs drain too.** An autoscaler that removes a worker stops its
  long tasks after `DrainTimeout` ([Autoscaling](../production/autoscaling.md)).

## See also

- [Workers and queues](../guides/workers-and-queues.md): how a worker stops
- [Kubernetes](../production/kubernetes.md): the grace period and probes
- [`HandlerOptions`](https://pkg.go.dev/github.com/code19m/taskmill#HandlerOptions) and [`WorkerConfig`](https://pkg.go.dev/github.com/code19m/taskmill#WorkerConfig) on pkg.go.dev

**Next:** [Recover from an incident](recover-from-an-incident.md)
