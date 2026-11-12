# Move a cron job to a schedule

**Situation.** "One server has the crontab line `0 3 * * * /opt/shop/report`,
which builds our nightly report. We want it in the app instead: one run for
all replicas, with retries, and visible in the dashboard."

## The recommended way

Turn the job into a task. Give the task a number of attempts that ends long
before the next run:

```go
type BuildReport struct{}

func (BuildReport) Kind() string { return "report.build" }

func (BuildReport) EnqueueDefaults() taskmill.EnqueueOptions {
    return taskmill.EnqueueOptions{MaxAttempts: 5} // about 15 seconds of retries, then dead
}
```

Copy the five fields of the crontab line into a
[schedule](../concepts/glossary.md#schedule), and name the time zone:

```go
berlin, err := time.LoadLocation("Europe/Berlin") // the time zone that the server's cron used
if err != nil {
    return err
}
taskmill.AddHandler(&handlers, &ReportBuilder{}, &taskmill.HandlerOptions{
    Timeout: 15 * time.Minute, // cron set no time limit. The default here is 1 minute
})
worker, err := taskmill.NewWorker(client, &taskmill.WorkerConfig{
    Handlers: &handlers,
    Schedules: []taskmill.Schedule{
        {Name: "nightly-report", Cron: "0 3 * * *", Location: berlin, Args: BuildReport{}},
    },
})
```

Then switch in three steps:

1. Deploy this to every worker. The crontab line stays for now.
2. On the dashboard's **Schedules** page, choose **Run now**, or call
   `client.RunSchedule(ctx, "nightly-report")`. Check that the task
   completes.
3. Remove the crontab line before its next run.

## What is different from cron

| | cron | A schedule |
|---|---|---|
| Where it runs | On one server | On any worker, once for all of them |
| Time zone | The server's | UTC, unless you set `Location` |
| Time limit | None | `Timeout`: 1 minute by default |
| A failed run | Is over | Is retried, and then becomes a [dead task](../concepts/glossary.md#dead-task) that you can see and retry |
| A run that was due while nothing ran | Never happens | Happens when a worker is back: one run for all the missed times |
| A slow run and the next one | Overlap, unless you use `flock` | Overlap, unless you set a `GroupKey` |
| Output | Mail, or a log file | The returned error is saved with the task. The rest goes to your logs |

The five fields mean the same as in cron, with names such as `MON-FRI` and
`JAN`. As in cron, a line that sets both the day of the month and the day of
the week runs when either matches. taskmill has no `@reboot`, no `@every`,
no seconds, and no `L` or `?`: `NewWorker` returns an error for them.

## Why

- **The [maintainer](../concepts/glossary.md#maintainer) enqueues each run
  once,** however many workers you have. No server is special, and a
  restart loses nothing.
- **A run is an ordinary task.** It has retries, an error history, and a
  place in the dashboard. A dead run is found by the same alert as any
  other dead task.
- **A new schedule starts with its next time.** It doesn't run for times
  before the deploy, so step 1 starts nothing by itself.

## Other options

| Option | When it fits | The cost |
|---|---|---|
| Keep the script, and start it from the handler with `exec.CommandContext` | You can't rewrite the job in Go now | The worker's image must hold the script and its tools |
| Keep cron, and let it only enqueue the task, with the date as `DedupeKey` | Other systems depend on that crontab | Two places to look, and the server stays special |
| A `GroupKey` in `Schedule.Options` | Two runs must never run at the same time | A dead run stops the runs after it ([Run tasks on a schedule](../guides/schedules.md)) |

## Watch out

- **The handler can run twice,** and it can run late: after a retry, or
  after an outage. So don't take the day of the report from the clock, or
  from `t.RunAt`, which a retry moves. `t.ScheduledFor()` returns the time
  that the run is for, on every attempt. After an outage, one run stands for
  all the times that were missed: find the days that are missing in your
  own data, and build those.
- **Give every worker the same `Schedules`.** Only the maintainer runs
  schedules, and only the ones that it defines
  ([Run tasks on a schedule](../guides/schedules.md)).
- **Don't rename the schedule later.** A new name is a new schedule, with
  its own next run.

## See also

- [Run tasks on a schedule](../guides/schedules.md): time zones, missed runs, and overlapping runs
- [Run long tasks](run-long-tasks.md): jobs that take more than a few minutes
- [Do work once](do-work-once.md): make the job safe to run twice
- [`Schedule`](https://pkg.go.dev/github.com/code19m/taskmill#Schedule) and [`Client.RunSchedule`](https://pkg.go.dev/github.com/code19m/taskmill#Client.RunSchedule) on pkg.go.dev

**Next:** [Consume from a broker](consume-from-a-broker.md)
