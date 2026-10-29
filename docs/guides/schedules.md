# Run tasks on a schedule

A schedule enqueues a task on a cron timetable, such as every night at 03:00.
This page shows how to define one, and how time zones work. It also shows
how to keep runs from overlapping, and what happens when no worker runs.

## Which should I use?

| Situation | Use | Why |
|---|---|---|
| A fixed timetable that you know when you deploy: a nightly report, an hourly cleanup | A `Schedule` | It runs once per time across all workers, survives restarts, and handles time zones. |
| A time that depends on your data: a reminder 2 days after each order | A task with `RunAt`, and a [dedupe key](deduplication.md#set-the-window-for-a-task-that-runs-later) | Delayed tasks are rows: millions can wait cheaply. |
| A repeating interval per customer, or more often than once a minute | A task that enqueues its own next run with `RunAt` | Schedules are the same for everyone, and run at most once a minute. |

Use a schedule for the timetable of your app. Use delayed tasks for
timetables that come from your data.

## Quick example

Define schedules on the worker:

```go
berlin, err := time.LoadLocation("Europe/Berlin")
if err != nil {
    return err
}
worker, err := taskmill.NewWorker(client, &taskmill.WorkerConfig{
    Handlers: &handlers,
    Schedules: []taskmill.Schedule{
        {Name: "nightly-report", Cron: "0 3 * * *", Location: berlin, Args: BuildReport{}},
        {Name: "clean-up-carts", Cron: "@hourly", Args: CleanUpCarts{}},
    },
})
```

A scheduled run is an ordinary task of its kind, so it needs a handler like
any other. Its `RunAt` is the time it was scheduled for. Use it to know which
day or hour the run is for:

```go
func (b *ReportBuilder) Handle(ctx context.Context, t *taskmill.Task[BuildReport]) error {
    day := t.RunAt.In(b.berlin).AddDate(0, 0, -1) // the report covers the day before the run
    return b.build(ctx, day)
}
```

## Write the timetable

`Cron` takes a standard five-field expression: minute, hour, day of month,
month, and day of week. For example, `0 3 * * *` is every day at 03:00, and
`*/15 * * * 1-5` is every 15 minutes on weekdays. It also takes the macros
`@yearly`, `@annually`, `@monthly`, `@weekly`, `@daily`, `@midnight`, and
`@hourly`. There are no seconds and no `@every`.

`NewWorker` checks every schedule. It returns an error for an expression
that never matches, and for `Options` that set `RunAt`, `DedupeKey`, or
`DedupeWindow`: the schedule sets those itself.

## Time zones and daylight saving time

The expression is read on the wall clock of `Location`, as classic cron does.

| Situation | What happens |
|---|---|
| You set no `Location` | The time is UTC. |
| Clocks go forward and skip the run time | The run happens right after the jump. |
| Clocks go back and repeat the run time | The run happens once. |

## How a run happens

Only the [maintainer](../concepts/glossary.md#maintainer), one worker that
taskmill picks, enqueues scheduled runs. It checks every second. Each run
time is enqueued once, even when you run many workers. Then a worker runs
the task like any other.

The maintainer runs only the schedules that **it** defines. Any worker can
become the maintainer, unless it sets `NoMaintainer`.

> [!WARNING]
> Give every worker the same `Schedules` list, or set `NoMaintainer` on the
> workers without it. If a worker without your schedules becomes the
> maintainer, they stop running, and nothing tells you.

## What if no worker runs at that time?

Then the run waits. When a maintainer that defines the schedule starts
again, it enqueues **one** run for all the times it missed, not one per
missed time. That run's `RunAt` is the first time it missed. A paused
schedule works the same way: when you resume it, the runs it missed become
one run.

## Keep runs from overlapping

Each run time enqueues its own task, whatever the last run is doing. So a
slow run, or one that retries for hours, can still run when the next one
starts.

First, make the handler safe to overlap: work out its period from `t.RunAt`,
not from the clock. For frequent schedules, also lower `MaxAttempts`, so a
failing run gives up before the next ones pile up.

If two runs must **never** run at the same time, give the schedule a
`GroupKey`. Its runs then form a [FIFO group](ordering.md), and run one
after the other:

```go
{
    Name: "nightly-report", Cron: "0 3 * * *", Location: berlin, Args: BuildReport{},
    Options: &taskmill.EnqueueOptions{GroupKey: "nightly-report", MaxAttempts: 5},
},
```

> [!WARNING]
> With a `GroupKey`, a run that becomes dead stops all the runs after it,
> until you retry or delete the dead run. See
> [Fix a halted group](ordering.md#fix-a-halted-group), and alert on halted
> groups.

## Pause, run now, or remove a schedule

| You want to… | Do this |
|---|---|
| See every schedule, with its next and last run | `client.Schedules(ctx)`, or the dashboard's **Schedules** page |
| Stop a schedule for a while | `client.PauseSchedule(ctx, name)`, then `ResumeSchedule` |
| Run it once now, such as after a fix | `client.RunSchedule(ctx, name)`. The run comes within about a second. The regular timetable doesn't change. Resume a paused schedule first. |
| Change its time, time zone, or task | Change the definition and deploy. The next run follows the new definition. |
| Remove it | Remove it from the code. taskmill deletes it from the database after a day without a run or a change. |

The name identifies a schedule. Renaming one creates a new schedule, and
the old one is removed as above.

## See also

- [Workers and queues](workers-and-queues.md): the maintainer, and `NoMaintainer`
- [Autoscaling](../production/autoscaling.md): keep schedules running when workers scale to zero
- [`Schedule`](https://pkg.go.dev/github.com/code19m/taskmill#Schedule) on pkg.go.dev

**Next:** [Publish events with the outbox](outbox.md)
