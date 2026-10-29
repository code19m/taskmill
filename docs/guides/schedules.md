# Run tasks on a schedule

A schedule enqueues a task on a cron timetable, such as every night at
03:00. This page shows how to define one, how time zones work, and how to
keep runs from overlapping.

## Which should I use?

| Situation | What to use | Why |
|---|---|---|
| A fixed timetable that you know when you deploy: a nightly report, an hourly cleanup | A `Schedule` | It runs once per time across all workers, survives restarts, and handles time zones |
| A time that depends on your data: a reminder 2 days after each order | A task with `RunAt`, and a [dedupe key](deduplication.md#set-the-window-for-a-task-that-runs-later) | Delayed tasks are rows: millions can wait cheaply |
| A repeating interval per customer, or more often than once a minute | A task that enqueues its own next run with `RunAt` | Schedules are the same for everyone, and run at most once a minute |

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
any other. Its `RunAt` is the time it was scheduled for. In the handler,
use `t.RunAt`, not the clock, to know which day or hour the run is for.

## Write the timetable

`Cron` takes a standard five-field expression: minute, hour, day of month,
month, and day of week. For example, `*/15 * * * 1-5` is every 15 minutes on
weekdays. It also takes the macros `@yearly`, `@annually`, `@monthly`,
`@weekly`, `@daily`, `@midnight`, and `@hourly`. There are no seconds, and
no `@every`.

`NewWorker` returns an error for an expression that never matches, and for
`Options` that set `RunAt`, `DedupeKey`, or `DedupeWindow`: the schedule
sets those itself.

The expression is read on the wall clock of `Location`, as classic cron
does. Without a `Location`, the time is UTC. When clocks go forward and skip
the run time, the run happens right after the jump. When clocks go back and
repeat it, the run happens once.

## How a run happens

Only the [maintainer](../concepts/glossary.md#maintainer) enqueues
scheduled runs, so each run time is enqueued only once, even with many
workers. Then a worker runs the task like any other.

> [!WARNING]
> The maintainer runs only the schedules that **it** defines. Give every
> worker the same `Schedules` list, or set `NoMaintainer` on the workers
> without it. Otherwise your schedules stop while such a worker leads, and
> nothing tells you.

If no maintainer runs at the run time, the run waits. When a maintainer
that defines the schedule starts again, it enqueues **one** run for all the
times it missed. That run's `RunAt` is the first time it missed. A paused
schedule works the same way when you resume it.

## Keep runs from overlapping

Each run time enqueues its own task, whatever the last run is doing. A slow
run, or one that retries for hours, can still run when the next one starts.
Make the handler safe to overlap: each run works on the period of its own
`t.RunAt`. For frequent schedules, also lower `MaxAttempts`.

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
> until you retry or delete it. See
> [Fix a halted group](ordering.md#fix-a-halted-group), and alert on halted
> groups.

## Pause, run now, or remove a schedule

| You want to… | Do this |
|---|---|
| See every schedule, with its next and last run | `client.Schedules`, or the dashboard's **Schedules** page |
| Stop a schedule for a while | `client.PauseSchedule`, then `ResumeSchedule` |
| Run it once now, such as after a fix | `client.RunSchedule`. The run comes within about a second, and the timetable doesn't change. It fails for a paused schedule |
| Change its time, time zone, or task | Change the definition and deploy |
| Remove it, or rename it | Change the code. The old schedule no longer runs. A maintainer that defines other schedules deletes it from the database after a day |

## See also

- [Workers and queues](workers-and-queues.md#which-worker-does-the-shared-work): the maintainer, and `NoMaintainer`
- [Autoscaling](../production/autoscaling.md): keep schedules running when workers scale to zero
- [`Schedule`](https://pkg.go.dev/github.com/code19m/taskmill#Schedule) on pkg.go.dev

**Next:** [Publish events with the outbox](outbox.md)
