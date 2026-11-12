# Run tasks on a schedule

A schedule enqueues a task on a cron timetable, such as every night at
03:00. This page shows how to define one, how time zones work, and how to
keep runs from overlapping.

## Which should I use?

| Situation | What to use | Why |
|---|---|---|
| A fixed timetable that you know when you deploy: a nightly report, an hourly cleanup | A `Schedule` | Each run happens once, however many workers you have. It survives restarts, and handles time zones |
| A time that depends on your data: a reminder 2 days after each order | A task with `RunAt`, and a [dedupe key](deduplication.md#set-the-window-for-a-task-that-runs-later) | Delayed tasks are rows: millions can wait cheaply |
| A repeating time per customer: a summary at 09:00 in each customer's time zone | A due time per customer in your own table, and one `Schedule` that enqueues what is due: [Run a task at each customer's local time](../recipes/run-at-each-customers-local-time.md) | Schedules are the same for everyone. A new customer is then only a change to a row |
| More often than once a minute, or a next time that depends on what the last run did | A task that enqueues its own next run with `RunAt` | A schedule runs at most once a minute. But the chain ends silently when one of its tasks becomes dead or is canceled |

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
any other. In the handler, use `t.ScheduledFor()` to know which day or hour
the run is for:

```go
func (h *ReportBuilder) Handle(ctx context.Context, t *taskmill.Task[BuildReport]) error {
    night := t.ScheduledFor() // 03:00 Berlin time, on the night the run is for
    if night.IsZero() {
        night = t.RunAt // the task carries no such time: see below
    }
    return h.build(ctx, night)
}
```

Don't use the clock, or `t.RunAt` alone, for this. A run that fails at
03:00 may succeed hours later, after its retries. It must still build the
report of its own night.

| Value | What it tells you |
|---|---|
| `t.ScheduledFor()` | The time the run is for, on the clock of the schedule's `Location`. It is the same on every attempt |
| `t.RunAt` | The same time, but on the first attempt only. A retry, a snooze, or a lost worker moves it to the time the task runs again |
| The clock | When this attempt runs |

taskmill keeps this time in the task's
[metadata](../concepts/glossary.md#metadata), as `taskmill.scheduled_for`,
next to the schedule's name, as `taskmill.schedule`. So the dashboard shows
which schedule made a task.

`ScheduledFor` returns the zero time in two cases. So fall back to
`t.RunAt`, as the handler above does:

- **No schedule enqueued the task.** To test a handler as a scheduled run,
  enqueue its task with a time such as `2026-11-12T03:00:00+01:00` in its
  `Metadata`, under `taskmill.MetadataScheduledFor`.
- **An older maintainer enqueued the run.** While you upgrade, a worker of
  the older taskmill release may still enqueue runs, and it stores no such
  time.

## Write the timetable

`Cron` takes a standard five-field expression: minute, hour, day of month,
month, and day of week. For example, `*/15 * * * 1-5` is every 15 minutes on
weekdays.

- Months and weekdays also have names: `JAN` to `DEC`, and `SUN` to `SAT`.
  So `*/15 * * * MON-FRI` means the same.
- The macros `@yearly`, `@annually`, `@monthly`, `@weekly`, `@daily`,
  `@midnight`, and `@hourly` work too. There are no seconds, and no
  `@every`.
- **Take care when you set both day fields.** A run then happens when
  **either** one matches, as in classic cron. So `0 3 1 * 1` runs on the 1st
  of each month and on every Monday, not only on a 1st that is a Monday.

`NewWorker` returns an error for an expression that never matches, and for
`Options` that set `RunAt`, `DedupeKey`, or `DedupeWindow`: the schedule
sets those itself.

The expression is read on the wall clock of `Location`. Without a
`Location`, the time is UTC. Each time that the expression names on that
clock runs once, also when the clocks change:

| When the clocks… | What happens | Example, in Berlin |
|---|---|---|
| Go forward, and skip an hour | Each skipped time runs once, as long after the jump as the time is into the skipped hour | `0,30 2 * * *` runs at 03:00 and at 03:30 |
| Go back, and repeat an hour | A time in the repeated hour runs the first time the clock shows it, and not the second | `30 2 * * *` runs once. `*/15 * * * *` makes no run during the second pass of that hour, where classic cron keeps running |

## How a run happens

Only the [maintainer](../concepts/glossary.md#maintainer) enqueues
scheduled runs, so each run time is enqueued only once, even with many
workers. Then a worker runs the task like any other.

> [!WARNING]
> The maintainer runs only the schedules that **it** defines. Give every
> worker the same `Schedules` list, or set `NoMaintainer` on the workers
> without it. Otherwise your schedules stop while such a worker is the
> maintainer, and no error or log line tells you.

If no maintainer runs at the run time, the run waits. When a maintainer
that defines the schedule starts again, it enqueues **one** run for all the
times it missed. That run is for the first time it missed: its
`ScheduledFor()` returns that time. A paused schedule works the same way
when you resume it.

## Keep runs from overlapping

Each run time enqueues its own task, whatever the last run is doing. A slow
run, or one that retries for hours, can still run when the next one starts.
Make the handler safe to overlap: each run works on the period of its own
`t.ScheduledFor()`, which no retry changes. For frequent schedules, also
lower `MaxAttempts`.

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
> [Fix a halted group](ordering.md#fix-a-halted-group), and
> [alert on halted groups](../production/monitoring.md).

## Pause, run now, or remove a schedule

| You want to… | Do this |
|---|---|
| See every schedule, with its next and last run | `client.Schedules`, or the dashboard's **Schedules** page |
| Stop a schedule for a while | `client.PauseSchedule`, then `ResumeSchedule` |
| Run it once now, such as after a fix | `client.RunSchedule`. The run comes within about a second, and the timetable doesn't change. It fails for a paused schedule |
| Change its time, time zone, or task | Change the definition and deploy |
| Remove it, or rename it | Change the code. The old schedule no longer runs. A maintainer that defines other schedules deletes it from the database after a day |

A run that you ask for with `RunSchedule` is for the time of your request:
its `ScheduledFor()` returns that time. Close to a regular run, you get both
runs. While a run of the schedule is already due, a request adds none: that
run starts within about a second anyway.

## See also

- [Workers and queues](workers-and-queues.md#which-worker-does-the-shared-work): the maintainer, and `NoMaintainer`
- [Autoscaling](../production/autoscaling.md): keep schedules running when workers scale to zero
- [`Schedule`](https://pkg.go.dev/github.com/code19m/taskmill#Schedule) on pkg.go.dev

**Next:** [Publish events with the outbox](outbox.md)
