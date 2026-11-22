# Dashboard pages

This page says what each part of the dashboard's pages shows. To run the
dashboard, protect it, and act on tasks in it, read
[The dashboard](../guides/dashboard.md).

## Overview

The cards at the top count the tasks of every queue, and each leads to their
list. **Waiting** counts the tasks that wait for their time, for a retry, or
behind another task of their FIFO group: the scheduled, retryable, and
pending tasks.

The **Activity** card shows the last 1, 6, or 24 hours, in steps of 1, 5,
or 15 minutes. To read the values of one step, point at a chart, or press
and drag on a touch screen: a note shows them, with the step's time on your
clock.

- **From the keyboard,** a chart is a slider that a screen reader reads step
  by step. The arrow keys, Home, and End move it, and Escape hides the
  values. While a chart has the focus, the page waits with its updates.
- **A chart with nothing to draw** says why behind a hint, an "i" button.
  In a browser without popovers, its text is on the page.

The **Throughput** dial reads like this:

| Part of the dial | What it shows |
|---|---|
| The needle and the number | The attempts a minute, completed and failed together, in the newest step whose count is complete. In the longer periods, that is the mean of 5 or 15 minutes |
| The scale | From 0 to the smallest round number that holds the highest rate of the period: 1, 2, or 5 times a power of ten, and at least 10 |
| A dash in place of the number | No worker recorded anything in that step |

So the needle reads against the busiest step of the period that you chose:
near the top of the scale, the workers are as busy as at their peak.
Workers write their counts every 10 seconds. So the dial reads a step 15
seconds after it ends, not sooner, and the **Attempts** chart draws it then.

At the end of the top bar, "Counted 4s ago" says when the
[maintainer](../concepts/glossary.md#maintainer) last counted the tasks,
which it does every 10 seconds. After a minute, it turns amber: check that a
worker runs as the maintainer ([Monitoring](../production/monitoring.md)
has an alert). Such times keep counting each second, by the server's clock.
A count that ends in "+" stopped at its cap, as its tooltip says.

## Tasks

A task list is in the order of
[`Client.List`](https://pkg.go.dev/github.com/code19m/taskmill#Client.List).
So on the **Dead** page, the task that died last is at the top.

| Part of the list | What it shows |
|---|---|
| An arrow in the header | The column that orders the list: **Finished** on a list of only dead or canceled tasks, **Created** on a list that names a queue and no FIFO group or that has a range of creation times, and **ID** on any other |
| The row under the list | More of the list: click anywhere in it |
| A row at the top | The way to the list's start, on a list that starts after other tasks |
| The state | An icon in the state's color. The two states that need you also have their name in color: `dead` in red, and `retryable` in amber |
| A badge beside the state | A priority that is not normal, without a state color: `high` is bolder than `low`, and `critical` is filled |

## A task

A banner at the top of a task's page says how the task stands in its
[FIFO group](../concepts/glossary.md#fifo-group):

| The task | The banner |
|---|---|
| Dead, and its group waits for it | "This task halts its FIFO group", with how many tasks wait behind it, or that a task added to the group waits, and a **View group** button, which lists the group's [unfinished](../concepts/glossary.md#finished) and dead tasks |
| Pending | "Waits behind" the task that its group runs first, with that task's state |
| Its group could not be read | That it could not be read. The page shows the task, and its next update tries again |

## Schedules

The **Cron** column says each timetable in words, such as "Weekdays at
09:00", above the cron expression. The times are in the schedule's time
zone, which stands beside the expression. The words are there only when
they say exactly what the expression does, in a few words. An expression
with both day fields set, or with more than three times, shows alone.
[Write the timetable](../guides/schedules.md#write-the-timetable) says how
an expression is read, and what happens when the clocks change.

## Workers

| Column | What it shows |
|---|---|
| **Worker** | The worker's ID without its host name. The tooltip has the whole ID |
| **Host** | The worker's host name |
| **Queues** and **Kinds** | The worker's first queues, with a count of the rest, and a count of its kinds. Press a count to list them all, each a link to its tasks: that needs a browser with popovers |
| **Checked in** | The worker's last [check-in](../concepts/glossary.md#check-in). A yellow dot and the word "stale" mark a worker that made none for longer than its `LeaseDuration`: it has stopped, or cannot reach the database |

*schedules differ* marks a worker whose schedule names differ from the
maintainer's: press it to see which.

## See also

- [The dashboard](../guides/dashboard.md): run it, protect it, and act on tasks
- [The `taskmill-ui` command](taskmill-ui.md): its settings and its routes
- [Fixed timings and limits](defaults-and-limits.md#dashboard): the dashboard's limits, such as its page size

**Next:** [Log messages](log-messages.md)
