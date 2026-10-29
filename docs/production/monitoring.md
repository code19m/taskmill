# Monitoring

This page lists what to alert on, and the catches in each signal. The
numbers come from the `otel` module's gauges, from taskmill's logs, and from
PostgreSQL.

## What to alert on

| Signal | When to alert | What to do |
|---|---|---|
| [Lag](../concepts/glossary.md#lag): `taskmill_queue_lag_seconds` | Above what your users accept, for 5 minutes | [Tasks wait longer and longer](troubleshooting.md#tasks-wait-longer-and-longer) |
| Statistics age: `taskmill_stats_age_seconds` | Over 60 seconds | No [maintainer](../concepts/glossary.md#maintainer) runs: [Retries, delayed tasks, or schedules don't run](troubleshooting.md#retries-delayed-tasks-or-schedules-dont-run) |
| New dead tasks: the log line `attempt failed; the task is dead`, or a dead count that grew | Any | Read the task's errors, fix the cause, and retry: [Recover from an incident](../recipes/recover-from-an-incident.md) |
| [Halted FIFO groups](../concepts/glossary.md#halted-group): `taskmill_queue_halted_groups` | Above 0 | [Tasks wait in `pending`](troubleshooting.md#tasks-wait-in-pending) |
| Tasks that keep failing: `taskmill_queue_failed_attempts` | 10 or more, in the last 15 minutes | [Tasks keep failing, and lag stays low](troubleshooting.md#tasks-keep-failing-and-lag-stays-low) |
| Any Error line in taskmill's logs | Any | Each one needs a person: [Log messages](../reference/log-messages.md) |
| The Warn line `rescued tasks whose workers lost their leases` | When no crash, outage, or killed pod explains it | [A task ran twice](troubleshooting.md#a-task-ran-twice) |
| Snapshot ages: the queries in [Database](database.md#keep-long-transactions-from-slowing-the-queue) | Growing for 10 minutes | End the long transaction, or drop the unused replication slot |
| Worker pods not ready | For 5 minutes | [Kubernetes marks workers as not ready](troubleshooting.md#kubernetes-marks-workers-as-not-ready) |

## Rules for Prometheus

The first five alerts, as Prometheus rules. Change the thresholds to fit
your app.

```yaml
groups:
  - name: taskmill
    rules:
      - alert: TaskmillBacklog
        expr: max by (messaging_destination_name) (taskmill_queue_lag_seconds) > 300
        for: 5m
      - alert: TaskmillNoMaintainer
        expr: max(taskmill_stats_age_seconds) > 60
        for: 1m
      - alert: TaskmillNewDeadTasks
        expr: |
          max by (messaging_destination_name) (taskmill_queue_tasks{taskmill_task_state="dead"})
            > max by (messaging_destination_name) (taskmill_queue_tasks{taskmill_task_state="dead"} offset 15m)
      - alert: TaskmillHaltedGroups
        expr: max by (messaging_destination_name) (taskmill_queue_halted_groups) > 0
      - alert: TaskmillTasksKeepFailing
        expr: max by (messaging_destination_name) (max_over_time(taskmill_queue_failed_attempts[15m])) >= 10
```

## Know the catches

- **Counts stop at 10,000** per queue and state, and so do halted groups per
  queue. Scheduled and retryable tasks also stop at 100,000 for all queues
  together. Above that, a count
  shows "at least": `taskmill_queue_capped` is 1, and the dashboard shows
  "10,000+". So alert on lag for backlogs: lag has no cap.
- **A paused queue's lag keeps growing.** Its tasks wait on purpose, but the
  lag alert fires. Silence it while you pause a queue.
- **A "dead > 0" alert stays red for a month.** A dead task stays until
  someone retries or deletes it, or until retention deletes it after 30
  days. So alert on *new* dead tasks. The log line is the surest signal: the
  dead count also stops at 10,000.
- **Lag misses work that waits for a person or for a retry.** Tasks behind
  a halted group wait as `pending`, and a task that keeps failing waits for
  its next retry. Neither counts in lag, so a broker outage can last hours
  with lag near zero. The halted-groups and failed-attempts alerts cover
  them.
- **`taskmill_queue_failed_attempts` counts attempts, not time.** With the
  default retry policy, and the outbox's, the 10th attempt fails about 9
  minutes after the first. With a slower policy it takes longer, so choose
  the threshold from yours. A kind whose tasks die after fewer attempts
  never reaches it: the dead-task alert covers that.
- **A failing task drops out of `taskmill_queue_failed_attempts` while it
  runs its next attempt.** So the rule takes the maximum over 15 minutes.
  Without it, the alert flaps.
- **A halted group's alert stays on until someone acts.** That is on
  purpose: the group's tasks wait until you retry or delete its dead task.
- **Every process reports the same values** for the whole schema. Aggregate
  the gauges with `max`, never with `sum`.
- **Lag can read up to 10 seconds high.** Set thresholds above that, and use
  a `for` duration.
- **`taskmill_stats_age_seconds` is missing until a maintainer counts for the
  first time.** If no maintainer ever ran, its alert stays silent.
- **During an upgrade, halted groups and failed attempts read 0** while the
  maintainer still runs a release that doesn't count them.

## Logs: which levels matter

Pass your `*slog.Logger` to `ClientConfig.Logger`: the client and its
workers use it. Also pass it to `MigrateConfig.Logger` and `ui.Config.Logger`.
Without a logger, taskmill writes no logs at all.

| Level | It means | What you do |
|---|---|---|
| Error | A person must act | Page someone |
| Warn | Something failed, and taskmill recovers by itself | Look if it repeats |
| Info | A worker starts or stops, the maintainer changes, a migration runs, or someone acts in the dashboard | Keep it, for audits |
| Debug | Single tasks, such as each retry | Turn it on only to debug |

When an operation keeps failing, such as fetching tasks while the database is
down, taskmill does not log every try. It logs `operation failed` (Warn) when
the problem starts, `operation still failing` (Error) at most once a minute
while it lasts, and `operation recovered` (Info) when it ends.

`taskmill-ui` logs text lines to standard error, at Info level. You cannot
change the format or the level.

## See also

- [Log messages](../reference/log-messages.md): every line, and what to do
- [Metrics](../reference/metrics.md): every span, metric, and gauge
- [Observability](../guides/observability.md): how to set up logs, traces, and metrics
- [`otel.Instrumentation.ObserveQueues`](https://pkg.go.dev/github.com/code19m/taskmill/otel#Instrumentation.ObserveQueues) on pkg.go.dev

**Next:** [Security](security.md)
