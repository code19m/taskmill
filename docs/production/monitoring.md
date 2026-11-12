# Monitoring

This page lists what to alert on, gives Prometheus rules to copy, and names
the limits of each signal. The numbers come from the `otel` module's gauges
and counters, from taskmill's logs, and from PostgreSQL.

## What to alert on

| Signal | When to alert | What to do |
|---|---|---|
| [Lag](../concepts/glossary.md#lag): `taskmill_queue_lag_seconds` | Above what your users accept, for 5 minutes | [Tasks wait longer and longer](troubleshooting.md#tasks-wait-longer-and-longer) |
| Statistics age: `taskmill_stats_age_seconds` | Over 60 seconds | No [maintainer](../concepts/glossary.md#maintainer) runs, or it cannot count: [Retries, delayed tasks, or schedules don't run](troubleshooting.md#retries-delayed-tasks-or-schedules-dont-run) |
| The gauges are missing | For 5 minutes | No process reports them, so the alerts above see nothing: [Kubernetes marks workers as not ready](troubleshooting.md#kubernetes-marks-workers-as-not-ready) |
| New dead tasks: the log line `attempt failed; the task is dead`, or a dead count that grew | Any | Read the task's errors, fix the cause, and retry: [Recover from an incident](../recipes/recover-from-an-incident.md) |
| [Halted FIFO groups](../concepts/glossary.md#halted-group): `taskmill_queue_halted_groups` | Above 0 | [Tasks wait in `pending`](troubleshooting.md#tasks-wait-in-pending) |
| Tasks that keep failing: `taskmill_queue_most_failures` | 10 or more, in the last 15 minutes | [Tasks keep failing, and lag stays low](troubleshooting.md#tasks-keep-failing-and-lag-stays-low) |
| Failed enqueues: `messaging_client_sent_messages_total` with an `error_type` | More than 5% of the tasks, for 5 minutes | [Enqueue calls fail](troubleshooting.md#enqueue-calls-fail) |
| An Error line in taskmill's logs | Any, except [the two that can be expected](#logs-which-levels-matter) | Each one needs a person: [Log messages](../reference/log-messages.md) |
| The Warn line `rescued tasks whose workers lost their leases`: the maintainer [rescued](../concepts/glossary.md#rescue) the tasks of a lost worker | When no crash, outage, or killed pod explains it | [A task ran twice](troubleshooting.md#a-task-ran-twice) |
| A transaction that blocks vacuum: the queries in [Database](database.md#keep-long-transactions-from-slowing-the-queue) | Open for 10 minutes, or a slot's age that keeps growing | [Throughput falls over hours](troubleshooting.md#throughput-falls-over-hours) |
| Worker pods not ready | For 5 minutes | [Kubernetes marks workers as not ready](troubleshooting.md#kubernetes-marks-workers-as-not-ready) |

## Rules for Prometheus

These are the alerts above as Prometheus rules. Before you load them:

- **Change `job="shop-worker"`** to the Prometheus job that reports your
  gauges. Each service has its own
  [schema](../concepts/glossary.md#schema), but the gauges do not name it,
  and every service has a `default` queue. Without the selector, a rule
  mixes all your services. The enqueue rule selects every job of the
  service, because each process counts its own enqueues.
- **Change the thresholds** to fit your app.
- **Check the last two rules.** They read metrics of kube-state-metrics and
  postgres_exporter, which taskmill does not test.

```yaml
groups:
  - name: taskmill
    rules:
      - alert: TaskmillBacklog
        expr: max by (job, messaging_destination_name) (taskmill_queue_lag_seconds{job="shop-worker"}) > 300
        for: 5m
        labels: {severity: page}
        annotations:
          summary: 'Queue {{ $labels.messaging_destination_name }}: the oldest task has waited over 5 minutes'
          runbook_url: https://github.com/code19m/taskmill/blob/main/docs/production/troubleshooting.md#tasks-wait-longer-and-longer
      - alert: TaskmillNoMaintainer
        expr: max by (job) (taskmill_stats_age_seconds{job="shop-worker"}) > 60
        for: 1m
        labels: {severity: page}
        annotations:
          summary: No maintainer has counted the queues for over a minute
          runbook_url: https://github.com/code19m/taskmill/blob/main/docs/production/troubleshooting.md#retries-delayed-tasks-or-schedules-dont-run
      - alert: TaskmillGaugesMissing
        expr: absent(taskmill_stats_age_seconds{job="shop-worker"})
        for: 5m
        labels: {severity: page}
        annotations:
          summary: No process reports the taskmill gauges, so the other rules see nothing
          runbook_url: https://github.com/code19m/taskmill/blob/main/docs/production/troubleshooting.md#kubernetes-marks-workers-as-not-ready
      - alert: TaskmillNewDeadTasks
        expr: |
          max by (job, messaging_destination_name) (taskmill_queue_tasks{job="shop-worker", taskmill_task_state="dead"})
            > max by (job, messaging_destination_name) (taskmill_queue_tasks{job="shop-worker", taskmill_task_state="dead"} offset 15m)
        labels: {severity: ticket}
        annotations:
          summary: 'Queue {{ $labels.messaging_destination_name }} has new dead tasks'
          runbook_url: https://github.com/code19m/taskmill/blob/main/docs/recipes/recover-from-an-incident.md
      - alert: TaskmillHaltedGroups
        expr: max by (job, messaging_destination_name) (taskmill_queue_halted_groups{job="shop-worker"}) > 0
        labels: {severity: ticket}
        annotations:
          summary: 'Queue {{ $labels.messaging_destination_name }}: a dead task halts {{ $value }} FIFO groups'
          runbook_url: https://github.com/code19m/taskmill/blob/main/docs/production/troubleshooting.md#tasks-wait-in-pending
      - alert: TaskmillTasksKeepFailing
        expr: max by (job, messaging_destination_name) (max_over_time(taskmill_queue_most_failures{job="shop-worker"}[15m])) >= 10
        labels: {severity: ticket}
        annotations:
          summary: 'Queue {{ $labels.messaging_destination_name }}: a task has failed {{ $value }} times in a row'
          runbook_url: https://github.com/code19m/taskmill/blob/main/docs/production/troubleshooting.md#tasks-keep-failing-and-lag-stays-low
      - alert: TaskmillEnqueueFailures
        expr: |
          sum(rate(messaging_client_sent_messages_total{job=~"shop-.+", messaging_system="taskmill", error_type!=""}[5m]))
            / sum(rate(messaging_client_sent_messages_total{job=~"shop-.+", messaging_system="taskmill"}[5m])) > 0.05
        for: 5m
        labels: {severity: page}
        annotations:
          summary: More than 5% of the tasks that the shop enqueues fail to enqueue
          runbook_url: https://github.com/code19m/taskmill/blob/main/docs/production/troubleshooting.md#enqueue-calls-fail
      - alert: TaskmillWorkersNotReady # needs kube-state-metrics
        expr: kube_deployment_status_replicas_unavailable{namespace="shop", deployment="worker"} > 0
        for: 5m
        labels: {severity: page}
        annotations:
          summary: '{{ $value }} worker pods are not ready'
          runbook_url: https://github.com/code19m/taskmill/blob/main/docs/production/troubleshooting.md#kubernetes-marks-workers-as-not-ready
      - alert: TaskmillLongTransaction # needs postgres_exporter
        expr: max(pg_stat_activity_max_tx_duration{datname="shop"}) > 600
        for: 5m
        labels: {severity: ticket}
        annotations:
          summary: A transaction has been open for over 10 minutes, and can keep vacuum from cleaning the tasks table
          runbook_url: https://github.com/code19m/taskmill/blob/main/docs/production/troubleshooting.md#throughput-falls-over-hours
```

## Know the limits of each signal

- **The gauges exist only while a process reports them.** They are missing
  when every reporting process is down, or cannot read the database. A
  threshold on a missing gauge never fires: the `TaskmillGaugesMissing` rule
  covers that. Report the gauges from a process that stays up.
- **`taskmill_stats_age_seconds` is missing until a maintainer counts for the
  first time.** The same rule covers a maintainer that never ran.
- **The statistics age compares two clocks:** the app's clock, and the time
  that the database stored. A pod whose clock is a minute ahead reports an
  age that is a minute too high, and `max` picks that pod.
- **Every process reports the same values** for the whole schema. Aggregate
  the gauges with `max`, never with `sum`.
- **Counts stop at 10,000** per queue and state, and so do halted groups per
  queue. Scheduled and retryable tasks also stop at 100,000 for all queues
  together. Above that, a count shows "at least": `taskmill_queue_capped` is
  1, and the dashboard shows "10,000+". So alert on lag for backlogs: lag
  has no cap.
- **Lag comes from the last count, which can be 10 seconds old.** A task
  that finished after that count still counts, so lag can be too high for up
  to 10 seconds. Set thresholds above 10 seconds, and use a `for` duration.
- **A paused queue's lag keeps growing.** Its tasks wait on purpose, but the
  lag alert fires. Silence it while you pause a queue.
- **Lag misses work that waits for a person or for a retry.** Tasks behind
  a halted group wait as `pending`, and a task that keeps failing waits for
  its next retry. Neither counts in lag, so a broker outage can last hours
  with lag near zero. The halted-groups and most-failures alerts cover
  them.
- **A "dead > 0" alert stays red for a month.** A dead task stays until
  someone retries or deletes it, or until retention deletes it after 30
  days. So alert on *new* dead tasks.
- **The dead-count rule can miss new dead tasks.** The count stops at
  10,000, and it can fall while new tasks die, such as during a bulk retry.
  The log line `attempt failed; the task is dead` is the most reliable
  signal.
- **`taskmill_queue_most_failures` counts failures in a row, not time.**
  With the default retry policy, and the outbox's, a task fails the 10th
  time about 9 minutes after the first. With a slower policy it takes
  longer, so choose the threshold from yours. A kind whose tasks die after
  fewer attempts never reaches it: the dead-task alert covers that.
- **A failing task does not count in `taskmill_queue_most_failures` while it
  runs its next attempt.** So the rule takes the maximum over 15 minutes.
  Without that, the alert turns on and off.
- **A halted group's alert stays on until someone acts.** That is on
  purpose: the group's tasks wait until you retry or delete its dead task.
- **Failed enqueues are counted only where the `otel` module's
  `EnqueueMiddleware` runs.** taskmill logs nothing for a failed enqueue:
  the call returns the error to your code. The counter counts tasks, not
  calls.

## Logs: which levels matter

Pass your `*slog.Logger` to `ClientConfig.Logger`: the client and its
workers use it. Also pass it to `MigrateConfig.Logger` and `ui.Config.Logger`.
Without a logger, taskmill writes no logs at all.

Alert a person on every Error line. Look at Warn lines when they repeat.
Keep Info lines if you need a record of what was done in the dashboard.
[Levels](../reference/log-messages.md#levels) says what each level means.

Two Error lines can be expected. Silence them only in these cases:

| Error line | Expected when |
|---|---|
| `operation still failing` for `apply retention`, with `detach` and `timeout` in its `error` | A long transaction uses the history, such as a `pg_dump` backup. The line stops when that transaction ends |
| `notifications disabled; polling only` | A worker starts behind a pooler in transaction mode, and you chose not to set `ListenConfig` ([Connection poolers](connection-poolers.md)) |

When an operation keeps failing, such as fetching tasks while the database
is down, taskmill does not log every try.
[Operations that repeat](../reference/log-messages.md#operations-that-repeat)
shows the three lines that it logs.

`taskmill-ui` logs text lines to standard error, at Info level. Set
`TASKMILL_UI_LOG_FORMAT=json` for JSON lines. You cannot change the level.
It exports no metrics: the queue gauges come from your apps
([The dashboard](../guides/dashboard.md)).

## See also

- [Log messages](../reference/log-messages.md): every line, and what to do
- [Metrics and traces](../reference/metrics.md): every span, metric, and gauge
- [Logs, traces, and metrics](../guides/observability.md): how to set up logs, traces, and metrics
- [`otel.Instrumentation.ObserveQueues`](https://pkg.go.dev/github.com/code19m/taskmill/otel#Instrumentation.ObserveQueues) on pkg.go.dev

**Next:** [Security](security.md)
