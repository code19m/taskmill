# 0007. Count stuck work with the queue statistics

- Status: Proposed
- Date: 2026-10-29

## Context

Lag is how long a queue's oldest due task has waited. It is the alert for a
backlog, but two kinds of stuck work stay out of it:

- **Halted FIFO groups.** When a group's head becomes dead, the tasks behind
  it wait as `pending`, which no lag counts. No metric counted halted groups:
  the monitoring guide told operators to poll `Client.Groups` from code.
- **Tasks that keep failing.** A failed task waits for its next retry, so it
  is not due. During a broker outage, the first outbox message of each key
  is `retryable` with a run time minutes ahead, and the messages behind it
  are `pending`. Lag stays near zero for hours, until the messages die after
  about a month. The same holds for any queue whose tasks keep failing: with
  the default policy, a task retries for about 13 hours.

An operator who sets the documented alerts is paged for neither.
[ADR 0006](0006-health-and-scaling-read-paths.md) applies: gauges and
scalers read the maintainer's statistics, never the tables, so watching them
adds no load.

## Decision

**The maintainer counts both with the counts it already makes,** every 10
seconds, and stores them in `queue_stats`, in two nullable columns
(migration `0004_stuck_work.sql`). `Client.Queues` returns them as
`QueueInfo.HaltedGroups` and `QueueInfo.MostFailures`, and the `otel`
module reports them as `taskmill.queue.halted_groups` and
`taskmill.queue.most_failures`.

**Halted groups are counted per queue,** through the partial index of halted
groups, up to 10,000, like the other counts. The count is stored with the
queue's dead count: the dead task that halts a group is always in the
group's queue.

**"Keeps failing" is measured in failures in a row:** the most times in a
row that one retryable task of the queue has failed, stored with its
retryable count. It is the highest attempt number among those tasks. Every
attempt of a retryable task failed: a success completes the task, a snooze
gives its attempt back, and an operator's retry starts again from 0. So the
attempt number says how many times in a row the task failed. It
ignores tasks that fail now and then, and it catches both a whole queue that
fails and one task that fails among many that succeed. With the default
retry policy and the outbox's, the 10th failure comes about 8.5 minutes
after the first, so 10 is the documented threshold.

**The tasks that failed most are read first, and the cost stays
bounded.** `attempt` is not in the index that the count reads, the one on
run time, so the count reads it with one index probe per task, for two sets
of retryable tasks:

- **The 10,000 that retry last,** among the counted ones. A task's wait
  before its retry grows with its failures, so the tasks that failed most
  retry last. In an outage with steady traffic, more than 10,000 tasks can
  wait to retry. The new failures, with few attempts and short waits, retry
  first, so reading the tasks that retry first would miss the old ones
  exactly when the alert matters. The count reads the run time index
  backward, from the last retryable task it counted, so no task past the
  counted ones, and no scheduled task after them, is read.
- **Each queue's task that retries last,** which the count already knows.
  So a queue whose tasks all retry before 10,000 others is still read: the
  global cap cannot starve it.

Measured on PostgreSQL 18 with JIT off, with 1.7 million tasks, 150,000 of
them retryable: the count took about 15 ms longer, about 40 ms instead of
25 ms. With few retryable tasks it costs nothing more.

Run time stops telling tasks apart once their waits reach the policy's
maximum, such as 1 hour by default and 5 minutes for the outbox. All the
tasks at that wait have failed at least the times it takes to get there,
about 10 for the outbox and 13 by default. So the value still reaches the
documented threshold of 10, but it may miss the single task that failed
most.
Exact values would need `attempt` in an index, which means a new index on
`tasks`.

We considered a cap per queue. The run time index does not lead with the
queue, so reading one queue's last tasks means reading through every other
queue's tasks, or sorting the counted ones by queue in memory: up to 100,000
rows every 10 seconds, which spill to disk at the default `work_mem`.

### Options we rejected

| Option | Why not |
|---|---|
| How long the oldest failing task has failed | taskmill stores no time when a task started to fail. `attempted_at` is the latest attempt. The errors keep the last 20 entries, which an outbox message passes in about an hour, and reading them means reading large JSON. `created_at` includes a task's delay and its wait in a backlog, so a task delayed by a day that fails once would look like it failed for a day. Storing the time needs a column on `tasks` that every failure writes, and an index to find the oldest. `Migrate` cannot build an index on a large table inside its transaction. |
| A documented alert on the retryable count, such as "above 0 for 15 minutes" | It fires in any busy queue with a steady trickle of failures that a retry fixes, and it cannot tell one task that failed 10 times from 10 tasks that failed once. |
| The error rate from the `otel` middleware | It needs the middleware in every worker, and misses one task that keeps failing among many that succeed: for example one outbox message that the broker refuses, which blocks its key for a month. |
| Count halted groups when `Client.Queues` reads | Every process that reports the gauges would read `groups` at each collection, against ADR 0006. |

## Consequences

- Operators alert on two more gauges, with rules in
  [Monitoring](../production/monitoring.md). The advice to poll
  `Client.Groups` goes.
- Most failures count failures, not time, so the threshold depends on the
  retry policy. A kind whose tasks die after fewer attempts never reaches
  it, and the alert on new dead tasks covers it.
- A task leaves the value while it runs its next attempt. Alerts use the
  maximum over some minutes, or they flap.
- With more than 10,000 tasks at the longest wait, the value can read lower
  than the most failures of any one task, though not below the failures it
  takes to reach that wait.
- A maintainer of an older release leaves both columns `NULL`, and they read
  0, for example during a rolling deploy. Telling "not counted" apart would
  need another field in `QueueInfo` for a gap of minutes.
