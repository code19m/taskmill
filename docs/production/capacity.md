# Capacity planning

This page helps you estimate what taskmill needs before you go live: how
many workers, how many connections, and how much disk. The results are
estimates. Measure your own system once it runs.

## Estimate the workers

Your handlers set the number, not taskmill. Count the tasks that run at the
same time at your peak, and divide by one worker's
[`Concurrency`](../concepts/glossary.md#concurrency):

```text
tasks per second × seconds per task = tasks running at the same time
tasks running at the same time ÷ Concurrency = workers
```

| Example | Value |
|---|---|
| Tasks at the peak | 200 a second |
| Time per task | 0.5 seconds |
| Tasks running at the same time | 200 × 0.5 = 100 |
| `Concurrency` of one worker | 25 |
| Workers | 100 ÷ 25 = 4. Add one, so that a rollout or a lost pod does not build a backlog |

- **Prefer a few workers with a higher `Concurrency`** to many small ones on
  the same queue ([Autoscaling](autoscaling.md#size-each-worker)).
- **More workers do not help a queue with a limit.** A queue's `MaxRunning`
  caps its running tasks across all workers, and a
  [FIFO group](../concepts/glossary.md#fifo-group) runs one task at a time.
- **A worker itself is small:** about 17 MiB of memory when idle, plus about
  8 KiB per running task. Your handlers need the rest
  ([Performance](../reference/performance.md#resource-use)).

## Estimate the connections

Each worker opens its total `Concurrency` plus 5 connections, plus 1 for
notifications. The 4 workers above open 4 × 31 = 124. Count the peak of a
rollout too:
[Count the database connections](kubernetes.md#count-the-database-connections).

## Estimate the disk

A task takes about 200 bytes, plus its payload and its metadata, indexes
included. A task with a small payload of about 90 bytes takes about 300
bytes, waiting or in history.

| Where | What it holds | Estimate |
|---|---|---|
| The history table | Completed tasks, for `HistoryRetention` (7 days) plus up to 1 day | Completed tasks per day × 8 days × the size of a task |
| The `tasks` table | Tasks that wait or run. Dead and canceled tasks stay for `DeadRetention` (30 days) | Your largest backlog × the size of a task |
| The write-ahead log (WAL) | Every change, until PostgreSQL archives or recycles it | About 0.8 KiB per task from fetch to history, with a small payload. The enqueue and larger payloads add to it |

For example, 1 million tasks a day at 300 bytes need about 2.4 GB of
history: 1,000,000 × 8 × 300 bytes.

To see how much the history uses now, ask PostgreSQL for the size of its
partitions:

```sql
SELECT pg_size_pretty(sum(pg_total_relation_size(inhrelid)))
FROM pg_inherits WHERE inhparent = 'taskmill.task_history'::regclass;
```

To use less disk, shorten `HistoryRetention`, or set `DiscardCompleted` for
busy kinds whose history nobody reads:
[Choose how long finished tasks are kept](../guides/operations-api.md#choose-how-long-finished-tasks-are-kept).
The [maintainer](../concepts/glossary.md#maintainer) drops old history a
whole day at a time, so retention itself costs the database little.

## Know what an idle system costs

taskmill never leaves the database fully idle:

| Who | What it runs while nothing happens |
|---|---|
| The maintainer | A few small statements every second: it keeps its role, wakes queues, and looks for due tasks, lost leases, and due schedules |
| Each worker | A few small statements every 10 seconds, and one fetch per queue per minute |

So a serverless database that stops when it is idle, such as Neon, never
stops while a worker runs. Plan for a compute that stays on.

When many schemas share one database, PostgreSQL wakes every listening
connection for each notification. 100 schemas with 500 workers cost the
database about a quarter of a CPU
([Performance](../reference/performance.md#resource-use)).

## Know the limits of these numbers

The sizes and speeds on this page come from runs on a laptop, with small
payloads and handlers that do nothing
([Performance](../reference/performance.md#where-the-numbers-come-from)).
Use them to choose a starting size. Then watch lag, the database's CPU, and
its disk, and correct the size.

## See also

- [Autoscaling](autoscaling.md): let KEDA add and remove workers
- [Performance](../reference/performance.md): how fast and how light taskmill is
- [Defaults and limits](../reference/defaults-and-limits.md): retention and every other default

**Next:** [Monitoring](monitoring.md)
