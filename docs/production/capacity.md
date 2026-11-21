# Capacity planning

This page helps you estimate what taskmill needs before you go live: how
many workers, how many connections, and how much disk. The results are
estimates. Measure your own system once it runs, and watch for the signs
that you outgrow taskmill.

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
- **A worker itself is small**
  ([Performance](../reference/performance.md#resource-use) has its memory).
  Your handlers need the rest.

## Estimate the connections

Each worker opens its pool, sized as
[Size the connection pool](database.md#size-the-connection-pool) says, plus
1 connection for notifications. With a pool of 30 each, the 4 workers above
open 4 × 31 = 124. Count the peak of a rollout too:
[Count the database connections](kubernetes.md#count-the-database-connections).

## Estimate the disk

A task takes about 300 bytes, plus its payload and its metadata, indexes
included. A task with a small payload of about 100 bytes takes about 420
bytes while it waits, and about 390 in history. The `tasks` table leaves a
fifth of each page free, so that PostgreSQL can write each change of a task
in the same page, which costs less. The history fills its pages.

| Where | What it holds | Estimate |
|---|---|---|
| The history table | Completed tasks, for `HistoryRetention` (7 days) plus up to 1 day | Completed tasks per day × 8 days × the size of a task |
| The `tasks` table | Tasks that wait or run. Dead and canceled tasks stay for `DeadRetention` (30 days) | Your largest backlog × the size of a task |
| The write-ahead log (WAL) | Every change, until PostgreSQL archives or recycles it | About 1 KiB per task from fetch to history, with a small payload. The enqueue and larger payloads add to it |

For example, 1 million tasks a day at 390 bytes need about 3.1 GB of
history: 1,000,000 × 8 × 390 bytes.

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
| Each worker | A few small statements every 10 seconds, one fetch per queue per minute, and a statement every 2 to 3 seconds that asks for the maintainer's role, unless the worker sets `NoMaintainer`. It writes nothing while another worker has the role |

So a serverless database that stops when it is idle, such as Neon, never
stops while a worker runs. Plan for a compute that stays on.

When many schemas share one database, PostgreSQL wakes every listening
connection for each notification. 100 schemas with 500 workers cost the
database about 0.15 of a CPU
([benchmarks](../design/benchmarks.md#resource-use)).

## Know the limits of these numbers

The sizes and speeds on this page come from benchmark runs on 4 CPUs
shared with the database, with small payloads and handlers that do nothing
([Performance](../reference/performance.md#where-the-numbers-come-from)).
Use them to choose a starting size. Then watch lag, the database's CPU, and
its disk, and correct the size.

## Know when you outgrow taskmill

taskmill fits while one PostgreSQL database carries your tasks well. These
signs say that it no longer does:

| Sign | What you see |
|---|---|
| Your rates near the measured limits | Enqueues or runs stay near the rates in [Performance](../reference/performance.md#throughput), and more workers no longer cut the [lag](../concepts/glossary.md#lag) |
| The queues hurt your app's database | taskmill's statements lead the database's CPU or WAL, or your app's queries slow down while the queues are busy |
| Many services consume the same events | Each producer enqueues into the schema of every consumer, and must know them all |
| Producers write from other regions | Each enqueue waits a round trip to the primary ([Run in several regions](backups-and-failover.md#run-in-several-regions)) |

First try the cheaper fixes: fewer, larger workers on a busy queue,
`EnqueueMany` for bulk work, and `DiscardCompleted` for busy kinds whose
history nobody reads. If the signs stay, move to a broker one kind at a
time, with no task lost:

1. **Publish the kind's work with the [outbox](../guides/outbox.md)**
   instead of enqueueing it, in the same transaction. Consumers of the
   broker take the work over.
2. **Drain the kind:** keep its handler until no unfinished task of the kind
   is left, as [Remove a kind](../recipes/change-a-task-safely.md#remove-a-kind)
   says.
3. **Repeat for each kind.** In the end, the schema holds only the outbox's
   messages, and the workers run only its
   [relay](../concepts/glossary.md#relay).

## See also

- [Autoscaling](autoscaling.md): let KEDA add and remove workers
- [Performance](../reference/performance.md): how fast and how light taskmill is
- [`Settings`](https://pkg.go.dev/github.com/code19m/taskmill#Settings) on pkg.go.dev: the retention settings, with their defaults and limits

**Next:** [Monitoring](monitoring.md)
