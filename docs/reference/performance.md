# Performance

How fast and how light taskmill is, how it behaves when things fail, and the
limits of these numbers. The [full benchmark results](../design/benchmarks.md)
have every target, the method, and how to run them yourself.

## Where the numbers come from

The numbers below come from runs of v0.15.0's code on GitHub's standard
runner, not on server hardware
([the latest results](../design/benchmarks.md#latest-results)):

- **Machine:** 4 CPUs (Intel Xeon 6975P-C) and 15.3 GiB of memory, shared
  by the database and the benchmark.
- **Database:** PostgreSQL 18.6 in Docker, on the same machine, with
  `shared_buffers = 1GB`.
- **Runs:** shorter than the targets ask: 1 minute instead of 10, and one
  run instead of three. Handlers do nothing.

Read them as rough sizes, not as a promise for your hardware.

## Throughput

| Scenario | Result |
|---|---|
| One worker, with `Concurrency` 64, drains a backlog | About 26,000 tasks a second |
| The same, with tasks in 100,000 FIFO groups | About 19,000 tasks a second |
| End to end: [producers](../concepts/glossary.md#producer) enqueue, 2 workers run and complete, history kept | About 16,000 tasks a second |
| One outbox relay publishes messages on 10,000 keys | About 20,000 messages a second |
| `EnqueueMany` in batches of 100, 8 producers | About 150,000 tasks a second |
| `Enqueue`, one task per transaction, 16 producers | About 11,000 tasks a second: each task is a round trip and a commit |

## Latency

| Scenario | Result |
|---|---|
| An idle worker starts a new task | About 2 ms after the commit (p99: 3 ms) |
| The same, at 10,000 tasks a second | p99: about 47 ms |
| An `Enqueue` call, one task | p99: 0.4 ms |
| A delayed task, retry, or schedule run starts after its time | p99: about 0.3 s; at most 1 s |
| A killed worker's task runs again | After the [lease](../concepts/glossary.md#lease), plus about 2 seconds |
| `Worker.Run` returns when its handlers ignore shutdown | After `DrainTimeout`, plus about 5 seconds |

## Resource use

| What | Result |
|---|---|
| SQL statements per task, end to end | About 0.16: fetches and completions serve many tasks each |
| WAL written per task, from fetch to history | About 1 KiB |
| Size of the `tasks` table during an hour at 10,000 tasks a second | At the end, at most 6% bigger than after 15 minutes, in a laptop run of older code |
| Worker memory | About 15 MiB idle, plus about 12 KiB per running task, of which 8 KiB is the stack of a handler that called the database |
| Database connections | A client uses its pool. A worker uses its pool, plus 1 connection for notifications |
| An idle worker | About one fetch per queue per minute, plus a few small queries |
| Many schemas in one database | 100 schemas with 500 workers cost the database about 0.15 of a CPU, for the [heartbeats](../concepts/glossary.md#heartbeat) of the [maintainers](../concepts/glossary.md#maintainer) |

## Reliability under faults

Every fault test passed. Each one fails on a single lost or repeated task:

- **Workers killed** every second: no task lost or stuck, and each task
  completed exactly once in history. Some tasks ran again after a kill, as
  [at-least-once](../concepts/guarantees.md) allows, and no more often than
  expected.
- **FIFO groups** with failing attempts and killed workers: no task ran
  before the one ahead of it, and nothing ran past a dead task.
- **Dedupe keys** raced by 20 processes: exactly one task per key.
- **PostgreSQL restarted**, cleanly or killed: workers resumed within 2
  seconds of its return, and no task was lost.
- **A worker that lost its connection to the database** for longer than its
  lease: none of its late results counted, and every task completed once.
- **PgBouncer** in transaction mode: every test passes, and CI checks it on
  every pull request.
- **Clocks 5 minutes off** between the app and the database: no task
  started early.

## Honest limits

- **No server run yet.** The targets are set for a database server with 8
  CPUs of its own. In these runs, the database shared 4 CPUs with the
  benchmark.
- **These runs miss three targets.** Enqueueing one task per transaction
  reached about 11,000 tasks a second, against 15,000, and end-to-end
  throughput about 16,000, against 20,000. Only a run on that server can
  judge these two.
- **The third is a real cost: many small workers on one queue.** The test
  ran 50 worker processes that each read the same 100 queues, at 1,000
  tasks a second. The slowest round of the maintainer's work then took
  3.2 seconds, against a target of 1. Each new task wakes every worker of
  its queue, and all but one find nothing.

## What this means for you

| You want to… | Do this | Why |
|---|---|---|
| Enqueue thousands of tasks | `EnqueueMany`, in batches of 100 or more | About 14 times faster than one task per call |
| Run more tasks of a busy queue | Raise `Concurrency` on fewer workers, before you add many small ones | Every new task wakes every worker of its queue |
| Keep fetches fast over hours | Keep long transactions away from the database | They stop vacuum, and fetches slow down. See [Database](../production/database.md) |

## See also

- [Full benchmark results](../design/benchmarks.md): targets, method, and how to run them
- [Fixed timings and limits](defaults-and-limits.md): the exact timings of the workers and the maintainer
- [Autoscaling](../production/autoscaling.md): when to add workers

**Next:** [How taskmill is built](../design/README.md)
