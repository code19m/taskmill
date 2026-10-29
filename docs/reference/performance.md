# Performance

How fast and how light taskmill is, how it behaves when things fail, and the
limits of these numbers. The [full benchmark results](../design/benchmarks.md)
have every target, the method, and how to run them yourself.

## Where the numbers come from

All numbers below come from one run on a laptop, not on server hardware:

- **Machine:** an Apple M5 laptop, with 10 cores and 16 GiB of memory. Other
  work ran on it at the same time, so results moved by up to half between
  runs.
- **Database:** PostgreSQL 18 in Docker, limited to 4 CPUs, with
  `shared_buffers = 1GB`. Docker on macOS makes disk syncs cheaper than real
  disks do, so numbers bound by commits are higher than on a server.
- **Runs:** shorter than the targets ask: 60 seconds end to end instead of
  10 minutes, and a 1-hour soak instead of 4 hours. Handlers do nothing.

Read them as the shape of the results, not as a promise for your hardware.

## Throughput

| Scenario | Result |
|---|---|
| One worker, with `Concurrency` 64, drains a backlog | About 30,000 tasks a second |
| The same, with tasks in 100,000 FIFO groups | About 26,000 tasks a second |
| End to end: producers enqueue, 2 workers run and complete, history kept | About 17,000 tasks a second |
| One outbox relay publishes messages on 10,000 keys | About 27,000 messages a second |
| `EnqueueMany` in batches of 100, 8 producers | About 190,000 tasks a second |
| `Enqueue`, one task per transaction, 16 producers | 8,000 to 16,000 tasks a second, limited by database round trips |

## Latency

| Scenario | Result |
|---|---|
| An idle worker starts a new task | About 5 ms after the commit (p99: 10 ms) |
| The same, at 10,000 tasks a second | p99: about 45 ms |
| An `Enqueue` call, one task | p99: 0.5 ms |
| A delayed task, retry, or schedule run starts after its time | p99: about 0.4 s; at most 1 s |
| A killed worker's task runs again | After the lease, plus about 2 seconds |
| `Worker.Run` returns when its handlers ignore shutdown | After `DrainTimeout`, plus about 5 seconds |

## Resource use

| What | Result |
|---|---|
| SQL statements per task, end to end | About 0.16: fetches and completions serve many tasks each |
| WAL written per task, from fetch to history | About 0.8 KiB |
| Size of the `tasks` table during an hour at 10,000 tasks a second | At the end, at most 6% bigger than after 15 minutes |
| Worker memory | About 17 MiB idle, plus about 8 KiB per running task |
| Database connections | A client uses its pool. A worker uses its pool, plus 1 connection for notifications |
| An idle worker | About one fetch per queue per minute, plus a few small queries |
| Many schemas in one database | 100 schemas with 500 workers cost the database about a quarter of a CPU, for the maintainers' heartbeats |

## Reliability under faults

Every fault test passed, and each has zero tolerance:

- **Workers killed** every second: no task lost or stuck, and each task
  completed exactly once in history. Some tasks ran again after a kill, as
  [at-least-once](../concepts/guarantees.md) allows, and no more often than
  expected.
- **FIFO groups** with failing attempts and killed workers: no task ran
  before the one ahead of it, and nothing ran past a dead task.
- **Dedupe keys** raced by 20 processes: exactly one task per key.
- **PostgreSQL restarted**, cleanly or killed: workers resumed within a
  second of its return, and no task was lost.
- **A worker cut off from the database** for longer than its lease: none of
  its late results counted, and every task completed once.
- **PgBouncer** in transaction mode: every test passes, and CI checks it on
  every pull request.
- **Clocks 5 minutes off** between the application and the database: no task
  started early.

## Honest limits

- **No server run yet.** The targets are set for an 8-CPU database server.
  These numbers come from a shared laptop.
- **Three targets miss on the laptop:** enqueueing one task per transaction,
  end-to-end throughput, and throughput while 100,000 FIFO groups wait in
  another queue (73% to 90% of normal, against a target of 90%). They need a
  run on server hardware.
- **Many small workers on one queue cost more.** With 50 worker processes
  that each serve the same 100 queues, at 1,000 tasks a second, the
  maintainer's work took 1.2 to 2.1 seconds a round, against a target of 1.
  Each new task wakes every worker of its queue, and all but one find
  nothing.

## What this means for you

| You want to… | Do this | Why |
|---|---|---|
| Enqueue thousands of tasks | `EnqueueMany`, in batches of 100 or more | About 20 times faster than one task per call |
| Run more tasks of a busy queue | Raise `Concurrency` on fewer workers, before you add many small ones | Every new task wakes every worker of its queue |
| Keep fetches fast over hours | Keep long transactions away from the database | They stop vacuum, and fetches slow down. See [Database](../production/database.md) |

## See also

- [Full benchmark results](../design/benchmarks.md): targets, method, and how to run them
- [Defaults and limits](defaults-and-limits.md): the exact limits
- [Autoscaling](../production/autoscaling.md): when to add workers

**Next:** [Documentation home](../README.md)
