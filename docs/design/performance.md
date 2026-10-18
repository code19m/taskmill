# Performance targets

- Status: Accepted
- Milestone: M1

This document sets the numbers taskmill promises, the hardware they apply to,
and how milestone M7 measures them. The targets are release gates for v1.0.0:
a release that misses one either fixes it or revises the target here, with the
measurements that justify the change.

The targets are anchored in a baseline: the statements from the
[schema document](schema.md), run with pgbench before any Go code existed. The
baseline shows the ceiling that the database itself sets, and the targets leave
room below it for the Go code.

## Reference hardware

Absolute numbers apply only to this setup, so that anyone can reproduce them:

| Role | Specification | Example |
|---|---|---|
| Database | 8 vCPU, 32 GiB RAM, local NVMe SSD, PostgreSQL 18 | AWS `i4i.2xlarge` |
| Workers and producers | 8 vCPU, 16 GiB RAM, a separate machine | AWS `c7i.2xlarge` |
| Network | Same zone, round trip under 0.5 ms | |

PostgreSQL runs with its defaults, except `shared_buffers = 8GB`,
`max_wal_size = 16GB`, `checkpoint_timeout = 15min`, and the autovacuum settings
that taskmill's migrations set on its own tables. `fsync` and
`synchronous_commit` stay on: every number is for durable commits.

## Targets

Each target has an ID that the benchmark reports use. Throughput is sustained
for 10 minutes after a 1-minute warm-up; every number is the median of three
runs. Handlers are no-ops unless stated otherwise.

### Throughput

| ID | Workload | Target |
|---|---|---|
| T1 | Enqueue one task per transaction, 16 producers | ≥ 15,000 tasks/s |
| T2 | Enqueue in batches of 100, 8 producers | ≥ 150,000 tasks/s |
| T3 | End to end: enqueue, fetch, run, complete, history kept | ≥ 20,000 tasks/s |
| T4 | T3 with FIFO groups, 100,000 distinct keys | ≥ 5,000 tasks/s |
| T5 | Outbox relay with a batch publisher, 10,000 keys | ≥ 10,000 messages/s |

### Latency

| ID | Measure | Target |
|---|---|---|
| L1 | Pickup latency, from enqueue commit to handler start, idle worker | p50 ≤ 5 ms, p99 ≤ 20 ms |
| L2 | Pickup latency at half of T3's throughput | p99 ≤ 100 ms |
| L3 | `Enqueue` call latency, one task, no load | p99 ≤ 5 ms |
| L4 | Start of a scheduled task after its run time | p99 ≤ 1.5 s |
| L5 | A killed worker's task running again | ≤ lease duration + 2 s |
| L6 | `Worker.Run` returning after its context is canceled | ≤ drain timeout + 5 s |

### Scalability

| ID | Condition | Target |
|---|---|---|
| S1 | T3 with 10 million scheduled and 1 million available tasks waiting | Within 20% of T3 with empty queues |
| S2 | T3 with 100,000 FIFO groups holding pending tasks in another queue | Within 10% of T3 |
| S3 | 100 queues served by 50 worker processes | Maintainer cycle under 1 s |

### Resource use

| ID | Measure | Target |
|---|---|---|
| R1 | SQL statements per task under load, ungrouped | ≤ 3, amortized over batches |
| R2 | WAL written per task, history kept | ≤ 2 KiB |
| R3 | `tasks` heap and index size during a 4-hour run at half of T3 | Within 2× of their size after the first hour |
| R4 | Database connections | Client: its pool only. Worker: its pool plus one `LISTEN` connection |
| R5 | Worker memory | Idle ≤ 30 MiB RSS; ≤ 8 KiB per running task, beyond the handler's own |
| R6 | An idle worker's load on the database | One fetch per queue per poll interval, plus heartbeats |

### Reliability

These targets have zero tolerance: one violation blocks the release.

| ID | Scenario | Target |
|---|---|---|
| F1 | 1 million tasks while a random worker is killed (`SIGKILL`) every 10 s | Every task completes; none is lost or stuck |
| F2 | F1 | Every task appears exactly once in history |
| F3 | F1 | Extra executions ≤ kills × worker concurrency |
| F4 | 10,000 FIFO groups of 100 tasks, 5% of attempts failing | No handler sees a task before the one ahead of it; with halting, nothing after a dead head runs |
| F5 | 1 million enqueues of 100,000 dedupe keys from 20 processes at once | Exactly 100,000 tasks |
| F6 | PostgreSQL restarted during a run | Workers resume within 10 s of the database; no task lost |
| F7 | A worker cut off from the database for longer than its lease | Its late result is rejected; the task completes once |
| F8 | The integration suite behind PgBouncer in transaction mode | Passes, with `LISTEN` on a direct connection |
| F9 | Application servers' clocks 5 minutes off | No early or late starts beyond L4 |

## How M7 measures

- **A benchmark program** in `internal/cmd/bench` drives each workload against
  a real database and reports every target by ID. It records latency
  histograms, statement counts from `pg_stat_statements`, WAL from
  `pg_current_wal_lsn()`, and table sizes, and writes the results to
  `docs/benchmarks.md` with the hardware and versions used.
- **Failure scenarios** run in the same program: it starts worker processes,
  kills them, restarts PostgreSQL, and cuts connections with a proxy.
- **Micro-benchmarks** (`go test -bench`) cover the worker's hot paths. They are
  compared before and after a change with `benchstat`; they guard against
  regressions rather than prove the targets.
- **On a laptop** the program runs scaled down, with the same IDs, so
  contributors can check a change before the full run.

## Baseline

The schema's statements alone, run with pgbench on an Apple M5 laptop, with
PostgreSQL 18.6 in Docker limited to 4 CPUs and pgbench in the same container.
Docker on macOS makes `fsync` cheaper than real disks do, so these numbers
overstate commit-bound throughput, and they leave out all Go code. They show
relative costs, not promises.

| Workload | Clients | Result | WAL per task |
|---|---|---|---|
| Enqueue, 1 task per transaction | 16 | 42,441 tasks/s | 0.39 KiB |
| Enqueue, 100 tasks per transaction | 8 | 387,288 tasks/s | 0.37 KiB |
| Fetch and complete into history, batch of 1 | 32 | 6,944 tasks/s | 2.54 KiB |
| Fetch and complete into history, batch of 10 | 16 | 31,431 tasks/s | 1.14 KiB |
| Fetch and complete into history, batch of 100 | 8 | 47,916 tasks/s | 1.16 KiB |
| Enqueue with `NOTIFY` in every transaction | 4 | 10,738 tasks/s (19,559 without) | |
| Enqueue with `NOTIFY` in every transaction | 16 | 9,326 tasks/s (35,278 without) | |

A soak ran 10,000 tasks/s through enqueue and processing for 5 minutes,
3 million tasks in all, starting from an empty queue. The backlog stayed near
zero, and the `tasks` heap stayed between 35 and 41 MB while autovacuum ran
about every 7 seconds. Each fetch-and-complete cycle took 1 to 3 ms. Its
indexes grew from 7.5 to 12 MB, which is why R3 measures over hours.

## What the baseline changed in the design

1. **Workers batch automatically.** Processing tasks one at a time is 4.5
   times slower than in batches of 10. Workers fetch up to their free
   capacity at once, and record completions for every result that is ready in
   one statement. At low load a batch is a single task, so batching adds no
   latency.
2. **Notifications are throttled.** `NOTIFY` in every enqueue cut throughput
   to about 10,000 tasks/s, and more producers made it worse, because
   PostgreSQL serializes notifying commits. A client sends at most one
   notification per queue per 50 ms, inside the enqueue's transaction, so an
   idle worker wakes for the first task of a burst at once. If more enqueues
   follow within the window, the client sends one more notification when the
   window ends, so a wake-up is at most about 50 ms late, unless the enqueue's
   transaction is still open then and polling picks the task up. A busy worker
   needs none of this: it fetches again as soon as it has free capacity.
3. **The hot table stays small,** as the schema intended: in steady state it
   holds only the tasks in flight, and vacuum keeps up with the churn.
