# Benchmarks

This page sets taskmill's performance targets, shows the latest results, and
explains how to measure them yourself. It keeps the exact numbers; for a
short summary, read [Performance](../reference/performance.md).

The targets are release gates for v1.0.0. A release that misses one either
fixes it, or changes the target here, with the measurements that justify
the change.

## Target hardware

The targets apply to this setup:

| Role | Specification | Example |
|---|---|---|
| Database | 8 vCPU, 32 GiB RAM, local NVMe SSD, PostgreSQL 18 | AWS `i4i.2xlarge` |
| Workers and producers | 8 vCPU, 16 GiB RAM, a separate machine | AWS `c7i.2xlarge` |
| Network | Same zone, round trip under 0.5 ms | |

PostgreSQL runs with its defaults, except `shared_buffers = 8GB`,
`max_wal_size = 16GB`, `checkpoint_timeout = 15min`, and the autovacuum
settings that taskmill's migrations set on its own tables. `fsync` and
`synchronous_commit` stay on: every number is for durable commits.

**Method.** Throughput runs for 10 minutes after a 1-minute warm-up. Every
number is the median of three runs. Handlers do nothing, unless a scenario
says otherwise.

## Reference runner

The [Benchmarks workflow](../../.github/workflows/bench.yml) measures on a
machine that anyone can use, every Sunday and on demand:

| What | Setup |
|---|---|
| Runner | GitHub-hosted `ubuntu-24.04`, the standard runner of a public repository: 4 vCPUs (x64), 16 GiB RAM, 14 GiB SSD |
| Database | PostgreSQL 18 (`postgres:18-alpine`, pinned by digest) in Docker on the same runner, with the settings of [Running the benchmarks](#running-the-benchmarks) |
| Run | Every scenario but the soak (E3), once, with `-duration 1m` and `-container`; the fault scenarios at full size |

The database and the benchmark share 4 CPUs, so compare a run with other
runs, not with the targets. GitHub fixes the runner's size, not its CPU
model, so each report starts with its machine: compare several runs before
you trust a small change. The report is in the run's summary and in its
artifact `bench-report`. A failed check shows there as **FAIL**; only an
error fails the run.

## Latest results

The [Benchmarks workflow](../../.github/workflows/bench.yml) ran on the
reference runner on 21 November 2026, on commit `1d1f779`, the code that
v0.15.0 releases
([run 44906576337](https://github.com/code19m/taskmill/actions/runs/44906576337)).
Its machine: 4 × Intel Xeon 6975P-C, 15.3 GiB of memory, PostgreSQL 18.6.
No run on the target hardware exists yet.

**Where this run misses:** T1, T3 end to end, and S3. The targets are for
the target hardware, where the database has 8 CPUs of its own; here it
shares 4 with the benchmark. S3 misses because of a design cost.

### Throughput

| ID | Target | Result |
|---|---|---|
| T1 | Enqueue one task per transaction, 16 producers: ≥ 15,000 tasks/s | 10,651 tasks/s: below target |
| T2 | Enqueue in batches of 100, 8 producers: ≥ 150,000 tasks/s | 151,153 tasks/s |
| T3 | End to end: enqueue, fetch, run, complete, history kept: ≥ 20,000 tasks/s | 16,291 tasks/s end to end, sustained: below target. One worker with concurrency 64 draining 200,000 queued tasks: 26,320 tasks/s |
| T4 | T3 with FIFO groups, 100,000 keys: ≥ 5,000 tasks/s | One worker with concurrency 64 draining 200,000 tasks in 100,000 groups: 18,770 tasks/s |
| T5 | Outbox relay with a batch publisher, 10,000 keys: ≥ 10,000 messages/s | One relay worker with concurrency 8 draining 200,000 messages on 10,000 keys: 20,466 messages/s |

- **T1 commits each task on its own:** one round trip and one durable
  commit per task, where T2 has one for 100 tasks.
- **T3 end to end** ran 8 producers enqueueing batches of 100, holding back
  while 20,000 tasks waited, beside 2 workers with concurrency 64.
- **A backlog drains slower as it goes.** Each fetch reads the queue's
  entries in `tasks_fetch_idx` from the front, where fetched tasks' entries
  stay, marked dead, until autovacuum removes them.

### Latency

| ID | Target | Result |
|---|---|---|
| L1 | Pickup latency of an idle worker, from enqueue commit to handler start: p50 ≤ 5 ms, p99 ≤ 20 ms | p50 1.6 ms, p99 2.6 ms, max 5.6 ms, over 591 tasks |
| L2 | Pickup latency at half of T3's throughput: p99 ≤ 100 ms | p50 16.4 ms, p99 46.7 ms, max 67 ms, at 10,000 tasks/s |
| L3 | `Enqueue` call latency, one task, no load: p99 ≤ 5 ms | p50 0.3 ms, p99 0.4 ms, max 2 ms |
| L4 | Start of a scheduled task after its run time: p99 ≤ 1.5 s | p50 37.8 ms, p99 320.1 ms, max 1.0 s, over 3,601 starts |
| L5 | A killed worker's task running again: ≤ lease duration + 2.5 s | 5.06 s with a 3 s lease: the lease + 2.06 s |
| L6 | `Worker.Run` returning after its context is canceled: ≤ drain timeout + 6 s | 7.01 s with a 2 s drain timeout and handlers that ignore their contexts: the drain timeout + 5.01 s |

- **L1 and L2** include the enqueue's own round trips. L2 enqueued 10,000
  tasks a second in transactions of 10. **L4** measured the starts of
  delayed tasks, retries, snoozes, and schedule runs, against the database's
  clock.
- **L5 and L6 include the time the design adds.** For L5, a killed
  worker's [lease](../concepts/glossary.md#lease) expires one lease after
  its last renewal; the maintainer rescues its tasks within a second; the
  first retry waits 1 second, with 10% jitter. For L6, `Run` cancels the
  attempts' contexts at the drain timeout, waits 5 seconds of grace, then
  releases the tasks still running, and unregisters.

### Scalability

| ID | Target | Result |
|---|---|---|
| S1 | T3 with 10 million scheduled and 1 million available tasks waiting: within 20% of T3 | 15,589 tasks/s against 16,814 without them, 93%: pass |
| S2 | T3 with 100,000 FIFO groups holding pending tasks in another queue: within 10% of T3 | 15,684 tasks/s against 16,814, 93%: pass |
| S3 | 100 queues read by 50 worker processes: maintainer cycle under 1 s | At 1,000 tasks/s, slowest iteration of each loop: statistics 3.25 s, promotion 1.38 s, wake 1.03 s, main 0.75 s: fails, see below. Idle first: 148 transactions/s on average, 457 in the busiest 10 seconds |

- **S1 and S2** compare T3 end to end with and without the waiting tasks,
  which were vacuumed, analyzed, and checkpointed first.
- **S1 and S2 catch bad plans.** With millions of tasks waiting, statistics
  can make a bad plan look cheap. They caught two such bugs, both fixed (see
  the [changelog](../../CHANGELOG.md)): promotion of due tasks must use its
  index (a full scan took 1.2 s, not 0.2 ms), and recording a result or
  renewing leases must find each task by its ID, not read
  `tasks_running_idx`. Run them after you change those statements.
- **S3** runs one maintainer and 50 worker processes that take tasks from
  100 queues each, with concurrency 1. Idle, processes started together
  poll at nearly the same moments. Then 1,000 tasks a second arrive in
  random queues, a fifth of them delayed, and a worker process is killed
  every 10 seconds.
- **S3 fails at 1,000 tasks a second, because of a design cost:** a
  notification wakes every worker of its queue
  ([why](schema-maintenance.md#notifications)). Each notification makes 50
  processes fetch, and all but one find nothing. The database shares the
  runner's 4 CPUs with those processes, and the maintainer's slowest
  statements took 1.4 to 2.1 s: promotion, statistics, and storing activity.

### Resource use

| ID | Target | Result |
|---|---|---|
| E1 | SQL statements per task under load, ungrouped: ≤ 3 | 0.13 while one worker drains a backlog; 0.16 end to end, enqueues included |
| E2 | WAL written per task, history kept: ≤ 2 KiB | 0.99 KiB from fetch to history |
| E3 | `tasks` heap and index size during a 4-hour run at half of T3: within 2× of their size after the first hour | Not in this run, which skips the soak. A 1-hour laptop run in October 2026, before the latest statement changes: 36 million tasks at 10,000 tasks/s, none left waiting, completed tasks discarded (the table sees the same writes): heap 167 MiB throughout, indexes 88 to 116 MiB; at most 283 MiB, 1.06× its size after 15 minutes: pass |
| E4 | Database connections: a client uses its pool only; a worker, its pool plus one `LISTEN` connection | A client with a pool of 8 used at most 8; a worker with a pool of 8 and concurrency 64, at most 9, one of them listening: pass |
| E5 | Worker memory: idle ≤ 30 MiB RSS; ≤ 13 KiB per running task | The benchmark program, a little bigger than a worker: 15.4 MiB idle; 11.7 KiB per running task, with 10,000 handlers waiting: pass |
| E6 | An idle worker's load: while the maintainer's heartbeats arrive, about one fetch per queue per minute, plus updates of the worker's own row; while they do not, one fetch per queue per poll interval | Heartbeats arriving, 10 queues for 1 minute: 70 fetches, 7.0 per queue, of which up to 7 before the first heartbeat; 5 updates of the worker's own row; 20 other statements: pass |
| E7 | The database's CPU for heartbeats when several schemas share a database (no target) | 500 connections listening: 0.15 CPUs at 20 heartbeats a second, what 100 schemas send, and 0.62 CPUs at 100 a second; about 14.9 µs per heartbeat per listening connection |

- **E1 counts every statement,** with a fetch's `BEGIN`, settings, and
  `COMMIT` ([Fetch](schema-statements.md#fetch)). End to end, batches are smaller.
- **E2 leaves out the enqueue's WAL** and the full-page writes after
  checkpoints, which a short run misses. A long run writes more.
- **E5 measures what a task holds:** the process frees its garbage before
  three samples, and the median counts: 8 KiB of a handler's stack after a
  database call, about 3 KiB of taskmill's, 1 KiB of the runtime's, and 1 KiB
  of room.
- **E6** measured a worker with `NoMaintainer`. Each queue fetches at start,
  when the notification connection starts, and every second until the first
  heartbeat (within 5 seconds); then once a minute, ± 6 seconds.
- **E7** keeps 500 connections listening on channels of their own, as other
  schemas' workers do ([why](schema-maintenance.md#notifications)).

### Reliability under faults

These targets have zero tolerance: one violation blocks the release. Every
check is exact: it counts rows, or compares timestamps from the database's
clock. Handlers record each call in a table, so the counts include calls
whose worker died before it reported.

| ID | Target | Result |
|---|---|---|
| F1 | 1 million tasks while a random worker is killed every 10 s: every task completes; none is lost or stuck | 60,000 completed, 0 left: pass |
| F2 | F1: every task appears exactly once in history | 60,000 distinct of 60,000: pass |
| F3 | F1: extra handler calls ≤ kills × worker concurrency | 410 extra, bound 2,144: pass |
| F4 | 10,000 FIFO groups of 100 tasks, 5% of attempts failing: no handler sees a task before the one ahead of it; nothing after a dead head runs | 1 million tasks: 0 calls before the task ahead completed, 0 after their own, 0 past a dead head; 992,020 completed, none twice; 153 dead heads, 7,827 tasks waiting behind them; each task accounted for, 0 lost; groups exact: pass |
| F5 | 1 million enqueues of 100,000 dedupe keys from 20 processes at the same time: exactly 100,000 tasks | 100,000 tasks in 36 s; 100,000 enqueues reported a new task, 0 results named another task; no producer saw an error; FIFO groups, on every fifth key, exact: pass |
| F6 | PostgreSQL restarted during a run: workers resume within 10 s; no task lost | Shut down cleanly: back after 0.8 s, and workers resumed 0.23 s later. Crashed and down for 15 s, longer than the maintainer's lease: back after 15.5 s, and workers resumed 1.7 s later. 60,000 completed once, 0 left, 0 extra calls: pass |
| F7 | A worker cut off from the database for longer than its lease: its late result is rejected; the task completes once | 0 of 16 tasks completed by the cut worker's stale attempts; 116 completed once, 0 left; the cut worker ran 100 new tasks on its own in 20 ms: pass |
| F8 | The integration suite behind PgBouncer in transaction mode: passes, with `LISTEN` on a direct connection | Every test passes through PgBouncer 1.25.2; CI checks it on every pull request: pass |
| F9 | Application servers' clocks 5 minutes off: no early or late starts beyond L4 | Not in this run: the database's clock agreed with the runner's. The [Faults workflow](../../.github/workflows/faults.yml) on the same code ([run 44906006554](https://github.com/code19m/taskmill/actions/runs/44906006554)), database clock 5 minutes ahead: 0 early starts, p99 812.9 ms late, 0 leases lost. 5 minutes behind: 0 early, p99 632.8 ms, 0 leases lost: pass |

- **F1 to F3** ran 60,000 tasks on 4 worker processes with concurrency 16,
  and killed one with `SIGKILL` every second (134 kills), ten times as
  often as the target. The extra calls are the at-least-once guarantee at
  work.
- **F4** also killed a worker process every 2 seconds (94 kills, beside
  52,282 failed attempts), and allowed 3 attempts, so some
  [heads](../concepts/glossary.md#head) (a group's first task) died and
  halted their groups. Each task is checked to have completed once, to be a
  dead head, or to wait behind one.
- **F7** cut a worker off with a proxy that held its traffic. Half of its 16
  handlers returned during the cut, half after; another worker held the
  tasks until the late results arrived, so a wrongly accepted one would show.
- **F8:** tests that need a session of their own (a session setting, a
  startup parameter, or `LISTEN`) connect directly. F8 caught two `Migrate`
  bugs, both fixed: one with `max_prepared_statements` ([pgbouncer#1630](https://github.com/pgbouncer/pgbouncer/issues/1630)),
  one when several processes migrate a new schema at the same time.
- **F9** ran L4 under libfaketime. Times that taskmill sets come from the
  database's clock; a run time that your code passes comes from its own
  clock, by design ([invariant 5](task-lifecycle.md#invariants)). So with the
  database behind, delayed tasks waited 5 minutes more, and none started early.

## Running the benchmarks

[`internal/cmd/bench`](../../internal/cmd/bench) runs a scenario for every
target against a real database, and prints a Markdown report. It counts
statements with a pgx query tracer, WAL with `pg_current_wal_lsn()`,
connections with `pg_stat_activity`, and memory with `ps`. The fault
scenarios start worker and producer processes, kill them, restart the
database, and cut a worker off with a proxy. A failed check, such as a
missed target, ends the run with exit status 2; an error ends it with 1.

`make bench` runs every scenario against the database of `make db`. It has
PostgreSQL's default settings, so its numbers are lower and vary more. For
numbers you can compare with this page, use the settings of the reference
runner, as its workflow does:

```sh
docker run -d --name taskmill-bench-db --cpus 4 -p 54330:5432 \
  -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=taskmill postgres:18-alpine \
  -c shared_buffers=1GB -c max_wal_size=8GB -c max_connections=700
TASKMILL_TEST_DATABASE_URL='postgres://postgres:postgres@localhost:54330/taskmill?sslmode=disable' \
  go run ./internal/cmd/bench -container taskmill-bench-db -duration 1m
```

- `-container` names the database's Docker container, which F6 restarts and
  E7 measures. Without it, they are skipped.
- The soak (E3) runs only when listed: `-scenarios soak -soak 4h`.
- `-help` lists the other flags. The program recreates its schema, `taskmill_bench`.

`make faults` runs the fault scenarios as the nightly CI run does: F1 to F7
at a tenth of their size, and F9 against databases of its own whose clocks
are 5 minutes ahead and behind
([`faults.sh`](../../internal/cmd/bench/faults.sh) shows how).
`make test-pgbouncer` runs F8. With `-skew`, the timing scenario fails,
rather than skips F9, if the database's clock is not off by that much.

## See also

- [Performance](../reference/performance.md): the summary for users
- [Key statements](schema-statements.md): the statements these numbers measure
- [Task lifecycle and guarantees](task-lifecycle.md): what F1 to F9 check

**Next:** [Releasing](releasing.md)
