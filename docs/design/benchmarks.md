# Benchmarks

This page sets taskmill's performance targets, shows the latest results, and
explains how to measure them yourself. It keeps the exact numbers; for a
short summary, read [Performance](../reference/performance.md).

The targets are release gates for v1.0.0. A release that misses one either
fixes it, or changes the target here, with the measurements that justify
the change.

## Reference hardware

The targets apply to this setup, so that anyone can reproduce them:

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

## Latest results

No run on the reference hardware exists yet. These results come from a
smaller laptop run in October 2026: they show the shape of the results, not
the numbers of the reference hardware.

- **Machine:** Apple M5, 10 cores, 16 GiB, shared with builds, tests, and an
  example application. Throughput moved by up to half between runs.
- **Database:** PostgreSQL 18.6 in Docker Desktop (a VM with 6 CPUs and 6
  GiB), started as in [Running the benchmarks](#running-the-benchmarks), and
  reached from the host through Docker's port forwarding. Docker on macOS
  makes `fsync` cheaper than real disks do, so numbers that depend on
  commits are higher than on a server.
- **Shorter runs:** enqueue and latency scenarios ran for 10 seconds, backlog
  drains for 200,000 tasks, end-to-end runs for 60 seconds after a 30-second
  warm-up, and the soak (E3) for 1 hour. Throughput and L1 to L3 are the
  median of three runs.

**Where this run misses.** T1, T3 end to end, and S2 (just, in one of two
runs) miss here, and need a run on the reference hardware before v1.0.0.
S3 misses at 1,000 tasks a second: a notification wakes every worker of its
queue.

### Throughput

| ID | Target | Result |
|---|---|---|
| T1 | Enqueue one task per transaction, 16 producers: ≥ 15,000 tasks/s | 8,460 tasks/s; 8,015 to 16,379 across runs: below target in two runs of three, see below |
| T2 | Enqueue in batches of 100, 8 producers: ≥ 150,000 tasks/s | 189,110 tasks/s; 175,120 to 366,130 |
| T3 | End to end: enqueue, fetch, run, complete, history kept: ≥ 20,000 tasks/s | 16,879 tasks/s end to end, sustained: below target on this laptop. One worker with concurrency 64 draining 200,000 queued tasks: 31,382 tasks/s |
| T4 | T3 with FIFO groups, 100,000 keys: ≥ 5,000 tasks/s | One worker with concurrency 64 draining 200,000 tasks in 100,000 groups: 25,769 tasks/s |
| T5 | Outbox relay with a batch publisher, 10,000 keys: ≥ 10,000 messages/s | One relay worker with concurrency 8 draining 200,000 messages on 10,000 keys: 26,818 messages/s |

- **T1 is limited by the network path, not by taskmill.** Each enqueue is
  one round trip, so 16 producers do at most 16 per round-trip time. A bare
  one-row `INSERT` from 16 goroutines reaches about 15,500 per second on the
  same connection. taskmill's enqueue does more (it encodes the payload,
  checks the options, and limits notifications), and the one quiet run of
  the three still reached 16,379 tasks/s.
- **T3 end to end** ran 8 producers enqueueing batches of 100, holding back
  while 20,000 tasks waited, beside 2 workers with concurrency 64. 4 workers
  with concurrency 16 batch less: 15,536 tasks/s, 0.58 statements per task.
- **A backlog drains slower as it goes,** and taskmill, not the laptop,
  causes it. Each fetch
  reads the queue's entries in `tasks_fetch_idx` from the front, where
  fetched tasks' entries stay, marked dead, until autovacuum removes them.

### Latency

| ID | Target | Result |
|---|---|---|
| L1 | Pickup latency of an idle worker, from enqueue commit to handler start: p50 ≤ 5 ms, p99 ≤ 20 ms | p50 4.5 ms, p99 10 ms |
| L2 | Pickup latency at half of T3's throughput: p99 ≤ 100 ms | p50 19 ms, p99 44 ms, at 10,000 tasks/s |
| L3 | `Enqueue` call latency, one task, no load: p99 ≤ 5 ms | p50 0.4 ms, p99 0.5 ms |
| L4 | Start of a scheduled task after its run time: p99 ≤ 1.5 s | p50 37 ms, p99 426 ms, max 1.0 s, over 3,601 starts |
| L5 | A killed worker's task running again: ≤ lease duration + 2.5 s | 5.06 s with a 3 s lease: the lease + 2.06 s |
| L6 | `Worker.Run` returning after its context is canceled: ≤ drain timeout + 6 s | 7.01 s with a 2 s drain timeout and handlers that ignore their contexts: the drain timeout + 5.01 s |

- **L1 and L2** include the enqueue's own round trips. L2 enqueued 10,000
  tasks a second in transactions of 10. **L4** measured the starts of
  delayed tasks, retries, snoozes, and schedule runs, against the database's
  clock.
- **L5 and L6 include the time the design adds.** For L5, a killed worker's
  [lease](../concepts/glossary.md#lease) expires one lease duration after its last
  renewal; once a second, the maintainer makes the tasks of expired leases
  retryable; the first retry waits 1 second, with 10% jitter. For L6, `Run`
  cancels the attempts' contexts at the drain timeout, waits a fixed 5
  seconds of grace, then releases the tasks still running, one statement
  each, and unregisters.

### Scalability

| ID | Target | Result |
|---|---|---|
| S1 | T3 with 10 million scheduled and 1 million available tasks waiting: within 20% of T3 | Two runs: 14,869 tasks/s against 17,164 without them, 87%; and 16,257 against 18,539, 88%: pass |
| S2 | T3 with 100,000 FIFO groups holding pending tasks in another queue: within 10% of T3 | Two runs: 12,522 tasks/s against 17,164, 73%; and 16,671 against 18,539, 89.9%: just below target here |
| S3 | 100 queues served by 50 worker processes: maintainer cycle under 1 s | At 1,000 tasks/s, three runs on a busy laptop: slowest loop 1.23 to 2.12 s: fails, see below. Idle first: 153 transactions/s on average, 456 to 464 in the busiest 10 seconds |

- **S1 and S2** compare T3 end to end with and without the waiting tasks,
  which were vacuumed, analyzed, and on disk first. Runs here move by up to
  a fifth (T3 alone: 17,164 to 20,859 tasks/s), so S2 needs a run on the
  reference hardware.
- **S1 and S2 catch bad plans.** With millions of tasks waiting, statistics
  can make a bad plan look cheap. They caught two such bugs, both fixed (see
  the [changelog](../../CHANGELOG.md)): promotion of due tasks must use its
  index (a full scan took 1.2 s, not 0.2 ms), and recording a result or
  renewing leases must find each task by its ID, not read
  `tasks_running_idx`. Run them after you change those statements.
- **S3** runs one maintainer and 50 worker processes that take tasks from
  100 queues each, with concurrency 1. Idle, processes started together poll at nearly
  the same moments. Then 1,000 tasks a second arrive in random queues, a
  fifth of them delayed, and a worker process is killed every 10 seconds.
- **S3 fails at 1,000 tasks a second, because of a design cost.** A
  notification wakes every worker of its queue, so about 1,000
  notifications a second each wake 50 processes, which all fetch, and all
  but one find nothing. That uses the database's 4 CPUs, and the slowest
  loop, promotion or statistics, takes 1.2 to 2.1 seconds (the laptop's load
  average reached 83). The lesson for users: on one queue, a few workers
  with more concurrency cost the database less than many small ones.

### Resource use

| ID | Target | Result |
|---|---|---|
| E1 | SQL statements per task under load, ungrouped: ≤ 3 | 0.13 while one worker drains a backlog; 0.16 end to end, enqueues included |
| E2 | WAL written per task, history kept: ≤ 2 KiB | 0.81 KiB from fetch to history |
| E3 | `tasks` heap and index size during a 4-hour run at half of T3: within 2× of their size after the first hour | 36 million tasks in 1 hour at 10,000 tasks/s, none left waiting, completed tasks discarded (the table sees the same writes): heap 167 MiB throughout, indexes 88 to 116 MiB; at most 283 MiB, 1.06× its size after 15 minutes: pass |
| E4 | Database connections: a client uses its pool only; a worker, its pool plus one `LISTEN` connection | A client with a pool of 8 used at most 8; a worker with a pool of 8 and concurrency 64, at most 9, one of them listening: pass |
| E5 | Worker memory: idle ≤ 30 MiB RSS; ≤ 8 KiB per running task | The benchmark program, a little bigger than a worker, on macOS: 17.3 MiB idle; 7.9 KiB per running task, with 10,000 handlers waiting: pass |
| E6 | An idle worker's load: while the maintainer's heartbeats arrive, about one fetch per queue per minute, plus updates of the worker's own row; while they do not, one fetch per queue per poll interval | Heartbeats arriving, 10 queues for 3 minutes: 90 fetches, 9.0 per queue, of which up to 7 before the first heartbeat; 17 updates of the worker's own row; 29 other statements: pass |
| E7 | The database's CPU for heartbeats when several schemas share a database (no target) | 500 connections listening: 0.24 CPUs at 20 heartbeats a second, what 100 schemas send, and 1.11 CPUs at 100 a second; about 24 µs per heartbeat per listening connection |

- **E1 counts every statement,** with a fetch's `BEGIN`, settings, and
  `COMMIT` ([Fetch](schema.md#fetch)). End to end, batches are smaller.
- **E2 leaves out the enqueue's WAL** and the full-page writes after
  checkpoints, which a short run misses. A long run writes more.
- **E6** measured a worker with `NoMaintainer`. Each queue fetches at start,
  when the notification connection starts, and every second until the first
  heartbeat (within 5 seconds); then once a minute, ± 6 seconds, since the
  maintainer's wake already nudges every queue whose tasks wait.
- **E7** keeps 500 connections listening on channels of their own, as the
  workers of other schemas do. PostgreSQL wakes every listening connection
  in a database for each notification, whatever its channel, so the cost
  grows with schemas × listening connections. That is why the heartbeat
  runs every 5 seconds, not with every wake. E7 needs `-container`.

### Reliability under faults

These targets have zero tolerance: one violation blocks the release. Every
check is exact: it counts rows, or compares timestamps from the database's
clock. Handlers record each call in a table, so the counts include calls
whose worker died before it reported.

| ID | Target | Result |
|---|---|---|
| F1 | 1 million tasks while a random worker is killed every 10 s: every task completes; none is lost or stuck | 60,000 completed, 0 left: pass |
| F2 | F1: every task appears exactly once in history | 60,000 distinct of 60,000: pass |
| F3 | F1: extra handler calls ≤ kills × worker concurrency | 417 extra, bound 976: pass |
| F4 | 10,000 FIFO groups of 100 tasks, 5% of attempts failing: no handler sees a task before the one ahead of it; nothing after a dead head runs | 1 million tasks: 0 calls before the task ahead completed, 0 after their own, 0 past a dead head; 993,523 completed, none twice; 137 dead heads, 6,340 tasks waiting behind them; each task accounted for, 0 lost; groups exact: pass |
| F5 | 1 million enqueues of 100,000 dedupe keys from 20 processes at the same time: exactly 100,000 tasks | 100,000 tasks in 16 s; 100,000 enqueues reported a new task, 0 results named another task; no producer saw an error; FIFO groups, on every fifth key, exact: pass |
| F6 | PostgreSQL restarted during a run: workers resume within 10 s; no task lost | Shut down cleanly: back after 0.7 s, and a task fetched after it completed 0.17 s later. Crashed and down for 15 s, longer than the maintainer's lease: 0.41 s after it was back. 60,000 completed once, 0 left: pass |
| F7 | A worker cut off from the database for longer than its lease: its late result is rejected; the task completes once | 0 of 16 tasks completed by the cut worker's stale attempts; 116 completed once, 0 left; the cut worker ran new tasks on its own: pass |
| F8 | The integration suite behind PgBouncer in transaction mode: passes, with `LISTEN` on a direct connection | Every test passes through PgBouncer 1.25.2; CI checks it on every pull request: pass |
| F9 | Application servers' clocks 5 minutes off: no early or late starts beyond L4 | Database clock 5 minutes ahead: 0 early starts, p99 432 ms late, 0 leases lost. 5 minutes behind: 0 early, p99 677 ms, 0 leases lost: pass |

- **F1 to F3** ran 60,000 tasks on 4 worker processes with concurrency 16,
  and killed one with `SIGKILL` every second, ten times as often as the
  target. The extra calls are the at-least-once guarantee at work.
- **F4** also killed a worker process every 2 seconds (93 kills, beside
  52,283 failed attempts), and allowed 3 attempts, so some
  [heads](../concepts/glossary.md#head) (a group's first task) died and
  halted their groups. Each task is checked to have completed once, to be a
  dead head, or to wait behind one.
- **F7** cut a worker off with a proxy that held its traffic. Half of its 16
  handlers returned during the cut, half after; another worker held the
  tasks until the late results arrived, so a wrongly accepted one would show.
- **F8:** tests that need a session of their own (a session setting, a
  startup parameter, or `LISTEN`) connect directly. F8 caught two `Migrate`
  bugs, both fixed: one with `max_prepared_statements`
  ([pgbouncer#1630](https://github.com/pgbouncer/pgbouncer/issues/1630)),
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
database, and cut a worker off with a proxy. A failed check fails the run.

`make bench` runs every scenario against the database of `make db`. It has
PostgreSQL's default settings, so its numbers are lower and vary more. For
numbers you can compare with this page, use the same settings:

```sh
docker run -d --name taskmill-bench-db --cpus 4 -p 54330:5432 \
  -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=taskmill postgres:18-alpine \
  -c shared_buffers=1GB -c max_wal_size=8GB -c max_connections=700
TASKMILL_TEST_DATABASE_URL='postgres://postgres:postgres@localhost:54330/taskmill?sslmode=disable' \
  go run ./internal/cmd/bench -container taskmill-bench-db
```

- `-container` names the database's Docker container, which F6 restarts and
  E7 measures. Without it, they are skipped.
- The soak (E3) runs only when listed: `-scenarios soak -soak 4h`.
- `go run ./internal/cmd/bench -help` lists the other flags. The program
  recreates its own schema, `taskmill_bench`.

`make faults` runs the fault scenarios as the nightly CI run does: F1 to F7
at a tenth of their size, and F9 against databases whose clocks are 5
minutes ahead and behind. It starts and removes them itself
([`faults.sh`](../../internal/cmd/bench/faults.sh)). `make test-pgbouncer`
runs F8.

To run F9 by hand, start PostgreSQL under libfaketime, and pass the skew.
The timing scenario then fails, rather than skips F9, if the database's
clock is not off by that much:

```sh
printf 'FROM postgres:18-alpine\nRUN apk add --no-cache libfaketime\n' | docker build -t taskmill-faketime -
docker run -d --name taskmill-bench-skew -p 54333:5432 \
  -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=taskmill \
  -e LD_PRELOAD=/usr/lib/faketime/libfaketime.so.1 -e FAKETIME=+5m -e DONT_FAKE_MONOTONIC=1 \
  taskmill-faketime
go run ./internal/cmd/bench -db 'postgres://postgres:postgres@localhost:54333/taskmill?sslmode=disable' \
  -scenarios timing -skew 5m
```

## See also

- [Performance](../reference/performance.md): the summary for users
- [Storage schema](schema.md): the statements these numbers measure
- [Task lifecycle and guarantees](task-lifecycle.md): what F1 to F9 check

**Next:** [Releasing](releasing.md)
