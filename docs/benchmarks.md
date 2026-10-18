# Benchmarks

This page sets the numbers taskmill aims for, shows what has been measured so
far, and explains how. The targets are release gates for v1.0.0: a release
that misses one either fixes it or revises the target here, with the
measurements that justify the change.

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
Throughput is sustained for 10 minutes after a 1-minute warm-up, and every
number is the median of three runs. Handlers do nothing unless stated
otherwise.

## Laptop run, 2026-10-18

No run on the reference hardware exists yet. This run is scaled down: it
shows the shape of the results, not the numbers on the reference hardware.

- **Machine:** Apple M5, 10 cores, 16 GiB, shared with other work.
- **Database:** PostgreSQL 18.6 in Docker Desktop, started as shown in
  [Running the benchmarks](#running-the-benchmarks): limited to 4 CPUs, with
  `shared_buffers = 1GB` and `max_wal_size = 8GB`. Docker on macOS makes
  `fsync` cheaper than real disks do, so commit-bound numbers are higher than
  on a server.
- **Client:** the benchmark program on the host, reaching PostgreSQL through
  Docker's port forwarding. Enqueue scenarios ran for 10 seconds, and
  processing scenarios for 200,000 tasks.

### Throughput

| ID | Target | Result |
|---|---|---|
| T1 | Enqueue one task per transaction, 16 producers: ≥ 15,000 tasks/s | 13,000 to 17,000 tasks/s; this run 16,104 |
| T2 | Enqueue in batches of 100, 8 producers: ≥ 150,000 tasks/s | 359,860 tasks/s |
| T3 | End to end: enqueue, fetch, run, complete, history kept: ≥ 20,000 tasks/s | Not measured end to end. One worker with 64 slots draining 200,000 queued tasks into history: 38,128 tasks/s |
| T4 | T3 with FIFO groups, 100,000 keys: ≥ 5,000 tasks/s | One worker with 64 slots draining 200,000 tasks in 100,000 groups: 35,141 tasks/s |
| T5 | Outbox relay with a batch publisher, 10,000 keys: ≥ 10,000 messages/s | One relay worker with 8 slots draining 200,000 messages on 10,000 keys: 36,326 messages/s |

**T1 is bound by the network path, not by taskmill.** Each enqueue is one
round trip, so 16 producers can do at most 16 per round-trip time. A bare
one-row `INSERT` from 16 goroutines reaches about 15,500 per second over the
same connection. taskmill's enqueue does more work (it encodes the payload,
checks the options, and limits notifications) and is still close to that
speed.

### Latency

| ID | Target | Result |
|---|---|---|
| L1 | Pickup latency of an idle worker, from enqueue commit to handler start: p50 ≤ 5 ms, p99 ≤ 20 ms | p50 3.7 ms, p99 6.3 ms |
| L2 | Pickup latency at half of T3's throughput: p99 ≤ 100 ms | — |
| L3 | `Enqueue` call latency, one task, no load: p99 ≤ 5 ms | — |
| L4 | Start of a scheduled task after its run time: p99 ≤ 1.5 s | — |
| L5 | A killed worker's task running again: ≤ lease duration + 2 s | — |
| L6 | `Worker.Run` returning after its context is canceled: ≤ drain timeout + 5 s | — |

L1 includes the enqueue's own round trips: the send time is taken just before
the enqueue's transaction.

### Scalability

| ID | Target | Result |
|---|---|---|
| S1 | T3 with 10 million scheduled and 1 million available tasks waiting: within 20% of T3 | — |
| S2 | T3 with 100,000 FIFO groups holding pending tasks in another queue: within 10% of T3 | — |
| S3 | 100 queues served by 50 worker processes: maintainer cycle under 1 s | — |

### Resource use

| ID | Target | Result |
|---|---|---|
| E1 | SQL statements per task under load, ungrouped: ≤ 3 | 0.13 while one worker drains a backlog (T3) |
| E2 | WAL written per task, history kept: ≤ 2 KiB | 0.80 KiB from fetch to history |
| E3 | `tasks` heap and index size during a 4-hour run at half of T3: within 2× of their size after the first hour | — |
| E4 | Database connections: a client uses its pool only; a worker, its pool plus one `LISTEN` connection | — |
| E5 | Worker memory: idle ≤ 30 MiB RSS; ≤ 8 KiB per running task | — |
| E6 | An idle worker's load: one fetch per queue per poll interval, plus heartbeats | — |

**E1 counts every statement,** including the `BEGIN`, settings, and `COMMIT`
that a fetch sends with it in one round trip (see
[Fetch](design/schema.md#fetch)). While a worker drains a backlog, each fetch
and completion serves many tasks. With producers running at the same time,
batches are smaller and the count is higher.

**E2 leaves out the enqueue,** which writes WAL too. A run of a few seconds
also misses checkpoints, after which the first change to each page writes the
whole page to WAL. The end-to-end number on a long run is higher.

### Reliability under faults

These targets have zero tolerance: one violation blocks the release. In this
run, 60,000 tasks went through 4 worker processes with 16 slots each, and one
process was killed with `SIGKILL` every second: 39 kills in 39 seconds. Each
handler call wrote a row to a table, so the count includes calls whose worker
died before reporting.

| ID | Target | Result |
|---|---|---|
| F1 | 1 million tasks while a random worker is killed every 10 s: every task completes; none is lost or stuck | 60,000 completed, 0 left: pass |
| F2 | F1: every task appears exactly once in history | 60,000 distinct of 60,000: pass |
| F3 | F1: extra handler calls ≤ kills × worker concurrency | 406 extra, bound 624: pass |
| F4 | 10,000 FIFO groups of 100 tasks, 5% of attempts failing: no handler sees a task before the one ahead of it; nothing after a dead head runs | — |
| F5 | 1 million enqueues of 100,000 dedupe keys from 20 processes at once: exactly 100,000 tasks | — |
| F6 | PostgreSQL restarted during a run: workers resume within 10 s; no task lost | — |
| F7 | A worker cut off from the database for longer than its lease: its late result is rejected; the task completes once | — |
| F8 | The integration suite behind PgBouncer in transaction mode: passes, with `LISTEN` on a direct connection | — |
| F9 | Application servers' clocks 5 minutes off: no early or late starts beyond L4 | — |

The extra calls in F3 are the at-least-once guarantee at work: a killed
worker's running tasks run again after their leases expire.

### Not yet measured

Every target marked "—", T3 end to end, and E1 in steady state with producers
and several workers running at once. These need the reference hardware, runs
of the full 10 minutes, latency histograms, table sizes over hours (E3), and
fault injection beyond `SIGKILL`: database restarts, network cuts, PgBouncer,
and clock skew. They are planned before v1.0.0.

## Running the benchmarks

[`internal/cmd/bench`](../internal/cmd/bench/main.go) runs T1 to T5, E1, E2,
L1, and F1 to F3 against a real database, and prints a Markdown report. It
counts statements with a pgx query tracer, and WAL with
`pg_current_wal_lsn()`. For F1 to F3, it starts worker processes and kills
them while they work.

`make bench` runs every scenario against the database that `make db` starts.
That database has PostgreSQL's default settings, so its numbers are lower and
vary more than the ones above. For comparable numbers, start a database with
the same settings:

```sh
docker run -d --name taskmill-bench-db --cpus 4 -p 54330:5432 \
  -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=taskmill postgres:18-alpine \
  -c shared_buffers=1GB -c max_wal_size=8GB -c max_connections=500
TASKMILL_TEST_DATABASE_URL='postgres://postgres:postgres@localhost:54330/taskmill?sslmode=disable' \
  go run ./internal/cmd/bench
```

`go run ./internal/cmd/bench -help` lists the flags for running single
scenarios, longer runs, or another database. The program recreates its own
schema, `taskmill_bench`.
