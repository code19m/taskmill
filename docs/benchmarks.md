# Benchmarks

Results of [`internal/cmd/bench`](../internal/cmd/bench/main.go) against the
targets in the [performance design](design/performance.md). The IDs match the
targets there.

## Laptop run, 2026-10-18

This run is scaled down: it shows that taskmill meets its targets in shape,
not the numbers on the reference hardware, which a dedicated run will
publish before v1.0.0.

- **Machine:** Apple M5, 10 cores, 16 GiB, shared with other work.
- **Database:** PostgreSQL 18.6 in Docker Desktop, started as shown in
  [Running the benchmarks](#running-the-benchmarks): limited to 4 CPUs, with
  `shared_buffers = 1GB` and `max_wal_size = 8GB`. `fsync` and
  `synchronous_commit` stay on, so every number is for durable commits.
- **Client:** the benchmark program on the host, reaching PostgreSQL through
  Docker's port forwarding.

| ID | Workload | Result | Target |
|---|---|---|---|
| T1 | Enqueue one task per transaction, 16 producers | 13,000 to 17,000 tasks/s; this run 15,828 | ≥ 15,000 |
| T2 | Enqueue in batches of 100, 8 producers | 367,010 tasks/s | ≥ 150,000 |
| T3 | 200,000 queued tasks fetched, run, and completed into history; one worker with concurrency 64 | 38,272 tasks/s | ≥ 20,000 |
| T4 | 200,000 tasks in 100,000 FIFO groups; one worker with concurrency 64 | 35,683 tasks/s | ≥ 5,000 |
| T5 | 200,000 outbox messages on 10,000 keys; one relay worker with concurrency 8 | 35,954 messages/s | ≥ 10,000 |
| E1 | SQL statements per task while processing (T3) | 0.13 | ≤ 3 |
| E2 | WAL written per task, from fetch to history | 0.81 KiB | ≤ 2 KiB |
| L1 | Pickup latency of an idle worker woken by a notification | p50 3.4 ms, p99 7.9 ms | p50 ≤ 5 ms, p99 ≤ 20 ms |

**T1 is bound by the network path, not by taskmill.** Each enqueue is one
round trip, so 16 producers can do at most 16 per round-trip time, and on
this laptop T1 varies between runs from about 13,000 to 17,000 tasks per
second. A bare one-row `INSERT` from 16 goroutines reaches about 15,500 per
second over the same connection: taskmill's enqueue does more work than a
bare `INSERT` (it encodes the payload, checks the options, and limits
notifications) and is still close to its speed. On the reference hardware,
with round trips under 0.5 ms, T1 has headroom.

**E1 counts every statement,** including the `BEGIN`, settings, and `COMMIT`
that a fetch sends with it in one round trip (see
[Fetch](design/schema.md#fetch)). A worker batches its fetches and
completions: the busier it is, the more tasks each statement serves.

**L1 includes the enqueue's own round trips:** the send time is taken just
before the enqueue's transaction.

### Reliability under faults

60,000 tasks, 4 worker processes with concurrency 16, and one process killed
with `SIGKILL` every second: 45 kills in 46 seconds. Each handler records its
executions in a table, so the run counts every execution, including those
whose worker died before reporting.

| ID | Check | Result | |
|---|---|---|---|
| F1 | Every task completed, none left behind | 60,000 completed, 0 left | Pass |
| F2 | Every task completed exactly once | 60,000 distinct of 60,000 | Pass |
| F3 | Extra executions at most kills × concurrency | 425 extra, bound 720 | Pass |

The extra executions are the at-least-once guarantee at work: a killed
worker's in-flight tasks run again after their leases expire.

## Running the benchmarks

`make bench` runs every scenario against the database that `make db` starts.
That database has PostgreSQL's default settings, so its numbers are lower and
vary more than the ones above. For numbers comparable to this report, start a
database with the same settings:

```sh
docker run -d --name taskmill-bench-db --cpus 4 -p 54330:5432 \
  -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=taskmill postgres:18-alpine \
  -c shared_buffers=1GB -c max_wal_size=8GB -c max_connections=500
TASKMILL_TEST_DATABASE_URL='postgres://postgres:postgres@localhost:54330/taskmill?sslmode=disable' \
  go run ./internal/cmd/bench
```

`go run ./internal/cmd/bench -help` lists the flags for running single
scenarios, longer runs, or another database. The program recreates its own
schema, `taskmill_bench`, prints a Markdown report, and counts statements
with a pgx query tracer.
