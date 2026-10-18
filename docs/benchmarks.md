# Benchmarks

Results of [`internal/cmd/bench`](../internal/cmd/bench/main.go) against the
targets in the [performance design](design/performance.md). The IDs match the
targets there.

## Laptop run, 2026-10-18

This run is scaled down: it shows that taskmill meets its targets in shape,
not the numbers on the reference hardware, which a dedicated run will
publish before v1.0.0.

- **Machine:** Apple M5, 10 cores, 16 GiB. PostgreSQL 18.6 in Docker Desktop,
  limited to 4 CPUs, with `shared_buffers = 1GB`, `max_wal_size = 8GB`, and
  `fsync` and `synchronous_commit` on.
- **Client:** the benchmark program on the host, reaching PostgreSQL through
  Docker's port forwarding.

| ID | Workload | Result | Target |
|---|---|---|---|
| T1 | Enqueue one task per transaction, 16 producers | 12,898 to 14,678 tasks/s | ≥ 15,000 |
| T2 | Enqueue in batches of 100, 8 producers | 278,930 tasks/s | ≥ 150,000 |
| T3 | 200,000 tasks end to end, one worker with concurrency 64, history kept | 35,167 tasks/s | ≥ 20,000 |
| R1 | SQL statements per task while processing | 0.08 | ≤ 3 |
| R2 | WAL written per task, from fetch to history | 0.81 KiB | ≤ 2 KiB |
| L1 | Pickup latency of an idle worker woken by a notification | p50 4.8 ms, p99 6.5 ms | p50 ≤ 5 ms, p99 ≤ 20 ms |

**T1 is bound by the network path, not by taskmill.** Each enqueue is one
round trip, so 16 producers can do at most 16 per round-trip time. Over the
same connection, a bare one-row `INSERT` from 16 goroutines reached 15,520 per
second; taskmill's enqueue, which also encodes the payload, validates the
options, and throttles notifications, reaches about 90% of that. On the
reference hardware, with round trips under 0.5 ms, T1 has headroom.

### Reliability under faults

60,000 tasks, 4 worker processes with concurrency 16, and one process killed
with `SIGKILL` every second: 34 kills in 35 seconds. Each handler records its
executions in a table, so the run counts every execution, including those
whose worker died before reporting.

| ID | Check | Result | |
|---|---|---|---|
| F1 | Every task completed, none left behind | 60,000 completed, 0 left | Pass |
| F2 | Every task completed exactly once | 60,000 distinct of 60,000 | Pass |
| F3 | Extra executions at most kills × concurrency | 386 extra, bound 544 | Pass |

The extra executions are the at-least-once guarantee at work: a killed
worker's in-flight tasks run again after their leases expire.

## Running the benchmarks

```sh
make db
make bench
```

`go run ./internal/cmd/bench -help` lists the flags for running single
scenarios, longer runs, or another database.

The program recreates its own schema, `taskmill_bench`, and prints a Markdown
report. For the statement count (R1), install `pg_stat_statements` in the
database first.
