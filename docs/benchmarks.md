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

## Laptop run, 2026-10-26

No run on the reference hardware exists yet. This run is scaled down: it
shows the shape of the results, not the numbers on the reference hardware.

- **Machine:** Apple M5, 10 cores, 16 GiB, shared with other work: other
  sessions built code, ran tests, and served an example application during
  the run. Throughput moved by up to half between runs of the same
  scenario.
- **Database:** PostgreSQL 18.6 in Docker Desktop, in a virtual machine with
  6 CPUs and 6 GiB, started as shown in
  [Running the benchmarks](#running-the-benchmarks): limited to 4 CPUs, with
  `shared_buffers = 1GB`, `max_wal_size = 8GB`, and `max_connections = 700`.
  Docker on macOS makes `fsync` cheaper than real disks do, so commit-bound
  numbers are higher than on a server.
- **Client:** the benchmark program on the host, reaching PostgreSQL through
  Docker's port forwarding. Enqueue and latency scenarios ran for 10 seconds,
  backlog drains for 200,000 tasks, and end-to-end runs for 60 seconds after
  a 30-second warm-up, not 10 minutes after one. Throughput and L1 to L3 are
  the median of three runs.

### Throughput

| ID | Target | Result |
|---|---|---|
| T1 | Enqueue one task per transaction, 16 producers: ≥ 15,000 tasks/s | 8,460 tasks/s; 8,015 to 16,379 across runs: below target in two runs of three, see below |
| T2 | Enqueue in batches of 100, 8 producers: ≥ 150,000 tasks/s | 189,110 tasks/s; 175,120 to 366,130 |
| T3 | End to end: enqueue, fetch, run, complete, history kept: ≥ 20,000 tasks/s | 16,879 tasks/s end to end, sustained: below target on this laptop. One worker with 64 slots draining 200,000 queued tasks: 31,382 tasks/s |
| T4 | T3 with FIFO groups, 100,000 keys: ≥ 5,000 tasks/s | One worker with 64 slots draining 200,000 tasks in 100,000 groups: 25,769 tasks/s |
| T5 | Outbox relay with a batch publisher, 10,000 keys: ≥ 10,000 messages/s | One relay worker with 8 slots draining 200,000 messages on 10,000 keys: 26,818 messages/s |

**T1 is bound by the network path, not by taskmill.** Each enqueue is one
round trip, so 16 producers can do at most 16 per round-trip time. A bare
one-row `INSERT` from 16 goroutines reaches about 15,500 per second over the
same connection. taskmill's enqueue does more work (it encodes the payload,
checks the options, and limits notifications) and is still close to that
speed. Two of the three runs coincided with other work on the laptop; the
quiet one reached 16,379 tasks/s.

**T3 end to end** ran 8 producers, enqueueing batches of 100 and holding back
while 20,000 tasks waited, beside 2 workers with 64 slots each. Fewer slots
per worker batch less: 4 workers with 16 slots each ran 15,536 tasks/s, with
0.58 statements per task. Draining a backlog slows as it goes, and that is
taskmill's doing, not this laptop's: four drains of 200,000 tasks in a row,
before the fix described under S1 and S2, ran 39,000, 27,000, 19,000, and
15,000 tasks/s. Each fetch reads the queue's entries in `tasks_fetch_idx`
from the front, where the entries of the tasks fetched before stay, marked
dead, until autovacuum removes them, at most once a minute; and until that
fix, each completion read all of `tasks_running_idx`, which grows the same
way.

### Latency

| ID | Target | Result |
|---|---|---|
| L1 | Pickup latency of an idle worker, from enqueue commit to handler start: p50 ≤ 5 ms, p99 ≤ 20 ms | p50 4.5 ms, p99 10 ms |
| L2 | Pickup latency at half of T3's throughput: p99 ≤ 100 ms | p50 19 ms, p99 44 ms, at 10,000 tasks/s |
| L3 | `Enqueue` call latency, one task, no load: p99 ≤ 5 ms | p50 0.4 ms, p99 0.5 ms |
| L4 | Start of a scheduled task after its run time: p99 ≤ 1.5 s | p50 37 ms, p99 426 ms, max 1.0 s, over 3,601 starts |
| L5 | A killed worker's task running again: ≤ lease duration + 2.5 s, revised from + 2 s | 5.06 s with a 3 s lease: the lease + 2.06 s |
| L6 | `Worker.Run` returning after its context is canceled: ≤ drain timeout + 6 s, revised from + 5 s | 7.01 s with a 2 s drain timeout and handlers that ignore their contexts: the drain timeout + 5.01 s |

L1 and L2 include the enqueue's own round trips: the send time is taken just
before the enqueue's transaction. L2 enqueued 10,000 tasks a second, half of
T3's target, in transactions of 10. L4 measured the starts of delayed tasks,
retries, snoozes, and schedule runs, each against its run time on the
database's clock.

**L5 and L6 were revised** by the margin that the design adds to them, which
the first targets left out:

- **L5.** A killed worker's lease expires one lease duration after its last
  renewal. The maintainer rescues expired leases once a second, and the
  rescued task then waits for its first retry, 1 second with 10% jitter. The
  worst case is the lease plus about 2.1 seconds; this run measured
  5.06 seconds with a 3-second lease, over 20 tasks.
- **L6.** At the drain timeout, `Run` cancels the attempts' contexts, waits
  the fixed 5 seconds of grace, then releases the tasks whose handlers still
  run, one statement each, and unregisters. With handlers that ignore their
  contexts, it returns just after the drain timeout plus 5 seconds; this run
  measured 7.01 seconds with a 2-second drain timeout.

### Scalability

| ID | Target | Result |
|---|---|---|
| S1 | T3 with 10 million scheduled and 1 million available tasks waiting: within 20% of T3 | Two runs: 14,869 tasks/s against 17,164 without them, 87%; and 16,257 against 18,539, 88%: pass |
| S2 | T3 with 100,000 FIFO groups holding pending tasks in another queue: within 10% of T3 | Two runs: 12,522 tasks/s against 17,164, 73%; and 16,671 against 18,539, 89.9%: just below target here |
| S3 | 100 queues served by 50 worker processes: maintainer cycle under 1 s | At 1,000 tasks/s, three runs: slowest loop 0.85 s once; promotion loop 1.28 s and 1.43 s in the others, with the wake loop at most 0.59 s and the main loop at most 0.24 s: fails, see below. Idle first: 154 transactions/s on average, 391 to 404 in the busiest 10 seconds |

S1 and S2 compare T3 end to end with and without the waiting tasks, which
were vacuumed, analyzed, and written to disk before the run, as tasks that
wait long are.

**S1 found a bug.** Every promotion of due tasks scanned the whole table:
past run times belong to available tasks too, so the statistics, which see
state and run time apart, suggested that many scheduled tasks were due, and a
sequential scan looked cheaper than the index. Each promotion took 1.2
seconds instead of 0.2 milliseconds. It is fixed; see the
[changelog](../CHANGELOG.md). With the fix, an earlier form of S1, which
drained backlogs, rose from 46% to 58% of T3.

**S1 and S2 found a second bug.** Recording an attempt's result and renewing
leases read `tasks_running_idx` whenever statistics, taken while few tasks
ran, made that index look small. Until vacuum, it keeps an entry for every
task that ran, so with millions of tasks waiting, a completion read about
4,000 pages instead of 240, and a heartbeat 8,600. They now look each task up
by its ID; see the [changelog](../CHANGELOG.md). Measured back to back on the
same database, S1 rose from 63% to 87% of T3, and S2 from 57% to 73%; a
second run with the fix measured 88% and 89.9%. Runs here move by up to a
fifth: T3 alone measured 17,164 to 20,859 tasks/s on the same code, so S2,
at the edge of its target, needs a run on the reference hardware.

**S3** runs one maintainer while 50 worker processes serve 100 queues each,
with concurrency 1. They idle first, and for 3 minutes after a 30-second
start the database ran 154 transactions a second on average in each of
three runs, and 391 to 404 in the busiest 10 seconds: the queues of
processes started together poll near the same moments of each minute.
Before 2026-10-27, every worker polled every queue once a second even while
notifications worked, and the same idle processes sent 5,600 transactions a
second, 1.2 of this database's 4 CPUs. Then 1,000 tasks a second arrive in
random queues, a fifth of them delayed, and a worker process is killed every
10 seconds. Each loop's iteration is the time of its statements in order;
the maintainer's wake, which sends its heartbeat too, runs in a loop of its
own. **At 1,000 tasks a second it fails in two runs of three, and the cause
is a design cost:** a notification wakes every worker that serves its
queue, so about 1,000 notifications a second each wake 50 processes, which
fetch, and all but one find nothing. That saturates this database's 4 CPUs,
and promotion, the slowest loop, takes up to 1.4 seconds. Before
2026-10-27, S3 ran at 100 tasks a second, and passed.

### Resource use

| ID | Target | Result |
|---|---|---|
| E1 | SQL statements per task under load, ungrouped: ≤ 3 | 0.13 while one worker drains a backlog; 0.16 end to end, enqueues included |
| E2 | WAL written per task, history kept: ≤ 2 KiB | 0.81 KiB from fetch to history |
| E3 | `tasks` heap and index size during a 4-hour run at half of T3: within 2× of their size after the first hour | 36 million tasks in 1 hour at 10,000 tasks/s, none left waiting: heap 167 MiB throughout, indexes 88 to 116 MiB; at most 283 MiB, 1.06× its size after 15 minutes: pass |
| E4 | Database connections: a client uses its pool only; a worker, its pool plus one `LISTEN` connection | A client with a pool of 8 used at most 8; a worker with a pool of 8 and 64 slots, at most 9, one of them listening: pass |
| E5 | Worker memory: idle ≤ 30 MiB RSS; ≤ 8 KiB per running task | 17.3 MiB idle; 7.9 KiB per running task, with 10,000 running: pass |
| E6 | An idle worker's load, revised on 2026-10-27: while the maintainer's heartbeats arrive, about one fetch per queue per minute, plus heartbeats; while they do not, one per queue per poll interval | Heartbeats arriving, 10 queues for 3 minutes: 55 fetches, 5.5 per queue, of which 3 or 4 at start; 17 worker heartbeats; 22 other statements: pass |

**E1 counts every statement,** including the `BEGIN`, settings, and `COMMIT`
that a fetch sends with it in one round trip (see
[Fetch](design/schema.md#fetch)), and in steady state the producers'
enqueues. While a worker drains a backlog, each fetch and completion serves
many tasks. With producers running at the same time, batches are smaller and
the count is higher.

**E2 leaves out the enqueue,** which writes WAL too. A run of a few seconds
also misses checkpoints, after which the first change to each page writes the
whole page to WAL. The end-to-end number on a long run is higher.

**E3 ran for 1 hour, not 4,** at 10,000 tasks a second, half of T3's target,
and compares the size after the hour with the size after its first quarter.
Completed tasks were discarded, so the disk held the `tasks` table only; the
table sees the same writes either way.

**E5** ran a worker process with concurrency 10,000, and measured its
resident memory idle and then with 10,000 tasks running, their handlers
waiting. The process is the benchmark program, a little bigger than a worker
alone. macOS counts resident memory a little differently from Linux.

**E6** counts the statements of a worker that serves 10 idle queues and stays
out of the maintainer election, while a maintainer runs in another worker.
Each queue fetches when `Run` starts, when the notification connection
starts, and every second until the maintainer's first heartbeat arrives;
then once a minute, give or take 6 seconds. Before 2026-10-27, an idle queue
polled every second: 316 fetches in 30 seconds here. Its target was revised
with the design: the poll interval now applies only while heartbeats do not
arrive. S3 measures the same at scale.

### Reliability under faults

These targets have zero tolerance: one violation blocks the release. Every
check is exact: it counts rows, or compares timestamps from the database's
clock, so a violation cannot hide in noise. Handlers record each call in a
table, so the counts include calls whose worker died before reporting.

| ID | Target | Result |
|---|---|---|
| F1 | 1 million tasks while a random worker is killed every 10 s: every task completes; none is lost or stuck | 60,000 completed, 0 left: pass |
| F2 | F1: every task appears exactly once in history | 60,000 distinct of 60,000: pass |
| F3 | F1: extra handler calls ≤ kills × worker concurrency | 417 extra, bound 976: pass |
| F4 | 10,000 FIFO groups of 100 tasks, 5% of attempts failing: no handler sees a task before the one ahead of it; nothing after a dead head runs | 1 million tasks: 0 calls before the task ahead completed, 0 after their own, 0 past a dead head; 993,523 completed, none twice; 137 dead heads, 6,340 tasks waiting behind them; each task accounted for, 0 lost; groups exact: pass |
| F5 | 1 million enqueues of 100,000 dedupe keys from 20 processes at once: exactly 100,000 tasks | 100,000 tasks; 100,000 enqueues reported a new task, 0 results named another task; FIFO groups exact: pass |
| F6 | PostgreSQL restarted during a run: workers resume within 10 s; no task lost | Shut down cleanly: back after 0.7 s, and a task fetched after it completed 0.17 s later. Crashed and down for 15 s: 0.41 s after it was back. 60,000 completed once, 0 left: pass |
| F7 | A worker cut off from the database for longer than its lease: its late result is rejected; the task completes once | 0 of 16 tasks completed by the cut worker's stale attempts; 116 completed once, 0 left; the cut worker ran new tasks on its own: pass |
| F8 | The integration suite behind PgBouncer in transaction mode: passes, with `LISTEN` on a direct connection | Every test passes through PgBouncer 1.25.2, after two fixes; CI checks it on every pull request: pass |
| F9 | Application servers' clocks 5 minutes off: no early or late starts beyond L4 | Database clock 5 minutes ahead: 0 early starts, p99 432 ms late, 0 leases lost. 5 minutes behind: 0 early, p99 677 ms, 0 leases lost: pass |

**F1 to F3** ran 60,000 tasks through 4 worker processes with 16 slots each, and
killed one with `SIGKILL` every second, ten times as often as the target. The
extra calls are the at-least-once guarantee at work: a killed worker's
running tasks run again after their leases expire.

**F4** also killed one of 4 worker processes every 2 seconds, which the
target does not ask for: 93 kills in 3 minutes, beside 52,283 failed
attempts. It allowed 3 attempts per task, so that some heads died and halted
their groups. Each call's start is compared with when the tasks of its
group completed: a call of a task before the one ahead of it completed, or
after its own task completed, is a violation, and so is any call past a dead
head. Each of the million tasks is checked, one by one, to have completed
once, to be a dead head that halts its group, or to wait behind one.

**F5** took 16 seconds. It gave every fifth key a FIFO group as well, and
the groups' counts stayed exact. No producer saw an error: the enqueues of
one key do not deadlock each other.

**F6** restarted PostgreSQL twice during the run: once shutting down cleanly,
and once killed with `SIGKILL` and kept down for 15 seconds, longer than the
maintainer's lease. Workers count as resumed once a task that they fetched
after the database was back completes; the results of attempts from before
the restart do not count.

**F7** cut a worker off with a proxy inside the benchmark, which held all its
traffic, as a network partition does. Half of its 16 handlers returned during
the cut, so their completions waited in the network; the other half returned
after it, once the worker had learned that it lost their leases. Another
worker took the tasks over and held them until after the late results had
arrived, so a late result accepted by mistake would have completed a task
whose current attempt was still running.

**F8 found two bugs,** both fixed; see the [changelog](../CHANGELOG.md).
Through PgBouncer with `max_prepared_statements`, `Migrate` failed on every
new schema: PgBouncer keeps the name of a statement whose parse failed, and
disconnects the client that prepares it again
([pgbouncer#1630](https://github.com/pgbouncer/pgbouncer/issues/1630)). And
`Migrate` from several processes at once could fail on a new schema, which
PgBouncer's shared, open connections made happen every time: a migration
that waited for another's lock trusted its connection's stale view that the
schema did not exist yet. It fails directly against PostgreSQL too, once the
connections are open. Four tests that need a session of their own, for a
session setting, a startup parameter, or `LISTEN`, now connect directly:
through a transaction pooler, a session setting leaks into other clients.
`make test-pgbouncer` runs the tests this way, and CI runs it on every pull
request.

**F9** ran the timing scenario (L4) against PostgreSQL under libfaketime,
its clock 5 minutes ahead of the application's, and again 5 minutes behind.
Every time taskmill sets comes from the database's clock, so retries,
snoozes, schedule runs, promotions, and leases kept time. A run time that the
application passes in comes from its own clock, by design (invariant 5 of the
[task lifecycle](design/task-lifecycle.md#invariants)): with the database
behind, the 1,000 tasks delayed by up to 20 seconds on the application's
clock waited 5 minutes more, as their run times said, and none started early.

### Limits of this run

- It ran on a shared laptop, not the reference hardware, with end-to-end
  runs of 60 seconds instead of 10 minutes, and a soak of 1 hour instead
  of 4.
- T1, T3 end to end, and S2 miss their targets here, S2 by a hair in one of
  its two runs, as described above. They need a run on the reference
  hardware before v1.0.0.
- L5 and L6 were revised, as described above, and E6 with the idle polling
  of 2026-10-27.
- S3 misses at 1,000 tasks a second, because each notification wakes every
  worker of its queue.

## Running the benchmarks

[`internal/cmd/bench`](../internal/cmd/bench) runs a scenario for every
target against a real database, and prints a Markdown report. It counts
statements with a pgx query tracer, WAL with `pg_current_wal_lsn()`,
connections with `pg_stat_activity`, and a worker process's memory with `ps`.
The fault scenarios start worker and producer processes, which are the same
program, kill them, restart the database, and cut a worker off from it with a
proxy inside the program. Every fault check compares counts or timestamps
from the database's clock exactly, and a failed check makes the program exit
with an error.

`make bench` runs every scenario against the database that `make db` starts.
That database has PostgreSQL's default settings, so its numbers are lower and
vary more than the ones above. For comparable numbers, start a database with
the same settings:

```sh
docker run -d --name taskmill-bench-db --cpus 4 -p 54330:5432 \
  -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=taskmill postgres:18-alpine \
  -c shared_buffers=1GB -c max_wal_size=8GB -c max_connections=700
TASKMILL_TEST_DATABASE_URL='postgres://postgres:postgres@localhost:54330/taskmill?sslmode=disable' \
  go run ./internal/cmd/bench -container taskmill-bench-db
```

`-container` names the database's Docker container, which the restart
scenario (F6) restarts; without it, F6 is skipped. The soak scenario (E3)
runs only when listed: `-scenarios soak -soak 4h`.
`go run ./internal/cmd/bench -help` lists the other flags, for single
scenarios, longer runs, or another database. The program recreates its own
schema, `taskmill_bench`.

`make faults` runs the fault scenarios as the nightly CI run does: F1 to F7
at a tenth of their size, and the timing scenario against databases whose
clocks are 5 minutes ahead and behind for F9. It starts those databases
itself and removes them afterwards;
[`faults.sh`](../internal/cmd/bench/faults.sh) shows how.
`make test-pgbouncer` runs the tests through PgBouncer for F8, against the
database that `make db` starts, and CI runs it on every pull request.

A database with a skewed clock runs PostgreSQL under libfaketime. Pass the
skew to the timing scenario, which then fails, rather than skips F9, when
the database's clock is not off by that much:

```sh
printf 'FROM postgres:18-alpine\nRUN apk add --no-cache libfaketime\n' | docker build -t taskmill-faketime -
docker run -d --name taskmill-bench-skew -p 54333:5432 \
  -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=taskmill \
  -e LD_PRELOAD=/usr/lib/faketime/libfaketime.so.1 -e FAKETIME=+5m -e DONT_FAKE_MONOTONIC=1 \
  taskmill-faketime
go run ./internal/cmd/bench -db 'postgres://postgres:postgres@localhost:54333/taskmill?sslmode=disable' \
  -scenarios timing -skew 5m
```
