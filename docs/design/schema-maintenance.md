# Maintenance and notifications

This page explains the work that the maintainer does, how it counts the
queues, and how notifications and polling wake the workers.
[Storage schema](schema.md) has the tables.

## Maintenance

| Step | What it does |
|---|---|
| Promote | Makes up to 1,000 due `scheduled` and `retryable` tasks `available` per statement, through `tasks_due_idx`, and notifies their queues in the same statement. It runs on its own, so a slow step never delays it. |
| Rescue | Every second, fails up to 1,000 attempts per statement whose leases expired. It uses the default backoff, because the maintainer does not know each kind's retry policy, and caps the exponent so that a task with thousands of attempts cannot overflow it. |
| Retention | Every minute, deletes old dead and canceled tasks, expired dedupe keys, and activity older than a day in batches of 1,000, skipping locked rows. Dead heads that halt their groups are kept. Also deletes workers that have not checked in for 5 minutes, and schedules that the maintainer does not define and that did not run or change for a day. |
| History | Keeps partitions for today and the next 6 days, and drops a partition once its whole day is older than the retention period (see [Operational notes](schema.md#operational-notes)). |
| Statistics | Every 10 seconds, counts each queue's tasks per state, finds each count's oldest due task, counts each queue's halted groups, finds the most failures of its retryable tasks, and stores all of these in `queue_stats` and the counts in the current minute of `activity`. |
| Election | An upsert on `leader` that succeeds only for the current leader, or when the lease has expired. |

- **Retention stays on its index.** It finds the queues that have dead or
  canceled tasks with one probe each, and reads each queue's old tasks as a
  range of `tasks_finished_idx`: a few pages when no task is old enough. It
  looks up each old task's halt by its group's key.
- **The database's clock decides the history's days:** which days need a
  partition, and which partitions have expired, in UTC.
- **A failed election try writes nothing.** The upsert proposes its row only
  if no other worker's lease lasts. A conflict would lock the leader's row,
  and make every try of every other worker a write.
- **A worker that stops deletes its lease,** also when it does not know that
  it holds one, as after a renewal whose answer was lost.

### Statistics

**Statistics read indexes, and every count is capped,** so the cost stays
bounded however large the backlog:

- **Caps.** A count stops at 10,000 tasks per queue and state, and at
  100,000 scheduled and retryable tasks in all. The one exception, below,
  reads up to 10,000 retryable tasks, and one more per queue.
- **JIT is off.** The count runs in the transaction that stores it. Its many
  subqueries make the planner think it costly, and compiling it would take
  longer than running it.
- **One probe per queue.** A recursive query finds the next queue name with
  one index probe each, a technique called a loose index scan. So finding
  the queues costs one probe per queue, not one row per task.
- **Scheduled and retryable tasks are counted together.** They are read from
  `tasks_due_idx`, which is ordered by run time, not by queue. Then each
  queue's count stops at 10,000 too.
- **A capped count is a lower bound.** Each cap is read one row past, so a
  count that stops at its cap with more tasks left is stored with
  `capped = true`. A count of exactly 10,000 is not capped. Once the shared
  cap is passed, every queue's scheduled and retryable counts are capped,
  stored as capped zeros where none were counted. Its other counts keep
  their own flags.

**Lag comes with the counts.** Each row of `queue_stats` keeps, in
`oldest_due_at`, the run time of the oldest due task it counted:

- For `available`, it is the first entry of `tasks_fetch_idx` per priority:
  one more probe per queue and priority.
- For `scheduled` and `retryable`, it is the oldest task whose run time has
  passed. The count meets that task first, because it reads `tasks_due_idx`
  in run time order.

A queue's lag is then the age of its oldest `oldest_due_at` by the
database's clock. `Client.Queues` computes it without touching `tasks`. Lag
keeps growing when no maintainer runs, and can be too high for up to one
refresh. The maintainer also stores a zero count for each known queue
without tasks, so every queue shows when it was last counted.

Only if more than 100,000 scheduled and retryable tasks are overdue can a
queue's overdue tasks fall past the cap. The oldest ones, which set the
largest lag, are always read.

**Stuck work that lag leaves out comes with the counts too**
([ADR 0007](../adr/0007-count-stuck-work-with-the-queue-statistics.md)):

- **Halted groups.** Each queue's `dead` row keeps, in `halted_groups`, how
  many of its groups a dead head halts. They are counted through one of the
  indexes of halted groups, up to 10,000: one more probe per queue with dead
  or canceled tasks. The tasks behind them are `pending`, which lag leaves
  out.
- **Most failures.** Each queue's `retryable` row keeps, in `most_failures`,
  the highest `attempt` of its retryable tasks: the most times in a row that
  one of them has failed. Such a task waits for its next retry, so lag
  leaves it out until the retry is due.
- **How most failures are read.** `attempt` is not in `tasks_due_idx`, so
  the count reads it with one probe by run time per task. It does so for the
  10,000 counted retryable tasks that retry last, and for each queue's task
  that retries last. Backoff grows with failures, so the tasks that failed
  most retry last, and the new failures of an outage retry first. The count
  finds them by reading `tasks_due_idx` backward from the last retryable
  task it counted, so the scheduled tasks after it cost nothing. Measured on
  PostgreSQL 18 with JIT off and 1.7 million tasks, 150,000 of them
  retryable: the count took about 15 ms longer.

## Notifications

When an enqueue or a promotion makes tasks available, taskmill sends
`pg_notify` on a channel named after the schema, so idle workers fetch right
away instead of waiting for their next poll. Notifications are delivered
only when the transaction commits. The channel carries five payloads:

| Payload | Meaning |
|---|---|
| `a\|<queue>` | Tasks became available in the queue |
| `c\|<id>` | The cancellation of the running task `<id>` was requested |
| `q\|<queue>` | The queue's settings changed |
| `h\|` | The maintainer's heartbeat |
| `r\|` | The maintainer resigned, so that another worker takes over right away |

**A notification is only a hint.** PostgreSQL lets every role of a database
send one, as often as it likes, also a role with no rights on taskmill's
tables. So a worker changes nothing because of a notification alone, and
never reads the database where it receives them:

- **`a|` only wakes a fetch,** which reads the tables. A worker whose fetch
  found nothing pauses 20 ms before it acts on the next one.
- **`c|` and `q|` only mark what they name.** One loop for each then reads
  the truth: the cancel requests in the rows of the marked tasks, by ID and
  lease token, in one statement for all of them, and the queues' settings.
  Each loop pauses 20 ms after a read. So a flood of notifications costs a
  worker about 50 reads of each kind a second, and `Cancel` still stops a
  running task within that time.
- **Only an ID in its canonical form counts.** PostgreSQL delivers equal
  notifications of one transaction once, and other ways to write the same
  number, such as `+7` or `007`, would get around that.
- **The task's row decides a cancellation.** The statements that record an
  attempt cancel the task only if its row has the request. If the read after
  a `c|` fails, the next check-in reads the requests.

**At most one notification per queue per 50 ms.** Each notifying transaction
takes a global lock at commit. At high commit rates this lock becomes a
bottleneck: in a pgbench test before taskmill had any Go code, a `NOTIFY` in
every enqueue capped throughput near 10,000 tasks per second. So a client
sends the first notification in its own transaction, and one for the rest
when the 50 ms end.

**The maintainer wakes the queues that were missed.** That last notification
can arrive before the transactions it stands for commit, and a producer can
crash before sending it. So once a second, the maintainer also notifies
every queue that is not paused and whose next task has waited longer than a
second, in one statement with one index probe per queue. A queue whose tasks
start right away gets nothing, and one whose workers are all busy gets a
notification that changes nothing.

**The heartbeat shows that this works.** Every 5 seconds, the same statement
sends `h|`, which shows workers that the wake ran and that notifications
reach them. A new maintainer sends one right away. PostgreSQL wakes every
connection that listens in a database for each notification, on any
channel, so schemas that share a database pay for each other's: E7 in
[Benchmarks](benchmarks.md#resource-use) measures what heartbeats cost then.
Hence a heartbeat every 5 seconds, not with every wake.

### Polling

Workers poll each queue as a fallback. How often depends on the maintainer's
heartbeats:

| Situation | An idle queue polls | A task whose notification was lost starts within |
|---|---|---|
| Heartbeats arrive | Once a minute | About 2 seconds: the maintainer wakes its queue |
| No heartbeat came for 15 seconds, three missed | Every `PollInterval`, starting right away | About 16 seconds after the last heartbeat. From then on, `PollInterval` |
| A `MaxRunning` limit blocked the queue's last fetch | Every `PollInterval` | `PollInterval`: finishing a task sends no notification |

- **While heartbeats arrive,** notifications reach the worker, and the
  maintainer wakes every queue whose task waits. The poll once a minute only
  bounds the harm of a fault in either. So idle workers cost little, however
  many queues and replicas there are.
- **When heartbeats stop,** whatever the reason (no maintainer, or a
  connection that died silently), the worker also checks that its
  notification connection answers, by listening again. It replaces one that
  does not.
- **A maintainer that stops hands over without such a gap.** It resigns with
  `r|`, and another worker takes over and sends its heartbeat.
- **A new connection makes every queue fetch right away,** since
  notifications may have been missed meanwhile.

### The notification connection

`LISTEN` needs a session, so each worker holds one dedicated connection for
it, made with the pool's settings unless `WorkerConfig.ListenConfig` says
otherwise. Behind a pooler in transaction mode, `ListenConfig` must point at
the database directly. A worker checks that when it starts:

- Through a direct connection, or a pooler that keeps sessions, the
  connection always runs on one server process, which the pool never gets.
- Through a pooler in transaction mode, either changes within a few
  statements. The worker then logs an error and polls instead of listening,
  since `LISTEN` would stay on a server connection that goes back to the
  pooler's pool.

All other traffic works through such poolers.

## See also

- [Storage schema](schema.md): the tables `queue_stats`, `activity`, and `leader`
- [ADR 0006](../adr/0006-health-and-scaling-read-paths.md) and [ADR 0007](../adr/0007-count-stuck-work-with-the-queue-statistics.md): why the statistics look like this
- [Defaults and limits](../reference/defaults-and-limits.md#the-maintainer): the maintainer's timings

**Next:** [Benchmarks](benchmarks.md)
