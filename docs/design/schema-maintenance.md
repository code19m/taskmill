# Maintenance and notifications

This page explains the work that the maintainer does, and how notifications
and polling wake the workers. [Storage schema](schema.md) has the tables,
and [Queue statistics and health](schema-statistics.md) says how the
maintainer counts the queues.

## Maintenance

| Step | What it does |
|---|---|
| Promote | Makes up to 1,000 due `scheduled` and `retryable` tasks `available` per statement, through `tasks_due_idx`, and notifies their queues in the same statement. It runs on its own, so a slow step never delays it. |
| Rescue | Every second, fails up to 1,000 attempts per statement whose leases expired. It uses the default backoff, because the maintainer does not know each kind's retry policy, and caps the exponent so that a task with thousands of attempts cannot overflow it. |
| Retention | Every minute, in a loop of its own, deletes old dead and canceled tasks, expired dedupe keys, and activity older than a day in batches of 1,000, skipping locked rows. Dead heads that halt their groups are kept. Also deletes workers that made no check-in for 5 minutes, or for twice their `LeaseDuration` if that is longer, and schedules that the maintainer does not define and that did not run or change for a day. |
| History | Every minute, keeps partitions for today and the next 6 days, and drops up to 10 partitions whose whole day is older than the retention period (see [Operational notes](schema.md#operational-notes)). |
| Statistics | Every 10 seconds, in a loop of its own, so that a long retention pass never delays it, counts each queue's tasks per state, finds each count's oldest due task, counts each queue's halted groups, finds the most failures of its retryable tasks, and stores all of these in `queue_stats` and the counts in the current minute of `activity`. [Queue statistics and health](schema-statistics.md) says how. |
| Election | An upsert on `leader` that succeeds only for the current leader, or when no other worker's lease lasts. |

- **Retention stays on its index.** It finds the queues that have dead or
  canceled tasks with one probe each, and reads each queue's old tasks as a
  range of `tasks_finished_idx`: a few pages when no task is old enough. It
  looks up each old task's halt by its group's key.
- **The database's clock decides the history's days:** which days need a
  partition, and which partitions have expired, in UTC.
- **Retention drops only the history that it detached.** A pass marks a
  partition with a comment before it detaches it. It drops a table that is
  no partition only with that mark, so one that an operator detached stays.
  A later pass finishes what failed: see [`history.go`](../../history.go).
- **A failed election try writes nothing.** The upsert proposes its row only
  if no other worker's lease lasts. A conflict would lock the leader's row,
  and make every try of every other worker a write.
- **A lease that no worker writes is no lease:** one that ends later than
  two leases from now, such as a row written by hand. Otherwise it would
  keep every worker from leading: see
  [Rows that taskmill did not write](schema-rows.md).
- **A worker that stops deletes its lease,** also when it does not know that
  it holds one, as after a renewal whose answer was lost.
- **A worker's silence is judged by its own lease.** A worker checks in
  every third of its `LeaseDuration`, and stores the lease in its row. After
  one lease without a check-in it is stale: its tasks' leases have run out.
  `Client.Workers` works that out by the database's clock. Retention deletes
  it after two leases, but not before 5 minutes. A worker of a release that
  stores no lease is stale after 30 seconds, and deleted after 5 minutes.

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

- **`a|` only wakes a fetch,** which reads the tables. After a fetch that
  found nothing, the next `a|` waits 20 ms. A worker's own tasks do not.
- **`c|` and `q|` only mark what they name.** One loop for each then reads
  the truth: the cancel requests in the rows of the marked tasks, by ID and
  lease token, in one statement for all of them, and the queues' settings.
  Each loop pauses 20 ms after a read. So a flood of notifications costs a
  worker about 50 reads of each kind a second, and `Cancel` still stops a
  running task within that time.
- **`r|` only moves a worker's next election try forward.** A worker that
  leads ignores it. Another worker follows it at most once per election
  interval of 2 seconds. So a flood adds at most one try per interval to
  the one that each worker makes anyway, and a try writes nothing while the
  maintainer's lease lasts.
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
| A `MaxRunning` limit blocked the queue's last fetch | Every `PollInterval` | `PollInterval`, when another worker's task made the room: finishing a task sends no notification |

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
  notifications may have been missed meanwhile. The worker also reads its
  queues' settings again then: it may have missed a pause.

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

- [Storage schema](schema.md): the tables `workers`, `leader`, and `schedules`
- [Queue statistics and health](schema-statistics.md): how the maintainer counts the queues
- [Fixed timings and limits](../reference/defaults-and-limits.md#the-maintainer): the maintainer's timings

**Next:** [Queue statistics and health](schema-statistics.md)
