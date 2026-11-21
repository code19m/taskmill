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
| Retention | Every minute, in a loop of its own, deletes old dead and canceled tasks, expired dedupe keys, and activity older than a day in batches of 1,000, skipping locked rows. Dead heads that halt their groups are kept. Also deletes workers that made no check-in for 5 minutes, or for twice their `LeaseDuration` if that is longer, and schedules that no maintainer has defined for a day. |
| History | Every minute, keeps partitions for today and the next 6 days, and drops up to 10 partitions whose whole day is older than the retention period (see [Operational notes](schema.md#operational-notes)). |
| Statistics | Every 10 seconds, in a loop of its own, so that a long retention pass never delays it, counts each queue's tasks per state, finds each count's oldest due task, counts each queue's halted groups, finds the most failures of its retryable tasks, and stores all of these in `queue_stats` and the counts in the current minute of `activity`. [Queue statistics and health](schema-statistics.md) says how. |
| Election | An upsert on `leader` that succeeds only for the current leader, or when no other worker's lease lasts. |
| Schedule comparison | Every minute, in a loop of its own, reads the live workers whose stored schedules differ from its own, up to 10, in one statement. |

- **Retention stays on its index.** It finds the queues that have dead or
  canceled tasks with one probe each, and reads each queue's old tasks as a
  range of `tasks_finished_idx`: a few pages when no task is old enough. It
  looks up each old task's halt by its group's key.
- **A schedule's day counts from when a maintainer last defined it,** not
  from its last run or change. Each pass notes in `updated_at` that its
  maintainer defines a schedule, once the note is an hour old. So a rolling
  deploy or a rollback in which some workers do not define a schedule keeps
  its pause and its last run, and a schedule that is gone from the code is
  deleted 23 to 24 hours later. A pass skips a schedule's row that another
  transaction holds, so two maintainers never wait for each other.
- **The maintainer compares the workers' schedules, because a difference
  is silent.** It runs only its own schedules. A worker that lacks one
  stops it while it leads, and one that defines it in another way changes
  it. So each worker that takes part in the election stores, when it
  registers, the names of its schedules in `schedules`, and a hash of each
  one's definition in `schedule_hashes`. A worker's schedules never change,
  so the check-in writes nothing more. A worker with `NoMaintainer`, and one
  of a release without the columns, store `NULL`, and are not compared.
- **Two definitions hash the same exactly when their runs happen at the
  same times and enqueue the same task.** The hash covers the expression,
  the time zone, the queue, the kind, the priority, the most attempts, the
  FIFO group, and the payload as jsonb holds it, with its numbers in one
  form and its keys in any order. A payload that changes in every encoding,
  such as one that holds the time, changes the stored row in every pass
  anyway, and is left out. The metadata is left out too: it may hold values
  of each process. A zone named Local is each process's own: its offsets
  count too.
- **A difference is logged once it has lasted longer than a rolling deploy
  takes,** again now and then while it lasts, and its end once:
  [Fixed timings and limits](../reference/defaults-and-limits.md#the-maintainer)
  has the times. A rolling deploy that changes a schedule makes one for a
  few minutes, which needs no person. A new term counts again from its
  first comparison. The dashboard compares the names alone, which
  `WorkerInfo` lists: the hashes are no API. We rejected one hash for all
  the schedules, which cannot say which one differs, and a mark on the
  dashboard alone, which nobody watches.
- **The database's clock decides the history's days:** which days need a
  partition, and which partitions have expired, in UTC.
- **No statement reads the session's time zone.** Each one compares moments,
  and where it needs a day, it takes the clock's time in UTC. A period that
  holds days, as the default retention periods do, is subtracted from that
  time: subtracted from `now()`, a day of a period is a day of the session's
  zone, 23 or 25 hours long when its clocks change. An error's time is
  stored as text, so it is written in UTC too. The tests run in sessions 24
  hours and 45 minutes ahead of UTC, where a statement that reads the zone
  gets another day at any time of day.
- **Retention drops only the history that it detached.** A pass marks a
  partition with a comment before it detaches it. It drops a table that is
  no partition only with that mark, so one that an operator detached stays.
  A later pass finishes what failed: see [`history.go`](../../history.go).
- **Expired history leaves in an order that cannot get stuck.** PostgreSQL
  detaches one partition of a table at a time, so an interrupted detach
  comes first, and is finished. Then come the attached partitions, the
  oldest first, so an interrupted detach is always the oldest one's: when
  the retention grows, it is the last to leave the list. In another order
  it could leave while older partitions stay, and none of them could be
  detached. The detached tables come last, in random order. Some cannot be
  dropped, such as one that a view depends on, and no state says which: in
  a fixed order, enough of them would fill every pass, and keep the others
  for good.
- **The cutoff of the history is a day of our era, and not after today.**
  The settings refuse a period of more than 300 years, and one with a
  negative field. `to_char` prints a year before our era without a sign, so
  such a cutoff would expire every partition, as a cutoff after today does.
- **A failed election try writes nothing.** The upsert proposes its row only
  if no other worker's lease lasts. A conflict would lock the leader's row,
  and make every try of every other worker a write.
- **A lease that no worker writes is no lease:** one that ends later than
  two leases from now, such as a row written by hand. Otherwise it would
  keep every worker from leading: see
  [Rows that taskmill did not write](schema-rows.md).
- **A worker that stops deletes its lease,** also when it does not know that
  it holds one, as after a renewal whose answer was lost. The same statement
  clears the schedules that it stored: it checks in while it drains its
  tasks, which can take long, but no longer takes part in the election.
- **It waits for its election first.** A worker that is told to stop gives
  up the statements that it has sent, and the database may still run one of
  them later. An election that ran after the lease was deleted would give
  the role to a worker that has stopped, and nobody would lead until that
  lease ran out. So the election does not end when the worker stops: the
  worker waits for its answer, starts no other election, and then deletes
  the lease. It waits up to 5 seconds, as long as the deletion itself may
  take, so that a database that does not answer cannot hold the stop up
  for longer. Then it gives the election up. Should the database run it
  only after that, the lease lasts until it runs out, as a crashed
  worker's does.
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

**A notification wakes every worker of its queue, and that stays.**
`LISTEN` delivers each notification to every connection on the channel, so
every idle worker of the queue fetches, and all but the first few find
nothing. Waking one worker would need a coordinator that knows which
workers are idle: a new part, with its own failures. An empty fetch is
one round trip and one probe of the queue's index, and the limits above
bound the wakes: one notification per queue per 50 ms from each client, and
a worker that found nothing waits 20 ms. The cost grows with the workers of
one queue: S3 in [Benchmarks](benchmarks.md#scalability) measured 50
processes on 100 queues at 1,000 tasks a second, and their empty fetches
used the database's 4 CPUs. So on one queue, a few workers with more
concurrency cost the database less than many small ones.

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
