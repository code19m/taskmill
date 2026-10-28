# Task lifecycle and guarantees

This document defines the states a task moves through, what moves it from one
state to another, and what taskmill guarantees along the way. It is the
contract that the implementation and its tests are written against. The
terms are defined in the guide's [Concepts](../guide.md#concepts). Two more
are used here:

- **Lease token:** a number that grows by one every time a task is fetched.
  Every change a worker makes to a task must present the current token.
- **Run time:** the earliest time a task may start (`RunAt`).

## States

| State | Meaning | Finished |
|---|---|---|
| `scheduled` | Waiting for its run time. | |
| `pending` | Waiting behind its FIFO group's head. | |
| `available` | Ready, waiting for a worker. | |
| `running` | Leased by an attempt. | |
| `retryable` | An attempt failed; waiting for its backoff to pass. | |
| `completed` | The handler succeeded. | Yes |
| `dead` | Will not run again: its attempts are used up, it failed with a permanent error, its payload does not decode, or no worker with a handler for its kind appeared in time. Dead tasks are kept for operators to inspect and retry. | Yes |
| `canceled` | Canceled on request. | Yes |

A finished task stays finished unless an operator retries it. Finished tasks
are deleted when their retention period ends.

Workers fetch only `available` tasks. Keeping `scheduled`, `pending`, and
`retryable` as separate states means the fetch index holds only tasks that
can run now, so millions of future or waiting tasks do not slow fetching
down. Keeping `retryable` apart from `scheduled` shows operators at a glance
how many tasks are failing.

## Transitions

```mermaid
flowchart TD
    enqueue((enqueue)) -->|run later| scheduled
    enqueue -->|run now| available
    enqueue -->|group busy| pending

    subgraph waiting [Unfinished]
        scheduled
        pending
        available
        retryable
    end

    scheduled -->|run time reached| available
    pending -->|its turn in the group| available
    retryable -->|backoff passed| available
    available -->|fetch| running

    running -->|failed, attempts left| retryable
    running -->|snoozed| scheduled
    running -->|released at shutdown| available

    subgraph finished [Finished]
        completed
        dead
        canceled
    end

    running -->|succeeded| completed
    running -->|failed, no attempts left| dead
    running -->|cancel requested| canceled
```

The diagram shows the main flow. It leaves out a few edges: a waiting task
can be canceled or deleted, an expired lease is rescued, an operator can
retry a dead or canceled task, and `Worker.RunTask` can start a scheduled or
retryable task at once. The table below is complete.

Each transition is atomic. Most are a single SQL statement. Those that
involve a FIFO group, and those that an operator starts, take a few
statements in one transaction (see [FIFO groups](schema.md#fifo-groups)).

| # | From | To | Trigger | By |
|---|---|---|---|---|
| 1 | | `available` | Enqueued with a run time that is now or in the past. | Client |
| 2 | | `scheduled` | Enqueued with a future run time. | Client |
| 3 | | `pending` | Enqueued into a FIFO group that already has an unfinished task, or that a dead head halts. | Client |
| 4 | `scheduled`, `retryable` | `available` | The run time is reached. | Maintainer |
| 5 | `pending` | `available` or `scheduled` | The task ahead of it in its group finished. It becomes `scheduled` if its run time is still in the future. | Whoever finished that task |
| 6 | `available`; also `scheduled` or `retryable` through `Worker.RunTask` | `running` | Fetched, or run at once by `Worker.RunTask`, which ignores the run time. Uses one attempt, issues a new lease token, and starts a lease. | Worker |
| 7 | `running` | `running` | A heartbeat extends the lease. | Worker |
| 8 | `running` | `completed` | The handler returned no error. | Worker |
| 9 | `running` | `retryable` | The handler failed and attempts remain. The run time becomes now plus a backoff. | Worker |
| 10 | `running` | `dead` | The handler failed with no attempts left, or returned a permanent error; the payload did not decode; or no worker with a handler for the kind appeared within 1 hour. | Worker |
| 11 | `running` | `scheduled`, or `available` for a snooze of zero or less | The handler snoozed, or the worker has no handler for the kind and waits for one that has. Gives the attempt back. | Worker |
| 12 | `running` | `available` | The worker released the task during graceful shutdown. Gives the attempt back. | Worker |
| 13 | `running` | `retryable`, `dead`, or `canceled` | The lease expired: the worker crashed or lost the database. Counts as a failed attempt. | Maintainer |
| 14 | `scheduled`, `pending`, `available`, `retryable` | `canceled` | Cancellation was requested. | Client |
| 15 | `running` | `canceled` | Cancellation was requested. The attempt's context is canceled, and the task becomes `canceled` when the attempt ends, however it ends, unless the handler completed it. | Worker |
| 16 | `dead`, `canceled` | `available` or `pending` | An operator retried the task. It gets a fresh set of attempts, and waits in `pending` if other tasks of its group are unfinished. | Client |
| 17 | Any finished state | Deleted | The retention period ended. A dead head that halts its group is kept. | Maintainer |
| 18 | Any state except `running` | Deleted | An operator deleted the task. | Client |

Notes on specific transitions:

- **3 and 5.** Only the head of a FIFO group can be fetched. The tasks behind
  it wait in `pending`, which no fetch reads, so a long group never slows
  fetching down. When the head finishes or is deleted, the next task of its
  group is promoted. The exception is a head that became dead: the group
  then halts, and nothing is promoted until an operator retries or deletes
  that task. A canceled head does not halt its group.
- **8.** A completed task moves to a history table, where transition 17
  deletes it after the retention period. A kind registered with
  `HandlerOptions.DiscardCompleted` keeps no history: transition 8 then
  deletes the task at once.
- **11.** A worker that has no handler for a task's kind snoozes it for
  1 minute at a time, without using an attempt. The first snooze records a
  note with the code `no_handler`. If no worker runs the task within 1 hour of
  that note, the task becomes dead with the code `unknown_kind`. A retry
  starts a new wait. This keeps tasks safe during rolling deploys, when some
  workers do not know a new kind yet.
- **13.** A lease that expires counts as a failed attempt on purpose. A task
  that crashes its worker every time (a "poison" task) must stop being
  retried at some point, not crash workers forever. A task whose
  cancellation was requested becomes `canceled` instead.
- **14, 15, and 18.** Canceling or deleting a task releases its dedupe key,
  so the same key can be enqueued again at once. A completed or dead task
  keeps its key until the key's window ends.
- **15.** If the handler succeeds before it notices the cancellation, the
  task becomes `completed`, because the work was done. Canceling one task of
  a batch does not cancel the batch's context: the task becomes `canceled`
  when the batch ends.
- **16.** Only `dead` and `canceled` tasks can be retried. To run completed
  work again, enqueue a new task, so that re-running work is always an
  explicit choice.
- **14, 16, and 18.** Bulk actions apply these transitions to every task
  that matches a filter, with the same rules, in batches that each commit
  whole.

## Invariants

These hold at all times. The schema's `CHECK` constraints enforce some of
them, and every test checks the FIFO group invariants when it ends.

1. A task is in exactly one state.
2. Only the attempt that holds the current lease token can move a task out of
   `running`. The one exception is transition 13, which applies only after the
   lease has expired.
3. A task in `running` always has a lease expiry time, and a task in any other
   state has none. A task's lease token only grows, so it never repeats.
4. Attempts used never exceed the maximum attempts, and the maximum is at
   least 1.
5. Every time taskmill sets itself, such as an attempt's start or a retry's
   run time, comes from the database clock (`now()`), never from an
   application server's clock. A run time that the application passes in
   comes from its own clock.
6. In a FIFO group, at most one task is neither `pending` nor finished: the
   group's head.

## Guarantees

Each guarantee names the mechanism that provides it.

1. **Enqueue is transactional and durable.** A task enqueued inside a database
   transaction exists only if that transaction commits, and workers cannot see
   it before then. Once committed, it is as durable as any other row in
   PostgreSQL. *Mechanism: the task is an ordinary row written in the caller's
   transaction.*
2. **No task is silently dropped.** A committed task keeps being run, at least
   once, until it completes or becomes dead, unless it is canceled or deleted
   first. A task ends without its handler running only when its payload does
   not decode, or when no worker with a handler for its kind appears in time.
   It then becomes dead, where operators can see it. *Mechanism: leases,
   rescue of expired leases (transition 13), and retries.*
3. **A lost worker does not lose tasks.** If a worker crashes or loses its
   connection, its tasks are rescued within one lease duration plus about one
   second, and then retried like any failed attempt. If the lost worker was the
   maintainer, add up to about 12 seconds for another worker to take over.
   *Mechanism: transition 13.*
4. **A stale attempt cannot change a task.** An attempt that lost its lease
   cannot complete, fail, or snooze the task, and its result is discarded. The
   worker learns this at its next heartbeat and cancels the attempt's context.
   *Mechanism: every worker update requires the current lease token.*
5. **Database writes can be exactly-once.** A handler that writes only to the
   same PostgreSQL database can complete the task inside its own transaction.
   The writes and the completion then commit together or not at all. This is
   opt-in: a handler that does not use it is completed by its worker as usual.
   *Mechanism: transition 8 runs in the handler's transaction and checks the
   lease token.*
6. **Retries are bounded and spread out.** A failed task is retried with
   exponential backoff and random jitter, up to its maximum attempts, and then
   becomes dead. The task keeps its 20 most recent errors. *Mechanism:
   transitions 9, 10, and 13.*
7. **Tasks start in a predictable order.** Within a queue, workers fetch tasks
   by priority (higher first), then by run time, then by enqueue order. This is
   the order in which tasks *start*. Tasks run concurrently, so they may finish
   in any order.
8. **FIFO groups keep their order.** The tasks of a group run one at a time,
   in enqueue order, and none runs past a dead head. *Mechanism: the `pending`
   state, the group's row lock, and halting.*
9. **A dedupe key admits one task per window.** Concurrent producers that
   enqueue the same key create one task; the others get a duplicate result.
   *Mechanism: the key is claimed in the enqueue statement.*
10. **Delayed tasks start on time, within limits.** While a maintainer runs, a
    task becomes available within about one second after its run time, and
    starts as soon as a worker has a free slot. A scheduled task's run time is
    the time it was scheduled for. This is not a real-time guarantee.
11. **Payloads arrive exactly as they were sent.** The payload is stored as
    JSON and decoded directly into the handler's type. Values such as large
    integers are never converted to floating point on the way.

### What taskmill does not guarantee

- **Exactly-once execution in general.** A handler can finish its work and then
  crash before the completion is recorded; the task then runs again. Handlers
  must be idempotent, or use transactional completion (guarantee 5).
- **No overlap between attempts.** If a worker is cut off from the database
  long enough for its lease to expire, a second attempt can start while the
  first is still running. Guarantee 4 ensures that only one of them can record
  a result.
- **A global order across workers.** See guarantee 7.
- **Survival of an unsynchronized failover.** With asynchronous replication, a
  PostgreSQL failover can lose recently committed transactions, including
  enqueued tasks. This is a property of the database setup, not of taskmill.

## Handler contract

taskmill's guarantees hold only if handlers follow these rules:

- **Be idempotent.** Running a task twice must have the same effect as
  running it once, or the handler must use transactional completion.
- **Respect the context.** Stop promptly when the context is canceled: the
  task was canceled, its lease was lost, its attempt timed out, or the worker
  is shutting down.
- **Return errors, do not panic.** taskmill recovers panics and records them as
  failed attempts, but a returned error carries better information.
- **Mark permanent failures.** Return a permanent error when retrying cannot
  help, such as a payload that fails validation, so the task does not use up
  its attempts for nothing.
