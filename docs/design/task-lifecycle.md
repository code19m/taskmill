# Task lifecycle and guarantees

- Status: Draft
- Milestone: M1

This document defines the states a task moves through, what moves it from one
state to another, and what taskmill guarantees along the way. It is the
contract that the implementation and its tests are written against.

## Terms

| Term | Meaning |
|---|---|
| Task | A unit of work: a kind, a JSON payload, and options such as priority. |
| Kind | The name that selects the handler for a task, such as `email.send`. |
| Queue | A named stream of tasks. A worker fetches from the queues it is configured for. |
| Attempt | One run of a task's handler. |
| Lease | The right of one attempt to run a task. It expires unless the worker renews it with heartbeats. |
| Lease token | A value that changes every time a task is fetched. Every update a worker makes must present it. |
| Run time | The earliest time a task may start (`run_at`). |
| Deadline | Optional. The latest time a task may start. |
| Client | The API an application uses to enqueue, cancel, and retry tasks. |
| Worker | The process component that fetches tasks and runs their handlers. |
| Maintainer | Background work done by one elected worker process: promoting due tasks, rescuing expired leases, deleting old tasks. |

## States

| State | Meaning | Terminal |
|---|---|---|
| `scheduled` | Waiting for its run time. | |
| `available` | Due, and waiting for a worker. | |
| `running` | Leased by an attempt. | |
| `retryable` | An attempt failed; waiting for its backoff to pass. | |
| `completed` | The handler succeeded. | Yes |
| `dead` | Will not run again: its attempts are used up, it failed with a permanent error, or its deadline passed. Dead tasks form the dead-letter queue. | Yes |
| `cancelled` | Stopped on request. | Yes |

A terminal task stays terminal unless an operator retries it. Terminal tasks
are deleted when their retention period ends.

Workers fetch only `available` tasks. Keeping `scheduled` and `retryable` as
separate states means the fetch index holds only tasks that can run now, so
millions of future tasks do not slow fetching down. Keeping `retryable` apart
from `scheduled` shows operators at a glance how many tasks are failing.

## Transitions

```mermaid
flowchart TD
    enqueue((enqueue)) -->|run later| scheduled
    enqueue -->|run now| available

    subgraph waiting [Waiting]
        scheduled
        available
        retryable
    end

    scheduled -->|run time reached| available
    retryable -->|backoff passed| available
    available -->|fetch| running

    running -->|failed, attempts left| retryable
    running -->|snoozed| scheduled
    running -->|released at shutdown| available

    subgraph terminal [Terminal]
        completed
        dead
        cancelled
    end

    running -->|succeeded| completed
    running -->|failed, no attempts left| dead
    running -->|cancel requested| cancelled
```

The diagram shows the main flow. For readability it leaves out a few edges:
a waiting task can also be cancelled or pass its deadline, an expired lease is
rescued, and an operator can retry a dead or cancelled task. The table below
is complete and is the authoritative definition. Each transition is a single
atomic SQL statement.

| # | From | To | Trigger | By |
|---|---|---|---|---|
| 1 | | `available` | Enqueued with a run time that is now or in the past. | Client |
| 2 | | `scheduled` | Enqueued with a future run time. | Client |
| 3 | `scheduled`, `retryable` | `available` | The run time is reached. | Maintainer |
| 4 | `available` | `running` | Fetched. Uses one attempt, issues a new lease token, and starts a lease. | Worker |
| 5 | `running` | `running` | A heartbeat extends the lease. | Worker |
| 6 | `running` | `completed` | The handler returned no error. | Worker |
| 7 | `running` | `retryable` | The handler failed and attempts remain. The run time becomes now plus a backoff. | Worker |
| 8 | `running` | `dead` | The handler failed with no attempts left, or returned a permanent error. | Worker |
| 9 | `running` | `scheduled` | The handler asked to run again later (snooze). Gives the attempt back. | Worker |
| 10 | `running` | `available` | The worker released the task during graceful shutdown. Gives the attempt back. | Worker |
| 11 | `running` | `retryable` or `dead` | The lease expired: the worker crashed or lost the database. Counts as a failed attempt. | Maintainer |
| 12 | `scheduled`, `available`, `retryable` | `dead` | The deadline passed before the task started. | Maintainer |
| 13 | `scheduled`, `available`, `retryable` | `cancelled` | Cancellation was requested. | Client |
| 14 | `running` | `cancelled` | Cancellation was requested. The attempt's context is cancelled, and the task becomes `cancelled` when the attempt ends. | Worker |
| 15 | `dead`, `cancelled` | `available` | An operator retried the task. It gets a fresh set of attempts. | Client |
| 16 | Any terminal state | Deleted | The retention period ended. | Maintainer |

Notes on specific transitions:

- **4 and 12.** The fetch query skips tasks whose deadline has passed, so a
  worker never starts a task late, even before the maintainer marks it dead.
- **6 and 16.** A queue can be set to delete completed tasks at once instead
  of keeping them for a retention period (requirement R10 in the
  [scope document](scope.md)). Such a task is deleted by transition 6 itself
  and never stays in `completed`.
- **9.** A worker that has no handler for a task's kind snoozes it without
  using an attempt, until a time limit passes, and then makes it dead. This
  keeps tasks safe during rolling deploys, when some workers do not know a new
  kind yet (requirement R5).
- **11.** A lease that expires counts as a failed attempt on purpose. A task
  that crashes its worker every time (a "poison" task) must eventually stop
  being retried, not crash workers forever.
- **14.** If the handler succeeds before it notices the cancellation, the task
  becomes `completed`, because the work was done. If the lease expires while
  cancellation is pending, the task becomes `cancelled`.

## Invariants

These hold at all times. Tests check them after every operation.

1. A task is in exactly one state.
2. Only the attempt that holds the current lease token can move a task out of
   `running`. The one exception is transition 11, which applies only after the
   lease has expired.
3. A task in `running` always has a lease token and a lease expiry time; a task
   in any other state has neither.
4. Attempts used never exceed the maximum attempts, and the maximum is at
   least 1.
5. Every time taskmill records comes from the database clock (`now()`), never
   from an application server's clock.

## Guarantees

Each guarantee names the mechanism that provides it.

1. **Enqueue is transactional and durable.** A task enqueued inside a database
   transaction exists only if that transaction commits, and workers cannot see
   it before then. Once committed, it is as durable as any other row in
   PostgreSQL. *Mechanism: the task is an ordinary row written in the caller's
   transaction.*

2. **No task is silently dropped.** A committed task keeps being run, at least
   once, until it completes or becomes dead, unless it is cancelled first. The
   only way a task ends without running is a deadline that passes before it
   starts, and it then becomes dead, where operators can see it. *Mechanism:
   leases, rescue of expired leases (transition 11), and retries.*

3. **A lost worker does not lose tasks.** If a worker crashes or loses its
   connection, its tasks are rescued within one lease duration plus one
   maintainer interval, and then retried like any failed attempt.
   *Mechanism: transition 11.*

4. **A stale attempt cannot change a task.** An attempt that lost its lease
   cannot complete, fail, or snooze the task, and its result is discarded. The
   worker learns this at its next heartbeat and cancels the attempt's context.
   *Mechanism: every worker update requires the current lease token.*

5. **Database writes can be exactly-once.** A handler that writes only to the
   same PostgreSQL database can complete the task inside its own transaction.
   The writes and the completion then commit together or not at all. This is
   opt-in: a handler that does not use it is completed by its worker as usual.
   *Mechanism: transition 6 runs in the handler's transaction and checks the
   lease token.*

6. **Retries are bounded and spread out.** A failed task is retried with
   exponential backoff and random jitter, up to its maximum attempts, and then
   becomes dead. The error of every attempt is kept with the task (the most
   recent ones, up to a fixed limit). *Mechanism: transitions 7, 8 and 11.*

7. **Tasks start in a predictable order.** Within a queue, workers fetch tasks
   by priority (higher first), then by run time, then by enqueue order. This is
   the order in which tasks *start*. Tasks run concurrently, so they may finish
   in any order. Strict one-at-a-time ordering per key is provided by FIFO
   groups (milestone M4).

8. **No task starts after its deadline.** *Mechanism: the fetch query filters
   on the deadline, and transition 12.*

9. **Delayed tasks start on time, within limits.** While a maintainer is
   running, a task becomes available within one maintainer interval after its
   run time (target: one second or less), and starts as soon as a worker has
   capacity. This is not a real-time guarantee.

10. **Payloads arrive exactly as they were sent.** The payload is stored as
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

- **Be idempotent.** Running a task twice must be safe, or the handler must use
  transactional completion.
- **Respect the context.** Stop promptly when the context is cancelled. A
  cancelled context means the task was cancelled, its lease was lost, or the
  worker is shutting down.
- **Return errors, do not panic.** taskmill recovers panics and records them as
  failed attempts, but a returned error carries better information.
- **Mark permanent failures.** Return a permanent error when retrying cannot
  help, such as a payload that fails validation, so the task does not use up
  its attempts for nothing.

## Decisions

These questions were settled while drafting this document.

1. **A dead task in a FIFO group.** By default the rest of the group continues
   with its next task. A per-group option halts the group instead, until an
   operator retries or deletes the dead task. Use it for streams where
   skipping a task would corrupt data, such as ledger events.
2. **A passed deadline makes a task `dead`, not `cancelled`.** Missed deadlines
   then show up in the dead-letter queue and in alerts, because they usually
   signal a backlog that operators should know about.
3. **Completed tasks cannot be retried in v1.** Only `dead` and `cancelled`
   tasks can (transition 15). To run the same work again, enqueue a new task.
   This keeps re-running work an explicit choice.
