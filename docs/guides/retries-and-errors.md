# Retries and errors

When a handler fails, taskmill runs the task again later. This page shows
how long it waits, how to change that, and what to do with a task that
fails for good.

## How retries work

Each failed attempt leads to a wait, or to a dead task:

```mermaid
flowchart TD
    A["An attempt runs"] --> B{"What did the handler return?"}
    B -->|nil| C["completed"]
    B -->|an error| D{"Attempts left?"}
    B -->|"Permanent(err)"| E["dead"]
    D -->|yes| F["retryable: waits 1 s, 2 s, 4 s ... up to 1 h"]
    D -->|no| E
    F -->|the wait is over| A
```

- After a failed attempt, the task is `retryable`. It waits, then runs
  again.
- The wait, or [backoff](../concepts/glossary.md#backoff), doubles after each
  failure: 1 second, 2, 4, and so on, up to 1 hour. A small random change
  of ±10% spreads the retries, so failed tasks don't all come back at the
  same moment.
- After the last attempt, 25 by default, the task becomes dead.
- The [maintainer](../concepts/glossary.md#maintainer) moves waiting tasks back into
  their queue when their time comes. Without a maintainer, retries wait.

## How long does a failing task wait?

With the defaults:

| After failed attempt | Wait before the next attempt |
|---|---|
| 1 | 1 second |
| 2 | 2 seconds |
| 3 | 4 seconds |
| 4, 5, 6 | 8, 16, and 32 seconds |
| 7, 8, 9 | About 1, 2, and 4 minutes |
| 10, 11, 12 | About 9, 17, and 34 minutes |
| 13 to 24 | 1 hour each |
| 25 | None: the task is dead |
| **In total** | **About 13 hours** |

Each wait varies by ±10%, and a retry can start up to about a second late.

## Which should I use?

| Situation | Use | Why |
|---|---|---|
| The work must happen in the end: a receipt, a sync with a partner | The defaults: 25 attempts over about 13 hours | The task outlasts an outage of several hours |
| Late work is useless: a login code, a "your order shipped" email | A lower `MaxAttempts` for the kind, such as 3 or 5 | The task stops trying after seconds |
| The work must never run again after a failure | `MaxAttempts: 1` | One try, then dead |
| A retry can't help: bad input | `return taskmill.Permanent(err)` | Dead right away; no attempts wasted |
| What you call recovers on a known time scale | `ExponentialBackoff` with your own `Base` and `Max` | Retries match the recovery time |
| A fixed wait, or a timetable of your own | Your own `RetryPolicy` | Full control |

Most kinds need only the defaults.

## Set the number of attempts per kind

Set `MaxAttempts` in the kind's `EnqueueDefaults`:

```go
func (SendLoginCode) EnqueueDefaults() taskmill.EnqueueOptions {
    return taskmill.EnqueueOptions{MaxAttempts: 3} // a late code is useless
}
```

- `MaxAttempts` is saved with each task when it is enqueued. Set on the
  kind, it is the same for every producer. One enqueue call can still
  change it.
- It can be 1 to 10,000.
- When someone retries a dead task, it gets all its attempts again.

## Change the wait between retries

Set `Retry` in the kind's `HandlerOptions`:

```go
taskmill.AddHandler(&handlers, &PartnerSync{}, &taskmill.HandlerOptions{
    Retry: taskmill.ExponentialBackoff{Base: 10 * time.Second, Max: 30 * time.Minute},
})
```

For a wait of your own, implement `RetryPolicy`. `Backoff` gets the number
of the attempt that failed, from 1:

```go
// EveryFiveMinutes waits 5 minutes before every retry.
type EveryFiveMinutes struct{}

func (EveryFiveMinutes) Backoff(attempt int) time.Duration { return 5 * time.Minute }
```

> [!NOTE]
> If a task's worker crashes or loses its lease, the task always waits by
> the default backoff, not by your policy. The maintainer retries such a
> task, and the maintainer doesn't know your handler's options.

## What is a dead task?

A [dead task](../concepts/glossary.md#dead-task) failed for good: its attempts ran
out, its handler returned `Permanent`, or its payload does not decode.
taskmill does not run it again unless someone retries it. Dead tasks are
your dead-letter queue.

- A dead task is kept for 30 days by default, then deleted.
- In a FIFO group, a dead task stops the tasks behind it until you retry
  or delete it, and it is kept until then. See
  [Task order and FIFO groups](ordering.md).
- When a task becomes dead, taskmill logs
  `attempt failed; the task is dead` at the Error level.

What to do with a dead task:

1. Read its error history (below) to find the cause.
2. Fix the cause: deploy a fix, or correct the data.
3. Retry the task with `client.Retry` or in the [dashboard](dashboard.md).
   Or delete it, if the work is no longer needed.

For many dead tasks, follow
[Recover from an incident](../recipes/recover-from-an-incident.md).

> [!TIP]
> Alert when the number of dead tasks grows, not when it is above zero.
> Dead tasks stay for 30 days, so "above zero" stays red for a month. See
> [Monitoring](../production/monitoring.md).

## Where do I see a task's errors?

Open the task in the dashboard, or read it with `client.Get`.
`TaskInfo.Errors` holds the 20 most recent entries, oldest first. Each
entry has:

- `Attempt` and `At`: which attempt failed, and when;
- `Message`: the error's text, up to 4 KiB;
- `Code`: empty for an error that your handler returned in time.
  Otherwise it says what happened, such as `timeout`, `panic`, or
  `lease_lost`.

[Errors](../reference/errors.md) lists every code and what to do about it.

## See also

- [Handle tasks](handle-tasks.md): what a handler returns, and what each
  result does
- [Recover from an incident](../recipes/recover-from-an-incident.md): retry
  thousands of dead tasks safely
- [`RetryPolicy`](https://pkg.go.dev/github.com/code19m/taskmill#RetryPolicy)
  and [`ExponentialBackoff`](https://pkg.go.dev/github.com/code19m/taskmill#ExponentialBackoff)
  on pkg.go.dev

**Next:** [Handle tasks in batches](batch-handlers.md)
