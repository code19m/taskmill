# Call a rate-limited API

This recipe shows how to call a partner's API that allows only a few calls
at the same time, without losing tasks and without making a person retry
them by hand.

**Your situation:** "A shipping partner allows 4 calls at the same time. We
run 3 worker replicas. When we send too much, the partner answers HTTP 429,
'too many requests'."

## The preferred way

Give the partner's calls a queue of their own, and limit that queue across
all workers with [`MaxRunning`](../concepts/glossary.md#maxrunning):

```go
// SyncOrder sends an order to the partner's API.
type SyncOrder struct {
    OrderID int64 `json:"order_id"`
}

func (SyncOrder) Kind() string { return "partner.sync_order" }

// The partner's calls get a queue of their own.
func (SyncOrder) EnqueueDefaults() taskmill.EnqueueOptions {
    return taskmill.EnqueueOptions{Queue: "partner-api"}
}
```

```go
// At startup: at most 4 calls at the same time, across all workers.
err := client.UpdateQueue(ctx, "partner-api", func(s *taskmill.QueueSettings) { s.MaxRunning = 4 })
```

In the handler, turn each answer into the right result:

```go
func (p *PartnerSync) Handle(ctx context.Context, t *taskmill.Task[SyncOrder]) error {
    resp, err := p.partner.SendOrder(ctx, t.Args.OrderID, t.ID) // t.ID in the idempotency key
    if err != nil {
        return err // a network error: retried with backoff
    }
    defer resp.Body.Close()

    switch {
    case resp.StatusCode == http.StatusTooManyRequests:
        if time.Since(t.CreatedAt) > 24*time.Hour {
            return taskmill.Permanent(errors.New("partner: still too many requests after a day"))
        }
        return taskmill.Snooze(retryAfter(resp)) // runs again later, uses no attempt
    case resp.StatusCode == http.StatusBadRequest:
        return taskmill.Permanent(fmt.Errorf("partner refused order %d", t.Args.OrderID)) // dead right away
    case resp.StatusCode >= 300:
        return fmt.Errorf("partner: %s", resp.Status) // retried with backoff
    }
    return nil
}

// retryAfter reads the partner's Retry-After header, in seconds.
func retryAfter(resp *http.Response) time.Duration {
    if s, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && s > 0 {
        return time.Duration(s) * time.Second
    }
    return time.Minute
}
```

## Why

- **`MaxRunning` counts across every worker and replica.** At most 4 tasks of
  the queue run at the same time, however many workers you run, and however
  an autoscaler changes them. It is stored in the database, so calling
  `UpdateQueue` at every start is safe. Workers apply a change within
  seconds.
- **A queue of its own** keeps the limit away from your other work. Other
  kinds keep their speed while the partner's calls wait.
- **`Snooze` is not a failure.** The task runs again after the wait, and the
  wait uses no attempt. So a busy hour at the partner doesn't use up the
  task's attempts and make it dead.
- **`Permanent` is for answers that a retry can't fix,** such as a bad
  request. The task becomes dead right away, for a person to look at.
- **The idempotency key** makes a repeated call safe, because a task can run
  twice. See [Do work once](do-work-once.md).

## Other options

| Option | Choose it when | The cost |
|---|---|---|
| `Concurrency` for the queue, in `WorkerConfig.Queues` | You run one worker process, and never scale it | It limits one process: 3 replicas run 3 times as many calls |
| A plain error on HTTP 429, without `Snooze` | The partner says "too many requests" only rarely | Each 429 uses an attempt, and the task waits for the backoff |
| Your own rate limiter in the handler that returns `Snooze` when it is empty | The partner limits calls per minute, not calls at the same time | Code that you write and share between workers |
| `PauseQueue` | The partner is down for hours | A person must resume the queue; its lag grows meanwhile |

## Watch out

- **`MaxRunning` limits calls at the same time, not calls per minute.** If
  each call takes about 1 second, 4 at the same time is about 240 calls a
  minute. For a per-minute limit, choose a `MaxRunning` that stays below it,
  and keep the `Snooze` on HTTP 429.
- **`MaxRunning` counts tasks, not batches.** A [batch
  handler](../concepts/glossary.md#batch-handler) in this queue gets at most 4
  tasks per batch.
- **Snoozes never end on their own.** A task that always snoozes never
  becomes dead, so no one sees it. That is why the handler gives up after a
  day. In a FIFO group, a snoozed task also holds back the rest of its group.

## See also

- [Workers and queues](../guides/workers-and-queues.md): `Concurrency`, `MaxRunning`, and pausing
- [Handle tasks](../guides/handle-tasks.md): what each return value does
- [`Client.UpdateQueue`](https://pkg.go.dev/github.com/code19m/taskmill#Client.UpdateQueue) on pkg.go.dev

**Next:** [Keep order per key](keep-order-per-key.md)
