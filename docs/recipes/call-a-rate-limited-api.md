# Call a rate-limited API

**Situation.** "A shipping partner allows 4 calls at the same time. We run 3
worker replicas. When we send too much, the partner answers HTTP 429, 'too
many requests'."

## The recommended way

Give the partner's calls a queue of their own. Limit that queue across all
workers with [`MaxRunning`](../concepts/glossary.md#maxrunning):

```go
func (SyncOrder) Kind() string { return "partner.sync_order" }

func (SyncOrder) EnqueueDefaults() taskmill.EnqueueOptions {
    return taskmill.EnqueueOptions{Queue: "partner-api"} // a queue of its own
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
        return taskmill.Snooze(time.Minute) // or the wait in the Retry-After header
    case resp.StatusCode == http.StatusBadRequest:
        return taskmill.Permanent(fmt.Errorf("partner refused order %d", t.Args.OrderID))
    case resp.StatusCode >= 300:
        return fmt.Errorf("partner: %s", resp.Status) // retried with backoff
    }
    return nil
}
```

## Why

- **`MaxRunning` counts across every worker and replica,** however many an
  autoscaler starts. It is stored in the database, so you can set it at
  every start. Workers apply a change within seconds.
- **A queue of its own** keeps the limit away from other work. Other kinds
  keep their speed while the partner's calls wait.
- **[`Snooze`](../concepts/glossary.md#snooze) uses no attempt.** So a busy
  hour at the partner doesn't use up the task's attempts.
- **`Permanent` is for answers that a retry can't fix,** such as a bad
  request. The task is dead right away, for a person to check.

## Other options

| Option | When it fits | The cost |
|---|---|---|
| `Concurrency` for the queue, in `WorkerConfig.Queues` | You run one worker process, and never scale it | It limits one process: 3 replicas make 3 times as many calls |
| A plain error on HTTP 429, without `Snooze` | The partner rarely says "too many requests" | Each 429 uses an attempt, and the task waits for the backoff |
| Your own rate limiter that returns `Snooze` when it is empty | The partner limits calls per minute, not calls at the same time | Code that you write, shared between workers |
| `PauseQueue` | The partner is down for hours | A person must resume the queue, and its lag grows |

## Watch out

- **`MaxRunning` limits calls at the same time, not calls per minute.** If
  a call takes about 1 second, 4 at the same time is about 240 calls a
  minute. For a per-minute limit, pick a `MaxRunning` below it, and keep the
  `Snooze` on HTTP 429.
- **`MaxRunning` counts tasks, not batches.** A
  [batch handler](../concepts/glossary.md#batch-handler) in this queue gets
  at most 4 tasks per batch.
- **A task that always snoozes never becomes dead,** so no one sees it.
  That is why the handler gives up after a day. In a FIFO group, a snoozed
  task also holds back the rest of its group.

## See also

- [Workers and queues](../guides/workers-and-queues.md): `Concurrency`, `MaxRunning`, and pausing
- [Handle tasks](../guides/handle-tasks.md): what each return value does
- [Do work once](do-work-once.md): the idempotency key
- [`Client.UpdateQueue`](https://pkg.go.dev/github.com/code19m/taskmill#Client.UpdateQueue) on pkg.go.dev

**Next:** [Keep order per key](keep-order-per-key.md)
