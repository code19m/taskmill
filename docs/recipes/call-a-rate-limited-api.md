# Call a rate-limited API

**Situation.** "A shipping partner allows 4 calls at the same time. We run 3
worker replicas. When we send too much, the partner answers HTTP 429, 'too
many requests'."

## Which should I use?

| The partner limits | What to use | Why |
|---|---|---|
| Calls at the same time | A queue of its own with [`MaxRunning`](../concepts/glossary.md#maxrunning) | It counts across all workers |
| Calls per minute | The same queue, and [one row that spaces the calls](#limit-calls-per-minute) | `MaxRunning` doesn't count calls per minute |
| Something it doesn't tell you | [`Snooze`](../concepts/glossary.md#snooze) on HTTP 429 | The task waits, and uses no attempt |

You can combine them. The code below uses the first and the third.

## The recommended way

Give the partner's calls a queue of their own. Limit that queue across all
workers with `MaxRunning`:

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
- **A queue of its own** means that the limit applies only to the partner's
  calls. Other kinds keep their speed while the partner's calls wait.
- **`Snooze` uses no attempt.** So a busy hour at the partner doesn't use up
  the task's attempts.
- **`Permanent` is for answers that a retry can't fix,** such as a bad
  request. The task is dead right away, for a person to check.

## Limit calls per minute

`MaxRunning` limits how many calls run at the same time, not how many start
in a minute. For a limit per minute, space the calls. Keep the time of the
next free call in one row of your own database:

```sql
CREATE TABLE api_limits (
    name         text PRIMARY KEY, -- one row per limit
    next_call_at timestamptz NOT NULL
);
```

Each task books the next free time in that row, and gets back how long to
wait for it:

```go
const (
    callEvery = time.Minute / 540 // the partner allows 600 a minute: stay a little below
    maxWait   = 5 * time.Second   // the longest that a task waits for its turn
)

// bookCall books the next free call time, and returns how long to wait for
// it. ok is false if that time is more than maxWait away.
func (p *PartnerSync) bookCall(ctx context.Context) (wait time.Duration, ok bool, err error) {
    err = p.pool.QueryRow(ctx, `
        INSERT INTO api_limits AS l (name, next_call_at)
        VALUES ('partner', now() + $1::interval)
        ON CONFLICT (name) DO UPDATE
        SET next_call_at = greatest(l.next_call_at, now()) + $1::interval
        WHERE l.next_call_at <= now() + $2::interval
        RETURNING next_call_at - $1::interval - now()`,
        callEvery, maxWait).Scan(&wait)
    if errors.Is(err, pgx.ErrNoRows) {
        return 0, false, nil // too many tasks wait already
    }
    return wait, err == nil, err
}
```

At the start of the handler, before the call, book a turn and wait for it:

```go
wait, ok, err := p.bookCall(ctx)
if err != nil {
    return err
}
if !ok {
    return taskmill.Snooze(maxWait) // come back when fewer tasks wait
}
select {
case <-time.After(wait): // the task keeps its slot while it waits
case <-ctx.Done():
    return ctx.Err()
}
```

- **The row is shared by every replica.** A limiter in your process can't do
  this: 3 replicas would make 3 times as many calls.
- **The calls are evenly spaced,** one every `callEvery`, in the order that
  the tasks booked. In a test with 3 workers, 300 calls at 20 a second took
  15 seconds.
- **A waiting task keeps its [slot](../concepts/glossary.md#slot),** so
  `MaxRunning` also limits how many tasks wait. Keep `MaxRunning` times
  `callEvery` below `maxWait`. Otherwise tasks snooze again and again, and
  each snooze is wasted work for the database.
- **`MaxRunning` must also be high enough for the pace.** At 9 calls a
  second that take 1 second each, 9 calls run at the same time.

## Other options

| Option | When it fits | The cost |
|---|---|---|
| `Concurrency` for the queue, in `WorkerConfig.Queues` | You run one worker process, and never scale it | It limits one process: 3 replicas make 3 times as many calls |
| A plain error on HTTP 429, without `Snooze` | The partner rarely says "too many requests" | Each 429 uses an attempt, and the task waits for the backoff |
| A token bucket that returns `Snooze` when it is empty | Short bursts above the pace are fine, and few tasks wait | Every waiting task runs again and again only to snooze. The work grows with the backlog |
| `PauseQueue` | The partner is down for hours | A person must resume the queue, and its [lag](../concepts/glossary.md#lag) grows |

## Watch out

- **A queue with `MaxRunning` often runs fewer tasks than its limit.** When
  a task ends, a worker can wait up to `PollInterval`, 1 second by default,
  before it starts the next one. In a test with calls of 1 second, a limit
  of 4 ran 3 calls at the same time on average.
- **`MaxRunning` counts tasks, not batches.** A
  [batch handler](../concepts/glossary.md#batch-handler) in this queue gets
  at most 4 tasks per batch.
- **A task that always snoozes never becomes dead,** so no one sees it.
  That is why the handler gives up after a day. In a
  [FIFO group](../concepts/glossary.md#fifo-group), a snoozed task also
  blocks the rest of its group.
- **A spaced call can still start a little late.** Then a minute can hold
  one call more than the pace allows. So keep the pace below the partner's
  limit, and keep the `Snooze` on HTTP 429.
- **Set the handler's `Timeout` above `maxWait` plus the slowest call.** The
  default of 1 minute fits the values above.

## See also

- [Workers and queues](../guides/workers-and-queues.md): `Concurrency`, `MaxRunning`, and pausing
- [Handle tasks](../guides/handle-tasks.md): what each return value does
- [Do work once](do-work-once.md): the idempotency key
- [`Client.UpdateQueue`](https://pkg.go.dev/github.com/code19m/taskmill#Client.UpdateQueue) on pkg.go.dev

**Next:** [Keep order per key](keep-order-per-key.md)
