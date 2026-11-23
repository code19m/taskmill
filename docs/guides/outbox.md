# Publish events with the outbox

The outbox sends messages to a broker, such as Kafka, NATS, or RabbitMQ,
only if your transaction commits. You save each message together with your
data, and a [relay](../concepts/glossary.md#relay) on your workers publishes
it.

## Which should I use?

| Situation | What to use | Why |
|---|---|---|
| Your own service does the work: send an email, call an API | A task | It is the simplest |
| Other systems must receive an event through a broker | The outbox | It keeps the order per key, gives each message an ID for consumers, and retries for about a month |
| Messages arrive from a broker, and the work must not happen twice | The inbox: one task per message, with the message ID as its dedupe key | See [Consume from a broker](../recipes/consume-from-a-broker.md) |
| Another service in the same database must do some work | `EnqueueTx` with a `Client` for that service's schema | See [Split work across services](../recipes/split-across-services.md) |

Use the outbox whenever an event leaves your service. A task that publishes
by itself gives you no order per key, no message ID, and fewer retries.

## Quick example

Create one `Outbox` and share it. Call `StoreTx` in the same transaction as
the change that the event is about:

```go
ob := outbox.New(client) // once, at start

err := pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
    if err := markPaid(ctx, tx, orderID); err != nil {
        return err
    }
    _, err := ob.StoreTx(ctx, taskmill.Pgx(tx), outbox.Message{
        Topic: "orders",
        Key:   []byte(strconv.FormatInt(orderID, 10)), // messages with the same key stay in order
        Value: event,
    })
    return err
})
```

- `StoreTx` only writes to PostgreSQL, so your app keeps working while the
  broker is down.
- It returns the ID of each message's delivery, the one that consumers
  deduplicate by. Log it, or pass it on, to follow an event to its
  consumers.
- A `Value` can be up to about 750 KiB: see
  [`outbox.Message`](https://pkg.go.dev/github.com/code19m/taskmill/outbox#Message).
- A [relay](#relay-messages-to-your-broker) on your workers then publishes
  the message.

## How it works

The diagram shows one message, from your transaction to the broker.

```mermaid
sequenceDiagram
    participant App as Your code
    participant DB as PostgreSQL
    participant R as Relay in a worker
    participant B as Message broker
    App->>DB: Save your data and store the message, in one transaction
    App->>DB: COMMIT
    R->>DB: Fetch a batch, at most one message of each key
    R->>B: Publish the batch
    B-->>R: One result per message
    R->>DB: Delete published messages, retry failed ones later
    Note over R,DB: The next message of each key can go now
```

If your transaction rolls back, the message never existed, and nothing is
published.

## Relay messages to your broker

Register the relay on your workers. It calls your `Publisher`, which wraps
your broker client:

```go
var handlers taskmill.Handlers
ob.AddRelay(&handlers, outbox.PublisherFunc(func(ctx context.Context, ds []outbox.Delivery) []error {
    errs := make([]error, len(ds)) // one result per delivery, in the same order
    for i, d := range ds {
        errs[i] = broker.Publish(ctx, d.Topic, d.Key, d.Value, d.Headers, d.ID) // your broker client
    }
    return errs
}), nil) // nil: the default RelayOptions
worker, err := taskmill.NewWorker(client, &taskmill.WorkerConfig{Handlers: &handlers})
```

Stored messages wait in the queue `outbox` (`outbox.Queue`). A worker with
no `Queues` set reads it. If you set `Queues`, add `outbox.Queue`.

| Your publisher returns for a delivery | What happens |
|---|---|
| `nil` | The message is published. taskmill deletes it, and keeps no history of it |
| An error, or no result | The message is retried later. So is every message of the call if it returns more results than deliveries |
| `taskmill.Permanent(err)` | The message is dead right away. Use it for a message that can never succeed |

`RelayOptions` sets how many deliveries one call gets (`BatchSize`, 100 by
default), and how long one call may take (`Timeout`, 1 minute by default).
When the time is up, the call's context is canceled, and every message
without a `nil` result is retried. Return when the context ends: a call
that ignores it keeps running, and a late send can reach the broker after
later messages of the same key (see
[`outbox.Publisher`](https://pkg.go.dev/github.com/code19m/taskmill/outbox#Publisher)).

A worker runs up to 10 calls at a time, one per slot of the `outbox` queue,
so with large messages, size its memory for them:
[Performance](../reference/performance.md#resource-use) gives the numbers.
Lower `BatchSize` to need less, or when the link to the database is slow:
the messages of a call come in one fetch, while their leases run.

To test your publisher, run the relay with a fake broker: see
[Test your code](testing.md#test-that-everything-gets-done).

## In which order are messages published?

- Messages with the same topic and key are published **one at a time, in
  the order you stored them**, even with the relay on many workers.
- Messages with different keys, or without a key, go in any order, at the
  same time.

So choose the key by what must stay in order: the order ID, or the account
ID.

**A key also makes stores wait.** Each message with a key joins its key's
[FIFO group](../concepts/glossary.md#fifo-group). So two transactions that
store messages with the same topic and key wait for each other's commits
([Task order and FIFO groups](ordering.md)). Choose the narrowest key that
keeps the order you need: the order ID if only each order's events must
stay in order, not the account ID. Never use one key for all messages: every
store would wait in line. Leave the key empty when the order does not
matter, and keep the transactions that store keyed messages short.

## Let consumers drop duplicates

Publishing is **at least once**. For example, a worker can crash after the
broker accepted a message, but before taskmill recorded it. Then the
message is published again.

Each delivery has an `ID`, a UUIDv7. It stays the same when a message is
retried, and is unique across services. Send it with the message, and let
consumers skip an ID they have seen. If the consumer uses taskmill too, use
the inbox: see [Consume from a broker](../recipes/consume-from-a-broker.md).

## What if the broker is down?

The relay retries each message that fails:

| Time after the first failure | What the relay does |
|---|---|
| The first minutes | Tries again after 1 s, 2 s, 4 s, and so on |
| After about 10 minutes | Tries again every 5 minutes |
| After about a month (10,000 tries) | Gives up: the message becomes dead |

While the broker is down, messages wait in the database. The dashboard
shows them in the `outbox` queue as `retryable`, or as `pending` when they
wait behind an earlier message with the same key: the topic and the key
stand beside the queue, as "orders › order-13". When the broker is back,
the messages go out within about 5 minutes, still in order per key.

The queue's [lag](../concepts/glossary.md#lag) stays near zero during the
outage, because each message waits for its next try, not for a worker. So
alert on failures in a row instead: `taskmill_queue_most_failures` for the
`outbox` queue reaches 10 about 9 minutes into an outage. See
[Monitoring](../production/monitoring.md).

> [!WARNING]
> A dead message stops its key: later messages with the same topic and key
> wait, so they are never published before it. `taskmill_queue_halted_groups`
> counts the keys it stops. Fix the cause, then retry the dead message, or
> delete it. See [Fix a halted group](ordering.md#fix-a-halted-group).

## See also

- [Consume from a broker](../recipes/consume-from-a-broker.md): the inbox, on the receiving side
- [Logs, traces, and metrics](observability.md): trace each message with `tel.Publisher`
- [Package `outbox`](https://pkg.go.dev/github.com/code19m/taskmill/outbox) on pkg.go.dev

**Next:** [The dashboard](dashboard.md)
