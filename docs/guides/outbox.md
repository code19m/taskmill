# Publish events with the outbox

The outbox sends messages to a broker, such as Kafka, NATS, or RabbitMQ,
only if your transaction commits. You save each message together with your
data, and a [relay](../concepts/glossary.md#relay) on your workers publishes
it.

## Which should I use?

| Situation | Use | Why |
|---|---|---|
| Your own service does the work: send an email, call an API | A task | It is the simplest. |
| Other systems must receive an event through a broker | The outbox | It keeps the order per key, gives each message an ID for consumers, and retries for about a month. |
| Messages arrive from a broker, and the work must not happen twice | The inbox: one task per message, with the message ID as its dedupe key | See [Consume from a broker](../recipes/consume-from-a-broker.md). |
| Another service in the same database must do some work | `EnqueueTx` with a `Client` for that service's schema | See [Split across services](../recipes/split-across-services.md). |

Use the outbox whenever an event leaves your service. A task that publishes
by itself gives you none of the above.

## How it works

The diagram shows one message, from your transaction to the broker.

```mermaid
sequenceDiagram
    participant App as Your code
    participant DB as PostgreSQL
    participant R as Relay in a worker
    participant B as Message broker
    App->>DB: Save your data and Store the message, in one transaction
    App->>DB: COMMIT
    R->>DB: Fetch a batch, at most one message per topic and key
    R->>B: Publish the batch
    B-->>R: One result per message
    R->>DB: Delete published messages, retry failed ones later
    Note over R,DB: The next message of each key can go now
```

A message exists only if your data committed. If your transaction rolls
back, nothing is published.

## Store messages in your transaction

Create one `Outbox` and share it. Call `Store` in the same transaction as
the change that the event is about:

```go
ob := outbox.New(client) // once, at start

err := pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
    if err := markPaid(ctx, tx, orderID); err != nil {
        return err
    }
    return ob.Store(ctx, taskmill.Pgx(tx), outbox.Message{
        Topic: "orders",
        Key:   []byte(strconv.FormatInt(orderID, 10)), // messages with the same key stay in order
        Value: event,
    })
})
```

`Store` needs no broker. Your transactions keep working while the broker is
down.

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

Stored messages wait in the queue `outbox` (`outbox.Queue`). A worker with no
`Queues` set reads it. If you set `Queues`, add `outbox.Queue` to them.

For each delivery, your publisher returns one result:

| Result | What happens |
|---|---|
| `nil` | The message is published. taskmill deletes it, and keeps no history of it. |
| An error | The message is retried later. |
| `taskmill.Permanent(err)` | The message is dead right away. Use it for a message that can never succeed. |
| No result, because the slice is too short | The message is retried later. |

`RelayOptions` sets how many deliveries one call gets (`BatchSize`, 100 by
default) and how long one call may take (`Timeout`, 1 minute by default).
When the time is up, the call's context is canceled, and every message
without a `nil` result is retried later.

## In which order are messages published?

- Messages with the same topic and key are published **one at a time, in
  the order you stored them**. A batch never holds two of them.
- Messages with different keys go in any order, at the same time.
- Messages without a key go in any order.

So choose the key by what must stay in order: the order ID, or the account
ID. You can run the relay on many workers at the same time. The order per
key still holds.

## Let consumers drop duplicates

Publishing is **at least once**. For example, a worker can crash after the
broker accepted a message, but before taskmill recorded it. Then the
message is published again.

Each delivery has an `ID`, a UUIDv7. It stays the same when a message is
retried, and is unique across services. Send it with the message, and let
consumers skip an ID they have seen. If the consumer uses taskmill too, use
the inbox: see [Consume from a broker](../recipes/consume-from-a-broker.md).

## What if the broker is down?

Your app keeps working: `Store` only writes to PostgreSQL. The relay
retries each message that fails:

| Time after the first failure | What the relay does |
|---|---|
| The first minutes | Tries again after 1 s, 2 s, 4 s, and so on |
| After about 10 minutes | Tries again every 5 minutes |
| After about a month (10,000 tries) | Gives up: the message becomes dead |

Meanwhile, messages wait in the database. The dashboard shows them in the
`outbox` queue as `retryable`, and as `pending` when they wait behind an
earlier message with the same key. When the broker is back, the next tries
succeed, at most about 5 minutes later, still in order per key.

> [!WARNING]
> A dead message stops its key. Later messages with the same topic and key
> wait, so they never overtake it. Retry the dead message once the cause is
> fixed, or delete it. See [Fix a halted group](ordering.md#fix-a-halted-group).

A message's `Value` can be up to about 750 KiB, and one `Store` call takes
up to 10,000 messages. See [Defaults and limits](../reference/defaults-and-limits.md#outbox).

## See also

- [Consume from a broker](../recipes/consume-from-a-broker.md): the inbox, on the receiving side
- [Logs, traces, and metrics](observability.md): trace each message with `tel.Publisher`
- [Package `outbox`](https://pkg.go.dev/github.com/code19m/taskmill/outbox) on pkg.go.dev

**Next:** [The dashboard](dashboard.md)
