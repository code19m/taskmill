# Part 5: The outbox

**Goal:** publish two events for each order to a message broker, only if
the order was saved, and in order. **Time:** about 10 minutes. **You start
from:** the program of [part 4](4-queues-delays-schedules.md).

This part is optional. Do it if your service sends events to a broker, such
as Kafka, NATS, or RabbitMQ. If you skip it, leave `ob` and `outbox` out of
the code in parts 6 and 7.

## Step 1. Store two events with each order

The [outbox](../concepts/glossary.md#outbox) saves a message in your own
transaction, as `EnqueueTx` saves a task. Add `"strconv"` and
`"github.com/code19m/taskmill/outbox"` to the imports. In `run`, create the
outbox below the client:

```go
	// The outbox stores messages for your broker in your own transaction.
	ob := outbox.New(client)
```

Pass it to `placeOrder`. Change the route, and the first line of
`placeOrder`:

```go
	mux.Handle("POST /orders", placeOrder(pool, client, ob, logger))
```

```go
func placeOrder(pool *pgxpool.Pool, client *taskmill.Client, ob *outbox.Outbox, logger *slog.Logger) http.Handler {
```

In the transaction, replace the `return err` below the reminder with these
lines:

```go
			if err != nil {
				return err
			}
			// Store the order's events. Messages with the same key stay in order.
			key := []byte(strconv.FormatInt(orderID, 10))
			return ob.Store(ctx, taskmill.Pgx(tx),
				outbox.Message{Topic: "orders", Key: key, Value: []byte("order.placed")},
				outbox.Message{Topic: "orders", Key: key, Value: []byte("order.paid")},
			)
```

Start the shop, and place an order:

```sh
curl -s -d email=user2@example.com localhost:8080/orders
```

Open the dashboard's **Overview**, and wait about 10 seconds. **You should
see** a third queue, `outbox`, with one `available` task and one `pending`
task.

These are the two messages. The second one waits behind the first, because
they have the same key. Nothing publishes them yet.

## Step 2. Publish the events

A [relay](../concepts/glossary.md#relay) on your worker gives the stored
messages to a publisher: your code around your broker's client. This
publisher only writes to the log, and fails one time in four. Add two keys
to the log keys:

```go
	logKeyEvent        = "event"
	logKeyOrder        = "order"
```

Add the publisher above `main`:

```go
// publish stands for your broker client, such as Kafka's: it writes each
// message to the log. One time in four it fails, as a broker sometimes does.
func publish(logger *slog.Logger) outbox.PublisherFunc {
	return func(ctx context.Context, ds []outbox.Delivery) []error {
		errs := make([]error, len(ds)) // one result per delivery, in the same order
		for i, d := range ds {
			if rand.IntN(4) == 0 {
				errs[i] = errors.New("broker did not answer") // the relay tries this message again later
				continue
			}
			logger.LogAttrs(ctx, slog.LevelInfo, "event published",
				slog.String(logKeyOrder, string(d.Key)), slog.String(logKeyEvent, string(d.Value)))
		}
		return errs
	}
}
```

In `run`, add the relay below the other handlers:

```go
	ob.AddRelay(&handlers, publish(logger), nil)
```

The worker must read the outbox's queue. Add it to `Queues`:

```go
			outbox.Queue: {Concurrency: 2},
```

The worker can now run 12 tasks at the same time. So change `cfg.MaxConns`
to `17`, and the numbers in the comment above it to 17 and 12.

Start the shop, and place six orders:

```sh
for i in $(seq 1 6); do curl -s -d "email=user$i@example.com" localhost:8080/orders; done
```

**You should see** the events of step 1 first, and then two events for each
new order. When the publisher fails, the relay tries that message again:

```text
time=… level=INFO msg="event published" order=21 event=order.placed
time=… level=DEBUG msg="attempt failed; retrying" worker_id=… task_id=31 kind=taskmill.outbox.message attempt=1 error="broker did not answer" duration=29.583µs retry_in=1.002438899s
time=… level=INFO msg="event published" order=21 event=order.paid
time=… level=INFO msg="event published" order=23 event=order.placed
…
time=… level=INFO msg="event published" order=22 event=order.placed
time=… level=INFO msg="event published" order=22 event=order.paid
```

Look at one order's lines: `order.placed` always comes before `order.paid`,
also after a failure. Events of different orders can mix.

> [!NOTE]
> A message can be published twice: for example, when the worker crashes
> after the broker took the message. Each delivery has an `ID` that stays
> the same, so that consumers can drop a message they have seen.
> [Publish events with the outbox](../guides/outbox.md#let-consumers-drop-duplicates)
> shows how.

## What you learned

| In the code | taskmill calls it |
|---|---|
| `outbox.New(client)` | the [outbox](../concepts/glossary.md#outbox) |
| `ob.Store(ctx, taskmill.Pgx(tx), …)` | messages that are saved with your transaction, or not at all |
| `Key` | what keeps messages in order: the same topic and key means one at a time |
| `ob.AddRelay(…)` | the [relay](../concepts/glossary.md#relay), which calls your publisher |
| `errs[i] = errors.New(…)` | a failed delivery, which the relay tries again |

## Go deeper

- [Publish events with the outbox](../guides/outbox.md): the order of messages, and what happens when the broker is down
- [Consume from a broker](../recipes/consume-from-a-broker.md): the receiving side, with a dedupe key per message
- [Package `outbox`](https://pkg.go.dev/github.com/code19m/taskmill/outbox) on pkg.go.dev

**Next:** [Part 6: Test it](6-test-it.md)
