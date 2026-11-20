# Work with other languages

**Situation.** "Our checkout service is written in Node.js, and our billing
service in Go, with taskmill. Checkout must start billing's tasks. And a
Python service must learn when an invoice is paid."

## Which should I use?

| Situation | What to use | Why |
|---|---|---|
| A program in another language starts a Go service's task | **An HTTP endpoint in the Go service** that enqueues it | One request, and the Go type checks the payload |
| That program already sends messages through a broker | The [inbox](../concepts/glossary.md#inbox) in the Go service: one task per message, with the message ID as its dedupe key ([Consume from a broker](consume-from-a-broker.md)) | No new endpoint, and the broker keeps the messages while the Go service is down |
| A program in another language acts on your events | The [outbox](../guides/outbox.md): it publishes to your broker, and the other program consumes there | The consumer can use any language |
| A handler in another language | Not provided: handlers are Go. A Go handler can call the other program's API | taskmill runs handlers in your Go process |

## The recommended way

The Go service that owns the tasks takes them over HTTP. A map from each
allowed kind to its Go type decides what a caller may enqueue:

```go
// enqueuers maps each kind that other programs may enqueue to the function
// that decodes it into its Go type and enqueues it. Other kinds are refused.
var enqueuers = map[string]enqueuer{
    SendReceipt{}.Kind(): enqueueJSON[SendReceipt],
}
// ...
func enqueueJSON[A taskmill.Args](ctx context.Context, client *taskmill.Client, body io.Reader, opts *taskmill.EnqueueOptions) (taskmill.EnqueueResult, error) {
    var args A
    dec := json.NewDecoder(body)
    dec.DisallowUnknownFields()
    if err := dec.Decode(&args); err != nil {
        return taskmill.EnqueueResult{}, fmt.Errorf("%w: %w", errBadTask, err)
    }
    return client.Enqueue(ctx, args, opts)
}
// ...
func enqueueHandler(client *taskmill.Client, logger *slog.Logger) http.Handler {
    return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        enqueue, ok := enqueuers[r.PathValue("kind")]
        if !ok {
            http.Error(w, "unknown kind", http.StatusNotFound)
            return
        }
        body := http.MaxBytesReader(w, r.Body, 1<<20)
        opts := &taskmill.EnqueueOptions{DedupeKey: r.Header.Get("Idempotency-Key")}
        res, err := enqueue(r.Context(), client, body, opts)
        switch {
        case errors.Is(err, errBadTask) || errors.Is(err, taskmill.ErrInvalidArgument):
            http.Error(w, err.Error(), http.StatusBadRequest)
        case err != nil: // the database is slow or gone: the producer sends the request again
            logger.LogAttrs(r.Context(), slog.LevelError, "enqueue failed", slog.Any(logKeyError, err))
            http.Error(w, "enqueue failed, try again", http.StatusServiceUnavailable)
        default:
            w.Header().Set("Content-Type", "application/json")
            w.WriteHeader(http.StatusAccepted)
            _, _ = fmt.Fprintf(w, "{\"id\":%d,\"duplicate\":%t}\n", res.ID, res.Duplicate)
        }
    })
}
```

From any language, the producer sends the task's JSON, with a key that
stays the same when it sends the request again:

```sh
curl -i localhost:8080/tasks/email.send_receipt -H 'Idempotency-Key: order-42' -d '{"order_id": 42}'
```

The answer is `202` with the task's ID, `404` for a kind that the map
doesn't have, `400` for a body that isn't a task of that kind, and `503`
when the database can't take the task. [The example program](../../examples/enqueue-over-http)
runs this endpoint with a worker.

## Why

- **The map is the allow-list.** A caller can enqueue only the kinds that
  you put in it, and each payload must decode into its Go type, with no
  unknown field. A wrong payload fails in the request, not later in a
  handler.
- **A request sent again makes no second task.** The `Idempotency-Key`
  header becomes the task's [dedupe key](../concepts/glossary.md#dedupe-key),
  so a producer can retry after a timeout or a `503`. The key blocks
  duplicates for 24 hours by default
  ([`DedupeWindow`](https://pkg.go.dev/github.com/code19m/taskmill#EnqueueOptions.DedupeWindow)).
- **No SQL function to enqueue.** taskmill's tables are internal and change
  between releases, so only taskmill's Go client writes to them.

## Other options

| Option | When it fits | The cost |
|---|---|---|
| A broker and the inbox | The producer already publishes to a broker | A broker to run, and a consumer in the Go service |
| A gRPC method instead of HTTP | Your services already talk gRPC | The same code, with a request type per kind |
| A kind name and a raw payload, enqueued without a Go type | Never: taskmill does not offer it | Nothing would check the payload before a handler fails on it |

## Watch out

> [!WARNING]
> The endpoint lets its callers start work. Put it behind the same
> authentication as your service's other internal APIs, and never expose it
> to the internet.

- **A dedupe key belongs to a kind and a queue, not to a caller.** If several
  programs call the endpoint, put the caller's name, as your authentication
  knows it, in front of each key. Then one caller can't block another's keys.
- **The endpoint refuses unknown fields.** When a task gains a field,
  deploy the Go service before any producer sends the field.

## See also

- [Split work across services](split-across-services.md): when both services are written in Go
- [Consume from a broker](consume-from-a-broker.md): the inbox
- [`EnqueueOptions.DedupeKey`](https://pkg.go.dev/github.com/code19m/taskmill#EnqueueOptions.DedupeKey) on pkg.go.dev

**Next:** [One dashboard for many services](one-dashboard-for-many-services.md)
