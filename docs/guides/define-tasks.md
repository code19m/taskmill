# Define tasks

A task is a Go struct with a name. This page shows how to name it, what to
put in it, and how to give it default options.

## Quick example

```go
// SendReceipt emails the receipt of an order.
type SendReceipt struct {
    OrderID int64 `json:"order_id"`
}

func (SendReceipt) Kind() string { return "email.send_receipt" }
```

That is all a task needs: the struct implements the `Args` interface.

- The struct is the task's [payload](../concepts/glossary.md#payload): its
  data. taskmill stores it as JSON with `encoding/json`, so use `json` tags.
- `Kind` returns the task's [kind](../concepts/glossary.md#kind). The kind
  picks the handler that runs the task.
- The code that enqueues and the code that handles use the same struct.

## Name the kind

- Return a constant string, from a value receiver. Don't read the struct's
  fields: taskmill also calls `Kind` on an empty struct.
- Start the kind with the module that owns it: `email.send_receipt`,
  `billing.invoice.send`.
- A kind has 1 to 128 characters.

> [!WARNING]
> Don't rename a kind while tasks of it wait. Those tasks find no handler,
> and they become dead after an hour. Add a new kind instead.

## Keep the payload small

Put IDs in the payload, not whole records. `SendReceipt` holds only the
order's ID, and its handler loads the order when it runs. A copy of the
order could be out of date by then.

A payload can be up to 1 MiB of valid UTF-8 JSON. Keep it far below that.
A payload that breaks a limit fails the enqueue with
[`ErrInvalidArgument`](https://pkg.go.dev/github.com/code19m/taskmill#ErrInvalidArgument).

## Change a payload safely

Tasks wait in the database, sometimes for hours. So a worker may decode a
task that an older version of your code enqueued.

| Change | Safe? | What happens to old tasks |
|---|---|---|
| Add an optional field | Yes | The field gets its zero value |
| Remove a field | Yes | The worker ignores the field |
| Rename a field, or its `json` tag | No | The renamed field silently gets its zero value |
| Change a field's type, such as string to number | No | They don't decode, and become dead right away, with the code `invalid_payload` |

For a change that is not safe, add a new kind. See
[Change a task safely](../recipes/change-a-task-safely.md).

## Set defaults per kind

A kind can set its own default options. Implement `EnqueueDefaults`:

```go
func (SendReceipt) EnqueueDefaults() taskmill.EnqueueOptions {
    return taskmill.EnqueueOptions{Queue: "emails", MaxAttempts: 5}
}
```

Now every receipt goes to the `emails` queue and runs at most 5 times,
unless the enqueue call sets other values. Return the same `Queue` for every
value: a worker calls `EnqueueDefaults` on an empty struct to learn which
queue to read.

### Which should I use?

| Situation | What to use | Why |
|---|---|---|
| The queue, max attempts, or priority of every task of a kind | `EnqueueDefaults` | One place decides, so producers and workers can't disagree |
| A value that changes per task: run time, group key, dedupe key, metadata | Options in the enqueue call | Only the caller knows it |
| One kind that belongs in two queues, such as a bulk import next to live traffic | `Queue` in the enqueue call | Then list both queues in `WorkerConfig.Queues` |

Options in the call win over the kind's defaults: see
[Enqueue tasks](enqueue.md#options).

## See also

- [Enqueue tasks](enqueue.md): every option, and which value wins
- [Change a task safely](../recipes/change-a-task-safely.md): new fields, new kinds, and rolling deploys
- [`Args`](https://pkg.go.dev/github.com/code19m/taskmill#Args) and [`ArgsWithDefaults`](https://pkg.go.dev/github.com/code19m/taskmill#ArgsWithDefaults) on pkg.go.dev

**Next:** [Enqueue tasks](enqueue.md)
