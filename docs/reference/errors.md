# Errors

What each error, context cause, error class, and error history code means,
and what to do about it. Each name links to its godoc on pkg.go.dev.

## Test errors with `errors.Is`

taskmill wraps its errors with details, so compare them with `errors.Is`, not
with `==`, and never by their message:

```go
err := client.Retry(ctx, id)
switch {
case errors.Is(err, taskmill.ErrNotFound):
    // the task is gone: deleted, or past its retention
case errors.Is(err, taskmill.ErrInvalidState):
    // the task is not dead or canceled
}
```

The names and classes on this page never change within a major version.
Messages may change in any release
([compatibility](compatibility.md)).

## Errors that calls return

| Error | Class | What it means | What to do |
|---|---|---|---|
| [`ErrNotFound`](https://pkg.go.dev/github.com/code19m/taskmill#ErrNotFound) | `not_found` | The task or schedule does not exist, or no longer exists: it was deleted, or retention removed it. | Treat it as gone. |
| [`ErrInvalidState`](https://pkg.go.dev/github.com/code19m/taskmill#ErrInvalidState) | `invalid_state` | The action does not fit the task's state. For example: retry a task that is not dead or canceled, cancel a finished task, delete a running one, or run a paused schedule. `Worker.Run` also returns it if the worker already runs. | Check the state first with `State.CanRetry`, `CanCancel`, or `CanDelete`, or accept that there is nothing to do. |
| [`ErrInvalidArgument`](https://pkg.go.dev/github.com/code19m/taskmill#ErrInvalidArgument) | `invalid_argument` | An argument, option, or configuration is wrong: out of range, too long, or text that PostgreSQL can't store, such as a NUL character. `NewClient` also returns it when the pool does not use READ COMMITTED. | Fix the call: the message says what is wrong. Retrying the same call doesn't help. |
| [`ErrSchemaOutdated`](https://pkg.go.dev/github.com/code19m/taskmill#ErrSchemaOutdated) | `schema_outdated` | The [schema](../concepts/glossary.md) is missing, or older than this version of taskmill. `NewClient` checks it. | Run `Migrate`, or apply `Migrations` with your own tool, before you start the new version. |
| [`ErrConflict`](https://pkg.go.dev/github.com/code19m/taskmill#ErrConflict) | `conflict` | `UpdateQueue` or `UpdateSettings` lost to other updates of the same settings 10 times in a row. | Try again a little later. |
| [`ErrLeaseLost`](https://pkg.go.dev/github.com/code19m/taskmill#ErrLeaseLost) | `lease_lost` | Returned by `Task.CompleteTx`: another attempt took the task over. | Return it from your handler, so that your transaction rolls back. |

The `outbox`, `ui`, and `otel` packages return the same errors, such as
`ErrInvalidArgument` for a message without a topic.

## Errors your handler returns

| Return | Type | What happens |
|---|---|---|
| [`taskmill.Permanent(err)`](https://pkg.go.dev/github.com/code19m/taskmill#Permanent) | [`*PermanentError`](https://pkg.go.dev/github.com/code19m/taskmill#PermanentError) | The task becomes dead right away, without more retries. `Permanent(nil)` returns nil. |
| [`taskmill.Snooze(d)`](https://pkg.go.dev/github.com/code19m/taskmill#Snooze) | [`*SnoozeError`](https://pkg.go.dev/github.com/code19m/taskmill#SnoozeError) | The task runs again after `d`, without using an attempt. |
| Any other error | Yours | The attempt fails. The task is retried after a backoff, or becomes dead when no attempts are left. |

[Handle tasks](../guides/handle-tasks.md) says which one to return when.

## Why a handler's context ended

When a handler's context is canceled, `context.Cause(ctx)` says why. Return
soon in every case.

| Cause | Class | What happened | What happens to the task |
|---|---|---|---|
| [`ErrTaskCanceled`](https://pkg.go.dev/github.com/code19m/taskmill#ErrTaskCanceled) | `canceled` | Someone canceled the task while it ran. | It becomes canceled when the attempt ends. If the handler returns nil, it completes instead. |
| [`ErrLeaseLost`](https://pkg.go.dev/github.com/code19m/taskmill#ErrLeaseLost) | `lease_lost` | The worker could not renew the [lease](../concepts/glossary.md) in time. The task may already run on another worker. | The result is thrown away. The task is retried. |
| [`ErrAttemptTimeout`](https://pkg.go.dev/github.com/code19m/taskmill#ErrAttemptTimeout) | `timeout` | The attempt ran longer than `HandlerOptions.Timeout`. | An error fails the attempt, with the code `timeout`. Nil completes the task. |
| [`ErrShutdown`](https://pkg.go.dev/github.com/code19m/taskmill#ErrShutdown) | `shutdown` | The worker is stopping and `DrainTimeout` passed, or the context of `Worker.RunTask` ended. | It goes back to its queue without using an attempt. Nil completes it. |

A batch handler's context ends only for a timeout or a shutdown. It can't
see a cancel request for one of its tasks: that task becomes completed if
the batch returns nil for it, and canceled if the batch returns an error for
it.

## Error classes

Each of taskmill's errors names its class with an `ErrorType() string`
method. The `otel` module reports it as `error.type` on spans and metrics.

| Class | Comes from |
|---|---|
| `not_found`, `invalid_state`, `invalid_argument`, `schema_outdated`, `conflict`, `lease_lost` | The errors that calls return, above |
| `canceled`, `timeout`, `shutdown` | The context causes, above |
| `panic` | A handler that panicked |
| `invalid_payload` | A payload that did not decode into the kind's `Args` type |
| `middleware` | A middleware that returned nil without calling `next` |

Your own errors get a name that OpenTelemetry takes from their Go type,
unless they have an `ErrorType() string` method too. A minor release may add
a class: treat one you don't know like any other failure.

## Error history codes

A task keeps its 20 most recent errors in `TaskInfo.Errors`. Each
[`AttemptError`](https://pkg.go.dev/github.com/code19m/taskmill#AttemptError)
has a `Code`:

| Code | Constant | What it means | What to do |
|---|---|---|---|
| (empty) | — | Your handler returned this error. | Read the message. |
| `timeout` | [`CodeTimeout`](https://pkg.go.dev/github.com/code19m/taskmill#CodeTimeout) | The attempt ran longer than `HandlerOptions.Timeout`, and the handler then returned an error. | Make the handler faster, split the work, or raise `Timeout`. |
| `lease_lost` | [`CodeLeaseLost`](https://pkg.go.dev/github.com/code19m/taskmill#CodeLeaseLost) | The worker lost the attempt's lease: it crashed, stopped, or lost the database. | Usually nothing: the task is retried. If you see it often, check for crashed workers, the pool size, and the database. |
| `panic` | [`CodePanic`](https://pkg.go.dev/github.com/code19m/taskmill#CodePanic) | The handler panicked. The message holds the stack trace. | Fix the bug, then retry the task. |
| `invalid_payload` | [`CodeInvalidPayload`](https://pkg.go.dev/github.com/code19m/taskmill#CodeInvalidPayload) | The payload did not decode into the kind's `Args` type, so the task became dead right away. | Usually a payload change that old tasks don't fit. Fix the type, then retry, or delete the tasks. See [Change a task safely](../recipes/change-a-task-safely.md). |
| `unknown_kind` | [`CodeUnknownKind`](https://pkg.go.dev/github.com/code19m/taskmill#CodeUnknownKind) | No worker with a handler for the kind ran the task within an hour, so it became dead. | Run a worker that handles the kind, then retry the task. Or delete it. |
| `no_handler` | [`CodeNoHandler`](https://pkg.go.dev/github.com/code19m/taskmill#CodeNoHandler) | A note, not a failure: a worker without a handler for the kind fetched the task. The task waits up to an hour for a worker that has one, without using attempts. | Usually a rolling deploy: wait. If it lasts, check that every worker of the queue has the same handlers. |

A minor release may add a code: treat one you don't know like any other
failure.

## See also

- [Retries and errors](../guides/retries-and-errors.md): retries, dead tasks, and error history
- [Log messages](log-messages.md): what taskmill logs when these happen
- [Errors on pkg.go.dev](https://pkg.go.dev/github.com/code19m/taskmill#pkg-variables)

**Next:** [Log messages](log-messages.md)
