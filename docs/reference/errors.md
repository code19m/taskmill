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
| [`ErrInvalidState`](https://pkg.go.dev/github.com/code19m/taskmill#ErrInvalidState) | `invalid_state` | The action does not fit the task's state. For example: retry a task that is not dead or canceled, cancel a finished task, delete a running one, or run a paused schedule. | Check the state first with `State.CanRetry`, `CanCancel`, or `CanDelete`, or accept that there is nothing to do. |
| [`ErrInvalidArgument`](https://pkg.go.dev/github.com/code19m/taskmill#ErrInvalidArgument) | `invalid_argument` | An argument, option, or configuration is wrong: out of range, too long, or text that PostgreSQL can't store, such as a NUL character. `NewClient` also returns it when the pool does not use READ COMMITTED. | Fix the call: the message says what is wrong. Retrying the same call doesn't help. |
| [`ErrSchemaOutdated`](https://pkg.go.dev/github.com/code19m/taskmill#ErrSchemaOutdated) | `schema_outdated` | The [schema](../concepts/glossary.md#schema) is missing, or older than this version of taskmill. `NewClient` checks it. | Run `Migrate`, or apply `Migrations` with your own tool, before you start the new version. |
| [`ErrConflict`](https://pkg.go.dev/github.com/code19m/taskmill#ErrConflict) | `conflict` | `UpdateQueue` or `UpdateSettings` tried 10 times. Each time, another update changed the same settings first. | Try again a little later. |
| [`ErrLeaseLost`](https://pkg.go.dev/github.com/code19m/taskmill#ErrLeaseLost) | `lease_lost` | Returned by `Task.CompleteTx`: another attempt took the task over. | Return it from your handler, so that your transaction rolls back. |

The `outbox`, `ui`, and `otel` packages return the same errors, such as
`ErrInvalidArgument` for a message without a topic.

A database or network error has none of these classes. taskmill wraps it
with what it was doing, so `errors.As` still finds pgx's error types in it.

One of them is PostgreSQL's `deadlock detected` (SQLSTATE `40P01`), which
ends one of two transactions that wait for each other:

- **`Cancel`, `Delete`, and `Retry` run again by themselves** when it hits
  them, up to 5 times in all.
- **`CancelTx` cannot,** because the transaction is yours. When another
  transaction changes the same task or its FIFO group at the same moment,
  yours may get this error. Run your transaction again
  ([Use your own transactions](../guides/transactions.md#cancel-a-task-in-the-same-transaction)).

## Which call returns which error

Every call that takes arguments can return `ErrInvalidArgument`. This table
lists the other errors of each call.

| Call | Errors |
|---|---|
| `NewClient` | `ErrSchemaOutdated` |
| `Get` | `ErrNotFound` |
| `Cancel`, `CancelTx` | `ErrNotFound`. `ErrInvalidState` for a finished task |
| `Retry` | `ErrNotFound`. `ErrInvalidState` for a task that is not dead or canceled |
| `Delete` | `ErrNotFound`. `ErrInvalidState` for a running task |
| `RetryMatching`, `CancelMatching`, `DeleteMatching` | The context's error, if it ends first. They skip the tasks that the action does not fit |
| `PauseQueue`, `ResumeQueue` | Never `ErrNotFound`: they create the queue's settings if it has none |
| `UpdateQueue`, `UpdateSettings` | `ErrConflict`. `UpdateQueue` never returns `ErrNotFound` |
| `PauseSchedule`, `ResumeSchedule` | `ErrNotFound` |
| `RunSchedule` | `ErrNotFound`. `ErrInvalidState` for a paused schedule |
| `Worker.Run` | `ErrInvalidState` if the worker already runs. An error without a class if it cannot register in the database |
| `Worker.RunTask` | `ErrNotFound`. `ErrInvalidState` for a task that is running, that waits behind its FIFO group, or that has finished |
| `Worker.Health` | An error without a class |
| `Task.CompleteTx` | `ErrLeaseLost` |

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
| [`ErrLeaseLost`](https://pkg.go.dev/github.com/code19m/taskmill#ErrLeaseLost) | `lease_lost` | The worker could not renew the [lease](../concepts/glossary.md#lease) in time, so the task may already run on another worker. Or this worker fetched the task again, after the maintainer rescued it: the older attempt gets this cause. | The result is thrown away. The task is retried. |
| [`ErrAttemptTimeout`](https://pkg.go.dev/github.com/code19m/taskmill#ErrAttemptTimeout) | `timeout` | The attempt ran longer than `HandlerOptions.Timeout`. | An error fails the attempt, with the code `timeout`. Nil completes the task. |
| [`ErrShutdown`](https://pkg.go.dev/github.com/code19m/taskmill#ErrShutdown) | `shutdown` | The worker is stopping and `DrainTimeout` passed, or the context of `Worker.RunTask` ended. | It goes back to its queue without using an attempt. Nil completes it. |

A batch handler's context ends only for a timeout or a shutdown, never for
a cancel request or a lost lease of one of its tasks
([Handle tasks in batches](../guides/batch-handlers.md)).

## Error classes

Each error in the tables above names its class with an `ErrorType() string`
method. The `otel` module reports it as `error.type` on spans and metrics.

| Class | Comes from |
|---|---|
| `not_found`, `invalid_state`, `invalid_argument`, `schema_outdated`, `conflict`, `lease_lost` | The errors that calls return, above |
| `canceled`, `timeout`, `shutdown` | The context causes, above |
| `panic` | A handler that panicked, or an outbox publisher that panicked |
| `invalid_payload` | A payload that did not decode into the kind's `Args` type |
| `middleware` | A middleware that returned nil without calling `next` |
| A Go type name, such as `*errors.errorString` | Your own error. OpenTelemetry takes the name from the error's type, unless the error has an `ErrorType() string` method |
| `*taskmill.PermanentError` | `Permanent(err)`, when `err` has no `ErrorType() string` method. If it has one, you see that class instead |

A snooze is not a failure, so it has no class. A minor release may add a
class: treat one you don't know like any other failure.

## Error history codes

A task keeps its 20 most recent errors in `TaskInfo.Errors`. Each
[`AttemptError`](https://pkg.go.dev/github.com/code19m/taskmill#AttemptError)
has a `Code`:

| Code | Constant | What it means | What to do |
|---|---|---|---|
| (empty) | — | Your handler returned this error. | Read the message. |
| `timeout` | [`CodeTimeout`](https://pkg.go.dev/github.com/code19m/taskmill#CodeTimeout) | The attempt ran longer than `HandlerOptions.Timeout`, and the handler then returned an error. | Make the handler faster, split the work, or raise `Timeout`. |
| `lease_lost` | [`CodeLeaseLost`](https://pkg.go.dev/github.com/code19m/taskmill#CodeLeaseLost) | The worker lost the attempt's lease: it crashed, stopped, or lost the database. | Usually nothing: the task is retried. If you see it often, check for crashed workers, the pool size, and the database. |
| `panic` | [`CodePanic`](https://pkg.go.dev/github.com/code19m/taskmill#CodePanic) | The handler panicked. Or code that the worker ran after the handler returned panicked: a method of the error it returned, such as `Error` on a nil pointer, or `RetryPolicy.Backoff`. After such a panic, the task is retried with the default backoff, not with the kind's policy. The message holds the stack trace. | Fix the bug, then retry the task. |
| `invalid_payload` | [`CodeInvalidPayload`](https://pkg.go.dev/github.com/code19m/taskmill#CodeInvalidPayload) | The payload did not decode into the kind's `Args` type, so the task became dead right away. | Usually a payload change that old tasks don't fit. Fix the type, then retry, or delete the tasks. See [Change a task safely](../recipes/change-a-task-safely.md). |
| `unknown_kind` | [`CodeUnknownKind`](https://pkg.go.dev/github.com/code19m/taskmill#CodeUnknownKind) | No worker with a handler for the kind took the task within an hour of its `no_handler` note, so it became dead. | Run a worker that handles the kind, then retry the task. Or delete it. |
| `no_handler` | [`CodeNoHandler`](https://pkg.go.dev/github.com/code19m/taskmill#CodeNoHandler) | A note, not a failure: a worker without a handler for the kind fetched the task. The task waits up to an hour for a worker that has one, without using attempts. The wait counts only while this note is the task's last error. A worker with a handler that snoozes the task, or gives it back, removes the note, and so does a retry. | Usually a rolling deploy: wait. If it lasts, check that every worker of the queue has the same handlers. |

A minor release may add a code: treat one you don't know like any other
failure.

## See also

- [Retries and errors](../guides/retries-and-errors.md): retries, dead tasks, and error history
- [Log messages](log-messages.md): what taskmill logs when these happen
- [Errors on pkg.go.dev](https://pkg.go.dev/github.com/code19m/taskmill#pkg-variables)

**Next:** [Log messages](log-messages.md)
