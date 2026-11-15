# Errors

The names of taskmill's errors, as you see them in code, in `error.type`,
and in a task's error history, each with the identifier that explains it.
The doc comment of the identifier says what the error means, which calls
return it, and what happens to the task.

Test an error with `errors.Is`, and the cause of a handler's canceled
context with `context.Cause`. Never compare messages: they may change in
any release. The example of
[`Client.Cancel`](https://pkg.go.dev/github.com/code19m/taskmill#example-Client.Cancel)
shows how.

## Error classes

A class is the name that an error has as `error.type`, on the spans and the
metrics of the `otel` module ([Metrics and traces](metrics.md)).

| Class | Identifier | Where you meet it |
|---|---|---|
| `not_found` | [`ErrNotFound`](https://pkg.go.dev/github.com/code19m/taskmill#ErrNotFound) | A call returns it |
| `invalid_state` | [`ErrInvalidState`](https://pkg.go.dev/github.com/code19m/taskmill#ErrInvalidState) | A call returns it |
| `invalid_argument` | [`ErrInvalidArgument`](https://pkg.go.dev/github.com/code19m/taskmill#ErrInvalidArgument) | A call returns it |
| `schema_outdated` | [`ErrSchemaOutdated`](https://pkg.go.dev/github.com/code19m/taskmill#ErrSchemaOutdated) | `NewClient` returns it |
| `conflict` | [`ErrConflict`](https://pkg.go.dev/github.com/code19m/taskmill#ErrConflict) | `UpdateQueue`, `UpdateSettings`, or `Retry` returns it |
| `lease_lost` | [`ErrLeaseLost`](https://pkg.go.dev/github.com/code19m/taskmill#ErrLeaseLost) | `Task.CompleteTx` returns it, or a handler's context ends with it |
| `canceled` | [`ErrTaskCanceled`](https://pkg.go.dev/github.com/code19m/taskmill#ErrTaskCanceled) | A handler's context ends with it |
| `timeout` | [`ErrAttemptTimeout`](https://pkg.go.dev/github.com/code19m/taskmill#ErrAttemptTimeout) | A handler's context ends with it |
| `shutdown` | [`ErrShutdown`](https://pkg.go.dev/github.com/code19m/taskmill#ErrShutdown) | A handler's context ends with it |
| `panic`, `invalid_payload`, `middleware` | No error value: see [the comment of the errors](https://pkg.go.dev/github.com/code19m/taskmill#pkg-variables) | An attempt or an enqueue fails with it |
| `*taskmill.PermanentError` | [`PermanentError`](https://pkg.go.dev/github.com/code19m/taskmill#PermanentError) | An attempt fails with it |
| A Go type name, such as `*errors.errorString` | Your own error: see [package `otel`](https://pkg.go.dev/github.com/code19m/taskmill/otel) | An attempt fails with it |

## Errors that your handler returns

Your handler chooses what happens to its task with
[`Permanent`](https://pkg.go.dev/github.com/code19m/taskmill#Permanent) and
[`Snooze`](https://pkg.go.dev/github.com/code19m/taskmill#Snooze), which
return a [`*PermanentError`](https://pkg.go.dev/github.com/code19m/taskmill#PermanentError)
and a [`*SnoozeError`](https://pkg.go.dev/github.com/code19m/taskmill#SnoozeError).
[Handle tasks](../guides/handle-tasks.md) says which one to return when.

## Error history codes

A task keeps its latest errors in
[`TaskInfo.Errors`](https://pkg.go.dev/github.com/code19m/taskmill#TaskInfo.Errors).
Each entry is an
[`AttemptError`](https://pkg.go.dev/github.com/code19m/taskmill#AttemptError),
and its `Code` is empty when your handler returned the error in time.

| Code | Constant |
|---|---|
| `timeout` | [`CodeTimeout`](https://pkg.go.dev/github.com/code19m/taskmill#CodeTimeout) |
| `lease_lost` | [`CodeLeaseLost`](https://pkg.go.dev/github.com/code19m/taskmill#CodeLeaseLost) |
| `panic` | [`CodePanic`](https://pkg.go.dev/github.com/code19m/taskmill#CodePanic) |
| `invalid_payload` | [`CodeInvalidPayload`](https://pkg.go.dev/github.com/code19m/taskmill#CodeInvalidPayload) |
| `invalid_row` | [`CodeInvalidRow`](https://pkg.go.dev/github.com/code19m/taskmill#CodeInvalidRow) |
| `unknown_kind` | [`CodeUnknownKind`](https://pkg.go.dev/github.com/code19m/taskmill#CodeUnknownKind) |
| `no_handler` | [`CodeNoHandler`](https://pkg.go.dev/github.com/code19m/taskmill#CodeNoHandler) |

[Retries and errors](../guides/retries-and-errors.md) says what to do about
each code.

## See also

- [Retries and errors](../guides/retries-and-errors.md): retries, dead tasks, and error history
- [Troubleshooting](../production/troubleshooting.md): from an error to its fix
- [Compatibility](compatibility.md): which of these names may change
- [Log messages](log-messages.md): what taskmill logs when these happen

**Next:** [The `taskmill-ui` command](taskmill-ui.md)
