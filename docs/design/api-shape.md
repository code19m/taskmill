# The API's shape

From v1.0.0 on, [the API does not break](../reference/compatibility.md): an
interface keeps its methods, a name stays until v2, and CI fails a change
that is not compatible. So today's API is the API for years. This page says
why each point that could have another shape has this one, which options we
rejected, and when we look at a point again.

| Point | Its shape |
|---|---|
| `PgxQuerier` and `SQLQuerier` | Each has only the two methods that taskmill calls |
| The JSON form of the result types | No result type has JSON tags |
| `ui.New` | It returns `http.Handler` |
| A nil or short result of a batch | A task without a result fails |
| `Middleware`, `AttemptError`, the two `Counts`, `GroupParams` | The names stay as they are |
| `TaskInfo.FinishedAt` | A task's finish has one name |

## The querier interfaces hold what taskmill calls, and no more

`PgxQuerier` is `Exec` and `Query`, and `SQLQuerier` is `ExecContext` and
`QueryContext`. Neither has `QueryRow` or `QueryRowContext`: taskmill never
calls them on a caller's value. An interface keeps its methods for good.
So with them, every wrapper that a user writes, to trace statements or for
a test, would carry a method that nothing calls.

A pgx or `database/sql` value passes as it is, and a user's wrapper of a
transaction needs two methods. The cost: the interfaces are not the type
for a value of the caller's own that needs more of pgx. The `DBTX` that
sqlc generates for pgx has `QueryRow`, for example. Such code passes the
`pgx.Tx` or the pool itself there, or declares an interface of its own.

## The result types have no JSON tags, and get none

The types that taskmill hands to an application are Go values, not a
format for the network: the types that `Client` returns, the `Task` of a
handler, and the outbox's `Delivery`. They have no JSON tags, and no
marshal methods, which change the JSON as a tag does.

- **Go's default JSON keeps its keys anyway.** Field names cannot change
  within v1. So a release never renames a key, and adds one only with a new
  field. The promise is only this one: no tags, and no marshal methods.
- **A program that sends a result to another program encodes a type of its
  own.**
- **The stored names live in a private type.** taskmill reads the `errors`
  column of a task into that type, and converts each entry to an
  `AttemptError`. With tags on `AttemptError`, the public type would also be
  the storage format, and an encoded `TaskInfo` would have two styles of
  keys. A test pins the stored names.

## `ui.New` returns `http.Handler`

The dashboard starts no goroutine and owns no resource: it uses the
client's pool. So it needs no method, such as `Close`. Callers wrap the
result, as in `dashboard = requireAdmin(dashboard)`, and that line stops
building if `New` returns a concrete type. The standard library returns
`http.Handler` from its helpers of this kind too.

## A task without a result fails

When a `BatchHandler` or an `outbox.Publisher` returns nil, or fewer errors
than tasks, each task without an entry fails, and is retried. The other
rule, "nil means that all succeeded", reads like Go. But compare what each
mistake costs:

| The rule | The mistake | What happens |
|---|---|---|
| A missing result fails | A publisher ends in `return nil` after it sent its messages | The messages are sent again at each retry. It is loud: the tasks stay retryable, their errors say why, and `QueueInfo.MostFailures` grows. Duplicates are within the promise of at least once |
| A missing result succeeds | A handler returns early with nil, or forgets one entry | The tasks are completed, and their work was never done. Nothing shows it |

A lost task breaks the first promise of the project, so the rule fails
closed, and the doc comments of both types say so. Attempt middleware that
returns nil without calling `next` fails for the same reason.

## The names stay

Each of these names is less regular than it could be. Each rename would
break the build of its callers for no change in behavior.

| Name | Why it stays |
|---|---|
| `Middleware`, next to `EnqueueMiddleware` | `WorkerConfig.Middleware []Middleware` reads right: it is the worker's. The later and rarer kind has the longer name. A rename changes every user's middleware, and the `otel` module |
| `AttemptError`, which is no `error`, and also holds notes | It is an entry of `TaskInfo.Errors`, and callers rarely write its name. It cannot pass for an error: the compiler refuses to return it as one, and `go vet` reports `errors.As` with it |
| `QueueInfo.Counts` and `Activity.Counts`, with different values | The facts differ. A queue's count may have stopped at its cap, so it is a `TaskCount`. A point of the activity holds the largest of several counts, where "capped" has no clear meaning, so it is a number |
| `GroupParams`, next to `GroupsResult` | `ListParams` and `ListResult` follow their method's name, and `GroupParams` does not. `GroupsParams` would be regular, but the gain is cosmetic |

`GroupParams` stays the one name of its kind that does not follow its
method. New pairs follow `ListParams` and `ListResult`.

## A task's finish has one name

`TaskInfo.FinishedAt`, `State.Finished`, and the `FinishedSince` and
`FinishedBefore` of `ListParams` all say "finished", as the comments and
the pages do. A range's `Since` starts a period, as in `ActivityParams`, and
its `Before` leaves the end out, as `time.Time.Before` does. `CreatedSince`
and `CreatedBefore` follow them, and `TaskInfo.CreatedAt`.

Unlike [the names that stay](#the-names-stay), which are only less regular,
two names for one concept make a reader look for two things. So this one
name follows the others while the version is below v1.0.0, where a change
that breaks the API is still allowed.

The column keeps its name, `finalized_at`: renaming it needs a migration,
and the workers of the release before would fail on it during a rolling
deploy.

## Options we rejected

| Option | Why not |
|---|---|
| `QueryRow` and `QueryRowContext` in the querier interfaces | They cost nothing today. But an interface that never grows never shrinks either: the unused methods would stay until v2 |
| Tag every result type in snake_case | The tags are cheap to write. Their cost is the promise: a wire format for about 15 types, kept for all of v1, that taskmill itself never puts on a wire. A duration would still be a number of nanoseconds in it, unless we also wrote an encoder for durations |
| Tags on `AttemptError` only | One type would encode with other keys than the rest, and the reference would explain that accident for good |
| Keep the field named after its column, `finalized_at` | One field would say "finalized" in an API that says "finished" everywhere else. Before v1.0.0, a rename costs each caller one line. From v1.0.0 on, the odd name would stay until v2 |
| No tags, and still decode the stored entries into `AttemptError` | It works, because `encoding/json` matches keys without regard to case. But the storage format would rest on a rule that nobody sees in the code |
| `ui.New` returns a concrete type | It pays now, with code that stops building, for a need that is a guess. And `ui.Handler`, the natural name of that type, was the name of an earlier constructor |
| "Nil means that all succeeded" | Its mistake is silent: see the table above |
| Rename `GroupParams` to `GroupsParams`, or add that name as an alias | The rename is allowed until v1.0.0 is tagged, and breaks the build of every caller of `Client.Groups` for a gain that no user feels. The alias gives one thing two names until v2 |

## What guards the shape

`make api` compares the API with the latest release. It does not see a
struct tag, and it calls a new method compatible.
[`compatibility_test.go`](../../compatibility_test.go) sees both. It fails
on:

- a tag or a marshal method in any type that a method of `Client` returns,
  in `Task`, and in `outbox.Delivery`;
- a wrapper with only the two methods of a querier interface that can no
  longer enqueue;
- a stored error entry that no longer reads back.

## When we look at a point again

Only when this happens:

| Point | When | What then |
|---|---|---|
| The querier interfaces | taskmill needs another method of the caller's transaction, such as a batch | A new optional interface, never a method on these two |
| The JSON form | taskmill itself sends results to another program, such as a JSON API of the dashboard | Types of its own for that API, with tags. The result types still get none |
| `ui.New` | The dashboard owns something that the caller must close | A second constructor that returns a concrete type. `New` stays, and calls it |
| A missing result | Users make this mistake often | An error code of its own for it, which any minor release may add. The rule stays for all of v1 |
| The names | v2 | Rename them together |

## See also

- [Compatibility](../reference/compatibility.md): the promise that this shape must keep
- [CONTRIBUTING.md](../../CONTRIBUTING.md): the pattern that method names follow, and `make api`
- [`BatchHandler` on pkg.go.dev](https://pkg.go.dev/github.com/code19m/taskmill#BatchHandler): the rule for a missing result

**Next:** [Dependencies and modules](dependencies.md)
