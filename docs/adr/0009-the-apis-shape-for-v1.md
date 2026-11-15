# 0009. The API's shape for v1.0.0

- Status: Accepted
- Date: 2026-11-16

## Context

From v1.0.0 on, [the API does not break](../reference/compatibility.md): an
interface keeps its methods, a name stays until v2, and CI fails a change
that is not compatible. So the API of that day is the API for years. A
review before it listed the points that can change now or never. This record
holds the decision on each one, so that the next review finds the answer
with its reason.

## Decision

**Two points change before v1.0.0. The others are frozen as they are.**

| Point | Decision |
|---|---|
| `PgxQuerier` and `SQLQuerier` | Change: each keeps only the two methods that taskmill calls |
| The JSON form of the result types | Change: no result type has JSON tags |
| `ui.New` | Frozen: it returns `http.Handler` |
| A nil or short result of a batch | Frozen: a task without a result fails |
| `Middleware`, `AttemptError`, the two `Counts`, `GroupParams` | Frozen: the names stay |

**The querier interfaces hold what taskmill calls, and no more.**
`PgxQuerier` is `Exec` and `Query`, and `SQLQuerier` is `ExecContext` and
`QueryContext`. They also had `QueryRow` and `QueryRowContext`, which
taskmill never called on a caller's value. From v1.0.0 on, those methods
would stay for good, and every wrapper that a user writes, to trace
statements or for a test, would carry a method that nothing calls. No code
that passes a pgx or `database/sql` value breaks.

**The result types have no JSON tags, and get none.** `AttemptError` had
tags, because taskmill decoded the `errors` column straight into it. So the
public type was also the storage format, and an encoded `TaskInfo` had two
styles of keys. The stored names now live in a private type, and a test
pins them. Field names cannot change within v1, so Go's default JSON of a
result keeps its keys without any promise but this one: no tags. A program
that sends results to another program encodes a type of its own.

**`ui.New` returns `http.Handler`.** The dashboard starts no goroutine and
owns no resource: it uses the client's pool. So it needs no method, such as
`Close`. Callers wrap the result, as in `dashboard = requireAdmin(dashboard)`,
and that line stops building if `New` returns a concrete type. The standard
library returns `http.Handler` from its helpers of this kind too.

**A task without a result fails.** When a `BatchHandler` or an
`outbox.Publisher` returns nil, or fewer errors than tasks, each task
without an entry fails, and is retried. The other rule, "nil means that all
succeeded", reads like Go. But compare what each mistake costs:

| The rule | The mistake | What happens |
|---|---|---|
| A missing result fails | A publisher ends in `return nil` after it sent its messages | The messages are sent again at each retry. It is loud: the tasks stay retryable, their errors say why, and `QueueInfo.MostFailures` grows. Duplicates are within the promise of at least once |
| A missing result succeeds | A handler returns early with nil, or forgets one entry | The tasks are completed, and their work was never done. Nothing shows it |

A lost task breaks the first promise of the project, so the rule fails
closed. Attempt middleware that returns nil without calling `next` fails
for the same reason.

**The names stay.** Each is less regular than it could be, and each rename
would break the build of its callers for no change in behavior.

| Name | Why it stays |
|---|---|
| `Middleware`, next to `EnqueueMiddleware` | `WorkerConfig.Middleware []Middleware` reads right: it is the worker's. The later and rarer kind has the longer name. A rename changes every user's middleware, and the `otel` module |
| `AttemptError`, which is no `error`, and also holds notes | It is an entry of `TaskInfo.Errors`, and callers rarely write its name. It cannot pass for an error: the compiler refuses to return it as one, and `go vet` reports `errors.As` with it |
| `QueueInfo.Counts` and `Activity.Counts`, with different values | The facts differ. A queue's count may have stopped at its cap, so it is a `TaskCount`. A point of the activity holds the largest of several counts, where "capped" has no clear meaning, so it is a number |
| `GroupParams`, next to `GroupsResult` | `ListParams` and `ListResult` follow their method's name, and `GroupParams` does not. `GroupsParams` would be regular, but the gain is cosmetic |

### Options we rejected

| Option | Why not |
|---|---|
| Keep the two querier methods | It costs nothing now. But an interface that never grows never shrinks either: the unused methods would stay until v2 |
| Tag every result type in snake_case, with durations in seconds | A tag on every field of about 15 types, an encoder of our own for durations, and a promise to keep all of it: a format for the network that taskmill itself never uses |
| Keep the tags of `AttemptError` only | One type would encode with other keys than the rest, and the reference would explain that accident for good |
| Remove the tags, and still decode the stored entries into `AttemptError` | It works, because `encoding/json` matches keys without regard to case. But the storage format would rest on a rule that nobody sees in the code |
| `ui.New` returns a concrete type | It pays now, with code that stops building, for a need that is a guess. And `ui.Handler`, the natural name of that type, was the constructor before `ui.New` |
| "Nil means that all succeeded" | Its mistake is silent: see the table above |
| Rename `GroupParams` to `GroupsParams` now, or add that name as an alias | The rename is allowed before v1.0.0, and breaks the build of every caller of `Client.Groups` for a gain that no user feels. The alias gives one thing two names until v2 |

## Consequences

- A user's wrapper of a transaction has two methods. Code that called
  `QueryRow` or `QueryRowContext` through taskmill's interface stops
  building once, and declares an interface of its own.
- Whoever encodes a `TaskInfo` gets other keys for its `Errors` entries
  once, with no build error: the changelog names the old and the new keys.
  After that, a release never renames a key, and adds one only with a new
  field.
- `make api` does not see a struct tag. `compatibility_test.go` does: it
  fails on a tag in any type that a method of `Client` returns, and on a
  stored error entry that no longer reads back.
- A batch handler or a publisher that returns too few results fails,
  loudly. Both doc comments say so.
- `GroupParams` stays the one name of its kind that does not follow its
  method. New pairs follow `ListParams` and `ListResult`.

We look at a point again only when this happens:

| Point | When | What then |
|---|---|---|
| The querier interfaces | taskmill needs another method of the caller's transaction, such as a batch | A new optional interface, never a method on these two |
| The JSON form | taskmill itself sends results to another program, such as a JSON API of the dashboard | Types of its own for that API, with tags. The result types still get none |
| `ui.New` | The dashboard owns something that the caller must close | A second constructor that returns a concrete type. `New` stays, and calls it |
| A missing result | Users make this mistake often | An error code of its own for it, which any minor release may add. The rule stays for all of v1 |
| The names | v2 | Rename them together |
