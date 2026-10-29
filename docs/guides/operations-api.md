# Manage tasks from code

The `Client` lets your code and your tools do what the dashboard does: find
tasks, and retry, cancel, or delete them, one at a time or a million in one
call. This page also shows how long taskmill keeps finished tasks, and how
to look at FIFO groups.

## Which should I use?

| Situation | What to use | Why |
|---|---|---|
| Run failed tasks again, after you fixed the cause | `Retry`, or `RetryMatching` for many | They run again with fresh attempts |
| Stop some tasks for good, and keep a record | `Cancel`, or `CancelMatching` for many | Canceled tasks are kept, 30 days by default, and you can retry them |
| Stop a whole queue for a while: an incident, a partner is down | `PauseQueue`, then `ResumeQueue` | Tasks wait, and nothing is lost. Running tasks finish |
| Remove tasks with no record: test data, a mistake | `Delete`, or `DeleteMatching` for many | It can't be undone |

Prefer cancel to delete: a canceled task stays visible, and you can undo it.

The `Client` also reads and changes queues (`Queues`, `UpdateQueue`),
schedules (`Schedules`, `PauseSchedule`), settings (`UpdateSettings`), and
lists the workers (`Workers`). The
[`Client`](https://pkg.go.dev/github.com/code19m/taskmill#Client) on
pkg.go.dev lists every method.

## Find tasks

`List` returns the tasks that match a `TaskFilter`, newest first, one page
at a time:

```go
params := &taskmill.ListParams{Filter: taskmill.TaskFilter{
    Queues: []string{"emails"},
    States: []taskmill.State{taskmill.StateDead},
}}
for {
    page, err := client.List(ctx, params)
    if err != nil {
        return err
    }
    for _, t := range page.Tasks {
        log.Printf("task %d (%s) is dead after %d attempts", t.ID, t.Kind, t.Attempt)
    }
    if page.Next == "" {
        break
    }
    params.Cursor = page.Next
}
```

- A filter can name queues, states, kinds, and FIFO group keys. A task
  matches when it matches every field you set. An empty field matches all.
- Each page's query stops after 5 seconds, so listing never overloads the
  database. On a very large table, a filter that matches few tasks can hit
  that limit and return an error. Add a queue or a state to the filter.
- `Get` returns one task by ID. `Count` counts the matching tasks per
  state, up to 10,000 per state.

## Retry, cancel, or delete one task

Each action works only on some states:

| State | `Cancel` | `Retry` | `Delete` |
|---|---|---|---|
| `scheduled`, `pending`, `available`, `retryable` | Yes, right away | No | Yes |
| `running` | Yes, when its attempt ends | No | No: cancel it first |
| `completed` | No | No: enqueue a new task | Yes, from history |
| `dead`, `canceled` | No | Yes, with fresh attempts | Yes |

In other states, the action returns `ErrInvalidState`. An unknown ID
returns `ErrNotFound`. `CancelTx` cancels inside your own transaction.

**Canceling a running task** asks it to stop. The worker cancels the
handler's context, with the cause `ErrTaskCanceled`. The task becomes
`canceled` when the handler returns an error. If the handler returns `nil`,
because the work was already done, the task is `completed`. A
[batch handler](batch-handlers.md#cancel-a-task-that-runs-in-a-batch)
never sees the request: the task becomes `completed` if the batch returns
`nil` for it, and `canceled` if the batch returns an error for it.

In a [FIFO group](ordering.md), retrying the dead task that halts the group
resumes the group. Any other retried task of a group waits as `pending`
until the group's current head is done.

## Act on many tasks

`RetryMatching`, `CancelMatching`, and `DeleteMatching` act on every task
that matches a filter. Use them to retry thousands of dead tasks after an
incident:

```go
res, err := client.RetryMatching(ctx, &taskmill.BulkParams{Filter: taskmill.TaskFilter{
    Queues: []string{"emails"},
    States: []taskmill.State{taskmill.StateDead},
}})
if err != nil {
    return err // res says what it did so far; pass res.Next as BulkParams.Cursor to continue
}
log.Printf("retried %d tasks, skipped %d", res.Changed, res.Skipped)
```

- **The filter must not be empty.** To retry every dead task, set `States`.
- **They skip tasks that the action doesn't apply to,** as in the table
  above.
- **They work in small steps:** batches of 1,000 tasks, each in a short
  transaction, so they never hold locks for long. A retry keeps each FIFO
  group in order.
- **`Skipped` counts the tasks that were busy** in another transaction at
  that moment. Run the action again to act on them.
- **If your context ends,** they stop after the current batch. They return
  what they did, an error, and `Next`: pass it as `BulkParams.Cursor` to go
  on from there.

> [!TIP]
> Call `Count` with the same filter first: `TaskCounts.Retry`, `Cancel`,
> and `Delete` say how many tasks each action would change. And add a queue
> or a state to the filter: a filter by kind or group alone reads many more
> rows.

After an incident, follow
[Recover from an incident](../recipes/recover-from-an-incident.md): pause,
retry, then resume.

## Choose how long finished tasks are kept

| Tasks | Kept for | Setting |
|---|---|---|
| Completed | 7 days, in a history table | `Settings.HistoryRetention` |
| Dead and canceled | 30 days | `Settings.DeadRetention` |
| A dead task that halts its FIFO group | Until you retry or delete it | None |
| Completed tasks of a kind with `DiscardCompleted` | Not kept | `HandlerOptions.DiscardCompleted` |

The settings apply to the whole schema, and each must be at least 1 hour.
History is dropped one whole day at a time, so a completed task can stay up
to a day longer. To change a setting:

```go
err := client.UpdateSettings(ctx, func(s *taskmill.Settings) {
    s.HistoryRetention = 2 * 24 * time.Hour // zero means the default
})
```

Your function may run more than once, so only set fields in it.

History grows with every completed task. If nobody reads the completed
tasks of a busy kind, set `DiscardCompleted` in its
[`HandlerOptions`](handle-tasks.md#options-per-kind). Otherwise, keep
history, and shorten `HistoryRetention` if disk space matters.

## Look at FIFO groups

`Groups` lists the [FIFO groups](ordering.md) that have unfinished tasks,
or a dead task that halts them, one page at a time. Each `GroupInfo` has
the group's `Queue` and `Key`, how many `Tasks` it has, and `HaltedBy`: the
ID of the dead task that halts it, or 0.

In `GroupParams`, set `Halted: true` for only the halted groups, and
`Search` to find groups by a part of their key. To list the tasks of a
group, use `List` with `TaskFilter.GroupKeys`. To fix a halted group, see
[Fix a halted group](ordering.md#fix-a-halted-group).

## See also

- [The dashboard](dashboard.md): the same actions, in a browser
- [Recover from an incident](../recipes/recover-from-an-incident.md): pause, retry, resume
- [`Client`](https://pkg.go.dev/github.com/code19m/taskmill#Client) and [`Client.RetryMatching`](https://pkg.go.dev/github.com/code19m/taskmill#Client.RetryMatching) on pkg.go.dev

**Next:** [Logs, traces, and metrics](observability.md)
