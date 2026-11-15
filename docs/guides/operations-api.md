# Manage tasks from code

The `Client` lets your code and your tools do what the dashboard does: find
tasks, and retry, cancel, or delete them, one at a time or a million in one
call. This page also shows how long taskmill keeps finished tasks, and how
to look at FIFO groups.

## Quick example

```go
// Run a dead or canceled task again, with a new set of attempts.
if err := client.Retry(ctx, taskID); err != nil {
    return err
}
```

## Which should I use?

| Situation | What to use | Why |
|---|---|---|
| Run failed tasks again, after you fixed the cause | `Retry`, or `RetryMatching` for many | They run again, with a new set of attempts |
| Stop some tasks permanently, and keep a record | `Cancel`, or `CancelMatching` for many | Canceled tasks are kept, 30 days by default, and you can retry them |
| Stop a whole queue for a while: an incident, a partner is down | `PauseQueue`, then `ResumeQueue` | Tasks wait, and nothing is lost. Running tasks finish |
| Remove tasks with no record: test data, a mistake | `Delete`, or `DeleteMatching` for many | It can't be undone |

Prefer cancel to delete: a canceled task stays visible, and you can retry
it. A cancel frees the task's dedupe key, and a retry takes the key back,
unless another task took it meanwhile. See
[Retry a canceled task that has a key](deduplication.md#retry-a-canceled-task-that-has-a-key).

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
- Each page's query stops after 5 seconds, so a list never loads the
  database for longer. A list that needs more returns an error.
- Three lists read about the tasks they show, on a table of any size: all
  tasks, the completed tasks of one queue or of all, and the pending tasks
  of one queue's FIFO group. An index holds their tasks newest first.
- A list of dead or canceled tasks has no such index, for one queue or for
  all. It is fast in two cases: the tasks that it shows are among the
  newest of all tasks, or the database expects to find few of them, and
  reads them all. It expects few when few tasks are dead, or when the queue
  that you name holds a small part of all tasks. In other cases it reads
  the newer tasks of every queue and state first, which can take the 5
  seconds on a very large table. The same holds for a list by kind, or by
  another state.
- Set `Summary: true` in `ListParams` when you show one line per task. A
  page then comes without payloads and metadata, and with only the newest
  error of each task. So it stays small, whatever the tasks hold: a full
  page of 1,000 tasks can hold 1 GiB of payloads.
- `Get` returns one task by ID, in full. `Count` counts the matching tasks
  per state, up to 10,000 per state.

## Retry, cancel, or delete one task

```go
err := client.Cancel(ctx, taskID)
switch {
case errors.Is(err, taskmill.ErrInvalidState):
    // The task already finished: there is nothing to cancel.
case errors.Is(err, taskmill.ErrNotFound):
    // No task has this ID, or it was deleted.
case err != nil:
    return err
}
```

- **Each action works only in some states.** For example, you can retry
  only a dead or a canceled task.
  [Task lifecycle](../concepts/task-lifecycle.md#what-you-can-do-in-each-state)
  has the table. In another state, the action returns `ErrInvalidState`.
- **An unknown ID returns `ErrNotFound`.**
- **A retry returns `ErrConflict`** when another task holds the dedupe key
  that the canceled task had. Nothing changes: that task stands for the
  same work.
- **A running task is only asked to stop.** What its handler returns then
  decides if it becomes `canceled` or `completed`. See
  [Cancel a running task](../concepts/task-lifecycle.md#cancel-a-running-task).
  For a batch, see
  [Handle tasks in batches](batch-handlers.md#cancel-a-task-that-runs-in-a-batch).
- **`CancelTx` cancels inside your own transaction:** see
  [Use your own transactions](transactions.md#cancel-a-task-in-the-same-transaction).
- **`Cancel`, `Delete`, and `Retry` run again by themselves,** up to 5 times
  in all, when another transaction changes the same task or its FIFO group
  at the same moment, and PostgreSQL ends theirs with `deadlock detected`.
  `CancelTx` cannot do that in your transaction: run it again yourself.
- **In a [FIFO group](ordering.md),** a retry of the dead task that halts
  the group resumes the group. Any other retried task of a group waits as
  `pending` until the group's current head is done.

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
- **They change only tasks in a state that allows the action.** For
  example, `DeleteMatching` leaves running tasks as they are.
- **They work in small steps:** batches of 1,000 tasks, each in a short
  transaction, so they never hold locks for long. A retry keeps each FIFO
  group in order.
- **They get past any number of tasks that don't match.** If a batch can't
  reach its tasks within a second, the action continues in batches that
  read at most 100,000 tasks each.
- **`Skipped` counts the tasks that were busy** in another transaction at
  that moment. Run the action again to act on them. While a transaction
  that enqueued a dedupe key stays open, a retry can skip every task of a
  batch that needs a key back.
- **`Conflicts` counts the canceled tasks that a retry left,** because
  another task holds their dedupe key. Running the action again does not
  change them.
- **If your context ends,** they stop after the current batch. They return
  what they did, an error, and `Next`: pass it as `BulkParams.Cursor` to
  continue from there.
- **After an error, the count can be one batch short.** If the database
  does not confirm the last batch in time, it may still commit it. Running
  the action again is safe: the tasks that it changed no longer match.

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
