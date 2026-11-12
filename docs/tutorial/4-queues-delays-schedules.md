# Part 4: Queues, delays, and schedules

**Goal:** give receipts a queue of their own, run a reminder a minute after
each order, and build a report every minute. **Time:** about 15 minutes.
**You start from:** the program of [part 3](3-when-a-task-fails.md).

## Step 1. Give receipts a queue of their own

A [queue](../concepts/glossary.md#queue) is a named line of tasks. A kind
can choose its queue, and its number of attempts. Add this method below
`Kind`:

```go
// EnqueueDefaults puts receipts in a queue of their own, and gives up on one
// after 5 attempts.
func (SendReceipt) EnqueueDefaults() taskmill.EnqueueOptions {
	return taskmill.EnqueueOptions{Queue: "emails", MaxAttempts: 5}
}
```

Start the shop. **You should see** that the worker reads another queue:

```text
time=… level=INFO msg="worker started" worker_id=… queues=[emails]
```

By default, a worker reads the queue of each kind that it has a handler
for. Your worker handles one kind, so it now reads only `emails`.

## Step 2. Choose how many tasks run per queue

In `run`, put each field of `WorkerConfig` on a line of its own, and add
`Queues`:

<!-- grows -->
```go
		Handlers: &handlers,
		Queues: map[string]taskmill.QueueOptions{
			"default": {Concurrency: 5},
			"emails":  {Concurrency: 5},
		},
```

Start the shop again. **You should see** both queues:

```text
time=… level=INFO msg="worker started" worker_id=… queues="[default emails]"
```

The worker now runs at most 5 tasks of each queue at the same time, so 10
in all. That is the number in the comment above `cfg.MaxConns`: the pool
needs one connection for each of them, plus 5.

## Step 3. Remind the customer a minute later

A new kind asks the customer how the order went. Add it above `main`:

```go
// RemindCustomer asks a customer how the order went.
type RemindCustomer struct {
	OrderID int64  `json:"order_id"`
	Email   string `json:"email"`
}

// Kind names the task.
func (RemindCustomer) Kind() string { return "email.remind_customer" }

// remindCustomer returns the handler of RemindCustomer tasks.
func remindCustomer(logger *slog.Logger) taskmill.HandlerFunc[RemindCustomer] {
	return func(ctx context.Context, t *taskmill.Task[RemindCustomer]) error {
		logger.LogAttrs(ctx, slog.LevelInfo, "reminder sent",
			slog.Int64(logKeyTaskID, t.ID), slog.String(logKeyTo, t.Args.Email))
		return nil
	}
}
```

In `run`, add the handler below the one for receipts:

```go
	taskmill.AddHandler(&handlers, remindCustomer(logger), nil)
```

In `placeOrder`, declare a second result in place of `var res`:

```go
		var res, reminder taskmill.EnqueueResult
```

In the transaction, replace the `return err` below `EnqueueTx` with these
lines. `RunAt` delays the task. The
[dedupe key](../concepts/glossary.md#dedupe-key) allows one reminder per
address in an hour:

<!-- grows -->
```go
			if err != nil {
				return err
			}
			// Ask the customer a minute later how it went: once per address in an hour.
			reminder, err = client.EnqueueTx(ctx, taskmill.Pgx(tx), RemindCustomer{OrderID: orderID, Email: email}, &taskmill.EnqueueOptions{
				RunAt:        time.Now().Add(time.Minute),
				DedupeKey:    "remind-" + email,
				DedupeWindow: time.Hour, // counts from now, so it must be longer than the delay
			})
			return err
```

A duplicate is not an error. Log it, above the line that writes the answer:

```go
		if reminder.Duplicate {
			logger.LogAttrs(ctx, slog.LevelInfo, "the customer already has a reminder", slog.Int64(logKeyTaskID, reminder.ID))
		}
```

Start the shop, and place two orders with the same address:

```sh
curl -s -d email=user1@example.com localhost:8080/orders
curl -s -d email=user1@example.com localhost:8080/orders
```

**You should see** two receipts, but only one reminder task. A minute
later, the worker runs it:

```text
time=… level=INFO msg="receipt sent" task_id=19 to=user1@example.com
time=… level=INFO msg="the customer already has a reminder" task_id=20
time=… level=INFO msg="receipt sent" task_id=21 to=user1@example.com
time=… level=INFO msg="reminder sent" task_id=20 to=user1@example.com
```

Until then, the dashboard's **Tasks** page shows the reminder as
`scheduled`, in the queue `default`.

## Step 4. Build a report every minute

A [schedule](../concepts/glossary.md#schedule) enqueues a task on a cron
timetable. Add a key to the log keys:

```go
	logKeyScheduledFor = "scheduled_for"
```

Add the kind and its handler above `main`. The handler waits 5 seconds, as
real work would take time:

```go
// BuildReport builds the sales report. A schedule enqueues it every minute.
type BuildReport struct{}

// Kind names the task.
func (BuildReport) Kind() string { return "reports.build" }

// buildReport returns the handler of BuildReport tasks.
func buildReport(logger *slog.Logger) taskmill.HandlerFunc[BuildReport] {
	return func(ctx context.Context, t *taskmill.Task[BuildReport]) error {
		// Pretend that the report takes 5 seconds. Stop early if the context ends.
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(5 * time.Second):
		}
		// ScheduledFor is the minute that this run is for, on every attempt.
		logger.LogAttrs(ctx, slog.LevelInfo, "sales report built", slog.Time(logKeyScheduledFor, t.ScheduledFor()))
		return nil
	}
}
```

In `run`, add the handler, and give `WorkerConfig` a third field:

```go
	taskmill.AddHandler(&handlers, buildReport(logger), nil)
```

```go
		Schedules: []taskmill.Schedule{
			{Name: "sales-report", Cron: "* * * * *", Args: BuildReport{}},
		},
```

Start the shop, and wait for the next full minute. **You should see** one
report per minute, 5 seconds after the minute that it is for:

```text
time=…02:38:05.147+05:00 level=INFO msg="sales report built" scheduled_for=…21:38:00.000Z
time=…02:39:05.145+05:00 level=INFO msg="sales report built" scheduled_for=…21:39:00.000Z
```

The schedule sets no `Location`, so `scheduled_for` shows the minute in
UTC. The log's own time is in your time zone.

## Step 5. Look at it in the dashboard

Open <http://localhost:8080/taskmill/>. **You should see** two queues on
the **Overview**: `default` and `emails`.

Open **Schedules**. It shows `sales-report`, with its timetable and its
next and last run. Choose **Run now**, and confirm. The schedule enqueues
one more run within about a second, and the log shows its report 5 seconds
later.

## What you learned

| In the code | taskmill calls it |
|---|---|
| `EnqueueDefaults()` | the default options of a kind, such as its [queue](../concepts/glossary.md#queue) |
| `Queues`, with `Concurrency` | the queues that a worker reads, and its [concurrency](../concepts/glossary.md#concurrency) for each |
| `RunAt` | a delayed task. It waits in the `scheduled` [state](../concepts/glossary.md#state) |
| `DedupeKey` and `DedupeWindow` | a [dedupe key](../concepts/glossary.md#dedupe-key), and how long it blocks a second task |
| `Schedules` | a [schedule](../concepts/glossary.md#schedule), which the [maintainer](../concepts/glossary.md#maintainer) runs |
| `t.ScheduledFor()` | the time that a scheduled run is for. It is the same on every attempt |

## Go deeper

- [Define tasks](../guides/define-tasks.md#set-defaults-per-kind): defaults per kind, and which value wins
- [Workers and queues](../guides/workers-and-queues.md): how many queues you need, and limits across all workers
- [Avoid duplicate tasks](../guides/deduplication.md): the window of a task that runs later
- [Run tasks on a schedule](../guides/schedules.md): time zones, and runs that overlap

**Next:** [Part 5: The outbox](5-outbox.md)
