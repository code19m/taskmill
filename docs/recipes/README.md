# How do I…?

Find the recommended way to do a common thing. Each recipe solves one real
problem from start to end. Other answers are in the guides and the
production pages: the tables after the recipes link to them.

## Recipes

Each row is one recipe, in reading order:

| I want to… | Recommended way | Read |
|---|---|---|
| Charge a card once, although a task may run twice | An idempotency key from the task ID, and `Task.CompleteTx` for your own data | [Do work once](do-work-once.md) |
| Respect a partner's limit: 4 calls at the same time, or 600 a minute | Its own queue with `MaxRunning`, one row that spaces the calls, and `Snooze` on HTTP 429 | [Call a rate-limited API](call-a-rate-limited-api.md) |
| Apply one account's events in order | A `GroupKey` per account | [Keep order per key](keep-order-per-key.md) |
| Stop one customer's big batch from delaying all the others | A queue for batch work, and a `GroupKey` per customer | [Share a queue fairly between customers](share-a-queue-between-customers.md) |
| Run one refresh for 50 changes to the same product | A flag in your own row, set in the transaction that enqueues | [Keep at most one waiting task per key](one-waiting-task-per-key.md) |
| Stop a reminder when the customer pays, or move it to another day | `CancelTx` in the transaction that changes your data, and a check in the handler | [Cancel or replace a delayed task](cancel-a-delayed-task.md) |
| Send each customer a summary at 09:00 local time | A due time per customer in your own table, and one schedule that enqueues what is due | [Run a task at each customer's local time](run-at-each-customers-local-time.md) |
| Replace a crontab line | A `Schedule` with the same five fields, and a `Location` | [Move a cron job to a schedule](move-a-cron-job-to-a-schedule.md) |
| Act on Kafka messages without doing the work twice | Enqueue with the message ID as `DedupeKey` | [Consume from a broker](consume-from-a-broker.md) |
| Change a payload, rename a kind, or move it to another queue | A new kind, in three deploys | [Change a task safely](change-a-task-safely.md) |
| Run tasks that take 10 minutes or more | A longer `Timeout` and `DrainTimeout`. Split work that takes hours | [Run long tasks](run-long-tasks.md) |
| Retry 5,000 dead tasks after a fix | Pause the queue, `RetryMatching`, then resume | [Recover from an incident](recover-from-an-incident.md) |
| Run several services, or give work to another one | One schema per service, and a `Client` for the other's schema | [Split work across services](split-across-services.md) |
| See every service in one admin app | A dashboard in each service. The admin app forwards a path to each | [One dashboard for many services](one-dashboard-for-many-services.md) |
| Use goose, Atlas, or Flyway | Apply `taskmill.Migrations` with your tool | [Use your own migration tool](use-your-migration-tool.md) |

## Write and enqueue tasks

| I want to… | Recommended way | Read |
|---|---|---|
| Send an email only if the order was saved | `EnqueueTx` in the order's transaction | [Use your own transactions](../guides/transactions.md) |
| Enqueue thousands of tasks | `EnqueueMany`, up to 10,000 per call | [Enqueue tasks](../guides/enqueue.md) |
| Set a kind's queue and attempts in one place | `EnqueueDefaults` on the task type | [Define tasks](../guides/define-tasks.md) |
| Never enqueue the same receipt twice | A `DedupeKey` | [Avoid duplicate tasks](../guides/deduplication.md) |
| Remind a customer 2 days after an order, once | `RunAt`, and a `DedupeWindow` longer than the delay | [Avoid duplicate tasks](../guides/deduplication.md) |
| Run a report every night at 03:00 in Berlin | A `Schedule` with a `Location` | [Run tasks on a schedule](../guides/schedules.md) |
| Keep a nightly run from overlapping the next one | A `GroupKey` in `Schedule.Options` | [Run tasks on a schedule](../guides/schedules.md) |
| Publish to Kafka only if my transaction commits | The outbox | [Publish events with the outbox](../guides/outbox.md) |

## Run tasks

| I want to… | Recommended way | Read |
|---|---|---|
| Choose what my handler returns | `nil`, an error, `Permanent`, or `Snooze` | [Handle tasks](../guides/handle-tasks.md) |
| Let VIP receipts go first | A higher `Priority` in the same queue | [Task order and FIFO groups](../guides/ordering.md) |
| Keep slow work from delaying fast work | A separate queue | [Workers and queues](../guides/workers-and-queues.md) |
| Do work that is cheaper in bulk | A batch handler | [Handle tasks in batches](../guides/batch-handlers.md) |
| Give up early on work that is useless when late | A lower `MaxAttempts` in `EnqueueDefaults` | [Retries and errors](../guides/retries-and-errors.md) |

## Fix and manage tasks

| I want to… | Recommended way | Read |
|---|---|---|
| Unblock a halted FIFO group | Fix the cause, then retry its dead task | [Task order and FIFO groups](../guides/ordering.md) |
| Stop some tasks, and keep a record | `Cancel` or `CancelMatching` | [Manage tasks from code](../guides/operations-api.md) |
| Stop a whole queue for a while | `PauseQueue` | [Workers and queues](../guides/workers-and-queues.md) |
| Keep history small for a busy kind | `DiscardCompleted` | [Handle tasks](../guides/handle-tasks.md) |

## Deploy and operate

| I want to… | Recommended way | Read |
|---|---|---|
| Run workers in production | A worker deployment of its own, from the same image | [Deploys](../production/deploys.md) |
| Run migrations | `Migrate` at the start of every process | [Deploys](../production/deploys.md) |
| Size the connection pool | One pool: the worker's total `Concurrency`, plus 5 | [Database](../production/database.md) |
| Use PgBouncer | A direct connection in `ListenConfig` | [Connection poolers](../production/connection-poolers.md) |
| Add the dashboard to my app | `ui.New`, behind your own login | [The dashboard](../guides/dashboard.md) |
| Scale workers with the load | KEDA, on the queue's available plus running tasks | [Autoscaling](../production/autoscaling.md) |
| Probe workers in Kubernetes | `Worker.Health`, for readiness only | [Kubernetes](../production/kubernetes.md) |
| Know when something goes wrong | One list of alerts: lag, no maintainer, new dead tasks, halted groups, tasks that keep failing, Error logs | [Monitoring](../production/monitoring.md) |
| Test that my code enqueues the right task | `taskmilltest`, with a real PostgreSQL | [Test your code](../guides/testing.md) |
| Follow a task from the HTTP request that made it | The `otel` module | [Logs, traces, and metrics](../guides/observability.md) |

**Next:** [Do work once](do-work-once.md)
