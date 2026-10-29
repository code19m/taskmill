# How do I…?

Find the preferred way to do common things with taskmill. Each row says what
we recommend, and links to the page that shows how.

## Recipes

Each recipe starts from a real situation, shows the preferred way in code,
and says when to choose another way.

| Recipe | Read it when… |
|---|---|
| [Do work once](do-work-once.md) | your task charges a card or writes rows, and it may run twice |
| [Call a rate-limited API](call-a-rate-limited-api.md) | a partner allows only a few calls at the same time |
| [Keep order per key](keep-order-per-key.md) | the events of one account must apply in order |
| [Consume from a broker](consume-from-a-broker.md) | you receive messages from Kafka, NATS, or RabbitMQ |
| [Change a task safely](change-a-task-safely.md) | you change a payload, rename a kind, or move or remove a kind |
| [Run long tasks](run-long-tasks.md) | your tasks take minutes or hours |
| [Recover from an incident](recover-from-an-incident.md) | a bug or an outage left many dead tasks |
| [Split work across services](split-across-services.md) | several services use taskmill |
| [One dashboard for many services](one-dashboard-for-many-services.md) | you want one admin app for all of them |
| [Use your own migration tool](use-your-migration-tool.md) | you manage the schema with goose, Atlas, or Flyway |

## Write and enqueue tasks

| I want to… | Preferred way | Read |
|---|---|---|
| Send an email only if the order was saved | `EnqueueTx` in the order's transaction | [Use your own transactions](../guides/transactions.md) |
| Enqueue thousands of tasks | `EnqueueMany`, up to 10,000 per call | [Enqueue tasks](../guides/enqueue.md) |
| Set a kind's queue and attempts in one place | `EnqueueDefaults` on the task type | [Define tasks](../guides/define-tasks.md) |
| Never enqueue the same receipt twice | A `DedupeKey` | [Avoid duplicate tasks](../guides/deduplication.md) |
| Remind a customer 2 days after an order, once | `RunAt`, and a `DedupeWindow` longer than the delay | [Avoid duplicate tasks](../guides/deduplication.md) |
| Run a report every night at 03:00 in Berlin | A `Schedule` with a `Location` | [Run tasks on a schedule](../guides/schedules.md) |
| Keep a nightly run from overlapping the next one | A `GroupKey` in `Schedule.Options` | [Run tasks on a schedule](../guides/schedules.md) |
| Publish to Kafka only if my transaction commits | The outbox | [Publish events with the outbox](../guides/outbox.md) |
| Act on Kafka messages without doing the work twice | Enqueue with the message ID as `DedupeKey` | [Consume from a broker](consume-from-a-broker.md) |
| Give work to another service in the same database | A `Client` for its schema, and `EnqueueTx` in your transaction | [Split work across services](split-across-services.md) |
| Change a payload, or rename a kind | A new kind, in three deploys | [Change a task safely](change-a-task-safely.md) |

## Run tasks

| I want to… | Preferred way | Read |
|---|---|---|
| Update a balance and complete the task exactly once | `Task.CompleteTx` in the same transaction | [Do work once](do-work-once.md) |
| Call a payment API safely when a task may run twice | An idempotency key: service, schema, and task ID | [Do work once](do-work-once.md) |
| Start the next step only if this one completes | `EnqueueTx` and `CompleteTx` in one transaction | [Do work once](do-work-once.md) |
| Respect a partner's limit of 4 calls at the same time | Its own queue with `MaxRunning: 4`, and `Snooze` on HTTP 429 | [Call a rate-limited API](call-a-rate-limited-api.md) |
| Apply one account's events in order | A `GroupKey` per account | [Keep order per key](keep-order-per-key.md) |
| Let VIP receipts go first | A higher `Priority` in the same queue | [Task order and FIFO groups](../guides/ordering.md) |
| Keep slow work from delaying fast work | A separate queue | [Workers and queues](../guides/workers-and-queues.md) |
| Do work that is cheaper in bulk | A batch handler | [Handle tasks in batches](../guides/batch-handlers.md) |
| Give up early on work that is useless when late | A lower `MaxAttempts` in `EnqueueDefaults` | [Retries and errors](../guides/retries-and-errors.md) |
| Skip a task that is no longer needed | Return `nil` | [Handle tasks](../guides/handle-tasks.md) |
| Run tasks that take 10 minutes or more | A longer `Timeout` and `DrainTimeout`; split work that takes hours | [Run long tasks](run-long-tasks.md) |

## Fix and manage tasks

| I want to… | Preferred way | Read |
|---|---|---|
| Retry 5,000 dead tasks after a fix | Pause the queue, `RetryMatching`, then resume | [Recover from an incident](recover-from-an-incident.md) |
| Unblock a halted FIFO group | Fix the cause, then retry its dead task | [Keep order per key](keep-order-per-key.md) |
| Stop some tasks, and keep a record | `Cancel` or `CancelMatching` | [Manage tasks from code](../guides/operations-api.md) |
| Stop a whole queue for a while | `PauseQueue` | [Manage tasks from code](../guides/operations-api.md) |
| Keep history small for a busy kind | `DiscardCompleted` | [Handle tasks](../guides/handle-tasks.md) |

## Deploy and operate

| I want to… | Preferred way | Read |
|---|---|---|
| Run workers in production | A worker deployment of its own, from the same image | [Deploys](../production/deploys.md) |
| Run migrations | `Migrate` at the start of every process | [Deploys](../production/deploys.md) |
| Use goose, Atlas, or Flyway | Apply `taskmill.Migrations` with your tool | [Use your own migration tool](use-your-migration-tool.md) |
| Size the connection pool | One pool: the worker's total `Concurrency`, plus 5 | [Database](../production/database.md) |
| Use PgBouncer | A direct connection in `ListenConfig` | [Connection poolers](../production/connection-poolers.md) |
| Run several services | One schema per service | [Split work across services](split-across-services.md) |
| Add the dashboard to my app | `ui.New`, behind your own login | [The dashboard](../guides/dashboard.md) |
| See every service in one admin app | A dashboard in each service; the admin app forwards a path to each | [One dashboard for many services](one-dashboard-for-many-services.md) |
| Scale workers with the load | KEDA, on available plus running tasks, or on lag | [Autoscaling](../production/autoscaling.md) |
| Probe workers in Kubernetes | `Worker.Health`, for readiness only | [Kubernetes](../production/kubernetes.md) |
| Know when something goes wrong | Alerts on lag, a missing maintainer, new dead tasks, halted groups, and Error logs | [Monitoring](../production/monitoring.md) |
| Test that my code enqueues the right task | `taskmilltest`, with a real PostgreSQL | [Test your code](../guides/testing.md) |
| Follow a task from the HTTP request that made it | The `otel` module | [Logs, traces, and metrics](../guides/observability.md) |

## See also

- [Guides](../guides/README.md): one page per feature
- [Production checklist](../production/checklist.md)
- [API on pkg.go.dev](https://pkg.go.dev/github.com/code19m/taskmill)

**Next:** [Do work once](do-work-once.md)
