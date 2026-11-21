# Why taskmill

This page helps you decide if taskmill fits your project: what you gain,
what it costs, and what it does not do.

## What taskmill does

taskmill runs background tasks for Go programs, such as "email the receipt
for order 42". It stores each [task](../concepts/glossary.md#task) as a row
in your PostgreSQL database, next to your own data. Your program adds tasks,
and [workers](../concepts/glossary.md#worker) in your program run them.

That gives you three things:

- **A task commits with your data.** Save an order and its "send the
  receipt" task in one transaction. If the order rolls back, the task is
  gone too, so no customer gets a receipt for an order that does not exist.
- **One less system.** There is no Redis or broker to run, secure, and back
  up. Your PostgreSQL backups and failover cover your tasks too.
- **Everything in one module.** Retries, schedules, ordering, limits, an
  [outbox](../concepts/glossary.md#outbox), and a
  [dashboard](../concepts/glossary.md#dashboard), with one direct
  dependency: pgx.

## Which should I use?

| Situation | What to use | Why |
|---|---|---|
| Short work that you can lose if the process stops, such as warming a cache | A goroutine | Nothing to set up. But a crash loses the work, nothing retries it, and you cannot see it. |
| Background work for a Go service that uses PostgreSQL | **taskmill** | Tasks commit with your data, survive crashes, and retry by themselves. |
| You need workflows (tasks that wait for other tasks), a command-line tool, or many years of use in production | [River](https://riverqueue.com) | River has these, and taskmill does not. |
| Your app does not use PostgreSQL | A queue for the database you use, or a Redis-based queue | taskmill runs on PostgreSQL only. |
| Your services exchange many events through Kafka, NATS, or RabbitMQ | Your broker, with taskmill's outbox to publish to it | The outbox publishes only what your transaction committed. |

We recommend taskmill when your service already runs on PostgreSQL and its
background work must not get lost. To see where its parts go in a monolith,
modules, services, serverless functions, regions, or an app with tenants,
read [Fit taskmill to your architecture](architectures.md).

## taskmill and River

taskmill is closest to [River](https://riverqueue.com). Both keep tasks in
PostgreSQL, use pgx, and enqueue in your own transaction. taskmill also
includes [FIFO groups](../concepts/glossary.md#fifo-group), limits across all
workers, batch handlers, an outbox, and a dashboard, in one MIT-licensed
module. River offers several of these only in its paid River Pro. Choose
River if you need workflows, a command-line tool, or many years of use in
production.

## taskmill and other tools

Each of these fits some projects better than taskmill:

| Tool | What it is | It fits better when | What it costs you |
|---|---|---|---|
| [Asynq](https://github.com/hibiken/asynq) | A task queue for Go that keeps tasks in Redis | Your data is not in PostgreSQL, or you already run Redis and no task must commit with your data | An enqueue can't join your PostgreSQL transaction: a task enqueued before the commit outlives a rollback, and one enqueued after it is lost if the process crashes in between. What a crash of Redis loses depends on its persistence settings |
| [Temporal](https://temporal.io) | Durable workflows as a service: workflow code whose steps survive crashes, run by a Temporal cluster or the paid Temporal Cloud | Long processes with many steps, timers, and waits for outside events, across services and languages | A cluster to run, with a database of its own, or a cloud bill. Workflow code must be deterministic. Starting a workflow can't join your transaction |
| [pgmq](https://github.com/pgmq/pgmq) | A message queue in PostgreSQL, like Amazon SQS, used through SQL functions | Programs in many languages share queues through SQL, and plain messages are enough | It stores and hands out messages. The workers, retries with backoff, cron schedules, and a dashboard are yours to build |
| PostgreSQL with a broker: Kafka, NATS, or RabbitMQ | Your data in PostgreSQL, and events in a system made for streams | Many services consume the same events, at high rates, or replay them | One more system to run, secure, and watch. A publish can't join your transaction: taskmill's outbox fills that gap |

When you outgrow taskmill, [Capacity planning](../production/capacity.md#know-when-you-outgrow-taskmill)
lists the signs, and how to move to a broker one kind at a time.

## What does it cost?

- **Work for your database.** Each task is a few row writes. Idle workers
  cost the database little.
- **A speed limit.** On a laptop, one worker runs about 30,000 small tasks a
  second. That is plenty for most apps, but less than a broker that is made
  for streams of events. [Performance](../reference/performance.md) has the
  numbers, and their limits.
- **At least once, not exactly once.** A task can run twice, so your
  handlers must be safe to run twice. [Guarantees](../concepts/guarantees.md)
  shows how.

## What taskmill does not do

Each row says what to do instead:

| Not provided | Do this instead |
|---|---|
| Workflows: tasks that wait for other tasks | A handler enqueues the next task in its own transaction, which covers simple chains |
| A broker between services | The outbox hands messages to your broker, once your transaction commits |
| Exactly-once delivery across systems | Through a broker, each message arrives at least once: consumers drop duplicates by message ID |
| Other databases | None: taskmill runs on PostgreSQL only |
| Users and roles in the dashboard | It has one optional user with a password. For more, put your own login in front of it |
| A command-line tool | Run migrations with `Migrate`. Manage tasks in the dashboard or from Go code |
| A public SQL interface, such as a SQL function to enqueue | Read and change tasks through the `Client`: the tables may change in any release |
| Handlers in other languages | Handlers are Go. Other languages enqueue through your Go service: [Work with other languages](../recipes/work-with-other-languages.md) |
| An enqueue by kind name, with a raw payload | Declare the task's Go type, or a copy of it. For callers outside Go, map each allowed kind to its type |
| A worker that takes only some kinds of a queue | A worker reads whole queues: give those kinds a queue of their own |
| Priorities across queues or services | A priority orders the tasks of one queue. Give urgent work a queue of its own |
| A limit of running tasks per tenant | A few [FIFO groups](../concepts/glossary.md#fifo-group) per tenant: [Share a queue fairly between customers or tenants](../recipes/share-a-queue-between-customers.md) |
| Schedules of several services in one schema | A schema per service: [Split work across services](../recipes/split-across-services.md) |
| A worker that stops when the queue is empty | `Run` with a deadline: [Run on serverless platforms](../production/deploys.md#run-on-serverless-platforms) |
| The schema as an attribute of the `otel` metrics | Tell services apart by the label of each service in your metrics, such as Prometheus's `job` |

## What you need

- **Go 1.26** or newer.
- **PostgreSQL 15** or newer. Managed PostgreSQL works, and so do
  connection poolers such as PgBouncer, with a few settings
  ([Database](../production/database.md),
  [Connection poolers](../production/connection-poolers.md)).
- **pgx**, the PostgreSQL driver. If your app uses `database/sql`, it must
  use pgx's driver, `github.com/jackc/pgx/v5/stdlib`. lib/pq does not work.

## Will the API change?

It may: taskmill is before v1.0.0, so a minor release may break the API. The
changelog marks each such change with **Breaking:** and says how to migrate.
[Compatibility](../reference/compatibility.md) says what each release
promises, and which Go and PostgreSQL versions we support.

## See also

- [How it works](../concepts/how-it-works.md): the parts, in one picture
- [Performance](../reference/performance.md): how fast, and how light
- [Shop example](../../examples/shop/README.md): most features in one small program

**Next:** [Fit taskmill to your architecture](architectures.md)
