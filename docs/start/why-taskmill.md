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
  [dashboard](../concepts/glossary.md#dashboard), with one dependency: pgx.

## Which should I use?

| Situation | What to use | Why |
|---|---|---|
| Short work that you can lose if the process stops, such as warming a cache | A goroutine | Nothing to set up. But a crash loses the work, nothing retries it, and you cannot see it. |
| Background work for a Go service that uses PostgreSQL | **taskmill** | Tasks commit with your data, survive crashes, and retry by themselves. |
| You need workflows (tasks that wait for other tasks), a command-line tool, or a long production record | [River](https://riverqueue.com) | River has these, and taskmill does not. |
| Your app does not use PostgreSQL | A queue for the database you use, or a Redis-based queue | taskmill runs on PostgreSQL only. |
| Your services exchange many events through Kafka, NATS, or RabbitMQ | Your broker, with taskmill's outbox to publish to it | The outbox publishes only what your transaction committed. |

We recommend taskmill when your service already runs on PostgreSQL and its
background work must not get lost.

## taskmill and River

taskmill is closest to [River](https://riverqueue.com). Both keep tasks in
PostgreSQL, use pgx, and enqueue in your own transaction. taskmill also
includes [FIFO groups](../concepts/glossary.md#fifo-group), limits across all
workers, batch handlers, an outbox, and a dashboard, in one MIT-licensed
module. River offers several of these only in its paid River Pro. Choose
River if you need workflows, a command-line tool, or a long production
record.

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

- **Workflows.** Tasks do not wait for each other. A handler can enqueue the
  next task in its own transaction, which covers simple chains.
- **Deliver messages between services.** The outbox hands messages to your
  broker; it is not a broker.
- **Exactly-once delivery across systems.** Through a broker, each message
  arrives at least once, and consumers drop duplicates by message ID.
- **Other databases.** taskmill runs on PostgreSQL only.
- **Users and roles in the dashboard.** It has one optional user with a
  password. For more, put your own login in front of it.
- **A command-line tool.** You run migrations with `Migrate`, and manage
  tasks in the dashboard or from Go code.
- **A public SQL interface.** taskmill's tables may change in any release.
  Read and change tasks through the `Client`, not with SQL.

## What you need

- **Go 1.26** or newer.
- **PostgreSQL 15** or newer. Managed PostgreSQL works, and so do
  connection poolers such as PgBouncer, with a few settings
  ([Database](../production/database.md),
  [Connection poolers](../production/connection-poolers.md)).
- **pgx**, the PostgreSQL driver. If your app uses `database/sql`, it must
  use pgx's driver, `github.com/jackc/pgx/v5/stdlib`. lib/pq does not work.

## Is it stable?

[Compatibility](../reference/compatibility.md) says what each release
promises, and which Go and PostgreSQL versions we support.

## See also

- [How it works](../concepts/how-it-works.md): the parts, in one picture
- [Performance](../reference/performance.md): how fast, and how light
- [Shop example](../../examples/shop/README.md): most features in one small program

**Next:** [Quickstart](quickstart.md)
