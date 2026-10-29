# Guides

The guides show how to build with taskmill, one feature per page. New to
taskmill? Follow the [learning path](../README.md#learning-path) first: it
takes you through the main guides in order.

## Which guide do I need?

The guides are in reading order.

| I want to… | Read |
|---|---|
| Create taskmill's tables, and connect to them | [Set up the database and client](setup.md) |
| Write a task type, and choose what goes in it | [Define tasks](define-tasks.md) |
| Run a task now, later, or before others | [Enqueue tasks](enqueue.md) |
| Send an email only if the order was saved | [Use your own transactions](transactions.md) |
| Write the code that does the work | [Handle tasks](handle-tasks.md) |
| Know how often a failing task is retried, and what a dead task is | [Retries and errors](retries-and-errors.md) |
| Do work that is cheaper in bulk, such as a bulk insert | [Handle tasks in batches](batch-handlers.md) |
| Run workers, limit how many tasks run, or pause a queue | [Workers and queues](workers-and-queues.md) |
| Run one account's tasks one at a time, in order | [Task order and FIFO groups](ordering.md) |
| Never enqueue the same task twice | [Avoid duplicate tasks](deduplication.md) |
| Run a report every night at 03:00 | [Run tasks on a schedule](schedules.md) |
| Publish events to Kafka, NATS, or RabbitMQ only after a commit | [Publish events with the outbox](outbox.md) |
| See and fix tasks in a browser | [The dashboard](dashboard.md) |
| Retry, cancel, or delete thousands of tasks from code | [Manage tasks from code](operations-api.md) |
| Get logs, traces, metrics, and queue depth | [Logs, traces, and metrics](observability.md) |
| Test code that enqueues and handles tasks | [Test your code](testing.md) |

## See also

- [How it works](../concepts/how-it-works.md): the big picture behind the guides
- [How do I…?](../recipes/README.md): the best way for common situations
- [API reference](https://pkg.go.dev/github.com/code19m/taskmill) on pkg.go.dev

**Next:** [Set up the database and client](setup.md)
