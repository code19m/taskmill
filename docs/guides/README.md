# Guides

The guides show how to build with taskmill, one feature per page. Pick the
one you need below. New to taskmill? Follow the
[learning path](../README.md#learning-path) instead: it takes you through the
main guides in order.

## Which guide do I need?

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

## All guides, in order

| Guide | Read this when… |
|---|---|
| [Set up the database and client](setup.md) | You add taskmill to an app. |
| [Define tasks](define-tasks.md) | You write your first task type. |
| [Enqueue tasks](enqueue.md) | You start background work. |
| [Use your own transactions](transactions.md) | A task must exist only if your data was saved. |
| [Handle tasks](handle-tasks.md) | You write a handler. |
| [Retries and errors](retries-and-errors.md) | Tasks fail, or you want to change how they retry. |
| [Handle tasks in batches](batch-handlers.md) | One call for many tasks is much cheaper than many calls. |
| [Workers and queues](workers-and-queues.md) | You run workers, or choose your queues. |
| [Task order and FIFO groups](ordering.md) | Order matters, or some tasks are urgent. |
| [Avoid duplicate tasks](deduplication.md) | The same task can be enqueued twice. |
| [Run tasks on a schedule](schedules.md) | Work must happen on a timetable. |
| [Publish events with the outbox](outbox.md) | Other systems must learn what happened. |
| [The dashboard](dashboard.md) | You want to see and fix tasks in a browser. |
| [Manage tasks from code](operations-api.md) | Your code or tools must act on tasks. |
| [Logs, traces, and metrics](observability.md) | You connect taskmill to your monitoring. |
| [Test your code](testing.md) | You write tests. |

## See also

- [How it works](../concepts/how-it-works.md): the big picture behind the guides
- [How do I…?](../recipes/README.md): the best way for common situations
- [API reference](https://pkg.go.dev/github.com/code19m/taskmill) on pkg.go.dev

**Next:** [Set up the database and client](setup.md)
