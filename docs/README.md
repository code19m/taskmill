# Documentation

taskmill runs background tasks for Go from your PostgreSQL database. Tasks
commit with your data, run at least once, and survive a crashed worker.

## Where do you want to start?

| I want to… | Go to |
|---|---|
| Decide if taskmill fits my project | [Why taskmill](start/why-taskmill.md) |
| Run my first task in 15 minutes | [Quickstart](start/quickstart.md) |
| Learn by building a small shop, step by step | [Tutorial](tutorial/README.md) |
| Understand how it works | [How it works](concepts/how-it-works.md) |
| Use a feature: retries, schedules, the outbox… | [Guides](guides/README.md) |
| Find the best way to do something | [How do I…?](recipes/README.md) |
| Go to production | [Production checklist](production/checklist.md) |
| Fix a problem | [Troubleshooting](production/troubleshooting.md) |
| Look up a default, an error, a log line, or a metric | [Reference](reference/README.md) |
| See every function and type | [API on pkg.go.dev](https://pkg.go.dev/github.com/code19m/taskmill) |
| Contribute | [CONTRIBUTING.md](../CONTRIBUTING.md) and [Design](design/README.md) |

## Learning path

New to taskmill? Follow these steps in order. First you run a finished
program, then you build it yourself, and then you read why it works. After
that, open a [guide](guides/README.md) when you need a feature.

| Step | Page | What you do | Time |
|---|---|---|---|
| 1 | [Why taskmill](start/why-taskmill.md) | Decide if it fits | 5 min |
| 2 | [Quickstart](start/quickstart.md) | Run a finished shop, and watch it in the dashboard | 15 min |
| 3 | [Tutorial](tutorial/README.md) | Build that shop yourself, in seven parts of 10 to 15 minutes | 85 min |
| 4 | [How it works](concepts/how-it-works.md) | See the parts in one picture | 5 min |
| 5 | [Guarantees](concepts/guarantees.md) | Learn why your handler may run twice | 5 min |
| 6 | [Production checklist](production/checklist.md) | Check your app before it goes live | 3 min |

## All pages

### [Get started](start/README.md)

- [Why taskmill](start/why-taskmill.md)
- [Quickstart](start/quickstart.md)

### [Tutorial](tutorial/README.md)

- [Part 1: Your first task](tutorial/1-first-task.md)
- [Part 2: Tasks with your data](tutorial/2-tasks-with-your-data.md)
- [Part 3: When a task fails](tutorial/3-when-a-task-fails.md)
- [Part 4: Queues, delays, and schedules](tutorial/4-queues-delays-schedules.md)
- [Part 5: The outbox](tutorial/5-outbox.md)
- [Part 6: Test it](tutorial/6-test-it.md)
- [Part 7: Ready to ship](tutorial/7-ready-to-ship.md)

### [Concepts](concepts/README.md)

- [How it works](concepts/how-it-works.md)
- [Task lifecycle](concepts/task-lifecycle.md)
- [Guarantees](concepts/guarantees.md)
- [Glossary](concepts/glossary.md)

### [Guides](guides/README.md)

- [Set up the database and client](guides/setup.md)
- [Define tasks](guides/define-tasks.md)
- [Enqueue tasks](guides/enqueue.md)
- [Use your own transactions](guides/transactions.md)
- [Handle tasks](guides/handle-tasks.md)
- [Retries and errors](guides/retries-and-errors.md)
- [Handle tasks in batches](guides/batch-handlers.md)
- [Workers and queues](guides/workers-and-queues.md)
- [Task order and FIFO groups](guides/ordering.md)
- [Avoid duplicate tasks](guides/deduplication.md)
- [Run tasks on a schedule](guides/schedules.md)
- [Publish events with the outbox](guides/outbox.md)
- [The dashboard](guides/dashboard.md)
- [Manage tasks from code](guides/operations-api.md)
- [Logs, traces, and metrics](guides/observability.md)
- [Test your code](guides/testing.md)

### [How do I…?](recipes/README.md)

- [Do work once](recipes/do-work-once.md)
- [Call a rate-limited API](recipes/call-a-rate-limited-api.md)
- [Keep order per key](recipes/keep-order-per-key.md)
- [Consume from a broker](recipes/consume-from-a-broker.md)
- [Change a task safely](recipes/change-a-task-safely.md)
- [Run long tasks](recipes/run-long-tasks.md)
- [Recover from an incident](recipes/recover-from-an-incident.md)
- [Split work across services](recipes/split-across-services.md)
- [One dashboard for many services](recipes/one-dashboard-for-many-services.md)
- [Use your own migration tool](recipes/use-your-migration-tool.md)

### [Run in production](production/README.md)

- [Production checklist](production/checklist.md)
- [Database](production/database.md)
- [Connection poolers](production/connection-poolers.md)
- [Deploys](production/deploys.md)
- [Kubernetes](production/kubernetes.md)
- [Autoscaling](production/autoscaling.md)
- [Monitoring](production/monitoring.md)
- [Security](production/security.md)
- [Troubleshooting](production/troubleshooting.md)
- [Upgrading](production/upgrading.md)

### [Reference](reference/README.md)

- [Configuration](reference/configuration.md)
- [Defaults and limits](reference/defaults-and-limits.md)
- [The `taskmill-ui` command](reference/taskmill-ui.md)
- [Errors](reference/errors.md)
- [Log messages](reference/log-messages.md)
- [Metrics and traces](reference/metrics.md)
- [Compatibility](reference/compatibility.md)
- [Packages](reference/packages.md)
- [Performance](reference/performance.md)

### Contribute

- [CONTRIBUTING.md](../CONTRIBUTING.md): setup, tests, and pull requests
- [Design](design/README.md): how taskmill is built, for contributors
- [Decisions](adr/README.md): why taskmill is built this way

## See also

- [API reference](https://pkg.go.dev/github.com/code19m/taskmill) on pkg.go.dev
- [Shop example](../examples/shop/README.md): most features in one small program
- [Changelog](../CHANGELOG.md): what changed in each release

**Next:** [Get started](start/README.md)
