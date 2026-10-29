# Documentation

taskmill runs background tasks for Go from your PostgreSQL database. Tasks
commit with your data, run at least once, and survive a crashed worker.

## Where do you want to start?

| I want to… | Go to |
|---|---|
| Decide if taskmill fits my project | [Why taskmill](start/why-taskmill.md) |
| Run my first task in 15 minutes | [Quickstart](start/quickstart.md) |
| Understand how it works | [How it works](concepts/how-it-works.md) |
| Use a feature: retries, schedules, the outbox… | [Guides](guides/README.md) |
| Find the best way to do something | [How do I…?](recipes/README.md) |
| Go to production | [Production checklist](production/checklist.md) |
| Fix a problem | [Troubleshooting](production/troubleshooting.md) |
| Look up a default, an error, a log line, or a metric | [Reference](reference/README.md) |
| See every function and type | [API on pkg.go.dev](https://pkg.go.dev/github.com/code19m/taskmill) |
| Contribute | [CONTRIBUTING.md](../CONTRIBUTING.md) and [Design](design/README.md) |

## Learning path

New to taskmill? Read these pages in order. Together they take about an
hour, from "what is this?" to "ready for production".

| Step | Page | Time |
|---|---|---|
| 1 | [Why taskmill](start/why-taskmill.md) | 5 min |
| 2 | [Quickstart](start/quickstart.md) | 15 min |
| 3 | [How it works](concepts/how-it-works.md) | 5 min |
| 4 | [Guarantees](concepts/guarantees.md): your handler may run twice | 5 min |
| 5 | [Define tasks](guides/define-tasks.md) | 4 min |
| 6 | [Enqueue tasks](guides/enqueue.md) | 5 min |
| 7 | [Use your own transactions](guides/transactions.md) | 5 min |
| 8 | [Handle tasks](guides/handle-tasks.md) | 6 min |
| 9 | [Workers and queues](guides/workers-and-queues.md) | 8 min |
| 10 | [Test your code](guides/testing.md) | 5 min |
| 11 | [Production checklist](production/checklist.md) | 3 min |

## All pages

### Get started

- [Why taskmill](start/why-taskmill.md)
- [Quickstart](start/quickstart.md)

### Understand

- [How it works](concepts/how-it-works.md)
- [Task lifecycle](concepts/task-lifecycle.md)
- [Guarantees](concepts/guarantees.md)
- [Glossary](concepts/glossary.md)

### Build with taskmill

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

### How do I…?

- [Do work exactly once](recipes/do-work-once.md)
- [Call a rate-limited API](recipes/call-a-rate-limited-api.md)
- [Keep order per key](recipes/keep-order-per-key.md)
- [Consume from a broker](recipes/consume-from-a-broker.md)
- [Change a task safely](recipes/change-a-task-safely.md)
- [Run long tasks](recipes/run-long-tasks.md)
- [Recover from an incident](recipes/recover-from-an-incident.md)
- [Split taskmill across services](recipes/split-across-services.md)
- [One dashboard for many services](recipes/one-dashboard-for-many-services.md)
- [Use your own migration tool](recipes/use-your-migration-tool.md)

### Run in production

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

### Look it up

- [Defaults and limits](reference/defaults-and-limits.md)
- [Errors](reference/errors.md)
- [Log messages](reference/log-messages.md)
- [Metrics](reference/metrics.md)
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

**Next:** [Why taskmill](start/why-taskmill.md)
