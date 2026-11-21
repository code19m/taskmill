# Fit taskmill to your architecture

This page shows where taskmill's parts go in your system: in a monolith, a
modular monolith, services, serverless functions, several regions, or an
app with many tenants. Find your row, then follow its link.

## Which should I use?

A [schema](../concepts/glossary.md#schema) is one set of taskmill's tables,
with its own tasks, queues, and workers.

| Your architecture | Schemas | Where workers run | How work crosses a boundary | Read next |
|---|---|---|---|---|
| A monolith | One | In a worker role of the same binary | It doesn't: `EnqueueTx` in your own transaction | [Deploys](../production/deploys.md) |
| A modular monolith | One for the binary | In a worker role of the same binary. Later, a module can have workers of its own | A module enqueues another module's task with that module's task types | [Organize tasks by module](../recipes/organize-tasks-by-module.md) |
| Services that share one database | One per service | Each service runs its own | A `Client` for the other service's schema, in the same transaction | [Split work across services](../recipes/split-across-services.md) |
| A database per service | One per service | Each service runs its own | The [outbox](../concepts/glossary.md#outbox) publishes to a broker, and the [inbox](../concepts/glossary.md#inbox) takes from it | [Consume from a broker](../recipes/consume-from-a-broker.md) |
| Serverless functions | One | In a process that stays up, or in a worker with a time limit | Functions only enqueue | [Run on serverless platforms](../production/deploys.md#run-on-serverless-platforms) |
| Several regions | One, on the one primary | In the primary's region | Producers in other regions enqueue across the distance | [Run in several regions](../production/backups-and-failover.md#run-in-several-regions) |
| Many tenants | One for all tenants | As in your architecture above | Each task carries its tenant in its metadata, and a [FIFO group](../concepts/glossary.md#fifo-group) per tenant shares the workers fairly | [Share a queue fairly between customers or tenants](../recipes/share-a-queue-between-customers.md) |

The rows combine: a modular monolith can have tenants, and services can
span regions. Programs in other languages fit any row: see
[Work with other languages](../recipes/work-with-other-languages.md).

## A monolith

Build one binary with two roles: a `web` role that enqueues, and a `worker`
role that runs the handlers. Each role gets its own deployment, so workers
scale with the queue, not with web traffic. [Deploys](../production/deploys.md)
shows the binary, and [Ready to ship](../tutorial/7-ready-to-ship.md) builds
it step by step.

## A modular monolith

Keep one schema for the whole binary. Each module owns a package of task
types, with kinds and a queue named after the module, and registers its own
handlers and schedules. `main` puts them together in one worker. Because each
module has its own queue, it can later get workers of its own without a
change to its tasks. [Organize tasks by module](../recipes/organize-tasks-by-module.md)
shows the packages, the tests, and the split.

## Services that share one database

Give each service its own schema. To give work to another service, enqueue
into that service's schema in the same transaction as your data. The other
service's task types are the contract between you.
[Split work across services](../recipes/split-across-services.md) covers the
contract, versions, privileges, traces, and tests. To see every service in
one place, read [One dashboard for many services](../recipes/one-dashboard-for-many-services.md).

Sharing the database couples the services in two ways:

- **Versions.** Services that enqueue into each other's schemas must stay
  within one taskmill release of each other
  ([Compatibility](../reference/compatibility.md#old-code-on-a-newer-schema)).
- **Load and failures.** One service's busy queues slow the others' queries,
  and when the database is slow or down, so is every service that uses it.

If that coupling is too tight, give each service its own database.

## A database per service

No transaction spans two databases. So the sending service publishes events
with the [outbox](../guides/outbox.md), only if its transaction commits. The
receiving service enqueues one task per message, with the message ID as the
dedupe key: [Consume from a broker](../recipes/consume-from-a-broker.md).

## Serverless functions

A function can enqueue: it needs a small pool, created once per instance. A
function can't host a worker that runs all the time: between requests, the
platform stops or slows its CPU, and the worker can't renew its leases. Run
workers in a process that stays up with its CPU always on, or as a worker
with a time limit, such as a scheduled container.
[Run on serverless platforms](../production/deploys.md#run-on-serverless-platforms)
has the settings for each case.

## Several regions

taskmill needs one primary database that every process writes to. Run the
workers and the [maintainer](../concepts/glossary.md#maintainer) in the
primary's region. Producers in other regions can enqueue, at one round trip
per call. Databases with several primaries are not supported.
[Run in several regions](../production/backups-and-failover.md#run-in-several-regions)
says why, and what a regional failover needs.

## Many tenants

Use one schema for all tenants: a schema per tenant would need workers and
a maintainer of its own. Carry the tenant in each task's metadata, and give
each tenant a FIFO group, so that one tenant can't take every worker.
[Run tasks for many tenants](../recipes/share-a-queue-between-customers.md#run-tasks-for-many-tenants)
also shows how to erase a tenant's tasks, and how to work with a schema per
tenant for your own data.

## See also

- [Why taskmill](why-taskmill.md): what taskmill does, and what it does not do
- [How it works](../concepts/how-it-works.md): the parts, in one picture
- [Production checklist](../production/checklist.md): what to check before you go live

**Next:** [Quickstart](quickstart.md)
