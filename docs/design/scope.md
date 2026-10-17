# Scope and use cases

- Status: Draft
- Milestone: M1

This document defines what taskmill is for, which architectures it must serve,
and the requirements that follow from them. The storage schema and the public
API must satisfy every requirement here from the start, so that later features
do not force a redesign.

## Product shape

taskmill is one engine with focused packages on top of it, all in this
repository.

| Import path | What it is | Who uses it |
|---|---|---|
| `github.com/code19m/taskmill` | The engine: client, worker, maintainer, migrations, and the inspect API | Everyone |
| `github.com/code19m/taskmill/outbox` | Transactional outbox and inbox, in terms of topics, keys, and messages | Services that publish to a message broker |
| `github.com/code19m/taskmill/ui` | A web interface, served as an `http.Handler` | Operators |
| `github.com/code19m/taskmill/cmd/taskmill` | A command-line tool and a standalone web interface server | Operators and deployment pipelines |

The packages use only the engine's public API. Integrations that need
third-party modules, such as OpenTelemetry or broker clients, are separate Go
modules ([ADR 0001](../adr/0001-dependency-policy.md)).
[ADR 0003](../adr/0003-outbox-on-the-shared-engine.md) explains why the outbox
is a package and not a separate project.

## Use cases

### U1. Background tasks in a monolith

An application enqueues work, such as emails, reports, or calls to external
APIs, inside its own transactions. Its workers run in the same process, or in
a separate deployment of the same binary.

### U2. Modular monolith

Several modules share one binary and one database. Each module registers its
own task kinds and owns its own queues, without knowing about the others. Kind
names are namespaced, for example `billing.invoice.send`.

### U3. Microservices with a database per service

Each service runs taskmill in its own database. Services never enqueue into
each other's databases: that would couple them, and the enqueue could not be
part of the producer's transaction. Services communicate through a message
broker instead:

```text
 Service A (one transaction)        Relay           Broker       Service B
 business write + outbox message ─▶ publishes ─▶ topic ─▶ consumer enqueues ─▶ handler
                                                          (deduplicated by message ID)
```

The outbox makes publishing as reliable as the business write. The inbox on
the receiving side turns each broker message into exactly one task.

### U4. Shared database cluster and polyglot producers

Several services, possibly written in different languages, share one
PostgreSQL cluster. Each service gets its own taskmill schema, or producers
enqueue into a shared queue database whose workers are the only consumers.

### U5. Scheduled and recurring work

Tasks that run at a given time, or on a cron schedule, exactly once per
scheduled time across all processes.

### U6. Operations

Operators inspect queues, find failing tasks, read their errors, and retry,
cancel, or delete them, from the web interface, the command-line tool, or
their own code.

## Requirements

The schema designed in M1 must support every requirement, including those
delivered in later milestones.

| ID | Requirement | Use cases | Milestone |
|---|---|---|---|
| R1 | The schema name is configurable, so several independent installations can share one database. | U2, U4 | M2 |
| R2 | Producers and consumers are independent. The client enqueues by kind name and payload, without knowing any handler. The kind name and the JSON payload are the contract between them. | U2, U3, U4 | M2 |
| R3 | No global state. One process can run several clients and workers. | U2 | M2 |
| R4 | Queues route work and set capacity: each deployment chooses the queues it serves. Workers do not filter by kind when they fetch. | All | M2 |
| R5 | A task whose kind has no handler in the worker that fetched it waits without using attempts, up to a time limit, and then becomes dead. This keeps tasks safe during rolling deploys. | U2, U3 | M3 |
| R6 | FIFO groups: tasks with the same group key run one at a time, in enqueue order, for millions of distinct keys. A queue can be set to halt a group when one of the group's tasks becomes dead. | U3 | M4 |
| R7 | Batch handlers: one handler call receives up to N tasks and reports an outcome for each one. | U3 | M4 |
| R8 | Concurrency limits: a cap on how many tasks of a queue run at once, across all workers. | U1, U3 | M4 |
| R9 | Deduplicated enqueue: a key admits at most one task within a time window, even when enqueued concurrently from several processes. | U3, U5 | M4 |
| R10 | Completion policy per queue: keep completed tasks in history for the retention period, or delete them as soon as they complete. | U1, U3 | M2 |
| R11 | Every task has a stable identity that handlers can pass on, for example as a broker message ID. | U3 | M2 |
| R12 | Enqueue accepts the caller's transaction, from pgx or `database/sql`. Handlers can opt in to completing their task inside their own transaction. | All | M2, M3 |
| R13 | Every attempt's error is kept with the task, up to a fixed number of attempts. | U6 | M2 |
| R14 | Recurring schedules enqueue exactly one task per scheduled time across all processes. | U5 | M5 |
| R15 | Cheap monitoring: registries of queues and live workers, and statistics refreshed by the maintainer, so that no request counts the rows of a large table. | U6 | M5 |
| R16 | An inspect API shared by the web interface, the command-line tool, and users: list and filter tasks, read one task, and retry, cancel, or delete tasks in bulk. Lists are paginated by key, never by offset, and bulk actions run in small batches. | U6 | M6 |
| R17 | Queues can be paused and resumed. Workers stop fetching from a paused queue promptly, and tasks already running finish normally. | U6 | M6 |

## The web interface

The web interface must be cheap to build and maintain, and safe to point at a
busy production database.

- **Server-rendered with the standard library:** `html/template`, `embed`, and
  `net/http`. There is no JavaScript build step and no Node.js toolchain.
- **An `http.Handler`,** so applications mount it on their own router and
  protect it with their own authentication middleware. A standalone server in
  `cmd/taskmill` serves the same handler for teams that prefer a separate
  deployment.
- **A read-only mode,** and protection against cross-site request forgery from
  `net/http`.
- **Never expensive.** It reads only through the inspect API (R16) and the
  maintained statistics (R15), so every page is a bounded query.

## Out of scope for v1

- **Workflows:** dependencies between tasks, or graphs of tasks.
- **Being a message broker.** The outbox hands messages to a broker; taskmill
  does not deliver messages between services itself.
- **Authentication in the web interface.** Applications provide it, as above.
- **Databases other than PostgreSQL.**
- **Exactly-once delivery across systems.** Through a broker, delivery is
  at-least-once, and consumers deduplicate by message ID (R9, R11).

## Decisions

1. **No public SQL enqueue function in v1.** A stable `enqueue` function in the
   taskmill schema would let services written in other languages, and database
   triggers, enqueue tasks (U4), but it would also freeze part of the schema as
   a public interface. The schema keeps it possible, and it can be added once
   users need it. Decided in the [API document](api.md#decisions).
