# Design

These documents describe how taskmill works and what it promises. A change
that alters a documented behavior updates the document in the same pull
request.

| Document | Covers |
|---|---|
| [Task lifecycle and guarantees](task-lifecycle.md) | Task states, transitions, and the promises taskmill makes: the contract |
| [Storage schema](schema.md) | Tables, indexes, and the reasoning behind the SQL |
| [Benchmarks](../benchmarks.md) | Performance targets, reference hardware, and measured results |
| [Decisions](../adr) | Why taskmill is built the way it is |

## Product shape

taskmill is one engine with focused packages on top of it, all in one Go
module, as the [README](../../README.md#documentation) lists. The packages
use only the engine's public API, plus small internal helpers such as UUID
generation. [ADR 0003](../adr/0003-outbox-on-the-shared-engine.md) explains
why the outbox is a package and not a separate project.

The web interface is server-rendered with `html/template`, `embed`, and
`net/http`, so it needs no JavaScript build step. Applications mount it on
their own router behind their own authentication. It reads only through the
operations API and the statistics the maintainer keeps. The task list's
query stops after 5 seconds, and the other pages read small tables or single
rows, so it is safe to point at a busy database.

## Out of scope

- **Workflows:** dependencies between tasks, or graphs of tasks.
- **Being a message broker.** The outbox hands messages to a broker; taskmill
  does not deliver messages between services itself.
- **Exactly-once delivery across systems.** Through a broker, delivery is at
  least once, and consumers deduplicate by message ID.
- **Authentication in the web interface.** Applications provide it.
- **Databases other than PostgreSQL.**
- **A command-line tool.** Migrations run through `Migrate`, or through your
  own tool with the SQL from `Migrations`.
- **Integrations that need third-party modules,** such as OpenTelemetry or
  broker clients, inside the core module. Middleware and
  `ClientConfig.ContextMetadata` cover tracing and metrics, and the guide
  shows how. Such integrations would be separate Go modules
  ([ADR 0001](../adr/0001-dependency-policy.md)).
- **A public SQL interface.** The tables are internal, so that they can
  change. A stable SQL function to enqueue from other languages or from
  triggers can be added once users need it.
- **Bulk actions, for now.** Retrying, canceling, or deleting many tasks at
  once waits for a later release. Until then, page with `List` and act on
  each task; the [guide](../guide.md#running-in-production) shows how.
