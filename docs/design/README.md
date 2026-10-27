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
`net/http`. One small script of our own, under a strict content security
policy, keeps pages live, draws a chart, and adds dialogs; every page also
works without it, and nothing needs a JavaScript build step or third-party
code. Applications mount it on their own router behind their own
authentication, or run it on its own with basic authentication through the
`taskmill-ui` command. It reads only through the operations API and the
statistics the maintainer keeps. The task list's queries stop after 5
seconds, and the other pages read small tables or single rows, so it is safe
to point at a busy database, even with pages refreshing every 5 seconds.

## Out of scope

- **Workflows:** dependencies between tasks, or graphs of tasks.
- **Being a message broker.** The outbox hands messages to a broker; taskmill
  does not deliver messages between services itself.
- **Exactly-once delivery across systems.** Through a broker, delivery is at
  least once, and consumers deduplicate by message ID.
- **Users and roles in the web interface.** It offers one optional user,
  through basic authentication; applications that need more put their own
  authentication in front of it.
- **Databases other than PostgreSQL.**
- **A command-line tool for operations.** Migrations run through `Migrate`,
  or through your own tool with the SQL from `Migrations`, and operators use
  the web interface or the operations API. The `taskmill-ui` command only
  serves the web interface.
- **Integrations that need third-party modules,** such as OpenTelemetry or
  broker clients, inside the core module. They are separate Go modules
  ([ADR 0001](../adr/0001-dependency-policy.md)), built on the core's
  middleware for enqueues and attempts: the `otel` module instruments
  taskmill with OpenTelemetry ([ADR 0005](../adr/0005-opentelemetry-in-a-module-of-its-own.md)).
- **A public SQL interface.** The tables are internal, so that they can
  change. A stable SQL function to enqueue from other languages or from
  triggers can be added once users need it.
