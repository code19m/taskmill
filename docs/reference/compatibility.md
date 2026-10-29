# Compatibility

What stays the same between releases, which Go and PostgreSQL versions
taskmill supports, and which releases get fixes. For the steps of an
upgrade, see [Upgrading](../production/upgrading.md).

## Status

- The latest release is **v0.3.0**.
- **The API is final for v1.0.0.** v1.0.0 is not tagged yet: it waits for
  more use in production.
- taskmill follows [Semantic Versioning](https://semver.org/). Before v1.0.0,
  it allows a minor release to break the API. If one does, the
  [changelog](../../CHANGELOG.md) marks the change with **Breaking:** and
  says how to migrate.

## What never changes within a major version

From v1.0.0 on, this is the promise:

- **Your code keeps building and working.** Code that builds against a v1
  release builds and works with every later v1 release. A breaking change
  needs a new major version, with a new import path, such as
  `github.com/code19m/taskmill/v2`. CI compares every change with the latest
  release, and fails on one that breaks the API.
- **Deprecated names stay until v2.** A replaced identifier gets a
  `// Deprecated:` comment that says what to use instead. It keeps working
  until the next major version. The changelog lists each deprecation.
- **Interfaces never grow.** The interfaces you implement keep their
  methods: `Args`, `ArgsWithDefaults`, `Handler`, `BatchHandler`,
  `RetryPolicy`, and `outbox.Publisher`. So do `PgxQuerier` and
  `SQLQuerier`, which your transactions and pools satisfy. New features come
  as new optional interfaces, the way `ArgsWithDefaults` extends `Args`.
  `Executor` is sealed: only taskmill implements it.
- **Function types keep their signatures:** `HandlerFunc`, `Middleware`,
  `EnqueueMiddleware`, and `outbox.PublisherFunc`.
- **Config structs only get new fields whose zero value keeps the old
  behavior.**
- **Names that are stored, or that your code compares, never change:**
  - the `State` values, such as `available` and `dead`, and no state is added
    within v1;
  - the error history codes, such as `timeout`
    ([list](errors.md#error-history-codes));
  - the error classes that `ErrorType()` returns
    ([list](errors.md#error-classes));
  - the outbox's queue, `outbox`, and its task kind,
    `taskmill.outbox.message`.
- **One table is public: `migrations`.** If you apply migrations with your
  own tool, you record each one in `<schema>.migrations`. It keeps its name
  and its columns: `version` (`integer`, the primary key), `name` (`text`),
  and `applied_at` (`timestamptz`, default `now()`).

## What may change in a minor release

| What | What to do |
|---|---|
| A new error code or error class | Treat one you don't know like any other failure. |
| Log messages and their attributes | Match on the [current list](log-messages.md), and check it when you upgrade. |
| Error messages | Test errors with `errors.Is`, never by their text. |
| Span, attribute, and metric names of the `otel` module | The module stays below v1 while OpenTelemetry's messaging conventions settle. Its [changelog](../../otel/CHANGELOG.md) announces each rename. |
| The Go version that taskmill needs | It moves up after each new Go release (see below). |
| Support for a PostgreSQL version past its end of life | It is dropped (see below). |
| Every table except `migrations` | The tables are internal. Read and change tasks through `Client`, never with SQL. |

## Supported Go and PostgreSQL versions

| | Supported | Tested in CI |
|---|---|---|
| Go | The two newest Go releases, as Go itself supports. The `go` line in `go.mod` names the older one, today 1.26. | Go 1.26 with every supported PostgreSQL, and the newest Go with PostgreSQL 18 |
| PostgreSQL | Every major version with community support, from 15: today 15, 16, 17, and 18 | All four |

When a new Go comes out, the next minor release of taskmill needs the Go
before it. A PostgreSQL version is dropped in the first minor release after
its end of life. CI also runs every test through PgBouncer in transaction
mode. [ADR 0002](../adr/0002-supported-versions.md) explains these choices.

## Which releases get fixes

Only the latest minor release gets fixes, security fixes included. This is
true before and after v1.0.0. A fix ships as a patch release, such as
v1.2.1 after v1.2.0.

> [!TIP]
> Stay on the latest minor release. An older one gets no fixes, not even
> security fixes.

To report a vulnerability, see [SECURITY.md](../../SECURITY.md).

## Old code on a newer schema

Migrations only move forward: there are no down migrations. Each release's
[schema](../concepts/glossary.md) works with the previous release's
clients, workers, and dashboard. So old and new versions can run side by
side during a rolling deploy.

| Your code | The schema | Result |
|---|---|---|
| The same release | The same release | Works |
| One release older | One release newer | Works. CI tests this on every change |
| Newer | Older | `NewClient` fails with `ErrSchemaOutdated`, and `taskmill-ui` exits, until you migrate |
| Two or more releases older | Newer | Not promised, and not tested |

This is a promise that CI tests, not a check in the code. `NewClient`
refuses only a schema that is older than the code. It accepts any newer
schema, so code that is two releases older starts, but nothing promises that
it works.

So upgrade one minor release at a time. If you skip a release, stop every
process of the old version before the new ones start. For the same reason, rolling back one release is safe: the
old code runs on the new schema.

## See also

- [Upgrading](../production/upgrading.md): the steps of a safe upgrade
- [Deploys](../production/deploys.md): when to run migrations
- [CHANGELOG.md](../../CHANGELOG.md): what changed in each release

**Next:** [Packages](packages.md)
