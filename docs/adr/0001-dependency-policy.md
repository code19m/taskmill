# 0001. Dependency policy

- Status: Accepted
- Date: 2026-10-17

## Context

taskmill is a library, so every module it requires becomes part of its users'
builds. Each one adds code to audit, supply-chain risk, possible version
conflicts, and upgrades to track. Enterprise users often review every new
dependency before they adopt a library.

The Go standard library covers most of what a task queue needs:
`database/sql`, `log/slog`, `context`, `encoding/json`, `time`, `sync`, and
`testing`.

## Decision

1. **Standard library first.** A third-party module is added only when the
   standard library cannot do the job well, and an ADR records why.
2. **The allowlist is enforced.** The `depguard` linter runs in strict mode and
   allows only the standard library and this module ([ADR 0004](0004-pgx-as-the-postgresql-driver.md)
   later added pgx). Adding a dependency
   extends the allowlist in [.golangci.yml](../../.golangci.yml) in the same
   pull request as its ADR.
3. **Heavy integrations live in separate modules.** If we add an
   integration that needs a third-party module, such as OpenTelemetry, it
   gets its own Go module. Users who do not need it never download it. The
   core module offers plain extension points, such as middleware, that such
   modules build on.
4. **Tests use only the standard library.** No assertion frameworks and no
   container libraries. Integration tests connect to a PostgreSQL server named
   by an environment variable, and CI provides one for each supported version.
5. **Development tools stay out of go.mod.** Linters and scanners are pinned
   in the [Makefile](../../Makefile) and installed into `./bin`. We do not use
   go.mod `tool` directives, because their requirements become part of every
   user's module graph and can raise the versions of modules users share with
   them.

We expect the core to need exactly one dependency: a PostgreSQL driver. That
choice gets its own ADR.

## Consequences

- Users get a small module graph that is quick to audit.
- We write and maintain some small helpers that a dependency would otherwise
  provide. We accept this cost while those helpers stay small and well tested.
- Dependabot does not update the development tools; they are bumped by hand in
  the Makefile.
