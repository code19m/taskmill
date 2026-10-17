# 0002. Supported Go and PostgreSQL versions

- Status: Accepted
- Date: 2026-10-17

## Context

Supporting older versions widens the audience, but it has costs. We cannot use
the newest language, standard library, and PostgreSQL features, and the test
matrix grows. taskmill has no users yet, so there is no installed base to
protect.

## Decision

**Go.** taskmill targets the latest stable Go release. The `go` line in
[go.mod](../../go.mod) names its minor version (now `go 1.27.0`), and the
`toolchain` line pins the latest patch release for development (now
`go1.27.2`). CI tests with that version and with the newest stable Go.

**PostgreSQL.** taskmill supports every PostgreSQL major version that still
receives community support, starting with 15. PostgreSQL 14 reaches end of life
in November 2026, before taskmill's first release. A version is dropped in the
first minor release after its end of life. Once integration tests exist, CI
runs them against every supported major version.

## Consequences

- We can use modern Go and PostgreSQL features freely.
- Users on Go 1.26 or PostgreSQL 14 cannot adopt taskmill. We accept this for a
  new project.
- We will revisit the Go policy before v1.0.0. Supporting the two newest Go
  releases, which matches Go's own security support window, is the likely
  choice once there are users to protect.
