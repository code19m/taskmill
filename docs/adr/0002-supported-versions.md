# 0002. Supported Go and PostgreSQL versions

- Status: Accepted
- Date: 2026-10-17

## Context

Supporting older versions widens the audience, but it has costs. We cannot use
the newest language, standard library, and PostgreSQL features, and the test
matrix grows. taskmill has no users yet, so there is no installed base to
protect.

## Decision

**Go.** taskmill supports the two newest Go releases, matching Go's own
support policy. The `go` line in [go.mod](../../go.mod) names the older one
(now `go 1.26.0`), and the `toolchain` line pins the patch release used for
development. CI tests with the oldest supported Go and with the newest stable
Go.

**PostgreSQL.** taskmill supports every PostgreSQL major version that still
receives community support, starting with 15. PostgreSQL 14 reaches end of life
in November 2026, weeks after taskmill's first release; rather than support it
for those few weeks, taskmill starts with 15. A version is dropped in the
first minor release after its end of life. CI runs the integration tests
against every supported major version: today PostgreSQL 15, 16, 17, and 18.

## Consequences

- We can use modern Go and PostgreSQL features freely.
- Users on Go 1.25 or PostgreSQL 14 cannot adopt taskmill. We accept this for a
  new project.
- When a new Go release comes out, the `go` line moves up in the next minor
  release of taskmill.
