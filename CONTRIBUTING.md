# Contributing to taskmill

Thank you for helping. This guide covers how to set up the project, what we
expect from every change, and how changes are merged.

## Before you start

For anything larger than a small fix, open an issue first. Describe the problem
and the approach you have in mind. Agreeing on the design before writing code
saves time for everyone. Ask questions in
[Discussions](https://github.com/code19m/taskmill/discussions/categories/q-a),
not in issues.

## Development setup

You need Git, Make, Go, and Docker. The exact Go version comes from the
`toolchain` line in [go.mod](go.mod); with the default `GOTOOLCHAIN=auto`, any
recent Go downloads it for you. The integration tests need PostgreSQL, which
`make db` starts in Docker.

```sh
git clone https://github.com/code19m/taskmill.git
cd taskmill
make db
make check
```

`make check` runs everything CI runs. The first run installs the pinned
development tools into `./bin`. `make help` lists every target:

| Target | What it does |
|---|---|
| `make db` | Start PostgreSQL in Docker for the tests (`make db-stop` removes it) |
| `make check` | All CI checks: tidy, lint, test, vulnerability scan |
| `make fmt` | Format the code with gofumpt and goimports |
| `make lint` | Run golangci-lint and actionlint |
| `make test` | Run the tests with the race detector, in random order |
| `make cover` | Run the tests and open an HTML coverage report |
| `make vuln` | Scan for known vulnerabilities with govulncheck |
| `make bench` | Run the benchmarks and the fault scenarios, printing a Markdown report |
| `make faults` | Run the fault scenarios as the nightly CI run does, against databases of their own |

## Code standards

Every change is held to the same bar:

- **Idiomatic Go.** Follow [Effective Go](https://go.dev/doc/effective_go) and
  [Go Code Review Comments](https://go.dev/wiki/CodeReviewComments). When in
  doubt, do what the standard library does.
- **The linters pass.** CI enforces [.golangci.yml](.golangci.yml). A
  `//nolint` directive must name the linter and give a reason, for example
  `//nolint:gosec // the query text is a constant`.
- **Every exported identifier has a doc comment.** It starts with the name and
  describes behavior and guarantees, not implementation.
- **Errors carry context.** Wrap with `fmt.Errorf("...: %w", err)`. Conditions
  that callers must handle are exposed as sentinel errors (`ErrX`) or error
  types (`XError`).
- **No new dependencies without an ADR.** The `depguard` linter enforces the
  allowlist. See [ADR 0001](docs/adr/0001-dependency-policy.md).
- **Library manners.** No global state, no `init` functions, no writing to
  stdout, and no panics on bad input. Every call that can block takes a
  `context.Context`.

## Tests

- Use the standard `testing` package. Prefer table-driven tests when they make
  the cases easier to read.
- Test behavior against a real PostgreSQL: each test migrates its own schema
  through `internal/testdb`, so tests run in parallel. CI starts PostgreSQL
  with `make db`, the same as a local run, once for each supported version.
- A test that waits for the worker's internal timers, such as the
  maintainer's interval, may shorten them through `export_test.go`
  (`taskmill.SetTiming`) instead of waiting for the defaults.
- Tests must pass with `-race` and `-shuffle=on`. They must not rely on timing
  luck. To wait for a result, poll with a deadline (`waitFor`, `waitState`, `recv`);
  never sleep a fixed time and assume the result is ready. Every test also
  checks the FIFO group invariants when it ends.
- A bug fix includes a test that fails without the fix.
- `Example` functions compile with the tests. Those that need no database
  include an `// Output:` comment, so they run as tests too.

## Commit messages

We use [Conventional Commits](https://www.conventionalcommits.org/). Pull
requests are squash-merged, so the **pull request title becomes the commit
message** on `main`. CI checks the title.

```text
<type>(<optional scope>): <summary>
```

- **type** is one of `feat`, `fix`, `perf`, `refactor`, `test`, `docs`,
  `build`, `ci`, `chore`, `style`, `revert`.
- **scope** names the package or area, such as `worker`, `migrate`, or `deps`.
- **summary** is in the imperative mood, starts with a lowercase letter, has
  no trailing period, and is at most 72 characters.
- **Breaking changes** add `!` after the type or scope, such as
  `feat(worker)!: rename Run to Start`. Explain how to migrate in the pull
  request description and in CHANGELOG.md.

```text
feat(worker): drain in-flight tasks on shutdown
fix(queue): keep int64 payload values exact
docs: explain at-least-once delivery
```

## Pull requests

- Keep each pull request to one logical change. Large changes are easier to
  review as a series of small ones.
- Fill in the template: what changed, why, and how you tested it.
- `make check` must pass, and a maintainer reviews every change.
- **Update CHANGELOG.md.** A user-visible change adds one line under
  `[Unreleased]`, in the right category: Added, Changed, Deprecated, Removed,
  Fixed, or Security. A breaking change starts with **Breaking:** and says
  how to migrate.
- **Follow the [compatibility policy](docs/guide.md#upgrading).** Interfaces
  that users implement never grow, and a removal is deprecated for at least
  one minor release first.

## Schema migrations

- A schema change is a new file in `internal/migrations`. Released
  migrations never change; a test pins their checksums.
- The previous release's code must keep working on the new schema, so that
  rolling deploys work.
- New migrations must be safe to run on a busy database: set `lock_timeout`,
  and avoid statements that hold long locks on `tasks`, such as table
  rewrites.

## Design decisions

Significant decisions are recorded as Architecture Decision Records in
[docs/adr](docs/adr). If your change alters a decision, supersede its ADR in
the same pull request.

## Releasing

Maintainers release from a green `main`:

1. In CHANGELOG.md, move `[Unreleased]` to a section `## [X.Y.Z] - YYYY-MM-DD`.
   Update the links at the bottom: `[Unreleased]` compares `vX.Y.Z...HEAD`,
   and `[X.Y.Z]` points to the release.
2. Check that `internal/migrations/released_test.go` pins the checksum of
   every migration in the release.
3. Tag the release commit with an annotated tag, and push it:
   `git tag -a vX.Y.Z -m "vX.Y.Z" && git push origin vX.Y.Z`.
4. Make the Go module proxy fetch the version, so pkg.go.dev shows it:
   `GOPROXY=https://proxy.golang.org go list -m github.com/code19m/taskmill@vX.Y.Z`.
5. Create the GitHub release from that CHANGELOG section.

Never move or delete a published tag: the Go checksum database keeps every
version forever. Fix a bad release forward with a new patch release, and
mark the bad version with a `retract` directive in go.mod.
