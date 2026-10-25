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

`make check` runs the checks that CI runs; see [Checks](#checks). The first
run installs the pinned development tools into `./bin`. `make help` lists
every target:

| Target | What it does |
|---|---|
| `make db` | Start PostgreSQL in Docker for the tests (`make db-stop` removes it) |
| `make check` | The CI checks: tidy, lint, test, vulnerability scan, rolling deploy |
| `make fmt` | Format the code with gofumpt and goimports |
| `make lint` | Run golangci-lint and actionlint |
| `make test` | Run the tests with the race detector, in random order, and write `coverage.out` |
| `make cover` | Run the tests and open an HTML coverage report |
| `make cover-report` | Print the coverage of the last `make test` per package |
| `make vuln` | Scan for known vulnerabilities with govulncheck |
| `make api` | Compare the API of the last commit with the latest release |
| `make compat` | Run the latest release's client and worker on the current schema |
| `make fuzz` | Run each fuzz target for `FUZZTIME`, 10 seconds by default |
| `make bench` | Run the benchmarks and the chaos test, printing a Markdown report |

`make check` leaves out `make api`: gorelease compares committed code, so it
stops on uncommitted changes. Commit, then run it.

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
- Input from outside, such as the dashboard's query parameters, list
  cursors, and task payloads, has a fuzz target. Every test run replays its
  seeds; add the input of each bug a fuzzer finds as a seed.

## Checks

A pull request merges only when CI passes. Each check but CodeQL is a
Makefile target, so you can run it locally:

| Check | Target | What fails it |
|---|---|---|
| Lint | `make lint` | A finding of golangci-lint or actionlint, here or in `internal/compat`. |
| Test | `make test` | A failing test, on each supported Go and PostgreSQL. The job of the newest Go writes the coverage per package to its summary; no level of coverage is required. |
| Vulnerabilities | `make vuln` | A known vulnerability in code that taskmill calls. |
| API compatibility | `make api` | [gorelease](https://pkg.go.dev/golang.org/x/exp/cmd/gorelease) compares the API with the latest release, and the job summary lists the changes and the lowest version that allows them. Before v1.0.0, a minor release may break the API, so only a module that does not build or has a bad `go.mod` fails. From v1.0.0 on, an incompatible change fails too: it needs a new major version. |
| Rolling deploy | `make compat` | The latest release's client and worker failing on a schema that the current code migrated; see [Schema migrations](#schema-migrations). |
| CodeQL | — | Code scanning reports new alerts on the pull request and in the Security tab. |

Two more run on a schedule. **Fuzz** runs each fuzz target for 3 minutes
every day (`make fuzz FUZZTIME=3m`); a failing input is kept as an artifact
of the run, for the fix and its seed. **Scorecard** rates the project's
security practices each week and on every push to `main`, for the badge in
the README.

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
  rolling deploys work. `make compat` checks it: `internal/compat` is a
  module of its own that requires the latest release. Its test migrates a
  schema with the current code, which also enqueues grouped, delayed, and
  deduplicated tasks there (`internal/cmd/compat`). Then it runs the
  release's client, worker, schedules, outbox, and dashboard on that schema.
- New migrations must be safe to run on a busy database: set `lock_timeout`,
  and avoid statements that hold long locks on `tasks`, such as table
  rewrites.

## Design decisions

Significant decisions are recorded as Architecture Decision Records in
[docs/adr](docs/adr). If your change alters a decision, supersede its ADR in
the same pull request.

## Releasing

Maintainers release from a green `main`:

1. Choose the version: at least the one that the API check suggests for the
   release commit. A release with only fixes is a patch release; one that
   adds to the API, or before v1.0.0 breaks it, is a minor release.
2. In CHANGELOG.md, move `[Unreleased]` to a section `## [X.Y.Z] - YYYY-MM-DD`.
   Update the links at the bottom: `[Unreleased]` compares `vX.Y.Z...HEAD`,
   and `[X.Y.Z]` points to the release.
3. Check that `internal/migrations/released_test.go` pins the checksum of
   every migration in the release.
4. Tag the release commit with an annotated tag, and push it:
   `git tag -a vX.Y.Z -m "vX.Y.Z" && git push origin vX.Y.Z`.
5. Make the Go module proxy fetch the version, so pkg.go.dev shows it:
   `GOPROXY=https://proxy.golang.org go list -m github.com/code19m/taskmill@vX.Y.Z`.
6. Create the GitHub release from that CHANGELOG section.
7. Point the checks at the new release: in `internal/compat`, run
   `go get github.com/code19m/taskmill@vX.Y.Z && go mod tidy`, and merge
   that. Until then, the API check fails once the proxy knows the new
   version, because gorelease compares only with the newest release of a
   major version.

Never move or delete a published tag: the Go checksum database keeps every
version forever. Fix a bad release forward with a new patch release, and
mark the bad version with a `retract` directive in go.mod.
