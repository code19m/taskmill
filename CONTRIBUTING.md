# Contributing to taskmill

Thank you for helping. This page explains how to set up the project, what
every change needs, and how changes get merged. Everyone who takes part
follows the [code of conduct](CODE_OF_CONDUCT.md).

## Before you start

| You want to… | Do this |
|---|---|
| Fix something small, such as a typo | Open a pull request. |
| Make a larger change | Open an issue first. Describe the problem and your plan. If we agree on the design before you write code, nobody wastes time. |
| Ask a question | Ask in [Discussions](https://github.com/code19m/taskmill/discussions/categories/q-a), not in issues. |
| Report a security issue | Follow [SECURITY.md](SECURITY.md). Never use a public issue. |

## Set up the project

You need Git, Make, Go, and Docker.

- **Go:** the `toolchain` line in [go.mod](go.mod) names the exact version.
  With the default `GOTOOLCHAIN=auto`, any recent Go downloads it for you.
- **Docker:** the integration tests need PostgreSQL, and `make db` starts it
  in Docker.

```sh
git clone https://github.com/code19m/taskmill.git
cd taskmill
make db
make check
```

`make check` runs the same checks as CI (see [Checks](#checks)). It runs
them in every module of the repository: the core, and the `otel` module in
`otel/`, which has its own go.mod. The first run installs the pinned
development tools into `./bin`.

`make help` lists every target. These are the ones you need most:

| Target | What it does |
|---|---|
| `make db` | Start PostgreSQL in Docker for the tests. `make db-stop` removes it. |
| `make check` | Run every CI check except `make release-check` and `make api`: tidy, lint, test, test through PgBouncer, vulnerability scan, otel release check, and rolling deploy. |
| `make fmt` | Format the code with gofumpt and goimports. |
| `make lint` | Run golangci-lint and actionlint. |
| `make test` | Run the tests with the race detector, in random order, and write `coverage.out`. |
| `make test-pgbouncer` | Run the tests through PgBouncer in transaction mode, as CI does. |
| `make cover` | Run the tests and open an HTML coverage report. |
| `make cover-report` | Print the coverage of the last `make test`, per package. |
| `make vuln` | Scan for known vulnerabilities with govulncheck. |
| `make api` | Compare the API of the last commit with the latest release. |
| `make compat` | Run the latest release's client, worker, schedules, outbox, and dashboard on the current schema. |
| `make fuzz` | Run each fuzz target for `FUZZTIME` (10 seconds by default). |
| `make bench` | Run the benchmarks and the fault scenarios, and print a Markdown report. |
| `make faults` | Run the fault scenarios as the nightly CI run does, against databases of their own. |
| `make release-check` | Check `.goreleaser.yaml` with `goreleaser check`. |
| `make snapshot` | Build a release into `./dist` and its image into Docker, without publishing anything. |

> [!NOTE]
> `make check` leaves out `make api`, because gorelease compares committed
> code and stops on uncommitted changes. Commit first, then run `make api`.

## Code standards

Every change meets the same bar:

- **Idiomatic Go.** Follow [Effective Go](https://go.dev/doc/effective_go)
  and [Go Code Review Comments](https://go.dev/wiki/CodeReviewComments). When
  you are unsure, do what the standard library does.
- **The linters pass.** CI enforces [.golangci.yml](.golangci.yml). A
  `//nolint` directive names the linter and gives a reason, for example
  `//nolint:gosec // the query text is a constant`.
- **Every exported name has a doc comment.** It starts with the name. It
  describes behavior and guarantees, not how the code works inside.
- **Errors carry context.** Wrap them with `fmt.Errorf("...: %w", err)`.
  When callers must handle a condition, expose it as a sentinel error
  (`ErrX`) or an error type (`XError`).
- **No new dependencies without an ADR.** The `depguard` linter enforces the
  allowlist. See [ADR 0001](docs/adr/0001-dependency-policy.md).
- **Library manners.** No global state, no `init` functions, no writing to
  stdout, and no panics on bad input. Every call that can block takes a
  `context.Context`.
- **Method names follow one pattern:**

| What the method does | Name | Examples |
|---|---|---|
| Acts on tasks, the client's main resource | A bare verb | `Get`, `List`, `Count`, `Cancel`, `Retry`, `Delete` |
| Acts on every task that a filter matches | The verb plus `Matching` | `CancelMatching` |
| Reads another resource | Its plural noun | `Queues`, `Groups`, `Schedules`, `Workers` |
| Acts on one other resource | A verb and its noun | `PauseQueue`, `UpdateQueue`, `RunSchedule` |
| Runs in the caller's transaction | The name plus `Tx` | `EnqueueTx` |

## Tests

| Rule | Details |
|---|---|
| Use the standard `testing` package. | Prefer table-driven tests when they make the cases easier to read. |
| Test behavior against a real PostgreSQL. | Each test migrates its own schema through `internal/testdb`, so tests run in parallel. CI starts PostgreSQL with `make db`, as you do locally, once for each supported version. |
| Pass with `-race` and `-shuffle=on`. | Never rely on lucky timing. To wait for a result, poll with a deadline (`waitFor`, `waitState`, `recv`). Never sleep a fixed time and hope the result is ready. |
| Shorten timers instead of waiting for them. | A test that waits for the worker's internal timers, such as the maintainer's interval, may shorten them with `taskmill.SetTiming` from `export_test.go`. |
| Fix a bug with a test. | The test fails without the fix. |
| Keep `Example` functions running. | They compile with the tests. Those that need no database have an `// Output:` comment, so they run as tests too. |
| Fuzz input from outside. | The dashboard's query parameters, list cursors, and task payloads each have a fuzz target. Every test run replays the seeds. When a fuzzer finds a bug, add its input as a seed. |

Every test also checks the FIFO group invariants when it ends.

## Checks

A pull request merges only when CI passes. Every check except CodeQL is a
Makefile target, so you can run it on your machine:

| Check | Target | It fails when… |
|---|---|---|
| Lint | `make tidy-check lint` | golangci-lint or actionlint reports a finding, in any module or in `internal/compat`; a go.mod or go.sum is not tidy; or a committed file is over 1 MiB. |
| Test | `make test` | A test fails, on any supported Go and PostgreSQL. The job with the newest Go writes the coverage per package to its summary. No coverage level is required. |
| Test through PgBouncer | `make test-pgbouncer` | A test fails through PgBouncer in transaction mode. |
| Vulnerabilities | `make vuln` | Code that taskmill calls has a known vulnerability. |
| API compatibility | `make api` | The API breaks the compatibility rules (see below). |
| Rolling deploy | `make compat` | The latest release's client, worker, or dashboard fails on a schema that the current code migrated. See [Schema migrations](#schema-migrations). |
| Otel on its required core | `make otel-release-check` | The `otel` module does not build, or its tests fail, against the core version it requires, once that version is released. |
| Release config | `make release-check` | `.goreleaser.yaml` is not valid. |
| CodeQL | none | Code scanning finds new alerts. It reports them on the pull request and in the Security tab. |

**How the API check works.**
[gorelease](https://pkg.go.dev/golang.org/x/exp/cmd/gorelease) compares the
API with the latest release. The job summary lists the changes and the
lowest version that allows them.

- **While the latest release is before v1.0.0,** a minor release may break
  the API. So the check fails only when a module does not build, or has a
  bad `go.mod`.
- **Once the latest release is v1.0.0 or later,** an incompatible change
  fails too ("Incompatible changes were detected"). It needs a new major
  version.

The check switches by itself, because it compares with the release that
`internal/compat` requires. Nothing else needs to change.

Three more checks run on a schedule:

| Run | When | What it does |
|---|---|---|
| Faults | Every night, and on pull requests that change them | Runs the fault scenarios (`make faults`). |
| Fuzz | Every day | Runs each fuzz target for 3 minutes (`make fuzz FUZZTIME=3m`). A failing input is kept as an artifact of the run, for the fix and its seed. |
| Scorecard | Every week, and on every push to `main` | Rates the project's security practices, for the badge in the README. |

## Commit messages

We use [Conventional Commits](https://www.conventionalcommits.org/). Pull
requests are squash-merged, so **the pull request title becomes the commit
message** on `main`. CI checks the title.

```text
<type>(<optional scope>): <summary>
```

| Part | Rule |
|---|---|
| type | One of `feat`, `fix`, `perf`, `refactor`, `test`, `docs`, `build`, `ci`, `chore`, `style`, `revert`. |
| scope | The package or area, such as `worker`, `migrate`, or `deps`. |
| summary | A command ("add", not "added"). It starts with a lowercase letter, has no period at the end, and is 72 characters or fewer. |
| `!` | Marks a breaking change: `feat(worker)!: rename Run to Start`. Explain how to migrate in the pull request description and in CHANGELOG.md. |

```text
feat(worker): drain in-flight tasks on shutdown
fix(queue): keep int64 payload values exact
docs: explain at-least-once delivery
```

## Pull requests

- **Keep each pull request to one change.** A large change is easier to
  review as a series of small ones.
- **Fill in the template:** what changed, why, and how you tested it.
- **Pass `make check`.** Someone from the project team reviews every change.
- **Update CHANGELOG.md.** A change that users can see adds one line under
  `[Unreleased]`, in the right category: Added, Changed, Deprecated, Removed,
  Fixed, or Security. A breaking change starts with **Breaking:** and says
  how to migrate.
- **Update the docs in the same pull request.** If you change documented
  behavior, change the docs and the godoc with it. See
  [Writing docs](#writing-docs).
- **Follow the [compatibility promise](docs/reference/compatibility.md).**
  From v1.0.0 on, no change breaks the API. Interfaces never grow. A replaced
  name is deprecated and stays until v2. Stored names, such as the `State`
  values, never change.

## Schema migrations

- **A schema change is a new file** in `internal/migrations`. Released
  migrations never change: a test pins their checksums.
- **The latest release must keep working on the new schema,** so that
  rolling deploys work. `make compat` checks this. `internal/compat` is a
  module of its own that requires the latest release. Its test migrates a
  schema with the current code, which also enqueues grouped, delayed, and
  deduplicated tasks there (`internal/cmd/compat`). Then it runs the
  release's client, worker, schedules, outbox, and dashboard on that schema.
- **A migration must be safe on a busy database.** Set `lock_timeout`. Avoid
  statements that hold long locks on `tasks`, such as table rewrites.

## Design decisions

We record important decisions as Architecture Decision Records (ADRs) in
[docs/adr](docs/adr/README.md). If your change goes against a decision,
write a new ADR that replaces it, in the same pull request. [How taskmill is
built](docs/design/README.md) explains the design.

## Writing docs

The docs live in [docs](docs/README.md). Follow the style guide in
[Writing docs](docs/design/writing-docs.md): plain words, short sentences,
and a table or a diagram where it explains better. `go test ./internal/docs/`
fails on a broken link or a missing anchor.

## Releasing

The project team releases from a green `main`. The steps are in
[Releasing](docs/design/releasing.md).

## See also

- [How taskmill is built](docs/design/README.md): where to start reading the
  design
- [Writing docs](docs/design/writing-docs.md): the style guide
- [SECURITY.md](SECURITY.md): how to report a vulnerability

**Next:** [How taskmill is built](docs/design/README.md)
