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

`make check` runs the same checks as CI (see [Checks](#checks)), in every
module: the core, `otel/`, and `examples/orms/`, each with its own go.mod.
The first run installs the pinned development tools into `./bin`.

`make help` lists every target. These are the ones you need most:

| Target | What it does |
|---|---|
| `make db` | Start PostgreSQL in Docker for the tests. `make db-stop` removes it. |
| `make check` | Run every CI check except `make release-check` and `make api`. |
| `make fmt` | Format the code. |
| `make test` | Run the tests with the race detector, in random order. |
| `make api` | Compare the API of the last commit with the latest release. It needs committed code, so `make check` leaves it out. |
| `make compat` | Run the latest release on the schema that the current code migrated. |
| `make bench` | Run the benchmarks and the fault scenarios, and print a Markdown report. |

## Code standards

The linters in [.golangci.yml](.golangci.yml) and `make fmt` check the
mechanical rules, and their messages say what to change. These rules need
your judgment:

- **Idiomatic Go.** Follow [Effective Go](https://go.dev/doc/effective_go)
  and [Go Code Review Comments](https://go.dev/wiki/CodeReviewComments). When
  you are unsure, do what the standard library does.
- **Code that reads in steps.** One blank line separates the steps of a
  function, as paragraphs do in prose, so a reader sees its shape before
  reading a word. A call and the check of its error are one step.
- **Doc comments are the reference of the API.** They describe behavior and
  guarantees, not how the code works inside: the default, the limits, and
  the errors. See [Writing doc comments](docs/design/writing-godoc.md).
- **Errors carry context.** Wrap an error with what the code was doing.
  When callers must handle a condition, expose it as a sentinel error
  (`ErrX`) or an error type (`XError`).
- **A new dependency has its reason on record** in
  [Dependencies and modules](docs/design/dependencies.md).
- **Library manners.** No global state, and no panics on bad input. Every
  call that can block takes a `context.Context`.
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
| Test behavior against a real PostgreSQL. | Each test migrates its own schema through `internal/testdb`, so tests run in parallel. Run them with `make test`: a plain `go test ./...` does not know where the database is, so it skips them and still prints `ok`. |
| Never rely on lucky timing. | To wait for a result, poll with a deadline (`waitFor`, `waitState`, `recv`), never sleep a fixed time. To skip the worker's internal timers, such as the maintainer's interval, shorten them with `taskmill.SetTiming` from `export_test.go`. |
| Never depend on the real date. | To test a clock change, set the database's clock (`newClockEnv`). |
| Fix a bug with a test. | The test fails without the fix. |
| Let examples run. | An `Example` function that needs no database has an `// Output:` comment, so it runs as a test. |
| Fuzz input from outside. | The dashboard's query parameters, list cursors, task payloads, and cron expressions each have a fuzz target. When a fuzzer finds a bug, add its input as a seed. |

## Checks

A pull request merges only when CI passes. Each check runs a Makefile
target, so you can run it on your machine. The only exceptions are CodeQL
and the [title check](#commit-messages).

| Check | Target | It fails when… |
|---|---|---|
| Lint | `make tidy-check lint` | golangci-lint or actionlint reports a finding, in any module or in `internal/compat`; a go.mod or go.sum is not tidy; or a committed file is over 1 MiB. |
| Test | `make test` | A test fails, on any supported Go and PostgreSQL. The job with the newest Go writes the coverage per package to its summary. |
| Test through PgBouncer | `make test-pgbouncer` | A test fails through PgBouncer in transaction mode. |
| Vulnerabilities | `make vuln` | Code that taskmill calls has a known vulnerability. |
| API compatibility | `make api` | A module does not build or has a bad `go.mod`. From v1.0.0 on, also an incompatible change: the job summary lists the changes. |
| Rolling deploy | `make compat` | The latest release's client, worker, or dashboard fails on a schema that the current code migrated. See [Schema migrations](#schema-migrations). |
| Otel on its required core | `make otel-release-check` | The `otel` module does not build, or its tests fail, against the core version it requires, once that version is released. |
| Release config | `make release-check` | `.goreleaser.yaml` is not valid, or `make release-notes` can no longer read CHANGELOG.md. |
| CodeQL | none | Code scanning finds new alerts. It reports them on the pull request and in the Security tab. |

Four more checks run on a schedule:

| Run | When | What it does |
|---|---|---|
| Faults | Every night, and on pull requests that change them | Runs the fault scenarios (`make faults`). |
| Benchmarks | Every week, and on demand | Runs the benchmarks on the [reference runner](docs/design/benchmarks.md#reference-runner), and keeps the report in the run's summary. Only an error fails it. |
| Fuzz | Every day | Runs each fuzz target for 3 minutes, in a job of its own (`make fuzz FUZZ=…`). A failing input is kept as an artifact of the run, for the fix and its seed. |
| Scorecard | Every week, and on every push to `main` | Rates the project's security practices, for the badge in the README. |

## Commit messages

We use [Conventional Commits](https://www.conventionalcommits.org/). Pull
requests are squash-merged, so **the pull request title becomes the commit
message** on `main`. CI checks its form.

```text
<type>(<optional scope>): <summary>
```

- **type** is one of `feat`, `fix`, `perf`, `refactor`, `test`, `docs`,
  `build`, `ci`, `chore`, `style`, `revert`.
- **scope** is the package or area, such as `worker`, `migrate`, or `deps`.
- **summary** is a command: "add", not "added".
- **`!`** marks a breaking change: `feat(worker)!: rename Run to Start`.
  Explain how to migrate in the pull request description and in
  CHANGELOG.md.

```text
feat(worker): drain in-flight tasks on shutdown
fix(queue): keep int64 payload values exact
docs: explain at-least-once delivery
```

## Pull requests

- **Keep each pull request to one change.** A large change is easier to
  review as a series of small ones.
- **Fill in the template:** what changed, why, and how you tested it.
  Someone from the project team reviews every change.
- **Update CHANGELOG.md.** A change that users can see adds one line under
  `[Unreleased]`, in the right category: Added, Changed, Deprecated, Removed,
  Fixed, or Security. A breaking change starts with **Breaking:** and says
  how to migrate.
- **Update the docs in the same pull request.** If you change documented
  behavior, change the docs and the godoc with it. See
  [Writing docs](#writing-docs).
- **Follow the [compatibility promise](docs/reference/compatibility.md).**
  From v1.0.0 on, no change breaks the API: a replaced name is deprecated
  and stays until v2, and stored names, such as the `State` values, never
  change.

## Schema migrations

- **A schema change is a new file** in `internal/migrations`.
- **The latest release must keep working on the new schema,** because a
  rolling deploy runs old and new code side by side. `make compat` runs the
  release's client, worker, schedules, outbox, and dashboard there.
- **A migration must be safe on a busy database.** Avoid statements that
  hold long locks on `tasks`, such as table rewrites.

## Design decisions

The design pages in [How taskmill is built](docs/design/README.md) say why
taskmill works the way it does, and which options we rejected. Each
decision is on the page of the mechanism that it explains. If your change
goes against a decision, change that page in the same pull request, and say
why in it.

## Writing docs

The docs live in [docs](docs/README.md). Follow the style guide in
[Writing docs](docs/design/writing-docs.md): plain words, short sentences,
and a table or a diagram where it explains better. The doc comments follow
[Writing doc comments](docs/design/writing-godoc.md). `go test
./internal/docs/` checks the rules that a machine can check, in the pages
and in the comments, and its messages say what to change.

## Releasing

The project team releases from a green `main`. The steps are in
[Releasing](docs/design/releasing.md).

**Next:** [How taskmill is built](docs/design/README.md)
