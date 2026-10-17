# Contributing to taskmill

Thank you for helping. This guide covers how to set up the project, what we
expect from every change, and how changes are merged.

## Before you start

For anything larger than a small fix, open an issue first. Describe the problem
and the approach you have in mind. Agreeing on the design before writing code
saves time for everyone.

## Development setup

You need Git, Make, and Go. The exact Go version comes from the `toolchain`
line in [go.mod](go.mod); with the default `GOTOOLCHAIN=auto`, any recent Go
downloads it for you.

```sh
git clone https://github.com/code19m/taskmill.git
cd taskmill
make check
```

`make check` runs everything CI runs. The first run installs the pinned
development tools into `./bin`. `make help` lists every target:

| Target | What it does |
|---|---|
| `make check` | All CI checks: tidy, lint, test, vulnerability scan |
| `make fmt` | Format the code with gofumpt and goimports |
| `make lint` | Run golangci-lint and actionlint |
| `make test` | Run the tests with the race detector, in random order |
| `make cover` | Run the tests and open an HTML coverage report |
| `make vuln` | Scan for known vulnerabilities with govulncheck |

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
- Tests must pass with `-race` and `-shuffle=on`. They must not rely on timing
  luck: never `time.Sleep` while waiting for a result.
- A bug fix includes a test that fails without the fix.
- `Example` functions include an `// Output:` comment, so they run as tests.

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
  `feat(worker)!: rename Run to Start`, and the body explains how to migrate.

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

## Design decisions

Significant decisions are recorded as Architecture Decision Records in
[docs/adr](docs/adr). If your change alters a decision, supersede its ADR in
the same pull request.

## Versioning

taskmill follows [Semantic Versioning](https://semver.org/). Before v1.0.0, a
minor release may contain breaking changes; the release notes always call them
out.
