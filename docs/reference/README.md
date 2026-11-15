# Reference

Pages to look things up in. The API has its reference on
[pkg.go.dev](https://pkg.go.dev/github.com/code19m/taskmill): the doc
comment of each function, type, and field says what it does, with its
default, its limits, and its errors. The pages here hold what has no Go
identifier, and three indexes that lead you to the right identifier.

## Find it in the API

Each of these pages is an index: names and links, with no facts of its own.
Your editor and `go doc` show the same doc comments as pkg.go.dev.

| Page | Read this when… |
|---|---|
| [Packages](packages.md) | you wonder which package to import |
| [Configuration](configuration.md) | you look for the type that holds a setting |
| [Errors](errors.md) | you see an error, an `error.type`, or a code in a task's history, and want the identifier that explains it |

## What has no Go identifier

| Page | Read this when… |
|---|---|
| [The `taskmill-ui` command](taskmill-ui.md) | you set up the dashboard command or a proxy in front of it, or you link to a dashboard page |
| [Log messages](log-messages.md) | you see a log line and want to know what it means and what to do |
| [Metrics and traces](metrics.md) | you build a dashboard on taskmill's spans, metrics, and gauges |
| [Fixed timings and limits](defaults-and-limits.md) | you need a timing of the workers, the maintainer, or the dashboard that no setting changes |
| [Compatibility](compatibility.md) | you want to know what an upgrade may change, which Go and PostgreSQL versions work, or which releases get fixes |
| [Performance](performance.md) | you want to know how fast and how light taskmill is, and the limits of those numbers |

## Lookup pages in other sections

| Page | What you look up there |
|---|---|
| [Task lifecycle](../concepts/task-lifecycle.md) | The eight states of a task, and what you can do in each |
| [Glossary](../concepts/glossary.md) | Every term that the docs use |
| [Monitoring](../production/monitoring.md) | Which signals to alert on, with rules and thresholds |
| [Troubleshooting](../production/troubleshooting.md) | The way from a symptom to its fix |
| [Changelog](../../CHANGELOG.md) | What changed in each release |

## Why there is no page about the tables

taskmill's tables are internal on purpose, so that a minor release can
change them. Read and change tasks through `Client` or the dashboard, never
with SQL. Only the `migrations` table is public, for teams that apply
migrations with their own tool:
[Compatibility](compatibility.md#what-never-changes-within-a-major-version)
names its columns.

Contributors find the tables and their indexes in
[Storage schema](../design/schema.md), and the exact definitions in
[`internal/migrations`](../../internal/migrations).

**Next:** [Packages](packages.md)
