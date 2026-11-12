# Reference

Exact numbers and complete lists, to look things up. For the full API, see
[pkg.go.dev](https://pkg.go.dev/github.com/code19m/taskmill).

| Page | Read this when… |
|---|---|
| [Configuration](configuration.md) | you want the type, default, or limit of a field you can set |
| [Defaults and limits](defaults-and-limits.md) | you need a fixed timing, or a limit that is not a field |
| [The `taskmill-ui` command](taskmill-ui.md) | you set up the dashboard command or a proxy in front of it, or you link to a dashboard page |
| [Errors](errors.md) | a call returned an error, a handler's context ended, or a task's history shows a code |
| [Log messages](log-messages.md) | you see a log line and want to know what it means and what to do |
| [Metrics and traces](metrics.md) | you build a dashboard on taskmill's spans, metrics, and gauges |
| [Compatibility](compatibility.md) | you want to know what an upgrade may change, which Go and PostgreSQL versions work, or which releases get fixes |
| [Packages](packages.md) | you wonder which package to import |
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

**Next:** [Configuration](configuration.md)
