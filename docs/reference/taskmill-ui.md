# The `taskmill-ui` command

Everything to look up about `taskmill-ui`, the dashboard as a program of its
own: its environment variables, why it exits at start, its probes, and the
dashboard's routes. To choose between the command and the `ui` package, see
[The dashboard](../guides/dashboard.md).

## Environment variables

The command reads all its settings from the environment. An empty variable
counts as not set.

| Variable | Default | Valid values | What it does |
|---|---|---|---|
| `TASKMILL_DATABASE_URL` | Required | A PostgreSQL connection string | Names the database to connect to |
| `TASKMILL_SCHEMA` | `taskmill` | Matches `^[a-z_][a-z0-9_]{0,62}$` | Names the [schema](../concepts/glossary.md#schema) to show |
| `TASKMILL_UI_ADDR` | `localhost:8080` | A host and a port | Sets the address to listen on. In a container, use `:8080` |
| `TASKMILL_UI_USERNAME` | `admin` | Any text | Sets the user of HTTP basic authentication. It does nothing without a password |
| `TASKMILL_UI_PASSWORD` | None | Any text | Sets that user's password. Without one, anyone who reaches the address can use the dashboard, and the command logs a warning |
| `TASKMILL_UI_READ_ONLY` | `false` | `true`, `false`, `1`, `0`, and the other values that Go's `strconv.ParseBool` accepts | `true` hides and refuses every action |
| `TASKMILL_UI_PREFIX` | None: the root | A path that starts with `/` | Sets the path that the dashboard is served at, such as `/admin/tasks`, when a proxy forwards that path unchanged |
| `TASKMILL_UI_FRAME_ANCESTORS` | None: no page can frame the dashboard | Origins, separated by commas or white space | Lists the origins of the pages that may show the dashboard in a frame, such as `https://admin.example.com` |
| `TASKMILL_UI_LOG_FORMAT` | `text` | `text` or `json` | Sets the format of the log lines on standard error |

The command has no flags and no config file. It sets the fields of
[`ui.Config`](configuration.md#uiconfig) from these variables.

## Why it exits at start

The command checks everything before it listens. If a check fails, it logs
`taskmill-ui stopped` at Error, with the cause in `error`, and exits with
status 1.

| Cause | What to do |
|---|---|
| `TASKMILL_DATABASE_URL` is not set, or is not a valid connection string | Set it |
| The database is out of reach | Check the URL, the network, and the database |
| The schema is not migrated, or is older than this version of `taskmill-ui` | Let your app run `Migrate`: the command never migrates. Then start it again |
| `TASKMILL_SCHEMA` does not match the pattern | Fix the name |
| The connection's `default_transaction_isolation` is not `read committed` | Remove that setting from the URL or the role |
| `TASKMILL_UI_READ_ONLY` is not a boolean | Use `true` or `false` |
| `TASKMILL_UI_LOG_FORMAT` is not `text` or `json` | Use one of the two. This error is logged as text |
| `TASKMILL_UI_PREFIX` does not start with `/` | Start it with `/` |
| An entry of `TASKMILL_UI_FRAME_ANCESTORS` holds a semicolon | Remove it: an entry is one origin |
| The address is in use, or is not valid | Change `TASKMILL_UI_ADDR` |

On `SIGINT` or `SIGTERM`, the command stops listening, waits up to 10
seconds for open requests, and exits.

## Probes

The command answers two more paths, for probes. They are at the root,
whatever the prefix, and they need no login.

| Path | Answers 200 | Answers 503 |
|---|---|---|
| `GET /livez` | While the server runs | Never |
| `GET /readyz` | While the database answers too | When the database does not answer within 2 seconds |

`/readyz` asks the database at most once a second, however many requests
come. Use it for the readiness probe only: a restart does not bring the
database back.

## HTTP server

| What | Value |
|---|---|
| Wait for a request's headers | Up to 10 seconds |
| Time to write an answer | Up to 30 seconds |
| An idle connection is kept | 2 minutes |
| Wait for open requests at shutdown | Up to 10 seconds |

The command serves plain HTTP. Put a proxy with TLS in front of it when you
set a password: basic authentication sends the password with every request.

## Dashboard routes

These are the routes of the dashboard, below the prefix. They are the same
when you mount the `ui` package in your own app. Use them for a proxy's
allow-list, or to link to a page from an alert.

| Method and path | What it does | Query parameters |
|---|---|---|
| `GET /` | The overview: queues, counts, and charts | `range`: `1h` (the default), `6h`, or `24h` |
| `GET /tasks` | The task list, 50 tasks per page | `state`, `queue`, `kind`, `group`: each can repeat, and its first 100 values count. `cursor`: the next page |
| `GET /tasks/{id}` | One task | — |
| `GET /schedules` | The schedules | — |
| `GET /workers` | The workers | — |
| `GET /tasks/bulk/{action}` | The page that confirms a bulk action. It counts the tasks first | The filters of `/tasks`, at least one |
| `POST /tasks/bulk/{action}` | Retries, cancels, or deletes every task that the filters select. `{action}` is `retry`, `cancel`, or `delete` | The filters of `/tasks`, at least one |
| `POST /tasks/{id}/{action}` | Retries, cancels, or deletes one task | — |
| `POST /queues/{name}/{action}` | Pauses or resumes a queue. `{action}` is `pause` or `resume` | — |
| `POST /schedules/{name}/{action}` | Pauses, resumes, or runs a schedule. `{action}` is `pause`, `resume`, or `run` | — |
| `GET /choices/groups` | A JSON list of up to 100 FIFO group keys, for the group filter | `search`: text that the keys contain |
| `GET /static/{name}` | The scripts, the styles, and the icon | `v`: the file's version |

For example, `/tasks?state=dead&queue=emails` lists the dead tasks of the
queue `emails`.

- **Every action is a `POST`.** A `GET` never changes anything.
- **`theme` works on every page:** `?theme=light`, `dark`, or `system`. The
  browser remembers the choice.
- **The dashboard sets some parameters itself,** in the redirect after an
  action: `done`, `count`, `skipped`, and the `cursor` of a bulk action.
  Don't write them.

| Status | When |
|---|---|
| 303 | An action worked. The redirect leads to a page that says so |
| 304 | The page did not change since the browser's copy |
| 400 | A filter value is not valid, such as an unknown state. Or the page of a bulk action got no filter |
| 401 | The login is missing or wrong, when basic authentication is on |
| 403 | An action in read-only mode, or a `POST` from another site |
| 404 | No such page, task, or schedule |
| 405 | The path exists, but not with this method |
| 409 | The action does not fit, such as a retry of a running task |
| 500 | A read or an action failed. The log has `dashboard request failed` |
| 503 | The database did not answer in time. The page says "Taking too long" |

## Actions in the log

Each action that works logs one Info line, `dashboard action`. A bulk
action that stops early also logs it, if it changed any task. Keep these
lines if you need a record of what was done.

| `action` | `target` | More attributes |
|---|---|---|
| `retry task`, `cancel task`, `delete task` | The task ID | — |
| `pause queue`, `resume queue` | The queue name | — |
| `pause schedule`, `resume schedule`, `run schedule` | The schedule name | — |
| `retry tasks`, `cancel tasks`, `delete tasks` | The filters, as a query string such as `queue=emails&state=dead` | `tasks`: how many tasks it changed. `skipped`: how many it left, because another transaction held them |

The line does not say who did it: the command knows one user only. To log
the person, see
[One dashboard for many services](../recipes/one-dashboard-for-many-services.md).

## What it does not do

- **It does not migrate the schema.** The app that owns the schema does.
- **It exports no metrics.** Your apps report the queue gauges, with the
  `otel` module ([Metrics and traces](metrics.md)).
- **You cannot change its log level.** It logs at Info and above.
  [Log messages](log-messages.md#the-taskmill-ui-command) lists its lines.

## See also

- [The dashboard](../guides/dashboard.md): run it, protect it, and show it inside another app
- [Configuration](configuration.md#uiconfig): the fields of `ui.Config`
- [Defaults and limits](defaults-and-limits.md#dashboard): the dashboard's limits, such as its page size
- [Kubernetes](../production/kubernetes.md): manifests with the probes
- [`taskmill-ui` on pkg.go.dev](https://pkg.go.dev/github.com/code19m/taskmill/cmd/taskmill-ui)

**Next:** [Errors](errors.md)
