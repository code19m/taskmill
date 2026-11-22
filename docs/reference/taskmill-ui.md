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
| `TASKMILL_UI_USERNAME` | `admin` | Text without a colon or a control character | Sets the user of HTTP basic authentication. It does nothing without a password |
| `TASKMILL_UI_PASSWORD` | None | Text without a control character | Sets that user's password. Without one, anyone who reaches the address can use the dashboard, and the command logs a warning |
| `TASKMILL_UI_READ_ONLY` | `false` | `true`, `false`, `1`, `0`, and the other values that Go's `strconv.ParseBool` accepts | `true` hides and refuses every action |
| `TASKMILL_UI_PREFIX` | None: the root | A path that starts with `/` | Sets the path that the dashboard is served at, such as `/admin/tasks`, when a proxy forwards that path unchanged |
| `TASKMILL_UI_FRAME_ANCESTORS` | None: no page can frame the dashboard | Origins, separated by commas or white space | Lists the origins of the pages that may show the dashboard in a frame, such as `https://admin.example.com` |
| `TASKMILL_UI_LOG_FORMAT` | `text` | `text` or `json` | Sets the format of the log lines on standard error |

The command has no flags and no config file. It sets the fields of
[`ui.Config`](https://pkg.go.dev/github.com/code19m/taskmill/ui#Config)
from these variables.

## Why it exits at start

The command checks everything before it listens. If a check fails, it logs
`taskmill-ui stopped` at Error, with the cause in `error`, and exits with
status 1.

| Cause | What to do |
|---|---|
| `TASKMILL_DATABASE_URL` is not set | Set it |
| `TASKMILL_DATABASE_URL` is not a valid connection string. To keep the password out of the log, `error` does not say what is wrong with it | In a URL, percent-encode the password: `@` is `%40`, and `/` is `%2F`. Then check the value of each parameter, such as `sslmode`, each file that the string names, such as `sslrootcert`, and the `PG*` variables of libpq, such as `PGSSLMODE`, which pgx reads too |
| The database is out of reach | Check the URL, the network, and the database |
| The schema is not migrated, or is older than this version of `taskmill-ui` | Let your app run `Migrate`: the command never migrates. Then start it again |
| `TASKMILL_SCHEMA` does not match the pattern | Fix the name |
| The connection's `default_transaction_isolation` is not `read committed` | Remove that setting from the URL or the role |
| `TASKMILL_UI_READ_ONLY` is not a boolean | Use `true` or `false` |
| `TASKMILL_UI_LOG_FORMAT` is not `text` or `json` | Use one of the two. This error is logged as text |
| `TASKMILL_UI_PREFIX` does not start with `/` | Start it with `/` |
| An entry of `TASKMILL_UI_FRAME_ANCESTORS` holds a semicolon | Remove it: an entry is one origin |
| `TASKMILL_UI_USERNAME` holds a colon, or it or `TASKMILL_UI_PASSWORD` holds a control character | Remove it. Basic authentication ends a username at its first colon, and a browser cannot send a control character, such as the line break at the end of a value read from a file |
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
| Time for a whole request to arrive: its headers and its body | Up to 10 seconds. Then the connection is closed |
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
| `GET /tasks` | The task list: 50 tasks, and up to 250 as you load more | `state`, `queue`, `kind`, `group`: each can repeat, and its first 100 values count. `limit`: how many tasks to show, from 1 to 250. `cursor`: the task that a later page starts after. `since`, `before`: a range of the time that orders the list. It is one of finish times for a list of only dead and canceled tasks, and one of creation times for any other |
| `GET /tasks/{id}` | One task | — |
| `GET /schedules` | The schedules | — |
| `GET /workers` | The workers | — |
| `GET /tasks/bulk/{action}` | The page that confirms a bulk action: with JavaScript, the task list shows it in a dialog. It counts the tasks first | The filters of `/tasks`, at least one |
| `POST /tasks/bulk/{action}` | Retries, cancels, or deletes every task that the filters select. `{action}` is `retry`, `cancel`, or `delete` | The filters of `/tasks`, at least one |
| `GET /tasks/{id}/{action}` | The page that confirms an action on one task: with JavaScript, the task's page shows it in a dialog | — |
| `POST /tasks/{id}/{action}` | Retries, cancels, or deletes one task. A delete leads to the list of the task's queue and state, which its page puts in the query | `queue`, `state`: for a delete |
| `GET /queues/pause` | The page that confirms a pause: with JavaScript, the overview shows it in a dialog. A resume does not ask | `name`, `range`: as for the `POST` |
| `POST /queues/{action}` | Pauses or resumes a queue. `{action}` is `pause` or `resume` | `name`: the queue. `range`: the period of the overview that the answer leads back to, as for `GET /` |
| `GET /schedules/{action}` | The page that confirms a run or a pause: with JavaScript, the schedules page shows it in a dialog. `{action}` is `run` or `pause`. A resume does not ask | `name`: the schedule |
| `POST /schedules/{action}` | Pauses, resumes, or runs a schedule. `{action}` is `pause`, `resume`, or `run` | `name`: the schedule |
| `GET /choices/groups` | A JSON list of up to 100 FIFO group keys, for the group filter | `search`: text that the keys contain |
| `GET /static/{version}/{name}` | The scripts, the styles, and the icon. `{version}` is a hash of every static file. A page links the current one, which may be cached for a year. Another version gets the current file, which the browser must check again | — |

For example, `/tasks?state=dead&queue=emails` lists the dead tasks of the
queue `emails`, and `POST /queues/pause?name=emails` pauses that queue.

- **Every action is a `POST`.** A `GET` never changes anything.
- **`since` and `before` are instants.** The list shows the tasks that
  finished, or were created, at `since` or later, and before `before`. Write
  a time to the minute or to the second, with `Z` or an offset: `2026-01-31T14:05Z`, or
  `2026-01-31T19:05:30+05:00`. A time without a zone, such as
  `2026-01-31T14:05`, is in UTC. The dashboard's own links use `Z`.
- **`theme` works on every page:** `?theme=light`, `dark`, or `system`. The
  browser remembers the choice. Where it blocks storage, as some browsers
  do in a frame of another site, the choice holds for that page only.
- **The dashboard sets some parameters itself,** in the redirect after an
  action: `done`, `count`, `skipped`, `conflicts`, and the `cursor` of a
  bulk action. Don't write them.

| Status | When |
|---|---|
| 303 | An action worked. The redirect leads to a page that says so |
| 304 | A live update: the page did not change since the copy that the browser shows, but for the relative times, such as "4s ago", that its script keeps counting |
| 307 | With a prefix: the prefix without its last slash, such as `/admin/tasks`, or a path that is not clean, such as one with a double slash. The redirect leads to the clean path under the prefix |
| 400 | A filter value is not valid, such as an unknown state. Or the `cursor` is not one that this list made, as in a bookmark from before an upgrade. Or the `limit` is not a number from 1 to 250. Or `since` or `before` is not a time in one of their forms, or `since` is not before `before`. Or a bulk action, or its page, got no filter. Or an action on a queue or a schedule got no valid `name`, or the page that confirms it got no `name` |
| 401 | The login is missing or wrong, when basic authentication is on |
| 403 | An action, or the page that confirms it, in read-only mode, or a `POST` from another site |
| 404 | No such page, task, or schedule, also for a `GET` of an action that does not ask first, such as a resume. With a prefix, also a path that only starts like it, such as `/admin/tasksx` |
| 405 | A method that the path does not take, such as a `POST` to a page |
| 409 | The action does not fit, such as a retry of a running task. Or a retry of a canceled task whose dedupe key another task holds: the page names that task |
| 500 | A read or an action failed for another reason. The page says "Something went wrong", and the log has `dashboard request failed` |
| 503 | The dashboard cannot connect to the database: the page says "Can’t reach the database", and the log has `dashboard request failed`. Or the database did not answer within the request's 10 seconds, or before an earlier deadline that your app set in the request's context, or a list of tasks read for more than its 5 seconds: the page says "Taking too long". Either has `Retry-After: 5` |

With JavaScript, a page that answers 500 or 503 tries its address again
by itself, 5, 10, 20, and 40 seconds apart, then once a minute, and shows
the page once it answers. It never sends an action again.

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
- [Fixed timings and limits](defaults-and-limits.md#dashboard): the dashboard's limits, such as its page size
- [Kubernetes](../production/kubernetes.md): manifests with the probes
- [`taskmill-ui`](https://pkg.go.dev/github.com/code19m/taskmill/cmd/taskmill-ui) and [`ui.Config`](https://pkg.go.dev/github.com/code19m/taskmill/ui#Config) on pkg.go.dev

**Next:** [Dashboard pages](dashboard-pages.md)
