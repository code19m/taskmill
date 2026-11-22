# The dashboard

The dashboard is a web page for your queues, tasks, schedules, and workers.
Use it to see what taskmill is doing, and to retry, cancel, or delete
tasks. This page shows how to run it, how to protect it, and what it can do.

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="../images/dashboard-dark.png">
  <img alt="The dashboard's overview: task counts, a dial of the attempts a minute, charts of the last hour's tasks and attempts, and the queues" src="../images/dashboard-light.png">
</picture>

## Which should I use?

| Situation | What to use | Why |
|---|---|---|
| Your app has a Go HTTP server (most apps) | Mount `ui.New` in your app | It runs the same taskmill version as your schema, uses your login, and needs nothing new to deploy |
| Your Go app serves no HTTP, such as a process that only runs workers, or your admin app is in another language | The `taskmill-ui` command, or its container image | It runs on its own, and you set it up with environment variables |
| An admin panel must show the dashboard inside its own page | Either of the above, in an `<iframe>` | You list the panel's origin in `FrameAncestors` |
| You have several services | Mount it in each service, and let one admin app forward a path to each | See [One dashboard for many services](../recipes/one-dashboard-for-many-services.md) |

Mount it in your app unless you have a reason not to.

## Mount it in your app

`ui.New` returns an `http.Handler`. Mount it behind your own login:

```go
dashboard, err := ui.New(client, &ui.Config{Prefix: "/taskmill", Logger: logger})
if err != nil {
    return err
}
mux.Handle("/taskmill/", requireAdmin(dashboard)) // requireAdmin: your own login check
```

- `Prefix` is the path where you mount it. The dashboard removes it from
  requests itself, so don't wrap it in `http.StripPrefix`.
- It reads only through the [`Client`](operations-api.md)'s queries. Those
  over tasks, groups, and activity stop after 5 seconds, and the others read
  small tables or single rows: [What people can do in it](#what-people-can-do-in-it)
  says what its task lists read. An open tab updates every few seconds
  while it is visible, and less often while the server fails: a page that
  failed tries again by itself.
- A request can take 10 seconds when the database is slow. Then the page
  says "Taking too long". So set the `WriteTimeout` of your `http.Server`
  above 10 seconds, or leave it unset: a shorter one closes the connection
  before that page arrives. For a shorter limit, set a deadline in the
  request's context: the dashboard answers with the same page then.

## Run it on its own

The `taskmill-ui` command runs the dashboard with no code to write. Each
[release](https://github.com/code19m/taskmill/releases) has it as binaries
for Linux, macOS, and Windows, and as a small container image:

```sh
docker run -p 8080:8080 \
  -e TASKMILL_UI_ADDR=:8080 \
  -e TASKMILL_DATABASE_URL=postgres://... \
  -e TASKMILL_UI_PASSWORD=... \
  ghcr.io/code19m/taskmill-ui:0.16.1
```

Or install it with Go: `go install github.com/code19m/taskmill/cmd/taskmill-ui@latest`.
It reads its settings from environment variables. A first setup needs these:

| Variable | What it sets | Default |
|---|---|---|
| `TASKMILL_DATABASE_URL` | The PostgreSQL connection string | None: required |
| `TASKMILL_SCHEMA` | The taskmill [schema](../concepts/glossary.md#schema) | `taskmill` |
| `TASKMILL_UI_ADDR` | The address to listen on | `localhost:8080`. In a container, set `:8080` |
| `TASKMILL_UI_PASSWORD` | The login password | None: anyone who reaches it can use it |

[The `taskmill-ui` reference](../reference/taskmill-ui.md) lists every
variable, with its values and its default.

- **Pin the image tag** to the taskmill version your apps run. A newer
  dashboard refuses to start on an older schema, with `ErrSchemaOutdated`.
  So upgrade the dashboard after your apps have migrated.
- It never changes the schema. Your apps create and migrate it.
- For probes, it answers `/livez` and `/readyz` with no login. See
  [Kubernetes](../production/kubernetes.md) for what each one checks.
- It exports no metrics. Export the queue gauges from your apps, with the
  `otel` module ([Logs, traces, and metrics](observability.md)). A metrics
  library in `taskmill-ui` would become a dependency of every app that uses
  taskmill.
- To check that a download is genuine, see [Security](../production/security.md).

## Protect it

> [!WARNING]
> The dashboard has **no login** by default. Anyone who reaches it can
> retry, cancel, and delete every task. Always put it behind a login.

Each setting means the same in both ways of running it:

| You want to… | In your app (`ui.Config`) | With `taskmill-ui` |
|---|---|---|
| Ask for a login | Your own login in front of it, or `BasicAuth` | `TASKMILL_UI_PASSWORD`, and `TASKMILL_UI_USERNAME` |
| Let people look, but not change anything | `ReadOnly: true` | `TASKMILL_UI_READ_ONLY=true` |
| Put it under a path that a proxy forwards, such as `/admin/tasks` | `Prefix` | `TASKMILL_UI_PREFIX` |
| Show it in another site's frame | `FrameAncestors` | `TASKMILL_UI_FRAME_ANCESTORS` |
| Log who did what | `Logger` | Logs go to standard error. With one shared login, they can't say who |

- **Use HTTPS with a password.** *Basic authentication* is a username and
  password that the browser asks for. It sends the password with every
  request, and knows only one user. For single sign-on, put your own login
  in front.
- **Read-only** hides every action and refuses it. To let some people act
  and others only look, mount two handlers, one with `ReadOnly`, and pick
  one per request.
- **Every action is logged** to `Logger` at Info level. Use a log handler
  that adds the signed-in user, and you know who did it.
- **Forms are protected** against cross-site request forgery: another site
  can't make a signed-in browser click a button for it.

## Show it inside another app

Browsers refuse to show the dashboard in a frame. This protects it from
*clickjacking*: a site that hides it under its own buttons. To allow a
frame, list the origins of the pages that may show it:

```go
dashboard, err := ui.New(client, &ui.Config{
    Prefix:         "/taskmill",
    FrameAncestors: []string{"https://admin.example.com"}, // or "'self'" for your own pages
})
```

To match your app's theme, add `?theme=light`, `?theme=dark`, or
`?theme=system` to the frame's address, such as
`https://shop.example.com/taskmill/?theme=dark`. The browser remembers the
theme for the pages that the reader opens next. Some browsers block storage
in a frame of another site: there, only the page with the parameter has the
theme, and the next pages follow the device.

## What people can do in it

| Page | What you see | What you can do |
|---|---|---|
| **Overview** | Task counts per queue and state, a dial of the attempts a minute, charts of tasks and attempts over the last 1, 6, or 24 hours, the number of workers, and the retention periods | Pause and resume a queue |
| **Tasks** | Tasks filtered by state, queue, kind, FIFO group, and time: when they were created, or for dead and canceled tasks when they finished | Retry, cancel, or delete every task that matches the filters |
| **A task** | Its payload, metadata, error history, and details, with a timeline: when it was created, due, started, and finished, and how long it waited and ran, which counts each second while it waits or runs. A banner says when tasks of its [FIFO group](ordering.md) wait behind it, or when it waits in one | Retry, cancel, or delete it. Open its group |
| **Schedules** | Each schedule, with its timetable in words and its next and last run | Pause, resume, or run it now |
| **Workers** | Each worker, with its host, version, queues, kinds, and schedules, which one is the [maintainer](../concepts/glossary.md#maintainer), and which ones define other schedules than the maintainer | Nothing: this page is for looking |

[Dashboard pages](../reference/dashboard-pages.md) says what each part of
these pages shows: the charts and the dial, the icons of the task list, and
the columns of the workers.

Retry, cancel, delete, pause, and run now ask first, and you confirm: in a
dialog, or without JavaScript, on a page of their own. Resume does not ask.
A deleted task leads to the list of its queue and state, such as the dead
tasks of its queue.

Before an action on many tasks, the dialog shows the filters, the range,
and how many tasks will change. With very many tasks, it works in steps,
and offers to continue where it stopped. If some tasks were busy at that
moment, it says how many, and offers to try them again.

Every task list has a filter by time: **Finished** on a list of only dead
or canceled tasks, and **Created** on any other. It shows a range of time:

| You want… | Do this |
|---|---|
| The tasks of the last hours or days | Choose a preset, such as **Last 6 hours**. The list starts at that time and has no end, so it shows new tasks as they come |
| Whole days, or a precise period such as last night | Click the first and the last day in the calendar: **From** and **To** show the range, from 00:00 to 23:59. Type other times, or dates, if you need them, or empty one for a range with one end. Then choose **Apply**. The range holds the minute of **To**: in the address, `before` is the minute after it |
| Every task again | Choose **Any time** |

The dashboard shows times in your time zone, and names the zone: the range
on the button, the calendar's fields, and the exact time in a tooltip.
Without JavaScript, the browser's own fields take the range, in UTC. The
address holds instants, `since` and `before`, so a link shows the same tasks
to everyone ([Dashboard routes](../reference/taskmill-ui.md#dashboard-routes)
has the forms of a time). With a range, **Retry all**, **Cancel all**, and
**Delete all** act on the tasks of the range only, and the dialog names it:
filter by *dead* and the hours of an incident, and **Retry all** leaves the
tasks that died before it. The range is of the time that the button names:
change the states, and it can become one of the other time.

A task list has 5 seconds to read its tasks. If it runs out of time, the
page says "Taking too long", and what may help: a queue and a state in the
filters, or fewer filters. The doc comment of
[`Client.List`](https://pkg.go.dev/github.com/code19m/taskmill#Client.List)
says which lists read more than they show.

A list that names a queue and no FIFO group reads only that queue's tasks.
It is in creation order, unless it holds only dead or canceled tasks. In
creation order, with a state or a kind in the filters, the list reads the
queue's tasks from the newest until the page is full: for a state with few
tasks, all of a backlog in that queue.

The links to a FIFO group, and to the tasks of a schedule's or a worker's
kind, select the [unfinished](../concepts/glossary.md#finished) and the dead
tasks: every state but completed and canceled. So they read no history;
choose those states in the filter to see them.

A retry of a canceled task takes the task's
[dedupe key](../concepts/glossary.md#dedupe-key) back. If another task holds
that key now, the dashboard does not retry the task, and says which task
holds the key. A retry of many tasks says how many it left for that reason
([Avoid duplicate tasks](deduplication.md#retry-a-canceled-task-that-has-a-key)).

The dashboard has light and dark themes, works on phones, and works without
JavaScript. It can't change a queue's `MaxRunning` or the retention times:
do that [from code](operations-api.md).

## See also

- [One dashboard for many services](../recipes/one-dashboard-for-many-services.md): forward paths, viewers and operators, who did what
- [Kubernetes](../production/kubernetes.md): a Deployment for `taskmill-ui`
- [Package `ui`](https://pkg.go.dev/github.com/code19m/taskmill/ui) and [command `taskmill-ui`](https://pkg.go.dev/github.com/code19m/taskmill/cmd/taskmill-ui) on pkg.go.dev

**Next:** [Manage tasks from code](operations-api.md)
