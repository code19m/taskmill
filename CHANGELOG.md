# Changelog

All notable changes to taskmill are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and taskmill uses
[Semantic Versioning](https://semver.org/) as described in
[Compatibility](docs/reference/compatibility.md).

## [Unreleased]

### Changed

- **Breaking:** `Client.Count` takes `*CountParams`, not a bare
  `*TaskFilter`, as every other read and action takes params: an option
  such as a time range can then be added later without breaking callers.
  To migrate, write `client.Count(ctx, &taskmill.CountParams{Filter: f})`
  for `client.Count(ctx, &f)`. Nil still counts every task.
- A handler that ignores its context no longer keeps its task for good.
  Its timeout, a cancel, or a delete changed nothing, and nothing was
  logged. Now, 5 seconds after its context was canceled, the worker
  abandons the attempt, logs `handler ignored its context; abandoning its
  attempt`, and ends it as the cause says: a timeout fails it with the code
  `timeout`, so the task is retried, and a cancel cancels the task. The
  handler's goroutine keeps its slot until it returns, and may still finish
  its work later, while the task already runs again: at-least-once delivery
  allows that. A batch is abandoned only after its `Timeout`, which alone
  ends its context. In a queue with a `MaxRunning` limit nothing is
  abandoned, so that the limit holds: the task stays running until the
  handler returns, and the worker logs a warning.
- `Run` returns at most about `DrainTimeout` plus 10 seconds after its
  context ended, also when the database does not answer. Each step after
  the drain had 30 seconds of its own, so with an unreachable database
  `Run` took a minute, and the platform killed the process. Now the steps
  share 5 seconds: a result that cannot be recorded by then is rescued once
  its lease runs out, and its task runs again.
- Each filter of `TaskFilter`, and `ActivityParams.Queues`, takes at most
  100 values, as `GroupParams.Queues` did: a call with more returns an error
  that wraps `ErrInvalidArgument`, not a statement with an array of any
  size. The dashboard sends at most 100.
- A fetch from a queue with a `MaxRunning` limit holds the queue's row for
  one round trip, not four: the worker sends the transaction in one batch,
  up to its commit. With 4 workers of 16 slots, a limit of 32, and a network
  of 1 ms, such a queue runs about 4,400 tasks a second, where it ran 1,500;
  with 4 ms, 2,000 where it ran 500. The limit stays exact.
- A worker warns of tasks of a kind that it has no handler for at most once
  a minute per kind, with the number of tasks, as `no handler for task kind;
  its tasks wait for a worker that has one`. It warned once per task, and a
  rolling deploy flooded the log. The line for each task is at Debug.

### Fixed

- A panic in an enqueue middleware, or in the encoding of a schedule's
  args, during a scheduled run no longer ends the maintainer's process, and
  the process of every maintainer elected after it. The pass fails, with
  the stack in the `run schedules` line, and the run stays due.
- The maintainer no longer stalls for about 15 minutes on a connection
  that stops answering without a reset, as in a failover that drops its
  packets. Promotions and heartbeats stopped while the election, on other
  connections, kept the role. Each statement of the maintainer's work now
  ends within its lease of 10 seconds, and the next try takes a new
  connection.
- A task's row that another transaction holds, such as an open `CancelTx`,
  no longer stalls every completion of the worker. The batch waited for the
  row, and after 30 seconds all its tasks were rescued as lost leases:
  tasks that had succeeded ran again, or became dead. The completion now
  skips held rows and tries them again shortly, and warns once of a task
  that waits for longer than its lease. The failure, snooze, or release of
  a grouped task waits for its group's row at most a second at a time, so
  its lease stays renewed, and its own error is recorded, not `lease_lost`.
- A handler that ends its goroutine with `runtime.Goexit`, as `t.Fatal`
  does in a test, no longer leaves its task running for good, its lease
  renewed by a goroutine that never stops. `RunTask` and `Run` give the
  task back to its queue. `RunTask` also bounds its last read of the task.
- The maintainer no longer logs the end of a difference between the
  workers' schedules that a comparison found after its term ended.
- `EnqueueTx` and `EnqueueManyTx` with an executor that is no transaction,
  such as `SQL(db)` or `Pgx(conn)`, no longer return an error after the
  tasks were stored, when the second read of a dedupe key fails: a retry
  stored the tasks without a key twice. Like `Enqueue`, they log the failed
  read and return the results.
- A nil pointer as `Args`, such as `var a *Receipt`, and a nil entry in
  `ClientConfig.EnqueueMiddleware` or `WorkerConfig.Middleware`, are errors
  that wrap `ErrInvalidArgument`. They panicked in every call.
- A queue whose settings are back to their defaults is no longer stored:
  `PauseQueue` and `ResumeQueue` of a mistyped name, or a `MaxRunning` set
  back to 0, kept the queue in `Client.Queues`, the dashboard, and the
  `otel` gauges for good.
- `Outbox.Store` rejects a topic or a header name that is not valid UTF-8
  or holds a NUL character. Such bytes silently became U+FFFD, so two
  topics became one, while their messages kept separate orders. The doc of
  `Message.Value` no longer says that the topic is stored in base64.
- `Client.Activity` returns an error that wraps `ErrInvalidArgument` for a
  `Since` that PostgreSQL cannot store, as the other reads do, not the
  database's error.
- A batch of `DeleteMatching` changes at most 1,000 tasks of both tables in
  all, as `BulkResult.Changed` promises. It took up to 1,000 of each.
- An error about a name that is too long quotes only its start: it quoted
  the whole value, of any size.
- Docs that were wrong or unclear. Retried tasks run before new work, as
  [Recover from an incident](docs/recipes/recover-from-an-incident.md) now
  says. The order of a list and what a page reads are in the doc comment of
  `Client.List` alone. A task takes about 420 bytes while it waits and 390
  in history, with a small payload. A task that loses its lease on its last
  attempt becomes dead, and the failover checklist looks for it. A new
  page, [Dashboard pages](docs/reference/dashboard-pages.md), says what each
  part of a dashboard page shows.

### Security

- A payload that does not decode no longer puts its values in the task's
  errors, the log, and the span's status. `encoding/json` quotes numbers
  and characters that it rejects, and an `UnmarshalJSON` method may quote
  anything. The error now names the field, with `[key]` in place of a
  map's key, and the types, the place of a syntax error, or only the `Args`
  type, with the code `invalid_payload` as before.

## [0.13.0] - 2026-11-21

### Added

- Docs for each architecture: a new page, "Fit taskmill to your
  architecture", says where taskmill's parts go in a monolith, a modular
  monolith, services, serverless functions, several regions, and an app with
  many tenants. New recipes show how to organize tasks by module and how to
  work with other languages, with example programs that the tests run. The
  guides add GORM and ent to the transactions table, serverless platforms to
  Deploys, several regions to Backups, failover, and replicas, and tenants
  to the fair-share recipe. Split work across services adds the contract,
  versions, traces, and tests between services.
- The maintainer says when the workers' schedules differ. A worker that
  lacks a schedule stopped it silently while it was the maintainer, and one
  that defined it in another way changed it. Now the maintainer compares its
  schedules with those of the other workers that can become the maintainer:
  their names, and what each one enqueues and when. When they keep differing
  for longer than a rolling deploy takes, it logs an error that names the
  workers and the schedules, again now and then while they differ, and a
  line when they match again.
  `WorkerInfo.Schedules` lists a worker's schedules, and the dashboard's
  Workers page lists them too, and marks a worker whose schedule names
  differ from the maintainer's. Migration 8 adds two columns, and a check,
  to the small workers table.

### Changed

- **Breaking:** `TaskInfo.FinalizedAt` is now `TaskInfo.FinishedAt`, and
  so `Task.FinalizedAt`, which `Task` embeds, is now `Task.FinishedAt`: the
  name that `State.Finished` and `ListParams.FinishedSince` use. Rename the
  field where you read or set it; its value is the same.
- The dashboard's tooltips take the colors of the theme: a raised surface
  with the edge and the shadow of a menu, where they were inverted.

## [0.12.2] - 2026-11-20

### Changed

- The dashboard's status pages stand in the middle of the page: a picture,
  a title that says what happened, what it means, and the way on, "Try
  again" first for a failure. A page that failed tries again by itself, 5,
  10, 20, and 40 seconds apart and then once a minute, says when in a quiet
  note, and shows the page once the server answers. An action is never sent
  again: its page tries the page that the action was sent from.
- When the dashboard cannot connect to its database, a page says "Can’t
  reach the database" and answers 503 with `Retry-After`, where it said
  "Something went wrong" with 500.
- A page that gets no answer at all, because the network or the dashboard
  is down, or because a proxy answers in its place, says "Can’t reach the
  dashboard" in place, and tries again by itself, where the browser showed
  its own error page, which stayed.
- An action that gets no answer says what may have happened: from a device
  that is offline, it was not sent; when the session ended, it did not
  apply; otherwise it may have applied, and the page shows what is true as
  soon as the dashboard answers.
- Empty lists say what is missing and why, with a way on: a task list that
  its filters leave empty offers to clear them. A missing task, an address
  of no page, and a refused value have pages of the same kind, which name
  what happened in plain words.
- The script gives up on a request after 15 seconds, where it gave up after
  10, the server's own limit, and missed its "Taking too long" page.
- On a phone, the Live button says "Reconnecting", "Offline", or "Signed
  out" in words too.

- In a task's timeline, the line between two steps says what the task did:
  dashed while it waited, solid while an attempt ran, green when it
  completed, red when it failed, blue while it runs. The words, such as
  "Waited" or "Failed after", stand under the step's name, and the length
  of time stands with the times, so every number reads down one edge. A
  length below a second is in milliseconds, such as "35ms", where it said
  "<0.1s".

### Fixed

- A click on a link of a dashboard that fails costs one request, where it
  cost two: the script loaded the address again after the page that said
  the request failed. A failure page also showed "Live", and stayed after
  the database came back.
- The back and forward buttons no longer stop with a script error, which
  left the buttons of an action that was on its way disabled.
- A task's errors say where the task was retried. A retry starts the count
  of attempts again, so a task that died, was retried, and died again
  listed "Attempt 1" twice, as if one attempt had failed twice. A line
  between the two now says "Retried: the attempts started again".
- Pointing at a FIFO group key that the outbox made showed two tooltips
  with two texts: the dashboard's, and the browser's for the cell around
  it. The dashboard now holds the titles around the element too, so only
  its own shows, and the key has one text everywhere: "Outbox topic
  orders, key order-13".

## [0.12.1] - 2026-11-20

### Added

- A task's page in the dashboard has a timeline under its details, in
  place of its four times: when the task was created, due, started, and
  finished, with the time between two steps, such as "waited 0.2s", "ran
  3s", or "failed after 2s", and what comes next, such as its next attempt.
  How long a task has waited or run so far counts each second, by the
  server's clock, and a task that waits in a FIFO group names the task
  before it. The timeline shows what the task's row keeps: its last attempt
  and one due time. So it leaves out what the row cannot tell, such as
  whether a due time came from a retry or a snooze.

### Changed

- The dashboard says a time ahead as "in 3m", where it said "3m from now",
  and the task list's "Run at" column is "Due".
- On a task's page, the two columns end on one line: when the payload and
  the errors are shorter than the details, the last card grows to match,
  and the note of a task with no errors fills it.

### Fixed

- On a task's page, the meter of the attempts draws a short bar for a few
  attempts of many, where it drew a dot, and a kind as long as
  `taskmill.outbox.message` fits on one line.

## [0.12.0] - 2026-11-20

### Added

- Every task list of the dashboard has a filter by time, where only the
  lists of dead and canceled tasks had one. On those lists it is
  "Finished", as before. On every other list it is "Created", and shows the
  tasks that were created in a range of time, with the same presets, the
  same calendar, and the same fields. The button names the time. With a
  range of creation times, the list comes in the order of that time, the
  newest first, and the arrow in the header marks the "Created" column.
- `ListParams.CreatedSince` and `ListParams.CreatedBefore` limit
  `Client.List` to the tasks created in a range of time: at `CreatedSince`
  or later, and before `CreatedBefore`. Any list takes the range, whatever
  its states, the completed tasks too, and is then in creation order: by
  `CreatedAt`, the latest first, and then by ID. That is a third order of
  `List`, with cursors of its own. IDs follow creation times only roughly,
  because a task of a long transaction has an old creation time and a new
  ID: so the range is not a filter of the list in ID order. A page of one
  queue reads about the tasks it returns, wherever the range lies and
  however long it is. With no queue named, it reads up to a page of each
  queue. A list takes a range of creation times or one of finish times:
  both together return an error that wraps `ErrInvalidArgument`.
- Migration 7 adds the two indexes that the range reads: `tasks_created_idx`
  and `task_history_created_idx`, both on the queue, the creation time, and
  the ID. A task writes one more index entry when it is enqueued, and one
  more when it completes: about 18% more WAL for a task with a small
  payload. A fetch writes none, because the index of `tasks` leaves out the
  running ones. **The migration blocks writes while it builds the indexes:**
  enqueues, fetches, and completions wait, about half a second for each
  million tasks. Reads continue. On a large schema, migrate while the
  queues are quiet. [Lists by creation time](docs/design/schema-creation-order.md)
  has the measurements, and the options that we rejected.

### Changed

- In the dashboard, the range in the address of a task list, `since` and
  `before`, is of the time that orders the list: of finish times on a list
  of only dead and canceled tasks, and of creation times on any other. So
  a range stays when you change the states, where a list with another
  state dropped it and led to itself without it.

- The dashboard's main buttons are neutral, where v0.11.0 made them indigo.
  The main action of a page or a dialog has the surface of the other
  buttons, a stronger border, and bolder text. In a dialog, Cancel has no
  surface, so the action stands out. The current page's entry in the menu
  is bolder, and its icon is no longer indigo. Checked boxes are in the text
  color, and the loading bar and the meter of a task's attempts are gray.
  The indigo accent stays only for the focus ring and for the needle and
  the arc of the dial.
- On the dashboard's overview, the count of the schedules stands with the
  other counters, in one row of six, and its tooltip says when the next
  schedule runs. The retention periods are one quiet line under the queues:
  they are settings, and stood there as two large numbers.
  When the maintainer last counted the tasks stands at the end of the top
  bar again, as "Counted 4s ago" beside a clock: quiet while it is fresh,
  and amber once it is over a minute old. It was behind a hint beside the
  title. A count that stopped at its cap says so in its tooltip.
- The dashboard's relative times keep counting between updates: "Counted
  3s ago" becomes 4s, 5s, and 6s, second by second, where it stood still
  until the next update and then jumped. They count by the server's clock,
  so a device whose clock is wrong shows the same times, and no time jumps
  when an update arrives. A page that the back button brings back shows its
  times as they are now. The times also count while live updates are paused
  or the server is out of reach. They rest while the tab is hidden or text
  is selected, and then catch up. The server reads both times to the second
  before it words one, as a clock shows them, so every time of a page
  changes on the same second. Without JavaScript, a time stands until the
  page loads again, as before.
- **Retry all**, **Cancel all**, and **Delete all** on a task list ask in a
  dialog over the list, where each led to a page of its own. The dialog
  shows how many tasks the action will change, the filters that they match,
  and what the action does. With very many tasks, it stays open for each
  step: it says how many tasks are done so far, and offers to continue, or
  to try again for the tasks that were busy. It also says so if an action
  failed, or if its answer did not arrive. When the action is done, the
  dialog closes, and the list shows the result. Escape and **Cancel** close
  it, and it adds no entry to the browser's history. Without JavaScript,
  and at the page's own address, the page asks as before.
- A FIFO group key that the outbox made shows as its topic and its key, as
  "orders › order-13": in the group filter and on its button, beside the
  queue in a task list, on a task's page, and where an action on many tasks
  lists its filters. It showed as "orders␟order-13", because the outbox
  joins the two with a character that no font draws. The tooltip says
  "Outbox topic orders, key order-13". The filter and the links still use
  the key itself, and any other key shows as before.
- The dashboard's Workers page shows each worker in one line. The Queues
  column has the first queues as text, as many as fit, and a count of the
  rest: "default, emails, images +3". The Kinds column has a count. Press a
  count to list all the queues and kinds of the worker beside it, each a
  link to its tasks. The list opens over the table: the kinds no longer
  open a row under the worker's row. It stays open while the page updates,
  beside its worker, and closes on Escape or a press elsewhere. A browser
  without popovers shows the counts as text.
- On the Workers page, the Host column has more room, the "stale" mark
  stands after the worker's ID, and the texts and marks of a row stand on
  one baseline: a mark no longer makes its row taller. A worker built from
  taskmill's own tree shows "local build" as its version, where it showed
  Go's "(devel)": its tooltip says that it was built from taskmill's source,
  not from a release.
- The dashboard's **Finished** filter has a calendar of its own, in place
  of the browser's date pickers. Click the first and the last day of a
  range: the calendar marks the days, and **From** and **To** show them,
  from 00:00 to 23:59. Type a date as `2026-11-19` and a time as `14:05`
  to refine a bound, or empty it. **To** is now the last minute in the
  range, so a range to 19 November 23:59 holds every task of that day. The
  address has not changed: its `before` is the minute after **To**. The
  button words the range in the same way, and says "Until" for a range
  with only an end. The calendar works from the keyboard, with the arrow
  keys, Home, End, Page Up, and Page Down, and it names every day for a
  screen reader. It offers no day after today, and its week starts on
  Monday, or on the first day that the browser's language has. Without the
  script, the browser's own fields remain, in UTC: **From** and **Before**.
- The dashboard shows a tooltip right away, where the browser showed it
  after about a second. Point at a time such as "4s ago" to read the exact
  time in your time zone, and at text that ends in "…" to read the whole
  text, also where such text had no tooltip, as a long value on the button
  of a filter. A tooltip shows too while its element has the keyboard's
  focus, and it goes with Escape. On a touch screen, tap a time, or text
  that is cut short: the tooltip stays until the next tap. A tap on a link,
  a button, or a row of a task list still leads where it led. A tooltip that
  would only repeat text that the page shows whole no longer appears. On
  the overview, the age of the counts in their hint and the next run of the
  schedules have their exact time as well. Without JavaScript, the browser
  shows the tooltips as before.

## [0.11.1] - 2026-11-19

### Fixed

- A test that uses `taskmilltest` no longer fails at its end with
  `deadlock detected` while `NewClient` drops the test's schema. A worker
  that stops gives up the statements that it had sent, and the database
  could still run one of them when the drop began: the two waited for each
  other's locks, and PostgreSQL ended the drop. `NewClient` now waits until
  the pool has closed the connections of such statements, for up to a
  second, before it drops the schema. Where that wait cannot tell, through
  a proxy such as PgBouncer or while other tests use the same pool, it
  tries a drop again that PostgreSQL ended as a deadlock.
- After a worker stopped, it could happen that no worker led for up to 10
  seconds: no scheduled task or retry was promoted, no task whose lease had
  run out was rescued, and no schedule ran. A worker that stops gives up
  the statements that it has sent, and resigns as maintainer. If one of
  those statements was its election, the database could still run it after
  the resignation, and give the lease to the worker that had stopped. Now a
  worker that stops waits for the answer to its election, for up to 5
  seconds, before it resigns.

## [0.11.0] - 2026-11-19

### Added

- The dashboard's overview has a dial of the attempts a minute, first in
  the Activity card. It counts completed and failed attempts together, in
  the newest step of the chosen period whose count is complete. Its scale
  ends at the smallest round number that holds the highest rate of the
  period, so the needle reads against the busiest step of the last 1, 6,
  or 24 hours. With the script, the needle glides to each new value, unless
  the device asks for reduced motion.
- The overview's charts show the values of one step. Point at a chart, or
  press and drag on a touch screen: a guide marks the step, and a note
  beside it lists the value of each series, and the step's time on your
  clock, with its time zone. From the keyboard, move to a chart with Tab:
  it is a slider, on the newest step. The arrow keys, Home, and End move
  it, and Escape hides the values. A screen reader reads the values of
  each step as the slider's value. While a chart has the focus, the page
  waits with its updates, so that the steps do not change under you.
  Without the script, a chart is a picture with a name, and the tooltip of
  a step lists its values, as before.
- The dashboard's Schedules page says each schedule's cron expression in
  words, above the expression: "Every 15 minutes", "Weekdays at 09:00", or
  "On the 1st of every month at 03:00". The times are in the schedule's time
  zone, which stays beside the expression. The words come from the parser
  that runs the schedules, and they are there only when they say exactly
  what the expression does, in a few words. Any other expression shows
  alone, as before: one with both a day of the month and a day of the week,
  a list of more than three times or days, or a step such as `*/7`, which
  starts again at every full hour and so is not "every 7 minutes".
- The dashboard's lists of dead and canceled tasks have a filter by finish
  time, **Finished**. Its presets show the tasks of the last hour, 6 hours,
  24 hours, 7 days, or 30 days: each is a link from a fixed time on, with
  no end, so the list still shows new tasks as they finish. For a precise
  or a long period, its **From** and **To** fields take a date and a time
  each, with the browser's own picker. The address holds the range as
  instants, in `since` and `before`, so a link shows the same tasks to
  everyone: write a time with `Z` or an offset, or without a zone, which is
  UTC. "Load more" and the later pages keep the range. While a range is
  set, the list hides "Retry all", "Cancel all", and "Delete all", which
  would also change the tasks outside the range. A list with another state
  takes no range, and leads to itself without it. The filter works without
  the script, in UTC.
- `ListParams.FinishedSince` and `ListParams.FinishedBefore` limit
  `Client.List` to the dead and canceled tasks that finished in a range of
  time: at `FinishedSince` or later, and before `FinishedBefore`. So you
  reach a past period, such as last night, without paging down to it. A
  page reads only tasks of the range, also with a kind or a FIFO group in
  the filter. The bounds are instants: neither their location nor the
  database's time zone changes what a range selects. Only a list of dead
  tasks, canceled tasks, or both takes a range, because only that list is
  in finish order: any other list returns an error that wraps
  `ErrInvalidArgument`. The range needs no migration and no new index.

### Changed

- The dashboard's task list has a "Load more" row under it, in place of
  the link to the next page. It shows the same list with 50 more tasks: the
  tasks that you saw stay where they are, and the live updates keep all of them
  up to date. Without the script, the browser loads the longer list, and
  opens it at the first task that the row added. The address holds how
  many tasks the list shows, as `limit`, so a reload shows as many. A list
  shows up to 250 tasks, because every update reads them all. After that,
  it continues from its last task on a page of its own, as before. Under
  every list that has tasks, the page also says how many it shows, and
  tells a screen reader the new number after "Load more" or a change of
  the filters. If the filters match no task, it tells the screen reader
  that.
- With the script, the dashboard shows exact times in the reader's time
  zone, where it showed them in UTC: the time in a tooltip, such as
  "2026-11-19 19:05:07.089 UTC+5", and the range and the fields of the new
  filter by finish time. Each time names its zone by its distance from UTC
  at that time, so the times around a change of the clocks stay apart.
  Without the script, the times are in UTC, as before.
- The dashboard's pages show data, and keep what explains it out of the
  way:
  - **Hints.** A small button with an "i" shows a short text about what is
    beside it. It works with a mouse, a keyboard, and a finger, and without
    the script. The **Overview** has one beside its title, in place of
    "counted 4s ago": it says when the maintainer last counted the tasks,
    and what a count that ends in "+" means. If the last count is over a
    minute old, the page says so beside the title, in amber: no worker
    runs as the maintainer, or it cannot count. A chart with nothing to
    draw says why behind a hint beside its title, where the text stood
    over the plot. A browser without popovers cannot open a hint: there,
    the text stands on the page, in small gray type.
  - **Overview.** The Workers card no longer names the maintainer: the
    **Workers** page marks it, now also beside a long worker ID, which
    used to push the mark out of view. The Retention card shows both
    periods as values, each with the tasks that it holds for. The count
    of the queues stands beside the title of their card, as the counts of
    the schedules and of the workers stand beside theirs, and so does the
    count of a task's errors. On a screen of 600 pixels or less, the
    Workers card is as wide as the page: it stood alone in half a row.
  - **Workers.** The **Worker** column shows the part of a worker's ID
    after its host name: the process ID and the random suffix, which tell
    the workers of one host apart. The **Host** column has the name. A
    long host name cut the ID short, so that two workers of one host read
    the same. The tooltip has the whole ID, and a screen reader reads it
    whole.
  - **Task lists.** An arrow in the header marks the column that orders
    the list, **ID** or **Finished**, in place of the words "latest first"
    beside the title. A list that starts after other tasks has a row at
    its top that leads back to the first page, in place of the words
    "older" or "earlier" beside the title and a link under the list. A
    page after the last task says "No more tasks", where it said "No tasks
    yet".
  - **Details.** The filter buttons are as tall as the buttons beside
    them. A count of zero in the queue table is easier to read: its gray
    had too little contrast. The counts beside headings and in the menu
    are in the text font: the monospace font is for names from code.
- The dashboard has a new look, in the same style as before:
  - **One accent color, indigo.** The main button of a page or a dialog is
    indigo with white text in both themes, where it was black or white.
    Checked boxes, focus rings, and the icon of the current page use the
    same color. The state colors are as they were.
  - **Buttons.** They have corners of 8 pixels in place of round ends, one
    height, and an icon each. A button that deletes has red text, and a
    red fill only where it confirms.
  - **Text** is a little bolder, and the title of a page is larger.
  - **The light theme** has white cards on a light gray page.
  - **Task lists** have denser rows. A task's ID is in the text font, and
    bold. The last error is quiet gray text. Of the states, only `dead` and
    `retryable` are in color, red and amber. In a window of 1200 pixels or
    wider, the header of a list stays in view while the page scrolls.
  - **A priority badge** has no state color: amber and red are for the
    states `retryable` and `dead`, and a running task of critical priority
    showed the red of a dead one. Now `high` is bolder than `low`, and
    `critical` is white on black, or black on white in the dark theme.
  - **Amber text in the light theme** is a darker shade. The old one had
    too little contrast on a row under the pointer, and in the mark of
    counts that are late.
  - **The Live button's dot** pulses while updates run. It is still when
    updates are paused or late, on a page that does not update itself,
    while the updates wait because you are using the page, and for a
    reader who asked the system for less motion.
  - **The logo** is a mill wheel beside the name in lowercase, in the page
    and in the icon of the browser tab.

### Fixed

- In the dashboard, a live update no longer takes the focus from a list
  whose rows change. When a row left a list, as a task does that is
  retried or deleted, the update moved every row below it: in a list of
  250 tasks, up to 249 rows, and a link in one of them lost the focus. Now
  an update touches only the row that comes, goes, or moves.
- The dashboard's live updates no longer stop after a right-click. On
  macOS and Linux, the page waited for the end of a press that the context
  menu had taken, and showed "Live" while nothing updated, until the next
  click. A press now also ends when the menu opens, or when another window
  comes to the front.
- Pause and Resume on the dashboard's overview keep the period of the
  charts. On the overview of the last 6 or 24 hours, they led back to the
  last hour.
- With the keyboard, the focus no longer goes under the dashboard's header.
  Shift+Tab to a link that stood under the header, or under the header of
  a task list, left it there, out of sight. The browser now scrolls it
  into view. So does a link to a place on a page, such as the one that
  "Load more" uses without the script.
- The dashboard's **Attempts** chart no longer ends in a dip at the start
  of every step. Workers write their counts every 10 seconds, and the
  chart drew a step as soon as it ended, before its count was complete. It
  now draws a step 15 seconds after it ends, as the dial reads it.
- On a touch screen, the dashboard's theme buttons and the period links of
  its charts take a touch 44 pixels tall and wide, as its other buttons
  do. They took one of about 25 pixels.
- In the dashboard's task list, the priority badge of a task stood right
  against its state, a little lower than the text, and made its row taller
  than the others. It now stands apart from the state, in line with it, and
  every row has the same height. The State column is wider, so that the
  badge also fits beside a long state such as `scheduled`. On a screen of
  600 pixels or less, where the column is narrow, the badge stands under
  the state.
- On a screen of 600 pixels or less, the dashboard's task list cut an ID of
  five digits or more short, so that every row began with the same digits.
  The column now has room for six digits.
- taskmill works the same in every time zone of the database's sessions.
  Three statements read that zone.
  [Use any time zone](docs/production/database.md#use-any-time-zone) says
  where a time zone shows.
  - **A schedule that is gone from the code** is kept for 24 hours after a
    maintainer last defined it. The maintainer counted a day of the
    session's time zone. So on a day when that zone's clocks change, it
    deleted the stored schedule, with its pause, an hour early or an hour
    late.
  - **The time of an entry in a task's error history** is stored in UTC,
    such as `2026-11-19T10:24:12.923859Z`. It was stored with the offset
    of the session's time zone, so one moment read differently for each
    writer, and `AttemptError.At` had that offset as its location. The
    entries that are stored already stay as they are, and read as the same
    moments. PostgreSQL also accepts a zone that is more than a day from
    UTC, which Go cannot read: `TaskInfo.Errors` left out the entries of
    such a session.
  - **The history partitions** are prepared also in a session whose time
    zone leaves out a calendar day, as `Pacific/Apia` left out 30 December
    2011. In the days before such a day, each pass failed with `relation
    "task_history_…" already exists` and prepared no partition. So the
    tasks that completed in the first minutes of the day after it skipped
    history.

## [0.10.0] - 2026-11-19

### Changed

- The dashboard moves between its pages without loading each one anew. A
  link, a row of the task list, and the back and forward buttons show the
  next page in place: only the content changes, and a page that you return
  to is where you left it. Actions such as Retry, Pause, and the bulk
  actions run in place too, and show their notice without a reload. An
  action whose answer does not arrive now says so in a notice. The server
  sends the same pages as before: every page still works without the script,
  and whatever the script cannot show in place, the browser loads as a full
  page.

## [0.9.0] - 2026-11-18

### Changed

- The decision records in `docs/adr/` are gone. The design pages now hold
  what they said: each decision is on the page of the mechanism that it
  explains, with its reasons and the options that were rejected. Start at
  [How taskmill is built](docs/design/README.md). Two design pages were
  split by topic on the way: lists and counts left "Lists and operator
  actions", and the queue statistics left "Maintenance and notifications".
  A link to an old record, or to a section that moved, no longer works. No
  behavior changes.
- The comment at the top of migration `0004_stuck_work.sql` names the
  design page, and no longer a decision record that is gone. Its
  statements are the same, and `Migrate` applies migrations by their
  numbers, so it sees no change. If you apply the migrations with a tool
  that stores a checksum of each file, such as Flyway or Atlas, the
  checksum of this one file changes: repair it with your tool.
- The dashboard's actions on queues and schedules have new addresses: the
  name is now in the query, not in the path. `POST /queues/pause?name=emails`
  replaces `POST /queues/emails/pause`, and `resume` changes the same way.
  So do `pause`, `resume`, and `run` under `/schedules`. The dashboard's
  own buttons use the new addresses. If a proxy allows these routes by
  their path, or a script of yours sends them, change it:
  [Dashboard routes](docs/reference/taskmill-ui.md#dashboard-routes) lists
  them. If your admin app logs the path of each action, log the query too:
  the path no longer names the queue or the schedule, as the recipe
  [One dashboard for many services](docs/recipes/one-dashboard-for-many-services.md)
  now shows. While both releases serve one dashboard during a rolling
  deploy, these buttons can answer "Not found": try again when the deploy
  is done.
- The doc comment of `outbox.Publisher` no longer says that a batch never
  holds two deliveries with the same topic and key. That is true only of
  deliveries that have a key. Deliveries without a key have no order to
  keep, so a batch can hold many of one topic. The relay does what it did
  before: if your publisher counts on one delivery per topic in a batch,
  check it for messages without a key.

### Fixed

- `Enqueue` and `EnqueueMany` no longer return an error after their tasks
  are stored. When an enqueue finds a dedupe key taken by a task that was
  committed at the same moment, it reads that task's ID in a second
  statement. If that read failed, because the context ended or the
  connection dropped, the call returned the error, although its own
  transaction had committed. A caller that then tried again stored every
  task without a dedupe key a second time. Now the call returns its
  results, and logs `find duplicates failed after the enqueue committed;
  the duplicates have no ID` at Warn. The result of such a duplicate has
  `Duplicate` set and the ID 0. It has the same when the task that took
  the key was canceled or deleted before the read, as the comment of
  `EnqueueResult.ID` now says. In your own transaction, `EnqueueTx` and
  `EnqueueManyTx` still return the error, so that you can roll back.
- `Count` no longer reports its `Cancel` count as exact after counting
  stopped at the cap. `Cancel` leaves out the running tasks that were
  already asked to stop, so it can count fewer tasks than the cap of
  10,000, and its `Capped` was then false. With 30,000 running tasks, every
  third of them asked to stop, it read as exactly 6,668, where
  `CancelMatching` changes 20,000, and the dashboard showed that number as
  exact. Now `Capped` is true whenever counting stopped at the cap.
- `QueueInfo.Lag` no longer reads 0 for a queue whose oldest due task is
  due for more than about 292 years, such as a task with a `RunAt` in the
  year 1700. Such a lag does not fit a `time.Duration`, and it wrapped
  around: a stuck queue showed no lag, also in the lag gauge of the `otel`
  package. The lag now stops at the longest duration.
- `Cancel`, `Retry`, and `Delete` now say in every error what they were
  doing. An error of the begin or the commit of their transaction, such as
  a context that ended, came back bare: `context canceled`. It now reads
  `taskmill: cancel task 7: context canceled`, like the errors of the other
  calls. `errors.Is` finds the same errors as before.
- `UpdateQueue` now rejects a `MaxRunning` above 2,147,483,647, the most
  that its column holds, with an error that wraps `ErrInvalidArgument`.
  Such a value failed before too, but with an error of the driver that read
  like a failure of the database.
- A schedule no longer loses its pause and its last run time in a deploy
  in which some workers do not define it. A maintainer deletes the stored
  schedules that it does not define, after a day. But the day counted from
  the schedule's last run, pause, or change, not from when a maintainer
  last defined it. So one retention pass of a maintainer without the
  schedule, as in a rolling deploy, a canary, or a rollback, deleted a
  schedule that was paused more than a day ago, or that runs once a week.
  The next maintainer that defined it stored it again, not paused, so it
  ran at its next time. Now each maintainer notes once an hour which
  schedules it defines, in the column `updated_at` of `schedules`, and a
  schedule is deleted only when no maintainer has defined it for about a
  day. This holds once a maintainer of this release that defines the
  schedule has applied retention, which it does every minute.
- A task that is due more than about 292 years from now no longer makes
  the maintainer look for due tasks 20 times a second, for good. `RunAt`
  accepts times up to the year 294276. While such a task was the next one
  due, the wait until then did not fit the type that held it, and on
  amd64 processors the maintainer slept its shortest time, 50
  milliseconds, between two looks. It now sleeps a second, as when no task
  is due soon.
- Two maintainers that refresh the queue statistics at the same time now
  both store their counts. Two workers can be the maintainer for a moment,
  for example after a lease ran out on a slow database. One refresh then
  failed with a duplicate key on `queue_stats_pkey` (SQLSTATE `23505`), and
  its worker logged `operation failed` for `refresh statistics`. The stored
  statistics were right all the same. A refresh now waits for the other.
- `Activity.Counts` holds the most tasks counted in each state during the
  step, as its comment says. Within a minute, each count replaced the one
  before, so a minute kept only its last count. A queue that filled and
  emptied again within a minute did not show in `Client.Activity`, nor in
  the dashboard's charts.
- `DrainTimeout` now counts from the moment `Run`'s context ends. It
  counted from the end of the fetches that were in flight then, and a fetch
  waits up to 30 seconds for a connection of the pool. So when the handlers
  worked on the worker's pool and held every connection of it, a stopping
  worker waited for its fetch, and the fetch waited for the handlers: no
  handler saw `ErrShutdown`, and no task went back to its queue, for up to
  30 seconds plus `DrainTimeout`. With the defaults, Kubernetes killed such
  a pod first, and its tasks came back only when their leases ran out, each
  with a failed attempt (`lease_lost`). Handlers now get `ErrShutdown` one
  `DrainTimeout` after the stop, whatever a fetch does then, and the tasks
  that a late fetch leased go back to their queue as before. You change
  nothing.
- A stopping worker now bounds the time in which it gives tasks back: 30
  seconds for all the tasks of a fetch that it could not start, and 30
  seconds for all the tasks whose handlers ignored the shutdown. Each task
  had 30 seconds of its own, one after the other, so a database that
  did not answer kept `Run` from returning for that long per task. A task
  that is not given back in time is rescued when its lease runs out, as
  after a crash.
- A statement that fails after it returned its row now counts as failed.
  An error comes that late only from a deferred constraint or trigger of
  your own, which fails at the commit, or from a cancel at the last
  moment. taskmill took the row for the result. So a worker counted a
  failed attempt, a snooze, or a release as recorded when the database had
  rolled it back, and no longer renewed the task's lease: the task came
  back only when the lease ran out, with a failed attempt (`lease_lost`).
  `RunTask` ran the handler of a task that it had not leased. Now the
  worker records the result again, as after any other error, and `RunTask`
  returns the error and runs nothing.
- The dashboard's Pause and Resume buttons of a queue, and the Pause,
  Resume, and Run now buttons of a schedule, failed when the name
  held a colon, as `emails:high` does. The page wrote `#ZgotmplZ` in place
  of the name into the form's address, and the answer was "Not found". For
  a queue or a schedule named `.` or `..`, the browser resolved the dots
  out of the address, with the same answer. The buttons now work for every
  name: a query can carry any text, and a path cannot.
- A dashboard that your app mounts answered with an empty page, status
  200, and no log line, when your middleware gave the request a shorter
  deadline than the dashboard's 10 seconds and the database did not answer
  before it. The dashboard took such a request for one whose client went
  away. A request that passes any deadline now gets 503 and the page
  "Taking too long", and logs `dashboard request timed out`. Only a
  canceled request gets no answer. The doc comment of `ui.New` and
  [The dashboard](docs/guides/dashboard.md#mount-it-in-your-app) now also
  say what the dashboard needs from your `http.Server`: a `WriteTimeout`
  above 10 seconds, or none. A shorter one closes the connection before
  the dashboard answers.
- A dashboard action with a value that is not valid now answers 400, on a
  page titled "Invalid request", as the task list does for the same value.
  This is a bulk action with an unknown state, with a cursor that its list
  did not make, or with no filter, and an action on a queue or a schedule
  with no valid name. It answered 409, "Not possible". That answer now
  means only that the action does not fit the task or the schedule as it
  is, such as a retry of a running task.
- Where the browser blocks storage, as some do in a frame of another site,
  the dashboard ignored `?theme=light` and `?theme=dark`, and followed the
  device. The parameter now sets the theme of its page without storage.
  The pages that the reader opens from there still follow the device:
  [The dashboard](docs/guides/dashboard.md#show-it-inside-another-app) now
  says so.
- In a filter of the dashboard's task list, a tick on an option while the
  menu's search box held text showed every option again, and dropped the
  options that the search had found on the server. The box kept its text.
  The menu now keeps showing what the search finds.
- The dashboard's Workers page now shows the word "stale" beside the ID of
  a stale worker. Only the color of its dot said so before.
- At phone width, the dashboard's Overview and Schedules pages scrolled
  sideways as a whole: a hidden label of the table's last column reached
  out of the table's own scroll area. Now only the table scrolls.
- `taskmill-ui` logs the lines of Go's HTTP server, such as a request
  handler that panicked, in the format of `TASKMILL_UI_LOG_FORMAT`, at
  Error. They were plain text lines, also among JSON lines.
- `examples/quickstart` now waits at most 10 seconds for open requests
  when it stops, and part 2 of the tutorial shows the same code. The
  program called `Shutdown` without a deadline. So one client that sent
  the start of an order and then stalled kept it from exiting after
  Ctrl-C, and a second Ctrl-C did nothing. If you copied that `defer` line
  into a program of your own, give its `Shutdown` a deadline, as the
  example now does.

### Security

- A flood of notifications can no longer make the workers run elections
  one after the other. PostgreSQL lets every role of a database send
  notifications, and each one that said that the maintainer resigned made
  every worker try to lead right away. So with thousands of them a second,
  the maintainer renewed its lease, rescued tasks, and loaded its schedules
  hundreds of times a second, and every other worker tried to take over as
  often. A worker that leads now ignores that notification, and another
  worker follows it at most once in 2 seconds. A maintainer that stops is
  still replaced right away.
- `taskmill-ui` could log a part of the database password when
  `TASKMILL_DATABASE_URL` was wrong. For a connection string that does not
  parse, it logged pgx's error, which quotes the string and masks only what
  it takes for the password: the rest of a password with a space, or with
  an `@` in a URL, stayed readable. And for a URL whose password holds an
  `@` or a `/` that is not percent-encoded, pgx takes the rest of the
  password for the host or the database, and the connection error showed
  them. The command now refuses both when it starts. Its error names the
  variable, and says nothing about the string. If your log has a line
  `taskmill-ui stopped` whose `error` starts with `cannot parse`, or names
  a host or a database that you do not know, change that password. In a
  URL, percent-encode the password: `@` is `%40`, and `/` is `%2F`.
  [Security](docs/production/security.md#keep-secrets-out-of-tasks-and-errors)
  says what the logs show.
- taskmill now requires pgx v5.13.0, which fixes a vulnerability in pgx:
  `Rows.Values` panicked on a row with fewer values than the server had
  declared
  ([GHSA-w9rp-vfrh-q8vv](https://github.com/jackc/pgx/security/advisories/GHSA-w9rp-vfrh-q8vv)).
  taskmill does not call `Rows.Values`. Your application may, so upgrade.
  pgx v5.13.0 also loads a client certificate from its files again for each
  new connection: if your code replaces the parsed `TLSConfig`, read the
  changes in [pgx's changelog](https://github.com/jackc/pgx/blob/master/CHANGELOG.md).

## [0.8.0] - 2026-11-16

### Changed

- **Breaking:** `AttemptError` has no JSON tags any more, like every other
  type that `Client` returns. If you encode a `TaskInfo` with
  `encoding/json`, the keys of each entry of its `Errors` change:
  `attempt`, `at`, `message`, and `code` are now `Attempt`, `At`,
  `Message`, and `Code`, and `Code` is written when it is empty too. No
  build fails, so look for each place where a task leaves your program as
  JSON, such as an admin endpoint that a page or a script reads. To
  migrate, encode a type of your own there, with the keys that your readers
  expect, as the recipe
  [One dashboard for many services](docs/recipes/one-dashboard-for-many-services.md)
  now does. Go code that decodes that JSON into `AttemptError` needs no
  change. The types that taskmill hands to your code are Go values, not a
  format for the network, and get no JSON tags and no marshal methods: see
  [Compatibility](docs/reference/compatibility.md). The stored tasks do
  not change.
- The doc comments of `BatchHandler` and `outbox.Publisher` now say plainly
  what a nil or short result does: a task or a delivery without an entry
  fails, and is retried. Nil never means that all succeeded. So a publisher
  that ends in `return nil` after it sent its deliveries sends them again
  at each retry: return `make([]error, len(deliveries))`. The rule is the
  same as before.
- A delivery for which a publisher returns no result now fails with the
  error `the publisher returned no result for this delivery`. Its error
  was `the batch handler returned no result for this task`, which names
  the relay inside taskmill, and not the publisher that you wrote.
- [The API's shape](docs/design/api-shape.md) records the shape of
  the API for v1.0.0, with the reasons: the two breaking changes of this
  release, and what stays as it is. `ui.New` keeps returning
  `http.Handler`, a batch result that is missing keeps failing its task,
  and `Middleware`, `AttemptError`, `QueueInfo.Counts`, `Activity.Counts`,
  and `GroupParams` keep their names.
- A schema migration, `0006_refused_rows`, adds `CHECK` constraints to the
  tables `workers`, `schedules`, and `settings`. They refuse the values
  that taskmill never writes and cannot read: an infinite time, a `NULL`
  among a worker's queues or kinds, a worker's lease below zero or above
  300 years, and a retention period below 1 hour or above 300 years. No
  field of a lease or of a retention period may be negative either:
  `6 days 23 hours` is valid, and `7 days -1 hour` is not. If SQL of your
  own writes such a value, it now gets a check violation (SQLSTATE 23514)
  that names the constraint. `UpdateSettings` always refused a period
  below 1 hour, so nothing changes for code that uses `Client`. The tables
  are internal: see [Compatibility](docs/reference/compatibility.md).
- The migration first repairs the rows that break the new rules, which
  only SQL by hand can have written. It deletes such a row of `workers`,
  and a worker that still runs registers again at its next check-in, with
  the Warn line `worker was removed as stale; registering again`. It
  pauses a schedule whose next run time is infinite: resume it on the
  dashboard, and it runs right away, once for the time of the upgrade, and
  once more if it missed a regular run since. It forgets a last run time
  that is infinite. It changes a retention period outside the range to the
  nearest one inside, so a period that you set below 1 hour with SQL
  becomes 1 hour, and writes a period with a negative field without one:
  `7 days -1 hour` becomes `6 days 23:00:00`. A repaired schedule or
  settings row has the time of the migration as its `updated_at`, until
  something changes the row again: the maintainer does that to each
  schedule that it defines after an hour. So this query, with your
  schema's name, shows what the upgrade repaired, right after it:

  ```sql
  SELECT 'schedule ' || s.name AS repaired
  FROM taskmill.schedules s, taskmill.migrations m
  WHERE m.version = 6 AND s.updated_at = m.applied_at
  UNION ALL
  SELECT 'settings'
  FROM taskmill.settings s, taskmill.migrations m
  WHERE m.version = 6 AND s.updated_at = m.applied_at;
  ```

  The migration locks each of the three tables for a moment, waits at most
  5 seconds for each lock, and reads a few pages. The previous release's
  code keeps working on the new schema.
- **Breaking:** `Client.List` returns a list of only dead tasks, only
  canceled tasks, or both in a new order: by when the tasks finished, the
  latest first, and by ID among tasks that finished at the same time. It
  returned them by ID, newest first, as it still returns every other list:
  one that mixes these states with others too. No index holds dead and
  canceled tasks in ID order. So a page of them read every newer task of
  any state, or every dead task, and reached its time limit of 5 seconds
  when the table was large, as in an incident. It now reads the index of
  finished tasks, and about the tasks it returns: on a table of 61,500
  tasks with 10,000 dead ones, 52 rows for a page of 50 dead tasks of one
  queue, where it read 11,565. There is no new index and no migration.
  [Lists and counts](docs/design/schema-lists.md)
  has the reasons, and
  [Manage tasks from code](docs/guides/operations-api.md#find-tasks) says
  when such a list reads more. To migrate:
  - If your code needs dead or canceled tasks in ID order, read every page
    and sort the tasks by `TaskInfo.ID`.
  - If your code read only what died since its last look, and stopped at
    the highest ID that it had seen: read from the first page to a little
    before the latest `TaskInfo.FinalizedAt` that you saw last time, and
    drop the IDs that you already have. A task's finish time is the start
    of the transaction that finished it, so a task can show up later with
    an earlier finish time than one that you have seen. How far back to
    read depends on how long such a transaction stays open: usually
    seconds for taskmill's own, and as long as yours when it calls
    `CancelTx`.
  - A cursor that the previous release returned for such a list is now an
    error that wraps `ErrInvalidArgument`: start again from the first
    page. The same holds while both releases run during a rolling deploy.
    A list in one order also refuses a cursor of the other, so pass
    `ListResult.Next` back with the filter that it came from.
- The dashboard's lists of dead tasks, of canceled tasks, and of both show
  the tasks that finished last first. They say "latest first" beside their
  title, show when each task finished in place of its run time, and name
  their page links "Latest" and "Earlier". The page for a task list that
  is refused is now titled "Invalid request", and says what is wrong: the
  filter, or the cursor. If the address has a cursor, as a bookmark from
  before the upgrade has, the page links to the first page of the same
  list. Before, it was titled "Invalid filter", and offered only to clear
  the filters.
- Four warnings in the log now use the words of the
  [glossary](docs/concepts/glossary.md): a lease runs out, another worker
  takes over, and a worker reads a queue. Their levels and attributes stay
  the same. If an alert rule, a saved search, or a metric made from logs
  matches the text of one of them, change the match when you upgrade:
  nothing reports a rule that stopped matching. Three of the four keep
  their first clause, so a match on `resign maintainer failed`,
  `record attempt failed`, or `complete tasks failed` keeps working. The
  line about the default queue has no such clause. While a rolling deploy
  runs, workers log both texts.
  - `resign maintainer failed; another worker leads when the lease expires`
    is now
    `resign maintainer failed; another worker takes over when the lease runs out`
  - `the worker does not serve the default queue of these registered kinds`
    is now
    `the worker does not read the default queue of these registered kinds`
  - `record attempt failed; the task is rescued when its lease expires`
    is now
    `record attempt failed; the task is rescued when its lease runs out`
  - `complete tasks failed; they are rescued when their leases expire`
    is now
    `complete tasks failed; they are rescued when their leases run out`

### Removed

- **Breaking:** `PgxQuerier` no longer has `QueryRow`, and `SQLQuerier` no
  longer has `QueryRowContext`. taskmill never called either of them on a
  value that you passed, and from v1.0.0 on an interface keeps its methods
  for good. Nothing changes for code that passes a pgx or `database/sql`
  value to `taskmill.Pgx` or `taskmill.SQL`: a `pgx.Tx`, a `*pgx.Conn`, a
  `*pgxpool.Pool`, a `*sql.Tx`, a `*sql.Conn`, a `*sql.DB`, or a type that
  embeds one of them, such as `*sqlx.Tx`. A type of your own that
  implements one of the interfaces, such as a wrapper that traces
  statements, may now leave the method out: it needs `Exec` and `Query`, or
  `ExecContext` and `QueryContext`. `CancelTx` and `CompleteTx` ask for a
  transaction as before. What stops building is code that uses
  `taskmill.PgxQuerier` or `taskmill.SQLQuerier` as the type of its own
  values, and needs the removed method there. Such code calls the method
  on the value, embeds the interface in an interface of its own, or passes
  the value where the method is needed: for example to the code that sqlc
  generates for pgx, whose `DBTX` has `QueryRow`. To migrate, pass the
  `pgx.Tx` or the pool itself there, or give the value an interface of
  your own that has the method, such as that `DBTX`.

### Fixed

- A FIFO group that has no row is now reported by every operation that
  takes a task out of it. A group's row is missing only if something broke
  the group, such as a `DELETE` by hand, and taskmill then logs
  `fifo group missing` at Error. But only `Cancel`, `Delete`, an attempt
  that ended dead or canceled, and a rescue did. A completion,
  `CancelMatching`, and `DeleteMatching`, which end most tasks, logged
  nothing. They now log the line too, with the `queue`, the `group`, and
  the `task_id`. A bulk action that takes several tasks out of one such
  group logs one line for them, without `task_id`. The tasks end as
  before. If you see the line, please report it.
- Three fixes for a FIFO group whose rows were changed by hand:
  - **Two tasks of one group that complete in the same batch** are both
    taken off the group's count. One stayed counted. The group's row then
    never went away, and a task enqueued into the group later waited in
    `pending` for good, behind a head that did not exist. A group has two
    running tasks only after an `UPDATE` by hand, such as one that made a
    pending task available.
  - **A halt ends with the task that it names, however that task leaves
    its group.** `Cancel` and `Delete` already ended such a halt. Now
    `CancelMatching` and `DeleteMatching` end it too, and so does a
    completion, after which the next task runs: the group stayed halted
    by a task that was gone. taskmill halts a group only by its dead head,
    so only a row written by hand names another task.
  - **A rescue that ends two tasks of one group, one dead and one
    canceled,** leaves the group halted by the dead one. It made the next
    task available when the canceled task was the older of the two.

### Security

- One value that taskmill never writes, in a small table, no longer takes a
  page of the dashboard or the maintainer down, or deletes tasks. A role
  that can write taskmill's tables, or an `UPDATE` by hand, could leave
  such a value:
  - **An infinite start or check-in time of a worker.** `Client.Workers`
    failed, and the dashboard's Overview and Workers pages answered 500. A
    check-in time of `infinity` did so for good: the maintainer never
    removed the row.
  - **A `NULL` among a worker's queues.** Every refresh of the queue
    statistics failed, for all queues, and `Client.Queues` failed.
  - **An infinite next run time of a schedule.** `Client.Schedules` failed,
    and no schedule of a maintainer that defines this one ran.
  - **A retention period of a million years, or of `infinity`.**
    `Client.Settings` and `UpdateSettings` failed, and so did the
    dashboard's Overview.
  - **A negative retention period.** The maintainer obeyed it. It deleted
    dead and canceled tasks as soon as they finished, or dropped the
    history, today's included. A period with a negative part did the same
    when its months and its days had different signs, though it counts as
    positive in all, such as `-3000000 mons 90000000 days`.
  - **A lease of the maintainer that ends far in the future.** No worker
    became the maintainer until then: no retries, no delayed tasks, no
    schedules, no rescue of lost tasks.

  Now the tables refuse these values, as the migration above says. No
  constraint can refuse the lease, so the election ignores one that ends
  later than a worker can write: more than 20 seconds from now. Such a row
  stops the maintainer for 20 seconds at most.

## [0.7.0] - 2026-11-15

### Added

- More examples on pkg.go.dev: `Client.Enqueue`, `Client.EnqueueTx`,
  `Client.Get`, `WorkerConfig`, `AddHandler` with a handler that is a
  struct, and `outbox.PublisherFunc`. The examples of `Permanent`, `Snooze`,
  `State.CanRetry`, `TaskInfo.ScheduledFor`, and of testing an error now
  show their output.
- [llms.txt](llms.txt), at the module's root: every package and every page
  of the docs in one line each, for tools and AI agents. It ships with the
  module, like the docs, so its paths lead to the pages of the version that
  you use.
- `WorkerInfo.Stale`: `Client.Workers` says whether a worker made no
  check-in for its whole `LeaseDuration`, by the database's clock. Such a
  worker stopped without unregistering, or cannot reach the database. The
  dashboard's Workers page marks the same workers.
- `ListParams.Summary` makes `Client.List` return each task without its
  payload and metadata, and with only the newest of its errors. The
  database does not read or send what is left out, so a page costs about
  the same whatever its tasks hold. Use it for a list that shows one line
  per task. Without it, `List` returns every task in full, as before.

### Changed

- The doc comments are now the complete reference of the API. The comment of
  each identifier states its default, its limits, its zero value, and its
  errors, so look an identifier up on
  [pkg.go.dev](https://pkg.go.dev/github.com/code19m/taskmill) or with
  `go doc`. The package comments say where to start, `Client` groups its
  methods by what you want to do, and the main types link to the page that
  teaches them. No behavior changes.
- The pages no longer repeat the doc comments. In the
  [reference](docs/reference/README.md),
  [Configuration](docs/reference/configuration.md) and
  [Errors](docs/reference/errors.md) are now indexes: they name the type of
  each setting, and the identifier of each error, error class, and error
  history code, and link to pkg.go.dev. The page
  `docs/reference/defaults-and-limits.md` is now
  [Fixed timings and limits](docs/reference/defaults-and-limits.md): it
  keeps the timings of the workers, the maintainer, and the dashboard, which
  no identifier owns. Its sections about tasks, handlers, retention, the
  operations API, and the outbox are gone, like the sections of the old
  Configuration page: links to their anchors now open the top of the page.
- What to do about each error history code is now in
  [Retries and errors](docs/guides/retries-and-errors.md). The guides
  [Enqueue tasks](docs/guides/enqueue.md) and
  [Handle tasks](docs/guides/handle-tasks.md) no longer list every option
  with its default: they link to the type.
- The reference section has a new order: the three indexes come first, then
  the pages about what has no Go identifier.
- This release has a schema migration, `0005_worker_leases_and_freed_keys`.
  It adds a column to `workers` and two to `tasks`, changes only the
  catalog, and reads no row. It waits at most 5 seconds for each of its
  locks. The previous release's code keeps working on the new schema, so a
  rolling deploy is safe, as always.
- The migration also drops the index `schedules_due_idx`. No statement read
  it: each one finds its schedules by name. Every run of a schedule had to
  update it.
- An enqueue of a dedupe key that a task holds writes nothing. It locked
  the key's row to find out that the key was taken, so every duplicate took
  a transaction ID, wrote WAL, and committed as a write, and it took a task
  ID from the sequence. Now a duplicate only reads. Two things that you can
  see change with it. A cancel of the task that holds the key no longer
  waits for an open transaction that enqueued the same key. And an enqueue
  no longer waits for an open transaction that cancels or deletes the task
  that holds its key: until that transaction commits, the task holds the
  key, so the enqueue is a duplicate of it. Before, the enqueue waited, and
  created a task if the cancel committed. A new key costs one more index
  lookup.
- The dashboard says "Taking too long" when a list of tasks reaches its
  time limit of 5 seconds, with a way to try again, and for a list with
  filters, what may help. It said "Something went wrong", as for any
  failure, and answered 500 where it now answers 503. The docs now say
  which lists read only the tasks they show. A list of dead or canceled
  tasks is not always one of them, for one queue or for all: the notes of
  0.6.0 said that the dead tasks of a queue that has few of them are, which
  holds only for a queue with a small part of all tasks.
- Each read of the operations API is one round trip to the database.
  `List` made 5, `Count`, `Groups`, and `Activity` 4 each, and `Queues` 2:
  they sent `BEGIN`, their settings, the statement, and `COMMIT` one by
  one. Now they send the settings and the statement as one batch, which
  PostgreSQL runs as one transaction. The results are the same. A
  dashboard far from its database gains most: its overview makes 5 round
  trips where it made 9, its task list 2 where it made 7, and a task's page
  3 at most where it made 8.

### Fixed

- A healthy worker with a long `LeaseDuration` showed as stale in the
  dashboard, and the maintainer could remove it from the list of workers. A
  worker checks in every third of its `LeaseDuration`. But the dashboard
  marked a worker as stale after a fixed 30 seconds without a check-in, and
  the maintainer removed it after a fixed 5 minutes. So with a lease of 90
  seconds or more, healthy workers showed as stale between check-ins. With
  a lease above 15 minutes, the maintainer removed running workers, and
  each one logged `worker was removed as stale; registering again`. A
  worker now stores its `LeaseDuration` when it registers, in the column
  that the migration adds. It is stale after one `LeaseDuration` without a
  check-in, and the maintainer removes it after two, but not before 5
  minutes. `QueueInfo.Workers` counts it until then. With the default lease
  of 30 seconds, nothing changes. A worker with a shorter lease shows as
  stale sooner than before: after its own lease. The dashboard now judges
  by the database's clock, not by its own. During a rolling deploy, the
  fixed times still apply to the workers of the previous release, and to
  every worker while a maintainer or a dashboard of the previous release
  runs.
- A queue with `MaxRunning` ran fewer tasks than its limit. When a task
  ended, its worker fetched right away, before the task's completion was
  recorded. The queue still looked full, so the worker waited for its next
  poll: `PollInterval`, 1 second by default. With a limit of 4, tasks of 1
  second ran 2.8 at the same time on average, and tasks of 0.3 seconds
  only 1.3. A worker now fetches again when the completion is recorded,
  and both ran 3.9 at the same time. Room that another worker's task
  leaves is still found by the poll.
- The tasks of one FIFO group ran at about 20 a second in a quiet queue,
  however short they were. When a task of the group completed, the next
  one waited for a notification, and a client sends at most one per queue
  in 50 milliseconds. The worker that completes a group's task now fetches
  the next one right away: with one worker, 200 short tasks of one group
  ran at about 220 a second. Other workers wake no more often than before.
  A worker whose fetch after a notification found nothing still lets 20
  milliseconds pass before it acts on the next notification, but the
  fetches that its own tasks cause no longer wait for that.
- A worker that could not read the settings of its queues when it started
  ran the tasks of paused queues, and ignored `MaxRunning`, for up to 10
  seconds: until its next read. A worker now takes no task before it has
  read those settings, and until then `Worker.Health` returns an error, so
  a readiness probe shows the worker as not ready. If the read fails, the
  worker logs `operation failed` for `load queue settings`, and reads
  again after 0.1 seconds, then waiting twice as long each time, up to 10
  seconds. That read had no time limit either, so a database that did not
  answer it kept the worker from starting: no check-in, no maintainer, no
  fetch. Each read now ends after 10 seconds, and it no longer delays the
  rest of the start.
- A pause, a resume, or a new `MaxRunning` reached a worker up to 10
  seconds late when it was set while the worker did not listen for
  notifications: as the worker started, or while it reconnected. The
  worker now reads the settings of its queues again as soon as it listens.
- A history partition that the maintainer had detached but could not drop
  stayed in the schema for good, with a day of completed tasks on disk:
  for example when the worker stopped, or lost the database, between the
  two steps. No later pass found the table, because it was no partition
  any more. The maintainer now marks a partition with a comment before it
  detaches it, and a later pass drops a detached table that has the mark.
  A table without the mark stays: one that you detached or copied to keep
  it, and one that an earlier release left behind, which you remove with
  `DROP TABLE` ([Database](docs/production/database.md)). A pass drops at
  most 10 partitions, and the next pass, a minute later, continues. A
  table that cannot be dropped, for example because a view of yours
  depends on it, does not keep the others: the log names it, and the pass
  continues.
- Queue statistics went stale while retention had much to do. The
  maintainer refreshed them in the loop that also applies retention. So
  the counts, `QueueInfo.Lag`, and `QueueInfo.RefreshedAt` stood still
  while it deleted a large backlog of old dead or canceled tasks, or while
  the drop of old history waited for a long transaction such as a backup,
  up to 30 seconds in each minute. An alert on statistics older than 60
  seconds could then fire although a maintainer ran, and an autoscaler
  read an old lag. Statistics now have a loop of their own, and are
  refreshed every 10 seconds whatever retention does.
- `AddHandler` and `AddBatchHandler` panicked, which ends a process as it
  starts, when the `Kind` method of the args panicked with a value that
  panics again when it is formatted, such as an error whose `Error` method
  panics with its own receiver. `NewWorker` now reports such a `Kind` as
  an error, as it reports every other panic there, and the error names the
  value's type. For a handler that panicked with such a value,
  `taskmilltest.Run` returned a nil error, so a test that checked only the
  error passed. It now returns an error for that panic too.
- A worker that ran no task logged `worker stopping; waiting for running
  tasks` with `tasks=0` each time it stopped. It now logs that line only
  when it waits. And `tasks` counts the tasks whose handlers still run: it
  also counted the finished ones whose results were not recorded yet.
- A retry of a canceled task takes the task's dedupe key back. A cancel
  frees the key, and a retry did not take it back. So after a cancel, an
  enqueue of the same key, and a retry of the canceled task, two unfinished
  tasks stood for one key, and both ran. Now the retried task holds its key
  again, for the rest of the key's window, and an enqueue of the key is a
  duplicate again. If another task holds the key by then, the retry changes
  nothing. `Retry` returns `ErrConflict`, which only `UpdateQueue` and
  `UpdateSettings` returned so far, so check for it where you retry
  canceled tasks. `RetryMatching` leaves such a task, and counts it in the
  new `BulkResult.Conflicts`. The dashboard's Retry button says which task
  holds the key, and a retry of many tasks says how many it left. A task
  that is retried after its window ended takes no key, as before. The
  cancel stores the key with the task, in two columns that the migration
  adds to `tasks`, so the task looks as before. If the task that holds the
  key is gone, the error names none, and says when the key's window ends.
  During a rolling deploy, a task that the previous release cancels has no
  stored key, so its retry takes no key, as before.
- `CancelMatching`, `RetryMatching`, and `DeleteMatching` no longer fail
  with `canceling statement due to lock timeout` (SQLSTATE 55P03) when
  commits that notify are slow. Each batch of such an action waits at most
  100 ms for a row that another transaction holds. That limit also held
  for the batch's commit. A transaction that sent a notification, as a
  batch does that wakes a queue or stops a running task, waits at its
  commit for a lock that every such transaction of the database server
  takes in turn. With synchronous replication, each one holds it while it
  waits for the standby. When that wait took over 100 ms, the batch was
  undone, and the action returned the error after the batches before it.
  The dashboard's buttons for many tasks failed the same way. A batch now
  commits without the limit. A schedule's run had the same limit, of one
  second: when the wait took longer, the run was not enqueued, the worker
  logged the error, and the next cycle tried again. It also commits
  without the limit now.
- `Enqueue` and `EnqueueMany` no longer fail with PostgreSQL's `deadlock
  detected` (SQLSTATE 40P01) for a deadlock that other transactions opened.
  A `CancelTx` in an application's transaction can take its locks in an
  order that makes it wait for another transaction, and that one for it.
  An enqueue into the same FIFO group that waits between the two is part
  of the circle, and PostgreSQL may end the enqueue to end the wait. On
  the client's pool, an enqueue is one statement in a transaction of its
  own, so nothing of it was saved. It now runs again by itself, up to 5
  times in all, as `Cancel`, `Delete`, and `Retry` do. `EnqueueTx` and
  `EnqueueManyTx` cannot, because the transaction is yours: run it again.
- The dashboard's task list no longer reads what it does not show. It read
  every task of a page in full: the payload, up to 1 MiB, the metadata, and
  all 20 errors, to show one line per task. One page of 50 tasks with large
  payloads took 54 MiB of memory in the process that serves the dashboard,
  and an open tab asked for it again every 5 seconds. The list now asks for
  summaries: a page reads the columns it shows and the last error of each
  task, whatever the payloads hold. The page of a dead task that halts its
  FIFO group listed up to 100 of the tasks behind it, in full, only to
  count them. It now counts them, so it shows the number up to 10,000,
  where it showed "100+".

### Security

- One malformed row no longer stops a queue or the maintainer. A role with
  `INSERT` on `tasks`, or an `UPDATE` by hand, can write a row that taskmill
  never writes. Such rows did harm far beyond their own task:
  - **Metadata that is not a JSON object of strings, or a time of creation
    or a run time of `infinity`.** Every fetch that took the row lost all
    the tasks that it leased with it. They ran only after their leases
    expired, with one attempt used. In a queue with `MaxRunning`, no task
    started again. In a queue that no worker reads, an infinite run time
    failed every refresh of the statistics, for all queues.
  - **Errors that are not a JSON array.** The maintainer stopped rescuing
    expired leases, in every queue.
  - **A lease token at the largest integer, or a waiting task with no
    attempt left,** such as a dead task that an `UPDATE` made `available`.
    Every fetch from the queue failed, and no task started again.

  Now only that task fails. A worker makes it dead with the new error code
  `invalid_row` (`CodeInvalidRow`), and runs the tasks around it. Neither
  its handler nor the middleware runs for it. Errors that are not an array
  count as none, and a task with no attempt left runs once more.
  `Client.Get` and `Client.List`, and so the dashboard, show such a task
  without the value that does not decode: before, they failed, and a list
  failed for its whole page. If other roles can write to taskmill's tables,
  upgrade.
- The grants for a role that only enqueues, in
  [Split work across services](docs/recipes/split-across-services.md), now
  give `INSERT` on the columns that an enqueue writes, not on all of
  `tasks`. With the old grant, the role could write a task that looks
  leased, with a lease that never ends: such a row holds a place under its
  queue's `MaxRunning` for good. If you use the old grant, run
  `REVOKE INSERT ON billing.tasks FROM orders`, with your names, and then
  the new `GRANT` of the recipe. The recipe also lists what the role can
  still do, such as choose a task's ID. A role with `INSERT` on the whole
  table stays trusted, as [Security](docs/production/security.md) now says.
- A FIFO group whose row counts fewer tasks than the group holds no longer
  fails the work of other groups. A role with `UPDATE` on `groups`, which a
  role that only enqueues has, can write such a count. When a task then
  left the group, its count fell below zero, which the table refuses. So
  the completion of every grouped task in the same batch failed, and those
  tasks ran again after their leases expired. If the maintainer's rescue
  ended such a task, the rescue failed for every queue. The count now stops
  at zero, and only that group can lose its order.

## [0.6.0] - 2026-11-13

### Added

- A scheduled run knows the time it is for, on every attempt.
  `TaskInfo.ScheduledFor` returns that time, on the clock of the schedule's
  `Location`. Use it in handlers in place of `t.RunAt`, which is that time
  on the first attempt only: a retry, a snooze, or a lost lease moves
  `RunAt`, so a nightly report that failed once could build the wrong day.
  The maintainer stores the time, and the schedule's name, in the metadata
  of each task that a `Schedule` enqueues, under `MetadataScheduledFor`
  (`taskmill.scheduled_for`) and `MetadataSchedule` (`taskmill.schedule`).
  So the dashboard's task page shows which schedule made a task. These
  values replace those that the schedule's `Options` give the same keys,
  and enqueue middleware can read them but not change or remove them. Keys
  that start with `taskmill.` now belong to taskmill, and later releases
  may add more: don't use the prefix for keys of your own. During a rolling
  deploy, a run that a maintainer of the previous release enqueues has no
  such metadata. `ScheduledFor` returns the zero time for it, as for a task
  that no schedule enqueued, so let the handler fall back to `t.RunAt`.
- The docs have a [tutorial](docs/tutorial/README.md) that builds a small
  shop in seven parts, reference pages for every config field and for the
  `taskmill-ui` command, a guide to middleware, five new recipes, and
  production pages about backups and failover and about capacity planning.
- `examples/tutorial`, the program that the tutorial's reader has built
  after part 7: one binary with a web role and a worker role.

### Changed

- `examples/quickstart` listens on the address in `ADDR`, by default
  `localhost:8080` as before. Its `POST /orders` now answers
  `order 1 placed, task 1 enqueued`, without the link to the task's
  dashboard page.
- A worker with a free slot for a queue that is at its `MaxRunning` now
  sends one read each time it polls the queue: it counts the queue's running
  tasks without a lock. Before, each such worker opened a transaction and
  locked the queue's row, once a second by default, although no task could
  start. A fetch still locks the row and counts again before it takes a
  task, so the limit holds as before.

### Fixed

- A task of a queue with `MaxRunning` could start with a lease that was
  almost over, or already over. Its lease counted from the start of the
  fetch, before the fetch waited for the queue's row, which another worker's
  fetch can hold. The maintainer then rescued the task while it ran: the task
  used an attempt with the code `lease_lost` and ran twice, or became dead
  with `MaxAttempts: 1`. A lease now counts from the moment the task is
  taken. This holds for `Worker.RunTask` too, which lost the time it waited
  for a task's row that another transaction held.
- A fetch from a queue with `MaxRunning` waits at most one second for the
  queue's row, not 30. If a transaction holds the row longer, such as a
  stalled worker's, the fetch gives up, and the worker fetches again when
  the queue is next woken or polled. Before, each worker of the queue kept a
  connection waiting, and could not stop until the wait ended. Giving up is
  not a failed operation, and logs only a debug line: the queue's fetches
  take the row in turn, so with many workers on one queue a fetch can wait a
  second while nothing is stuck.
- A panic in `RetryPolicy.Backoff`, or in a method of the error that a
  handler returned, ended the worker's process. The usual cause of the
  second is a nil pointer returned as an `error`, whose `Error` method
  panics. Every other task of that process lost its lease, and the task
  ended each process that ran it next, until its attempts ran out. The
  worker now recovers such a panic as it recovers a handler's: the attempt
  fails with the code `panic` and the panic's stack in the task's errors.
  The task is retried after the default backoff, or becomes dead after its
  last attempt.
- A panic still ended the worker's process if its value panicked again when
  the worker formatted it, as an `Error` method that panics with its own
  receiver does. This held for a handler's own panic too. The worker now
  formats the value under a recover of its own, and the task's error names
  the value's type instead of its text.
- A worker that fetched a task again while its own older attempt of that
  task still ran forgot the older attempt. This can happen after the worker
  was cut off from the database, or paused, for longer than the lease: the
  task is rescued, and the worker's next fetch can come before its next
  check-in. The older handler's context was never canceled, so it ran to its
  end beside the new attempt; only its result was refused. The worker now
  cancels the older attempt's context with `ErrLeaseLost` as soon as it
  fetches the task again.
- A task could become dead with the code `unknown_kind` the first time a
  worker without a handler for its kind met it, although workers with the
  handler had run it since. The wait for a handler counted from the task's
  oldest `no_handler` note, such as one from a deploy hours ago. So after a
  rollback, or beside an old process that lingered, the hour was already
  over, and the dead task halted its FIFO group. The wait now ends when a
  worker with the handler takes the task. A note counts only while it is the
  last of the task's errors, and a worker with the handler that snoozes the
  task, or gives it back when it stops, removes the note. The next worker
  without the handler starts a new wait of an hour, with a new note.
- `Retry` and `RetryMatching` did not start a new wait for a handler if the
  task had been canceled while it waited. More than an hour after the task's
  `no_handler` note, the next worker without the handler made the retried
  task dead right away. A retry now removes that note, so the wait starts
  again, as documented.
- `Migrate` called from several processes at the same time failed in the
  processes that waited, with `schema ... already exists` or `relation ...
  already exists`, when the pool's `default_transaction_isolation` was not
  `read committed`. `Migrate` now works at any default level. A client still
  needs `read committed`, as `NewClient` checks.
- History partitions follow the database's clock. Workers created the daily
  partitions, and the maintainer dropped the expired ones, by the clock of
  their own machine. A maintainer whose clock was ahead by more than the
  history retention dropped history that had not expired, and a machine
  whose clock was days off created partitions for the wrong days, so
  completed tasks skipped history. Both now use the database's clock, as
  every other time that taskmill sets.
- `HistoryRetention` set to the longest duration,
  `time.Duration(math.MaxInt64)`, deleted all history: the period
  overflowed when the maintainer read it, so it dropped every history
  partition, each minute, and `Client.Settings` reported a negative period.
  The database now applies the period itself, and `Settings` reads it
  without overflow. The longest duration, about 292 years, keeps completed
  tasks for good, and so does a longer period that only SQL can store,
  which `Settings` reports as the longest duration.
- A worker that held the maintainer's lease without knowing it kept the
  lease when it stopped: after a renewal that failed, or an election whose
  answer was lost while the worker stopped. No worker then led until the
  lease expired, up to 10 seconds, so delayed tasks, retries, and schedules
  waited. A stopping worker now gives up the lease whatever it knows.
- `taskmill-ui` waited without a limit for the body of a request, so a
  client that never sent it kept its connection for good, without a login.
  Enough of them left the dashboard and its probes without connections. A
  whole request now has 10 seconds to arrive.
- A row of `queue_stats` whose count was the smallest 64-bit number ended
  the process that served the dashboard, at the next page, with `fatal
  error: stack overflow`: for a mounted dashboard, that process is your
  application. No maintainer stores such a count, but the column can hold
  it. The dashboard now shows every count that the column can hold.
- A dashboard with a `Prefix`, or `taskmill-ui` with `TASKMILL_UI_PREFIX`,
  redirected some requests out of the prefix, to the root of the host:
  `/admin/jobs` without its last slash led to `/`, and behind a router that
  passes paths on unchanged, a path with a double slash lost the prefix
  too. These now lead to the clean path under the prefix. A path that only
  starts like the prefix, such as `/admin/jobsx`, was redirected as well,
  to `/x`, and is now not found.
- Retention no longer reads the whole `tasks` table once a minute. On
  PostgreSQL 15 to 17, once a few hundred thousand dead or canceled tasks
  waited for the end of their retention, the maintainer's connection
  switched to a plan that read every task to find the old ones: 1.4 GB a
  minute with 3.4 million tasks, and about 1 GB for each 1,000 tasks it
  then deleted. The statement now reads each queue's old tasks through
  `tasks_finished_idx`: a few pages when no task is old enough.
- Renewing leases and completing tasks no longer take time that grows with the
  square of the number of tasks. A worker that held 5,000 tasks at the same
  time, as with a batch size of 1,000 and a concurrency of 5, spent about 1
  second in the database at each check-in, and now spends about 0.1 seconds.
  At that growth, a worker that held about 12,000 tasks could not have renewed
  its leases in time, and its tasks would have run twice. A full batch of 500
  completions takes a third of the time.
- A transaction that cancels several running tasks, oldest first, can no
  longer fail with `deadlock detected` against a worker that completes
  those tasks. A completion took the rows of its tasks in the order in
  which it met them, which is not always that of their IDs: on PostgreSQL
  18, for 200 completions among 20,000 tasks, once a renewed lease had
  moved an older task's row behind a newer one's. PostgreSQL then ended the
  transaction, or the completion, which the worker tried again. A
  completion now locks its tasks in the order of their IDs before it
  deletes them.
- A worker that is not the maintainer no longer writes to the database each
  time it tries to become it, 24 times a minute. Every such try took a
  transaction ID and wrote WAL, though it changed nothing: with 100 idle
  workers, 40 write transactions a second and 3.5 million transaction IDs a
  day. A try now writes only when it takes or renews the maintainer's
  lease.
- Filling a batch for a batch handler, as the outbox relay does at every
  fetch, reads only the tasks it takes. It read and sorted the queue's
  first 1,000 available tasks to take a few of them, about three times the
  pages of a plain fetch. On PostgreSQL 15 and 16 with JIT on, which is
  the default, each fill also took about 0.1 seconds, where it now takes
  about 2 milliseconds: the sort made PostgreSQL compile the statement at
  every run. PostgreSQL 17 plans it as they do, and 18 did not compile it.
- `Client.List` and the dashboard's task list read a page through the index
  that fits their filters. A list of one queue's completed tasks, of one
  FIFO group's waiting tasks (which a task's page shows), or of the dead
  tasks of a queue that has few of them read every newer task first: with
  3.4 million tasks and 1.5 million completed ones, 0.5 to 2 seconds a
  page, and past the 5-second limit on a larger table. Such a list now
  reads about the tasks it shows. A list of one state across all queues,
  such as every dead task, still reads the newer tasks of other states
  first.
- `RetryMatching`, `CancelMatching`, and `DeleteMatching` get past any
  number of tasks that do not match. A batch read on until it had found
  1,000 matching tasks: with 2.4 million other tasks before the first dead
  one, the first batch of a retry read them all. On a table where that
  took more than the batch's 5 seconds, the action failed, and failed
  again at the same place with the cursor it returned. Now the database
  ends a statement of such a batch after a second, and the action continues
  in batches that read at most 100,000 tasks, each of which moves its
  position. It still acts on each matching task once. An action whose
  tasks an index finds reads only those, as before.
- `Client.Count`, which the dashboard asks before it confirms a bulk
  action, reads each state's tasks through that state's index. Where the
  tasks of a state lay together in the table, as tasks of one age do, it
  read the table once for each state: with 3.4 million tasks, a count with
  no filter read 4 GB in 22 seconds, far past its 5-second limit, so it
  failed. That count now reads 0.6 GB in under half a second. A count by
  kind or FIFO group alone still reads every task of each state.
- A client no longer remembers every queue it ever enqueued into. With
  queues named after tenants, for example, its memory grew with each new
  name for the life of the process. It now forgets the queues whose last
  notification is older than the 50 milliseconds for which it matters.
- A `RunAt` after the year 294276, such as `time.Unix(math.MaxInt64, 0)` to
  mean "never", was stored as a time in the past, so the task ran right
  away. Other far-future times made PostgreSQL fail the statement, which
  aborted the caller's transaction. Now every enqueue returns
  `ErrInvalidArgument` for a `RunAt` that PostgreSQL cannot store, before
  4713 BC or after 294276 AD. The check runs before any statement, so the
  caller's transaction stays usable.
- A schedule never ran, with no error or log line, when its task's payload
  did not read back from PostgreSQL as the maintainer compared it: a
  `float64` of `-0`, a number that no `float64` holds, or args that encode
  differently each time. The maintainer found the schedule changed in every
  pass, stored it again every second, and skipped its run. Now it compares
  numbers by their exact value, as PostgreSQL does, so it stores such a
  schedule once. And a schedule that did change still runs when it is due.
- `RunSchedule` close to a regular run of the schedule lost one of the two
  runs. A request in the second after a regular run was dropped as its
  duplicate, although `RunSchedule` returned nil. A request in the second
  before a regular run, or while that run waited for the maintainer, took
  its place: one task ran, with the request's time as its `RunAt`. Now the
  requested run and the regular run both happen. A request made while a run
  of the schedule is already due adds no run: that run starts within about
  a second anyway.
- `taskmilltest.Run` returned a nil error when the handler never ran,
  because the task's payload did not decode into its args, for example
  with a field of an interface type. A test that checked only the error
  passed. Now `Run` returns an error that says the handler did not run, and
  why.
- `NewWorker` panicked on a schedule whose cron expression has a step near
  the largest integer, such as `0 0 */9223372036854775807 * *`. Now it reads
  such a step like any other step beyond the field's range: the schedule
  matches the first value only, here day 1.
- A schedule lost runs when clocks went forward. Of several times in the
  skipped hour, only the first ran: `0,30 2 * * *` ran at 03:00, and its
  02:30 run was dropped. In a zone whose clocks change by 30 minutes, a run
  just after the jump could be dropped too. And in a zone whose clocks
  change by two hours, the runs of the repeated hours came in their second
  pass. Now each time that the clock skips runs once, as long after the jump
  as the time is into the skipped hour, and each time that the clock shows
  twice runs the first time only.
- `Cancel` and `Delete` could fail with PostgreSQL's `deadlock detected`
  (SQLSTATE 40P01) after a second, and a bulk action with a lock timeout,
  when another `Cancel`, a `Retry`, a worker, or the commit of the task's
  enqueue changed the same task of a FIFO group at the same moment. They
  decided which row to lock first, the task's or its group's, from a state
  that could change before they held the lock. Now they check the state
  again under the lock. If it changed, they roll their transaction back and
  start again, which locks in the other order. `CancelTx` cannot do that in
  your transaction, so it works as before. When another transaction changes
  the same task or its FIFO group at the same moment, PostgreSQL may still
  end one of the two with that error, which may hit the other transaction.
  The transaction that got it can be run again.
- `Cancel`, `Delete`, and `Retry` run again, up to 5 times in all, when
  PostgreSQL ends their transaction with `deadlock detected` (SQLSTATE
  40P01). PostgreSQL ends any one of the transactions that wait for each
  other, so the error could hit a call that did nothing wrong, when another
  transaction, such as one with `CancelTx`, took the same rows in another
  order.
- When two producers enqueued the same dedupe key just after its window
  ended, one created the task, as it should, but the other's
  `EnqueueResult.ID` named the old task that held the key before. Now it
  names the task that the first producer created.

### Security

- A notification alone no longer cancels a running task. PostgreSQL lets
  every role of a database send notifications, also a role with no rights
  on taskmill's tables, and a worker canceled the task that a cancel
  notification named: the task ended as `canceled`, though nobody had
  called `Cancel`. A worker now takes the notification as a hint and reads
  the request from the task's row, and an attempt ends as `canceled` only
  if that row has the request. `Cancel` still stops a running task right away:
  the worker reads one row first. If other roles can connect to your
  database, upgrade.
- A role with no rights on taskmill's schema can no longer keep workers and
  `Migrate` from starting. Both waited, without a limit, for advisory locks,
  which PostgreSQL lets every role of a database take and hold: a worker
  registered but never fetched a task, `Migrate` did not return, and the
  maintainer stopped applying retention and refreshing statistics. They now
  wait only for locks on taskmill's own tables, which a role needs rights
  on those tables to take. Preparing history partitions takes no lock while
  every partition is ready, and waits at most 5 seconds otherwise. `Migrate`
  still waits while another process migrates. It now creates the schema and
  the `migrations` table before the transaction that migrates, so a first
  `Migrate` that fails leaves those two behind; run it again.
- A flood of notifications can no longer block a worker. A worker read
  the database once for each notification that a queue's settings changed,
  one after the other, where it receives notifications, and every role of a
  database can send thousands of them a second. Cancellations and wake-ups
  then waited behind them, for seconds, and the worker logged that the
  maintainer's heartbeats did not arrive and polled its queues. A worker
  now only notes what a notification says. It reads the settings, and the
  rows of the tasks to cancel, at most once every 20 milliseconds for all
  the notifications that arrived, so `Cancel` still stops a running task
  within that time.

## [0.5.0] - 2026-11-12

### Changed

- A worker that stops gives up the maintainer role when it starts to stop,
  not after its drain. Another worker takes over at once, and does the
  shared work while the first finishes its running tasks. So a new
  version's schedules start as soon as a deploy stops the old maintainer,
  and a process that is killed during its drain leaves no gap in which
  delayed tasks, retries, and schedules wait.

### Fixed

- `Snooze` with a delay above 35 minutes was not recorded. The worker
  logged `record attempt failed; the task is rescued when its lease
  expires`, and the task counted a failed attempt with the code
  `lease_lost`, ran again after the retry backoff, and became dead after its
  last attempt. Now the task waits the delay and keeps its attempt, as
  documented.

### Security

- taskmill now requires pgx v5.12.0, which fixes two vulnerabilities in
  pgx: SQL injection in batches sent with the simple protocol
  ([GHSA-j38q-h3ph-jjpq](https://github.com/jackc/pgx/security/advisories/GHSA-j38q-h3ph-jjpq)),
  and a bypass of `require_auth` and `channel_binding=require`
  ([GHSA-95fj-3fxw-g62g](https://github.com/jackc/pgx/security/advisories/GHSA-95fj-3fxw-g62g)).
  taskmill's own batches were not open to the first one, since none ends in
  a comment. Your application's batches and connection settings may be
  affected, so upgrade. pgx v5.12.0 also adds a `Tx` method to
  `pgx.BatchResults`: add it to your own implementations, such as mocks.

## [0.4.0] - 2026-10-29

### Added

- `examples/quickstart`, the program that the new quickstart runs.
- The dashboard's Workers page shows the kinds that each worker has handlers
  for. A worker's row counts them; click the count to list them all, each
  with a link to its tasks.
- `taskmill-ui` answers two paths for Kubernetes probes, with no login, at
  the root even with `TASKMILL_UI_PREFIX`. `/readyz` fails while the
  database does not answer, and `/livez` does not check the database, since
  a restart does not bring it back. `/readyz` asks the database at most once
  a second, and logs `taskmill-ui not ready` and `taskmill-ui ready again`
  when its answer changes.
- `TASKMILL_UI_LOG_FORMAT=json` makes `taskmill-ui` log JSON lines.
- `QueueInfo.HaltedGroups` and `QueueInfo.MostFailures` show work that is
  stuck while a queue's lag stays near zero. `HaltedGroups` counts the
  queue's FIFO groups that a dead task halts, up to 10,000. `MostFailures`
  is the most times in a row that one task of the queue, waiting to retry,
  has failed, such as an outbox message while the broker is down. The
  maintainer counts both with the other statistics, in two new columns of
  `queue_stats` (migration `0004_stuck_work.sql`).
  [Monitoring](docs/production/monitoring.md) has an alert for each, and no
  longer asks you to poll `Client.Groups` for halted groups.
  [Queue statistics and health](docs/design/schema-statistics.md)
  explains the choice.

### Changed

- **Breaking:** `WorkerInfo.HeartbeatAt` is now `WorkerInfo.CheckedInAt`.
  "Heartbeat" now means only the maintainer's signal to the workers. A
  worker's own report to the database, which renews its leases, is its
  check-in. To migrate, rename the field where you read it.
- A worker's check-in has the new name in logs and in the dashboard too. The
  `operation` attribute of the failing-operation lines says
  `record check-in` instead of `record heartbeat`: if you match on it,
  change the match. The Workers page's column says "Checked in" instead of
  "Heartbeat", and a stale worker's tooltip says "No check-in for a while".
- The dashboard's log messages say "dashboard": `web interface action` is
  now `dashboard action`, `web interface request failed` is now
  `dashboard request failed`, and `web interface request timed out` is now
  `dashboard request timed out`. Update the log filters and alerts that
  match them.
- `taskmill-ui` logs `taskmill-ui listening` once it listens, not just
  before, and its `addr` is the address it listens on, such as `[::]:8080`.
- The docs are rebuilt as short pages, by what you want to do: get started,
  concepts, guides, recipes ("How do I…?"), production, and reference. They
  draw flows as diagrams, and recommend one way wherever there is a choice.
  Start at [docs/README.md](docs/README.md). The old `docs/guide.md` is
  gone.

### Fixed

- The maintainer no longer compiles its statistics count with JIT at every
  refresh, every 10 seconds. Where JIT is on, as it is by default when
  PostgreSQL has LLVM, the compile took about 200 ms on a large table, many
  times the count itself.
- The worker no longer warns at every start that its pool has fewer
  connections than its concurrency plus 5. That check could not see which
  pool the handlers use, so it also warned when they had a pool of their
  own, as the docs advise. Now the worker warns when the pool was full and
  renewing leases had to wait for a connection, at most once a minute:
  `the pool was full, so renewing leases waited; …`.
- The docs of `BatchHandler` and `Client.Cancel` said that a task canceled
  during its batch becomes canceled. It becomes completed if the batch
  returns nil for it, because the work was done, as for other handlers.
- On a new schema applied with `Migrations` and your own migration tool,
  the first completed tasks skipped the history, and the worker logged
  `task history has no partition for today` at Error level. Now each worker
  creates the missing history partitions when it starts, before it runs a
  task, as `Migrate` does. If its role may not, it logs `prepare history
  partitions failed` at Warn level and starts all the same.
- In the dashboard, table cells now show their faint and warning colors: a
  stale worker's check-in time turns to the warning color, as its dot does.
- A maintainer that stopped while it renewed its lease did not resign. The
  next maintainer then took over only once that lease expired, up to about
  12 seconds later, and meanwhile delayed tasks, retries, and schedules
  waited. Now it resigns, and another worker takes over at once, as the
  docs say.

## [0.3.0] - 2026-10-28

### Added

- `ErrConflict`, of class `conflict`: `Client.UpdateQueue` and
  `Client.UpdateSettings` return an error that wraps it when each of their
  10 tries lost to a concurrent update of the same settings. Between tries
  they pause a short random time, up to 50 ms, so that colliding updates
  spread out and nearly all land.
- `ErrTaskCanceled`, `ErrAttemptTimeout`, and `ErrShutdown`: with
  `ErrLeaseLost`, the causes that `context.Cause` gives for a handler's
  context that ended. The error classes that `ErrorType()` returns, such as
  `invalid_argument`, are now part of the API.

### Changed

- The guide's [Upgrading](docs/reference/compatibility.md) section states the
  compatibility promise from v1.0.0 on: breaking changes only in a new major
  version, deprecated names kept until then, interfaces that never grow, and
  names that never change, such as the `State` values and error classes. It
  names the `migrations` table as the one public table, and what the promise
  leaves out: log and error messages, and the `otel` module's names.
- **Breaking:** `Client.UpdateQueue` and `Client.UpdateSettings` take a
  function that changes the current settings, instead of settings that
  replace them all. A setting that your code leaves alone, or that a later
  version adds, keeps its value. They hold no lock while the function runs:
  they write the settings back only if no other update changed them
  meanwhile, and otherwise run the function again on the new settings, up to
  10 times before they fail with `ErrConflict`. So concurrent updates no
  longer lose each other's changes, and the function must only change the
  settings it gets, with no I/O. To migrate, set the fields you mean to:
  `client.UpdateQueue(ctx, name, func(s *taskmill.QueueSettings) { s.MaxRunning = 4 })`.
  Calls that pass nil still compile, but now fail with `ErrInvalidArgument`.
  Replace `UpdateQueue(ctx, name, nil)`, which removed the limit, with
  `UpdateQueue(ctx, name, func(s *taskmill.QueueSettings) { s.MaxRunning = 0 })`,
  and `UpdateSettings(ctx, nil)`, which reset retention to its defaults, with
  `UpdateSettings(ctx, func(s *taskmill.Settings) { *s = taskmill.Settings{} })`.
- **Breaking:** `Client.Groups` pages through every group, like `List`: it
  returns a `*GroupsResult` with `Groups` and a `Next` cursor, which
  `GroupParams.Cursor` takes, and `GroupParams.Halted` finds the halted
  groups. Before, it returned at most 1,000 groups. To migrate, read
  `res.Groups`, and follow `res.Next` for more. `GroupParams.Queues` takes at
  most 100 names, since each named queue reads up to a page. Each page reads
  only its own groups, also from a few queues or of halted groups only, with
  migration 3's index on the halted groups, which `Migrate` adds. Its
  documentation now says exactly what it lists: the groups with unfinished
  tasks or a dead task that halts them, which `GroupInfo.Tasks` counts too.
- **Breaking:** `QueueInfo.Counts` holds a `TaskCount` per state, as
  `Client.Count` does, with its own `Capped` flag, and `QueueInfo.Capped` is
  gone. Every count stops at 10,000 per queue and state, scheduled and
  retryable ones too, and is capped only when more tasks are left: a count
  of exactly 10,000 is not. Once the shared cap of 100,000 scheduled and
  retryable tasks is passed, only those counts are capped, in every queue,
  even at zero. The dashboard marks only the counts that stopped at their
  cap with "+"; before, every count of a queue with one capped count had
  it. To migrate, read `Counts[state].N`, and `Counts[state].Capped` for
  whether that count stopped at its cap.
- **Breaking:** `ui.New(client, cfg)` replaces `ui.Handler`, and returns an
  error that wraps `taskmill.ErrInvalidArgument` instead of panicking on a
  configuration it refuses: a nil client, a `Prefix` without a leading slash,
  an empty `BasicAuth` username or password, or a bad `FrameAncestors` entry.
  To migrate, check the error: `dashboard, err := ui.New(client, cfg)`.
- **Breaking:** attempt middleware that returns nil without calling `next`
  fails the attempt, which is retried with backoff. Before, the task was
  completed although its handler never ran. Middleware that skips a task on
  purpose returns an error, such as `taskmill.Snooze(d)` or
  `taskmill.Permanent(err)`. Such a failure, and an enqueue middleware's that
  returns nil without calling `next`, now reaches the middleware outside it
  as an error from its `next`, of the class `middleware`, so tracing and
  metrics record it.
- **Breaking:** limits on one enqueue call, checked before any statement, so
  that the caller's transaction stays usable: at most 64 KiB of encoded
  metadata per task, after enqueue middleware, and at most 10,000 tasks and
  64 MiB of payload and metadata per call. They apply to `EnqueueMany`,
  `EnqueueManyTx`, and `outbox.Store`, and to the metadata of schedules,
  which `NewWorker` checks. Calls beyond them fail with `ErrInvalidArgument`.
  To migrate, split larger calls into chunks within both limits, and keep
  each task's metadata within 64 KiB. Sizes now count `&`, `<`, and `>` as
  one byte each, as stored, not as JSON escapes.
- **Breaking:** `CodeLeaseExpired` is now `CodeLeaseLost`, with the value
  `lease_lost`, like `ErrLeaseLost`. Error history entries recorded before
  keep `lease_expired`: code that reads them checks for both.
- **Breaking:** `NewWorker` rejects a `Schedule` whose `Options` set `RunAt`,
  `DedupeKey`, or `DedupeWindow`, with `ErrInvalidArgument`, instead of
  silently replacing them: the schedule sets them itself. Remove them from the
  options.
- `Worker.Run` on a running worker fails with `ErrInvalidState`, and
  `NewClient` with a pool that does not use READ COMMITTED with
  `ErrInvalidArgument`. Code that matched their messages can use `errors.Is`.
- `Worker.Run` returns nil when its context ends before the worker
  registered, as when a process is stopped while it starts, instead of a
  "register worker" error.
- **Breaking:** `outbox.Outbox.AddRelay` takes `*outbox.RelayOptions`, whose
  `BatchSize` and `Timeout` set the most deliveries per `Publish` call and
  the time each call has, as `HandlerOptions` do for other batch handlers.
  nil or zero values keep the defaults, 100 deliveries and 1 minute, and
  values out of range make `NewWorker` fail with `ErrInvalidArgument`. To
  migrate, pass nil: `ob.AddRelay(&handlers, publisher, nil)`.
- An idle queue polls once a minute instead of every `PollInterval` while
  the maintainer's heartbeats reach its worker. Every second, the maintainer
  notifies each queue whose next task has waited longer than a second, which
  finds tasks whose notification came before their transaction committed or
  never came, and every 5 seconds it sends a heartbeat with it. A worker
  polls every `PollInterval` until its first heartbeat, and again once none
  came for 15 seconds, starting at once: no maintainer may run, an older one
  may during an upgrade, or notifications may not reach the worker. It then
  replaces its notification connection if that does not answer, since a
  connection can die without a word. So such a task starts within about 2
  seconds while heartbeats arrive, within about 16 seconds if the maintainer
  or the notification connection fails without a word, and within
  `PollInterval` while no maintainer of this version runs. A queue whose
  installation-wide concurrency limit holds tasks back still polls every
  `PollInterval`. 50 idle worker processes serving 100 queues sent the
  database about 5,600 transactions a second, and now send about 150 on
  average, and 460 in the busiest 10 seconds. A worker logs when
  heartbeats start or stop arriving, and gives up opening its notification
  connection after 10 seconds, to try again.
- A maintainer that stops hands over at once: it tells the workers, and
  another takes over and sends its heartbeat. One that crashes is still
  replaced within about 12 seconds.

### Removed

- **Breaking:** `ClientConfig.ContextMetadata`, deprecated in v0.2.0. Use
  enqueue middleware, as the v0.2.0 entry below shows.

### Fixed

- A bulk action whose context ended during a batch closed that batch's
  connection, and so left the batch's transaction running on the server,
  holding up to 1,000 tasks and their FIFO groups, until its statement
  ended. A run that went on at once skipped those tasks as held by other
  work. The batch now finishes, for at most 5 seconds, before the action
  returns, and counts what it did.
- A bulk action whose context ended, or whose database call failed, while it
  read the newest task ID returned no `BulkResult.Next`, so going on started
  over from the first task. It now returns `Next` whenever it stops early.
- The module no longer includes a 20 MB binary of the example shop, which
  v0.2.0 shipped by mistake and every download of it carried.
- A worker no longer listens through a proxy that pools transactions, such
  as PgBouncer in transaction mode when `ListenConfig` is not set. `LISTEN`
  stayed on a server connection that went back to the proxy's pool, so
  notifications reached no worker, and PgBouncer logged a warning for each.
  The worker now finds that out when it starts, logs `notifications
  disabled; polling only` at Error, and polls every `PollInterval`.

## [0.2.0] - 2026-10-27

### Added

- Bulk actions by filter: `Client.RetryMatching`, `CancelMatching`, and
  `DeleteMatching` act on every task that matches the queues, kinds, states,
  and FIFO groups of a `TaskFilter`, oldest first, in short batches that skip
  rows other transactions hold. They take `BulkParams`, a filter and a
  cursor, and their `BulkResult` says how many tasks they changed and
  skipped, and, if they stopped early, gives the cursor to go on.
  `Client.Count` counts the matching tasks per state, and those each bulk
  action would change. `State.CanRetry`, `CanCancel`, and `CanDelete` say
  which actions a state allows.
- The dashboard's task list offers the bulk actions when filters are set.
  Each leads to a page that shows the filters and the number of tasks it
  changes, and does it.
- `ClientConfig.EnqueueMiddleware`, which wraps every enqueue, for tracing
  and metrics: it sees the tasks' queues and kinds, may add to their
  metadata, and sees their results.
- `QueueInfo.Lag`: how long a queue's oldest due task has waited since its
  run time. The maintainer finds that task when it counts, in migration 2's
  new column, which `Migrate` adds, and `Client.Queues` measures its age.
- `outbox.Delivery.Metadata`: the metadata stored with a message, such as
  its trace context, for the publisher.
- taskmill's errors name their class, such as `invalid_argument` or
  `timeout`, which OpenTelemetry reports as `error.type`.
- The `github.com/code19m/taskmill/otel` module, released on its own: traces
  and metrics through OpenTelemetry. See its [changelog](otel/CHANGELOG.md).
- Releases publish `taskmill-ui` binaries for Linux, macOS, and Windows, and
  a container image, `ghcr.io/code19m/taskmill-ui`, for amd64 and arm64, with
  checksums, SBOMs, and build provenance attestations.
- `taskmill-ui` logs its version when it starts.
- The shop example, `examples/shop`: a small shop that uses most features,
  with the dashboard. Run it with `go run ./examples/shop`.
- `Worker.Health`, for readiness probes: nil while `Run` runs and its last
  heartbeat reached the database within `LeaseDuration`. It reads only
  memory.
- A guide section, [Running on Kubernetes](docs/production/kubernetes.md):
  shutdown and probes, Deployments for workers and `taskmill-ui`,
  migrations, connection budgets, PgBouncer, autoscaling with KEDA, and
  managed PostgreSQL.

### Changed

- **Breaking:** `ListParams` holds its filters in a `Filter TaskFilter`
  field, which the bulk actions and `Count` take too, instead of its own
  `Queues`, `States`, `Kinds`, and `GroupKeys` fields. To migrate, move them
  into `Filter`: `&taskmill.ListParams{Filter: taskmill.TaskFilter{Queues:
  q}, Limit: 100}`.
- Logs follow one level policy: Error when a person must act, Warn when
  taskmill recovers on its own, Info for lifecycle, Debug for routine events
  of single tasks. A dead task now logs at Error, a retry at Debug, and a
  failed dashboard request at Warn. The [log catalog](docs/reference/log-messages.md)
  lists every message.
- An operation that a worker repeats, such as a fetch or a heartbeat, no
  longer logs at every failed try while the database is down. It logs
  `operation failed` once, `operation still failing` at most once a minute,
  and `operation recovered` when it works again. These replace messages such
  as `fetch failed` and `maintenance step failed`.
- A task that dies because no worker has a handler for its kind, or because
  its last attempt was rescued, is now logged, and a maintainer logs when it
  loses the role.
- The maintainer counts every known queue, also one without tasks, so
  `QueueInfo.RefreshedAt` shows when it last counted an empty queue too.
- `Client.List`, `Client.Groups`, `Client.Activity`, and the schedule methods
  return `ErrInvalidArgument` for a filter, search, or name longer than any
  task or schedule can have. They returned an empty result, or `ErrNotFound`
  for a schedule.
- The 1 MiB payload limit counts each number at the length PostgreSQL writes
  it out, as it stores `1e6` as `1000000`. A short payload of numbers with
  large exponents no longer stores a value that is too large to fetch, which
  blocked its queue.

### Deprecated

- `ClientConfig.ContextMetadata`, which v0.3.0 removes. It now runs as the
  outermost enqueue middleware, once per enqueue call. Enqueue middleware does
  the same; this one keeps the metadata that the enqueue sets, as before:

  ```go
  EnqueueMiddleware: []taskmill.EnqueueMiddleware{
      func(ctx context.Context, tasks []*taskmill.EnqueueTask, next func(context.Context) error) error {
          for _, task := range tasks {
              for k, v := range contextMetadata(ctx) { // your former ContextMetadata function
                  if _, set := task.Metadata[k]; !set {
                      task.Metadata[k] = v
                  }
              }
          }
          return next(ctx)
      },
  },
  ```

### Fixed

- A worker that the maintainer removed as stale, after a pause such as its
  machine's sleep, lists itself again at its next heartbeat. It went on
  running tasks but stayed missing from `Client.Workers` and the dashboard,
  which could also mean no maintainer was shown.
- `Client.List`, `Client.Groups`, `Client.Activity`, and the schedule methods
  return `ErrInvalidArgument` for a filter, search, or name that is not valid
  UTF-8 or holds a NUL character. They returned a database error, and the
  dashboard showed a server error.
- Enqueueing returns `ErrInvalidArgument` for a payload that is not valid
  UTF-8, such as a `json.RawMessage` that holds a request body in another
  encoding, or that holds a number beyond the range of PostgreSQL's
  `numeric` type, before the query, so the caller's transaction stays
  usable. A payload that PostgreSQL itself rejects, such as a string with an
  unpaired surrogate escape, returns `ErrInvalidArgument` too. All returned
  a database error.
- `Migrate` works for a role that owns its schema but may not create schemas
  in the database. It ran `CREATE SCHEMA IF NOT EXISTS` every time, and
  PostgreSQL checks that privilege before it checks whether the schema
  exists, so the role could not install or upgrade taskmill.
- Through PgBouncer in transaction mode with `max_prepared_statements` set,
  `Migrate` failed on a new schema, and `NewClient` could fail right after it
  found a schema not migrated. PgBouncer keeps the name of a prepared
  statement whose parse failed, and disconnects the client that prepares it
  again ([pgbouncer#1630](https://github.com/pgbouncer/pgbouncer/issues/1630)).
  taskmill now reads the schema version without a prepared statement.
- `Migrate` called by several processes at once on a new schema could fail
  with a duplicate key on the schema's name, when their connections were
  already open, as a running application's are. A migration that waited for
  another's lock trusted its connection's stale view that the schema did not
  exist yet.
- Recording an attempt's result and renewing leases could read the whole
  index of running tasks, which holds an entry for every task that ran since
  the last vacuum, when the statistics had been taken while few tasks ran.
  With millions of tasks waiting, a completion read about 4,000 pages instead
  of 240, and a heartbeat 8,600. They now look each task up by its ID.
- Promoting due tasks could scan the whole `tasks` table every second, when
  many tasks were scheduled far ahead while others waited available: with 10
  million scheduled tasks, each promotion took 1.2 seconds instead of under a
  millisecond. It now stays on its index.

## [0.1.0] - 2026-10-25

Initial release.

### Added

- Transactional enqueue, alone or in batches, inside your own pgx or
  `database/sql` transaction, with priorities, delays, deduplication, and
  metadata.
- Workers with typed handlers, batch handlers, middleware, retries with
  backoff, timeouts, snoozing, cancellation, and graceful shutdown. Dead
  tasks are kept for inspection and retry.
- FIFO groups that run tasks one at a time per key, and halt on a dead task.
- Concurrency limits per worker and across all workers, and pausing of
  queues.
- Cron schedules, enqueued at most once per scheduled time across workers,
  which operators can also run on demand.
- Transactional completion, for exactly-once effects in the same database.
- An operations API, and a live web dashboard: the `ui` package, an
  `http.Handler` with optional basic authentication that can also be shown in
  another application's frame, and the `taskmill-ui` command that serves it on
  its own.
- A day of activity history per minute: task counts and completed and failed
  attempts, from `Client.Activity` and in the dashboard's charts.
- The `outbox` package, a transactional outbox that publishes in order per
  key, and the `taskmilltest` package, helpers to test your code against a
  real PostgreSQL.

[Unreleased]: https://github.com/code19m/taskmill/compare/v0.13.0...HEAD
[0.13.0]: https://github.com/code19m/taskmill/compare/v0.12.2...v0.13.0
[0.12.2]: https://github.com/code19m/taskmill/compare/v0.12.1...v0.12.2
[0.12.1]: https://github.com/code19m/taskmill/compare/v0.12.0...v0.12.1
[0.12.0]: https://github.com/code19m/taskmill/compare/v0.11.1...v0.12.0
[0.11.1]: https://github.com/code19m/taskmill/compare/v0.11.0...v0.11.1
[0.11.0]: https://github.com/code19m/taskmill/compare/v0.10.0...v0.11.0
[0.10.0]: https://github.com/code19m/taskmill/compare/v0.9.0...v0.10.0
[0.9.0]: https://github.com/code19m/taskmill/compare/v0.8.0...v0.9.0
[0.8.0]: https://github.com/code19m/taskmill/compare/v0.7.0...v0.8.0
[0.7.0]: https://github.com/code19m/taskmill/compare/v0.6.0...v0.7.0
[0.6.0]: https://github.com/code19m/taskmill/compare/v0.5.0...v0.6.0
[0.5.0]: https://github.com/code19m/taskmill/compare/v0.4.0...v0.5.0
[0.4.0]: https://github.com/code19m/taskmill/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/code19m/taskmill/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/code19m/taskmill/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/code19m/taskmill/releases/tag/v0.1.0
