# The shop example

A small online shop that runs taskmill and its dashboard in one program. It
places an order every second, and some tasks fail on purpose, so the
dashboard always has something to show. Read [main.go](main.go) top to
bottom: short comments name the feature that each part shows.

## Run it

You need Go and Docker. From the repository's root:

```sh
make db
go run ./examples/shop
```

Then open the dashboard at <http://localhost:8090/taskmill/>. Ctrl-C stops
the shop: the workers finish their running tasks first.

The shop connects to `TASKMILL_DATABASE_URL`, by default the PostgreSQL that
`make db` starts. It keeps its tables and taskmill's in one schema,
`taskmill_example_shop`, and touches no other. It **drops that schema at
every start**, so each run begins afresh.

## What it shows

| Feature | In the shop |
|---|---|
| Enqueue in your own transaction | An order, its tasks, and its outbox messages commit together (`placeOrder`) |
| Queues and defaults per kind | `EnqueueDefaults` puts each kind in its own queue |
| Retries with backoff | One receipt in five fails at first, and is retried |
| Dead tasks | A receipt without an email address fails with `taskmill.Permanent` |
| FIFO groups | Ledger entries post one at a time per account |
| Deduplication | A receipt's `DedupeKey` admits one receipt per order, even if enqueued twice |
| Priorities and delays | A critical receipt; reminders two minutes after each order |
| Batch handlers | Images resize 25 at a time |
| A limit across all workers | At most two partner calls run at once (`UpdateQueue`) |
| Snoozing | The partner sometimes says "slow down" (`taskmill.Snooze`) |
| Cron schedules | A sales report every minute |
| A kind no worker handles | `legacy.invoice` waits, as during a rolling deploy |
| The outbox | Two events per order, published in order per key |
| Middleware | `logSlow` logs attempts that take over 1.5 seconds |
| Several workers | A second worker serves only emails and images, as another process would |
| Graceful shutdown | Handlers stop when their context ends; Ctrl-C drains the workers |

## Where to look in the dashboard

- **Overview.** The queues and their counts. The charts fill in over the
  first minutes.
- **Dead tasks.** Choose *Dead* on the overview, or open
  [tasks?state=dead](http://localhost:8090/taskmill/tasks?state=dead). Every
  17th order's receipt has no email address. Order 13's ledger entry and its
  first outbox message are dead too.
- **A halted FIFO group.** Order 13 has a negative amount, so its ledger
  entry is dead and halts the group `account-2`: that account's later entries
  wait, in order. Open the dead entry and choose *View group*. Retrying fails
  again, as the amount is still negative; delete the entry, and the group
  resumes.
- **The outbox.** The `outbox` queue holds the stored messages. The broker
  rejects order 13's first one, so its key halts the same way: the
  `order.paid` message waits behind the dead `order.placed` one.
- **The schedule.** *Schedules* shows `sales-report`, with its next and last
  run. *Run now* enqueues a run at once; *Pause* stops it.
- **The rest.** The `default` queue's scheduled reminders, the `partner-api`
  queue's limit of 2, the `legacy.invoice` task snoozed with a `no_handler`
  note, and the two workers under *Workers*, one of them the maintainer.
