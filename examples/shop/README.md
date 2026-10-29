# The shop example

A small online shop that runs taskmill and its dashboard in one program. It
places an order every second. Some orders are bad on purpose, so the
dashboard always has something to show.

Read [main.go](main.go) from top to bottom: short comments name the feature
that each part shows.

## Run it

You need Go and Docker. From the repository's root:

```sh
make db
go run ./examples/shop
```

Then open the dashboard at <http://localhost:8090/taskmill/>. Press Ctrl-C
to stop the shop: the workers finish their running tasks first.

| Setting | Value |
|---|---|
| Database | `SHOP_DATABASE_URL`, or by default the PostgreSQL that `make db` starts |
| Schema | `taskmill_example_shop`, for the shop's tables and taskmill's. The shop touches no other schema. |
| Data | Kept when you restart the shop |

To start again from zero, stop the shop and drop its schema:

```sh
docker exec taskmill-test-db psql -U postgres -d taskmill -c 'DROP SCHEMA taskmill_example_shop CASCADE'
```

> [!WARNING]
> The dashboard can retry, delete, and pause, so a real application puts it
> behind its own login. In main.go, `requireAdmin` marks the place.

## What it shows

| Feature | In the shop |
|---|---|
| Enqueue in your own transaction | An order, its tasks, and its outbox messages commit together (`placeOrder`). |
| Queues and defaults per kind | `EnqueueDefaults` puts each kind in its own queue. |
| Retries with backoff | One receipt attempt in five fails, and taskmill tries it again later. |
| Idempotent handlers (safe to run twice) | A ledger entry is saved in the same transaction that completes its task (`CompleteTx`). A partner call's idempotency key is the task ID, with the service and the schema. |
| Dead tasks | A receipt without an email address returns `taskmill.Permanent`, so it fails for good, with no retry. |
| FIFO groups | Each account's ledger entries post one at a time, in order. |
| Deduplication | A receipt's `DedupeKey` allows one receipt per order, even if it is enqueued twice. |
| Priorities and delays | Every 10th order is a VIP: its receipt runs first. A reminder runs two minutes after each order. |
| Batch handlers | Images resize 25 at a time. |
| A limit across all workers | At most two partner calls run at the same time (`UpdateQueue`). |
| Snoozing | Sometimes the partner says "slow down" (`taskmill.Snooze`). |
| Cron schedules | A sales report every minute. |
| A kind no worker handles | The first order after each start enqueues a `legacy.invoice` task. No worker handles it, so it waits, as it would during a rolling deploy. |
| The outbox | Two events per order, published in order per key. |
| Middleware | `logSlow` logs attempts that take over 1.5 seconds. |
| Several workers | A second worker takes tasks only from the `emails` and `images` queues, as another process would. |
| The dashboard in your application | Mounted at `/taskmill/` on the shop's own server. |
| Graceful shutdown | Handlers stop when their context ends. Ctrl-C lets the workers finish their running tasks first. |

## Where to look in the dashboard

- **Overview.** The queues and their counts. The charts fill in during the
  first minutes.
- **Dead tasks.** Choose *Dead* on the overview, or open
  [tasks?state=dead](http://localhost:8090/taskmill/tasks?state=dead). Every
  17th order's receipt has no email address. The 13th order after each start
  has a negative amount: its ledger entry and its `order.placed` message are
  dead too.
- **A halted FIFO group.** A group is
  [halted](../../docs/concepts/glossary.md#halted-group) when its first task
  is dead: the tasks behind it wait. The bad order's ledger entry halts the
  group `account-2`, so the account's later entries wait, in order. Open the
  dead entry and choose *View group*. *Retry* fails again, because the
  amount is still negative. Delete the entry, and the group runs again.
- **The outbox.** The `outbox` queue holds the stored messages. The broker
  rejects the bad order's `order.placed` message, so that order's key halts
  the same way: its `order.paid` message waits behind the dead one.
- **The schedule.** *Schedules* shows `sales-report`, with its next and last
  run. *Run now* enqueues a run right away; *Pause* stops it.
- **The rest.** Also look at:
  - the scheduled reminders in the `default` queue;
  - the limit of 2 on the `partner-api` queue;
  - the `legacy.invoice` task, snoozed with a `no_handler` note;
  - the two workers under *Workers*. One of them is the
    [maintainer](../../docs/concepts/glossary.md#maintainer): the worker
    that taskmill picks for shared work, such as schedules.

## See also

- [Quickstart](../../docs/start/quickstart.md): build your first program
  step by step
- [The dashboard](../../docs/guides/dashboard.md): mount it in your own
  application
- [How do I…?](../../docs/recipes/README.md): the recommended way for common
  situations

**Next:** [Guides](../../docs/guides/README.md)
