# Run a task at each customer's local time

**Situation.** "Every customer gets a daily summary at 09:00 in the
customer's own time zone. We have customers in 30 time zones, and new
customers sign up every day."

## Which should I use?

| Situation | What to use | Why |
|---|---|---|
| One time for everyone | A `Schedule` with a `Location` | [Run tasks on a schedule](../guides/schedules.md) shows how |
| A time that depends on each customer | **A due time per customer in your own table, and one schedule that enqueues what is due** | A new customer, or a new time zone, is only a change to a row |
| One task per event, far in the future, such as a reminder | A task with `RunAt` | See [Cancel or replace a delayed task](cancel-a-delayed-task.md) |

## The recommended way

Keep each customer's time zone and next run time in your own table:

```sql
ALTER TABLE customers
    ADD COLUMN time_zone      text NOT NULL DEFAULT 'UTC', -- such as 'Asia/Tashkent'
    ADD COLUMN next_digest_at timestamptz;                 -- NULL: no digest
CREATE INDEX ON customers (next_digest_at);
```

Define one [schedule](../concepts/glossary.md#schedule) that runs every 15
minutes:

```go
worker, err := taskmill.NewWorker(client, &taskmill.WorkerConfig{
    Handlers: &handlers,
    Schedules: []taskmill.Schedule{
        {Name: "enqueue-digests", Cron: "*/15 * * * *", Args: EnqueueDigests{}},
    },
})
```

Its handler enqueues one task for each customer whose time has come. In the
same transaction, it moves that customer to the next 09:00:

```go
const pageSize = 1000 // customers per transaction

// nextNine is SQL for the next 09:00 on the customer's own clock.
const nextNine = `(date_trunc('day', (now() AT TIME ZONE time_zone) - interval '9 hours')
    + interval '1 day 9 hours') AT TIME ZONE time_zone`

func (p *DigestPlanner) Handle(ctx context.Context, t *taskmill.Task[EnqueueDigests]) error {
    for {
        n, err := p.enqueuePage(ctx)
        if err != nil || n < pageSize {
            return err
        }
    }
}

func (p *DigestPlanner) enqueuePage(ctx context.Context) (n int, err error) {
    err = pgx.BeginFunc(ctx, p.pool, func(tx pgx.Tx) error {
        rows, err := tx.Query(ctx, `UPDATE customers SET next_digest_at = `+nextNine+`
            WHERE id IN (
                SELECT id FROM customers WHERE next_digest_at <= now()
                ORDER BY next_digest_at LIMIT $1
                FOR UPDATE SKIP LOCKED)
            RETURNING id`, pageSize)
        if err != nil {
            return err
        }
        ids, err := pgx.CollectRows(rows, pgx.RowTo[int64])
        if err != nil || len(ids) == 0 {
            return err
        }
        params := make([]taskmill.EnqueueParams, len(ids))
        for i, id := range ids {
            params[i] = taskmill.EnqueueParams{Args: SendDigest{CustomerID: id}}
        }
        n = len(ids)
        _, err = p.client.EnqueueManyTx(ctx, taskmill.Pgx(tx), params)
        return err
    })
    return n, err
}
```

`SendDigest` is an ordinary task with its own handler. To start a customer's
digests, or after a change of the time zone, set the time with the same SQL:

```go
_, err := pool.Exec(ctx, `UPDATE customers SET next_digest_at = `+nextNine+` WHERE id = $1`, customerID)
```

## Why

- **The new time and the task commit together.** So each customer gets
  exactly one task per day. That stays true when two runs overlap, or when a
  run fails in the middle.
- **A late run misses nobody.** The query takes every customer with a time
  in the past, not only the customers that are due in this minute. After an
  outage of 3 days, each customer gets one digest, not three.
- **Your table is the timetable.** A new customer, or a new time zone, is
  only a change to the customer's row. No task waits that you would have to
  find and replace.
- **PostgreSQL knows the time zones,** and their daylight saving time.
- **Every 15 minutes is enough.** All time zones differ from UTC by a
  multiple of 15 minutes, so each customer's 09:00 falls on a run.

## Other options

| Option | When it fits | The cost |
|---|---|---|
| A `Schedule` per time zone, each with its `Location` | A few zones that you know when you deploy | A new zone needs a deploy: a worker's schedules are fixed when it starts |
| A task per customer that enqueues its own next run with `RunAt` | The next time depends on what the last run did | The chain ends silently when a task becomes dead or is canceled. A change of time zone must cancel and replace the waiting task |
| The same schedule every minute: `* * * * *` | Customers choose their own time, such as 09:10 | 1,440 runs a day. A run with nothing due is one index lookup |

## Watch out

- **Check the time zone name when you save it.** One name that PostgreSQL
  doesn't know makes the whole query fail, for every customer. Accept a
  name only if PostgreSQL's view `pg_timezone_names` has it.
- **09:00 is when the task is enqueued, not when it runs.** A large time
  zone enqueues thousands of tasks in the same minute. Give them a queue of
  their own if they must not delay other work.
- **The schedule runs only while a
  [maintainer](../concepts/glossary.md#maintainer) defines it.** Give every
  worker the same `Schedules`
  ([Run tasks on a schedule](../guides/schedules.md)).

## See also

- [Run tasks on a schedule](../guides/schedules.md): cron expressions, time zones, and missed runs
- [Use your own transactions](../guides/transactions.md): enqueue in the transaction that changes your data
- [`Schedule`](https://pkg.go.dev/github.com/code19m/taskmill#Schedule) and [`Client.EnqueueManyTx`](https://pkg.go.dev/github.com/code19m/taskmill#Client.EnqueueManyTx) on pkg.go.dev

**Next:** [Move a cron job to a schedule](move-a-cron-job-to-a-schedule.md)
