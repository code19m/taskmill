# Rows that taskmill did not write

A role that enqueues has `INSERT` on `tasks`, and an operator can change a
row of any table by hand. So a row can hold what no release writes. This
page says what becomes of such a row. [Storage schema](schema.md) has the
tables.

## The rule

> **A value that no release writes fails where it is written, not where it
> is read.** Where the database cannot refuse it, the reader takes it as
> absent. A value that taskmill can read is obeyed.

| Table | A value that no release writes | Why |
|---|---|---|
| `workers`, `schedules`, `settings` | Is refused by a `CHECK` constraint | The tables are small and cold, and every value in them is taskmill's own |
| `leader` | Counts as no lease | A constraint cannot read the clock |
| `tasks`, and `groups` beside it | Fails alone: the code survives it | No constraint can state the rule, or it costs the hot table more than it gives |
| `queue_stats` | Fails the pages that show the queues, for 10 seconds at most | The maintainer replaces every row |

**Obeyed** means that the rule is about values, not about intent. A role
that can write these tables can pause every queue, or delete the one row of
`settings`. Such a role is trusted.

## The small tables refuse it

Read by the code, one such value fails for everyone. An infinite check-in
time fails the list of the workers. A queue that is `NULL` fails every
statistics refresh. An infinite next run fails the read of every schedule.
A negative retention is the worst: it can be read, so the maintainer obeys
it, and deletes the dead and canceled tasks.

So the constraints refuse these values, and the hand or the tool that
writes one gets the error, with the constraint's name:

| Constraint | It refuses |
|---|---|
| `workers_times_check` | An infinite `started_at` or `heartbeat_at` |
| `workers_names_check` | `queues` or `kinds` with a `NULL`, or with more than one dimension |
| `workers_lease_check` | A `lease_duration` below zero or above 300 years, or with a negative field |
| `schedules_times_check` | An infinite `next_run_at` or `last_run_at` |
| `settings_retention_check` | A retention period below 1 hour or above 300 years, or with a negative field |

- **A `NULL` passes a `CHECK`.** So a worker of a release that stores no
  lease is valid, and so is a schedule that never ran.
- **The limits are those of the API, with room.** `UpdateSettings` accepts
  a period from 1 hour. The longest Go duration is about 292 years, which
  is how Go code says "for good". A period between that and 300 years,
  which only SQL can write, reads as the longest duration.
- **No field of a period may be negative.** PostgreSQL stores a period's
  months, days, and microseconds apart, each with its own sign. It compares
  periods by their totals, with months of 30 days. So a range alone accepts
  `-3000000 mons 90000000 days 1 hour` as 1 hour. But a calendar month is
  not 30 days: a time moved back by that period is 3,600 years ahead, and
  a retention of it deletes what just finished.
- **The readers count on the limits.** The statements that move a time
  back by a lease, or by a retention period, have no guard of their own.
- **The migration repairs first.** It locks each table, repairs the rows
  that break the rule, and adds the constraint, which reads the table once:
  a few pages. [Upgrading](../production/upgrading.md#what-a-migration-repairs)
  says what becomes of each row.

## The election ignores a lease that no worker writes

No constraint can refuse a lease in `leader` that ends too late: a `CHECK`
cannot read the clock. Obeyed, such a row keeps every worker from becoming
the maintainer for as long as it says.

A worker writes a lease that ends one lease from now, 10 seconds, by the
database's clock. So the election takes another worker's lease for one only
if it ends within two leases from now. A lease that ends later counts as
none, and the next try takes the role. A row written by hand can then stop
the maintainer for 20 seconds at most.

- **Why two leases, not one.** It leaves room for a release that changes
  the lease. The workers of both releases take each other's leases for real
  ones if the new lease is at most twice, and at least half, the old one.
- **Two maintainers for a moment** are possible if a release changes the
  lease by more. The maintainer's work is safe for that: a lease never gave
  exclusive access.

## A task's row fails alone

One statement fetches, counts, or rescues the tasks of many queues. It must
not fail for all of them because of one row.

| A row with | What the code does |
|---|---|
| `metadata` that is not an object of strings | The worker that fetches the task makes it dead, with the code `invalid_row`. A list shows it without metadata |
| `errors` that are not an array of entries | They count as none, and the next failure replaces them. A list shows the entries that decode |
| An infinite `created_at` or `run_at` | As for metadata. A list shows no time. The statistics count the task, and take no lag from it |
| A `lease_token` at the largest integer | The fetch counts no further, and the worker makes the task dead with `invalid_row` |
| No attempt left, while it waits in a queue | The fetch counts the attempt up to the maximum, so the task runs once more |
| A `groups` row that counts fewer tasks than its group holds | The statements that take a task out of the group count down to zero, not below, so they succeed for the other groups of their batch. This group can lose its order, or keep tasks pending |
| A `groups` row whose halt names a task that is not its dead head | The head still runs. The halt ends when the named task leaves the group, as for a dead head. This group can lose its order, or keep tasks pending |

- **A fetched row always becomes an attempt.** If one row failed the read,
  the worker would lose every task that the statement leased with it. An
  attempt whose row does not decode runs no code of the application: not
  the handler, and not the middleware, whose result would decide what
  becomes of the task.
- **A bad row can wait for good,** in a queue that no worker reads. Every
  refresh of the statistics meets it there, so the statements that the
  maintainer runs for all queues survive it too.
- **No `CHECK` constraint refuses these rows.** A constraint cannot refuse
  an object with a value that is no string, or tell a lease token that was
  counted from one that was written: the code must survive such rows
  anyway. And a constraint on `tasks` costs more than it gives. The
  migration that adds it locks the busiest table. PostgreSQL then checks an
  old row whenever a statement updates it, so until every old row is
  validated, one that breaks the rule fails each fetch, promotion, and
  rescue that touches it.
- **What a role may write is a matter of grants.** A role that only
  enqueues gets `INSERT` on the columns that an enqueue writes
  ([Split work across services](../recipes/split-across-services.md)), so it
  cannot write a lease. A role with `INSERT` on the whole table can write a
  row that looks leased, which holds a place under `MaxRunning` until its
  lease ends. Such a role is trusted.

## The statistics are replaced, not guarded

`queue_stats` has neither a constraint nor a reader that survives its rows.
Its values are copied from `tasks`, so a constraint there would turn one
odd task into a failed refresh for every queue. And the maintainer replaces
every row within 10 seconds. Until then, a row that cannot be read fails
the pages that show the queues.

## See also

- [Storage schema](schema.md): the tables and their indexes
- [Maintenance and notifications](schema-maintenance.md): the election
- [Queue statistics and health](schema-statistics.md): how the maintainer replaces the statistics
- [Upgrading](../production/upgrading.md): what a migration repairs in your tables

**Next:** [The API's shape](api-shape.md)
