# Rows that taskmill did not write

taskmill supports only the rows that its releases write. But a role with
`INSERT` on a table, or an `UPDATE` by hand, can write a row that no
release writes. This page says what becomes of such a row.
[Storage schema](schema.md) has the tables.

## The rule

> **A value that no release writes fails where it is written, if the
> database can refuse it. Otherwise it may fail what reads it, but never a
> whole queue or the maintainer's work.**

| Table | A value that no release writes | Why |
|---|---|---|
| `workers`, `schedules`, `settings` | Is refused by a `CHECK` constraint | The tables are small and cold, and every value in them is taskmill's own |
| `tasks`, `groups`, `leader` | Is contained by a few cheap guards; any other read that meets it fails, with an error | One row must not stop a queue, the rescue, the statistics, or the election. A constraint there costs more than it gives |
| `queue_stats` | Fails the pages that show the queues, for 10 seconds at most | The maintainer replaces every row |

A value that taskmill can read is obeyed. A role that can write these
tables can pause every queue, or delete the one row of `settings`. Such a
role is trusted.

## The small tables refuse it

Read by the code, one such value fails for everyone. An infinite check-in
time fails the list of the workers, and a queue that is `NULL` fails every
statistics refresh. A negative retention is the worst: the maintainer can
read it, so it obeys it, and deletes the dead and canceled tasks. So the
constraints refuse these values, with the constraint's name in the error:

| Constraint | It refuses |
|---|---|
| `workers_times_check` | An infinite `started_at` or `heartbeat_at` |
| `workers_names_check` | `queues` or `kinds` with a `NULL`, or with more than one dimension |
| `workers_lease_check` | A `lease_duration` below zero or above 300 years, or with a negative field |
| `workers_schedules_check` | `schedules` or `schedule_hashes` with a `NULL`, or with more than one dimension, or two lists of different lengths, or one without the other |
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
- **The migration repairs first.** Migration 6 locks each table, repairs
  the rows that break the rule, and adds the constraint, which reads the
  table once: a few pages. The [changelog](../../CHANGELOG.md) of 0.8.0
  says what becomes of each row.

## The other tables contain it

These guards are the only ones. Each costs a comparison in a statement
that the code runs anyway:

| A row written by hand with | Guard | What it contains |
|---|---|---|
| `metadata` that is not an object of strings, or an infinite `created_at` or `run_at` | The worker makes the task dead with the code `invalid_row`, and runs no handler or middleware (`scanLeased`) | The fetch of every task that was leased with it |
| A `lease_token` at the largest integer | The fetch counts no further, and the worker makes the task dead with `invalid_row` (`leaseSet`) | The same. A release gets there too, after two billion fetches of one task |
| No attempt left, while it waits in a queue | The fetch counts the attempt up to the maximum, so the task runs once more (`leaseSet`) | The same |
| `errors` that are not an array | They count as none, and the next failure replaces them (`errorsTrimmedSQL`) | The rescue of every expired lease |
| A `groups` row that counts fewer tasks than its group holds | The count stops at zero (`recountGroupsSQL`) | A rescue, or a completion of other groups |
| An infinite `run_at` | It is no oldest task, and gives no lag (`countStatsSQL`) | The statistics of every queue |
| A lease in `leader` that ends far ahead | It counts as no lease (`electMaintainerSQL`) | The election of a maintainer |

Any other read that meets such a row fails, with an error that names the
task and the column: `Client.Get`, every list that holds the task, such as
the dead tasks once a worker made it dead, and their dashboard pages. The
rest of the dashboard works. The worker's log line for a task that became
dead with `invalid_row` names the value. Correct the row, or delete it with
`Client.Delete`: a retry ends the same way until then.

- **Why no `CHECK` on `tasks`.** A constraint cannot refuse an object with a
  value that is no string, or tell a lease token that was counted from one
  that was written. And it costs the busiest table more than it gives: the
  migration that adds it locks the table, and each fetch, promotion, and
  rescue then checks the old rows that it touches.
- **What a role may write is a matter of grants.** A role that only
  enqueues gets `INSERT` on the columns that an enqueue writes
  ([Split work across services](../recipes/split-across-services.md)), so it
  cannot write a lease.

## The election ignores a lease that no worker writes

No constraint can refuse a lease in `leader` that ends too late: a `CHECK`
cannot read the clock. Obeyed, such a row keeps every worker from becoming
the maintainer for as long as it says.

A worker writes a lease that ends one lease from now, 10 seconds, by the
database's clock. So the election takes another worker's lease for one only
if it ends within two leases from now. A row written by hand can then stop
the maintainer for 20 seconds at most.

- **Why two leases, not one.** It leaves room for a release that changes
  the lease. The workers of both releases take each other's leases for real
  ones if the new lease is at most twice, and at least half, the old one.
- **Two maintainers for a moment** are possible if a release changes the
  lease by more. The maintainer's work is safe for that: a lease never gave
  exclusive access.

## The statistics are replaced, not guarded

`queue_stats` has neither a constraint nor a reader that survives its rows.
Its values are copied from `tasks`, so a constraint there would turn one
odd task into a failed refresh for every queue. And the maintainer replaces
every row within 10 seconds.

## See also

- [Storage schema](schema.md): the tables and their indexes
- [Maintenance and notifications](schema-maintenance.md): the election
- [Upgrading](../production/upgrading.md): what a migration repairs in your tables

**Next:** [Schema migrations](schema-migrations.md)
