# Lists and counts

This page explains the statements behind `Client.List`, `Client.Count`, and
the dashboard's lists: the orders of a list, why dead and canceled tasks
come by finish time, and what each read costs.
[Operator actions](schema-operations.md) has the statements that change
tasks, and [`sql_lists.go`](../../sql_lists.go) has the exact text of these.

## A list has one of three orders

`Client.List` and the dashboard page through tasks with keyset pagination:
each page starts below the position where the previous one ended. A list
is in one of three orders, and each order has its own statement. All run
with a 5-second statement timeout, and with custom query plans
(`plan_cache_mode = force_custom_plan`), so the planner sees which filters
are set.

| The list holds | Its order | A page reads |
|---|---|---|
| Any tasks, in a range of creation times | By creation time, latest first, then by `id` | The two indexes of creation time: see [Lists by creation time](schema-creation-order.md) |
| Only dead tasks, only canceled tasks, or both | By finish time, latest first, then by `id` | `tasks_finished_idx`, and no other task |
| Anything else | By `id`, newest first | The index that holds its tasks in ID order. Without one, the primary key |

## Most lists page by ID

Unfinished tasks and history are read in two branches, each limited on its
own, and merged. Two things let the planner pick the index that gives a
page in ID order:

- **A single queue, kind, or group is compared with `=`,** not with a list
  of one value. Only then does the planner see that an index which leads
  with that column gives its rows in the order of the next column: the
  history's `(queue, id)` for one queue's completed tasks, and
  `tasks_pending_idx` for the waiting tasks of one queue's group.
- **Each selected state is tested as a literal,** so the planner sees which
  states are left, and which partial indexes hold them.

Where no index holds a list's tasks in ID order, a page reads the primary
key backward: every newer task of any queue and state, until it is full. A
list of one unfinished state across all queues is such a list, and so is
one that mixes dead tasks with other states. The time limit bounds them.

## Dead and canceled tasks page by finish time

The list that an operator needs most in an incident is "the dead tasks", of
one queue or of all. No index holds dead or canceled tasks in ID order. In
that order, PostgreSQL walks the primary key backward through every newer
task of another state, or reads every dead task and sorts: its statistics
decide which. Both plans grow with a backlog and with the dead tasks, which
is an incident: the list would reach its time limit when it is needed.

So a list of only dead tasks, only canceled tasks, or both pages by when
the tasks finished, latest first, and by `id` among the tasks of one
finish time. `tasks_finished_idx` holds them by queue, state, and finish
time, and retention needs that index anyway. The list reads it, and no
other task, with no new index and no migration:

- **One range for each queue and state,** backward from the page's
  position and up to a page, and then a merge of the ranges. With no queue
  named, the statement first finds the queues that hold such tasks, with
  one index probe each, as retention does.
- **A range ends with its last finish time whole** (`WITH TIES`). The index
  has no `id`, so the merge is what orders the tasks of one finish time. So
  a page also reads the tasks that share the finish time where it ends,
  and those of the finish time where it starts.
- **The plan does not depend on statistics.** No other index has this
  order, so the planner has no second ordered plan to prefer. Two settings
  of the list's transaction keep it from the plans that are left.
- **Sorts are off** (`enable_sort`), so each range is read in index order
  and ends with its page. With sorts on, the planner reads every dead task
  of a queue and sorts them where it expects few matches. For a kind that
  had just started to fail, with 300 of its queue's 20,300 dead tasks, a
  page of 50 read 20,300 rows, and 52 with sorts off.
- **JIT is off.** Until PostgreSQL 18, a sort that is off makes a plan look
  costly, and the merge is a sort that the page needs. The database then
  takes about 100 ms, on PostgreSQL 15, to compile a read of under 1 ms.
- **The cursor holds the finish time and the ID** of the page's last task,
  the time as PostgreSQL stores it. So a list gets past an infinite finish
  time, which only an `UPDATE` by hand writes. Each order refuses the
  cursors of the others, as an invalid argument.

The finish order is also the better order for this list. In ID order, a
task that died a second ago can be many pages down, behind tasks that were
enqueued later and died earlier. In finish order, what dies now is on the
first page.

Measured on PostgreSQL 18 with 61,500 tasks, 10,000 of them dead and older
than 10,000 other tasks, for a page of 50:

| List | In ID order | In finish order |
|---|---|---|
| Dead, all queues | 11,551 rows, 565 buffers | 106 rows, 54 buffers |
| Dead, the queue with 8,000 of them | 11,565 rows, 566 buffers | 52 rows, 19 buffers |
| Dead, a queue with none | 61,500 rows, 3,419 buffers | 0 rows, 8 buffers |
| Dead and canceled, all queues | 10,051 rows, 490 buffers | 158 rows, 65 buffers |

The finish order reads the same with fresh statistics, with statistics that
know of no dead task, and with a plan that knows neither the filters nor
the size of the page. PostgreSQL 15 reads the same rows, and
`TestPagesOfFinishedTasksReadOnlyTheirRows` guards these bounds on every
supported PostgreSQL version.

### What the finish order costs

- **`List` has several orders, and a cursor belongs to the order that made
  it.** For a cursor of another order, the dashboard shows one "Invalid
  request" page, which names the cursor and links to the list's first page.
  The order is part of what
  [v1.0.0 freezes](../reference/compatibility.md): an index can be added
  later, but an order cannot be changed.
- **With no queue named, a page reads up to a page of each queue that holds
  such tasks:** 10,707 rows and 11,524 buffers when 202 queues hold dead
  tasks, where one of those queues alone reads 52 rows. If that hurts, the
  statement can first read the latest finish time of each queue, with one
  probe each, and then read pages only from the queues that can reach the
  page. An index on `(state, finalized_at, id)` would serve the same order
  and the same cursor exactly. But it needs a way to build an index outside
  `Migrate`: see the first option [below](#options-we-rejected).
- **Tasks that one transaction finished share a finish time,** which is
  `now()`, the start of the transaction. A page reads them together: up to
  1,000 for a batch of a bulk action or of rescue, so 1,001 rows for a
  first page of 50, and up to two such batches for a later page. Many
  `CancelTx` calls in one transaction, or an `UPDATE` by hand, can make a
  larger group. Its page reads all of it, within the time limit.
- **Tasks do not show up in the order of their finish times,** because a
  finish time is the start of a transaction, not its commit. A walk
  through the pages can miss a task that finishes meanwhile, and can return
  twice a task that is retried and finishes again.
  [Manage tasks from code](../guides/operations-api.md#find-tasks) says how
  a caller polls for new dead tasks.
- **A filter by kind or FIFO group reads a queue's dead or canceled tasks
  until the page is full:** all of them in a queue that has none of that
  kind. It never reads a task of another state, and with a
  [range of finish times](#a-range-of-finish-times), only tasks of the range.

### Options we rejected

The numbers in this table were measured on 336,500 tasks with 50,000 dead.

| Option | Why not |
|---|---|
| An index in ID order, such as `(queue, state, id)` and `(state, id)` on dead and canceled tasks | Not enough alone: with both indexes built, the list in ID order still walked the primary key (46,500 rows), because PostgreSQL expected many dead tasks and priced the primary key lower. And `Migrate` cannot build an index on `tasks`: a plain `CREATE INDEX` blocks every write while it reads the table, and `CREATE INDEX CONCURRENTLY` cannot run in a transaction, waits for every older transaction, and can leave an invalid index. Something would have to build it after the upgrade, and the list would stay slow until then. Each finished task would also cost two more index entries |
| ID order with today's indexes, in a statement that always reads the dead tasks and sorts | Bounded by the dead tasks, not by the table: 50,000 rows and 6,296 buffers. It grows with the dead tasks, which is the incident |
| A page that reads a fixed number of tasks and may come back short, as the bulk actions do | The first page of dead tasks could be empty with more to come. That is not a list an operator can use |
| An option to choose the order | Two orders for every caller to choose from, and the ID order of dead tasks would still have no index |
| Sort each range by `id` with an incremental sort, in place of `WITH TIES` | It returns the same page. But it leaves a sort inside each range for the planner to choose: with sorts on, PostgreSQL 15 then sorted the whole queue when its statistics expected few dead tasks. And it needs incremental sorts to be on in the database's configuration |

### A range of finish times

`ListParams.FinishedSince` and `FinishedBefore` limit a list in finish order
to the tasks that finished from one time up to, but not including, another.
An operator reaches a past period, such as last night, in one request.
Without a range, the hour measured below starts 225 pages down.

- **Each bound is a condition of the index,** on its last column. A queue's
  read starts where the range ends. It stops where the range starts, or
  with the page. Custom plans see which bounds are set, so a list without a
  range keeps its plan. A generic plan would check the bounds as a filter,
  and read past them: `List` never runs one.
- **The cursor and the order stay as they are.** A cursor continues inside
  the range that it is passed with.
- **The bounds are instants.** The column and both parameters are
  `timestamptz`. So neither the location of a `time.Time` nor the session's
  `TimeZone` changes what a range selects.
- **A bound is rounded up to the microsecond.** PostgreSQL keeps
  microseconds, and the driver rounds a finer time down. Rounded down, a
  bound would count a task that finished just before it as one that
  finished at it.
- **A range belongs to the order of its time.** A range bounds a read only
  where the list is in the order of that time. In ID order, a range of any
  time is a filter: the list reads tasks until the page is full, up to the
  time limit. So a range of finish times needs a list in finish order, and
  a range of creation times puts any list in
  [creation order](schema-creation-order.md), with indexes and a cursor of
  its own. No index holds tasks by when they were last attempted.
- **The range is not part of `TaskFilter`.** `Count` and the bulk actions
  take that filter, and a bulk action reads in ID order.

Measured on PostgreSQL 18 with 420,000 tasks, 300,000 of them dead over 30
days: 100,000 in the queue `emails`, and 1,000 in each of 200 other queues.
The range is one hour of a night 3 days back, for a page of 50:

| List | Without a range | With the range |
|---|---|---|
| Dead, `emails` | 52 rows, 5 buffers | 52 rows, 5 buffers |
| Dead, `emails`, a kind that 1 task in 50 has | 2,600 rows, 73 buffers | 138 rows, 8 buffers |
| Dead, `emails`, a FIFO group that 1 task in 100 has | 5,200 rows, 142 buffers | 138 rows, 8 buffers |
| Dead, `emails`, a kind with no dead task | 100,000 rows, 2,673 buffers | 138 rows, 8 buffers |
| Dead, all queues | 10,654 rows, 1,811 buffers | 469 rows, 1,416 buffers |

The hour holds 138 dead tasks of `emails`. A range of 10 minutes holds 23,
and reads 23 rows and 5 buffers. A range that holds no task reads no row
and 3 buffers. With no queue named, the buffers are the index probes that
find the 201 queues, and the end of the range in each. PostgreSQL 15 reads
the same rows, and `TestRangesOfFinishedTasksReadOnlyTheirRows` guards
these bounds.

## A summary leaves out what makes a task large

With `ListParams.Summary`, the statement returns no payload and no
metadata, and of each task's errors only the newest. The planner sees the
parameter, so it drops those columns from the plan, and the database does
not read their values from TOAST.

The dashboard's lists ask for summaries: one page of 50 tasks with payloads
of 1 MiB took 54 MiB of memory to show 50 KB, at every refresh of every
open tab. A task's own page reads the task in full, with `Get`. For a dead
task that halts its group, that page counts the tasks behind it with
`Count`, up to its cap, and does not list them.

## Counts stop at their cap

`Client.Count` has one branch per state, and custom plans leave out the
branches of the states not counted. Sequential and bitmap scans are off for
it, so each branch reads the index of its state and stops at its cap of
10,000 tasks. A count by kind or FIFO group alone still reads every task of
each state.

A count of what an action would change is capped whenever a state that it
counts is. `Cancel` leaves out the running tasks already asked to stop, so
it can count fewer tasks than the cap and still be a lower bound: the tasks
that were not read may be ones that it would cancel.

The finish order changes neither `Count` nor the bulk actions. A bulk
action must go through tasks in ID order: its cursor is an ID, and a retry
keeps each FIFO group in order by ID. Its windows of 100,000 tasks bound
what it reads: see [Operator actions](schema-operations.md).

## Each read is one round trip

`List`, `Count`, `Groups`, and `Activity` send their settings and their
statement as one batch. PostgreSQL runs a batch as one transaction, so the
time limit and the plan settings end with it. The batch has no `BEGIN`:
with one, a read that runs out of time would leave a failed transaction
open, and the pool closes such a connection. `Queues` sends its two
statements the same way.

A dashboard page makes one round trip for each thing it shows: the overview
5, and the task list 2. A connection that sends a statement for the first
time prepares it in a round trip before the batch, outside the time limit:
only a lock on a whole table makes that step wait.

## See also

- [Lists by creation time](schema-creation-order.md): the third order, and its two indexes
- [Operator actions](schema-operations.md): the statements that retry, cancel, and delete tasks
- [Storage schema](schema.md#indexes-on-tasks): the indexes that the lists read
- [Manage tasks from code](../guides/operations-api.md): the user's view

**Next:** [Lists by creation time](schema-creation-order.md)
