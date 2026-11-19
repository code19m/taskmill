# Lists by creation time

This page explains the third order of `Client.List`: a list with a range of
creation times comes by creation time, and reads two indexes of that time.
It says why such a list is not in ID order, what a page reads, and what the
indexes cost. [Lists and counts](schema-lists.md) has the other two orders,
and [`sql.go`](../../sql.go) the exact statement.

## IDs follow creation time only roughly

A task's creation time is the start of the transaction that enqueued it:
`created_at` is `now()`. Its ID comes from a sequence at the moment of the
insert. A transaction that stays open, and enqueues at its end, leaves a
task with an old creation time and a new ID. So the tasks of one hour are
not the tasks of one range of IDs, and a list in ID order cannot read them
with a bounded read:

- **A filter by creation time in ID order reads tasks until the page is
  full.** It walks down from the newest ID, through every task of another
  time. In the history, it reads each day's partition to its end before it
  knows that the partition holds no task of the range. Measured below: a
  page of an hour 3 days back read 508,072 of 652,000 tasks.
- **A range of IDs cannot stand for a range of creation times.** The
  statement could find the IDs at both ends of the range with a few probes
  of the primary key, and read about a page with no new index. But the
  measured table has one long transaction: its 2,000 tasks were created 5
  days back and have the newest IDs, above 341,196 tasks that were created
  later. A range of IDs for that hour leaves those 2,000 tasks out, or
  spans all the others.

Only an index by creation time finds such a task. So a range of creation
times has indexes of that time, and the list is in their order.

## Two indexes, by queue and creation time

| Index | Key | Rows it holds |
|---|---|---|
| `tasks_created_idx` | `queue, created_at, id` | Every task of `tasks` that is not running |
| `task_history_created_idx` | `queue, created_at, id` | Every completed task |

A list with a range of creation times is ordered by creation time, the
latest first, and by `id` among the tasks of one creation time. That is the
order of both indexes within one queue:

- **One range for each queue, and then a merge,** as in a
  [list in finish order](schema-lists.md#dead-and-canceled-tasks-page-by-finish-time).
  Each range is read backward from the page's position, up to a page. The
  indexes end with `id`, so a range ends exactly with its page: tasks that
  share a creation time, as all tasks of one transaction do, cost nothing
  more.
- **Three places hold tasks,** and the statement reads a place only if the
  list asks for its states: the tasks that are not running, the running
  ones, and the history.
- **The index of `tasks` leaves out the running tasks,** so a fetch writes
  no entry to it. A queue runs as many tasks as its workers take at a time.
  The statement reads all of them from `tasks_running_idx`, and sorts those
  of the range.
- **With no queue named, each place finds its own queues,** with one index
  probe for each queue, and for each day of the history.
- **The bounds are conditions of the index under any plan.** The start of
  the range and the page's position are never `NULL`: a range without a
  start begins at `-infinity`, and a first page starts below `infinity`.
  The end of the range is not a condition of its own. The first page starts
  below it instead. Beside a position, PostgreSQL would start to read at
  the position, and compare every entry down to the end of the range.
- **The plan does not depend on statistics.** Sorts and JIT are off, as for
  the finish order. Sequential and bitmap scans are off too, for the running
  tasks: where the statistics expected every task to run, a plan read all
  6,630 tasks of a table to find the 30 running ones.
- **The cursor holds the creation time and the ID** of the page's last
  task, the time as PostgreSQL stores it. Each of the three orders refuses
  the cursors of the other two.
- **Any list takes the range, whatever its states.** Every task has a
  creation time, and the two indexes hold every task. A list of only dead
  tasks takes it too, and is then in creation order. A list has one order:
  a range of creation times together with a range of finish times is an
  invalid argument.
- **The bounds are instants, rounded up to the microsecond,** as those of a
  [range of finish times](schema-lists.md#a-range-of-finish-times).

## What a page reads

Measured on PostgreSQL 18 with 652,000 tasks of 10 days in 103 queues:
501,049 completed ones in a history of 16 partitions, and 150,951 in
`tasks`. The queue `emails` has 40% of the tasks and runs 640, and `q042`
has 1 task in 290. A page has 50 tasks:

| List | Range | Rows | Buffers |
|---|---|---|---|
| Completed, `emails` | An hour 3 days back | 51 | 46 |
| Completed, `emails` | 10 minutes 8 days back | 51 | 47 |
| Completed, `emails` | The last 7 days | 58 | 54 |
| Completed, `q042` | 7 days that ended 2 days back | 58 | 99 |
| Every state, `q042` | 7 days that ended 2 days back | 109 | 152 |
| Every state, `emails` | An hour 3 days back | 742 | 497 |
| Scheduled, `emails` | An hour 3 days back | 277 | 38 |
| `emails`, a kind that 1 task in 500 has | An hour 3 days back | 1,787 | 605 |
| `emails`, a kind with no task | The last 7 days | 193,645 | 20,915 |
| Completed, `emails` | 10 days with no task | 0 | 41 |
| Completed, all queues | An hour 3 days back | 1,939 | 9,011 |
| Every state, all queues | An hour 3 days back | 3,048 | 10,387 |
| Every state, all queues | The last 7 days | 13,001 | 19,774 |

A page of one queue reads the same wherever its range lies and however long
it is. The 742 rows of `emails` are a page from each of its two indexes,
and its 640 running tasks. With no queue named, most of the buffers are the
3,522 index probes that find the 103 queues, and each queue's place in each
partition. `TestRangesOfCreationTimesReadOnlyTheirRows` guards these
bounds, with a custom plan and with a generic one.

## What the creation order costs

- **Every task writes two more index entries:** one when it is enqueued,
  and one in the history when it completes. Measured with small payloads,
  1,000 tasks at a time through enqueue, fetch, and completion: 12,939 WAL
  records and 1.46 MB of WAL, where the same tasks wrote 10,929 records and
  1.24 MB without the two indexes. That is 18% more. The fetch writes what
  it wrote before. An entry takes about 42 bytes: 6.3 MB for 150,951 tasks,
  and 21 MB beside the history's 135 MB.
- **`List` has three orders, and a cursor belongs to the order that made
  it.** The range chooses the order with the states, so a list changes its
  order when it gets a range. The two orders differ only for tasks whose
  IDs do not follow their creation times.
- **With no queue named, a page reads up to a page of each queue,** from
  each place, and probes each day of the history for each queue. If that
  hurts, name the queue. An index without the queue would serve this list
  alone: see the options below.
- **A list that includes running tasks reads every running task of its
  queues,** on each page.
- **A filter by kind, by FIFO group, or by some states of `tasks` reads the
  range until the page is full:** all of it for a kind that the range does
  not hold. It never reads a task outside the range, or of another queue.
- **The migration builds both indexes in its transaction,** with a plain
  `CREATE INDEX`. Reads continue. Enqueues, fetches, and completions wait
  while it reads the tables: about half a second for each million tasks.
  `CREATE INDEX CONCURRENTLY` cannot run in a transaction, and needs a step
  outside `Migrate`, which taskmill does not have yet.

## Options we rejected

| Option | Why not |
|---|---|
| The range as a filter in ID order, with no index | Not bounded: 508,072 rows and 20,664 buffers for the page of an hour 3 days back, and more for an older hour |
| A range of IDs, found by probing the primary key at both ends of the range | Not exact: see the long transaction above. To find its tasks, each row would need a flag, set at the insert, that says "created long before its ID", and an index of the flagged rows by creation time. The flag trusts the database's clock, and a row written by hand has none. A page would also read every task with other creation times that lies between its own in ID order |
| Indexes on `created_at, id`, without the queue | A page of all queues reads only its rows: 51, where the chosen indexes read 1,939. But a queue's page reads the tasks of every queue until it is full: 18,964 rows for the completed tasks of `q042` in 7 days, and all 369,524 of the range for a queue with less than a page in it. That grows with the table, and a small queue beside a busy one is common |
| Both pairs of indexes | Four entries for each task. The second pair would only make the list of all queues cheaper, which the first pair already bounds |
| `tasks_created_idx` with the running tasks | One list fewer to merge, and no running task read from outside the range. But each fetch then writes an index entry: 28% more WAL records for a task, not 18% |
| The stored creation time as the time of the insert, which follows the IDs more closely | Still not exact: two transactions get their IDs and their times in different orders. And `created_at` would change its meaning for every reader |
| Every list that is not in finish order in creation order | One order fewer in the API. But the lists that read an index in ID order today would each need an index in creation order, and `List` would change its order for every caller |

## See also

- [Lists and counts](schema-lists.md): the ID order, the finish order, and counts
- [Storage schema](schema.md#indexes-on-tasks): the indexes on `tasks`
- [Manage tasks from code](../guides/operations-api.md#find-tasks): the user's view

**Next:** [Operator actions](schema-operations.md)
