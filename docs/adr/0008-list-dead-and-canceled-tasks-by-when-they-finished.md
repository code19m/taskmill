# 0008. List dead and canceled tasks by when they finished

- Status: Proposed
- Date: 2026-11-16

## Context

The list that an operator needs most in an incident is "the dead tasks", of
one queue or of all. `Client.List` ordered every list by ID, newest first.
No index holds dead or canceled tasks in ID order. So PostgreSQL either
walked the primary key backward through every newer task of another state,
or read every dead task and sorted. Which one, the statistics decided.

Both plans grow with a backlog and with dead tasks, which is an incident.
So the list reached its 5-second limit when it was needed. Measured on
PostgreSQL 18 with 61,500 tasks, 10,000 of them dead and older than 10,000
other tasks, for a page of 50: 11,551 rows for the dead tasks of all
queues, and every one of the 61,500 for a queue that has none.

## Decision

**A list of only dead tasks, only canceled tasks, or both is ordered by
when the tasks finished, latest first,** and by ID among tasks that finished
at the same time. Every other list keeps its ID order.

**It reads the index that retention already needs,**
`tasks_finished_idx (queue, state, finalized_at)`: for each listed queue
and state, one range, backward, up to a page. With no queue named, it finds
the queues that have such tasks with one probe each, as retention does, and
merges their pages. There is no new index and no migration.

**The plan does not depend on statistics.** No other index has this order,
so the planner has no second ordered plan to prefer. And two settings of
the list's transaction keep it from the plans that are left:

- **Sorts are off,** so the planner cannot read a whole queue's dead tasks
  and sort them. It did where it expected few matches. With a filter by a
  kind that had just started to fail, 300 of its queue's 20,300 dead tasks,
  a page read 20,300 rows, and 52 with sorts off.
- **JIT is off.** The merge of the queues is a sort that the page needs.
  Until PostgreSQL 18, a sort that is off makes its plan look costly enough
  to compile: about 100 ms on PostgreSQL 15, for a read of under 1 ms.

**A range ends with its last finish time whole.** The index has no ID, so
the statement takes every task of the page's last finish time (`FETCH FIRST
… WITH TIES`), and the merge orders them by ID. The first design sorted
each range by ID, with an incremental sort, which returns the same page.
But it leaves a sort inside each range for the planner to choose. With
sorts on, PostgreSQL 15 then sorted the whole queue when its statistics
expected few dead tasks. And it needs incremental sorts to be on in the
database's configuration.

**The cursor holds the finish time and the ID** of the page's last task. It
is a second kind of cursor: one kind with a filter of the other kind is an
invalid argument. It holds the time as PostgreSQL stores it, so a list gets
past a task whose finish time an `UPDATE` by hand made infinite.

**`Client.Count` and the bulk actions do not change.** A count reads each
state through its index and stops at 10,000 tasks. A bulk action must go
through tasks in ID order: its cursor is an ID, and a retry keeps each FIFO
group in order by ID. Its windows of 100,000 tasks bound it.

Measured on the table above, for a page of 50:

| List | Before | After |
|---|---|---|
| Dead, all queues | 11,551 rows, 565 buffers | 106 rows, 54 buffers |
| Dead, the queue with 8,000 of them | 11,565 rows, 566 buffers | 52 rows, 19 buffers |
| Dead, a queue with none | 61,500 rows, 3,419 buffers | 0 rows, 8 buffers |
| Dead and canceled, all queues | 10,051 rows, 490 buffers | 158 rows, 65 buffers |

"After" is the same with fresh statistics, with statistics that know of no
dead task, and with a plan that knows neither the filters nor the size of
the page. PostgreSQL 15 reads the same rows, and the test that guards
these bounds passes on PostgreSQL 15, 16, and 18.

The finish order is also the better order for this list. In ID order, a
task that died a second ago can be many pages down, behind tasks that were
enqueued later and died earlier. In finish order, what dies now is on the
first page.

### Options we rejected

The numbers in this table are from the design, measured on 336,500 tasks
with 50,000 dead.

| Option | Why not |
|---|---|
| An index in ID order, such as `(queue, state, id)` and `(state, id)` on dead and canceled tasks | Not enough alone: with both indexes built, the unchanged list still walked the primary key (46,500 rows), because PostgreSQL expected many dead tasks and priced the primary key lower. And `Migrate` cannot build an index on `tasks`: a plain `CREATE INDEX` blocks every write while it reads the table, and `CREATE INDEX CONCURRENTLY` cannot run in a transaction, waits for every older transaction, and can leave an invalid index. Something would have to build it after the upgrade, and the list would stay slow until then. Each finished task would also cost two more index entries. |
| Keep ID order with today's indexes, in a statement that always reads the dead tasks and sorts | Bounded by the dead tasks, not by the table: 50,000 rows and 6,296 buffers. It grows with the dead tasks, which is the incident. |
| A page that reads a fixed number of tasks and may come back short, as the bulk actions do | The first page of dead tasks could be empty with more to come. That is not a list an operator can use. |
| An option to choose the order | Two orders for every caller to choose from, and the ID order of dead tasks would still have no index. |

## Consequences

- `List` has two orders. Lists of dead and canceled tasks show the latest
  finished first. This is a breaking change for a caller that relied on
  their ID order, and it comes before v1.0.0 on purpose: an index can be
  added later, an order cannot be changed.
- A cursor belongs to the order that made it. A cursor that the previous
  release made for a list of dead or canceled tasks is invalid, also during
  a rolling deploy. The dashboard then shows one "Invalid request" page,
  which names the cursor and links to the list's first page.
- A list across all queues reads up to one page for each queue that has
  such tasks: 10,707 rows and 11,524 buffers when 202 queues have dead
  tasks, where one of those queues alone reads 52 rows. If that hurts, the
  statement can first read the latest finish time of each queue, one probe
  each, and then read pages only from the queues that can reach the page.
  An index on `(state, finalized_at, id)` would serve the same order and
  the same cursor exactly. It needs a way to build an index outside
  `Migrate`, which this decision avoids. ADR 0007 met the same limit.
- Tasks that one transaction finished share a finish time, and a page reads
  them together: up to 1,000 for a batch of a bulk action or of rescue
  (1,001 rows for a first page of 50). A page reads the tasks of the finish
  time where it starts, and those of the finish time where it ends: up to
  two such batches. Many `CancelTx` calls in one transaction, or an
  `UPDATE` by hand, can make a larger group. Its page reads all of it,
  within the 5-second limit.
- A finish time is the start of a transaction, not its commit, so tasks do
  not show up in the order of their finish times. A walk through the pages
  can miss a task that finishes meanwhile, and can return twice a task that
  is retried and finishes again. A caller that polls for new dead tasks
  reads again from a little before the latest finish time that it saw, and
  drops the IDs that it has.
- A filter by kind or FIFO group reads a queue's dead or canceled tasks
  until the page is full: all of them in a queue that has none of that
  kind. It never reads a task of another state.
- A list that mixes dead or canceled tasks with other states, and a list of
  one unfinished state across all queues, are as slow as before.
