package taskmill

import (
	"context"
	"strconv"

	"github.com/jackc/pgx/v5"
)

// This file holds the session settings that start a transaction, and why
// each is set. set_config(..., true) holds a setting until the transaction
// ends, so it never reaches the connection's next transaction, and one
// round trip can carry the settings with the statement that needs them: see
// readAlone.

// readTimeout bounds each read of the operations API that can grow with the
// tasks: a page of tasks or of groups, a count, and the activity. So
// inspection never loads the database for long.
const readTimeout = "5s"

// pageSettings start the read of a page of tasks or of groups. They bound
// the read, so that inspection never loads the database for long. Custom
// plans see which filters are set, and so use the indexes that serve them:
// for a page of groups, the index of halted groups, if only those are asked
// for.
const pageSettings = `SELECT set_config('statement_timeout', '` + readTimeout + `', true), set_config('plan_cache_mode', 'force_custom_plan', true)`

// timeOrderSettings are pageSettings for a page of tasks in the order of a
// time. With sorts off, each queue's tasks are read in the order of the
// index of that time, and the read ends with the page. With sorts on, a plan
// that expects few matches reads every such task of the queue and sorts
// them. It expects few of a kind that just started to fail, whose dead tasks
// are a small part of all tasks, and the same of a plan that does not know
// the size of the page. The sorts that a page needs run either way, such as
// the one that merges the queues. Until PostgreSQL 18, the planner prices a
// sort that is off so high that the database compiles the statement (JIT),
// which takes far longer than the read: so JIT is off too.
const timeOrderSettings = pageSettings + `, set_config('enable_sort', 'off', true), set_config('jit', 'off', true)`

// createdSettings are timeOrderSettings for a page in creation order. That
// page also reads the running tasks of its queues, which no index holds in
// its order: they are sorted either way, so the planner is free in how it
// finds them. With sequential and bitmap scans off, it reads them from their
// own index. Otherwise, where its statistics expect most tasks of a queue to
// run, as after a backlog arrived in a table that held little else, it
// scans the whole table for them.
const createdSettings = timeOrderSettings + `, set_config('enable_seqscan', 'off', true), set_config('enable_bitmapscan', 'off', true)`

// countSettings start a count. Custom plans leave out the branches of the
// states not counted. With sequential and bitmap scans off, each branch reads
// the index of its state and stops at its cap. Otherwise the planner, which
// expects to meet a state's tasks anywhere in the table, may scan the table
// for them, once for each state: far, when those tasks lie together, as
// tasks of one age do. JIT is off, as for the maintainer's statistics: the
// branches make the planner think the count costly, and compiling it takes
// longer than running it.
const countSettings = `SELECT set_config('statement_timeout', '` + readTimeout + `', true),
       set_config('plan_cache_mode', 'force_custom_plan', true), set_config('jit', 'off', true),
       set_config('enable_seqscan', 'off', true), set_config('enable_bitmapscan', 'off', true)`

// activitySettings start the read of the activity, and bound it.
const activitySettings = `SELECT set_config('statement_timeout', '` + readTimeout + `', true)`

// readAlone runs a read after settings, both in one round trip, and calls
// scan with each row of the result. The batch is one transaction without a
// BEGIN, so the settings end with it. And a read that fails, as one does
// that runs out of its time, leaves no open transaction on its connection:
// the pool would close such a connection, and open another for the next
// read.
func (c *Client) readAlone(ctx context.Context, settings, sql string, args []any, scan func(pgx.Rows) error) error {
	b := &pgx.Batch{}
	b.Queue(settings)
	b.Queue(sql, args...).Query(func(r pgx.Rows) error {
		for r.Next() {
			if err := scan(r); err != nil {
				return err
			}
		}
		return nil // Close returns an error of the rows.
	})

	return c.pool.SendBatch(ctx, b).Close()
}

// bulkSettings start each batch of a bulk action. Custom plans see which
// filters are set, and so use the indexes of the states they select. The
// lock timeout bounds each wait of the batch's statements for a row, far
// below the deadlock timeout, so that a batch never holds other transactions
// up for long, and never takes part in a deadlock. It ends before the
// commit: see liftLockTimeout.
const bulkSettings = `SELECT set_config('plan_cache_mode', 'force_custom_plan', true), set_config('lock_timeout', '100ms', true)`

// openSettings are bulkSettings for a batch that reads without a window. The
// database ends a statement of such a batch after openTimeout, so that a
// read that goes too far stops there, and does not continue in the database
// after the batch gave up on it.
var openSettings = bulkSettings + `, set_config('statement_timeout', '` + strconv.FormatInt(openTimeout.Milliseconds(), 10) + `ms', true)`

// windowSettings keep the search for a window's end on the indexes that give
// the tasks in ID order, where it stops at the window's size. Without them,
// a plan that expects few tasks after the position, as a plan made without
// the position does, reads every one of them and sorts them.
const windowSettings = `SELECT set_config('enable_sort', 'off', true)`

// planSettings keep a fetch on its ordered index. A queue's table changes so
// fast that the planner's statistics are often far off: with too low an
// estimate of the available tasks, it may read all of them and sort them, on
// every fetch. Turning sorts and bitmap scans off for the fetch's transaction
// rules that out.
const planSettings = `SELECT set_config('enable_sort', 'off', true), set_config('enable_bitmapscan', 'off', true)`

// limitedSettings are planSettings for the fetch transaction of a queue with
// a MaxRunning limit. They also bound its wait for the queue's row to one
// second: a transaction that holds the row for long, such as a stalled
// worker's, must not tie up a connection of every waiting worker, or keep
// them from stopping, for as long as it holds the row. The timeout can hold
// until the transaction ends: a fetch sends no notification, so its commit
// waits for no lock (see liftLockTimeout).
const limitedSettings = planSettings + `, set_config('lock_timeout', '1s', true)`

// statsSettings start the transaction of a statistics refresh. The count's
// many subqueries make the planner think it costly, so PostgreSQL would
// compile it with JIT at every refresh: about 200 ms on a large table, many
// times the count itself.
const statsSettings = `SELECT set_config('jit', 'off', true)`

// liftLockTimeout is the last statement of a transaction that set a lock
// timeout for its statements and may have sent a notification. Such a
// transaction takes one more lock when it commits: the lock that every
// notifying transaction of the whole server takes, and holds until its
// commit is done, which with synchronous replication includes the wait for
// the standby. Under the timeout, a commit that waits longer for that lock
// fails, and undoes work that was done. The wait cannot be part of a
// deadlock: whoever holds the lock is committing, and asks for nothing more.
const liftLockTimeout = `SELECT set_config('lock_timeout', '0', true)`

// lockTimeoutSQL bounds each wait for a lock of the transaction's later
// statements to $1, such as "5s". A transaction that may notify lifts the
// bound before its commit: see liftLockTimeout.
const lockTimeoutSQL = `SELECT set_config('lock_timeout', $1, true)`
