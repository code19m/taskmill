package taskmill

import (
	"strconv"
	"strings"
)

// This file holds what the statements share, and the table that fills in
// their values. Each statement is a constant in the file of its topic, as
// the design pages tell them apart, and each file's header links its page:
// https://github.com/code19m/taskmill/blob/main/docs/design/schema-statements.md
// has the lifecycle's key statements.
//
// A statement's comment starts with its parameters. A placeholder in braces
// stands for a value: {s} the schema, {batch} maintenanceBatch, {window}
// bulkWindow, {cap} countCap, {keptErrors} keptErrors, and {available},
// {cancel}, {heartbeat}, and {resigned} the start of a notification's
// payload. A fragment that statements share is a constant of its own, and
// the statements add it to their text.

// statements holds the statements of the task lifecycle and of the
// operations API, with the values filled in, so each has fixed text and can
// be prepared once per connection. Statements that build names at run time,
// such as those for history partitions, live with their callers. The session
// settings that start a transaction need no schema: they are in
// sql_session.go.
type statements struct {
	schemaVersion string

	// sql_enqueue.go: enqueue tasks.
	enqueue      string
	enqueuePlain string
	dedupeLookup string

	// sql_attempts.go: fetch tasks, and record how their attempts end.
	fetch            string
	fetchLimited     string
	fetchKind        string
	leaseTask        string
	atLimit          string
	lockQueue        string
	countRunning     string
	renewLeases      string
	cancelRequest    string
	complete         string
	completeSkipping string
	regainKeys       string
	lockLeased       string
	fail             string
	snooze           string
	release          string

	// sql_groups.go: change the rows of FIFO groups.
	haltGroup         string
	lockGroups        string
	recountGroups     string
	deleteEmptyGroups string
	promoteGroups     string
	lockPendingGroup  string
	enterGroup        string
	resumeHaltedHead  string
	releaseHalts      string
	createGroups      string

	// sql_operations.go: what an operator changes: tasks, queues, settings, and schedules.
	lockTask         string
	takeKeys         string
	keyHolder        string
	releaseKeys      string
	deleteTask       string
	deleteHistory    string
	taskExists       string
	lastTask         string
	windowEnd        string
	pickRetry        forms
	pickCancel       forms
	pickDelete       forms
	retryTasks       string
	cancelTasks      string
	deleteTasks      string
	pickCompleted    forms
	deleteCompleted  string
	readQueue        string
	swapQueue        string
	deleteEmptyQueue string
	pauseQueue       string
	queueSettings    string
	readSettings     string
	swapSettings     string
	runSchedule      string
	pauseSchedule    string

	// sql_maintenance.go: the maintainer's work, and the workers' rows.
	upsertWorker     string
	checkInWorker    string
	deleteWorker     string
	electMaintainer  string
	resignMaintainer string
	promoteDue       string
	wakeQueues       string
	rescue           string
	rescueBackoff    string
	deleteFinished   string
	deleteDedupeKeys string
	deleteWorkers    string
	deleteActivity   string
	expiredHistory   string
	pruneSchedules   string
	otherSchedules   string
	loadSchedules    string
	upsertSchedule   string
	advanceSchedule  string

	// sql_lists.go: read tasks, groups, queues, workers, and schedules.
	getTask         string
	listTasks       string
	listFinished    string
	listCreated     string
	countTasks      forms
	listGroups      string
	listQueueGroups string
	listQueues      string
	listWorkers     string
	listSchedules   string

	// sql_stats.go: count the queues, and keep their activity.
	countStats    string
	lockStats     string
	clearStats    string
	storeStats    string
	queueStats    string
	storeActivity string
	addActivity   string
	activity      string
}

// forms are the two forms of a statement that reads tasks of a range of
// times: one that reads them as it would without a range, and one that
// reads the range queue by queue. See rangeWithoutQueue.
type forms struct{ plain, queues string }

// choose returns the form that reads queue by queue if fromQueues holds,
// and the plain one if not.
func (f forms) choose(fromQueues bool) string {
	if fromQueues {
		return f.queues
	}
	return f.plain
}

func newStatements(schema string) *statements {
	q := strings.NewReplacer(
		"{s}", schema,
		"{batch}", strconv.Itoa(maintenanceBatch),
		"{window}", strconv.Itoa(bulkWindow),
		"{cap}", strconv.Itoa(countCap),
		"{keptErrors}", strconv.Itoa(keptErrors),
		"{available}", notifyAvailable+"|",
		"{cancel}", notifyCancel+"|",
		"{heartbeat}", notifyHeartbeat+"|",
		"{resigned}", notifyResigned+"|",
	).Replace

	return &statements{
		schemaVersion: q(schemaVersionSQL),

		enqueue:      q(enqueueSQL),
		enqueuePlain: q(enqueuePlainSQL),
		dedupeLookup: q(dedupeLookupSQL),

		fetch:            q(fetchSQL),
		fetchLimited:     q(fetchLimitedSQL),
		fetchKind:        q(fetchKindSQL),
		leaseTask:        q(leaseTaskSQL),
		atLimit:          q(atLimitSQL),
		lockQueue:        q(lockQueueSQL),
		countRunning:     q(countRunningSQL),
		renewLeases:      q(renewLeasesSQL),
		cancelRequest:    q(cancelRequestSQL),
		complete:         q(completeSQL),
		completeSkipping: q(completeSkippingSQL),
		regainKeys:       q(regainKeysSQL),
		lockLeased:       q(lockLeasedSQL),
		fail:             q(failSQL),
		snooze:           q(snoozeSQL),
		release:          q(releaseSQL),

		haltGroup:         q(haltGroupSQL),
		lockGroups:        q(lockGroupsSQL),
		recountGroups:     q(recountGroupsSQL),
		deleteEmptyGroups: q(deleteEmptyGroupsSQL),
		promoteGroups:     q(promoteGroupsSQL),
		lockPendingGroup:  q(lockPendingGroupSQL),
		enterGroup:        q(enterGroupSQL),
		resumeHaltedHead:  q(resumeHaltedHeadSQL),
		releaseHalts:      q(releaseHaltsSQL),
		createGroups:      q(createGroupsSQL),

		lockTask:         q(lockTaskSQL),
		takeKeys:         q(takeKeysSQL),
		keyHolder:        q(keyHolderSQL),
		releaseKeys:      q(releaseKeysSQL),
		deleteTask:       q(deleteTaskSQL),
		deleteHistory:    q(deleteHistorySQL),
		taskExists:       q(taskExistsSQL),
		lastTask:         q(lastTaskSQL),
		windowEnd:        q(windowEndSQL),
		pickRetry:        forms{q(pickRetrySQL), q(pickRetryQueuesSQL)},
		pickCancel:       forms{q(pickCancelSQL), q(pickCancelQueuesSQL)},
		pickDelete:       forms{q(pickDeleteSQL), q(pickDeleteQueuesSQL)},
		retryTasks:       q(retryTasksSQL),
		cancelTasks:      q(cancelTasksSQL),
		deleteTasks:      q(deleteTasksSQL),
		pickCompleted:    forms{q(pickCompletedSQL(false)), q(pickCompletedSQL(true))},
		deleteCompleted:  q(deleteCompletedSQL),
		readQueue:        q(readQueueSQL),
		swapQueue:        q(swapQueueSQL),
		deleteEmptyQueue: q(deleteEmptyQueueSQL),
		pauseQueue:       q(pauseQueueSQL),
		queueSettings:    q(queueSettingsSQL),
		readSettings:     q(readSettingsSQL),
		swapSettings:     q(swapSettingsSQL),
		runSchedule:      q(runScheduleSQL),
		pauseSchedule:    q(pauseScheduleSQL),

		upsertWorker:     q(upsertWorkerSQL),
		checkInWorker:    q(checkInWorkerSQL),
		deleteWorker:     q(deleteWorkerSQL),
		electMaintainer:  q(electMaintainerSQL),
		resignMaintainer: q(resignMaintainerSQL),
		promoteDue:       q(promoteDueSQL),
		wakeQueues:       q(wakeQueuesSQL),
		rescue:           q(rescueSQL),
		rescueBackoff:    q(rescueBackoffSQL),
		deleteFinished:   q(deleteFinishedSQL),
		deleteDedupeKeys: q(deleteDedupeKeysSQL),
		deleteWorkers:    q(deleteWorkersSQL),
		deleteActivity:   q(deleteActivitySQL),
		expiredHistory:   q(expiredHistorySQL),
		pruneSchedules:   q(pruneSchedulesSQL),
		otherSchedules:   q(otherSchedulesSQL),
		loadSchedules:    q(loadSchedulesSQL),
		upsertSchedule:   q(upsertScheduleSQL),
		advanceSchedule:  q(advanceScheduleSQL),

		getTask:         q(getTaskSQL),
		listTasks:       q(listTasksSQL),
		listFinished:    q(listFinishedSQL),
		listCreated:     q(listCreatedSQL),
		countTasks:      forms{q(countTasksSQL(false)), q(countTasksSQL(true))},
		listGroups:      q(listGroupsSQL),
		listQueueGroups: q(listQueueGroupsSQL),
		listQueues:      q(listQueuesSQL),
		listWorkers:     q(listWorkersSQL),
		listSchedules:   q(listSchedulesSQL),

		countStats:    q(countStatsSQL),
		lockStats:     q(lockStatsSQL),
		clearStats:    q(clearStatsSQL),
		storeStats:    q(storeStatsSQL),
		queueStats:    q(queueStatsSQL),
		storeActivity: q(storeActivitySQL),
		addActivity:   q(addActivitySQL),
		activity:      q(activitySQL),
	}
}

// schemaVersionSQL reads the version of the schema, and the isolation level
// that the pool's transactions start with.
const schemaVersionSQL = `
SELECT coalesce(max(version), 0), current_setting('default_transaction_isolation') FROM {s}.migrations`

// keptErrors is how many of a task's most recent attempt errors are kept.
const keptErrors = 20

// errorsTrimmedSQL is a task's errors without the oldest entry once it holds
// keptErrors, so that appending one keeps at most keptErrors. Errors written
// by hand that are not an array count as none, so that they cannot fail the
// rescue of every expired lease.
const errorsTrimmedSQL = "(CASE WHEN jsonb_typeof(errors) <> 'array' THEN '[]' WHEN jsonb_array_length(errors) >= " +
	"{keptErrors} THEN errors - 0 ELSE errors END)"

// errorsUnnotedSQL is a task's errors without a no_handler note at their
// end. The note says that the task waits for a worker with a handler for its
// kind: the wait is over once such a worker has the task, or an operator
// retries it.
const errorsUnnotedSQL = "(CASE WHEN (errors -> -1) ->> 'code' = '" + string(CodeNoHandler) + "' THEN errors - (-1) ELSE errors END)"

// nowRFC3339SQL is the database's time as an error entry stores it: text, in
// UTC, in the format of RFC 3339. As a value, jsonb would write it with the
// offset of the session's time zone: an entry would read differently for
// each writer, and Go cannot read every offset that PostgreSQL accepts, such
// as one of more than a day.
const nowRFC3339SQL = `to_char(now() AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"')`

// nowUTCSQL is the database's time in UTC, without a time zone. A retention
// period is subtracted from it, not from now(): there, a period's days are
// days of the session's time zone, which are 23 or 25 hours long when its
// clocks change.
const nowUTCSQL = "(now() AT TIME ZONE 'UTC')"

// leasedSQL checks that a task is leased. The statements that find tasks by
// their attempts look each one up by ID, and compare the state as text, so
// that the planner cannot read tasks_running_idx instead: statistics taken
// while few tasks ran make that index look small, but until vacuum it keeps
// an entry for every task that ran. Those that take a batch of attempts join
// from the attempts to the tasks, one lookup per attempt. The planner
// expects very few running tasks, so when the tasks are matched against the
// list of attempts instead, it compares every task with every attempt.
const leasedSQL = "state::text = 'running'"

// taskFilterSQL matches the tasks t of queues $1, kinds $2, and FIFO groups $3;
// an empty filter matches every task.
const taskFilterSQL = `(cardinality($1::text[]) = 0 OR t.queue = ANY($1::text[]))
  AND (cardinality($2::text[]) = 0 OR t.kind = ANY($2::text[]))
  AND (cardinality($3::text[]) = 0 OR t.group_key = ANY($3::text[]))`

// queuesWith is a recursive CTE called name: the queues that have a task
// whose state meets cond, in the order of their names. cond is the condition
// of the state's index, such as "= 'available'". That index leads with the
// queue, so each queue is found with one probe of it, however many tasks the
// queue has. The last row is NULL: a statement leaves it out with
// "queue IS NOT NULL".
//
// Every statement that needs the queues of a state finds them with this
// text. A copy written in other words would return the same queues, but
// could read every task of the state to find them.
func queuesWith(name, cond string) string {
	return queuesIn(name, `{s}.tasks`, `state `+cond)
}

// queuesIn is queuesWith for the rows of table that meet where: a condition
// on the row's own columns, which an index that leads with the queue holds.
// In the history, a probe reads each partition.
func queuesIn(name, table, where string) string {
	return name + ` AS (
    (SELECT queue FROM ` + table + ` WHERE ` + where + ` ORDER BY queue LIMIT 1)
    UNION ALL
    SELECT (SELECT t.queue FROM ` + table + ` t WHERE ` + where + ` AND t.queue > x.queue ORDER BY t.queue LIMIT 1)
    FROM ` + name + ` x WHERE x.queue IS NOT NULL
)`
}

// queuesOfStates is a query of the queues that have a task in one of the
// states of the text array states, with queuesWith, each queue once: those
// that have a task that is not running, if states names such a state, and
// those that have a running task, if it names running.
func queuesOfStates(states string) string {
	return `WITH RECURSIVE ` + queuesWith(`waiting`, `<> 'running'`) + `, ` + queuesWith(`running`, `= 'running'`) + `
        SELECT queue FROM waiting WHERE queue IS NOT NULL AND cardinality(array_remove(` + states + `, 'running')) > 0
        UNION
        SELECT queue FROM running WHERE queue IS NOT NULL AND 'running' = ANY(` + states + `)`
}

// historyQueues is a query of the queues of the completed tasks in history
// that finished at or after a day before since, a time or NULL, with
// queuesIn. See finishedSinceSQL.
func historyQueues(since string) string {
	return `WITH RECURSIVE ` + queuesIn(`kept`, `{s}.task_history`, finishedSinceSQL(`finalized_at`, since)) + `
        SELECT queue FROM kept WHERE queue IS NOT NULL`
}

// finishedSinceSQL matches the completed tasks whose finish time, column,
// is at or after a day before since, a time or NULL: those that a range of
// creation times from since can hold. A task finishes after it was created,
// and the day covers a clock that moved back, as after a failover to a
// server whose clock was behind. Partitions of history that end earlier are
// left out of the plan: without it, a statement that reads history queue
// by queue reads each partition once for each queue.
func finishedSinceSQL(column, since string) string {
	return `(` + since + ` IS NULL OR ` + column + ` >= ` + since + ` - interval '1 day')`
}

// inQueues matches the tasks t of the queues that the query queues returns.
// Named, the queues let the planner read each one's part of an index that
// leads with the queue. See rangeWithoutQueue.
func inQueues(queues string) string {
	return `t.queue = ANY(ARRAY(` + queues + `))`
}

// wholeIn is a FROM clause that reads the rows c of read, a query of the
// tasks t, whole, in the queues that the query queues returns. OFFSET 0
// keeps the planner from reading the rows in the order of another index, to
// stop early, as it would for an ORDER BY and a LIMIT around the query: it
// takes matching rows to be spread over that order.
func wholeIn(read, queues string) string {
	return `(` + read + ` AND ` + inQueues(queues) + `
    OFFSET 0) c`
}
