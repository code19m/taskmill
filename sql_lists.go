package taskmill

import "strings"

// This file holds the statements that read tasks, FIFO groups, queues,
// workers, and schedules for the operations API. The reasons are in
// https://github.com/code19m/taskmill/blob/main/docs/design/schema-lists.md, and
// those of the creation order in
// https://github.com/code19m/taskmill/blob/main/docs/design/schema-creation-order.md.

// $1 task ID.
//
// getTaskSQL reads task $1, unfinished or in history.
var getTaskSQL = `
SELECT ` + taskColumns(`state::text`) + ` FROM {s}.tasks WHERE id = $1
UNION ALL
SELECT ` + taskColumns(`'completed'`) + `
FROM {s}.task_history WHERE id = $1
LIMIT 1`

// $1 queues, $2 states, $3 kinds, $4 group keys, $5 limit, $6 summary, $7 below task ID,
// $8 read the tasks, $9 read the history.
//
// listTasksSQL pages through live tasks and history, newest first. Each
// branch is limited on its own, so both can use their indexes. It runs with
// custom plans, so that the planner sees which filters are set, and its
// filters are written so that the planner also sees a single queue or group
// and the selected states: see inList and inStates. If $6, it returns a
// summary of each task: see listColumns.
var listTasksSQL = `
SELECT ` + taskColumns(`state::text`) + ` FROM (
    (SELECT ` + listColumns(`state::text`) + ` FROM {s}.tasks
     WHERE $8 AND ` + inList(`queue`, `$1::text[]`) + `
       AND (cardinality($2::text[]) = 0 OR ` + inStates(`$2::text[]`) + `)
       AND ` + inList(`kind`, `$3::text[]`) + `
       AND ` + inList(`group_key`, `$4::text[]`) + ` AND id < $7
     ORDER BY id DESC LIMIT $5)
    UNION ALL
    (SELECT ` + listColumns(`'completed'`) + `
     FROM {s}.task_history
     WHERE $9 AND ` + inList(`queue`, `$1::text[]`) + `
       AND ` + inList(`kind`, `$3::text[]`) + `
       AND ` + inList(`group_key`, `$4::text[]`) + ` AND id < $7
     ORDER BY id DESC LIMIT $5)
) page(` + pageColumns + `)
ORDER BY id DESC LIMIT $5`

// $1 queues, $2 states, $3 kinds, $4 group keys, $5 limit, $6 summary, $7 below task ID,
// $8 below finish time, $9 finished since or NULL, $10 finished before or NULL.
//
// listFinishedSQL pages through dead and canceled tasks by when they
// finished, latest first, and by ID among the tasks of one finish time: the
// order of the index of finished tasks within one queue and state. It reads
// one range of that index for each listed queue and state, backward from the
// position ($8, $7) and up to a page, and merges the ranges. With no queue
// named, it finds the queues that hold such tasks with one index probe each.
// A queue named twice is read once.
//
// The state is compared with a value that the planner does not know, so the
// test for both states is what tells it that the index's condition holds. A
// range ends with its last finish time whole (WITH TIES), because the index
// has no ID. A bound of the range of finish times that is set is a condition
// of the index under custom plans. The page on lists says why, and what each
// costs.
var listFinishedSQL = `
WITH RECURSIVE ` + queuesWith(`finished`, `IN ('dead', 'canceled')`) + `, listed AS (
    SELECT queue FROM finished WHERE cardinality($1::text[]) = 0 AND queue IS NOT NULL
    UNION ALL
    SELECT DISTINCT q FROM unnest($1::text[]) AS q
)
SELECT ` + taskColumns(`state::text`) + `
FROM listed AS l(name)
CROSS JOIN unnest($2::text[]) AS selected
CROSS JOIN LATERAL (
    SELECT ` + listColumns(`t.state::text`) + `
    FROM {s}.tasks t
    WHERE t.queue = l.name AND t.state = selected::{s}.task_state AND t.state IN ('dead', 'canceled')
      AND (t.finalized_at, t.id) < ($8::timestamptz, $7::bigint)
      AND ($9::timestamptz IS NULL OR t.finalized_at >= $9)
      AND ($10::timestamptz IS NULL OR t.finalized_at < $10)
      AND ` + inList(`t.kind`, `$3::text[]`) + `
      AND ` + inList(`t.group_key`, `$4::text[]`) + `
    ORDER BY t.finalized_at DESC FETCH FIRST $5 ROWS WITH TIES
) page(` + pageColumns + `)
ORDER BY page.finalized_at DESC, page.id DESC LIMIT $5`

// $1 queues, $2 states, $3 kinds, $4 group keys, $5 limit, $6 summary, $7 below task ID,
// $8 below creation time, $9 created since, $10 read the history.
//
// listCreatedSQL pages through tasks by when they were created, latest
// first, and by ID among the tasks of one creation time: the order of the
// two indexes of creation time within one queue. It reads one range for
// each listed queue ($1), from the position ($8, $7) back to the start of
// the range ($9), up to a page ($5), and merges the ranges. It reads the
// tasks that are not running, the running tasks, and the history only if
// the list asks for their states ($2, $10). The position and the start are
// never NULL, so that both are conditions of the index under any plan. The
// page on creation order says why each part is there.
var listCreatedSQL = `
WITH RECURSIVE ` + queuesWith(`waiting`, `<> 'running'`) + `, ` +
	queuesWith(`running`, `= 'running'`) + `, ` +
	queuesIn(`kept`, `{s}.task_history`, `true`) + `, named AS (
    SELECT DISTINCT q AS queue FROM unnest($1::text[]) AS q
)
SELECT ` + taskColumns(`state`) + `
FROM (
    ` + createdPages(`waiting`, `{s}.tasks`, `t.state::text`, `t.state <> 'running' AND `+inStates(`$2::text[]`),
	`cardinality(array_remove($2::text[], 'running')) > 0`) + `
    UNION ALL
    ` + createdPages(`running`, `{s}.tasks`, `t.state::text`, `t.state = 'running'`, `'running' = ANY($2::text[])`) + `
    UNION ALL
    ` + createdPages(`kept`, `{s}.task_history`, `'completed'`, `true`, `$10`) + `
) page(` + pageColumns + `)
ORDER BY created_at DESC, id DESC LIMIT $5`

// $1 queues, $2 kinds, $3 group keys, $4 states, $5 to $8 the ranges of times.
//
// countTasksSQL counts the tasks in each state of $4 that match the
// filters and the ranges, up to one more than {cap} per state, with how
// many of them were not asked to stop. Each state has a branch of its own,
// run only if $4 lists it, so that each reads the index of its state, or
// the history.
var countTasksSQL = func() string {
	var b strings.Builder
	for _, s := range allStates {
		from := `SELECT t.cancel_requested_at FROM {s}.tasks t WHERE t.state = '` + string(s) + `' AND ` + timeRangesSQL(5) + ` AND `
		if s == StateCompleted {
			from = `SELECT NULL AS cancel_requested_at FROM {s}.task_history t WHERE ` + historyRangeSQL(5) + ` AND `
		}
		if b.Len() > 0 {
			b.WriteString("\nUNION ALL")
		}
		b.WriteString(`
SELECT '` + string(s) + `', n, unflagged FROM (
    SELECT count(*) AS n, count(*) FILTER (WHERE c.cancel_requested_at IS NULL) AS unflagged
    FROM (` + from + taskFilterSQL + ` LIMIT {cap} + 1) c) x
WHERE '` + string(s) + `' = ANY($4::text[])`)
	}
	return b.String()
}()

// $1 search, $2 limit, $3 halted only, $4 after queue, $5 after group key.
//
// listGroupsSQL reads a page of FIFO groups in the order of the primary
// key, after group ($4, $5) where the last page ended, so a page with
// no search reads only its own rows. Halted groups only ($3) read the
// index of halted groups, in the same order.
const listGroupsSQL = `
SELECT queue, group_key, tasks, coalesce(halted_task_id, 0) FROM {s}.groups
WHERE tasks > 0
  AND ($1 = '' OR strpos(lower(group_key), lower($1)) > 0)
  AND (NOT $3 OR halted_task_id IS NOT NULL)
  AND (queue, group_key) > ($4, $5)
ORDER BY queue, group_key
LIMIT $2`

// $1 to $5 as in listGroupsSQL, $6 queues.
//
// listQueueGroupsSQL reads a page of listGroups in queues $6 only. Each
// queue from the cursor's on reads its own groups in key order, up to
// a page, from where the last page ended, so a page reads no group
// of an earlier one.
const listQueueGroupsSQL = `
SELECT g.queue, g.group_key, g.tasks, coalesce(g.halted_task_id, 0)
FROM (SELECT DISTINCT unnest($6::text[])) AS u(q),
LATERAL (
    SELECT g.queue, g.group_key, g.tasks, g.halted_task_id FROM {s}.groups g
    WHERE g.queue = u.q AND g.group_key > CASE WHEN u.q = $4 THEN $5 ELSE '' END
      AND g.tasks > 0
      AND ($1 = '' OR strpos(lower(g.group_key), lower($1)) > 0)
      AND (NOT $3 OR g.halted_task_id IS NOT NULL)
    ORDER BY g.group_key
    LIMIT $2
) g
WHERE u.q >= $4
ORDER BY g.queue, g.group_key
LIMIT $2`

// listQueuesSQL lists every known queue, by name: those with settings, those
// that workers read, and those that the latest statistics counted, each with
// its settings and how many workers read it.
const listQueuesSQL = `
WITH served AS (
    SELECT q AS name, count(*)::int AS workers FROM {s}.workers, unnest(queues) AS q GROUP BY q
), names AS (
    SELECT name FROM {s}.queues
    UNION SELECT queue FROM {s}.queue_stats
    UNION SELECT name FROM served
)
SELECT n.name, coalesce(q.max_running, 0), q.paused_at IS NOT NULL, coalesce(s.workers, 0)
FROM names n
LEFT JOIN {s}.queues q ON q.name = n.name
LEFT JOIN served s ON s.name = n.name
ORDER BY n.name`

// $1 how long a worker without a stored lease may be silent, in microseconds.
//
// listWorkersSQL lists the workers, each with whether it is stale: it
// made no check-in for its whole lease, by the database's clock. A
// worker of a release that stores no lease is stale after $1
// microseconds.
const listWorkersSQL = `
SELECT w.id, w.hostname, w.version, w.queues, w.kinds, w.schedules, w.started_at, w.heartbeat_at, l.worker_id IS NOT NULL,
       w.heartbeat_at < now() - coalesce(w.lease_duration, $1 * interval '1 microsecond')
FROM {s}.workers w
LEFT JOIN {s}.leader l ON l.name = 'maintainer' AND l.worker_id = w.id AND l.expires_at > now()
ORDER BY w.id`

// listSchedulesSQL lists the stored schedules, by name.
const listSchedulesSQL = `
SELECT name, queue, kind, cron, timezone, next_run_at, last_run_at, paused_at IS NOT NULL
FROM {s}.schedules ORDER BY name`

// pageColumns name the columns of a page of tasks, which are listColumns.
const pageColumns = `id, queue, kind, state, priority, attempt, max_attempts, group_key, metadata, payload, errors,
       created_at, run_at, attempted_at, finalized_at`

// taskColumns are the columns that scanTaskInfo reads, in its order. state is
// the expression for the task's state: the history has no such column, so a
// statement that reads it passes "'completed'". Tasks and history are read
// through this one list, because scanTaskInfo reads both by position.
func taskColumns(state string) string {
	return `id, queue, kind, ` + state + `, priority, attempt, max_attempts, coalesce(group_key, ''),
       metadata, payload, errors, created_at, run_at, attempted_at, finalized_at`
}

// listColumns are taskColumns as a list reads them from a table of tasks in
// the given state. If $6, the summary parameter of every list statement, is
// set, they leave out what a list of tasks rarely shows, and what makes a
// task large: they have no metadata and no payload, and of the errors only
// the newest. The planner sees the parameter, so a summary does not read the
// values it leaves out.
func listColumns(state string) string {
	return `id, queue, kind, ` + state + `, priority, attempt, max_attempts, coalesce(group_key, ''),
            CASE WHEN $6 THEN NULL ELSE metadata END, CASE WHEN $6 THEN NULL ELSE payload END,
            CASE WHEN $6 THEN jsonb_path_query_array(errors, '$[last]') ELSE errors END,
            created_at, run_at, attempted_at, finalized_at`
}

// inList matches the rows whose column is among the values of the text array
// param, or every row if param is empty. It compares a single value with =,
// so that a custom plan can read an index that leads with the column in the
// order of the index's next column. Through a list, even a list of one
// value, the planner does not see that order.
func inList(column, param string) string {
	return `CASE cardinality(` + param + `) WHEN 0 THEN true WHEN 1 THEN ` + column + ` = (` + param + `)[1]
            ELSE ` + column + ` = ANY(` + param + `) END`
}

// inStates matches the tasks whose state is among the names in the text
// array param. It tests each state as a literal, and only if param names it,
// so that a custom plan sees which states are left, and which partial indexes
// hold them. A cast of param to the enum hides them: it is not evaluated when
// the statement is planned.
func inStates(param string) string {
	var tests []string
	for _, s := range allStates {
		if s != StateCompleted {
			tests = append(tests, `(state = '`+string(s)+`' AND '`+string(s)+`' = ANY(`+param+`))`)
		}
	}
	return `(` + strings.Join(tests, ` OR `) + `)`
}

// createdPages is one of the places that listCreated reads: for each listed
// queue, the tasks t of table that meet cond, in the range of creation times
// and below the position, the latest first and up to a page. With no queue
// named, the queues are those of the CTE found. state is the expression for
// a task's state. Nothing is read unless gate holds, which the planner
// knows: it tells whether the list asks for the tasks of this place.
func createdPages(found, table, state, cond, gate string) string {
	return `(SELECT ` + pageColumns + `
     FROM (SELECT queue FROM ` + found + ` WHERE cardinality($1::text[]) = 0 AND queue IS NOT NULL
           UNION ALL
           SELECT queue FROM named) AS l(name)
     CROSS JOIN LATERAL (
         SELECT ` + listColumns(state) + `
         FROM ` + table + ` t
         WHERE t.queue = l.name AND ` + cond + `
           AND t.created_at >= $9::timestamptz AND (t.created_at, t.id) < ($8::timestamptz, $7::bigint)
           AND ` + inList(`t.kind`, `$3::text[]`) + `
           AND ` + inList(`t.group_key`, `$4::text[]`) + `
         ORDER BY t.created_at DESC, t.id DESC LIMIT $5
     ) page(` + pageColumns + `)
     WHERE ` + gate + `)`
}
