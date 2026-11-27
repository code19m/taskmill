package taskmill

import (
	"context"
	"log/slog"
)

// This file holds what changes the row of a FIFO group: tasks leave the
// group, its dead head halts it, its next task becomes the head, and retried
// tasks join it again. The invariants that these changes keep are in
// https://github.com/code19m/taskmill/blob/main/docs/design/schema-groups.md.
//
// The callers keep the lock order, not this file: see takeTask in client.go,
// completeGrouped in complete.go, and the pick statements in sql_operations.go.

// groupLeave is a counted task that left its FIFO group.
type groupLeave struct {
	id           int64
	queue, group string
	head         bool // it was the group's head, not a pending task behind it
}

// groupID identifies a FIFO group, as the key of a map: a group's key is
// unique only within its queue.
type groupID struct{ queue, group string }

// groupSet holds groups as two parallel slices, as statements take them.
type groupSet struct {
	queues, groups []string
}

func (s *groupSet) add(queue, group string) {
	s.queues = append(s.queues, queue)
	s.groups = append(s.groups, group)
}

// leaveGroups updates FIFO groups after counted tasks of theirs finished or
// were removed: one task or many, each an entry of left. It takes the tasks
// off their groups' counts, and ends a halt that names one of them: a halt
// must not outlive the place of its task in the count. It deletes a group
// that has no task left, and makes the oldest pending task the head of a
// group whose head left, unless another task halts the group. A group that
// has no row, which should never happen, is logged.
//
// Callers run it in the transaction that changed the tasks, and pass their
// logger: a worker's names it. It takes the rows of several groups in the
// order of its plan, so the callers with more than one group lock the rows
// first, and only those that are free. The exception, the last completions
// of a stopping worker (completeWaiting), runs again after a deadlock.
func (c *Client) leaveGroups(ctx context.Context, logger *slog.Logger, q querier, left []groupLeave) error {
	if len(left) == 0 {
		return nil
	}

	l := c.leaving(logger, q, left)
	sql, args := l.recount()
	if err := readAll(ctx, q, func(r rows) error { return l.readRecount(ctx, r) }, sql, args...); err != nil {
		return err
	}
	if !l.settles() {
		return nil
	}
	sql, args = l.settle()
	return readAll(ctx, q, l.readSettle, sql, args...)
}

// readAll runs a statement on q, and read on its rows.
func readAll(ctx context.Context, q querier, read func(rows) error, sql string, args ...any) error {
	r, err := q.query(ctx, sql, args...)
	if err != nil {
		return err
	}
	defer r.Close()
	if err := read(r); err != nil {
		return err
	}
	return r.Err()
}

// groupLeaving is what leaveGroups does, as two statements: the recount,
// and the settling, which needs nothing that the recount returns. So
// leaveGroups runs the settling only if the recount says that it has work,
// while a worker's completion of grouped tasks sends both in one round trip
// with its commit: see Worker.completeGrouped.
type groupLeaving struct {
	c      *Client
	logger *slog.Logger
	q      querier // where the statements run, for their notifications

	asked  groupSet           // each group that tasks left, once: an UPDATE changes a row once
	counts []int              // the change of each asked group's count
	ids    []int64            // the tasks that left
	heads  groupSet           // the groups whose heads left
	leaves map[groupID]leaves // by group, until the recount reads its row back

	// What the recount read.
	emptied bool     // a group counts no task now
	next    groupSet // the groups whose heads left that the settling promotes
}

// leaves are the tasks that left one group.
type leaves struct {
	tasks int   // how many
	id    int64 // one of them: the task, if only one left
	head  bool  // the group's head was among them
}

// leaving returns the statements by which left leave their groups, which
// run in q's transaction.
func (c *Client) leaving(logger *slog.Logger, q querier, left []groupLeave) *groupLeaving {
	l := &groupLeaving{c: c, logger: logger, q: q, ids: make([]int64, len(left)), leaves: make(map[groupID]leaves, len(left))}
	for i, t := range left {
		l.ids[i] = t.id
		key := groupID{t.queue, t.group}
		g, seen := l.leaves[key]
		if !seen {
			l.asked.add(t.queue, t.group)
		}
		l.leaves[key] = leaves{tasks: g.tasks + 1, id: t.id, head: g.head || t.head}
	}

	l.counts = make([]int, len(l.asked.queues))
	for i := range l.counts {
		key := groupID{l.asked.queues[i], l.asked.groups[i]}
		l.counts[i] = -l.leaves[key].tasks
		if l.leaves[key].head {
			l.heads.add(key.queue, key.group)
		}
	}
	return l
}

// recount returns the statement that takes the tasks off their groups'
// counts.
func (l *groupLeaving) recount() (string, []any) {
	return l.c.sql.recountGroups, []any{
		l.asked.queues, // $1
		l.asked.groups, // $2
		l.counts,       // $3
		l.ids,          // $4
	}
}

// readRecount reads the rows of the recount, and logs the groups that have
// no row.
func (l *groupLeaving) readRecount(ctx context.Context, r rows) error {
	for r.Next() {
		var queue, group string
		var remaining int
		var halted bool
		if err := r.Scan(&queue, &group, &remaining, &halted); err != nil {
			return err
		}

		key := groupID{queue, group}
		head := l.leaves[key].head
		delete(l.leaves, key)
		switch {
		case remaining == 0:
			l.emptied = true
		case head && !halted:
			l.next.add(queue, group)
		}
	}
	if err := r.Err(); err != nil {
		return err
	}

	for key, g := range l.leaves { // not read back: the group has no row
		groupMissing(ctx, l.logger, key.queue, key.group, g.tasks, g.id)
	}
	return nil
}

// settles reports whether the settling has work, once the recount was read.
func (l *groupLeaving) settles() bool {
	return l.emptied || len(l.next.queues) > 0
}

// settle returns the statement that deletes the groups that count no task,
// and promotes the next task of the groups whose heads left. It notifies,
// in the caller's transaction, the queues of the groups that the recount
// read as about to get a head: before the recount is read, none, which
// only the client's own transactions may send it with, since they leave
// their notifications to the notifier.
func (l *groupLeaving) settle() (string, []any) {
	return l.c.sql.settleGroups, []any{
		l.asked.queues,                     // $1
		l.asked.groups,                     // $2
		l.heads.queues,                     // $3
		l.heads.groups,                     // $4
		l.c.schema,                         // $5
		l.c.notifiable(l.q, l.next.queues), // $6
	}
}

// readSettle reads the queues in which the settling made tasks available.
func (l *groupLeaving) readSettle(r rows) error {
	var available []string
	for r.Next() {
		var queue string
		if err := r.Scan(&queue); err != nil {
			return err
		}
		available = append(available, queue)
	}
	if err := r.Err(); err != nil {
		return err
	}
	madeAvailable(l.q, available...)
	return nil
}

// leaveAll updates the FIFO groups of the tasks among ts that leave them,
// those for which leaves is true, after the tasks changed.
func (c *Client) leaveAll(ctx context.Context, q querier, ts []bulkTask, leaves func(bulkTask) bool) error {
	var left []groupLeave
	for _, t := range ts {
		if t.group != "" && leaves(t) {
			left = append(left, groupLeave{id: t.id, queue: t.queue, group: t.group, head: t.state != StatePending})
		}
	}
	return c.leaveGroups(ctx, c.logger, q, left)
}

// haltGroup halts a FIFO group behind its head, task id, which became dead
// and stays counted. Callers run it in the transaction that made the task
// dead, with their logger. A group that has no row is logged, as in
// leaveGroups.
func (c *Client) haltGroup(ctx context.Context, logger *slog.Logger, q querier, queue, group string, id int64) error {
	halted, err := q.exec(ctx, c.sql.haltGroup, queue, group, id)
	if err == nil && halted == 0 {
		groupMissing(ctx, logger, queue, group, 1, id)
	}
	return err
}

// groupMissing logs a FIFO group that has no row, which should never happen,
// for the tasks that left it or halted it. The line names the task, id, if
// it is the only one.
func groupMissing(ctx context.Context, logger *slog.Logger, queue, group string, tasks int, id int64) {
	attrs := []slog.Attr{slog.String(logKeyQueue, queue), slog.String(logKeyGroup, group)}
	if tasks == 1 {
		attrs = append(attrs, slog.Int64(logKeyTaskID, id))
	}
	logger.LogAttrs(ctx, slog.LevelError, "fifo group missing", attrs...)
}

// releaseHalts ends the halts of the FIFO groups that dead tasks among ts
// halt, and returns those tasks.
func (c *Client) releaseHalts(ctx context.Context, q querier, ts []bulkTask) (map[int64]bool, error) {
	var dead []int64
	for _, t := range ts {
		if t.group != "" && t.state == StateDead {
			dead = append(dead, t.id)
		}
	}

	halting := make(map[int64]bool)
	if len(dead) == 0 {
		return halting, nil
	}

	r, err := q.query(ctx, c.sql.releaseHalts, dead)
	if err != nil {
		return nil, err
	}
	defer r.Close()

	for r.Next() {
		var id int64
		if err := r.Scan(&id); err != nil {
			return nil, err
		}
		halting[id] = true
	}
	return halting, r.Err()
}

// rejoinGroup counts a retried task back into its FIFO group, and reports
// whether it must wait behind the group's other tasks. A dead head that halts
// its group is still counted: retrying it resumes the group. The caller
// holds the task's lock.
func (c *Client) rejoinGroup(ctx context.Context, q querier, queue, group string, id int64) (pending bool, err error) {
	resumed, err := q.exec(ctx, c.sql.resumeHaltedHead, queue, group, id)
	if err != nil || resumed == 1 {
		return false, err
	}

	var tasks int
	if _, err := queryRow(ctx, q, c.sql.enterGroup, []any{queue, group}, &tasks); err != nil {
		return false, err
	}
	return tasks > 1, nil
}

// rejoinGroups is rejoinGroup for a batch of tasks to retry, with a few
// statements for all of them. It returns the tasks to retry, and those of
// them that wait in pending. A dead head that halts its group resumes it.
// The other tasks of a group that has a row wait behind its head. A group
// that has no row gets one, and its oldest task becomes its head; if it
// cannot get one now, its tasks are left out.
//
// The two are not one function, as leaveGroups is for the way out of a
// group: one task waits for its group's row and joins, and a batch never
// waits.
func (c *Client) rejoinGroups(ctx context.Context, q querier, ts []bulkTask) (retried []bulkTask, pending []int64, err error) {
	resumed, err := c.releaseHalts(ctx, q, ts)
	if err != nil {
		return nil, nil, err
	}

	pending = []int64{} // the tasks that become pending, or none
	for _, t := range ts {
		if t.group == "" || resumed[t.id] {
			retried = append(retried, t)
		}
	}

	counted, missing, byGroup := groupTasks(ts, resumed)
	if len(counted.queues) > 0 {
		left := []int64{} // the tasks join their groups: none leaves
		_, err := q.exec(ctx, c.sql.recountGroups,
			counted.queues,         // $1
			counted.groups,         // $2
			counted.sizes(byGroup), // $3
			left,                   // $4
		)
		if err != nil {
			return nil, nil, err
		}

		for _, t := range counted.tasks(byGroup) {
			retried = append(retried, t)
			pending = append(pending, t.id)
		}
	}

	if len(missing.queues) == 0 {
		return retried, pending, nil
	}
	created, err := c.createGroups(ctx, q, missing, byGroup)
	if err != nil {
		return nil, nil, err
	}

	for _, key := range created {
		for i, t := range byGroup[key] {
			retried = append(retried, t)
			if i > 0 {
				pending = append(pending, t.id)
			}
		}
	}
	return retried, pending, nil
}

// groupTasks sorts the grouped tasks of ts, except those of skip, by group:
// the groups that have a row, and those that have none. A group's tasks
// stay in ID order, as picked.
func groupTasks(ts []bulkTask, skip map[int64]bool) (counted, missing groupSet, byGroup map[groupID][]bulkTask) {
	byGroup = make(map[groupID][]bulkTask)
	for _, t := range ts {
		if t.group == "" || skip[t.id] {
			continue
		}
		key := groupID{t.queue, t.group}
		switch {
		case byGroup[key] != nil:
		case t.inGroup:
			counted.add(t.queue, t.group)
		default:
			missing.add(t.queue, t.group)
		}
		byGroup[key] = append(byGroup[key], t)
	}
	return counted, missing, byGroup
}

// createGroups creates the rows of groups s, which had none, and returns
// those it created. A concurrent enqueue may be creating the same row: the
// batch then waits for it for at most its lock timeout, and creates none.
func (c *Client) createGroups(ctx context.Context, q querier, s groupSet, byGroup map[groupID][]bulkTask) ([]groupID, error) {
	var created []groupID
	done, err := briefly(ctx, q, func() error {
		r, err := q.query(ctx, c.sql.createGroups, s.queues, s.groups, s.sizes(byGroup))
		if err != nil {
			return err
		}
		defer r.Close()

		for r.Next() {
			var key groupID
			if err := r.Scan(&key.queue, &key.group); err != nil {
				return err
			}
			created = append(created, key)
		}
		return r.Err()
	})
	if !done {
		return nil, err
	}
	return created, err
}

// tasks returns the tasks of byGroup in the groups of s.
func (s *groupSet) tasks(byGroup map[groupID][]bulkTask) []bulkTask {
	var out []bulkTask
	for i := range s.queues {
		out = append(out, byGroup[groupID{s.queues[i], s.groups[i]}]...)
	}
	return out
}

// sizes returns how many of the tasks of byGroup are in each group of s.
func (s *groupSet) sizes(byGroup map[groupID][]bulkTask) []int {
	out := make([]int, len(s.queues))
	for i := range s.queues {
		out[i] = len(byGroup[groupID{s.queues[i], s.groups[i]}])
	}
	return out
}
