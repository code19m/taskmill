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
// completeLocked in complete.go, and the pick statements in sql.go.

// groupLeave is a counted task that left its FIFO group.
type groupLeave struct {
	id           int64
	queue, group string
	head         bool // it was the group's head, not a pending task behind it
}

// groupKey identifies a FIFO group, as the key of a map: a group's key is
// unique only within its queue.
type groupKey struct{ queue, group string }

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
// logger: a worker's names it. It waits for a group's row that another
// transaction holds. It takes the rows of several groups in the order that
// the plan of its statement gives, which is not always the order of left:
// a caller cannot choose a lock order here. The callers with more than one
// group hold the rows before they call: lockGroups, the pick statements,
// and the rescue took them, and only rows that were free.
//
// The exception is completeWaiting with several groups: the last
// completions of a worker that stops, for the groups whose rows were held
// until then. It can meet a transaction that takes the same rows in another
// order, such as one of the application that enqueues into several groups.
// PostgreSQL then ends one of the two, and a completion that it ended runs
// again (whileLeased). The task of RunTask, which completeWaiting completes
// too, has one group at most.
func (c *Client) leaveGroups(ctx context.Context, logger *slog.Logger, q querier, left []groupLeave) error {
	if len(left) == 0 {
		return nil
	}
	// Each group goes to the recount once, with all its tasks that left: an
	// UPDATE changes a row once.
	type leave struct {
		tasks int   // how many of the group's tasks left
		id    int64 // one of them: the task, if only one left
		head  bool  // its head was among them: the next task becomes the head
	}
	var asked groupSet
	ids := make([]int64, len(left))
	leaves := make(map[groupKey]leave, len(left)) // by group, until its row is read back
	for i, l := range left {
		ids[i] = l.id
		key := groupKey{l.queue, l.group}
		g, seen := leaves[key]
		if !seen {
			asked.add(l.queue, l.group)
		}
		leaves[key] = leave{tasks: g.tasks + 1, id: l.id, head: g.head || l.head}
	}
	counts := make([]int, len(asked.queues))
	for i := range counts {
		counts[i] = -leaves[groupKey{asked.queues[i], asked.groups[i]}].tasks
	}
	r, err := q.query(ctx, c.sql.recountGroups, asked.queues, asked.groups, counts, ids)
	if err != nil {
		return err
	}
	var empty, next groupSet
	for r.Next() {
		var queue, group string
		var remaining int
		var halted bool
		if err := r.Scan(&queue, &group, &remaining, &halted); err != nil {
			r.Close()
			return err
		}
		key := groupKey{queue, group}
		head := leaves[key].head
		delete(leaves, key)
		switch {
		case remaining == 0:
			empty.add(queue, group)
		case head && !halted:
			next.add(queue, group)
		}
	}
	r.Close()
	if err := r.Err(); err != nil {
		return err
	}
	for key, g := range leaves { // not read back: the group has no row
		groupMissing(ctx, logger, key.queue, key.group, g.tasks, g.id)
	}
	if len(empty.queues) > 0 {
		if _, err := q.exec(ctx, c.sql.deleteEmptyGroups, empty.queues, empty.groups); err != nil {
			return err
		}
	}
	if len(next.queues) == 0 {
		return nil
	}
	return c.promoteGroups(ctx, q, next)
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

// promoteGroups makes the oldest pending task of every group in s its head.
func (c *Client) promoteGroups(ctx context.Context, q querier, s groupSet) error {
	_, err := q.exec(ctx, c.sql.promoteGroups, s.queues, s.groups, c.schema, c.notifiable(ctx, s.queues))
	return err
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
		if _, err := q.exec(ctx, c.sql.recountGroups, counted.queues, counted.groups, counted.sizes(byGroup), left); err != nil {
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
func groupTasks(ts []bulkTask, skip map[int64]bool) (counted, missing groupSet, byGroup map[groupKey][]bulkTask) {
	byGroup = make(map[groupKey][]bulkTask)
	for _, t := range ts {
		if t.group == "" || skip[t.id] {
			continue
		}
		key := groupKey{t.queue, t.group}
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
func (c *Client) createGroups(ctx context.Context, q querier, s groupSet, byGroup map[groupKey][]bulkTask) ([]groupKey, error) {
	var created []groupKey
	done, err := briefly(ctx, q, func() error {
		r, err := q.query(ctx, c.sql.createGroups, s.queues, s.groups, s.sizes(byGroup))
		if err != nil {
			return err
		}
		defer r.Close()
		for r.Next() {
			var key groupKey
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
func (s *groupSet) tasks(byGroup map[groupKey][]bulkTask) []bulkTask {
	var out []bulkTask
	for i := range s.queues {
		out = append(out, byGroup[groupKey{s.queues[i], s.groups[i]}]...)
	}
	return out
}

// sizes returns how many of the tasks of byGroup are in each group of s.
func (s *groupSet) sizes(byGroup map[groupKey][]bulkTask) []int {
	out := make([]int, len(s.queues))
	for i := range s.queues {
		out[i] = len(byGroup[groupKey{s.queues[i], s.groups[i]}])
	}
	return out
}
