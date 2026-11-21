package taskmill

import (
	"context"
	"encoding/base64"
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

// This file holds the reads that page through tasks and FIFO groups for the
// operations API, and their cursors. The statements are in sql_lists.go.

// Limits of a page of tasks or of FIFO groups.
const (
	defaultListLimit = 100
	maxListLimit     = 1000
	maxGroupQueues   = 100 // most queues named in one Groups call, each read up to a page
)

// ListParams filters and pages [Client.List]. A field outside its limits is
// an error that wraps [ErrInvalidArgument].
type ListParams struct {
	// Filter selects the tasks. Empty means every task.
	Filter TaskFilter

	// CreatedSince limits the list to the tasks created at this time or
	// later: those whose [TaskInfo.CreatedAt] is not before it. Zero means no
	// lower bound. Any list takes it, whatever its states, and is then in
	// creation order: see [Client.List]. Together with a range of finish
	// times it is an error that wraps [ErrInvalidArgument]: a list has one
	// order.
	//
	// The time is an instant: its location does not matter, and neither
	// does the time zone of the database or of the process. It must be a
	// time that PostgreSQL can store, from 4713 BC to 294276 AD, and it
	// must be before CreatedBefore if both are set.
	CreatedSince time.Time

	// CreatedBefore limits the list to the tasks created before this time,
	// under the rules of CreatedSince. Zero means no upper bound. A task
	// created at this very time is left out, so two ranges that meet at a
	// time share no task.
	CreatedBefore time.Time

	// FinishedSince limits the list to the tasks that finished at this time
	// or later: those whose [TaskInfo.FinishedAt] is not before it. Zero
	// means no lower bound. Only a list in finish order takes it, which is a
	// list of only dead tasks, only canceled tasks, or both, without a range
	// of creation times: see [Client.List]. On any other list it is an error
	// that wraps [ErrInvalidArgument].
	//
	// The time is an instant, under the rules of CreatedSince, and it must
	// be before FinishedBefore if both are set.
	FinishedSince time.Time

	// FinishedBefore limits the list to the tasks that finished before this
	// time, under the rules of FinishedSince. Zero means no upper bound. A
	// task that finished at this very time is left out, so two ranges that
	// meet at a time share no task.
	FinishedBefore time.Time

	// Limit is the page size. It must not be negative. Default: 100.
	// Maximum: 1,000.
	Limit int

	// Cursor continues from a previous page's [ListResult.Next]. Empty means
	// the first page. Pass it with the filter and the range that it came
	// from: each of the three orders of [Client.List] has cursors of its
	// own, and a cursor of another order, like any text that is not a
	// cursor, is an error that wraps [ErrInvalidArgument].
	Cursor string

	// Summary leaves out what makes a task large, for a list that shows one
	// line per task: each [TaskInfo] comes with a nil Payload, a nil
	// Metadata, and only the newest entry of its Errors. The database does
	// not read or send what is left out, so a page costs about the same
	// whatever its tasks hold. [TaskInfo.ScheduledFor] reads the metadata,
	// so it returns the zero time for such a task. Default: false, which
	// returns every field, as [Client.Get] does.
	Summary bool
}

// ListResult is one page of tasks, in the order of its list: see
// [Client.List].
type ListResult struct {
	Tasks []TaskInfo

	// Next is the cursor for the next page, or empty on the last page.
	Next string
}

// List returns the tasks that match params, one page at a time. params may
// be nil, for the first page of every task. A page holds every task in
// full, payloads included, unless [ListParams.Summary] is set.
//
// A list is in one of three orders:
//
//   - A list with a range of creation times, [ListParams.CreatedSince] or
//     [ListParams.CreatedBefore], is ordered by when the tasks were created,
//     the latest first: by [TaskInfo.CreatedAt], and by ID, the highest
//     first, among tasks created at the same time.
//   - Without such a range, a list of only dead tasks, only canceled tasks,
//     or both is ordered by when the tasks finished, the latest first: by
//     [TaskInfo.FinishedAt], and then by ID in the same way.
//   - Every other list is ordered by ID, the newest task first.
//
// A time that is infinite, as only an UPDATE by hand writes it, reads as the
// zero time, and comes before or after all others in the order of its
// time.
//
// IDs follow creation times only roughly. A task's creation time is the
// start of the transaction that enqueued it, and its ID is as new as the
// moment of its insert: a task that a long transaction enqueues at its end
// has an older creation time than tasks with lower IDs. So a list with a
// range of creation times is in the order of that time, in which a page
// reads an index, and not in ID order.
//
// A task's finish time is the start of the transaction that finished it.
// So tasks show up neither in the order of their creation times nor in that
// of their finish times: a walk through the pages of a list in either order
// can miss a task that is enqueued, or finishes, meanwhile. A walk in finish
// order can return a task twice, if the task is retried and finishes again
// meanwhile. So a list of dead tasks is not a log: to read the tasks that
// died since your last look, read from the first page to a little before
// the latest finish time that you saw, and drop the IDs that you have.
//
// Each page's query is canceled after 5 seconds, so listing cannot load the
// database for longer: a list that needs more returns an error. A page reads
// about the tasks that it returns, except in these lists:
//
//   - A list in ID order whose tasks no index holds in that order reads
//     every newer task, of any queue and state, until its page is full.
//     Indexes hold every task, the completed tasks of one queue or of all,
//     and the pending tasks of one queue's FIFO group, in ID order. They do
//     not hold, for example, one unfinished state, or dead tasks mixed with
//     other states.
//   - A list in creation or finish order with no queue named reads up to a
//     page of each queue that holds such tasks: with hundreds of queues,
//     name the queue.
//   - A list in finish order reads every task that shares a finish time
//     with the first or the last task of its page. The tasks that one
//     transaction finished share one: up to 1,000 for a batch of a bulk
//     action, and with no limit for the [Client.CancelTx] calls of one
//     transaction.
//   - A list in finish order with a kind or a FIFO group in its filter
//     reads the dead or canceled tasks of each queue until its page is
//     full.
//
// The reasons are in
// https://github.com/code19m/taskmill/blob/main/docs/design/schema-lists.md.
func (c *Client) List(ctx context.Context, params *ListParams) (*ListResult, error) {
	if params == nil {
		params = &ListParams{}
	}
	page, err := c.taskPage(params)
	if err != nil {
		return nil, err
	}

	var (
		out  ListResult
		at   pgtype.Timestamptz // the time of the task last read, in the order of a time
		last pgtype.Timestamptz // that of the page's last task, where the next page starts
	)

	// In the order of a time, each row ends with that time once more, as the
	// column holds it: the cursor needs it, and scanTaskInfo reads an
	// infinite time as the zero time.
	var orderedBy []any
	if page.order != byID {
		orderedBy = []any{&at}
	}

	err = c.readAlone(ctx, page.settings, page.sql, page.args, func(r pgx.Rows) error {
		info, err := scanTaskInfo(r, orderedBy...)
		if err != nil {
			return err
		}
		if out.Tasks = append(out.Tasks, *info); len(out.Tasks) == page.limit {
			last = at
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("taskmill: list tasks: %w", err)
	}

	if len(out.Tasks) > page.limit {
		out.Tasks = out.Tasks[:page.limit]
		if id := out.Tasks[page.limit-1].ID; page.order == byID {
			out.Next = encodeCursor(id)
		} else {
			out.Next = encodeTimeCursor(page.order, last, id)
		}
	}
	return &out, nil
}

// listOrder is the order of a list of tasks. An order by a time is the
// letter that starts its cursors.
type listOrder byte

const (
	byID       listOrder = 0
	byCreation listOrder = 'c'
	byFinish   listOrder = 'f'
)

// taskPage is the read of one page of tasks: its statement, which reads one
// task more than the page's limit, with the settings that start its
// transaction and with its arguments.
type taskPage struct {
	settings, sql string
	args          []any
	limit         int
	order         listOrder // in the order of a time, each row ends with that time
}

// taskPage checks p, and returns the read of its page. Each order has its
// own statement and its own cursor, and refuses the cursors of the others:
// the orders share no position.
func (c *Client) taskPage(p *ListParams) (*taskPage, error) {
	page := taskPage{settings: pageSettings, sql: c.sql.listTasks, limit: p.Limit}
	switch {
	case p.Limit == 0:
		page.limit = defaultListLimit
	case p.Limit < 0 || p.Limit > maxListLimit:
		return nil, invalidArgument("limit must be between 1 and %d", maxListLimit)
	}

	f, err := match(&p.Filter, allStates)
	if err != nil {
		return nil, err
	}
	created, err := rangeOf("CreatedSince", p.CreatedSince, "CreatedBefore", p.CreatedBefore)
	if err != nil {
		return nil, err
	}
	finished, err := rangeOf("FinishedSince", p.FinishedSince, "FinishedBefore", p.FinishedBefore)
	if err != nil {
		return nil, err
	}
	if page.order, err = orderOf(f, created.set, finished.set); err != nil {
		return nil, err
	}

	if page.order == byID {
		before := int64(math.MaxInt64)
		if p.Cursor != "" {
			if before, err = decodeCursor(p.Cursor); err != nil {
				return nil, invalidArgument("invalid cursor")
			}
		}

		page.args = append(listArgs(f, page.limit, p.Summary, before),
			len(f.states) > 0, // $8
			f.history,         // $9
		)
		return &page, nil
	}

	// The first page starts below the latest time there is.
	at, before := pgtype.Timestamptz{InfinityModifier: pgtype.Infinity, Valid: true}, int64(math.MaxInt64)
	if p.Cursor != "" {
		if at, before, err = decodeTimeCursor(page.order, p.Cursor); err != nil {
			return nil, err
		}
	}

	page.settings = timeOrderSettings
	if page.order == byFinish {
		page.sql = c.sql.listFinished
		page.args = append(listArgs(f, page.limit, p.Summary, before),
			at,             // $8
			finished.since, // $9
			finished.until, // $10
		)
		return &page, nil
	}

	at, before = created.below(at, before)
	page.settings, page.sql = createdSettings, c.sql.listCreated
	page.args = append(listArgs(f, page.limit, p.Summary, before),
		at,             // $8
		created.from(), // $9
		f.history,      // $10
	)
	return &page, nil
}

// listArgs returns the parameters that the three list statements share, $1
// to $7: the filter, the page's size and whether it is a summary, and the ID
// that the page starts below. It reads one task more than the page holds, to
// tell whether another page follows.
func listArgs(f *filterArgs, limit int, summary bool, before int64) []any {
	return []any{
		f.queues,  // $1
		f.states,  // $2
		f.kinds,   // $3
		f.groups,  // $4
		limit + 1, // $5
		summary,   // $6
		before,    // $7
	}
}

// orderOf returns the order of a list of the tasks f, with a range of
// creation times or of finish times, if it has one. A list with a range of
// creation times is in creation order, a list of only dead and canceled
// tasks in finish order, and every other list in ID order. A list has one
// order, so it takes one of the two ranges, and a range of finish times in
// finish order only.
func orderOf(f *filterArgs, created, finished bool) (listOrder, error) {
	unfinished := func(state string) bool { return state != string(StateDead) && state != string(StateCanceled) }
	switch {
	case created && finished:
		return byID, invalidArgument("a list takes a range of creation times or a range of finish times, not both")
	case created:
		return byCreation, nil
	case !f.history && !slices.ContainsFunc(f.states, unfinished):
		return byFinish, nil
	case finished:
		return byID, invalidArgument("FinishedSince and FinishedBefore need a list of only dead tasks, only canceled tasks, or both")
	}
	return byID, nil
}

// timeRange is a checked range of creation times or of finish times, with
// its bounds as the statements take them: each is NULL where the range has
// no bound.
type timeRange struct {
	since, until pgtype.Timestamptz
	set          bool // the range has a bound
}

// below returns the position that a page of the range starts below, for a
// page that would start below the task of time at with the ID: that
// position, unless the range ends further down. Then the page starts below
// every task of the time that ends the range. listCreated takes the end of
// its range this way.
func (r timeRange) below(at pgtype.Timestamptz, id int64) (pgtype.Timestamptz, int64) {
	if !r.until.Valid || at.InfinityModifier == pgtype.NegativeInfinity || at.InfinityModifier == pgtype.Finite && at.Time.Before(r.until.Time) {
		return at, id
	}
	return r.until, math.MinInt64
}

// from returns the start of the range as a time that is never NULL: the
// earliest time there is, for a range without a start.
func (r timeRange) from() pgtype.Timestamptz {
	if !r.since.Valid {
		return pgtype.Timestamptz{InfinityModifier: pgtype.NegativeInfinity, Valid: true}
	}
	return r.since
}

// rangeOf checks the range of times from since up to before, and returns it.
// The two names are those of the bounds in an error.
func rangeOf(sinceName string, since time.Time, beforeName string, before time.Time) (r timeRange, err error) {
	if r.set = !since.IsZero() || !before.IsZero(); !r.set {
		return r, nil
	}
	if !since.IsZero() && !before.IsZero() && !since.Before(before) {
		return r, invalidArgument("%s must be before %s", sinceName, beforeName)
	}

	if r.since, err = rangeBound(sinceName, since); err != nil {
		return r, err
	}
	r.until, err = rangeBound(beforeName, before)
	return r, err
}

// rangeBound returns the bound t of a range of times as the database
// compares it: NULL for the zero time, which is no bound, and otherwise t
// rounded up to the microsecond. PostgreSQL keeps microseconds, so the times
// of a column that are not before t are those that are not before the
// rounded time. The driver rounds down, which would put a task of t's
// microsecond, before t, on the wrong side of either bound. what names the
// bound in the error for a time that PostgreSQL cannot store.
func rangeBound(what string, t time.Time) (pgtype.Timestamptz, error) {
	if t.IsZero() {
		return pgtype.Timestamptz{}, nil
	}
	if past := time.Duration(t.Nanosecond()) % time.Microsecond; past > 0 {
		t = t.Add(time.Microsecond - past)
	}
	return pgtype.Timestamptz{Time: t, Valid: true}, checkTime(what, t)
}

// GroupParams selects and pages the FIFO groups that [Client.Groups]
// returns. A field outside its limits is an error that wraps
// [ErrInvalidArgument].
type GroupParams struct {
	// Queues limits the result to groups of these queues. Empty means all
	// queues. Maximum: 100 names.
	Queues []string

	// Search limits the result to groups whose keys contain it, ignoring
	// case. At most 256 characters. Empty means all groups.
	Search string

	// Halted limits the result to the groups that a dead task halts.
	Halted bool

	// Limit is the page size. It must not be negative. Default: 100.
	// Maximum: 1,000.
	Limit int

	// Cursor continues from a previous page's [GroupsResult.Next]. Empty
	// means the first page.
	Cursor string
}

// GroupsResult is one page of FIFO groups, ordered by queue and key.
type GroupsResult struct {
	Groups []GroupInfo

	// Next is the cursor for the next page, or empty on the last page.
	Next string
}

// GroupInfo describes a FIFO group that has unfinished tasks, or that a dead
// task halts.
type GroupInfo struct {
	Queue string
	Key   string

	// Tasks is how many tasks the group counts: its unfinished tasks, and
	// the dead task that halts it, if any. A group halted by its only task
	// counts 1.
	Tasks int

	// HaltedBy is the dead task that halts the group, or 0 if it runs.
	HaltedBy int64
}

// Groups returns the FIFO groups that have unfinished tasks or a dead task
// that halts them, one page at a time, ordered by queue and key. params may
// be nil, for the first page of every group. Each queue named in
// [GroupParams.Queues] reads up to a page, so it takes at most 100 names.
// Like [Client.List], each page's query is canceled after 5 seconds; a
// search that matches few of very many groups can reach that limit and
// return an error.
func (c *Client) Groups(ctx context.Context, params *GroupParams) (*GroupsResult, error) {
	p := GroupParams{}
	if params != nil {
		p = *params
	}
	sql, args, err := c.groupsQuery(&p)
	if err != nil {
		return nil, err
	}

	out := GroupsResult{Groups: []GroupInfo{}} // never nil, as callers have always got it
	err = c.readAlone(ctx, pageSettings, sql, args, func(r pgx.Rows) error {
		var g GroupInfo
		if err := r.Scan(&g.Queue, &g.Key, &g.Tasks, &g.HaltedBy); err != nil {
			return err
		}
		out.Groups = append(out.Groups, g)
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("taskmill: list groups: %w", err)
	}

	if len(out.Groups) > p.Limit {
		out.Groups = out.Groups[:p.Limit]
		last := out.Groups[p.Limit-1]
		out.Next = base64.RawURLEncoding.EncodeToString([]byte(last.Queue + "\x00" + last.Key))
	}
	return &out, nil
}

// groupsQuery checks p, sets its default limit, and returns the statement
// that reads its page, one group more than the limit, with its arguments.
func (c *Client) groupsQuery(p *GroupParams) (sql string, args []any, err error) {
	switch {
	case p.Limit == 0:
		p.Limit = defaultListLimit
	case p.Limit < 0 || p.Limit > maxListLimit:
		return "", nil, invalidArgument("limit must be between 1 and %d", maxListLimit)
	}

	if len(p.Queues) > maxGroupQueues {
		return "", nil, invalidArgument("at most %d queue names", maxGroupQueues)
	}
	queues, err := checkFilter("queue name", p.Queues, maxNameLength)
	if err != nil {
		return "", nil, err
	}
	if p.Search != "" {
		if err := checkName("search", p.Search, maxGroupKeyLength); err != nil {
			return "", nil, err
		}
	}

	var afterQueue, afterKey string
	if p.Cursor != "" {
		if afterQueue, afterKey, err = decodeGroupCursor(p.Cursor); err != nil {
			return "", nil, err
		}
	}

	args = []any{
		p.Search,    // $1
		p.Limit + 1, // $2
		p.Halted,    // $3
		afterQueue,  // $4
		afterKey,    // $5
	}
	if len(queues) == 0 {
		return c.sql.listGroups, args, nil
	}
	return c.sql.listQueueGroups, append(args, queues), nil
}

// decodeGroupCursor decodes the cursor of a page of groups into the queue
// and key of the group that the page starts after.
func decodeGroupCursor(cursor string) (queue, key string, err error) {
	raw, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return "", "", invalidArgument("invalid cursor")
	}

	queue, key, ok := strings.Cut(string(raw), "\x00")
	if !ok || checkName("queue", queue, maxNameLength) != nil || checkName("group key", key, maxGroupKeyLength) != nil {
		return "", "", invalidArgument("invalid cursor")
	}
	return queue, key, nil
}

// encodeCursor and decodeCursor turn a task ID into the opaque cursor of a
// list or a bulk action, and back.
func encodeCursor(id int64) string {
	return base64.RawURLEncoding.EncodeToString([]byte(strconv.FormatInt(id, 10)))
}

func decodeCursor(cursor string) (int64, error) {
	raw, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return 0, err
	}
	return strconv.ParseInt(string(raw), 10, 64)
}

// The cursor of a list in the order of a time holds that time and the ID of
// the page's last task, as "c<time>,<ID>" in creation order and as
// "f<time>,<ID>" in finish order: no cursor in ID order, which is a number,
// starts with a letter. The time is the number that PostgreSQL stores,
// microseconds since the year 2000, and so it holds every time that a
// column can: the largest and the smallest number are the two infinite
// times.
const (
	pgEpoch     = 946_684_800               // the year 2000 in Unix seconds
	pgFirstTime = -211_813_488_000_000_000  // 4714-11-24 BC, the first time of PostgreSQL
	pgTimesEnd  = 9_223_371_331_200_000_000 // the year 294277, which PostgreSQL's times end before
)

func encodeTimeCursor(order listOrder, at pgtype.Timestamptz, id int64) string {
	var micros int64
	switch at.InfinityModifier {
	case pgtype.Infinity:
		micros = math.MaxInt64
	case pgtype.NegativeInfinity:
		micros = math.MinInt64
	case pgtype.Finite:
		micros = (at.Time.Unix()-pgEpoch)*1e6 + int64(at.Time.Nanosecond())/1e3
	}

	return base64.RawURLEncoding.EncodeToString(fmt.Appendf(nil, "%c%d,%d", order, micros, id))
}

// decodeTimeCursor decodes the cursor of a list in the order of a time into
// the position that the page starts below. It refuses the cursor of another
// order, and a time that PostgreSQL would refuse.
func decodeTimeCursor(order listOrder, cursor string) (at pgtype.Timestamptz, id int64, err error) {
	raw, err := base64.RawURLEncoding.DecodeString(cursor)
	position, ordered := strings.CutPrefix(string(raw), string(rune(order)))
	when, which, _ := strings.Cut(position, ",")
	micros, errTime := strconv.ParseInt(when, 10, 64)
	id, errID := strconv.ParseInt(which, 10, 64)

	at.Valid = true
	switch {
	case err != nil || !ordered || errTime != nil || errID != nil:
		return at, 0, invalidArgument("invalid cursor")
	case micros == math.MaxInt64:
		at.InfinityModifier = pgtype.Infinity
	case micros == math.MinInt64:
		at.InfinityModifier = pgtype.NegativeInfinity
	case micros < pgFirstTime || micros >= pgTimesEnd:
		return at, 0, invalidArgument("invalid cursor")
	default:
		at.Time = time.Unix(pgEpoch+micros/1e6, micros%1e6*1e3)
	}
	return at, id, nil
}
