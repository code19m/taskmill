package taskmill_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib" // registers the "pgx" database/sql driver

	"github.com/code19m/taskmill"
	"github.com/code19m/taskmill/internal/testdb"
)

func TestEnqueueStoresTheTask(t *testing.T) {
	t.Parallel()
	e := newEnv(t)

	id := e.enqueue(t, sendEmail{To: "a@example.com", UserID: 1234567890123456789},
		&taskmill.EnqueueOptions{Metadata: map[string]string{"trace": "abc"}})
	info := e.get(t, id)

	if info.State != taskmill.StateAvailable || info.Queue != taskmill.DefaultQueue || info.Kind != "email.send" {
		t.Fatalf("got state %s, queue %s, kind %s", info.State, info.Queue, info.Kind)
	}
	if info.Priority != taskmill.PriorityNormal || info.MaxAttempts != 25 || info.Attempt != 0 {
		t.Fatalf("got priority %d, max attempts %d, attempt %d", info.Priority, info.MaxAttempts, info.Attempt)
	}
	if !strings.Contains(string(info.Payload), "1234567890123456789") {
		t.Fatalf("payload %s lost the large integer", info.Payload)
	}
	if info.Metadata["trace"] != "abc" {
		t.Fatalf("metadata %v", info.Metadata)
	}
}

func TestEnqueueOptionsMerge(t *testing.T) {
	t.Parallel()
	e := newEnv(t)

	defaults := e.get(t, e.enqueue(t, report{Name: "daily"}, nil))
	if defaults.Queue != "reports" || defaults.Priority != taskmill.PriorityLow || defaults.MaxAttempts != 3 {
		t.Fatalf("kind defaults not applied: %+v", defaults)
	}

	overridden := e.get(t, e.enqueue(t, report{Name: "daily"},
		&taskmill.EnqueueOptions{Priority: taskmill.PriorityCritical, MaxAttempts: 1}))
	if overridden.Queue != "reports" || overridden.Priority != taskmill.PriorityCritical || overridden.MaxAttempts != 1 {
		t.Fatalf("call options not merged over kind defaults: %+v", overridden)
	}
}

func TestEnqueueFutureTaskIsScheduled(t *testing.T) {
	t.Parallel()
	e := newEnv(t)

	runAt := time.Now().Add(time.Hour).Truncate(time.Microsecond)
	info := e.get(t, e.enqueue(t, sendEmail{To: "a@example.com"}, &taskmill.EnqueueOptions{RunAt: runAt}))
	if info.State != taskmill.StateScheduled || !info.RunAt.Equal(runAt) {
		t.Fatalf("got %s at %s, want scheduled at %s", info.State, info.RunAt, runAt)
	}
}

func TestEnqueueRejectsInvalidOptions(t *testing.T) {
	t.Parallel()
	e := newEnv(t)

	tests := []struct {
		name string
		args taskmill.Args
		opts *taskmill.EnqueueOptions
	}{
		{"nil args", nil, nil},
		{"nil pointer args", (*sendEmail)(nil), nil},
		{"empty kind", emptyKind{}, nil},
		{"priority out of range", sendEmail{}, &taskmill.EnqueueOptions{Priority: 9}},
		{"negative max attempts", sendEmail{}, &taskmill.EnqueueOptions{MaxAttempts: -1}},
		{"queue too long", sendEmail{}, &taskmill.EnqueueOptions{Queue: strings.Repeat("q", 129)}},
		{"payload over 1 MiB", sendEmail{To: strings.Repeat("x", 1<<20)}, nil},
		{"negative dedupe window", sendEmail{}, &taskmill.EnqueueOptions{DedupeKey: "k", DedupeWindow: -time.Second}},
		{"invalid UTF-8 group key", sendEmail{}, &taskmill.EnqueueOptions{GroupKey: "\xff"}},
		{"NUL in queue", sendEmail{}, &taskmill.EnqueueOptions{Queue: "a\x00b"}},
		{"NUL in payload", sendEmail{To: "a\x00b"}, nil},
		{"NUL in metadata", sendEmail{}, &taskmill.EnqueueOptions{Metadata: map[string]string{"k": "a\x00b"}}},
		{"invalid UTF-8 in payload", rawArgs{Body: json.RawMessage("\"\xff\"")}, nil},
		{"payload over 1 MiB as stored", rawArgs{Body: json.RawMessage("[" + strings.Repeat("1e131071,", 8) + "1e131071]")}, nil},
		{"number with too many digits before the point", rawArgs{Body: json.RawMessage("1e131072")}, nil},
		{"number with too many digits after the point", rawArgs{Body: json.RawMessage("1e-16384")}, nil},
		{"zero with too many digits after the point", rawArgs{Body: json.RawMessage("0e-99999")}, nil},
		// Run times that PostgreSQL cannot store. The first two are common
		// ways to say "never"; unchecked, they wrap around into the past.
		{"run time at the largest Unix second", sendEmail{}, &taskmill.EnqueueOptions{RunAt: time.Unix(math.MaxInt64, 0)}},
		{"run time at Go's largest time", sendEmail{}, &taskmill.EnqueueOptions{RunAt: time.Unix(1<<63-62135596801, 999999999)}},
		{"run time just after the last timestamp", sendEmail{}, &taskmill.EnqueueOptions{RunAt: lastTimestamp.Add(time.Microsecond)}},
		{"run time just before the first timestamp", sendEmail{}, &taskmill.EnqueueOptions{RunAt: firstTimestamp.Add(-time.Microsecond)}},
		{"a kind's default run time after the last timestamp", parked{}, nil},
	}

	// Runs after the parallel subtests, once the test's context is canceled.
	t.Cleanup(func() {
		var n int
		err := e.pool.QueryRow(context.Background(), replaceSchema("SELECT count(*) FROM {s}.tasks", e.schema)).Scan(&n)
		if err != nil || n != 0 {
			t.Errorf("invalid enqueues stored %d tasks (%v)", n, err)
		}
	})

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tx := e.begin(t)
			_, err := e.client.EnqueueTx(t.Context(), taskmill.Pgx(tx), tt.args, tt.opts)
			if !errors.Is(err, taskmill.ErrInvalidArgument) {
				t.Fatalf("got %v, want ErrInvalidArgument", err)
			}
			if n := strings.Count(err.Error(), "taskmill:"); n != 1 {
				t.Errorf("%q names the package %d times", err, n)
			}

			// The checks run before the query: the caller's transaction goes on.
			e.txExec(t, tx, "SELECT 1")
		})
	}
}

// The first and last instants that PostgreSQL's timestamptz type holds.
var (
	firstTimestamp = time.Date(-4713, time.November, 24, 0, 0, 0, 0, time.UTC)
	lastTimestamp  = time.Date(294276, time.December, 31, 23, 59, 59, 999_999_000, time.UTC)
)

// parked is a kind whose default run time is past what PostgreSQL stores.
type parked struct{}

func (parked) Kind() string { return "test.parked" }

func (parked) EnqueueDefaults() taskmill.EnqueueOptions {
	return taskmill.EnqueueOptions{RunAt: time.Date(300_000, time.January, 1, 0, 0, 0, 0, time.UTC)}
}

func TestEnqueueStoresRunTimesAtTheEdgesOfTimestamps(t *testing.T) {
	t.Parallel()
	e := newEnv(t)

	for _, c := range []struct {
		runAt time.Time
		state taskmill.State
	}{
		{firstTimestamp, taskmill.StateAvailable},
		{lastTimestamp, taskmill.StateScheduled},
		// Finer than a microsecond, which is all that PostgreSQL keeps.
		{lastTimestamp.Add(999 * time.Nanosecond), taskmill.StateScheduled},
	} {
		info := e.get(t, e.enqueue(t, sendEmail{}, &taskmill.EnqueueOptions{RunAt: c.runAt}))
		if want := c.runAt.Truncate(time.Microsecond); info.State != c.state || !info.RunAt.Equal(want) {
			t.Errorf("run time %s: the task is %s at %s, want %s at %s", c.runAt, info.State, info.RunAt, c.state, want)
		}
	}
}

func TestEnqueueStoresNumbersAtTheEdgesOfNumeric(t *testing.T) {
	t.Parallel()
	e := newEnv(t)

	// The most digits before and after the point that PostgreSQL's numeric
	// type holds, and a zero with a large exponent.
	for _, body := range []string{"1e131071", "1e-16383", "0e99999", "0." + strings.Repeat("0", 99) + "1e100"} {
		if _, err := e.client.Enqueue(t.Context(), rawArgs{Body: json.RawMessage(body)}, nil); err != nil {
			t.Errorf("enqueue %.20s: %v", body, err)
		}
	}
}

func TestEnqueueKeepsEscapedBackslashes(t *testing.T) {
	t.Parallel()
	e := newEnv(t)

	// A backslash followed by "u0000" is text, not a NUL character.
	id := e.enqueue(t, sendEmail{To: `\u0000`}, nil)
	var got sendEmail
	if err := json.Unmarshal(e.get(t, id).Payload, &got); err != nil || got.To != `\u0000` {
		t.Fatalf("got %q, %v", got.To, err)
	}
}

type emptyKind struct{}

func (emptyKind) Kind() string { return "" }

func TestEnqueueManyKeepsOrderInOneTransaction(t *testing.T) {
	t.Parallel()
	e := newEnv(t)

	params := make([]taskmill.EnqueueParams, 50)
	for i := range params {
		params[i] = taskmill.EnqueueParams{Args: sendEmail{UserID: int64(i)}}
	}

	res, err := e.client.EnqueueMany(t.Context(), params)
	if err != nil {
		t.Fatal(err)
	}
	for i, r := range res {
		var got sendEmail
		if err := json.Unmarshal(e.get(t, r.ID).Payload, &got); err != nil {
			t.Fatal(err)
		}
		if got.UserID != int64(i) {
			t.Fatalf("result %d points at the task of user %d", i, got.UserID)
		}
	}

	// One invalid task rejects the whole batch.
	params[10].Options = &taskmill.EnqueueOptions{Priority: 99}
	if _, err := e.client.EnqueueMany(t.Context(), params); !errors.Is(err, taskmill.ErrInvalidArgument) {
		t.Fatalf("got %v, want ErrInvalidArgument", err)
	}
	if n := e.count(t, "SELECT count(*) FROM {s}.tasks"); n != 50 {
		t.Fatalf("got %d tasks, want 50", n)
	}
}

func TestEnqueueTxCommitsWithTheTransaction(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ctx := t.Context()

	// Rolled back: the task never existed.
	err := pgx.BeginFunc(ctx, e.pool, func(tx pgx.Tx) error {
		if _, err := e.client.EnqueueTx(ctx, taskmill.Pgx(tx), sendEmail{To: "rollback"}, nil); err != nil {
			return err
		}
		return errors.New("roll back")
	})
	if err == nil || err.Error() != "roll back" {
		t.Fatal(err)
	}
	if n := e.count(t, "SELECT count(*) FROM {s}.tasks"); n != 0 {
		t.Fatalf("rolled-back enqueue left %d tasks", n)
	}

	// Committed: the task exists.
	var id int64
	err = pgx.BeginFunc(ctx, e.pool, func(tx pgx.Tx) error {
		res, err := e.client.EnqueueTx(ctx, taskmill.Pgx(tx), sendEmail{To: "commit"}, nil)
		id = res.ID
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	e.wantState(t, id, taskmill.StateAvailable)
}

func TestEnqueueTxWithDatabaseSQL(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ctx := t.Context()

	db, err := sql.Open("pgx", testdb.URL(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	res, err := e.client.EnqueueManyTx(ctx, taskmill.SQL(tx), []taskmill.EnqueueParams{
		{Args: sendEmail{To: "a"}},
		{Args: sendEmail{To: "b"}, Options: &taskmill.EnqueueOptions{GroupKey: "g", DedupeKey: "k"}},
	})
	if err != nil {
		t.Fatal(err)
	}

	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	for _, r := range res {
		e.wantState(t, r.ID, taskmill.StateAvailable)
	}
}

func TestEnqueueDeduplicates(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	opts := &taskmill.EnqueueOptions{DedupeKey: "order-42"}

	first, err := e.client.Enqueue(t.Context(), sendEmail{To: "a"}, opts)
	if err != nil || first.Duplicate {
		t.Fatalf("first enqueue: %+v, %v", first, err)
	}

	second, err := e.client.Enqueue(t.Context(), sendEmail{To: "b"}, opts)
	if err != nil {
		t.Fatal(err)
	}
	if !second.Duplicate || second.ID != first.ID {
		t.Fatalf("second enqueue: got %+v, want a duplicate of %d", second, first.ID)
	}

	// The same key for another kind or queue is a different key.
	other, err := e.client.Enqueue(t.Context(), report{Name: "x"}, opts)
	if err != nil || other.Duplicate {
		t.Fatalf("other kind: %+v, %v", other, err)
	}

	// Within one batch, the first occurrence wins.
	res, err := e.client.EnqueueMany(t.Context(), []taskmill.EnqueueParams{
		{Args: sendEmail{}, Options: &taskmill.EnqueueOptions{DedupeKey: "batch"}},
		{Args: sendEmail{}, Options: &taskmill.EnqueueOptions{DedupeKey: "batch"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if res[0].Duplicate || !res[1].Duplicate || res[1].ID != res[0].ID {
		t.Fatalf("batch: got %+v", res)
	}

	// An expired key admits a new task.
	e.exec(t, "UPDATE {s}.dedupe_keys SET expires_at = now() - interval '1 second'")
	third, err := e.client.Enqueue(t.Context(), sendEmail{To: "c"}, opts)
	if err != nil || third.Duplicate || third.ID == first.ID {
		t.Fatalf("after expiry: %+v, %v", third, err)
	}
}

// An enqueue of a key that a task holds creates nothing, so it must write
// nothing: no transaction ID, which every write takes, no lock on the key's
// row, and no task ID from the sequence. A producer that sends many
// duplicates, and a cancel of the task that holds the key, would pay for
// them.
func TestDuplicateEnqueueWritesNothing(t *testing.T) {
	t.Parallel()
	e := newEnv(t)

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second) // a wait for a lock would end it
	defer cancel()

	opts := &taskmill.EnqueueOptions{DedupeKey: "order-42", GroupKey: "customer-7"}
	first := e.keyed(t, opts)
	last := e.count(t, "SELECT last_value FROM {s}.tasks_id_seq")

	tx := e.begin(t)
	for _, enqueue := range []func() (taskmill.EnqueueResult, error){
		func() (taskmill.EnqueueResult, error) {
			return e.client.EnqueueTx(ctx, taskmill.Pgx(tx), sendEmail{}, opts)
		},
		func() (taskmill.EnqueueResult, error) {
			res, err := e.client.EnqueueManyTx(ctx, taskmill.Pgx(tx), []taskmill.EnqueueParams{
				{Args: sendEmail{}, Options: opts}, {Args: sendEmail{}, Options: opts},
			})
			if err != nil {
				return taskmill.EnqueueResult{}, err
			}
			return res[1], nil
		},
	} {
		if dup, err := enqueue(); err != nil || !dup.Duplicate || dup.ID != first.ID {
			t.Fatalf("got %+v, %v; want a duplicate of task %d", dup, err, first.ID)
		}
	}

	var wrote bool
	if err := tx.QueryRow(ctx, "SELECT pg_current_xact_id_if_assigned() IS NOT NULL").Scan(&wrote); err != nil {
		t.Fatal(err)
	}
	if wrote {
		t.Error("the transaction of the duplicate enqueues took a transaction ID: they wrote")
	}
	if n := e.count(t, "SELECT last_value FROM {s}.tasks_id_seq"); n != last {
		t.Errorf("the duplicate enqueues took %d task IDs from the sequence, want none", n-last)
	}

	// The open transaction holds nothing that a cancel of the task needs.
	if err := e.client.Cancel(ctx, first.ID); err != nil {
		t.Fatalf("cancel while the transaction of the duplicate enqueues is open: %v", err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
}

// An enqueue answers from what was committed when it began. While a cancel
// of the task that holds the key is not committed, the task holds the key,
// so the enqueue is a duplicate, and does not wait for the cancel.
func TestEnqueueDoesNotWaitForACancelOfTheKeysTask(t *testing.T) {
	t.Parallel()
	e := newEnv(t)

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second) // a wait for the cancel would end it
	defer cancel()

	opts := &taskmill.EnqueueOptions{DedupeKey: "order-42"}
	first := e.keyed(t, opts)
	tx := e.begin(t)
	if err := e.client.CancelTx(ctx, taskmill.Pgx(tx), first.ID); err != nil {
		t.Fatal(err)
	}

	if dup, err := e.client.Enqueue(ctx, sendEmail{}, opts); err != nil || !dup.Duplicate || dup.ID != first.ID {
		t.Fatalf("while the cancel is open: got %+v, %v; want a duplicate of task %d", dup, err, first.ID)
	}

	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if next, err := e.client.Enqueue(ctx, sendEmail{}, opts); err != nil || next.Duplicate {
		t.Fatalf("after the cancel: got %+v, %v; want a new task", next, err)
	}
}

// A transaction at REPEATABLE READ or SERIALIZABLE sees the keys as they
// were when it began. A key that it sees as held may be free by now: its
// enqueue must find that out, and create the task, not report a duplicate of
// a task that was canceled meanwhile.
func TestEnqueueInASnapshotTransactionSeesAFreedKey(t *testing.T) {
	t.Parallel()
	for _, level := range []pgx.TxIsoLevel{pgx.RepeatableRead, pgx.Serializable} {
		t.Run(string(level), func(t *testing.T) {
			t.Parallel()
			e := newEnv(t)
			ctx := t.Context()

			opts := &taskmill.EnqueueOptions{DedupeKey: "order-42"}
			first := e.keyed(t, opts)

			tx, err := e.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: level})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = tx.Rollback(context.Background()) }()

			// The transaction's snapshot: the first task holds the key.
			if _, err := tx.Exec(ctx, replaceSchema("SELECT count(*) FROM {s}.dedupe_keys", e.schema)); err != nil {
				t.Fatal(err)
			}
			e.cancel(t, first.ID)

			res, err := e.client.EnqueueTx(ctx, taskmill.Pgx(tx), sendEmail{}, opts)
			if err != nil || res.Duplicate {
				t.Fatalf("got %+v, %v; want a new task: the key is free", res, err)
			}

			if err := tx.Commit(ctx); err != nil {
				t.Fatal(err)
			}
			e.wantState(t, res.ID, taskmill.StateAvailable)
		})
	}
}

func TestEnqueueDeduplicatesConcurrently(t *testing.T) {
	t.Parallel()
	e := newEnvWith(t, testdb.PoolSize(t, 24), nil)

	const producers, keys = 20, 50
	var wg sync.WaitGroup
	errs := make(chan error, producers)
	for range producers {
		wg.Go(func() {
			params := make([]taskmill.EnqueueParams, keys)
			for k := range params {
				params[k] = taskmill.EnqueueParams{
					Args:    sendEmail{UserID: int64(k)},
					Options: &taskmill.EnqueueOptions{DedupeKey: strconv.Itoa(k)},
				}
			}

			res, err := e.client.EnqueueMany(t.Context(), params)
			if err != nil {
				errs <- err
				return
			}
			for _, r := range res {
				if r.ID == 0 {
					errs <- errors.New("result without a task ID")
					return
				}
			}
		})
	}

	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}

	if n := e.count(t, "SELECT count(*) FROM {s}.tasks"); n != keys {
		t.Fatalf("got %d tasks, want exactly %d", n, keys)
	}
}

// brokenKeys counts what breaks the rule of one unfinished task per dedupe
// key, in one snapshot: the keys with more than one unfinished task, the
// unfinished tasks that do not hold their key, and the keys that a canceled
// or deleted task holds. The tasks are sendEmails to their key.
func (e *env) brokenKeys(t *testing.T) int {
	t.Helper()
	return e.count(t, `
SELECT (SELECT count(*) FROM (SELECT FROM {s}.tasks WHERE state <> 'canceled' GROUP BY payload ->> 'to' HAVING count(*) > 1) k)
     + (SELECT count(*) FROM {s}.tasks t WHERE t.state <> 'canceled'
        AND NOT EXISTS (SELECT FROM {s}.dedupe_keys d WHERE d.task_id = t.id))
     + (SELECT count(*) FROM {s}.dedupe_keys d
        WHERE NOT EXISTS (SELECT FROM {s}.tasks t WHERE t.id = d.task_id AND t.state <> 'canceled'))`)
}

// lostToOthers reports whether err is one that an operation gets when it
// loses to concurrent ones: it finds the task in another state, or gone, or
// its key taken, or PostgreSQL ends it to end a wait.
func lostToOthers(err error) bool {
	pgErr, ok := errors.AsType[*pgconn.PgError](err)
	return errors.Is(err, taskmill.ErrConflict) || errors.Is(err, taskmill.ErrInvalidState) ||
		errors.Is(err, taskmill.ErrNotFound) || ok && (pgErr.Code == "40P01" || pgErr.Code == "55P03")
}

// operate does to task id what an operator may do: the choice turns with n.
func (e *env) operate(ctx context.Context, n int, id int64) error {
	switch n % 6 {
	case 0, 1:
		return e.client.Cancel(ctx, id)
	case 2, 3:
		return e.client.Retry(ctx, id)
	case 4:
		return e.client.Delete(ctx, id)
	}
	_, err := e.client.RetryMatching(ctx, bulk(&taskmill.TaskFilter{States: []taskmill.State{taskmill.StateCanceled}}))
	return err
}

// Producers enqueue a few keys again and again while operators cancel,
// retry, and delete their tasks, one at a time and in bulk. Whatever order
// the database gives them, a key stands for at most one unfinished task,
// every unfinished task holds its key, and no key is held by a task that is
// canceled or gone. Each check reads one snapshot, so the rule holds at
// every moment, not only at the end.
func TestDedupeKeysHoldUnderConcurrentOperations(t *testing.T) {
	t.Parallel()
	e := newEnvWith(t, testdb.PoolSize(t, 12), nil)

	const workers, rounds, keys = 8, 15, 3
	var last atomic.Int64 // the newest task that an enqueue named

	round := func(w, i int) error {
		key := strconv.Itoa((w + i) % keys)
		res, err := e.client.Enqueue(t.Context(), sendEmail{To: key}, &taskmill.EnqueueOptions{DedupeKey: key, GroupKey: "g" + key})
		if err != nil && !lostToOthers(err) {
			return fmt.Errorf("enqueue: %w", err)
		}

		// The task that the enqueue named, or another recent one.
		id := res.ID
		if err != nil || i%3 == 0 {
			id = max(1, last.Load()-int64(i%7))
		}
		last.Store(max(last.Load(), res.ID))

		if err := e.operate(t.Context(), w+i, id); err != nil && !lostToOthers(err) {
			return fmt.Errorf("on task %d: %w", id, err)
		}

		if n := e.brokenKeys(t); n != 0 {
			return fmt.Errorf("%d keys or tasks break the rule of one unfinished task per key", n)
		}
		return nil
	}

	var wg sync.WaitGroup
	errs := make(chan error, workers)
	for w := range workers {
		wg.Go(func() {
			for i := range rounds {
				if err := round(w, i); err != nil {
					errs <- fmt.Errorf("round %d of worker %d: %w", i, w, err)
					return
				}
			}
		})
	}

	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

func TestEnqueueFIFOGroupHasOneHead(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	group := &taskmill.EnqueueOptions{GroupKey: "order-1"}

	head := e.enqueue(t, sendEmail{To: "1"}, group)
	res, err := e.client.EnqueueMany(t.Context(), []taskmill.EnqueueParams{
		{Args: sendEmail{To: "2"}, Options: group},
		{Args: sendEmail{To: "3"}, Options: group},
		{Args: sendEmail{To: "other"}, Options: &taskmill.EnqueueOptions{GroupKey: "order-2"}},
	})
	if err != nil {
		t.Fatal(err)
	}

	e.wantState(t, head, taskmill.StateAvailable)
	e.wantState(t, res[0].ID, taskmill.StatePending)
	e.wantState(t, res[1].ID, taskmill.StatePending)
	e.wantState(t, res[2].ID, taskmill.StateAvailable)
	if n := e.count(t, "SELECT tasks FROM {s}.groups WHERE group_key = 'order-1'"); n != 3 {
		t.Fatalf("group count: got %d, want 3", n)
	}
}

func TestEnqueueLimitsLeaveTheTransactionUsable(t *testing.T) {
	t.Parallel()
	grow := func(ctx context.Context, tasks []*taskmill.EnqueueTask, next func(context.Context) error) error {
		for _, task := range tasks {
			if task.Metadata["grow"] != "" {
				task.Metadata["blob"] = strings.Repeat("x", 64<<10)
			}
		}
		return next(ctx)
	}
	e := newEnvWith(t, testdb.Pool(t), &taskmill.ClientConfig{EnqueueMiddleware: []taskmill.EnqueueMiddleware{grow}})

	many := make([]taskmill.EnqueueParams, 10_001)
	for i := range many {
		many[i] = taskmill.EnqueueParams{Args: sendEmail{}}
	}
	heavy := make([]taskmill.EnqueueParams, 65) // each payload just under its own limit of 1 MiB
	for i := range heavy {
		heavy[i] = taskmill.EnqueueParams{Args: sendEmail{To: strings.Repeat("x", 1<<20-64)}}
	}
	bigMetadata := &taskmill.EnqueueOptions{Metadata: map[string]string{"k": strings.Repeat("x", 64<<10)}}

	for _, c := range []struct {
		name   string
		params []taskmill.EnqueueParams
	}{
		{"metadata over 64 KiB", []taskmill.EnqueueParams{{Args: sendEmail{}, Options: bigMetadata}}},
		{"metadata that middleware grows over 64 KiB", []taskmill.EnqueueParams{{
			Args: sendEmail{}, Options: &taskmill.EnqueueOptions{Metadata: map[string]string{"grow": "yes"}},
		}}},
		{"more than 10,000 tasks", many},
		{"more than 64 MiB in one call", heavy},
	} {
		tx := e.begin(t)
		if _, err := e.client.EnqueueManyTx(t.Context(), taskmill.Pgx(tx), c.params); !errors.Is(err, taskmill.ErrInvalidArgument) {
			t.Fatalf("%s: got %v, want ErrInvalidArgument", c.name, err)
		}

		// No statement ran, so the caller's transaction goes on.
		if _, err := e.client.EnqueueTx(t.Context(), taskmill.Pgx(tx), sendEmail{}, nil); err != nil {
			t.Fatalf("%s: the transaction is no longer usable: %v", c.name, err)
		}
		if err := tx.Commit(t.Context()); err != nil {
			t.Fatal(err)
		}
	}

	if n := e.count(t, "SELECT count(*) FROM {s}.tasks"); n != 4 {
		t.Fatalf("got %d tasks, want the 4 enqueued after the failures", n)
	}

	// Sizes count what is stored: & takes one byte, not the six of \u0026.
	amps := &taskmill.EnqueueOptions{Metadata: map[string]string{"k": strings.Repeat("&", 20_000)}}
	if _, err := e.client.Enqueue(t.Context(), sendEmail{}, amps); err != nil {
		t.Fatalf("20,000 ampersands: %v", err)
	}
}

// countedArgs encodes to a payload just under its limit of 1 MiB, and counts
// how often it is encoded.
type countedArgs struct {
	encoded *atomic.Int32
}

func (countedArgs) Kind() string { return "test.counted" }

var nearlyMiB = []byte(`"` + strings.Repeat("x", 1<<20-64) + `"`)

func (a countedArgs) MarshalJSON() ([]byte, error) {
	a.encoded.Add(1)
	return nearlyMiB, nil
}

func TestEnqueueStopsAtTheFirstTaskPastTheLimit(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	var encoded atomic.Int32

	params := make([]taskmill.EnqueueParams, 100)
	for i := range params {
		params[i] = taskmill.EnqueueParams{Args: countedArgs{encoded: &encoded}}
	}

	if _, err := e.client.EnqueueMany(t.Context(), params); !errors.Is(err, taskmill.ErrInvalidArgument) {
		t.Fatalf("got %v, want ErrInvalidArgument", err)
	}
	// 64 such payloads fit in 64 MiB; the 65th is past it.
	if n := encoded.Load(); n != 65 {
		t.Fatalf("encoded %d payloads, want 65", n)
	}
}

func TestEnqueueMiddlewareWrapsEnqueues(t *testing.T) {
	t.Parallel()
	var calls []string

	outer := func(ctx context.Context, tasks []*taskmill.EnqueueTask, next func(context.Context) error) error {
		calls = append(calls, "outer")
		err := next(ctx)
		for _, task := range tasks {
			calls = append(calls, fmt.Sprintf("%s/%s %d %t", task.Queue, task.Kind, task.Result.ID, task.Result.Duplicate))
		}
		return err
	}
	inner := func(ctx context.Context, tasks []*taskmill.EnqueueTask, next func(context.Context) error) error {
		calls = append(calls, "inner")
		for _, task := range tasks {
			task.Metadata["traceparent"] = "00-" + task.Kind
		}
		return next(ctx)
	}

	e := newEnvWith(t, testdb.Pool(t), &taskmill.ClientConfig{
		EnqueueMiddleware: []taskmill.EnqueueMiddleware{outer, inner},
	})

	callerMeta := map[string]string{"source": "caller"}
	dedupe := &taskmill.EnqueueOptions{DedupeKey: "k", Metadata: callerMeta}
	res, err := e.client.EnqueueMany(t.Context(), []taskmill.EnqueueParams{
		{Args: sendEmail{}, Options: dedupe},
		{Args: report{Name: "daily"}},
		{Args: sendEmail{}, Options: dedupe}, // repeats the first task's dedupe key
	})
	if err != nil {
		t.Fatal(err)
	}

	want := []string{
		"outer", "inner",
		fmt.Sprintf("default/email.send %d false", res[0].ID),
		fmt.Sprintf("reports/report.build %d false", res[1].ID),
		fmt.Sprintf("default/email.send %d true", res[0].ID),
	}
	if !slices.Equal(calls, want) {
		t.Fatalf("got calls %q, want %q", calls, want)
	}

	meta := e.get(t, res[0].ID).Metadata
	if meta["traceparent"] != "00-email.send" || meta["source"] != "caller" {
		t.Fatalf("got metadata %v", meta)
	}
	if len(callerMeta) != 1 {
		t.Fatalf("middleware changed the caller's metadata: %v", callerMeta)
	}
}

func TestEnqueueMiddlewareThatMisusesNext(t *testing.T) {
	t.Parallel()
	for _, c := range []struct {
		name  string
		mw    taskmill.EnqueueMiddleware
		fails bool // the enqueue reports an error
		tasks int
	}{
		{
			name:  "returns nil without calling next",
			mw:    func(context.Context, []*taskmill.EnqueueTask, func(context.Context) error) error { return nil },
			fails: true,
			tasks: 0,
		},
		{
			// The first call committed the task; the second stores nothing.
			name: "calls next twice",
			mw: func(ctx context.Context, _ []*taskmill.EnqueueTask, next func(context.Context) error) error {
				if err := next(ctx); err != nil {
					return err
				}
				return next(ctx)
			},
			fails: false,
			tasks: 1,
		},
		{
			// The store refuses the metadata, and the middleware hides it.
			name: "hides the error of next",
			mw: func(ctx context.Context, tasks []*taskmill.EnqueueTask, next func(context.Context) error) error {
				tasks[0].Metadata["blob"] = strings.Repeat("x", 64<<10)
				_ = next(ctx)
				return nil
			},
			fails: true,
			tasks: 0,
		},
	} {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			e := newEnvWith(t, testdb.Pool(t), &taskmill.ClientConfig{EnqueueMiddleware: []taskmill.EnqueueMiddleware{c.mw}})
			if _, err := e.client.Enqueue(t.Context(), sendEmail{}, nil); (err != nil) != c.fails {
				t.Fatalf("got error %v, want an error: %t", err, c.fails)
			}
			if n := e.count(t, "SELECT count(*) FROM {s}.tasks"); n != c.tasks {
				t.Fatalf("got %d tasks, want %d", n, c.tasks)
			}
		})
	}
}

func TestEnqueueMiddlewareThatFailsAfterNext(t *testing.T) {
	t.Parallel()
	logs := &logRecorder{}

	failAfter := func(ctx context.Context, _ []*taskmill.EnqueueTask, next func(context.Context) error) error {
		if err := next(ctx); err != nil {
			return err
		}
		return errors.New("exporter unavailable")
	}

	e := newEnvWith(t, testdb.Pool(t), &taskmill.ClientConfig{
		Logger:            slog.New(logs),
		EnqueueMiddleware: []taskmill.EnqueueMiddleware{failAfter},
	})

	// Outside the caller's transaction the task committed: the enqueue
	// reports it, so that a retry does not enqueue it again.
	res, err := e.client.Enqueue(t.Context(), sendEmail{}, nil)
	if err != nil {
		t.Fatalf("got %v, want the committed task", err)
	}
	e.wantState(t, res.ID, taskmill.StateAvailable)
	if logs.count("enqueue middleware failed after the tasks were enqueued") != 1 {
		t.Fatal("the middleware's error was not logged")
	}

	// In the caller's transaction, the error lets the caller roll back, also
	// through a type that wraps the transaction with its Commit and Rollback.
	for name, wrap := range map[string]func(pgx.Tx) taskmill.PgxQuerier{
		"pgx.Tx":  func(tx pgx.Tx) taskmill.PgxQuerier { return tx },
		"wrapped": func(tx pgx.Tx) taskmill.PgxQuerier { return wrappedPgxTx{tx} },
	} {
		tx := e.begin(t)
		if _, err := e.client.EnqueueTx(t.Context(), taskmill.Pgx(wrap(tx)), sendEmail{}, nil); err == nil {
			t.Fatalf("%s: the enqueue in a transaction hid the middleware's error", name)
		}
		if err := tx.Rollback(t.Context()); err != nil {
			t.Fatal(err)
		}
	}

	if n := e.count(t, "SELECT count(*) FROM {s}.tasks"); n != 1 {
		t.Fatalf("got %d tasks, want only the first", n)
	}
}

func TestEnqueueNotifiesListeners(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	conn, err := pgx.Connect(ctx, testdb.DirectURL(t))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close(t.Context()) }()
	if _, err := conn.Exec(ctx, "LISTEN "+e.schema); err != nil {
		t.Fatal(err)
	}

	e.enqueue(t, sendEmail{}, &taskmill.EnqueueOptions{Queue: "mail"})
	n, err := conn.WaitForNotification(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if n.Payload != "a|mail" {
		t.Fatalf("got notification %q, want %q", n.Payload, "a|mail")
	}

	// A burst within the throttle window gets one trailing notification.
	for range 5 {
		e.enqueue(t, sendEmail{}, &taskmill.EnqueueOptions{Queue: "mail"})
	}
	if n, err = conn.WaitForNotification(ctx); err != nil || n.Payload != "a|mail" {
		t.Fatalf("trailing notification: %v, %v", n, err)
	}
}

// A client's own enqueues notify once they commit, in batches: a
// transaction of its own per window, which writes nothing, for the queues
// of all the enqueues in that window. No enqueue notifies in its own
// transaction, whose commit would hold the server's notify lock while it
// flushes.
func TestOwnEnqueuesNotifyInBatches(t *testing.T) {
	t.Parallel()
	notifies := &notifyCounter{}
	e := newTracedEnv(t, notifies)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	conn, err := pgx.Connect(ctx, testdb.DirectURL(t))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close(t.Context()) }()
	if _, err := conn.Exec(ctx, "LISTEN "+e.schema); err != nil {
		t.Fatal(err)
	}

	const queues = 50
	missing := map[string]bool{}
	start := time.Now()
	for i := range queues {
		queue := "q-" + strconv.Itoa(i)
		missing["a|"+queue] = true
		e.enqueue(t, sendEmail{}, &taskmill.EnqueueOptions{Queue: queue})
	}
	elapsed := time.Since(start)

	for len(missing) > 0 {
		n, err := conn.WaitForNotification(ctx)
		if err != nil {
			t.Fatalf("%d queues were not notified: %v", len(missing), err)
		}
		delete(missing, n.Payload)
	}
	if n := notifies.inline.Load(); n > 0 {
		t.Errorf("%d enqueues notified in their own transaction", n)
	}
	if n, most := notifies.batches.Load(), int64(elapsed/taskmill.NotifyWindow)+2; n > most {
		t.Errorf("%d enqueues over %v sent %d batches, want at most %d", queues, elapsed, n, most)
	}
}

// notifyCounter counts the batches of notifications that a pool sends, and
// the enqueue statements that notify themselves.
type notifyCounter struct {
	batches, inline atomic.Int64
}

func (c *notifyCounter) TraceQueryStart(ctx context.Context, _ *pgx.Conn, d pgx.TraceQueryStartData) context.Context {
	switch {
	case d.SQL == taskmill.NotifyQueuesSQL:
		c.batches.Add(1)
	case strings.Contains(d.SQL, "INSERT INTO") && strings.Contains(d.SQL, "pg_notify($11"):
		if notify, _ := d.Args[11].([]string); len(notify) > 0 {
			c.inline.Add(1)
		}
	}
	return ctx
}

func (*notifyCounter) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

// PostgreSQL ends one transaction of a deadlock, and it may be an enqueue
// that did nothing wrong: it waited for a dedupe key, or for the row of a
// FIFO group, between two transactions that wait for each other. On the
// client's pool, the enqueue is one statement in a transaction of its own:
// nothing of it stays, so it runs again, below its middleware, which sees one
// enqueue. In a transaction of the caller it cannot run again: that
// transaction is over, and the caller gets the deadlock.
//
// The test needs the error of a deadlock, not the deadlock: a trigger raises
// it at the statement's last write, the task, after the key and the group.
func TestEnqueueRunsAgainAfterADeadlock(t *testing.T) {
	t.Parallel()
	var calls atomic.Int32
	e := newEnvWith(t, testdb.Pool(t), &taskmill.ClientConfig{EnqueueMiddleware: []taskmill.EnqueueMiddleware{
		func(ctx context.Context, _ []*taskmill.EnqueueTask, next func(context.Context) error) error {
			calls.Add(1)
			return next(ctx)
		},
	}})
	ctx := t.Context()

	// The first insert of a task fails. A sequence counts the inserts: it
	// keeps its count when the statement that failed rolls back.
	e.exec(t, "CREATE SEQUENCE {s}.inserts")
	e.exec(t, `
CREATE FUNCTION {s}.deadlock_first() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF nextval('{s}.inserts') = 1 THEN
        RAISE EXCEPTION 'deadlock detected' USING ERRCODE = 'deadlock_detected';
    END IF;
    RETURN NEW;
END $$`)
	e.exec(t, "CREATE TRIGGER deadlock_first BEFORE INSERT ON {s}.tasks FOR EACH ROW EXECUTE FUNCTION {s}.deadlock_first()")

	opts := func(key string) *taskmill.EnqueueOptions {
		return &taskmill.EnqueueOptions{GroupKey: "g", DedupeKey: key}
	}

	for name, enqueue := range map[string]func() (taskmill.EnqueueResult, error){
		"Enqueue": func() (taskmill.EnqueueResult, error) {
			return e.client.Enqueue(ctx, sendEmail{}, opts("one"))
		},
		"EnqueueMany": func() (taskmill.EnqueueResult, error) {
			res, err := e.client.EnqueueMany(ctx, []taskmill.EnqueueParams{{Args: sendEmail{}, Options: opts("many")}})
			return append(res, taskmill.EnqueueResult{})[0], err
		},
	} {
		e.exec(t, "ALTER SEQUENCE {s}.inserts RESTART")
		calls.Store(0)

		if res, err := enqueue(); err != nil || res.Duplicate {
			t.Fatalf("%s: got %+v, %v; want a new task", name, res, err)
		}
		if n := calls.Load(); n != 1 {
			t.Errorf("%s: the enqueue middleware ran %d times, want 1", name, n)
		}
	}

	// The statements that failed left no task, and no count in the group.
	if n := e.count(t, "SELECT tasks FROM {s}.groups"); n != 2 || e.count(t, "SELECT count(*) FROM {s}.tasks") != 2 {
		t.Errorf("the group counts %d tasks, want it and the tasks table to hold 2", n)
	}

	e.exec(t, "ALTER SEQUENCE {s}.inserts RESTART")
	err := pgx.BeginFunc(ctx, e.pool, func(tx pgx.Tx) error {
		_, err := e.client.EnqueueTx(ctx, taskmill.Pgx(tx), sendEmail{}, opts("tx"))
		return err
	})
	if pgErr, ok := errors.AsType[*pgconn.PgError](err); !ok || pgErr.Code != "40P01" { // deadlock_detected
		t.Fatalf("EnqueueTx: got %v, want the deadlock", err)
	}
}

func TestEnqueueFindsADuplicateCommittedWhileItWaited(t *testing.T) {
	t.Parallel()
	for _, held := range []string{"a new key", "a key whose window ended"} {
		t.Run(held, func(t *testing.T) {
			t.Parallel()
			e := newEnv(t)
			opts := &taskmill.EnqueueOptions{DedupeKey: "order-7"}

			if held != "a new key" {
				// An earlier task held the key, and the maintainer has not
				// deleted the key yet.
				e.enqueue(t, sendEmail{To: "earlier"}, opts)
				e.exec(t, "UPDATE {s}.dedupe_keys SET expires_at = now() - interval '1 second'")
			}

			holder, first := e.holdKey(t, opts)
			got, err := behind(t, e, holder, func() (taskmill.EnqueueResult, error) {
				return e.client.Enqueue(t.Context(), sendEmail{To: "second"}, opts)
			})
			if err != nil || !got.Duplicate || got.ID != first {
				t.Fatalf("got %+v, %v; want a duplicate of task %d", got, err, first)
			}
		})
	}
}

// holdKey enqueues a task with the dedupe key of opts in a transaction that
// it leaves open, so that the transaction holds the key. It returns the
// transaction, and the task's ID.
func (e *env) holdKey(t *testing.T, opts *taskmill.EnqueueOptions) (pgx.Tx, int64) { //nolint:ireturn // pgx's transaction type is an interface
	t.Helper()
	tx := e.begin(t)
	first, err := e.client.EnqueueTx(t.Context(), taskmill.Pgx(tx), sendEmail{To: "first"}, opts)
	if err != nil || first.Duplicate {
		t.Fatalf("the enqueue that takes the key: got %+v, %v; want a new task", first, err)
	}
	return tx, first.ID
}

// behind runs enqueue, which waits for a dedupe key that the transaction
// holder holds, and commits holder while it waits. The statement of enqueue
// began before that commit, so it cannot see the task that took the key, and
// enqueue reads the key again.
func behind[T any](t *testing.T, e *env, holder pgx.Tx, enqueue func() (T, error)) (T, error) {
	t.Helper()
	type result struct {
		res T
		err error
	}
	done := make(chan result, 1)
	go func() {
		res, err := enqueue()
		done <- result{res, err}
	}()

	e.waitBlocked(t, "{s}.dedupe_keys")
	if err := holder.Commit(t.Context()); err != nil {
		t.Fatal(err)
	}

	got := recv(t, done, "the enqueue that waited for the key")
	return got.res, got.err
}

// beforeStatement calls a function once, before a connection of the pool
// sends a statement that contains fragment.
type beforeStatement struct {
	fragment string
	call     atomic.Pointer[func()]
}

// beforeKeyRead returns a beforeStatement for the statement with which an
// enqueue reads a dedupe key again.
func beforeKeyRead() *beforeStatement {
	return &beforeStatement{fragment: "SELECT key, task_id FROM"}
}

func (b *beforeStatement) TraceQueryStart(ctx context.Context, _ *pgx.Conn, d pgx.TraceQueryStartData) context.Context {
	if strings.Contains(d.SQL, b.fragment) {
		if f := b.call.Swap(nil); f != nil {
			(*f)()
		}
	}
	return ctx
}

func (*beforeStatement) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

// plainAndKeyed returns the params of a task without a key, for the queue
// "plain", and of a task with the dedupe key of opts.
func plainAndKeyed(opts *taskmill.EnqueueOptions) []taskmill.EnqueueParams {
	return []taskmill.EnqueueParams{
		{Args: sendEmail{}, Options: &taskmill.EnqueueOptions{Queue: "plain"}},
		{Args: sendEmail{}, Options: opts},
	}
}

// Outside a transaction, an enqueue has committed its tasks when it reads
// again a dedupe key that another transaction took while its statement ran,
// on the client's pool and on a connection of the caller's. So the end of
// the caller's context does not stop that read, and a duplicate never gets
// the ID 0: it gets the task that took the key, or, if that task was
// deleted meanwhile, a task of its own. Only a database that fails makes the
// enqueue return an error, once it stored the task without a key: an error
// that says so, so that the caller does not store it again.
func TestEnqueueFindsTheTaskOfADuplicateAfterItCommitted(t *testing.T) {
	t.Parallel()
	for _, c := range []struct {
		name   string
		conn   bool   // the enqueue runs on a connection outside a transaction; otherwise on the client's pool
		before string // what happens before the second read: "cancel", "delete", or "break"
		want   func(t *testing.T, r keyRead)
	}{
		{name: "the caller's context ends", before: "cancel", want: wantDuplicateOfFirst},
		{name: "the caller's context ends, on a connection", conn: true, before: "cancel", want: wantDuplicateOfFirst},
		{name: "the task that took the key is deleted", before: "delete", want: func(t *testing.T, r keyRead) {
			t.Helper()
			if r.err != nil || len(r.res) != 2 || r.res[1].ID == 0 || r.res[1].ID == r.first || r.res[1].Duplicate {
				t.Fatalf("got %+v, %v; want a task of its own", r.res, r.err)
			}
			if n := r.e.count(t, "SELECT count(*) FROM {s}.dedupe_keys WHERE task_id = $1", r.res[1].ID); n != 1 {
				t.Fatalf("the new task holds %d keys, want the freed one", n)
			}
		}},
		{name: "the connection breaks", conn: true, before: "break", want: func(t *testing.T, r keyRead) {
			t.Helper()
			if !errors.Is(r.err, taskmill.ErrStoredWithoutIDs) || r.res != nil {
				t.Fatalf("got %+v, %v; want an error that says that the tasks are stored", r.res, r.err)
			}
		}},
	} {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			read := beforeKeyRead()
			e := newTracedEnv(t, read)
			opts := &taskmill.EnqueueOptions{DedupeKey: "order-7"}
			holder, first := e.holdKey(t, opts)

			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			enqueue, conn := e.enqueueMany(ctx, t, c.conn, plainAndKeyed(opts))
			before := map[string]func(){
				"cancel": cancel,
				"delete": func() {
					if err := e.client.Delete(t.Context(), first); err != nil {
						t.Errorf("delete the task that took the key: %v", err)
					}
				},
				"break": func() { _ = conn.Conn().PgConn().Conn().Close() },
			}[c.before]
			read.call.Store(&before)

			res, err := behind(t, e, holder, enqueue)
			if n := e.count(t, "SELECT count(*) FROM {s}.tasks WHERE queue = 'plain'"); n != 1 {
				t.Errorf("the task without a key is stored %d times, want once", n)
			}
			c.want(t, keyRead{e: e, first: first, res: res, err: err})
		})
	}
}

// keyRead is what an enqueue returned that read a dedupe key again.
type keyRead struct {
	e     *env
	first int64 // the task that took the key
	res   []taskmill.EnqueueResult
	err   error
}

// wantDuplicateOfFirst fails the test unless the second task of r is a
// duplicate of the task that took the key.
func wantDuplicateOfFirst(t *testing.T, r keyRead) {
	t.Helper()
	if r.err != nil || len(r.res) != 2 || r.res[1] != (taskmill.EnqueueResult{ID: r.first, Duplicate: true}) {
		t.Fatalf("got %+v, %v; want a duplicate of task %d", r.res, r.err, r.first)
	}
}

// enqueueMany returns a call that enqueues params on ctx: on the client's
// pool, or, if conn, on a connection of e's pool outside a transaction,
// which it returns too.
func (e *env) enqueueMany(ctx context.Context, t *testing.T, conn bool, params []taskmill.EnqueueParams) (enqueue func() ([]taskmill.EnqueueResult, error), c *pgxpool.Conn) {
	t.Helper()
	if !conn {
		return func() ([]taskmill.EnqueueResult, error) { return e.client.EnqueueMany(ctx, params) }, nil
	}

	c, err := e.pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Release)
	return func() ([]taskmill.EnqueueResult, error) {
		return e.client.EnqueueManyTx(ctx, taskmill.Pgx(c.Conn()), params)
	}, c
}

// At REPEATABLE READ or SERIALIZABLE, a transaction cannot see a task that
// took a dedupe key after the transaction began. An enqueue in it then fails
// with PostgreSQL's serialization failure, which the caller matches and
// handles by running the transaction again, as for any write at those
// levels.
func TestEnqueueTxAtRepeatableReadFailsOnAKeyTakenSinceItBegan(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	opts := &taskmill.EnqueueOptions{DedupeKey: "order-7"}
	holder, _ := e.holdKey(t, opts)

	tx, err := e.pool.BeginTx(t.Context(), pgx.TxOptions{IsoLevel: pgx.RepeatableRead})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(t.Context()) }()
	e.txExec(t, tx, "SELECT 1") // the transaction's snapshot

	_, err = behind(t, e, holder, func() (taskmill.EnqueueResult, error) {
		return e.client.EnqueueTx(t.Context(), taskmill.Pgx(tx), sendEmail{To: "second"}, opts)
	})
	if pgErr, ok := errors.AsType[*pgconn.PgError](err); !ok || pgErr.Code != "40001" { // serialization_failure
		t.Fatalf("EnqueueTx: got %v, want a serialization failure", err)
	}
}

// In the caller's transaction nothing is committed yet, so the enqueue
// returns the error of the read, and the caller rolls back.
func TestEnqueueTxReturnsTheFailedReadOfADuplicate(t *testing.T) {
	t.Parallel()
	read := beforeKeyRead()
	e := newTracedEnv(t, read)
	opts := &taskmill.EnqueueOptions{DedupeKey: "order-7"}
	holder, _ := e.holdKey(t, opts)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	before := func() { cancel() }
	read.call.Store(&before)

	tx := e.begin(t)
	res, err := behind(t, e, holder, func() ([]taskmill.EnqueueResult, error) {
		return e.client.EnqueueManyTx(ctx, taskmill.Pgx(tx), plainAndKeyed(opts))
	})
	if !errors.Is(err, context.Canceled) || res != nil {
		t.Fatalf("got %+v, %v; want the error of the read", res, err)
	}
}

// A call with no tasks does nothing: it stores nothing and runs no enqueue
// middleware, which may expect at least one task.
func TestEnqueueOfNoTasksDoesNothing(t *testing.T) {
	t.Parallel()
	called := false
	atLeastOne := func(ctx context.Context, tasks []*taskmill.EnqueueTask, next func(context.Context) error) error {
		called = true
		if len(tasks) == 0 {
			return errors.New("no tasks")
		}
		return next(ctx)
	}
	e := newEnvWith(t, testdb.Pool(t), &taskmill.ClientConfig{EnqueueMiddleware: []taskmill.EnqueueMiddleware{atLeastOne}})

	if res, err := e.client.EnqueueMany(t.Context(), nil); res != nil || err != nil {
		t.Fatalf("EnqueueMany: got %v, %v; want nothing", res, err)
	}
	tx := e.begin(t)
	if res, err := e.client.EnqueueManyTx(t.Context(), taskmill.Pgx(tx), []taskmill.EnqueueParams{}); res != nil || err != nil {
		t.Fatalf("EnqueueManyTx: got %v, %v; want nothing", res, err)
	}
	if called {
		t.Fatal("the middleware ran for no tasks")
	}
}
