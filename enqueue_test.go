package taskmill_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
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
	e := newEnv(t)
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
	if n := e.count(t, "SELECT count(*) FROM {s}.tasks"); n != 3 {
		t.Fatalf("got %d tasks, want the 3 enqueued after the failures", n)
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

	// In the caller's transaction, the error lets the caller roll back.
	tx := e.begin(t)
	if _, err := e.client.EnqueueTx(t.Context(), taskmill.Pgx(tx), sendEmail{}, nil); err == nil {
		t.Fatal("the enqueue in a transaction hid the middleware's error")
	}
	if err := tx.Rollback(t.Context()); err != nil {
		t.Fatal(err)
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

func TestEnqueueFindsADuplicateCommittedWhileItWaited(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	opts := &taskmill.EnqueueOptions{DedupeKey: "order-7"}
	tx := e.begin(t)
	first, err := e.client.EnqueueTx(t.Context(), taskmill.Pgx(tx), sendEmail{To: "first"}, opts)
	if err != nil {
		t.Fatal(err)
	}

	// A second enqueue of the key waits for the first one's transaction.
	type result struct {
		res taskmill.EnqueueResult
		err error
	}
	second := make(chan result, 1)
	go func() {
		res, err := e.client.Enqueue(t.Context(), sendEmail{To: "second"}, opts)
		second <- result{res, err}
	}()
	e.waitBlocked(t, "{s}.dedupe_keys")
	if err := tx.Commit(t.Context()); err != nil {
		t.Fatal(err)
	}
	got := recv(t, second, "the second enqueue")
	if got.err != nil || !got.res.Duplicate || got.res.ID != first.ID {
		t.Fatalf("got %+v, %v; want a duplicate of task %d", got.res, got.err, first.ID)
	}
}
