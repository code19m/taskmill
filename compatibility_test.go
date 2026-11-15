package taskmill_test

// Tests of what docs/reference/compatibility.md promises never to change:
// the names that the database stores, or that the code of applications
// compares, the interfaces that applications implement, and the keys that
// results have when applications encode them. Each test writes the values
// out, because a test that compares a constant with itself passes after any
// rename. A failure here is a breaking change: it needs a new major version,
// not a new value in the test.

import (
	"context"
	"database/sql"
	"encoding"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/code19m/taskmill"
	"github.com/code19m/taskmill/internal/testdb"
	"github.com/code19m/taskmill/outbox"
)

// promised starts the reason of every failure in this file.
const promised = "docs/reference/compatibility.md promises that "

func TestStateValuesNeverChange(t *testing.T) {
	t.Parallel()
	const breaks = promised + "the State values never change, and that v1 adds none: the tasks table stores them, " +
		"so the tasks of a running installation would be in states that the code no longer knows, " +
		"and an application's comparisons with TaskInfo.State and its TaskFilter.States would stop matching"
	var want []string
	for _, tt := range []struct {
		state taskmill.State
		want  string
	}{
		{taskmill.StateScheduled, "scheduled"},
		{taskmill.StatePending, "pending"},
		{taskmill.StateAvailable, "available"},
		{taskmill.StateRunning, "running"},
		{taskmill.StateRetryable, "retryable"},
		{taskmill.StateCompleted, "completed"},
		{taskmill.StateDead, "dead"},
		{taskmill.StateCanceled, "canceled"},
	} {
		if string(tt.state) != tt.want {
			t.Errorf("the state %q is now %q. %s", tt.want, tt.state, breaks)
		}
		want = append(want, tt.want)
	}

	// The database stores a state as a value of this type: these are all
	// the states that a task can have.
	e := newEnv(t)
	var stored []string
	if err := e.pool.QueryRow(t.Context(), "SELECT enum_range(NULL::"+e.schema+".task_state)::text[]").Scan(&stored); err != nil {
		t.Fatal(err)
	}
	slices.Sort(stored)
	slices.Sort(want)
	if !slices.Equal(stored, want) {
		t.Errorf("the schema stores the states %q, want %q. %s", stored, want, breaks)
	}
}

func TestErrorCodesNeverChange(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		code taskmill.ErrorCode
		want string
	}{
		{taskmill.CodeTimeout, "timeout"},
		{taskmill.CodeLeaseLost, "lease_lost"},
		{taskmill.CodePanic, "panic"},
		{taskmill.CodeInvalidPayload, "invalid_payload"},
		{taskmill.CodeInvalidRow, "invalid_row"},
		{taskmill.CodeUnknownKind, "unknown_kind"},
		{taskmill.CodeNoHandler, "no_handler"},
	} {
		if string(tt.code) != tt.want {
			t.Errorf("the error code %q is now %q. %sthe error history codes never change: each task stores them in its errors, "+
				"so an application's comparisons with AttemptError.Code would miss the entries that earlier releases recorded",
				tt.want, tt.code, promised)
		}
	}
}

// textEmail is a sendEmail from a producer whose user IDs are text: its
// payload does not decode into sendEmail.
type textEmail struct {
	UserID string `json:"user_id"`
}

func (textEmail) Kind() string { return sendEmail{}.Kind() }

// An entry of a task's error history is stored as a JSON object. No type of
// the API carries its names: taskmill reads them into a type of its own.
func TestStoredErrorNamesNeverChange(t *testing.T) {
	t.Parallel()
	const breaks = promised + "the previous release's code works on this release's schema: the tasks of every release " +
		"store their error history under these names, so TaskInfo.Errors would lose what earlier releases recorded, " +
		"and the previous release would not read this one's entries during a rolling deploy"
	e := newEnv(t)

	// As every release so far stored it.
	at := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	stored := e.insertTask(t, `errors = '[{"attempt": 3, "at": "2026-01-02T03:04:05Z", "message": "smtp is down", "code": "timeout"}]'`)
	if got := e.get(t, stored).Errors; len(got) != 1 || got[0].Attempt != 3 || !got[0].At.Equal(at) ||
		got[0].Message != "smtp is down" || got[0].Code != taskmill.CodeTimeout {
		t.Errorf(`the stored entry reads as %+v, want attempt 3 at %s with the message "smtp is down" and the code "timeout". %s`,
			got, at, breaks)
	}

	// As this release stores it. Three statements write an entry, and an
	// entry that taskmill records has a code.
	w := newWorker(t, e, &taskmill.WorkerConfig{Handlers: noopHandlers()})
	failed := e.enqueue(t, textEmail{UserID: "not a number"}, nil) // its payload does not decode
	snoozed := e.enqueue(t, report{}, nil)                         // the worker has no handler for its kind
	for _, id := range []int64{failed, snoozed} {
		if _, err := w.RunTask(t.Context(), id); err != nil {
			t.Fatal(err)
		}
	}
	rescued := e.enqueue(t, sendEmail{}, nil)
	e.exec(t, `UPDATE {s}.tasks SET state = 'running', attempt = 1, lease_token = 1, attempted_at = now(), attempted_by = 'gone',
	           lease_expires_at = now() - interval '1 second' WHERE id = $1`, rescued)
	taskmill.Rescue(t.Context(), w)
	for what, id := range map[string]int64{"a failed attempt": failed, "the note of a snooze": snoozed, "a rescued attempt": rescued} {
		var names []string
		err := e.pool.QueryRow(t.Context(),
			"SELECT array(SELECT jsonb_object_keys(errors -> -1) ORDER BY 1) FROM "+e.schema+".tasks WHERE id = $1", id).Scan(&names)
		if err != nil {
			t.Fatal(err)
		}
		if want := []string{"at", "attempt", "code", "message"}; !slices.Equal(names, want) {
			t.Errorf("%s is stored under the names %q, want %q. %s", what, names, want, breaks)
		}
	}
}

// customEncodings walks typ and the types that it holds: those of its
// exported fields, its elements, and its keys. It returns what makes
// encoding/json write a type of pkgs in another form than Go's default: a
// struct tag, or a marshal method. seen notes the types of pkgs that it
// checked.
func customEncodings(typ reflect.Type, pkgs []string, seen map[reflect.Type]bool) (found []string) {
	if typ.Name() != "" {
		if !slices.Contains(pkgs, typ.PkgPath()) || seen[typ] {
			return nil // a type of another package, or one that is checked
		}
		seen[typ] = true
		for _, marshaler := range []reflect.Type{reflect.TypeFor[json.Marshaler](), reflect.TypeFor[encoding.TextMarshaler]()} {
			// A pointer has the methods of both kinds of receiver.
			if reflect.PointerTo(typ).Implements(marshaler) {
				found = append(found, fmt.Sprintf("%s implements %s", typ, marshaler))
			}
		}
	}
	switch typ.Kind() {
	case reflect.Map:
		found = append(found, customEncodings(typ.Key(), pkgs, seen)...)
		found = append(found, customEncodings(typ.Elem(), pkgs, seen)...)
	case reflect.Pointer, reflect.Slice:
		found = append(found, customEncodings(typ.Elem(), pkgs, seen)...)
	case reflect.Struct:
		for f := range typ.Fields() {
			if !f.IsExported() && !f.Anonymous {
				continue // no encoder reads it
			}
			if f.Tag != "" {
				found = append(found, fmt.Sprintf("%s.%s has the struct tag %q", typ, f.Name, f.Tag))
			}
			found = append(found, customEncodings(f.Type, pkgs, seen)...)
		}
	default:
	}
	return found
}

// What encoding/json makes of a type that taskmill hands to an application
// is Go's default, for good. gorelease does not see a struct tag, and calls
// a new method compatible, so this test looks for both.
func TestHandedTypesKeepTheirDefaultJSON(t *testing.T) {
	t.Parallel()
	client := reflect.TypeFor[*taskmill.Client]()
	// What a handler gets, what a publisher gets, and what each method of
	// Client returns.
	handed := []reflect.Type{reflect.TypeFor[taskmill.Task[sendEmail]](), reflect.TypeFor[outbox.Delivery]()}
	for method := range client.Methods() {
		handed = slices.AppendSeq(handed, method.Type.Outs())
	}
	pkgs := []string{client.Elem().PkgPath(), reflect.TypeFor[outbox.Delivery]().PkgPath()}
	seen := make(map[reflect.Type]bool)
	for _, typ := range handed {
		for _, found := range customEncodings(typ, pkgs, seen) {
			t.Errorf("%s. %sthe types that taskmill hands to an application have no JSON tags and no marshal methods, "+
				"and get none: either one changes what every application that encodes the type sends", found, promised)
		}
	}
	for _, reached := range []reflect.Type{reflect.TypeFor[taskmill.AttemptError](), reflect.TypeFor[taskmill.TaskCount](), reflect.TypeFor[outbox.Message]()} {
		if !seen[reached] {
			t.Errorf("the walk did not reach %s: it checks less than it says", reached)
		}
	}
}

func TestErrorClassesNeverChange(t *testing.T) {
	t.Parallel()
	check := func(what string, err error, want string) {
		t.Helper()
		// As OpenTelemetry's semconv.ErrorType reads it: from the first
		// error in the chain that names its class.
		var classified interface{ ErrorType() string }
		got := "none"
		if errors.As(err, &classified) {
			got = classified.ErrorType()
		}
		if got != want {
			t.Errorf("%s has the error class %q, want %q. %sthe classes that ErrorType() returns never change: "+
				"they are the error.type of spans and metrics, so the dashboards and alerts that match them would go blind",
				what, got, want, promised)
		}
	}
	for _, tt := range []struct {
		name string
		err  error
		want string
	}{
		{"ErrNotFound", taskmill.ErrNotFound, "not_found"},
		{"ErrLeaseLost", taskmill.ErrLeaseLost, "lease_lost"},
		{"ErrInvalidState", taskmill.ErrInvalidState, "invalid_state"},
		{"ErrInvalidArgument", taskmill.ErrInvalidArgument, "invalid_argument"},
		{"ErrSchemaOutdated", taskmill.ErrSchemaOutdated, "schema_outdated"},
		{"ErrConflict", taskmill.ErrConflict, "conflict"},
		{"ErrTaskCanceled", taskmill.ErrTaskCanceled, "canceled"},
		{"ErrAttemptTimeout", taskmill.ErrAttemptTimeout, "timeout"},
		{"ErrShutdown", taskmill.ErrShutdown, "shutdown"},
	} {
		check(tt.name, tt.err, tt.want)
	}

	// The other classes have no error value. An application meets them in
	// its middleware, as the error of an attempt, and as the error of an
	// enqueue.
	e := newEnv(t)
	var failed error // what the last attempt's middleware got from its next
	observe := func(ctx context.Context, _ *taskmill.TaskInfo, next func(context.Context) error) error {
		failed = next(ctx)
		return failed
	}
	skip := func(ctx context.Context, task *taskmill.TaskInfo, next func(context.Context) error) error {
		if task.Metadata["skip"] != "" {
			return nil
		}
		return next(ctx)
	}
	w := newWorker(t, e, &taskmill.WorkerConfig{
		Handlers:   handlers(func(context.Context, *taskmill.Task[sendEmail]) error { panic("boom") }, nil),
		Middleware: []taskmill.Middleware{observe, skip},
	})
	for _, tt := range []struct {
		what string
		args taskmill.Args
		opts *taskmill.EnqueueOptions
		want string
	}{
		{"a handler's panic", sendEmail{}, nil, "panic"},
		{"a payload that does not decode", textEmail{UserID: "not a number"}, nil, "invalid_payload"},
		{"middleware that skips the handler", sendEmail{}, &taskmill.EnqueueOptions{Metadata: map[string]string{"skip": "yes"}}, "middleware"},
	} {
		failed = nil
		if _, err := w.RunTask(t.Context(), e.enqueue(t, tt.args, tt.opts)); err != nil {
			t.Fatalf("%s: %v", tt.what, err)
		}
		check(tt.what, failed, tt.want)
	}

	skipping, err := taskmill.NewClient(t.Context(), e.pool, &taskmill.ClientConfig{
		Schema: e.schema,
		EnqueueMiddleware: []taskmill.EnqueueMiddleware{
			func(context.Context, []*taskmill.EnqueueTask, func(context.Context) error) error { return nil },
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = skipping.Enqueue(t.Context(), sendEmail{}, nil)
	check("enqueue middleware that skips the enqueue", err, "middleware")
}

func TestScheduledRunMetadataNeverChanges(t *testing.T) {
	t.Parallel()
	const breaks = promised + "the metadata keys of a scheduled run never change, nor the format of the second: " +
		"RFC 3339 with the schedule's offset from UTC, to the microsecond. Each run stores them, " +
		"so a handler that reads them would not find the time of its run, and TaskInfo.ScheduledFor " +
		"would not find the time of the runs that earlier releases enqueued"
	if taskmill.MetadataSchedule != "taskmill.schedule" || taskmill.MetadataScheduledFor != "taskmill.scheduled_for" {
		t.Errorf("MetadataSchedule is now %q, and MetadataScheduledFor %q. %s",
			taskmill.MetadataSchedule, taskmill.MetadataScheduledFor, breaks)
	}

	// A regular run is due at a whole minute. A run that Client.RunSchedule
	// asked for is due at the time of the request, to the microsecond.
	regular := time.Date(2024, 2, 29, 22, 30, 0, 0, time.UTC)
	requested := regular.Add(123_456 * time.Microsecond)
	cases := []struct {
		name string // of the schedule, and of its queue
		loc  *time.Location
		due  time.Time
		want string // the stored time that the run is for
	}{
		{"utc", time.UTC, regular, "2024-02-29T22:30:00Z"},
		{"ahead", time.FixedZone("ahead", (5*60+30)*60), regular, "2024-03-01T04:00:00+05:30"},
		{"behind", time.FixedZone("behind", -8*60*60), requested, "2024-02-29T14:30:00.123456-08:00"},
	}
	e := newEnv(t)
	var schedules []taskmill.Schedule
	for _, tt := range cases {
		schedules = append(schedules, taskmill.Schedule{
			Name: tt.name, Cron: "@yearly", Location: tt.loc, Args: sendEmail{}, Options: &taskmill.EnqueueOptions{Queue: tt.name},
		})
	}
	w := newWorker(t, e, &taskmill.WorkerConfig{Schedules: schedules})
	taskmill.RunSchedules(t.Context(), w) // stores the schedules
	for _, tt := range cases {
		e.exec(t, "UPDATE {s}.schedules SET next_run_at = $1 WHERE name = $2", tt.due, tt.name)
	}
	taskmill.RunSchedules(t.Context(), w)

	page, err := e.client.List(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]map[string]string{} // the metadata of each schedule's run
	for _, task := range page.Tasks {
		got[task.Queue] = task.Metadata
	}
	for _, tt := range cases {
		want := map[string]string{"taskmill.schedule": tt.name, "taskmill.scheduled_for": tt.want}
		if !maps.Equal(got[tt.name], want) {
			t.Errorf("the run of schedule %q has the metadata %v, want %v. %s", tt.name, got[tt.name], want, breaks)
		}
	}
}

// execAndQuery is the smallest PgxQuerier, and execAndQueryContext the
// smallest SQLQuerier: all that an application's own wrapper of a
// transaction must have. If one of them is no longer an argument for
// taskmill.Pgx or taskmill.SQL, its interface grew, and every such wrapper
// stops building.
type execAndQuery struct{ tx pgx.Tx }

func (q execAndQuery) Exec(ctx context.Context, query string, args ...any) (pgconn.CommandTag, error) {
	return q.tx.Exec(ctx, query, args...)
}

func (q execAndQuery) Query(ctx context.Context, query string, args ...any) (pgx.Rows, error) { //nolint:ireturn // pgx's rows type is an interface
	return q.tx.Query(ctx, query, args...)
}

type execAndQueryContext struct{ tx *sql.Tx }

func (q execAndQueryContext) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return q.tx.ExecContext(ctx, query, args...)
}

func (q execAndQueryContext) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return q.tx.QueryContext(ctx, query, args...)
}

func TestQuerierInterfacesNeverGrow(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ctx := t.Context()
	pgxTx := e.begin(t)
	sqlTx, err := openSQL(t).BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sqlTx.Rollback() }()
	for _, tt := range []struct {
		name   string
		tx     taskmill.Executor
		commit func() error
	}{
		{"PgxQuerier", taskmill.Pgx(execAndQuery{pgxTx}), func() error { return pgxTx.Commit(ctx) }},
		{"SQLQuerier", taskmill.SQL(execAndQueryContext{sqlTx}), sqlTx.Commit},
	} {
		res, err := e.client.EnqueueTx(ctx, tt.tx, sendEmail{}, nil)
		if err == nil {
			err = tt.commit()
		}
		if err != nil {
			t.Fatalf("an enqueue through a type with only the methods of %s: %v. %sinterfaces never grow: "+
				"an application's wrapper of its transaction has these methods and no other", tt.name, err, promised)
		}
		e.wantState(t, res.ID, taskmill.StateAvailable)
	}
}

func TestMigrationsTableKeepsItsColumns(t *testing.T) {
	t.Parallel()
	type column struct {
		name, kind, initial string // initial is the column's default
		primaryKey          bool
	}
	want := []column{
		{"version", "integer", "", true},
		{"name", "text", "", false},
		{"applied_at", "timestamp with time zone", "now()", false},
	}
	pool := testdb.Pool(t)
	for _, tt := range []struct {
		how    string
		create func(schema string) error
	}{
		{"Migrate", func(schema string) error {
			return taskmill.Migrate(t.Context(), pool, &taskmill.MigrateConfig{Schema: schema})
		}},
		// The first of Migrations creates the table for other tools.
		{"the bootstrap of Migrations", func(schema string) error {
			ms, err := taskmill.Migrations(schema)
			if err != nil {
				return err
			}
			_, err = pool.Exec(t.Context(), ms[0].SQL)
			return err
		}},
	} {
		schema := testdb.Schema(t, pool)
		if err := tt.create(schema); err != nil {
			t.Fatalf("%s: %v", tt.how, err)
		}
		// A table of another name has no columns here.
		r, err := pool.Query(t.Context(), `
SELECT a.attname, format_type(a.atttypid, a.atttypmod), coalesce(pg_get_expr(d.adbin, d.adrelid), ''),
       EXISTS (SELECT FROM pg_index i WHERE i.indrelid = a.attrelid AND i.indisprimary AND i.indnatts = 1 AND i.indkey[0] = a.attnum)
FROM pg_attribute a
LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
WHERE a.attrelid = to_regclass($1) AND a.attnum > 0 AND NOT a.attisdropped
ORDER BY a.attnum`, schema+".migrations")
		if err != nil {
			t.Fatal(err)
		}
		got, err := pgx.CollectRows(r, func(row pgx.CollectableRow) (c column, err error) {
			return c, row.Scan(&c.name, &c.kind, &c.initial, &c.primaryKey)
		})
		if err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(got, want) {
			t.Errorf("%s creates %s.migrations with the columns %+v, want %+v. %sthis table keeps its name and its columns: "+
				"teams that apply migrations with their own tool record each one there, so their "+
				"INSERT INTO <schema>.migrations (version, name) would fail, or NewClient would not find their schema up to date",
				tt.how, schema, got, want, promised)
		}
	}
}
