package testdb_test

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/code19m/taskmill/internal/testdb"
)

// ended is a testing.TB that notes how a call ended its test, in place of
// ending the test that runs it.
type ended struct {
	testing.TB
	skip, fatal string // the message of the Skipf or the Fatalf, if any
}

func (*ended) Helper() {}

func (e *ended) Skipf(format string, args ...any) {
	e.skip = fmt.Sprintf(format, args...)
	runtime.Goexit()
}

func (e *ended) Fatalf(format string, args ...any) {
	e.fatal = fmt.Sprintf(format, args...)
	runtime.Goexit()
}

func TestURLSkipsWithoutADatabaseButFailsInCI(t *testing.T) {
	const db = "postgres://localhost/test"
	for _, tt := range []struct {
		name, url, ci string
		want          ended
	}{
		{name: "a database", url: db},
		{name: "a database, in CI", url: db, ci: "true"},
		{name: "no database", want: ended{skip: "TASKMILL_TEST_DATABASE_URL is not set"}},
		{name: "no database, in CI", ci: "true", want: ended{fatal: "TASKMILL_TEST_DATABASE_URL is not set"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv(testdb.EnvURL, tt.url)
			t.Setenv("CI", tt.ci)
			// URL ends the goroutine of a test that it skips or fails.
			got, url, done := ended{TB: t}, "", make(chan struct{})
			go func() {
				defer close(done)
				url = testdb.URL(&got)
			}()
			<-done
			if url != tt.url {
				t.Errorf("got the URL %q, want %q", url, tt.url)
			}
			if (got.skip != "") != (tt.want.skip != "") || !strings.HasPrefix(got.skip, tt.want.skip) {
				t.Errorf("the test was skipped with %q, want %q", got.skip, tt.want.skip)
			}
			// The failure says what to set.
			if (got.fatal != "") != (tt.want.fatal != "") || !strings.HasPrefix(got.fatal, tt.want.fatal) {
				t.Errorf("the test failed with %q, want %q", got.fatal, tt.want.fatal)
			}
		})
	}
}

// A statement whose context ended can go on in the database after its caller
// returned: here it locks the schema's second table, outlives the request to
// cancel it, as a statement does that the server had not read by then, and
// locks the first table. A drop locks the tables in the order of their
// creation, as a rule, so a drop that began meanwhile would wait for the
// statement, and the statement for the drop: PostgreSQL would end the drop,
// as a deadlock.
func TestSchemaIsDroppedAfterTheStatementsThatItsTestGaveUp(t *testing.T) {
	t.Parallel()
	pool := testdb.Pool(t)
	schema := testdb.Schema(t, pool)
	ctx := t.Context()
	if _, err := pool.Exec(ctx, strings.ReplaceAll(`
CREATE SCHEMA {s};
CREATE TABLE {s}.first ();
CREATE TABLE {s}.second ();
CREATE FUNCTION {s}.outlive() RETURNS void LANGUAGE plpgsql AS $$
BEGIN
    LOCK TABLE {s}.second IN ACCESS SHARE MODE;
    BEGIN
        PERFORM pg_sleep(30);
    EXCEPTION WHEN query_canceled THEN
        PERFORM pg_sleep(0.3);
    END;
    LOCK TABLE {s}.first IN ACCESS SHARE MODE;
END $$`, "{s}", schema)); err != nil {
		t.Fatal(err)
	}

	statement, giveUp := context.WithCancel(ctx)
	defer giveUp()
	done := make(chan error, 1)
	go func() {
		_, err := pool.Exec(statement, "SELECT "+schema+".outlive()")
		done <- err
	}()
	for deadline := time.Now().Add(15 * time.Second); ; time.Sleep(10 * time.Millisecond) {
		var sleeping bool
		err := pool.QueryRow(ctx, `SELECT EXISTS (SELECT FROM pg_stat_activity WHERE wait_event = 'PgSleep' AND strpos(query, $1) > 0)`,
			schema+".outlive").Scan(&sleeping)
		if err != nil {
			t.Fatal(err)
		}
		if sleeping {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the statement did not start")
		}
	}
	giveUp()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("the statement that the test gave up returned %v, want context.Canceled", err)
	}
	// The test ends here, and Schema drops the schema.
}
