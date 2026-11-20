package testdb_test

import (
	"fmt"
	"runtime"
	"strings"
	"testing"

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

// A statement that a test gave up can go on in the database, and take its
// locks in another order than a drop of its schema does: here it holds the
// second table and then wants the first, and a drop locks the tables in the
// order of their creation, as a rule. A drop that began meanwhile would wait
// for the statement, and the statement for the drop: PostgreSQL would end the
// drop, as a deadlock, and the test would fail.
func TestSchemaIsDroppedAfterTheStatementsThatItsTestGaveUp(t *testing.T) {
	t.Parallel()
	pool := testdb.Pool(t)
	schema := testdb.Schema(t, pool)

	setup := strings.ReplaceAll("CREATE SCHEMA {s}; CREATE TABLE {s}.first (); CREATE TABLE {s}.second ()", "{s}", schema)
	if _, err := pool.Exec(t.Context(), setup); err != nil {
		t.Fatal(err)
	}

	testdb.GiveUp(t, pool, schema+".second", schema+".first")
	// The test ends here, and Schema drops the schema.
}
