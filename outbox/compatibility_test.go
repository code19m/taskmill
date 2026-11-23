package outbox_test

import (
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/code19m/taskmill"
	"github.com/code19m/taskmill/outbox"
)

// TestStoredNamesNeverChange pins the names that every stored message
// carries. The test writes them out, because a test that compares a constant
// with itself passes after any rename. A failure here is a breaking change:
// it needs a new major version, not a new value in the test.
func TestStoredNamesNeverChange(t *testing.T) {
	t.Parallel()
	client, ob, pool := setup(t)
	ctx := t.Context()

	err := pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		_, err := ob.StoreTx(ctx, taskmill.Pgx(tx), outbox.Message{Topic: "orders", Value: []byte("placed")})
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	page, err := client.List(ctx, nil)
	if err != nil || len(page.Tasks) != 1 {
		t.Fatalf("got %d tasks, %v; want the stored message", len(page.Tasks), err)
	}
	stored := page.Tasks[0]

	for _, tt := range []struct {
		what, got, want string
	}{
		{"outbox.Queue", outbox.Queue, "outbox"},
		{"the queue of a stored message", stored.Queue, "outbox"},
		{"the task kind of a stored message", stored.Kind, "taskmill.outbox.message"},
	} {
		if tt.got != tt.want {
			t.Errorf("%s is %q, want %q. docs/reference/compatibility.md promises that the outbox's queue and its task kind "+
				"never change: the messages that an earlier release stored, and those that it still stores during a rolling deploy, "+
				"would wait in a queue that no relay reads, or become dead as tasks of a kind that no relay handles",
				tt.what, tt.got, tt.want)
		}
	}
}
