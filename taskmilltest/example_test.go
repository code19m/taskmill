package taskmilltest_test

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/code19m/taskmill"
	"github.com/code19m/taskmill/taskmilltest"
)

// SendWelcome is the task that signing up enqueues.
type SendWelcome struct {
	UserID int64 `json:"user_id"`
}

func (SendWelcome) Kind() string { return "user.welcome" }

// signUp is the code under test.
func signUp(ctx context.Context, client *taskmill.Client, userID int64) error {
	_, err := client.Enqueue(ctx, SendWelcome{UserID: userID}, nil)
	return err
}

// testSignUp is the body of a test, TestSignUp(t), that checks that signing
// up enqueues a welcome email, and that the email's handler succeeds. pool
// connects to the test database.
func testSignUp(t *testing.T, pool *pgxpool.Pool, sendEmail func(userID int64) error) {
	t.Helper()
	client := taskmilltest.NewClient(t, pool, nil)
	if err := signUp(t.Context(), client, 42); err != nil {
		t.Fatal(err)
	}
	tasks := taskmilltest.Enqueued[SendWelcome](t, client)
	if len(tasks) != 1 || tasks[0].Args.UserID != 42 {
		t.Fatalf("enqueued %+v", tasks)
	}

	handler := taskmill.HandlerFunc[SendWelcome](func(_ context.Context, task *taskmill.Task[SendWelcome]) error {
		return sendEmail(task.Args.UserID)
	})
	info, err := taskmilltest.Run(t, client, handler, SendWelcome{UserID: 42}, nil)
	if err != nil || info.State != taskmill.StateCompleted {
		t.Fatalf("got %s, %v", info.State, err)
	}
}

// This example tests code that enqueues a task, and the task's handler; see
// testSignUp above, which TestExampleRuns calls.
func Example() {
	_ = testSignUp
}
