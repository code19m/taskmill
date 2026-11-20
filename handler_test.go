package taskmill_test

import (
	"context"
	"errors"
	"testing"

	"github.com/code19m/taskmill"
)

// pointerArgs is used through a pointer, with Kind on the value.
type pointerArgs struct {
	N int `json:"n"`
}

func (pointerArgs) Kind() string { return "pointer.args" }

func TestHandlerForPointerArgs(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	got := make(chan int, 1)
	var hs taskmill.Handlers
	taskmill.AddHandler(&hs, taskmill.HandlerFunc[*pointerArgs](func(_ context.Context, task *taskmill.Task[*pointerArgs]) error {
		got <- task.Args.N
		return nil
	}), nil)
	runWorker(t, e, &taskmill.WorkerConfig{Handlers: &hs, Queues: defaultQueue(1)})

	e.enqueue(t, &pointerArgs{N: 7}, nil)
	if n := recv(t, got, "the handler's result"); n != 7 {
		t.Fatalf("got args %d, want 7", n)
	}
}

func TestHandlerForInterfaceArgsIsRejected(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	var hs taskmill.Handlers
	taskmill.AddHandler(&hs, taskmill.HandlerFunc[taskmill.Args](func(context.Context, *taskmill.Task[taskmill.Args]) error {
		return nil
	}), nil)
	_, err := taskmill.NewWorker(e.client, &taskmill.WorkerConfig{Handlers: &hs, Queues: defaultQueue(1)})
	if !errors.Is(err, taskmill.ErrInvalidArgument) {
		t.Fatalf("got %v, want ErrInvalidArgument", err)
	}
}
