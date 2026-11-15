package outbox_test

import (
	"context"
	"errors"
	"fmt"
	"log"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/code19m/taskmill"
	"github.com/code19m/taskmill/outbox"
)

// This example stores an event with the business change that produced it,
// and relays stored events to a broker.
func Example() {
	ctx := context.Background()
	var pool *pgxpool.Pool      // the application's pool
	var client *taskmill.Client // from taskmill.NewClient
	ob := outbox.New(client)

	err := pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, "UPDATE orders SET status = 'paid' WHERE id = 42"); err != nil {
			return err
		}
		return ob.Store(ctx, taskmill.Pgx(tx), outbox.Message{
			Topic: "orders",
			Key:   []byte("42"),
			Value: []byte(`{"type": "order.paid", "order_id": 42}`),
		})
	})
	if err != nil {
		log.Fatal(err)
	}

	var handlers taskmill.Handlers
	ob.AddRelay(&handlers, outbox.PublisherFunc(func(_ context.Context, ds []outbox.Delivery) []error {
		errs := make([]error, len(ds)) // nil: published
		for _, d := range ds {
			log.Printf("publish %s to %s with key %s", d.ID, d.Topic, d.Key) // your broker client here
		}
		return errs
	}), nil) // nil: the default RelayOptions
	worker, err := taskmill.NewWorker(client, &taskmill.WorkerConfig{Handlers: &handlers}) // serves outbox.Queue
	if err != nil {
		log.Fatal(err)
	}
	_ = worker.Run(ctx)
}

// This example shows the contract of a publisher: one error per delivery, in
// the same order. Nil marks a delivery as published, an error retries it
// later, and a permanent error makes its message dead right away.
func ExamplePublisherFunc() {
	pub := outbox.PublisherFunc(func(_ context.Context, ds []outbox.Delivery) []error {
		errs := make([]error, len(ds))
		for i, d := range ds {
			switch {
			case len(d.Value) == 0:
				errs[i] = taskmill.Permanent(errors.New("empty event")) // no retry can fix it
			case d.Topic == "payments":
				errs[i] = errors.New("broker is down") // your broker client's error
			}
		}
		return errs
	})

	// The relay of a worker calls the publisher. This example calls it
	// itself, to show the results.
	errs := pub.Publish(context.Background(), []outbox.Delivery{
		{ID: "1", Message: outbox.Message{Topic: "orders", Value: []byte(`{"order_id": 42}`)}},
		{ID: "2", Message: outbox.Message{Topic: "payments", Value: []byte(`{"order_id": 42}`)}},
		{ID: "3", Message: outbox.Message{Topic: "orders"}},
	})
	for i, err := range errs {
		fmt.Printf("delivery %d: %v\n", i+1, err)
	}
	// Output:
	// delivery 1: <nil>
	// delivery 2: broker is down
	// delivery 3: permanent: empty event
}
