package outbox_test

import (
	"context"
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
