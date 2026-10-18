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

	err := pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, "UPDATE orders SET status = 'paid' WHERE id = 42"); err != nil {
			return err
		}
		return outbox.Store(ctx, client, taskmill.Pgx(tx), nil, outbox.Message{
			Topic: "orders",
			Key:   "42",
			Value: []byte(`{"type": "order.paid", "order_id": 42}`),
		})
	})
	if err != nil {
		log.Fatal(err)
	}

	if err := outbox.Setup(ctx, client, nil); err != nil {
		log.Fatal(err)
	}
	var handlers taskmill.Handlers
	outbox.AddRelay(&handlers, outbox.PublisherFunc(func(_ context.Context, ds []outbox.Delivery) []error {
		errs := make([]error, len(ds)) // nil: published
		for _, d := range ds {
			log.Printf("publish %s to %s with key %s", d.ID, d.Topic, d.Key) // your broker client here
		}
		return errs
	}), nil)
	worker, err := taskmill.NewWorker(client, &taskmill.WorkerConfig{
		Handlers: &handlers,
		Queues:   map[string]taskmill.QueueOptions{outbox.DefaultQueue: {Concurrency: 4}},
	})
	if err != nil {
		log.Fatal(err)
	}
	_ = worker.Run(ctx)
}
