package otel_test

import (
	"context"
	"log"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/code19m/taskmill"
	tmotel "github.com/code19m/taskmill/otel"
	"github.com/code19m/taskmill/outbox"
)

// This example instruments a client, its worker, and an outbox relay, with
// OpenTelemetry's global providers.
func Example() {
	ctx := context.Background()
	var pool *pgxpool.Pool         // from pgxpool.New
	var handlers taskmill.Handlers // with your handlers
	var kafka outbox.PublisherFunc // publishes to your broker

	tel := tmotel.New(nil)
	client, err := taskmill.NewClient(ctx, pool, &taskmill.ClientConfig{
		EnqueueMiddleware: []taskmill.EnqueueMiddleware{tel.EnqueueMiddleware},
	})
	if err != nil {
		log.Fatal(err)
	}
	outbox.New(client).AddRelay(&handlers, tel.Publisher("kafka", kafka))
	worker, err := taskmill.NewWorker(client, &taskmill.WorkerConfig{
		Handlers:   &handlers,
		Middleware: []taskmill.Middleware{tel.Middleware},
	})
	if err != nil {
		log.Fatal(err)
	}
	if _, err := tel.ObserveQueues(client); err != nil {
		log.Fatal(err)
	}
	_ = worker.Run(ctx)
}
