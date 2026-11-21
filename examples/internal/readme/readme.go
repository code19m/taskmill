// Package readme holds the code of the quick look in README.md, so that it
// compiles. Its test runs the code against PostgreSQL. It is internal, so
// that the README's code is not part of the module's API.
package readme

import (
	"context"

	"github.com/jackc/pgx/v5"

	"github.com/code19m/taskmill"
)

// Mailer sends the shop's emails. The README leaves it out: it is the
// reader's own code.
type Mailer interface {
	SendReceipt(ctx context.Context, orderID int64) error
}

// SendReceipt is a task: a Go struct with a kind. taskmill stores it as JSON.
type SendReceipt struct {
	OrderID int64 `json:"order_id"`
}

// Kind names the task, so that a worker finds its handler.
func (SendReceipt) Kind() string { return "email.send_receipt" }

// placeOrder saves an order and its task in tx, so both commit or neither does.
func placeOrder(ctx context.Context, tx pgx.Tx, client *taskmill.Client, email string) error {
	var orderID int64
	row := tx.QueryRow(ctx, "INSERT INTO orders (email) VALUES ($1) RETURNING id", email)
	if err := row.Scan(&orderID); err != nil {
		return err
	}

	_, err := client.EnqueueTx(ctx, taskmill.Pgx(tx), SendReceipt{OrderID: orderID}, nil)
	return err
}

// ReceiptSender is the handler of SendReceipt tasks.
type ReceiptSender struct{ mailer Mailer }

// Handle returns nil when the work is done, or an error to try again later.
func (s *ReceiptSender) Handle(ctx context.Context, t *taskmill.Task[SendReceipt]) error {
	return s.mailer.SendReceipt(ctx, t.Args.OrderID)
}

// runWorker runs tasks until ctx ends.
func runWorker(ctx context.Context, client *taskmill.Client, mailer Mailer) error {
	var handlers taskmill.Handlers
	taskmill.AddHandler(&handlers, &ReceiptSender{mailer: mailer}, nil)

	worker, err := taskmill.NewWorker(client, &taskmill.WorkerConfig{Handlers: &handlers})
	if err != nil {
		return err
	}
	return worker.Run(ctx)
}
