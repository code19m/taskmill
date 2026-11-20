// Package billingtasks holds the tasks of the billing module: the types that
// other modules enqueue, and nothing else. So a module that enqueues them
// imports no part of billing's code.
package billingtasks

import "github.com/code19m/taskmill"

// CreateInvoice asks billing to invoice an order.
type CreateInvoice struct {
	OrderID int64 `json:"order_id"`
}

// Kind starts with the module's name, so two modules never pick the same kind.
func (CreateInvoice) Kind() string { return "billing.create_invoice" }

// EnqueueDefaults puts the task in the module's queue, wherever it is enqueued.
func (CreateInvoice) EnqueueDefaults() taskmill.EnqueueOptions {
	return taskmill.EnqueueOptions{Queue: "billing"}
}

// SendReminders asks billing to remind customers of unpaid invoices.
type SendReminders struct{}

// Kind names the task.
func (SendReminders) Kind() string { return "billing.send_reminders" }

// EnqueueDefaults puts the task in the module's queue.
func (SendReminders) EnqueueDefaults() taskmill.EnqueueOptions {
	return taskmill.EnqueueOptions{Queue: "billing"}
}
