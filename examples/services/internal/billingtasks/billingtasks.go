// Package billingtasks is the contract of the billing service's tasks: their
// kinds, their JSON fields, and their defaults. Billing owns it. The services
// that enqueue billing's tasks import it, and nothing else of billing.
package billingtasks

import "github.com/code19m/taskmill"

// CreateInvoice asks billing to invoice an order.
type CreateInvoice struct {
	OrderID int64 `json:"order_id"`
}

// Kind names the task.
func (CreateInvoice) Kind() string { return "billing.create_invoice" }

// EnqueueDefaults puts the task in the queue that billing's workers read.
func (CreateInvoice) EnqueueDefaults() taskmill.EnqueueOptions {
	return taskmill.EnqueueOptions{Queue: "billing"}
}
