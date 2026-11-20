// Package ordertasks holds the tasks of the orders module: the types that
// other modules enqueue, and nothing else.
package ordertasks

import "github.com/code19m/taskmill"

// SendReceipt asks the orders module to email the receipt of an order.
type SendReceipt struct {
	OrderID int64 `json:"order_id"`
}

// Kind starts with the module's name, so two modules never pick the same kind.
func (SendReceipt) Kind() string { return "orders.send_receipt" }

// EnqueueDefaults puts the task in the module's queue, wherever it is enqueued.
func (SendReceipt) EnqueueDefaults() taskmill.EnqueueOptions {
	return taskmill.EnqueueOptions{Queue: "orders"}
}
