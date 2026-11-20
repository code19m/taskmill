// Package orms shows how to enqueue a task in the transaction of an ORM:
// GORM in gorm.go, and ent in ent.go. The guide to transactions,
// docs/guides/transactions.md, links here.
//
// Both ORMs open their database/sql handle on taskmill's pgx pool, with pgx's
// database/sql driver, so taskmill.SQL can run taskmill's statements in their
// transactions. This module is apart from taskmill's own, so that taskmill's
// users never download GORM or ent: see docs/design/dependencies.md.
package orms

// SendReceipt is the task that a new order enqueues.
type SendReceipt struct {
	OrderID int64 `json:"order_id"`
}

// Kind names the task.
func (SendReceipt) Kind() string { return "email.send_receipt" }
