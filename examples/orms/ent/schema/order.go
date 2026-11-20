// Package schema holds the ent schema of the example's orders.
package schema

import (
	"entgo.io/ent"
	"entgo.io/ent/schema/field"
)

// Order is a shop's order.
type Order struct {
	ent.Schema
}

// Fields of Order.
func (Order) Fields() []ent.Field {
	return []ent.Field{
		field.String("email"),
	}
}
