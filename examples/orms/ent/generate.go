// Package ent is the ent client of the example's orders, generated from
// ./schema. The sql/execquery feature gives its transactions the
// ExecContext and QueryContext methods that taskmill.SQL takes.
package ent

//go:generate go run -mod=mod entgo.io/ent/cmd/ent generate --feature sql/execquery ./schema
