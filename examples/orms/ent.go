package orms

import (
	"context"
	"errors"

	"entgo.io/ent/dialect"
	entsql "entgo.io/ent/dialect/sql"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"

	"github.com/code19m/taskmill"
	"github.com/code19m/taskmill/examples/orms/ent"
)

// openEnt opens an ent client on taskmill's pool.
func openEnt(pool *pgxpool.Pool) *ent.Client {
	return ent.NewClient(ent.Driver(entsql.OpenDB(dialect.Postgres, stdlib.OpenDBFromPool(pool))))
}

// placeOrderEnt saves an order and its receipt task in one ent transaction.
func placeOrderEnt(ctx context.Context, db *ent.Client, client *taskmill.Client, email string) error {
	tx, err := db.Tx(ctx)
	if err != nil {
		return err
	}
	order, err := tx.Order.Create().SetEmail(email).Save(ctx)
	if err != nil {
		return errors.Join(err, tx.Rollback())
	}
	// The sql/execquery feature gives *ent.Tx the methods that taskmill.SQL needs.
	_, err = client.EnqueueTx(ctx, taskmill.SQL(tx), SendReceipt{OrderID: int64(order.ID)}, nil)
	if err != nil {
		return errors.Join(err, tx.Rollback())
	}
	return tx.Commit()
}
