package orms

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"

	"github.com/code19m/taskmill"
)

// Order is an order, as GORM maps it to the orders table.
type Order struct {
	ID    int64
	Email string
}

// openGORM opens GORM on taskmill's pool.
func openGORM(pool *pgxpool.Pool) (*gorm.DB, error) {
	return gorm.Open(postgres.New(postgres.Config{Conn: stdlib.OpenDBFromPool(pool)}), &gorm.Config{})
}

// placeOrderGORM saves an order and its receipt task in one GORM transaction.
func placeOrderGORM(ctx context.Context, db *gorm.DB, client *taskmill.Client, email string) error {
	return db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		order := Order{Email: email}
		if err := tx.Create(&order).Error; err != nil {
			return err
		}
		// Inside Transaction, tx.Statement.ConnPool is the transaction's *sql.Tx.
		_, err := client.EnqueueTx(ctx, taskmill.SQL(tx.Statement.ConnPool), SendReceipt{OrderID: order.ID}, nil)
		return err
	})
}
