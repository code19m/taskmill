// Command migrate applies the current migrations to one schema of the
// database in TASKMILL_TEST_DATABASE_URL:
//
//	go run ./internal/cmd/migrate <schema>
//
// The rolling-deploy check in internal/compat runs it, then runs the latest
// release on that schema.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/code19m/taskmill"
	"github.com/code19m/taskmill/internal/testdb"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "migrate:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) != 1 {
		return errors.New("usage: go run ./internal/cmd/migrate <schema>")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	pool, err := pgxpool.New(ctx, os.Getenv(testdb.EnvURL))
	if err != nil {
		return err
	}
	defer pool.Close()
	return taskmill.Migrate(ctx, pool, &taskmill.MigrateConfig{Schema: args[0]})
}
