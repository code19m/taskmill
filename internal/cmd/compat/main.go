// Command compat is the current code's half of the rolling-deploy check in
// internal/compat. It migrates a schema of the database in
// TASKMILL_TEST_DATABASE_URL, and enqueues tasks there for the latest
// release's worker to run:
//
//	go run ./internal/cmd/compat <schema>
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

// job is the task that internal/compat's handler runs.
type job struct {
	What string `json:"what"`
	Seq  int    `json:"seq"`
}

func (job) Kind() string { return "compat.job" }

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "compat:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) != 1 {
		return errors.New("usage: go run ./internal/cmd/compat <schema>")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	pool, err := pgxpool.New(ctx, os.Getenv(testdb.EnvURL))
	if err != nil {
		return err
	}
	defer pool.Close()
	if err := taskmill.Migrate(ctx, pool, &taskmill.MigrateConfig{Schema: args[0]}); err != nil {
		return err
	}
	client, err := taskmill.NewClient(ctx, pool, &taskmill.ClientConfig{Schema: args[0]})
	if err != nil {
		return err
	}
	for seq := 1; seq <= 3; seq++ {
		if _, err := client.Enqueue(ctx, job{What: "current: fifo", Seq: seq}, &taskmill.EnqueueOptions{GroupKey: "current"}); err != nil {
			return err
		}
	}
	if _, err := client.Enqueue(ctx, job{What: "current: delayed"}, &taskmill.EnqueueOptions{RunAt: time.Now().Add(time.Second)}); err != nil {
		return err
	}
	dedupe := &taskmill.EnqueueOptions{DedupeKey: "compat"}
	if _, err := client.Enqueue(ctx, job{What: "current: deduplicated"}, dedupe); err != nil {
		return err
	}
	res, err := client.Enqueue(ctx, job{What: "current: deduplicated"}, dedupe)
	if err != nil {
		return err
	}
	if !res.Duplicate {
		return errors.New("the second deduplicated task is not a duplicate")
	}
	return nil
}
