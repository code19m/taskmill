// Command shop is a small online shop that shows taskmill at work, with its
// dashboard. Orders enqueue their receipts and ledger entries in the order's
// own transaction, ledger entries post in order per account, images resize
// in batches, a partner API is called under a limit across all workers, a
// report runs every minute, and an outbox publishes order events. Some orders
// are bad on purpose, so the dashboard has something to show.
//
//	make db
//	go run ./examples/shop
//
// The dashboard is at http://localhost:8090/taskmill/. The shop reads the
// database from SHOP_DATABASE_URL, by default the one that `make db` starts.
// It keeps its tables and taskmill's in the schema taskmill_example_shop, and
// touches no other schema. Ctrl-C stops it.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/code19m/taskmill"
	"github.com/code19m/taskmill/outbox"
	"github.com/code19m/taskmill/ui"
)

const (
	// defaultDatabaseURL is the database that `make db` starts.
	defaultDatabaseURL = "postgres://postgres:postgres@localhost:54329/taskmill?sslmode=disable" //nolint:gosec // the local test database of `make db`
	// schema holds taskmill's tables and the shop's own.
	schema = "taskmill_example_shop"
	addr   = "localhost:8090"
)

// Log attribute keys.
const (
	logKeyError        = "error"
	logKeyKind         = "kind"
	logKeyScheduledFor = "scheduled_for"
	logKeyTook         = "took"
	logKeyURL          = "url"
)

// A task is a struct with a kind. Its fields are the task's JSON payload.

// SendReceipt emails an order's receipt.
type SendReceipt struct {
	OrderID int64  `json:"order_id"`
	Email   string `json:"email"`
}

func (SendReceipt) Kind() string { return "email.send_receipt" }

// EnqueueDefaults puts receipts in a queue of their own, and gives up on one
// after five attempts.
func (SendReceipt) EnqueueDefaults() taskmill.EnqueueOptions {
	return taskmill.EnqueueOptions{Queue: "emails", MaxAttempts: 5}
}

// PostLedgerEntry books an order's amount on its account.
type PostLedgerEntry struct {
	AccountID int64 `json:"account_id"`
	OrderID   int64 `json:"order_id"`
	Cents     int64 `json:"cents"`
}

func (PostLedgerEntry) Kind() string { return "ledger.post_entry" }

// EnqueueDefaults puts ledger entries in a queue of their own.
func (PostLedgerEntry) EnqueueDefaults() taskmill.EnqueueOptions {
	return taskmill.EnqueueOptions{Queue: "ledger"}
}

// ResizeImage makes the thumbnails of a product image.
type ResizeImage struct {
	ImageID int64 `json:"image_id"`
}

func (ResizeImage) Kind() string { return "images.resize" }

// EnqueueDefaults puts images in a queue of their own.
func (ResizeImage) EnqueueDefaults() taskmill.EnqueueOptions {
	return taskmill.EnqueueOptions{Queue: "images"}
}

// SyncWithPartner sends an order to a partner's slow API.
type SyncWithPartner struct {
	OrderID int64 `json:"order_id"`
}

func (SyncWithPartner) Kind() string { return "partner.sync_order" }

// EnqueueDefaults puts partner calls in a queue of their own, which the shop
// limits across all workers.
func (SyncWithPartner) EnqueueDefaults() taskmill.EnqueueOptions {
	return taskmill.EnqueueOptions{Queue: "partner-api"}
}

// BuildReport builds the sales report. A schedule enqueues it every minute.
type BuildReport struct{}

func (BuildReport) Kind() string { return "reports.build" }

// RemindCustomer asks a customer, two minutes after the order, how it went.
type RemindCustomer struct {
	OrderID int64 `json:"order_id"`
}

func (RemindCustomer) Kind() string { return "email.remind_customer" }

// LegacyInvoice is a kind that no worker handles, as during a rolling deploy
// in which only the new version knows it.
type LegacyInvoice struct {
	OrderID int64 `json:"order_id"`
}

func (LegacyInvoice) Kind() string { return "legacy.invoice" }

// orderEvent is an event about an order, as the outbox publishes it.
type orderEvent struct {
	Type    string `json:"type"`
	OrderID int64  `json:"order_id"`
	Cents   int64  `json:"cents,omitempty"`
}

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	if err := run(logger); err != nil {
		logger.LogAttrs(context.Background(), slog.LevelError, "the shop stopped", slog.Any(logKeyError, err))
		os.Exit(1)
	}
}

// run connects to the database and runs the shop until Ctrl-C.
func run(logger *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	url := os.Getenv("SHOP_DATABASE_URL")
	if url == "" {
		url = defaultDatabaseURL
	}
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return err
	}
	// The workers and the ledger handler, which writes through it, share
	// this pool. So that handlers cannot starve the workers' heartbeats, it
	// has a connection for every task that may run at once, plus 5 per
	// worker: see Connections in docs/guide.md.
	cfg.MaxConns = 6*10 + 4 + 2 + 2*5 // the main worker's 6 queues of 10, the other's 4 and 2
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return err
	}
	defer pool.Close()

	ln, err := new(net.ListenConfig).Listen(ctx, "tcp", addr)
	if err != nil {
		return err
	}
	return runShop(ctx, pool, schema, ln, logger)
}

// runShop runs the shop in schema, with its dashboard on ln, until ctx ends.
// It closes ln.
func runShop(ctx context.Context, pool *pgxpool.Pool, schema string, ln net.Listener, logger *slog.Logger) error {
	defer func() { _ = ln.Close() }() // the server closes it too, once it serves

	s, err := openShop(ctx, pool, schema, logger)
	if err != nil {
		return err
	}
	// A schedule enqueues a task on a cron expression, once per run across
	// all workers. Every worker that may become the maintainer defines it.
	schedules := []taskmill.Schedule{{Name: "sales-report", Cron: "* * * * *", Args: BuildReport{}}}

	// The main worker runs every kind, and serves each kind's default queue.
	primary, err := taskmill.NewWorker(s.client, &taskmill.WorkerConfig{
		Handlers:   s.handlers(),
		Schedules:  schedules,
		Middleware: []taskmill.Middleware{logSlow(logger)},
	})
	if err != nil {
		return err
	}
	// A second worker serves only receipts and images, with its own
	// concurrency per queue, as another process would.
	var some taskmill.Handlers
	addReceiptsAndImages(&some)
	helper, err := taskmill.NewWorker(s.client, &taskmill.WorkerConfig{
		Handlers:  &some,
		Queues:    map[string]taskmill.QueueOptions{"emails": {Concurrency: 4}, "images": {Concurrency: 2}},
		Schedules: schedules,
	})
	if err != nil {
		return err
	}

	// The dashboard, mounted under a path of the shop's own server. It can
	// retry, delete, and pause, so only operators may reach it.
	dashboard, err := ui.New(s.client, &ui.Config{Prefix: "/taskmill", Logger: logger})
	if err != nil {
		return err
	}
	mux := http.NewServeMux()
	mux.Handle("/taskmill/", requireAdmin(dashboard))
	mux.Handle("GET /{$}", http.RedirectHandler("/taskmill/", http.StatusFound))
	server := &http.Server{Handler: mux, ReadHeaderTimeout: 10 * time.Second}

	// Run everything until ctx ends or a part fails, then stop all of it.
	ctx, fail := context.WithCancelCause(ctx)
	defer fail(nil)
	var wg sync.WaitGroup
	wg.Go(func() { fail(primary.Run(ctx)) }) // Run returns nil after draining, when ctx ends
	wg.Go(func() { fail(helper.Run(ctx)) })
	wg.Go(func() {
		if err := server.Serve(ln); !errors.Is(err, http.ErrServerClosed) {
			fail(err)
		}
	})
	wg.Go(func() { s.placeOrders(ctx) })
	logger.LogAttrs(ctx, slog.LevelInfo, "the shop is open; press Ctrl-C to stop it",
		slog.String(logKeyURL, "http://"+ln.Addr().String()+"/taskmill/"))

	<-ctx.Done()
	shutdown, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	err = server.Shutdown(shutdown)
	wg.Wait()
	if cause := context.Cause(ctx); !errors.Is(cause, context.Canceled) {
		return cause
	}
	return err
}

// requireAdmin stands for the shop's own authentication. In production, it
// must let only operators through to the dashboard.
func requireAdmin(next http.Handler) http.Handler { return next } // your authentication

// shop is the application: its tables, and the taskmill client and outbox it
// enqueues with.
type shop struct {
	pool   *pgxpool.Pool
	client *taskmill.Client
	outbox *outbox.Outbox
	schema string // taskmill's installation, which numbers its tasks
	orders string // the quoted names of the shop's tables
	ledger string
	logger *slog.Logger
}

// openShop creates the schema, with taskmill's tables and the shop's, unless
// they exist. A restart keeps the shop's data.
func openShop(ctx context.Context, pool *pgxpool.Pool, schema string, logger *slog.Logger) (*shop, error) {
	// Migrate creates the schema and taskmill's tables, or upgrades them.
	// It is safe to call from every process at start.
	if err := taskmill.Migrate(ctx, pool, &taskmill.MigrateConfig{Schema: schema, Logger: logger}); err != nil {
		return nil, err
	}
	s := &shop{
		pool:   pool,
		schema: schema,
		orders: pgx.Identifier{schema, "orders"}.Sanitize(),
		ledger: pgx.Identifier{schema, "ledger_entries"}.Sanitize(),
		logger: logger,
	}
	if _, err := pool.Exec(ctx, "CREATE TABLE IF NOT EXISTS "+s.orders+` (
			id         bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
			account_id bigint NOT NULL,
			cents      bigint NOT NULL,
			created_at timestamptz NOT NULL DEFAULT now());
		CREATE TABLE IF NOT EXISTS `+s.ledger+` (
			order_id   bigint PRIMARY KEY, -- an order posts once
			account_id bigint NOT NULL,
			cents      bigint NOT NULL,
			posted_at  timestamptz NOT NULL DEFAULT now())`); err != nil {
		return nil, err
	}
	client, err := taskmill.NewClient(ctx, pool, &taskmill.ClientConfig{Schema: schema, Logger: logger})
	if err != nil {
		return nil, err
	}
	// A limit across all workers: at most two partner calls run at once.
	if err := client.UpdateQueue(ctx, "partner-api", func(q *taskmill.QueueSettings) { q.MaxRunning = 2 }); err != nil {
		return nil, err
	}
	s.client, s.outbox = client, outbox.New(client)
	return s, nil
}

// placeOrders places an order every second until ctx ends, as the shop's
// customers would.
func (s *shop) placeOrders(ctx context.Context) {
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	for n := int64(1); ; n++ {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
		if err := s.placeOrder(ctx, n); err != nil && ctx.Err() == nil {
			s.logger.LogAttrs(ctx, slog.LevelError, "the order failed", slog.Any(logKeyError, err))
		}
	}
}

// placeOrder stores the nth order since the start with everything it
// triggers, in one transaction: its tasks and messages exist if and only if
// the order does. Some orders are bad on purpose.
func (s *shop) placeOrder(ctx context.Context, n int64) error {
	account := 1 + n%4
	cents := 1000 + rand.Int64N(9000) //nolint:gosec // a made-up amount needs no secure randomness
	if n == 13 {
		cents = -500 // a bad amount, which halts account 2's ledger and the order's outbox key
	}
	email := "customer" + strconv.FormatInt(n, 10) + "@example.com"
	if n%17 == 0 {
		email = "" // a receipt that can never be sent
	}

	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		var id int64
		if err := tx.QueryRow(ctx, "INSERT INTO "+s.orders+" (account_id, cents) VALUES ($1, $2) RETURNING id", account, cents).Scan(&id); err != nil {
			return err
		}
		key := "order-" + strconv.FormatInt(id, 10)
		// A dedupe key: one receipt per order, even if enqueued twice.
		receipt := &taskmill.EnqueueOptions{DedupeKey: "receipt-" + key}
		if n%10 == 0 {
			receipt.Priority = taskmill.PriorityCritical // a VIP: the receipt runs before every other waiting one
		}
		params := []taskmill.EnqueueParams{
			{Args: SendReceipt{OrderID: id, Email: email}, Options: receipt},
			// A FIFO group per account: its entries post one at a time, in order.
			{Args: PostLedgerEntry{AccountID: account, OrderID: id, Cents: cents}, Options: &taskmill.EnqueueOptions{
				GroupKey: "account-" + strconv.FormatInt(account, 10),
			}},
			// A delayed task.
			{Args: RemindCustomer{OrderID: id}, Options: &taskmill.EnqueueOptions{RunAt: time.Now().Add(2 * time.Minute)}},
		}
		if n == 1 {
			// No worker handles this kind: the task waits, without using
			// attempts, and is dead after an hour.
			params = append(params, taskmill.EnqueueParams{Args: LegacyInvoice{OrderID: id}})
		}
		if n%3 == 0 {
			params = append(params, taskmill.EnqueueParams{Args: SyncWithPartner{OrderID: id}})
		}
		if n%5 == 0 {
			for i := range int64(30) {
				params = append(params, taskmill.EnqueueParams{Args: ResizeImage{ImageID: id*100 + i}})
			}
		}
		// taskmill.Pgx enqueues in the shop's own transaction.
		if _, err := s.client.EnqueueManyTx(ctx, taskmill.Pgx(tx), params); err != nil {
			return err
		}
		// The outbox stores events in the same transaction. A relay
		// publishes them later, in order per key.
		placed, _ := json.Marshal(orderEvent{Type: "order.placed", OrderID: id, Cents: cents}) // a struct of numbers and strings always encodes
		paid, _ := json.Marshal(orderEvent{Type: "order.paid", OrderID: id})
		return s.outbox.Store(ctx, taskmill.Pgx(tx),
			outbox.Message{Topic: "orders", Key: []byte(key), Value: placed},
			outbox.Message{Topic: "orders", Key: []byte(key), Value: paid},
		)
	})
}

// handlers returns a handler for every kind except LegacyInvoice, and the
// outbox's relay.
func (s *shop) handlers() *taskmill.Handlers {
	var hs taskmill.Handlers
	addReceiptsAndImages(&hs)
	taskmill.AddHandler(&hs, taskmill.HandlerFunc[PostLedgerEntry](s.postLedgerEntry), nil)
	taskmill.AddHandler(&hs, taskmill.HandlerFunc[SyncWithPartner](s.syncWithPartner), &taskmill.HandlerOptions{Timeout: 10 * time.Second})
	taskmill.AddHandler(&hs, taskmill.HandlerFunc[BuildReport](
		func(ctx context.Context, t *taskmill.Task[BuildReport]) error {
			s.logger.LogAttrs(ctx, slog.LevelInfo, "the sales report is built", slog.Time(logKeyScheduledFor, t.RunAt))
			return nil
		},
	), nil)
	taskmill.AddHandler(&hs, taskmill.HandlerFunc[RemindCustomer](
		func(ctx context.Context, _ *taskmill.Task[RemindCustomer]) error {
			return work(ctx, 50*time.Millisecond)
		},
	), nil)
	// The relay publishes up to 100 messages per call by default. A broker
	// that does not answer within 10 seconds is tried again later.
	s.outbox.AddRelay(&hs, outbox.PublisherFunc(publish), &outbox.RelayOptions{Timeout: 10 * time.Second})
	return &hs
}

// addReceiptsAndImages adds the handlers that both workers have.
func addReceiptsAndImages(hs *taskmill.Handlers) {
	taskmill.AddHandler(hs, taskmill.HandlerFunc[SendReceipt](sendReceipt), nil)
	// A batch handler: up to 25 images in one call.
	taskmill.AddBatchHandler[ResizeImage](hs, resizeImages{}, &taskmill.HandlerOptions{BatchSize: 25})
}

func sendReceipt(ctx context.Context, t *taskmill.Task[SendReceipt]) error {
	if t.Args.Email == "" {
		// Retrying cannot help, so the task is dead at once.
		return taskmill.Permanent(errors.New("the order has no email address"))
	}
	if err := work(ctx, randomDuration(50*time.Millisecond, 300*time.Millisecond)); err != nil {
		return err
	}
	if oneIn(5) {
		return errors.New("smtp: connection reset by peer") // retried with backoff
	}
	return nil
}

// postLedgerEntry books the entry and completes its task in one transaction.
// A task can run more than once, after a crash or a lost lease, but the entry
// is booked once.
func (s *shop) postLedgerEntry(ctx context.Context, t *taskmill.Task[PostLedgerEntry]) error {
	if t.Args.Cents < 0 {
		// The dead entry halts its account's group: later entries wait
		// instead of posting out of order.
		return taskmill.Permanent(fmt.Errorf("order %d has a negative amount, %d cents", t.Args.OrderID, t.Args.Cents))
	}
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, "INSERT INTO "+s.ledger+" (order_id, account_id, cents) VALUES ($1, $2, $3)",
			t.Args.OrderID, t.Args.AccountID, t.Args.Cents); err != nil {
			return err
		}
		// Fails with taskmill.ErrLeaseLost if another attempt took the task
		// over; the entry then rolls back.
		return t.CompleteTx(ctx, taskmill.Pgx(tx))
	})
}

func (s *shop) syncWithPartner(ctx context.Context, t *taskmill.Task[SyncWithPartner]) error {
	if oneIn(8) {
		// The partner said "slow down": run again later, without using an
		// attempt.
		return taskmill.Snooze(5 * time.Second)
	}
	// The task's ID stays the same on every attempt, so as the partner's
	// idempotency key it makes the partner apply a repeated call once. It is
	// unique only within taskmill's schema, and the partner has other
	// clients, so the key also names the service and the schema.
	key := "shop/" + s.schema + "/" + strconv.FormatInt(t.ID, 10)
	return partnerRequest{IdempotencyKey: key, OrderID: t.Args.OrderID}.send(ctx)
}

// partnerRequest is a call to the partner's API.
type partnerRequest struct {
	IdempotencyKey string
	OrderID        int64
}

// send stands for the partner's API, which applies each idempotency key once.
func (partnerRequest) send(ctx context.Context) error {
	return work(ctx, randomDuration(800*time.Millisecond, 2*time.Second))
}

// resizeImages is the batch handler of images.
type resizeImages struct{}

// HandleBatch resizes a batch of images in one call to the image service.
func (resizeImages) HandleBatch(ctx context.Context, tasks []*taskmill.Task[ResizeImage]) []error {
	err := work(ctx, 300*time.Millisecond)
	errs := make([]error, len(tasks)) // one result per task
	for i := range errs {
		errs[i] = err
	}
	return errs
}

// publish stands for a broker client, such as Kafka's. The broker rejects an
// order.placed event with a negative amount, so that order's key halts in the
// outbox: its order.paid event waits behind the dead one.
func publish(_ context.Context, ds []outbox.Delivery) []error {
	errs := make([]error, len(ds))
	for i, d := range ds {
		var ev orderEvent
		switch err := json.Unmarshal(d.Value, &ev); {
		case err != nil:
			errs[i] = taskmill.Permanent(err)
		case ev.Cents < 0:
			errs[i] = taskmill.Permanent(errors.New("the broker rejects the message: a negative amount"))
		}
	}
	return errs
}

// logSlow is middleware, the place for metrics and tracing: it logs attempts
// that take over 1.5 seconds.
func logSlow(logger *slog.Logger) taskmill.Middleware {
	return func(ctx context.Context, task *taskmill.TaskInfo, next func(context.Context) error) error {
		start := time.Now()
		err := next(ctx)
		if took := time.Since(start); took > 1500*time.Millisecond {
			logger.LogAttrs(ctx, slog.LevelInfo, "a slow attempt", slog.String(logKeyKind, task.Kind), slog.Duration(logKeyTook, took))
		}
		return err
	}
}

// work stands for a handler's real work, such as a call to another service.
// It takes d, or stops early when the attempt's context ends: at a timeout,
// or when the worker shuts down.
func work(ctx context.Context, d time.Duration) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(d):
		return nil
	}
}

// oneIn reports true about once in n calls, for made-up failures.
func oneIn(n int) bool {
	return rand.IntN(n) == 0 //nolint:gosec // made-up failures need no secure randomness
}

// randomDuration returns a duration from lo up to hi, for made-up latency.
func randomDuration(lo, hi time.Duration) time.Duration {
	return lo + rand.N(hi-lo) //nolint:gosec // made-up latency needs no secure randomness
}
