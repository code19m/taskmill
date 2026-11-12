# Part 7: Ready to ship

**Goal:** one binary with two roles, as you run it in production: `shop web`
only enqueues, and `shop worker` runs the tasks. **Time:** about 15
minutes. **You start from:** the program of [part 6](6-test-it.md).

## Step 1. Share the setup

Steps 1 to 3 move code out of `run`, and step 4 runs the result. Both roles
connect, create the tables, and serve HTTP. Add these functions below `run`:

```go
// connect opens a pool of conns connections to the database.
func connect(ctx context.Context, conns int32) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(cmp.Or(os.Getenv("DATABASE_URL"), defaultDatabaseURL))
	if err != nil {
		return nil, err
	}
	cfg.MaxConns = conns
	return pgxpool.NewWithConfig(ctx, cfg)
}

// setUp creates taskmill's tables and the shop's, and returns the client.
// Every role calls it when it starts.
func setUp(ctx context.Context, pool *pgxpool.Pool, logger *slog.Logger) (*taskmill.Client, error) {
	// Create taskmill's tables, or update them. It is safe at every start.
	if err := taskmill.Migrate(ctx, pool, &taskmill.MigrateConfig{Logger: logger}); err != nil {
		return nil, err
	}
	if err := createTables(ctx, pool); err != nil {
		return nil, err
	}
	// The client adds and manages tasks. Without a logger, taskmill logs nothing.
	return taskmill.NewClient(ctx, pool, &taskmill.ClientConfig{Logger: logger})
}

// serve starts an HTTP server on addr. The function that it returns stops
// the server: it waits up to 10 seconds for open requests to finish.
func serve(ctx context.Context, addr string, handler http.Handler) (stop func(), err error) {
	ln, err := new(net.ListenConfig).Listen(ctx, "tcp", addr)
	if err != nil {
		return nil, err
	}
	server := &http.Server{Handler: handler, ReadHeaderTimeout: 10 * time.Second}
	go func() { _ = server.Serve(ln) }() // it returns when Shutdown is called
	return func() {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cancel()
		_ = server.Shutdown(ctx)
	}, nil
}
```

## Step 2. Write the web role

The web role takes orders and shows the dashboard. It has no worker, so it
runs no tasks. Add it below `serve`:

```go
// runWeb takes orders and shows the dashboard. It runs no tasks: it only
// enqueues them.
func runWeb(ctx context.Context, logger *slog.Logger) error {
	pool, err := connect(ctx, 10) // for the web traffic: this role runs no tasks
	if err != nil {
		return err
	}
	defer pool.Close()
	client, err := setUp(ctx, pool, logger)
	if err != nil {
		return err
	}
	handler, err := routes(pool, client, logger)
	if err != nil {
		return err
	}

	// Serve until Ctrl-C or SIGTERM. Set ADDR if port 8080 is taken.
	addr := cmp.Or(os.Getenv("ADDR"), defaultAddr)
	stop, err := serve(ctx, addr, handler)
	if err != nil {
		return err
	}
	logger.LogAttrs(ctx, slog.LevelInfo, "open the dashboard", slog.String(logKeyURL, "http://"+addr+"/taskmill/"))
	<-ctx.Done()
	stop()
	return nil
}

// routes returns the web role's HTTP handler: the orders and the dashboard.
func routes(pool *pgxpool.Pool, client *taskmill.Client, logger *slog.Logger) (http.Handler, error) {
	// The outbox stores messages for your broker in your own transaction.
	ob := outbox.New(client)

	// The shop's own server takes the orders.
	mux := http.NewServeMux()
	mux.Handle("POST /orders", placeOrder(pool, client, ob, logger))

	// The dashboard is an http.Handler. In a real app, put your login in front of it.
	dashboard, err := ui.New(client, &ui.Config{Prefix: "/taskmill"})
	if err != nil {
		return nil, err
	}
	mux.Handle("/taskmill/", dashboard)
	return mux, nil
}
```

## Step 3. Write the worker role

The worker role runs the tasks, and answers a readiness probe with
`Worker.Health`. Its pool has a connection for each of its 12 running tasks,
plus 5. Without part 5, it runs 10 tasks: pass 15 to `connect`. Add the
role below `routes`:

```go
// runWorker runs the tasks, the schedule, and the outbox's relay.
func runWorker(ctx context.Context, logger *slog.Logger) error {
	pool, err := connect(ctx, 17) // 12 for the tasks that run at the same time, 5 for the worker
	if err != nil {
		return err
	}
	defer pool.Close()
	client, err := setUp(ctx, pool, logger)
	if err != nil {
		return err
	}
	worker, err := newWorker(client, logger)
	if err != nil {
		return err
	}

	// A platform such as Kubernetes asks this server if the worker is ready.
	// Set HEALTH_ADDR if port 8081 is taken.
	stop, err := serve(ctx, cmp.Or(os.Getenv("HEALTH_ADDR"), "localhost:8081"), health(worker))
	if err != nil {
		return err
	}
	defer stop()

	// Run the worker until Ctrl-C or SIGTERM. Then it lets running tasks finish.
	return worker.Run(ctx)
}

// newWorker returns the shop's worker, with its handlers, queues, and
// schedule.
func newWorker(client *taskmill.Client, logger *slog.Logger) (*taskmill.Worker, error) {
	// The worker takes tasks from the database and runs their handlers.
	var handlers taskmill.Handlers
	taskmill.AddHandler(&handlers, sendReceipt(sendMail, logger), nil)
	taskmill.AddHandler(&handlers, remindCustomer(logger), nil)
	taskmill.AddHandler(&handlers, buildReport(logger), nil)
	ob := outbox.New(client)
	ob.AddRelay(&handlers, publish(logger), nil)
	return taskmill.NewWorker(client, &taskmill.WorkerConfig{
		Handlers: &handlers,
		Queues: map[string]taskmill.QueueOptions{
			"default":    {Concurrency: 5},
			"emails":     {Concurrency: 5},
			outbox.Queue: {Concurrency: 2},
		},
		Schedules: []taskmill.Schedule{
			{Name: "sales-report", Cron: "* * * * *", Args: BuildReport{}},
		},
	})
}

// health answers a readiness probe: 200 while the worker runs and reaches
// the database, and 503 otherwise.
func health(worker *taskmill.Worker) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if err := worker.Health(); err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
		}
	})
}
```

## Step 4. Choose the role when the program starts

Replace the old `run`, with its comment, by this one:

```go
// run runs the role that the first argument names, until Ctrl-C or SIGTERM.
func run(logger *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	role := ""
	if len(os.Args) == 2 {
		role = os.Args[1]
	}
	switch role {
	case "web":
		return runWeb(ctx, logger)
	case "worker":
		return runWorker(ctx, logger)
	default:
		return errors.New("usage: shop web|worker")
	}
}
```

Start the program without a role, with `go run .`. **You should see** the
error `usage: shop web|worker`.

## Step 5. Run the two roles

Start the web role with `go run . web`, and place an order:

```sh
curl -s -d email=user7@example.com localhost:8080/orders
```

**You should see** the order's answer, but no receipt in the log. Open
**Tasks** in the dashboard: the receipt waits as `available`, because no
worker runs.

In a second terminal, start the worker role with `go run . worker`. **You
should see** `worker started`, and then `receipt sent` for the order that
waited. Ask the worker if it is ready, with
`curl -i localhost:8081/healthz`: it answers `HTTP/1.1 200 OK`.

## Step 6. Stop the worker while a task runs

In the dashboard, open **Schedules**, choose **Run now**, and confirm.
Within 5 seconds, press Ctrl-C in the worker's terminal. **You should see**
the worker wait for the report, and then stop:

```text
time=… level=INFO msg="worker stopping; waiting for running tasks" worker_id=… tasks=1 timeout=20s
time=… level=INFO msg="sales report built" scheduled_for=…
time=… level=INFO msg="worker stopped" worker_id=… released=0
```

## What you learned

| In the code | taskmill calls it |
|---|---|
| `runWeb`: a client, and no worker | a process that only [enqueues](../concepts/glossary.md#enqueue), such as a web server |
| `connect(ctx, 17)` | a pool for the worker: its total [concurrency](../concepts/glossary.md#concurrency), plus 5 |
| `worker.Health()` | the answer for a readiness probe |
| `worker.Run(ctx)` after Ctrl-C or `SIGTERM` | a worker that [drains](../concepts/glossary.md#drain): running tasks get 20 seconds to finish |

## Go deeper

- [Deploys](../production/deploys.md): stop workers without losing work, and run two versions side by side
- [Kubernetes](../production/kubernetes.md): manifests for the two roles, and the probe
- [Production checklist](../production/checklist.md): every point to check before you go live

**Next:** [Concepts](../concepts/README.md)
