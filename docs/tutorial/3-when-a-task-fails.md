# Part 3: When a task fails

**Goal:** see taskmill retry a failed task, keep a task that cannot
succeed, and show it all in the dashboard. **Time:** about 10 minutes.
**You start from:** the program of [part 2](2-tasks-with-your-data.md).

## Step 1. Let the mail server fail sometimes

A handler that returns an error asks taskmill to try again later. In
`sendReceipt`, add these lines above the line that writes the log:

```go
		if rand.IntN(3) == 0 {
			// Pretend that the mail server failed: taskmill tries again soon.
			return errors.New("mail server did not answer")
		}
```

Add `"errors"` and `"math/rand/v2"` to the imports. Start the shop, and
place 10 orders:

```sh
for i in $(seq 1 10); do curl -s -d "email=user$i@example.com" localhost:8080/orders; done
```

**You should see** that about one [attempt](../concepts/glossary.md#attempt)
in three fails, and that the same task succeeds a moment later:

```text
time=… level=DEBUG msg="attempt failed; retrying" worker_id=… task_id=13 kind=email.send_receipt attempt=1 error="mail server did not answer" duration=17.209µs retry_in=909.57533ms
time=… level=INFO msg="receipt sent" task_id=13 to=user7@example.com
```

taskmill logs each failure at Debug level, which `main` turns on. Then it
waits before the next attempt: about 1 second, then 2, then 4. This wait is
the [backoff](../concepts/glossary.md#backoff).

## Step 2. Give up when a retry cannot help

Some failures never go away: an order without an email address gets no
receipt, however often you try. Add these lines as the first lines of the
handler's function:

```go
		if t.Args.Email == "" {
			// A retry cannot help, so give up now: the task becomes dead.
			return taskmill.Permanent(errors.New("the order has no email address"))
		}
```

Start the shop again, and place an order without an email address:

```sh
curl -s -d email= localhost:8080/orders
```

**You should see** one failed attempt, logged as an error, and no retry:

```text
time=… level=ERROR msg="attempt failed; the task is dead" worker_id=… task_id=17 kind=email.send_receipt attempt=1 error="permanent: the order has no email address" duration=79.583µs
```

A [dead task](../concepts/glossary.md#dead-task) stays in the database, but
does not run again until a person retries it.

## Step 3. Add the dashboard

The [dashboard](../concepts/glossary.md#dashboard) is an `http.Handler`.
Add `"github.com/code19m/taskmill/ui"` to the imports. In `run`, add these
lines below the route for the orders:

```go
	// The dashboard is an http.Handler. In a real app, put your login in front of it.
	dashboard, err := ui.New(client, &ui.Config{Prefix: "/taskmill"})
	if err != nil {
		return err
	}
	mux.Handle("/taskmill/", dashboard)
```

Below the `defer` line of the server, log where the dashboard is:

```go
	logger.LogAttrs(ctx, slog.LevelInfo, "open the dashboard", slog.String(logKeyURL, "http://"+addr+"/taskmill/"))
```

Start the shop again. **You should see** the address in the log:

```text
time=… level=INFO msg="open the dashboard" url=http://localhost:8080/taskmill/
```

Open <http://localhost:8080/taskmill/> in a browser. Within about 10
seconds, the menu shows **Dead 1**: the task of step 2.

**Overview** shows the queue `default` and two charts. **Tasks** lists every
task with its state and its last error. **Workers** shows your worker,
marked as the maintainer.

## Step 4. Retry the dead task

Choose **Dead** in the menu, and open the task. **You should see** its
payload, with the empty `email`, and its error:
`permanent: the order has no email address`.

Choose **Retry**, and confirm. The task runs again with a new set of
attempts. It is dead again a moment later, because the order still has no
address: **Errors** now lists two attempts. In a real shop, you fix the
cause first, and then retry.

Choose **Delete**, and confirm. The task is gone, and the dashboard shows
the list of tasks.

> [!WARNING]
> A task can also run twice: for example, when a worker crashes after the
> work is done, but before it saves the result. So every handler must be
> safe to run twice. [Guarantees](../concepts/guarantees.md) shows how.

Your `main.go` is now the program of the quickstart. Compare it with
[examples/quickstart/main.go](../../examples/quickstart/main.go) if your
program does not work.

## What you learned

| In the code | taskmill calls it |
|---|---|
| `return errors.New(…)` in a handler | a failed [attempt](../concepts/glossary.md#attempt), retried after a [backoff](../concepts/glossary.md#backoff) |
| `return taskmill.Permanent(…)` | a [permanent error](../concepts/glossary.md#permanent-error), which makes a [dead task](../concepts/glossary.md#dead-task) |
| `ui.New(client, …)` | the [dashboard](../concepts/glossary.md#dashboard), mounted in your own server |
| **Retry** in the dashboard | a [retry](../concepts/glossary.md#retry): the task runs again, with all its attempts |

## Go deeper

- [Retries and errors](../guides/retries-and-errors.md): how long taskmill waits, and how to change it
- [The dashboard](../guides/dashboard.md): protect it with a login, or run it on its own
- [How it works](../concepts/how-it-works.md): what happens when a worker crashes
- [Guarantees](../concepts/guarantees.md): what taskmill promises, and why a handler can run twice

**Next:** [Part 4: Queues, delays, and schedules](4-queues-delays-schedules.md)
