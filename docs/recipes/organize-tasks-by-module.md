# Organize tasks by module

**Situation.** "Our app is one binary with modules, such as orders and
billing, and each team owns a module. Where do a module's task types,
handlers, and schedules live? And how does orders start billing's work?"

## The recommended way

Use one taskmill [schema](../concepts/glossary.md#schema) for the binary,
and let each module own four things: a package of task types, a queue, its
handlers, and its schedules. [The example program](../../examples/modules)
has two modules, orders and billing.

**1. Give each module a package of task types only,** such as
`internal/billing/billingtasks`. Start each kind with the module's name, and send its
tasks to a queue named after the module:

```go
package billingtasks

import "github.com/code19m/taskmill"

// CreateInvoice asks billing to invoice an order.
type CreateInvoice struct {
    OrderID int64 `json:"order_id"`
}

// Kind starts with the module's name, so two modules never pick the same kind.
func (CreateInvoice) Kind() string { return "billing.create_invoice" }

// EnqueueDefaults puts the task in the module's queue, wherever it is enqueued.
func (CreateInvoice) EnqueueDefaults() taskmill.EnqueueOptions {
    return taskmill.EnqueueOptions{Queue: "billing"}
}
```

**2. Let each module register its handlers and return its schedules:**

```go
// Register adds the handlers of billing's tasks.
func (m *Module) Register(handlers *taskmill.Handlers) {
    taskmill.AddHandler(handlers, taskmill.HandlerFunc[billingtasks.CreateInvoice](m.createInvoice), nil)
    taskmill.AddHandler(handlers, taskmill.HandlerFunc[billingtasks.SendReminders](m.sendReminders), nil)
}

// Schedules returns billing's schedules.
func (*Module) Schedules() []taskmill.Schedule {
    return []taskmill.Schedule{{
        Name: "billing.send_reminders", // the module's name first, as in its kinds
        Cron: "0 9 * * *",
        Args: billingtasks.SendReminders{},
    }}
}
```

**3. Put the modules together in `main`,** in one worker:

```go
// module is what main needs from each module.
type module interface {
    Register(handlers *taskmill.Handlers)
    Schedules() []taskmill.Schedule
}
// ...
func newWorker(client *taskmill.Client, modules ...module) (*taskmill.Worker, error) {
    var handlers taskmill.Handlers
    var schedules []taskmill.Schedule
    for _, m := range modules {
        m.Register(&handlers)                           // NewWorker rejects a kind that two modules register
        schedules = append(schedules, m.Schedules()...) // and a schedule name that two modules use
    }
    return taskmill.NewWorker(client, &taskmill.WorkerConfig{Handlers: &handlers, Schedules: schedules})
}
```

**4. Enqueue another module's task with its owner's types.** Here orders
saves an order with its receipt and billing's invoice, in one transaction:

```go
_, err = m.client.EnqueueManyTx(ctx, taskmill.Pgx(tx), []taskmill.EnqueueParams{
    {Args: ordertasks.SendReceipt{OrderID: orderID}},
    {Args: billingtasks.CreateInvoice{OrderID: orderID}}, // billing's type, so billing's queue
})
```

## Why

- **One schema serves every module.** One client enqueues any module's task,
  in the same transaction as your data. One
  [maintainer](../concepts/glossary.md#maintainer) runs every module's
  schedules.
- **Kinds and schedule names are unique in a schema.** The module's name in
  front keeps teams apart, and `NewWorker` fails on a kind that two modules
  register, or a schedule name that two of them use.
- **A types package keeps modules apart.** Orders imports billing's types,
  not billing's code, so no import cycle can grow. Billing changes its
  types as [Change a task safely](change-a-task-safely.md) says.
- **A queue per module keeps the way open.** A worker reads the queues of
  the kinds it handles, so billing's tasks can later move to workers of
  their own, with no change to any task.

## Test a module

Check another module's task with its owner's type, and give `Work` the
handlers of every module that the flow reaches:

```go
orderID, err := ordersModule.PlaceOrder(t.Context(), "ada@example.com")
// ...
invoices := taskmilltest.Enqueued[billingtasks.CreateInvoice](t, client) // billing's type
// ...
// Work runs until every task is done, so it needs every module that the order reaches.
var handlers taskmill.Handlers
ordersModule.Register(&handlers)
billing.New(logger).Register(&handlers)
taskmilltest.Work(t, client, &taskmill.WorkerConfig{Handlers: &handlers})
```

`Work` waits until no task of the schema is available or running, in any
queue. Without billing's handlers, nobody reads the `billing` queue, and
`Work` fails the test after a minute ([Test your code](../guides/testing.md)).

## Give a module workers of its own

When billing must scale on its own, or must not share workers with the
other modules, give it a deployment of its own workers, from the same
binary:

| Workers | Register the handlers of | Define the schedules of | `NoMaintainer` |
|---|---|---|---|
| The app's workers | Every module but billing | Every module, billing included | `false` |
| Billing's workers | Billing only, so they read only the `billing` queue | No module | `true` |

The maintainer runs only the schedules that it defines itself. So every
worker that can become the maintainer defines all of them, and the others
set `NoMaintainer` ([Deploys](../production/deploys.md#run-two-versions-side-by-side)).
Deploy billing's workers first, then remove billing from the app's workers.

## Other options

| Option | When it fits | The cost |
|---|---|---|
| One queue for all modules | A small app, with one team | A slow module delays the others, and no module can get workers of its own without moving its tasks |
| A schema per module | The modules become services, with deploys of their own | A client per schema, workers and a maintainer per schema. See [Split work across services](split-across-services.md) |
| Handlers registered in `main` | A few handlers | `main` must know each module's handler code |

## Watch out

- **A kind's queue is part of its contract.** To move a kind to another
  queue later, follow [Change a task safely](change-a-task-safely.md).
- **Two modules can't share a kind.** If both need the same work, one module
  owns it, and the other enqueues it with the owner's types.

## See also

- [Fit taskmill to your architecture](../start/architectures.md): the other architectures
- [Workers and queues](../guides/workers-and-queues.md): which queues a worker reads
- [`WorkerConfig`](https://pkg.go.dev/github.com/code19m/taskmill#WorkerConfig) on pkg.go.dev

**Next:** [Split work across services](split-across-services.md)
