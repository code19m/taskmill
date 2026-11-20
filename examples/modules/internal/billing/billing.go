// Package billing is the billing module of the example: it creates invoices.
package billing

import (
	"context"
	"log/slog"

	"github.com/code19m/taskmill"
	"github.com/code19m/taskmill/examples/modules/internal/billing/billingtasks"
)

// logKeyOrderID is the log attribute key of an order's ID.
const logKeyOrderID = "order_id"

// Module is the billing module, with what its handlers need.
type Module struct {
	logger *slog.Logger
}

// New returns the billing module.
func New(logger *slog.Logger) *Module {
	return &Module{logger: logger}
}

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

func (m *Module) createInvoice(ctx context.Context, t *taskmill.Task[billingtasks.CreateInvoice]) error {
	m.logger.LogAttrs(ctx, slog.LevelInfo, "invoice created", slog.Int64(logKeyOrderID, t.Args.OrderID))
	return nil
}

func (m *Module) sendReminders(ctx context.Context, _ *taskmill.Task[billingtasks.SendReminders]) error {
	m.logger.LogAttrs(ctx, slog.LevelInfo, "reminders sent")
	return nil
}
