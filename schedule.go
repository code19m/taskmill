package taskmill

import (
	"context"
	"time"
)

// Schedule enqueues a task on a cron schedule, once per scheduled time across
// all workers.
type Schedule struct {
	// Name identifies the schedule. Schedules are kept in the database by
	// name, so renaming one creates a new schedule.
	Name string

	// Cron is a standard five-field cron expression, such as "0 3 * * *".
	Cron string

	// Location is the time zone the expression is read in. Default: UTC.
	Location *time.Location

	// Args and Options describe the task to enqueue.
	Args    Args
	Options *EnqueueOptions
}

func validateSchedules(ss []Schedule) error {
	seen := make(map[string]bool, len(ss))
	for _, s := range ss {
		if err := checkName("schedule name", s.Name, maxNameLength); err != nil {
			return err
		}
		if seen[s.Name] {
			return invalidArgument("schedule %q defined twice", s.Name)
		}
		seen[s.Name] = true
		if s.Args == nil {
			return invalidArgument("schedule %q has no args", s.Name)
		}
	}
	return nil
}

func (*maintainer) runSchedules(context.Context) {}
