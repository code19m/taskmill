package taskmill

import (
	"cmp"
	"context"
	"fmt"
	"math"
	"math/rand/v2"
	"time"
)

// This file holds the installation-wide settings, which the maintainer
// applies, and the retry that keeps concurrent updates of any settings from
// losing each other's changes.

// Defaults and limits of the retention periods.
const (
	defaultHistoryRetention = 7 * 24 * time.Hour
	defaultDeadRetention    = 30 * 24 * time.Hour
	minRetention            = time.Hour
)

// Settings are the installation-wide settings, stored in the database and
// applied by the maintainer. Read them with [Client.Settings], and change
// them with [Client.UpdateSettings]. Zero fields mean their defaults. A
// period shorter than its minimum is an error that wraps
// [ErrInvalidArgument]. A period is stored in whole microseconds, and the
// longest [time.Duration], about 292 years, keeps tasks for good. The
// database refuses a period below the minimum or above 300 years, so SQL
// cannot store one either.
type Settings struct {
	// HistoryRetention is how long completed tasks stay in history, at
	// least: history is dropped a whole UTC day at a time, so a task stays
	// up to a day longer. Minimum: 1 hour. Default: 7 days.
	HistoryRetention time.Duration

	// DeadRetention is how long dead and canceled tasks are kept for
	// operators to inspect and retry. A dead task that halts its FIFO group
	// is kept until it is retried or deleted. Minimum: 1 hour. Default: 30
	// days.
	DeadRetention time.Duration
}

// An update of settings that loses to a concurrent update tries again, up
// to updateAttempts times. Before each new try it pauses a random time below
// a cap that doubles from minUpdatePause to maxUpdatePause, so that updates
// that collided spread out; the pauses add up to at most 0.26 seconds.
const (
	updateAttempts = 10
	minUpdatePause = 2 * time.Millisecond
	maxUpdatePause = 50 * time.Millisecond
)

// retryConflicts runs try until it saves the settings or fails, and returns
// an error that wraps [ErrConflict] if every try lost to a concurrent update.
// op names the update in errors.
func retryConflicts(ctx context.Context, op string, try func() (saved bool, err error)) error {
	pause := minUpdatePause
	for i := range updateAttempts {
		if i > 0 {
			wait := time.NewTimer(rand.N(pause)) //nolint:gosec // jitter needs no cryptographic randomness
			select {
			case <-ctx.Done():
				wait.Stop()
				return fmt.Errorf("taskmill: %s: %w", op, ctx.Err())
			case <-wait.C:
			}
			pause = min(2*pause, maxUpdatePause)
		}

		if saved, err := try(); saved || err != nil {
			return err
		}
	}
	return fmt.Errorf("%w: %s: the settings changed during each of %d tries", ErrConflict, op, updateAttempts)
}

// Settings returns the installation-wide settings.
func (c *Client) Settings(ctx context.Context) (*Settings, error) {
	var history, dead int64
	if err := c.pool.QueryRow(ctx, c.sql.readSettings).Scan(&history, &dead); err != nil {
		return nil, fmt.Errorf("taskmill: read settings: %w", err)
	}
	return &Settings{HistoryRetention: fromMicros(history), DeadRetention: fromMicros(dead)}, nil
}

// fromMicros returns the duration of n microseconds, in which the database
// gives a retention period and a queue's lag. One beyond the longest duration
// reads as the longest one: a period that only SQL can write, or the lag of
// a task that is due for more than about 292 years.
func fromMicros(n int64) time.Duration {
	return time.Duration(min(n, math.MaxInt64/int64(time.Microsecond))) * time.Microsecond
}

// UpdateSettings changes the installation-wide settings. update gets a copy
// of the current settings and changes the ones it means to, so that settings
// it does not know of, such as those of a later version, keep their values.
// A setting changed to zero goes back to its default. Like
// [Client.UpdateQueue], it holds no lock while update runs, and runs update
// again if the settings changed meanwhile, up to 10 times before it fails
// with [ErrConflict], so update must only change the settings it gets.
//
//	err := client.UpdateSettings(ctx, func(s *taskmill.Settings) { s.HistoryRetention = 48 * time.Hour })
func (c *Client) UpdateSettings(ctx context.Context, update func(*Settings)) error {
	fail := func(err error) error { return fmt.Errorf("taskmill: update settings: %w", err) }
	if update == nil {
		return invalidArgument("update is nil")
	}

	return retryConflicts(ctx, "update settings", func() (bool, error) {
		var history, dead int64
		if err := c.pool.QueryRow(ctx, c.sql.readSettings).Scan(&history, &dead); err != nil {
			return false, fail(err)
		}

		s := Settings{HistoryRetention: fromMicros(history), DeadRetention: fromMicros(dead)}
		update(&s)
		s.HistoryRetention = cmp.Or(s.HistoryRetention, defaultHistoryRetention)
		s.DeadRetention = cmp.Or(s.DeadRetention, defaultDeadRetention)

		if s.HistoryRetention < minRetention || s.DeadRetention < minRetention {
			return false, invalidArgument("retention periods must be at least %s", minRetention)
		}

		tag, err := c.pool.Exec(ctx, c.sql.swapSettings,
			history,                           // $1
			dead,                              // $2
			s.HistoryRetention.Microseconds(), // $3
			s.DeadRetention.Microseconds(),    // $4
		)
		if err != nil {
			return false, fail(err)
		}
		return tag.RowsAffected() == 1, nil
	})
}
