package taskmill

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// This file follows a history partition through its life: who creates it,
// what a completion does when its day has none, and who drops it. Migrate,
// each worker that starts, and retention create the partitions. Retention
// also drops them: applyRetention in maintainer.go calls dropHistory.

// historyDaysAhead is how many days of history partitions are kept ready, so
// that completions never wait for a partition.
const historyDaysAhead = 7

// partitionLockTimeout bounds the wait for the history table's lock while
// attaching a partition, so that a long transaction cannot stall completions
// behind it.
const partitionLockTimeout = "5s"

// ensureHistoryPartitions creates the daily history partitions of the next
// historyDaysAhead days, today's first, skipping those that exist. Each
// partition is created detached and then attached, which does not block
// completions the way CREATE TABLE ... PARTITION OF would. An error for a
// missing privilege names the fix.
//
// A call that finds every partition ready takes no lock. Otherwise it first
// takes the lock on the history table that attaching takes anyway, so that
// processes that do this at the same time wait for each other, for at most
// partitionLockTimeout. Only a role that may change the table can take this
// lock, so a role without rights on the schema cannot keep workers from
// starting, as it could with an advisory lock, which any role may take.
func ensureHistoryPartitions(ctx context.Context, pool *pgxpool.Pool, schema string) error {
	err := pgx.BeginTxFunc(ctx, pool, readCommitted, func(tx pgx.Tx) error {
		days, err := missingHistoryDays(ctx, tx, schema)
		if err != nil || len(days) == 0 {
			return err
		}
		if _, err := tx.Exec(ctx, lockTimeoutSQL, partitionLockTimeout); err != nil {
			return err
		}
		// ONLY: the partitions stay free for autovacuum.
		lock := fmt.Sprintf("LOCK TABLE ONLY %s.task_history IN SHARE UPDATE EXCLUSIVE MODE", schema)
		if _, err := tx.Exec(ctx, lock); err != nil {
			return err
		}
		// Another process may have created them while this one waited.
		if days, err = missingHistoryDays(ctx, tx, schema); err != nil {
			return err
		}
		for _, from := range days {
			name := historyPartition(from)
			create := fmt.Sprintf("CREATE TABLE %[1]s.%[2]s (LIKE %[1]s.task_history INCLUDING DEFAULTS INCLUDING CONSTRAINTS)",
				schema, name)
			attach := fmt.Sprintf("ALTER TABLE %[1]s.task_history ATTACH PARTITION %[1]s.%[2]s FOR VALUES FROM ('%[3]s') TO ('%[4]s')",
				schema, name, from.Format(time.RFC3339), from.AddDate(0, 0, 1).Format(time.RFC3339))
			if _, err := tx.Exec(ctx, create); err != nil {
				return fmt.Errorf("create %s: %w", name, err)
			}
			if _, err := tx.Exec(ctx, attach); err != nil {
				return fmt.Errorf("attach %s: %w", name, err)
			}
		}
		return nil
	})
	if pgCode(err) == codeInsufficientPrivilege {
		return fmt.Errorf("prepare history partitions: %w; the role that runs workers must own the taskmill tables", err)
	}
	if err != nil {
		return fmt.Errorf("prepare history partitions: %w", err)
	}
	return nil
}

// missingHistoryDays returns the days among the next historyDaysAhead, from
// today, that have no history partition, in order. Today is the database's:
// its clock sets the day of a completed task. The days are UTC days, and a
// day's name is printed from a time without a time zone: to_char reads a
// date as midnight on the clock of the session's time zone, which a zone
// that leaves a day out does not have.
func missingHistoryDays(ctx context.Context, tx pgx.Tx, schema string) ([]time.Time, error) {
	r, err := tx.Query(ctx, `
SELECT d.day
FROM (SELECT (now() AT TIME ZONE 'UTC')::date + i AS day FROM generate_series(0, $2::integer - 1) AS i) d
WHERE NOT EXISTS (
    SELECT FROM pg_inherits h JOIN pg_class c ON c.oid = h.inhrelid
    WHERE h.inhparent = $1::regclass AND c.relname = 'task_history_' || to_char(d.day::timestamp, 'YYYYMMDD'))
ORDER BY d.day`, schema+".task_history", historyDaysAhead)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(r, pgx.RowTo[time.Time])
}

// historyPartition names the history partition of the UTC day that starts at
// day.
func historyPartition(day time.Time) string {
	return "task_history_" + day.UTC().Format("20060102")
}

// prepareHistory creates the history partitions that the next week lacks,
// and logs a failure. Each worker does it when it starts, because a schema
// applied with Migrations has none, and completions may come before the
// maintainer's first pass, or from workers that never lead.
func (w *Worker) prepareHistory(ctx context.Context) {
	if err := ensureHistoryPartitions(ctx, w.client.pool, w.client.schema); err != nil && ctx.Err() == nil {
		w.logger.LogAttrs(ctx, slog.LevelWarn, "prepare history partitions failed", slog.Any(logKeyError, err))
	}
}

// historyGap is how long completions skip history after one found no history
// partition for its day.
const historyGap = time.Minute

// noteMissingHistory makes completions skip history for a while if err says
// that the history has no partition for today: the maintainer could not
// create it, typically because the worker's role does not own the table.
// Completions keep working, and a retry of the failed one succeeds. The
// history table has no CHECK constraint, so a check violation on it can only
// mean a missing partition; the message is not read, as it may be
// translated.
func (c *Client) noteMissingHistory(ctx context.Context, logger *slog.Logger, err error) {
	pgErr, ok := errors.AsType[*pgconn.PgError](err)
	if !ok || pgErr.Code != codeCheckViolation || pgErr.TableName != "task_history" || pgErr.SchemaName != c.schema {
		return
	}
	c.historyGapUntil.Store(time.Now().Add(historyGap).UnixNano())
	logger.LogAttrs(ctx, slog.LevelError, "task history has no partition for today; completed tasks skip history",
		slog.Any(logKeyError, err))
}

// Limits of dropping history partitions.
const (
	detachTimeout = 30 * time.Second // bound on detaching one history partition, and on dropping it
	historyDrops  = 10               // most history partitions one retention pass drops

	// historyDropMark is the comment that retention sets on a history
	// partition before it detaches it. A table that is no partition is
	// retention's to drop only with this comment: one that an operator
	// detached or copied, to keep it, has none. Databases store the text,
	// so it never changes.
	historyDropMark = "taskmill drops this expired history partition"
)

// expiredPartition is a history partition whose day is older than the
// retention period.
type expiredPartition struct {
	name     string
	attached bool // false if an earlier pass marked and detached it, and could not drop it
	pending  bool // its detach was interrupted
}

// dropHistory detaches and drops history partitions whose whole day is older
// than the retention period, at most historyDrops in a pass. The database
// says which ones, by its own clock. It returns the errors of the partitions
// that failed, and continues with the others: what keeps one table, such as a
// view that depends on it, does not keep the next.
//
// A detach that times out is the exception. It waited for the transactions
// that use the history, which keep every other detach waiting as long, and
// PostgreSQL allows a table only one unfinished detach. So the pass detaches
// no more, and still drops what is detached already.
func (m *maintainer) dropHistory(ctx context.Context) error {
	r, err := m.c.pool.Query(ctx, m.c.sql.expiredHistory, historyDrops, historyDropMark)
	if err != nil {
		return err
	}
	expired, err := pgx.CollectRows(r, func(row pgx.CollectableRow) (expiredPartition, error) {
		var p expiredPartition
		err := row.Scan(&p.name, &p.attached, &p.pending)
		return p, err
	})
	if err != nil {
		return err
	}
	var errs []error
	busy := false // a detach timed out
	for _, p := range expired {
		if p.attached {
			if busy {
				continue
			}
			if err := m.detachPartition(ctx, p); err != nil {
				errs = append(errs, err)
				busy = errors.Is(err, context.DeadlineExceeded)
				continue
			}
		}
		if err := m.dropDetached(ctx, p.name); err != nil {
			errs = append(errs, err)
			continue
		}
		m.w.logger.LogAttrs(ctx, slog.LevelInfo, "dropped expired history", slog.String(logKeyPartition, p.name))
	}
	return errors.Join(errs...)
}

// detachPartition marks and detaches one history partition. DETACH ...
// CONCURRENTLY does not block completions, and must run outside a
// transaction. It waits for transactions that use the history, so it is
// bounded by a timeout. If it is interrupted, the partition stays pending
// detach, and a later pass finalizes the detach instead.
//
// The mark tells a later pass that the detached table is its own to drop,
// should the drop fail. So it is set first: a crash after it leaves a
// marked partition, which changes nothing. A comment needs the lock on the
// partition that the detach takes next anyway, which blocks neither reads
// nor writes.
func (m *maintainer) detachPartition(ctx context.Context, p expiredPartition) error {
	ctx, cancel := context.WithTimeout(ctx, detachTimeout)
	defer cancel()
	mark := fmt.Sprintf("COMMENT ON TABLE %s.%s IS '%s'", m.c.schema, p.name, historyDropMark)
	if _, err := m.c.pool.Exec(ctx, mark); err != nil {
		return fmt.Errorf("mark %s: %w", p.name, err)
	}
	mode := "CONCURRENTLY"
	if p.pending {
		mode = "FINALIZE"
	}
	detach := fmt.Sprintf("ALTER TABLE %[1]s.task_history DETACH PARTITION %[1]s.%[2]s %[3]s", m.c.schema, p.name, mode)
	if _, err := m.c.pool.Exec(ctx, detach); err != nil {
		return fmt.Errorf("detach %s: %w", p.name, err)
	}
	return nil
}

// dropDetached drops a history partition that is detached. The drop waits
// for whoever reads that table, so it is bounded by a timeout too. A table
// that is gone is no failure: while the maintainer's role changes hands, two
// workers can drop the same tables, and one of them comes second.
func (m *maintainer) dropDetached(ctx context.Context, name string) error {
	ctx, cancel := context.WithTimeout(ctx, detachTimeout)
	defer cancel()
	if _, err := m.c.pool.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s.%s", m.c.schema, name)); err != nil {
		return fmt.Errorf("drop %s: %w", name, err)
	}
	return nil
}
