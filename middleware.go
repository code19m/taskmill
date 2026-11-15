package taskmill

import (
	"context"
	"slices"
	"sync/atomic"
)

// through runs call through the middleware ms, the first outermost, and
// gives each one arg. It keeps the rules of a chain, the same for attempts
// and for enqueues:
//
//   - call runs at most once: a second call of next returns twice.
//   - A middleware that returns nil without calling its next fails with
//     skipped right away, so that the middleware outside it sees the failure
//     too. If that one hides the failure, the chain still fails with skipped.
//
// So a nil error means that call ran, not that it succeeded: a middleware
// may hide the error of call.
func through[T any, M ~func(context.Context, T, func(context.Context) error) error](
	ctx context.Context, ms []M, arg T, call func(context.Context) error, twice, skipped error,
) error {
	var called atomic.Bool
	next := func(ctx context.Context) error {
		if called.Swap(true) {
			return twice
		}
		return call(ctx)
	}
	for _, m := range slices.Backward(ms) {
		inner := next
		next = func(ctx context.Context) error {
			var reached atomic.Bool
			err := m(ctx, arg, func(ctx context.Context) error {
				reached.Store(true)
				return inner(ctx)
			})
			if err == nil && !reached.Load() {
				return skipped
			}
			return err
		}
	}
	if err := next(ctx); err != nil || called.Load() {
		return err
	}
	return skipped // a middleware hid the failure of the one inside it
}
