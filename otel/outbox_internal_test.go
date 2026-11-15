package otel

import (
	"strings"
	"testing"
)

// stubbornError is an error whose Error method panics with the error itself,
// so that the panic's value panics again when it is formatted.
type stubbornError struct{}

func (e *stubbornError) Error() string { panic(e) }

// The error of a publisher's panic names the panic's value, or the value's
// type if formatting it panics again. Error runs in the deferred function
// that ends the spans, where a second panic would leave them open.
func TestPanicErrorNamesAValueThatPanicsAgain(t *testing.T) {
	t.Parallel()
	for value, want := range map[any]string{
		"the broker's client broke": "panic: the broker's client broke",
		&stubbornError{}:            "panic: a *otel.stubbornError that panics",
	} {
		if got := (panicError{value: value}).Error(); !strings.HasPrefix(got, want) {
			t.Errorf("got the error %q, want one that starts with %q", got, want)
		}
	}
}
