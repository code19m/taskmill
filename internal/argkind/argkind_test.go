package argkind_test

import (
	"strings"
	"testing"

	"github.com/code19m/taskmill/internal/argkind"
)

// stubbornError is an error whose Error method panics with the error itself,
// so that the panic's value panics again when it is formatted.
type stubbornError struct{}

func (e *stubbornError) Error() string { panic(e) }

// stubbornArgs are args whose Kind method panics with such a value.
type stubbornArgs struct{}

func (stubbornArgs) Kind() string { panic(&stubbornError{}) }

// Of runs when a handler is added. A Kind method that panics there is an
// error, whatever the panic's value: it must not end the process.
func TestOfReportsAKindWhosePanicValuePanics(t *testing.T) {
	t.Parallel()
	kind, err := argkind.Of[stubbornArgs]()
	if kind != "" || err == nil || !strings.Contains(err.Error(), "stubbornError") {
		t.Fatalf("got the kind %q and the error %v, want an error that names the type of the panic's value", kind, err)
	}
}
