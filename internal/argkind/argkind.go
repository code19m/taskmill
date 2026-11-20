// Package argkind reads what a zero value of an Args type says about it.
package argkind

import (
	"fmt"
	"reflect"

	"github.com/code19m/taskmill/internal/recovered"
)

// Zero returns a value of A to call methods on: the zero value, or for a
// pointer type a pointer to a zero value, since a nil pointer's methods may
// panic.
func Zero[A any]() A {
	var zero A
	if t := reflect.TypeFor[A](); t.Kind() == reflect.Pointer {
		zero, _ = reflect.TypeAssert[A](reflect.New(t.Elem()))
	}
	return zero
}

// Of returns the kind of A, read from Zero. A Kind method that panics anyway
// is reported as an error, whatever the panic's value.
func Of[A interface{ Kind() string }]() (kind string, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%v.Kind panicked on a zero value: %s", reflect.TypeFor[A](), recovered.Text(r))
		}
	}()

	return Zero[A]().Kind(), nil
}
