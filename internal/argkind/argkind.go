// Package argkind reads the task kind of an Args type.
package argkind

import (
	"fmt"
	"reflect"
)

// Of returns the kind of A, read from its zero value. For a pointer type, it
// reads it from a pointer to a zero value, since a nil pointer's Kind method
// may panic. A Kind method that panics anyway is reported as an error.
func Of[A interface{ Kind() string }]() (kind string, err error) {
	var zero A
	if t := reflect.TypeFor[A](); t.Kind() == reflect.Pointer {
		zero, _ = reflect.TypeAssert[A](reflect.New(t.Elem()))
	}
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%v.Kind panicked on a zero value: %v", reflect.TypeFor[A](), r)
		}
	}()
	return zero.Kind(), nil
}
