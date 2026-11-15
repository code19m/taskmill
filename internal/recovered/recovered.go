// Package recovered formats the values of recovered panics.
package recovered

import "fmt"

// Text formats r, the value of a recovered panic. Formatting runs the
// value's own methods, which can panic again, as an Error method that panics
// with its receiver does. The text then names the value's type instead: a
// recover must not end the process that it is there to save.
func Text(r any) (text string) {
	defer func() {
		if recover() != nil {
			text = fmt.Sprintf("a %T that panics when it is formatted", r)
		}
	}()
	return fmt.Sprint(r)
}
