// Package buildinfo reads taskmill's version from the build information of
// the binary. The otel module reads it on its own: it is a module of its
// own, and cannot import this package.
package buildinfo

import "runtime/debug"

// module is taskmill's module path.
const module = "github.com/code19m/taskmill"

// Version returns the version of taskmill that the binary was built with,
// such as v1.0.0, or "unknown" if the binary has no build information. A
// binary built in taskmill's own module, as its tests are, has the main
// module's version, "(devel)" unless the build stamped one.
func Version() string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return "unknown"
	}
	for _, m := range append([]*debug.Module{&info.Main}, info.Deps...) {
		if m.Path == module {
			return m.Version
		}
	}
	return "unknown"
}
