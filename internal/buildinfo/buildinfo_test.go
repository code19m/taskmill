package buildinfo

import "testing"

// A test binary is built in taskmill's module, so the lookup finds it.
func TestVersionFindsTheModule(t *testing.T) {
	t.Parallel()
	if v := Version(); v == "" || v == "unknown" {
		t.Fatalf("Version() = %q, want the version of the module", v)
	}
}
