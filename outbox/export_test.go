package outbox

import (
	"time"

	"github.com/code19m/taskmill"
)

// RelayRetry is the relay's backoff as it is outside tests: FastRetry
// replaces it only after this copy is made.
var RelayRetry = relayRetry

// FastRetry shortens the relay's backoff, so that tests of retries are quick.
// Call it before any test runs.
func FastRetry() {
	relayRetry = taskmill.ExponentialBackoff{Base: 10 * time.Millisecond, Max: 10 * time.Millisecond}
}
