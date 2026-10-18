package outbox

import (
	"time"

	"github.com/code19m/taskmill"
)

// FastRetry shortens the relay's backoff, so that tests of retries are quick.
// Call it before any test runs.
func FastRetry() {
	relayRetry = taskmill.ExponentialBackoff{Base: 10 * time.Millisecond, Max: 10 * time.Millisecond}
}
