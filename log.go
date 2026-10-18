package taskmill

// Log attribute keys, shared so that logs are consistent and searchable.
const (
	logKeyAttempt     = "attempt"
	logKeyConcurrency = "concurrency"
	logKeyDuration    = "duration"
	logKeyError       = "error"
	logKeyGroup       = "group"
	logKeyKind        = "kind"
	logKeyKinds       = "kinds"
	logKeyMaxConns    = "max_conns"
	logKeyName        = "name"
	logKeyPartition   = "partition"
	logKeyQueue       = "queue"
	logKeyQueues      = "queues"
	logKeyReleased    = "released"
	logKeyRetryIn     = "retry_in"
	logKeyStep        = "step"
	logKeyTaskID      = "task_id"
	logKeyTasks       = "tasks"
	logKeyTimeout     = "timeout"
	logKeyVersion     = "version"
	logKeyWorker      = "worker_id"
)
