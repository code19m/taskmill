package taskmill

// Log attribute keys, shared so that logs are consistent and searchable.
const (
	logKeyAttempt  = "attempt"
	logKeyDuration = "duration"
	logKeyError    = "error"
	logKeyGroup    = "group"
	logKeyKind     = "kind"
	logKeyName     = "name"
	logKeyPart     = "partition"
	logKeyQueues   = "queues"
	logKeyRetryIn  = "retry_in"
	logKeyStep     = "step"
	logKeyTasks    = "tasks"
	logKeyQueue    = "queue"
	logKeyTaskID   = "task_id"
	logKeyVersion  = "version"
	logKeyWorker   = "worker_id"
)
