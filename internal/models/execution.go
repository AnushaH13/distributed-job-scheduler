package models

import "time"

// ExecutionStatus is the outcome of one attempt.
type ExecutionStatus string

// Execution outcomes.
const (
	ExecRunning   ExecutionStatus = "running"
	ExecSucceeded ExecutionStatus = "succeeded"
	ExecFailed    ExecutionStatus = "failed"
	ExecTimedOut  ExecutionStatus = "timed_out"
	ExecAbandoned ExecutionStatus = "abandoned"
	ExecCancelled ExecutionStatus = "cancelled"
)

// ErrorType says whether a failure is worth retrying.
type ErrorType string

// Error types.
const (
	ErrorTransient ErrorType = "transient"
	ErrorPermanent ErrorType = "permanent"
)

// Execution records one attempt to run a job.
type Execution struct {
	ID            string          `bson:"_id" json:"execution_id"`
	JobID         string          `bson:"job_id" json:"job_id"`
	JobType       string          `bson:"job_type" json:"job_type"`
	OccurrenceAt  time.Time       `bson:"occurrence_at" json:"occurrence_at"`
	AttemptNumber int             `bson:"attempt_number" json:"attempt_number"`
	WorkerID      string          `bson:"worker_id" json:"worker_id"`
	ClaimToken    string          `bson:"claim_token" json:"-"`
	StartedAt     time.Time       `bson:"started_at" json:"started_at"`
	CompletedAt   *time.Time      `bson:"completed_at,omitempty" json:"completed_at,omitempty"`
	DurationMS    int64           `bson:"duration_ms,omitempty" json:"duration_ms,omitempty"`
	Status        ExecutionStatus `bson:"execution_status" json:"execution_status"`
	ErrorMessage  string          `bson:"error_message,omitempty" json:"error_message,omitempty"`
	ErrorType     ErrorType       `bson:"error_type,omitempty" json:"error_type,omitempty"`
}
