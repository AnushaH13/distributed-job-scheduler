// Package models defines the documents stored in MongoDB and returned by the API.
package models

import "time"

// JobStatus is the stored state of a job. "Created" and "failed" are moments,
// not states: a failed attempt is recorded on its execution record.
type JobStatus string

// Stored job states. Completed and cancelled are final; dead_lettered is left
// only through a manual retry.
const (
	StatusScheduled    JobStatus = "scheduled"
	StatusQueued       JobStatus = "queued"
	StatusRunning      JobStatus = "running"
	StatusRetrying     JobStatus = "retrying"
	StatusCompleted    JobStatus = "completed"
	StatusCancelled    JobStatus = "cancelled"
	StatusDeadLettered JobStatus = "dead_lettered"
)

// Valid reports whether s is a known status.
func (s JobStatus) Valid() bool {
	switch s {
	case StatusScheduled, StatusQueued, StatusRunning, StatusRetrying,
		StatusCompleted, StatusCancelled, StatusDeadLettered:
		return true
	}
	return false
}

// ScheduleType says when a job runs.
type ScheduleType string

// Schedule types.
const (
	ScheduleImmediate ScheduleType = "immediate"
	ScheduleDelayed   ScheduleType = "delayed"
	ScheduleRecurring ScheduleType = "recurring"
)

// Job is the current state of one job. All times are UTC.
//
// Fields tagged json:"-" are internal coordination data and are never
// returned by the API.
type Job struct {
	ID       string         `bson:"_id" json:"job_id"`
	JobType  string         `bson:"job_type" json:"job_type"`
	Payload  map[string]any `bson:"payload" json:"payload,omitempty"`
	Schedule ScheduleType   `bson:"schedule_type" json:"schedule_type"`

	CronExpression string `bson:"cron_expression,omitempty" json:"cron_expression,omitempty"`
	Timezone       string `bson:"timezone,omitempty" json:"timezone,omitempty"`

	// omitempty matters: the unique partial index only covers string values,
	// so jobs without a key must not store an empty string.
	IdempotencyKey     string `bson:"idempotency_key,omitempty" json:"-"`
	RequestFingerprint string `bson:"request_fingerprint,omitempty" json:"-"`

	MaxRetries     int `bson:"max_retries" json:"max_retries"`
	TimeoutSeconds int `bson:"timeout_seconds" json:"timeout_seconds"`

	Status      JobStatus `bson:"status" json:"status"`
	ScheduledAt time.Time `bson:"scheduled_at" json:"scheduled_at"`
	NextRunAt   time.Time `bson:"next_run_at" json:"next_run_at"`
	// OccurrenceAt is the planned time of the current occurrence. Retries
	// change NextRunAt but not OccurrenceAt, so JobID + OccurrenceAt
	// identifies "this run" for handler idempotency keys.
	OccurrenceAt time.Time `bson:"occurrence_at" json:"occurrence_at"`
	RetryCount   int       `bson:"retry_count" json:"retry_count"`
	LastError    string    `bson:"last_error,omitempty" json:"last_error,omitempty"`

	LeaseOwner     string     `bson:"lease_owner,omitempty" json:"worker_id,omitempty"`
	LeaseExpiresAt *time.Time `bson:"lease_expires_at,omitempty" json:"-"`
	ClaimToken     string     `bson:"claim_token,omitempty" json:"-"`
	QueuedAt       *time.Time `bson:"queued_at,omitempty" json:"-"`

	LastSchedulerToken int64 `bson:"last_scheduler_token" json:"-"`
	CancelRequested    bool  `bson:"cancel_requested" json:"cancel_requested"`

	CreatedAt   time.Time  `bson:"created_at" json:"created_at"`
	UpdatedAt   time.Time  `bson:"updated_at" json:"updated_at"`
	CompletedAt *time.Time `bson:"completed_at,omitempty" json:"completed_at,omitempty"`
}

// Attempt returns the attempt number of the next (or current) run of this
// occurrence: 1 for the first try, 2 for the first retry, and so on.
func (j *Job) Attempt() int {
	return j.RetryCount + 1
}
