package models

import "time"

// Dead-letter reasons.
const (
	ReasonMaxRetriesExceeded = "max_retries_exceeded"
	ReasonPermanentError     = "permanent_error"
)

// Dead-letter resolution states.
const (
	ResolutionPending   = "pending"
	ResolutionRetried   = "retried"
	ResolutionDiscarded = "discarded"
)

// DeadLetter is an incident record for a job occurrence that needs a human.
type DeadLetter struct {
	ID               string         `bson:"_id" json:"dead_letter_id"`
	JobID            string         `bson:"job_id" json:"job_id"`
	JobType          string         `bson:"job_type" json:"job_type"`
	OccurrenceAt     time.Time      `bson:"occurrence_at" json:"occurrence_at"`
	Payload          map[string]any `bson:"payload" json:"payload,omitempty"`
	Reason           string         `bson:"reason" json:"reason"`
	LastError        string         `bson:"last_error" json:"last_error"`
	TotalAttempts    int            `bson:"total_attempts" json:"total_attempts"`
	LastWorkerID     string         `bson:"last_worker_id" json:"last_worker_id"`
	DeadLetteredAt   time.Time      `bson:"dead_lettered_at" json:"dead_lettered_at"`
	ResolutionStatus string         `bson:"resolution_status" json:"resolution_status"`
	ResolvedAt       *time.Time     `bson:"resolved_at,omitempty" json:"resolved_at,omitempty"`
	ResolutionNote   string         `bson:"resolution_note,omitempty" json:"resolution_note,omitempty"`
}
