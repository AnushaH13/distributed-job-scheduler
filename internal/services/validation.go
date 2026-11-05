package services

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/anushah13/distributed-job-scheduler/internal/models"
	"github.com/anushah13/distributed-job-scheduler/internal/schedule"
)

// Limits from the API design.
const (
	MaxPayloadBytes       = 64 * 1024
	DefaultMaxRetries     = 3
	MaxMaxRetries         = 10
	DefaultTimeoutSeconds = 300
	MaxTimeoutSeconds     = 3600
	MaxDelaySeconds       = 365 * 24 * 60 * 60
	MaxIdempotencyKeyLen  = 255
	runAtPastTolerance    = 5 * time.Minute
	runAtMaxFuture        = 365 * 24 * time.Hour
)

// CreateRequest is the body of POST /api/v1/jobs.
type CreateRequest struct {
	JobType        string          `json:"job_type"`
	Payload        json.RawMessage `json:"payload"`
	Schedule       ScheduleRequest `json:"schedule"`
	MaxRetries     *int            `json:"max_retries"`
	TimeoutSeconds *int            `json:"timeout_seconds"`
}

// ScheduleRequest says when the job runs.
type ScheduleRequest struct {
	Type         string `json:"type"`
	RunAt        string `json:"run_at,omitempty"`
	DelaySeconds *int64 `json:"delay_seconds,omitempty"`
	Cron         string `json:"cron,omitempty"`
	Timezone     string `json:"timezone,omitempty"`
}

// FieldIssue describes one invalid field.
type FieldIssue struct {
	Field string `json:"field"`
	Issue string `json:"issue"`
}

// ValidationError lists every problem with a request at once.
type ValidationError struct {
	Details []FieldIssue
}

func (e *ValidationError) Error() string {
	parts := make([]string, len(e.Details))
	for i, d := range e.Details {
		parts[i] = d.Field + ": " + d.Issue
	}
	return "validation failed: " + strings.Join(parts, "; ")
}

func (e *ValidationError) add(field, format string, args ...any) {
	e.Details = append(e.Details, FieldIssue{Field: field, Issue: fmt.Sprintf(format, args...)})
}

// validated is a request that passed validation, with defaults applied.
type validated struct {
	jobType        string
	payload        map[string]any
	scheduleType   models.ScheduleType
	runAt          time.Time
	cron           string
	timezone       string
	maxRetries     int
	timeoutSeconds int
	fingerprint    string
}

// validate checks req against the rules in the API design. now is the
// server's clock, used for delay_seconds and run_at limits.
func validate(req CreateRequest, isRegistered func(string) bool, now time.Time) (*validated, error) {
	v := &validated{maxRetries: DefaultMaxRetries, timeoutSeconds: DefaultTimeoutSeconds}
	verr := &ValidationError{}

	v.jobType = strings.TrimSpace(req.JobType)
	switch {
	case v.jobType == "":
		verr.add("job_type", "is required")
	case !isRegistered(v.jobType):
		verr.add("job_type", "%q is not a registered job type", v.jobType)
	}

	payload := bytes.TrimSpace(req.Payload)
	if len(payload) > MaxPayloadBytes {
		return nil, ErrPayloadTooLarge
	}
	if len(payload) == 0 || bytes.Equal(payload, []byte("null")) {
		v.payload = map[string]any{}
	} else if err := json.Unmarshal(payload, &v.payload); err != nil || v.payload == nil {
		verr.add("payload", "must be a JSON object")
	}

	if req.MaxRetries != nil {
		v.maxRetries = *req.MaxRetries
		if v.maxRetries < 0 || v.maxRetries > MaxMaxRetries {
			verr.add("max_retries", "must be between 0 and %d", MaxMaxRetries)
		}
	}
	if req.TimeoutSeconds != nil {
		v.timeoutSeconds = *req.TimeoutSeconds
		if v.timeoutSeconds < 1 || v.timeoutSeconds > MaxTimeoutSeconds {
			verr.add("timeout_seconds", "must be between 1 and %d", MaxTimeoutSeconds)
		}
	}

	s := req.Schedule
	switch models.ScheduleType(s.Type) {
	case models.ScheduleImmediate:
		v.scheduleType = models.ScheduleImmediate
		v.runAt = now
		if s.RunAt != "" || s.DelaySeconds != nil || s.Cron != "" || s.Timezone != "" {
			verr.add("schedule", "immediate jobs take no other schedule fields")
		}

	case models.ScheduleDelayed:
		v.scheduleType = models.ScheduleDelayed
		switch {
		case (s.RunAt == "") == (s.DelaySeconds == nil):
			verr.add("schedule", "delayed jobs need exactly one of run_at or delay_seconds")
		case s.RunAt != "":
			t, err := time.Parse(time.RFC3339, s.RunAt)
			switch {
			case err != nil:
				verr.add("schedule.run_at", "must be an RFC 3339 time such as 2026-11-03T18:30:00Z")
			case t.Before(now.Add(-runAtPastTolerance)):
				verr.add("schedule.run_at", "is more than 5 minutes in the past")
			case t.After(now.Add(runAtMaxFuture)):
				verr.add("schedule.run_at", "is more than 365 days in the future")
			default:
				v.runAt = t.UTC()
			}
		default:
			d := *s.DelaySeconds
			if d < 1 || d > MaxDelaySeconds {
				verr.add("schedule.delay_seconds", "must be between 1 and %d", MaxDelaySeconds)
			} else {
				v.runAt = now.Add(time.Duration(d) * time.Second)
			}
		}
		if s.Cron != "" || s.Timezone != "" {
			verr.add("schedule", "delayed jobs take no cron or timezone")
		}

	case models.ScheduleRecurring:
		v.scheduleType = models.ScheduleRecurring
		v.cron, v.timezone = strings.TrimSpace(s.Cron), strings.TrimSpace(s.Timezone)
		if v.timezone == "" {
			v.timezone = "UTC"
		}
		c, err := schedule.ParseCron(v.cron, v.timezone)
		if err != nil {
			verr.add("schedule", "%v", err)
		} else {
			v.runAt = c.Next(now)
		}
		if s.RunAt != "" || s.DelaySeconds != nil {
			verr.add("schedule", "recurring jobs take no run_at or delay_seconds")
		}

	case "":
		verr.add("schedule.type", "is required (immediate, delayed or recurring)")
	default:
		verr.add("schedule.type", "%q must be immediate, delayed or recurring", s.Type)
	}

	if len(verr.Details) > 0 {
		return nil, verr
	}
	v.fingerprint = fingerprint(req, v)
	return v, nil
}

// fingerprint is a SHA-256 of the normalized request. Two requests with the
// same idempotency key must have the same fingerprint, otherwise the key was
// reused for a different job. delay_seconds is hashed as given (not as the
// computed time), so retrying the same request a few seconds later matches.
func fingerprint(req CreateRequest, v *validated) string {
	normalized := struct {
		JobType        string          `json:"job_type"`
		Payload        map[string]any  `json:"payload"`
		Schedule       ScheduleRequest `json:"schedule"`
		MaxRetries     int             `json:"max_retries"`
		TimeoutSeconds int             `json:"timeout_seconds"`
	}{v.jobType, v.payload, req.Schedule, v.maxRetries, v.timeoutSeconds}
	if v.scheduleType == models.ScheduleDelayed && req.Schedule.RunAt != "" {
		normalized.Schedule.RunAt = v.runAt.Format(time.RFC3339Nano)
	}
	normalized.Schedule.Timezone = v.timezone
	normalized.Schedule.Cron = v.cron

	// encoding/json sorts map keys, so the output is deterministic.
	b, _ := json.Marshal(normalized)
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}
