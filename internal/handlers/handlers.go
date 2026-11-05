// Package handlers maps job types to the Go code that runs them.
//
// Handlers must be idempotent: the scheduler guarantees at-least-once
// execution, so the same occurrence can run more than once (for example
// when a worker crashes after the side effect but before recording success).
// Use JobID + occurrence time as the idempotency key for downstream calls,
// never the attempt number or claim token.
package handlers

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"time"

	"github.com/anushah13/distributed-job-scheduler/internal/models"
)

// Handler runs one attempt of a job. It must return promptly when ctx is
// cancelled (job timeout, cancellation, or worker shutdown).
type Handler func(ctx context.Context, job *models.Job) error

// PermanentError marks a failure that retrying cannot fix, such as invalid
// input. The job goes straight to the dead-letter queue.
type PermanentError struct{ Err error }

func (e *PermanentError) Error() string { return e.Err.Error() }
func (e *PermanentError) Unwrap() error { return e.Err }

// Permanent wraps err so the worker does not retry it.
func Permanent(err error) error { return &PermanentError{Err: err} }

// IsPermanent reports whether err (or anything it wraps) is permanent.
func IsPermanent(err error) bool {
	var p *PermanentError
	return errors.As(err, &p)
}

// Registry holds the handlers a worker can run.
type Registry struct {
	handlers map[string]Handler
}

// NewRegistry returns an empty registry.
func NewRegistry() *Registry {
	return &Registry{handlers: make(map[string]Handler)}
}

// Register adds a handler. Registering the same type twice is a programming
// error, so it panics at startup instead of silently replacing a handler.
func (r *Registry) Register(jobType string, h Handler) {
	if _, exists := r.handlers[jobType]; exists {
		panic(fmt.Sprintf("handlers: job type %q registered twice", jobType))
	}
	r.handlers[jobType] = h
}

// Get returns the handler for jobType.
func (r *Registry) Get(jobType string) (Handler, bool) {
	h, ok := r.handlers[jobType]
	return h, ok
}

// Has reports whether jobType is registered. The API uses it to validate requests.
func (r *Registry) Has(jobType string) bool {
	_, ok := r.handlers[jobType]
	return ok
}

// Types returns the registered job types in sorted order.
func (r *Registry) Types() []string {
	types := make([]string, 0, len(r.handlers))
	for t := range r.handlers {
		types = append(types, t)
	}
	sort.Strings(types)
	return types
}

// Default returns the built-in demonstration handlers:
//
//   - "echo" logs its payload.
//   - "sleep" waits payload.duration_ms milliseconds (default 1000), honouring cancellation.
//   - "fail" always fails; payload.permanent=true makes the failure permanent.
//     Useful for watching retries and the dead-letter queue.
func Default(logger *slog.Logger) *Registry {
	r := NewRegistry()

	r.Register("echo", func(ctx context.Context, job *models.Job) error {
		logger.InfoContext(ctx, "echo job", "job_id", job.ID, "payload", job.Payload)
		return nil
	})

	r.Register("sleep", func(ctx context.Context, job *models.Job) error {
		d := time.Second
		if ms, ok := number(job.Payload["duration_ms"]); ok {
			if ms < 0 || ms > 3_600_000 {
				return Permanent(fmt.Errorf("duration_ms %v must be between 0 and 3600000", ms))
			}
			d = time.Duration(ms) * time.Millisecond
		}
		timer := time.NewTimer(d)
		defer timer.Stop()
		select {
		case <-timer.C:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})

	r.Register("fail", func(ctx context.Context, job *models.Job) error {
		msg := "simulated failure"
		if m, ok := job.Payload["message"].(string); ok && m != "" {
			msg = m
		}
		if permanent, _ := job.Payload["permanent"].(bool); permanent {
			return Permanent(errors.New(msg))
		}
		return errors.New(msg)
	})

	return r
}

// number converts JSON or BSON numbers to int64.
func number(v any) (int64, bool) {
	switch n := v.(type) {
	case float64:
		return int64(n), true
	case int32:
		return int64(n), true
	case int64:
		return n, true
	case int:
		return int64(n), true
	}
	return 0, false
}
