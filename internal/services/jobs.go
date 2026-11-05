// Package services contains the API's business logic, independent of HTTP.
package services

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/anushah13/distributed-job-scheduler/internal/idgen"
	"github.com/anushah13/distributed-job-scheduler/internal/models"
	"github.com/anushah13/distributed-job-scheduler/internal/queue"
	"github.com/anushah13/distributed-job-scheduler/internal/repository"
)

var (
	// ErrNotFound: the job does not exist.
	ErrNotFound = repository.ErrNotFound
	// ErrInvalidState: the job's state does not allow the operation.
	ErrInvalidState = repository.ErrInvalidState
	// ErrUnavailable: MongoDB is unreachable.
	ErrUnavailable = repository.ErrUnavailable
	// ErrIdempotencyKeyReused: the key was used for a different request.
	ErrIdempotencyKeyReused = errors.New("idempotency key reused with a different request")
	// ErrPayloadTooLarge: the payload is over MaxPayloadBytes.
	ErrPayloadTooLarge = errors.New("payload too large")
)

// Lists.
const (
	DefaultListLimit = 20
	MaxListLimit     = 100
)

// JobService implements job management.
type JobService struct {
	store        *repository.Store
	queue        *queue.Queue
	isRegistered func(string) bool
	logger       *slog.Logger
	now          func() time.Time
}

// NewJobService returns a JobService. isRegistered reports whether a job
// type has a handler (handlers.Registry.Has).
func NewJobService(store *repository.Store, q *queue.Queue, isRegistered func(string) bool, logger *slog.Logger) *JobService {
	return &JobService{store: store, queue: q, isRegistered: isRegistered, logger: logger,
		now: func() time.Time { return time.Now().UTC() }}
}

// Create validates and stores a new job, then registers its run time in
// Redis. It returns replayed=true when idempotencyKey matched an earlier
// identical request; that earlier job is returned and nothing new is created.
func (s *JobService) Create(ctx context.Context, req CreateRequest, idempotencyKey string) (*models.Job, bool, error) {
	key := strings.TrimSpace(idempotencyKey)
	if len(key) > MaxIdempotencyKeyLen {
		return nil, false, &ValidationError{Details: []FieldIssue{{Field: "Idempotency-Key", Issue: fmt.Sprintf("must be at most %d characters", MaxIdempotencyKeyLen)}}}
	}

	now := s.now()
	v, err := validate(req, s.isRegistered, now)
	if err != nil {
		return nil, false, err
	}

	job := &models.Job{
		ID:             idgen.New("job"),
		JobType:        v.jobType,
		Payload:        v.payload,
		Schedule:       v.scheduleType,
		CronExpression: v.cron,
		Timezone:       v.timezone,
		MaxRetries:     v.maxRetries,
		TimeoutSeconds: v.timeoutSeconds,
		Status:         models.StatusScheduled,
		ScheduledAt:    v.runAt,
		NextRunAt:      v.runAt,
		OccurrenceAt:   v.runAt,
		CreatedAt:      now,
		UpdatedAt:      now,
	}
	if key != "" {
		job.IdempotencyKey = key
		job.RequestFingerprint = v.fingerprint
	}

	// Insert first, and let the unique index decide. Checking for the key
	// before inserting would race with a concurrent identical request.
	err = s.store.Jobs.Insert(ctx, job)
	if errors.Is(err, repository.ErrDuplicateIdempotencyKey) {
		existing, err := s.store.Jobs.GetByIdempotencyKey(ctx, key)
		if err != nil {
			return nil, false, err
		}
		if existing.RequestFingerprint != v.fingerprint {
			return nil, false, ErrIdempotencyKeyReused
		}
		return existing, true, nil
	}
	if err != nil {
		return nil, false, err
	}

	// MongoDB has the job, so the request has succeeded. If Redis is down,
	// the reconciler's overdue check adds the job later.
	if err := s.queue.Schedule(ctx, job.ID, job.NextRunAt); err != nil {
		s.logger.WarnContext(ctx, "redis schedule failed; the reconciler will repair it", "job_id", job.ID, "error", err)
	}
	return job, false, nil
}

// Get returns one job.
func (s *JobService) Get(ctx context.Context, id string) (*models.Job, error) {
	return s.store.Jobs.Get(ctx, id)
}

// ListParams are the filters for List.
type ListParams struct {
	Status        models.JobStatus
	JobType       string
	CreatedAfter  time.Time
	CreatedBefore time.Time
	Limit         int
	Cursor        string
}

// ListResult is one page of jobs.
type ListResult struct {
	Jobs       []models.Job `json:"jobs"`
	NextCursor string       `json:"next_cursor,omitempty"`
}

// ErrInvalidCursor is returned for a cursor this server did not produce.
var ErrInvalidCursor = &ValidationError{Details: []FieldIssue{{Field: "cursor", Issue: "is not a valid cursor"}}}

// List returns one page of jobs, newest first.
func (s *JobService) List(ctx context.Context, p ListParams) (ListResult, error) {
	limit := p.Limit
	if limit == 0 {
		limit = DefaultListLimit
	}
	filter := repository.ListFilter{
		Status: p.Status, JobType: p.JobType,
		CreatedAfter: p.CreatedAfter, CreatedBefore: p.CreatedBefore,
		Limit: limit + 1, // one extra row tells us whether another page exists
	}
	if p.Cursor != "" {
		c, err := decodeCursor(p.Cursor)
		if err != nil {
			return ListResult{}, ErrInvalidCursor
		}
		filter.After = c
	}

	jobs, err := s.store.Jobs.List(ctx, filter)
	if err != nil {
		return ListResult{}, err
	}
	res := ListResult{Jobs: jobs}
	if res.Jobs == nil {
		res.Jobs = []models.Job{}
	}
	if len(jobs) > limit {
		res.Jobs = jobs[:limit]
		last := res.Jobs[limit-1]
		res.NextCursor = encodeCursor(repository.Cursor{CreatedAt: last.CreatedAt, ID: last.ID})
	}
	return res, nil
}

// encodeCursor makes an opaque cursor: clients must not build or parse it,
// which leaves us free to change its contents later.
func encodeCursor(c repository.Cursor) string {
	b, _ := json.Marshal(c)
	return base64.RawURLEncoding.EncodeToString(b)
}

func decodeCursor(s string) (*repository.Cursor, error) {
	b, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return nil, err
	}
	var c repository.Cursor
	if err := json.Unmarshal(b, &c); err != nil || c.ID == "" {
		return nil, errors.New("bad cursor")
	}
	return &c, nil
}

// Cancel cancels a job. It returns the updated job and what happened.
func (s *JobService) Cancel(ctx context.Context, id string) (*models.Job, repository.CancelOutcome, error) {
	outcome, err := s.store.Jobs.Cancel(ctx, id, s.now())
	if err != nil {
		return nil, 0, err
	}
	if outcome == repository.Cancelled {
		// Best effort: a stale entry would be removed by the scheduler anyway.
		if err := s.queue.Remove(ctx, id); err != nil {
			s.logger.WarnContext(ctx, "remove cancelled job from redis", "job_id", id, "error", err)
		}
	}
	job, err := s.store.Jobs.Get(ctx, id)
	return job, outcome, err
}

// Retry restarts a dead-lettered job.
//
// This touches two collections without a transaction (MongoDB here is a
// standalone server; transactions need a replica set). The steps are
// ordered so that repeating the request after a crash converges:
//  1. resolve pending incidents as "retried" (repeatable)
//  2. move the job dead_lettered -> scheduled (conditional)
//  3. add it to Redis (repairable by the reconciler)
//
// A crash after step 1 leaves the job dead-lettered with no open incident;
// calling Retry again completes it.
func (s *JobService) Retry(ctx context.Context, id string) (*models.Job, error) {
	job, err := s.store.Jobs.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	if job.Status != models.StatusDeadLettered {
		return nil, ErrInvalidState
	}
	now := s.now()
	if err := s.store.DeadLetters.MarkRetried(ctx, id, "manual retry via API", now); err != nil {
		return nil, err
	}
	if err := s.store.Jobs.ManualRetry(ctx, id, now); err != nil {
		return nil, err
	}
	if err := s.queue.Schedule(ctx, id, now); err != nil {
		s.logger.WarnContext(ctx, "redis schedule failed; the reconciler will repair it", "job_id", id, "error", err)
	}
	return s.store.Jobs.Get(ctx, id)
}

// Executions returns a job's attempts, newest first.
func (s *JobService) Executions(ctx context.Context, id string, limit int) ([]models.Execution, error) {
	if _, err := s.store.Jobs.Get(ctx, id); err != nil {
		return nil, err
	}
	if limit == 0 {
		limit = DefaultListLimit
	}
	return s.store.Executions.ListByJob(ctx, id, limit)
}
