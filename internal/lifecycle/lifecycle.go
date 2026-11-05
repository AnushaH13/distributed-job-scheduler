// Package lifecycle decides what happens to a job after an attempt ends:
// complete, reschedule (recurring), retry with backoff, dead-letter, or cancel.
//
// The worker uses it after running a handler, and the reconciler uses it when
// a worker's lease expired (a crash counts as a failed attempt, which stops a
// job that crashes its worker from being retried forever).
//
// Write order is always MongoDB first, then Redis. If the Redis write fails,
// the job is still correct in MongoDB and the reconciler re-adds it to Redis.
package lifecycle

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/anushah13/distributed-job-scheduler/internal/idgen"
	"github.com/anushah13/distributed-job-scheduler/internal/models"
	"github.com/anushah13/distributed-job-scheduler/internal/queue"
	"github.com/anushah13/distributed-job-scheduler/internal/repository"
	"github.com/anushah13/distributed-job-scheduler/internal/schedule"
	"github.com/anushah13/distributed-job-scheduler/pkg/backoff"
)

// Outcome says what Fail decided.
type Outcome string

// Outcomes of Fail.
const (
	OutcomeRetrying     Outcome = "retrying"
	OutcomeDeadLettered Outcome = "dead_lettered"
)

// Lifecycle records attempt outcomes.
type Lifecycle struct {
	store   *repository.Store
	queue   *queue.Queue
	backoff backoff.Policy
	logger  *slog.Logger
}

// New returns a Lifecycle.
func New(store *repository.Store, q *queue.Queue, policy backoff.Policy, logger *slog.Logger) *Lifecycle {
	return &Lifecycle{store: store, queue: q, backoff: policy, logger: logger}
}

// Succeed completes a one-time job, or schedules the next occurrence of a
// recurring job.
func (l *Lifecycle) Succeed(ctx context.Context, job *models.Job, claim repository.Claim) error {
	now := time.Now().UTC()
	if job.Schedule != models.ScheduleRecurring {
		return l.store.Jobs.Complete(ctx, claim, now)
	}
	next, err := NextOccurrence(job, now)
	if err != nil {
		// The API validates cron expressions, so this means bad data.
		return fmt.Errorf("next occurrence of %s: %w", job.ID, err)
	}
	if err := l.store.Jobs.Reschedule(ctx, claim, next, ""); err != nil {
		return err
	}
	l.scheduleInRedis(ctx, job.ID, next)
	return nil
}

// Fail records a failed attempt. Permanent errors and exhausted retries go
// to the dead-letter queue; everything else is retried after a backoff.
func (l *Lifecycle) Fail(ctx context.Context, job *models.Job, claim repository.Claim,
	errMsg string, permanent bool, workerID string) (Outcome, error) {
	now := time.Now().UTC()
	attempt := job.Attempt()

	// max_retries = 3 means 4 attempts in total.
	if !permanent && attempt <= job.MaxRetries {
		next := now.Add(l.backoff.Delay(attempt))
		if err := l.store.Jobs.Retry(ctx, claim, next, errMsg); err != nil {
			return "", err
		}
		l.scheduleInRedis(ctx, job.ID, next)
		return OutcomeRetrying, nil
	}

	reason := models.ReasonMaxRetriesExceeded
	if permanent {
		reason = models.ReasonPermanentError
	}
	// The incident record is written first. If we crash before updating the
	// job, the reconciler retries the job and Insert ignores the duplicate;
	// an extra incident is better than a lost one.
	if err := l.store.DeadLetters.Insert(ctx, &models.DeadLetter{
		ID:               idgen.New("dlq"),
		JobID:            job.ID,
		JobType:          job.JobType,
		OccurrenceAt:     job.OccurrenceAt,
		Payload:          job.Payload,
		Reason:           reason,
		LastError:        errMsg,
		TotalAttempts:    attempt,
		LastWorkerID:     workerID,
		DeadLetteredAt:   now,
		ResolutionStatus: models.ResolutionPending,
	}); err != nil {
		return "", err
	}

	if job.Schedule == models.ScheduleRecurring {
		// A recurring job dead-letters this occurrence only and keeps its schedule.
		next, err := NextOccurrence(job, now)
		if err != nil {
			return "", fmt.Errorf("next occurrence of %s: %w", job.ID, err)
		}
		if err := l.store.Jobs.Reschedule(ctx, claim, next, errMsg); err != nil {
			return "", err
		}
		l.scheduleInRedis(ctx, job.ID, next)
		return OutcomeDeadLettered, nil
	}

	if err := l.store.Jobs.DeadLetter(ctx, claim, errMsg); err != nil {
		return "", err
	}
	return OutcomeDeadLettered, nil
}

// Cancel finishes a running job whose cancellation was requested.
func (l *Lifecycle) Cancel(ctx context.Context, claim repository.Claim) error {
	return l.store.Jobs.MarkCancelled(ctx, claim, time.Now().UTC())
}

func (l *Lifecycle) scheduleInRedis(ctx context.Context, jobID string, at time.Time) {
	if err := l.queue.Schedule(ctx, jobID, at); err != nil {
		l.logger.Warn("redis schedule failed; the reconciler will repair it", "job_id", jobID, "error", err)
	}
}

// NextOccurrence returns the first cron time after both now and the current
// occurrence, so a late run never schedules an occurrence in the past.
func NextOccurrence(job *models.Job, now time.Time) (time.Time, error) {
	c, err := schedule.ParseCron(job.CronExpression, job.Timezone)
	if err != nil {
		return time.Time{}, err
	}
	from := now
	if job.OccurrenceAt.After(from) {
		from = job.OccurrenceAt
	}
	return c.Next(from), nil
}
