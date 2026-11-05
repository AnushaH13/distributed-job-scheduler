// Package scheduler contains the work the leader scheduler does: moving due
// jobs from Redis's delayed set to the ready list, and reconciling MongoDB
// (the source of truth) with Redis.
package scheduler

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/anushah13/distributed-job-scheduler/internal/lifecycle"
	"github.com/anushah13/distributed-job-scheduler/internal/models"
	"github.com/anushah13/distributed-job-scheduler/internal/queue"
	"github.com/anushah13/distributed-job-scheduler/internal/repository"
)

// Options configure the scheduler. Zero values use the defaults from the design.
type Options struct {
	Tick           time.Duration // how often due jobs are moved (default 1s)
	ReconcileEvery time.Duration // how often the reconciler runs (default 5s)
	QueuedTooLong  time.Duration // re-push queued jobs older than this (default 60s)
	OverdueAfter   time.Duration // re-add waiting jobs this far past due (default 10s)
	BatchSize      int           // jobs per batch (default 100)
}

func (o Options) withDefaults() Options {
	if o.Tick == 0 {
		o.Tick = time.Second
	}
	if o.ReconcileEvery == 0 {
		o.ReconcileEvery = 5 * time.Second
	}
	if o.QueuedTooLong == 0 {
		o.QueuedTooLong = 60 * time.Second
	}
	if o.OverdueAfter == 0 {
		o.OverdueAfter = 10 * time.Second
	}
	if o.BatchSize == 0 {
		o.BatchSize = 100
	}
	return o
}

// Scheduler is the leader's work loop.
type Scheduler struct {
	store     *repository.Store
	queue     *queue.Queue
	lifecycle *lifecycle.Lifecycle
	logger    *slog.Logger
	opts      Options
}

// New returns a Scheduler.
func New(store *repository.Store, q *queue.Queue, lc *lifecycle.Lifecycle, logger *slog.Logger, opts Options) *Scheduler {
	return &Scheduler{store: store, queue: q, lifecycle: lc, logger: logger, opts: opts.withDefaults()}
}

// Lead runs until ctx is cancelled (leadership lost or shutdown). It is
// passed to locks.Elector.Run.
func (s *Scheduler) Lead(ctx context.Context, fencingToken int64) {
	tick := time.NewTicker(s.opts.Tick)
	defer tick.Stop()
	reconcile := time.NewTicker(s.opts.ReconcileEvery)
	defer reconcile.Stop()

	s.Reconcile(ctx) // repair anything a previous leader left behind
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			if _, err := s.MoveDue(ctx, fencingToken); err != nil && ctx.Err() == nil {
				s.logger.Warn("move due jobs", "error", err)
			}
		case <-reconcile.C:
			s.Reconcile(ctx)
		}
	}
}

// MoveDue moves one batch of due jobs to the ready list and returns how many
// it moved. Order matters: MongoDB is updated to "queued" first (fenced),
// then Redis. A crash in between leaves a queued job that is not in the
// ready list, which the reconciler's "queued too long" check repairs.
func (s *Scheduler) MoveDue(ctx context.Context, fencingToken int64) (int, error) {
	now := time.Now().UTC()
	ids, err := s.queue.Due(ctx, now, s.opts.BatchSize)
	if err != nil {
		return 0, err
	}

	moved := 0
	for _, id := range ids {
		ok, err := s.store.Jobs.MarkQueued(ctx, id, fencingToken, now)
		if err != nil {
			return moved, err
		}
		if ok {
			if _, err := s.queue.MoveToReady(ctx, id); err != nil {
				return moved, err
			}
			moved++
			continue
		}
		s.repairStaleEntry(ctx, id, now)
	}
	return moved, nil
}

// repairStaleEntry handles a delayed-set member whose job could not be
// queued: cancelled, finished, unknown, or scheduled for a later time.
func (s *Scheduler) repairStaleEntry(ctx context.Context, id string, now time.Time) {
	job, err := s.store.Jobs.Get(ctx, id)
	switch {
	case errors.Is(err, repository.ErrNotFound):
		_ = s.queue.Remove(ctx, id)
	case err != nil:
		s.logger.Warn("inspect stale queue entry", "job_id", id, "error", err)
	case job.Status == models.StatusScheduled || job.Status == models.StatusRetrying:
		if job.NextRunAt.After(now) {
			_ = s.queue.Schedule(ctx, id, job.NextRunAt) // fix an outdated score
		}
		// Otherwise a newer leader's fencing token blocked us; leave it to them.
	default:
		_ = s.queue.Remove(ctx, id) // not waiting any more
	}
}

// Reconcile repairs every known way MongoDB and Redis can disagree. Each
// step logs its own errors so one failure does not block the others.
func (s *Scheduler) Reconcile(ctx context.Context) {
	steps := []struct {
		name string
		fn   func(context.Context) (int, error)
	}{
		{"rebuild after redis data loss", s.rebuildIfWiped},
		{"recover expired leases", s.recoverExpiredLeases},
		{"re-push stuck queued jobs", s.repushStuckQueued},
		{"re-add overdue jobs", s.readdOverdue},
	}
	for _, step := range steps {
		n, err := step.fn(ctx)
		if err != nil && ctx.Err() == nil {
			s.logger.Warn("reconcile", "step", step.name, "error", err)
		}
		if n > 0 {
			s.logger.Info("reconcile", "step", step.name, "jobs", n)
		}
	}
}

func (s *Scheduler) rebuildIfWiped(ctx context.Context) (int, error) {
	exists, err := s.queue.EpochExists(ctx)
	if err != nil || exists {
		return 0, err
	}
	n := 0
	err = s.store.Jobs.EachPending(ctx, func(job models.Job) error {
		n++
		if job.Status == models.StatusQueued {
			return s.queue.PushReady(ctx, job.ID)
		}
		return s.queue.Schedule(ctx, job.ID, job.NextRunAt)
	})
	if err != nil {
		return n, err
	}
	return n, s.queue.SetEpoch(ctx, time.Now().UTC().Format(time.RFC3339))
}

// recoverExpiredLeases treats an expired lease as a failed attempt.
func (s *Scheduler) recoverExpiredLeases(ctx context.Context) (int, error) {
	jobs, err := s.store.Jobs.ExpiredLeases(ctx, s.opts.BatchSize)
	if err != nil {
		return 0, err
	}
	n := 0
	for i := range jobs {
		job := &jobs[i]
		claim := repository.Claim{JobID: job.ID, Token: job.ClaimToken, Expired: true}
		now := time.Now().UTC()

		if err := s.store.Executions.Abandon(ctx, job.ID, job.ClaimToken, now); err != nil {
			return n, err
		}
		if job.CancelRequested {
			err = s.lifecycle.Cancel(ctx, claim)
		} else {
			_, err = s.lifecycle.Fail(ctx, job, claim, "lease expired: worker stopped renewing", false, job.LeaseOwner)
		}
		if errors.Is(err, repository.ErrLeaseLost) {
			continue // the worker renewed or finished in the meantime
		}
		if err != nil {
			return n, err
		}
		n++
	}
	return n, nil
}

func (s *Scheduler) repushStuckQueued(ctx context.Context) (int, error) {
	jobs, err := s.store.Jobs.StuckQueued(ctx, time.Now().UTC().Add(-s.opts.QueuedTooLong), s.opts.BatchSize)
	if err != nil {
		return 0, err
	}
	for _, job := range jobs {
		if err := s.queue.PushReady(ctx, job.ID); err != nil {
			return 0, err
		}
		if err := s.store.Jobs.TouchQueued(ctx, job.ID); err != nil {
			return 0, err
		}
	}
	return len(jobs), nil
}

func (s *Scheduler) readdOverdue(ctx context.Context) (int, error) {
	jobs, err := s.store.Jobs.Overdue(ctx, time.Now().UTC().Add(-s.opts.OverdueAfter), s.opts.BatchSize)
	if err != nil {
		return 0, err
	}
	for _, job := range jobs {
		if err := s.queue.Schedule(ctx, job.ID, job.NextRunAt); err != nil {
			return 0, err
		}
	}
	return len(jobs), nil
}
