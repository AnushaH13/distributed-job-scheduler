// Package worker runs jobs: it pops ready job IDs from Redis, claims them
// atomically in MongoDB, runs the registered handler with a bounded number
// of goroutines, renews the job lease while it runs, and records the result.
package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/anushah13/distributed-job-scheduler/internal/handlers"
	"github.com/anushah13/distributed-job-scheduler/internal/idgen"
	"github.com/anushah13/distributed-job-scheduler/internal/lifecycle"
	"github.com/anushah13/distributed-job-scheduler/internal/models"
	"github.com/anushah13/distributed-job-scheduler/internal/queue"
	"github.com/anushah13/distributed-job-scheduler/internal/repository"
)

// Options configure a Worker. Zero values use the defaults from the design.
type Options struct {
	ID             string        // unique worker ID (default hostname + random suffix)
	Concurrency    int           // jobs run at the same time (default 4)
	LeaseDuration  time.Duration // job lease length (default 30s)
	RenewEvery     time.Duration // lease renewal interval (default 10s)
	PopTimeout     time.Duration // how long one BRPOP waits (default 2s)
	HeartbeatEvery time.Duration // Redis heartbeat interval (default 10s)
}

func (o Options) withDefaults() Options {
	if o.ID == "" {
		host, _ := os.Hostname()
		o.ID = fmt.Sprintf("%s-%s", host, idgen.Token()[:8])
	}
	if o.Concurrency == 0 {
		o.Concurrency = 4
	}
	if o.LeaseDuration == 0 {
		o.LeaseDuration = 30 * time.Second
	}
	if o.RenewEvery == 0 {
		o.RenewEvery = 10 * time.Second
	}
	if o.PopTimeout == 0 {
		o.PopTimeout = 2 * time.Second
	}
	if o.HeartbeatEvery == 0 {
		o.HeartbeatEvery = 10 * time.Second
	}
	return o
}

// recordTimeout bounds each MongoDB write that records a result. Results are
// recorded with a fresh context so they are saved even during shutdown.
const recordTimeout = 10 * time.Second

var errJobTimeout = errors.New("job exceeded its timeout")

// Worker executes jobs.
type Worker struct {
	store     *repository.Store
	queue     *queue.Queue
	lifecycle *lifecycle.Lifecycle
	registry  *handlers.Registry
	logger    *slog.Logger
	opts      Options
	active    atomic.Int64
}

// New returns a Worker.
func New(store *repository.Store, q *queue.Queue, lc *lifecycle.Lifecycle,
	registry *handlers.Registry, logger *slog.Logger, opts Options) *Worker {
	opts = opts.withDefaults()
	return &Worker{
		store: store, queue: q, lifecycle: lc, registry: registry,
		logger: logger.With("worker_id", opts.ID), opts: opts,
	}
}

// ID returns the worker's ID.
func (w *Worker) ID() string { return w.opts.ID }

// Active returns how many jobs are running now.
func (w *Worker) Active() int64 { return w.active.Load() }

// Run fetches and runs jobs until fetchCtx is cancelled, then waits for
// running jobs to finish. Running jobs see execCtx: cancelling it (after the
// shutdown grace period) asks handlers to stop early.
//
// Each of the Concurrency goroutines pops a job only when it is free, so the
// worker never takes more work than it can run (natural backpressure).
func (w *Worker) Run(fetchCtx, execCtx context.Context) error {
	var wg sync.WaitGroup // classic Add/Done: WaitGroup.Go needs Go 1.25

	wg.Add(1)
	go func() {
		defer wg.Done()
		w.heartbeatLoop(fetchCtx)
	}()

	for range w.opts.Concurrency {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for fetchCtx.Err() == nil {
				w.fetchAndRun(fetchCtx, execCtx)
			}
		}()
	}

	w.logger.Info("worker started", "concurrency", w.opts.Concurrency)
	wg.Wait()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := w.queue.RemoveWorker(ctx, w.opts.ID); err != nil {
		w.logger.Warn("remove worker heartbeat", "error", err)
	}
	w.logger.Info("worker stopped")
	return nil
}

func (w *Worker) fetchAndRun(fetchCtx, execCtx context.Context) {
	id, err := w.queue.PopReady(fetchCtx, w.opts.PopTimeout)
	if err != nil {
		if fetchCtx.Err() == nil {
			w.logger.Warn("pop ready job", "error", err)
			sleep(fetchCtx, time.Second) // don't spin while Redis is down
		}
		return
	}
	if id == "" {
		return // nothing ready
	}

	claimCtx, cancel := context.WithTimeout(context.Background(), recordTimeout)
	job, err := w.store.Jobs.Claim(claimCtx, id, w.opts.ID, w.opts.LeaseDuration)
	cancel()
	if err != nil {
		// The job stays "queued"; the reconciler re-pushes it later.
		w.logger.Warn("claim job", "job_id", id, "error", err)
		return
	}
	if job == nil {
		return // already claimed by another worker, or cancelled
	}

	w.active.Add(1)
	defer w.active.Add(-1)
	w.execute(execCtx, job)
}

// execute runs one claimed job and records what happened.
func (w *Worker) execute(execCtx context.Context, job *models.Job) {
	log := w.logger.With("job_id", job.ID, "job_type", job.JobType, "attempt", job.Attempt())
	claim := repository.Claim{JobID: job.ID, Token: job.ClaimToken}
	started := time.Now().UTC()

	exec := &models.Execution{
		ID:            idgen.New("exec"),
		JobID:         job.ID,
		JobType:       job.JobType,
		OccurrenceAt:  job.OccurrenceAt,
		AttemptNumber: job.Attempt(),
		WorkerID:      w.opts.ID,
		ClaimToken:    job.ClaimToken,
		StartedAt:     started,
		Status:        models.ExecRunning,
	}
	w.record(func(ctx context.Context) error { return w.store.Executions.Start(ctx, exec) }, log, "start execution")

	runCtx, cancelRun := context.WithCancelCause(execCtx)
	defer cancelRun(nil)
	runCtx, cancelTimeout := context.WithTimeoutCause(runCtx, time.Duration(job.TimeoutSeconds)*time.Second, errJobTimeout)
	defer cancelTimeout()

	var leaseLost, cancelRequested atomic.Bool
	stopRenew := make(chan struct{})
	renewDone := make(chan struct{})
	go func() {
		defer close(renewDone)
		w.renewLoop(claim, stopRenew, &leaseLost, &cancelRequested, cancelRun, log)
	}()

	log.Info("job started")
	err := runHandler(runCtx, w.registry, job)
	close(stopRenew)
	<-renewDone

	finished := time.Now().UTC()
	finish := func(status models.ExecutionStatus, msg string, errType models.ErrorType) {
		w.record(func(ctx context.Context) error {
			return w.store.Executions.Finish(ctx, exec.ID, status, started, finished, msg, errType)
		}, log, "finish execution")
	}

	switch {
	case leaseLost.Load():
		// Someone else may own the job now; recording anything on it would be wrong.
		log.Warn("job lease lost; result not recorded", "error", err)
		finish(models.ExecAbandoned, "lease lost", models.ErrorTransient)

	case cancelRequested.Load():
		w.recordJob(log, func(ctx context.Context) error { return w.lifecycle.Cancel(ctx, claim) }, "cancel job")
		finish(models.ExecCancelled, "cancelled by request", "")
		log.Info("job cancelled")

	case err == nil:
		w.recordJob(log, func(ctx context.Context) error { return w.lifecycle.Succeed(ctx, job, claim) }, "complete job")
		finish(models.ExecSucceeded, "", "")
		log.Info("job succeeded", "duration", finished.Sub(started))

	default:
		status, msg := models.ExecFailed, err.Error()
		permanent := handlers.IsPermanent(err)
		switch {
		case errors.Is(context.Cause(runCtx), errJobTimeout):
			status, msg = models.ExecTimedOut, fmt.Sprintf("timed out after %ds", job.TimeoutSeconds)
		case execCtx.Err() != nil:
			msg = "worker shut down before the job finished"
		}
		errType := models.ErrorTransient
		if permanent {
			errType = models.ErrorPermanent
		}

		var outcome lifecycle.Outcome
		w.recordJob(log, func(ctx context.Context) error {
			var err error
			outcome, err = w.lifecycle.Fail(ctx, job, claim, msg, permanent, w.opts.ID)
			return err
		}, "record failure")
		finish(status, msg, errType)
		log.Warn("job failed", "error", msg, "error_type", errType, "outcome", outcome)
	}
}

// renewLoop extends the lease until stop is closed. It cancels the job when
// the lease is lost, when renewals keep failing for too long (self-stop
// before the lease could expire), or when cancellation was requested.
func (w *Worker) renewLoop(claim repository.Claim, stop <-chan struct{},
	leaseLost, cancelRequested *atomic.Bool, cancelRun context.CancelCauseFunc, log *slog.Logger) {
	ticker := time.NewTicker(w.opts.RenewEvery)
	defer ticker.Stop()
	lastRenewed := time.Now()
	// Stop one renewal interval before the lease would expire.
	maxSilence := w.opts.LeaseDuration - w.opts.RenewEvery

	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
		}

		sent := time.Now()
		ctx, cancel := context.WithTimeout(context.Background(), w.opts.RenewEvery)
		cancelReq, err := w.store.Jobs.RenewLease(ctx, claim, w.opts.LeaseDuration)
		cancel()

		switch {
		case errors.Is(err, repository.ErrLeaseLost):
			leaseLost.Store(true)
			cancelRun(repository.ErrLeaseLost)
			return
		case err != nil:
			log.Warn("renew job lease", "error", err)
			if time.Since(lastRenewed) > maxSilence {
				log.Error("cannot renew job lease; stopping the job before the lease expires")
				leaseLost.Store(true)
				cancelRun(repository.ErrLeaseLost)
				return
			}
		default:
			lastRenewed = sent
			if cancelReq && !cancelRequested.Load() {
				cancelRequested.Store(true)
				cancelRun(errors.New("cancellation requested"))
			}
		}
	}
}

// runHandler runs the job's handler and turns a panic into a permanent error,
// so one bad job cannot crash the worker and every job running on it.
func runHandler(ctx context.Context, registry *handlers.Registry, job *models.Job) (err error) {
	h, ok := registry.Get(job.JobType)
	if !ok {
		return handlers.Permanent(fmt.Errorf("no handler registered for job type %q", job.JobType))
	}
	defer func() {
		if r := recover(); r != nil {
			err = handlers.Permanent(fmt.Errorf("handler panicked: %v", r))
		}
	}()
	return h(ctx, job)
}

// recordJob applies a job state change. ErrLeaseLost is expected when the
// reconciler already took the job over, so it is logged at a lower level.
func (w *Worker) recordJob(log *slog.Logger, fn func(context.Context) error, what string) {
	ctx, cancel := context.WithTimeout(context.Background(), recordTimeout)
	defer cancel()
	err := fn(ctx)
	switch {
	case errors.Is(err, repository.ErrLeaseLost):
		log.Warn(what+": lease already lost; the reconciler owns the job", "error", err)
	case err != nil:
		// The job stays "running" until its lease expires; the reconciler
		// then retries it (at-least-once).
		log.Error(what, "error", err)
	}
}

func (w *Worker) record(fn func(context.Context) error, log *slog.Logger, what string) {
	ctx, cancel := context.WithTimeout(context.Background(), recordTimeout)
	defer cancel()
	if err := fn(ctx); err != nil {
		log.Warn(what, "error", err)
	}
}

func (w *Worker) heartbeatLoop(ctx context.Context) {
	host, _ := os.Hostname()
	ticker := time.NewTicker(w.opts.HeartbeatEvery)
	defer ticker.Stop()
	for {
		info := map[string]any{
			"hostname":    host,
			"capacity":    w.opts.Concurrency,
			"active_jobs": w.Active(),
			"updated_at":  time.Now().UTC().Format(time.RFC3339),
		}
		if err := w.queue.Heartbeat(ctx, w.opts.ID, info, 3*w.opts.HeartbeatEvery); err != nil && ctx.Err() == nil {
			w.logger.Warn("worker heartbeat", "error", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func sleep(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}
