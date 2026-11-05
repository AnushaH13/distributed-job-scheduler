// Package tests holds end-to-end tests: the real service, scheduler and
// worker run in one process against the local MongoDB and Redis containers,
// with short timings so failure scenarios finish in seconds.
package tests

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/anushah13/distributed-job-scheduler/internal/handlers"
	"github.com/anushah13/distributed-job-scheduler/internal/idgen"
	"github.com/anushah13/distributed-job-scheduler/internal/lifecycle"
	"github.com/anushah13/distributed-job-scheduler/internal/models"
	"github.com/anushah13/distributed-job-scheduler/internal/queue"
	"github.com/anushah13/distributed-job-scheduler/internal/repository"
	"github.com/anushah13/distributed-job-scheduler/internal/scheduler"
	"github.com/anushah13/distributed-job-scheduler/internal/services"
	"github.com/anushah13/distributed-job-scheduler/internal/testutil"
	"github.com/anushah13/distributed-job-scheduler/internal/worker"
	"github.com/anushah13/distributed-job-scheduler/pkg/backoff"
)

type system struct {
	store *repository.Store
	queue *queue.Queue
	svc   *services.JobService
	lc    *lifecycle.Lifecycle
}

// start runs a scheduler leader and a worker until the test ends.
func start(t *testing.T, withWorker bool) *system {
	t.Helper()
	db := testutil.Mongo(t)
	rdb, prefix := testutil.Redis(t)
	logger := testutil.Logger()

	store := repository.New(db)
	if err := store.EnsureIndexes(context.Background()); err != nil {
		t.Fatal(err)
	}
	q := queue.New(rdb, prefix)
	registry := handlers.Default(logger)
	fastBackoff := backoff.Policy{Base: 20 * time.Millisecond, Cap: 100 * time.Millisecond}
	lc := lifecycle.New(store, q, fastBackoff, logger)

	sched := scheduler.New(store, q, lc, logger, scheduler.Options{
		Tick: 50 * time.Millisecond, ReconcileEvery: 200 * time.Millisecond,
		QueuedTooLong: time.Second, OverdueAfter: 500 * time.Millisecond,
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		sched.Lead(ctx, 1)
	}()
	if withWorker {
		w := worker.New(store, q, lc, registry, logger, worker.Options{
			ID: "test-worker", Concurrency: 3,
			LeaseDuration: time.Second, RenewEvery: 100 * time.Millisecond,
			PopTimeout: 200 * time.Millisecond, HeartbeatEvery: time.Second,
		})
		wdone := make(chan struct{})
		go func() {
			defer close(wdone)
			_ = w.Run(ctx, ctx)
		}()
		t.Cleanup(func() { <-wdone })
	}
	t.Cleanup(func() {
		cancel()
		<-done
	})

	return &system{store: store, queue: q, svc: services.NewJobService(store, q, registry.Has, logger), lc: lc}
}

func (s *system) create(t *testing.T, body string, key string) *models.Job {
	t.Helper()
	var req services.CreateRequest
	if err := json.Unmarshal([]byte(body), &req); err != nil {
		t.Fatal(err)
	}
	job, _, err := s.svc.Create(context.Background(), req, key)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	return job
}

func (s *system) waitStatus(t *testing.T, id string, want models.JobStatus) *models.Job {
	t.Helper()
	var job *models.Job
	testutil.Eventually(t, 10*time.Second, "job "+id+" to be "+string(want), func() bool {
		var err error
		job, err = s.store.Jobs.Get(context.Background(), id)
		return err == nil && job.Status == want
	})
	return job
}

func (s *system) executions(t *testing.T, id string) []models.Execution {
	t.Helper()
	execs, err := s.svc.Executions(context.Background(), id, 100)
	if err != nil {
		t.Fatal(err)
	}
	return execs
}

func TestImmediateJobCompletes(t *testing.T) {
	s := start(t, true)
	job := s.create(t, `{"job_type":"echo","payload":{"hello":"world"},"schedule":{"type":"immediate"}}`, "")

	done := s.waitStatus(t, job.ID, models.StatusCompleted)
	if done.CompletedAt == nil || done.LeaseOwner != "test-worker" {
		t.Errorf("completed job: completed_at=%v worker=%q", done.CompletedAt, done.LeaseOwner)
	}
	testutil.Eventually(t, 2*time.Second, "execution record", func() bool {
		execs := s.executions(t, job.ID)
		return len(execs) == 1 && execs[0].Status == models.ExecSucceeded
	})
}

func TestDelayedJobWaits(t *testing.T) {
	s := start(t, true)
	job := s.create(t, `{"job_type":"echo","schedule":{"type":"delayed","delay_seconds":1}}`, "")

	time.Sleep(500 * time.Millisecond)
	if got, _ := s.store.Jobs.Get(context.Background(), job.ID); got.Status != models.StatusScheduled {
		t.Fatalf("status after 0.5s = %s, want scheduled", got.Status)
	}
	s.waitStatus(t, job.ID, models.StatusCompleted)
}

func TestTransientFailuresRetryThenDeadLetter(t *testing.T) {
	s := start(t, true)
	job := s.create(t, `{"job_type":"fail","max_retries":2,"schedule":{"type":"immediate"}}`, "")

	dead := s.waitStatus(t, job.ID, models.StatusDeadLettered)
	if dead.RetryCount != 2 {
		t.Errorf("retry_count = %d, want 2", dead.RetryCount)
	}
	testutil.Eventually(t, 2*time.Second, "3 failed executions", func() bool {
		execs := s.executions(t, job.ID)
		return len(execs) == 3 && execs[0].AttemptNumber == 3 && execs[0].Status == models.ExecFailed
	})

	pending, err := s.store.DeadLetters.ListPending(context.Background(), 10)
	if err != nil || len(pending) != 1 {
		t.Fatalf("pending dead letters = %d (%v), want 1", len(pending), err)
	}
	if pending[0].Reason != models.ReasonMaxRetriesExceeded || pending[0].TotalAttempts != 3 {
		t.Errorf("dead letter = %+v", pending[0])
	}
}

func TestPermanentFailureSkipsRetries(t *testing.T) {
	s := start(t, true)
	job := s.create(t, `{"job_type":"fail","payload":{"permanent":true},"schedule":{"type":"immediate"}}`, "")

	dead := s.waitStatus(t, job.ID, models.StatusDeadLettered)
	if dead.RetryCount != 0 {
		t.Errorf("retry_count = %d, want 0 for a permanent error", dead.RetryCount)
	}
	pending, _ := s.store.DeadLetters.ListPending(context.Background(), 10)
	if len(pending) != 1 || pending[0].Reason != models.ReasonPermanentError {
		t.Errorf("dead letters = %+v", pending)
	}
}

func TestManualRetryRunsAgain(t *testing.T) {
	s := start(t, true)
	job := s.create(t, `{"job_type":"fail","max_retries":0,"schedule":{"type":"immediate"}}`, "")
	s.waitStatus(t, job.ID, models.StatusDeadLettered)

	if _, err := s.svc.Retry(context.Background(), job.ID); err != nil {
		t.Fatalf("retry: %v", err)
	}
	s.waitStatus(t, job.ID, models.StatusDeadLettered) // "fail" fails again
	testutil.Eventually(t, 2*time.Second, "second attempt recorded", func() bool {
		return len(s.executions(t, job.ID)) == 2
	})

	// Retrying a job that is not dead-lettered is a conflict.
	done := s.create(t, `{"job_type":"echo","schedule":{"type":"immediate"}}`, "")
	s.waitStatus(t, done.ID, models.StatusCompleted)
	if _, err := s.svc.Retry(context.Background(), done.ID); !errors.Is(err, services.ErrInvalidState) {
		t.Errorf("retry completed job: err = %v, want ErrInvalidState", err)
	}
}

func TestCancelRunningJobIsCooperative(t *testing.T) {
	s := start(t, true)
	job := s.create(t, `{"job_type":"sleep","payload":{"duration_ms":30000},"schedule":{"type":"immediate"}}`, "")
	s.waitStatus(t, job.ID, models.StatusRunning)

	_, outcome, err := s.svc.Cancel(context.Background(), job.ID)
	if err != nil || outcome != repository.CancelRequested {
		t.Fatalf("cancel: outcome=%v err=%v, want CancelRequested", outcome, err)
	}
	s.waitStatus(t, job.ID, models.StatusCancelled)
	testutil.Eventually(t, 2*time.Second, "cancelled execution", func() bool {
		execs := s.executions(t, job.ID)
		return len(execs) == 1 && execs[0].Status == models.ExecCancelled
	})
}

func TestCancelWaitingJob(t *testing.T) {
	s := start(t, true)
	job := s.create(t, `{"job_type":"echo","schedule":{"type":"delayed","delay_seconds":60}}`, "")
	if _, outcome, err := s.svc.Cancel(context.Background(), job.ID); err != nil || outcome != repository.Cancelled {
		t.Fatalf("cancel: %v %v", outcome, err)
	}
	if delayed, _, _ := s.queue.Lengths(context.Background()); delayed != 0 {
		t.Errorf("cancelled job still in the delayed set")
	}
}

func TestIdempotentCreate(t *testing.T) {
	s := start(t, false)
	body := `{"job_type":"echo","payload":{"order":42},"schedule":{"type":"delayed","delay_seconds":60}}`
	var req services.CreateRequest
	_ = json.Unmarshal([]byte(body), &req)
	ctx := context.Background()

	first, replayed, err := s.svc.Create(ctx, req, "order-42")
	if err != nil || replayed {
		t.Fatalf("first create: replayed=%v err=%v", replayed, err)
	}
	again, replayed, err := s.svc.Create(ctx, req, "order-42")
	if err != nil || !replayed || again.ID != first.ID {
		t.Fatalf("replay: id=%s replayed=%v err=%v", again.ID, replayed, err)
	}

	var different services.CreateRequest
	_ = json.Unmarshal([]byte(`{"job_type":"echo","payload":{"order":43},"schedule":{"type":"delayed","delay_seconds":60}}`), &different)
	if _, _, err := s.svc.Create(ctx, different, "order-42"); !errors.Is(err, services.ErrIdempotencyKeyReused) {
		t.Errorf("reused key: err = %v, want ErrIdempotencyKeyReused", err)
	}
}

func TestCrashedWorkerJobIsRecovered(t *testing.T) {
	s := start(t, true)
	ctx := context.Background()

	// Simulate a worker that claims a job and dies: no renewals, no result.
	job := s.create(t, `{"job_type":"echo","schedule":{"type":"delayed","delay_seconds":3600}}`, "")
	_ = s.queue.Remove(ctx, job.ID)
	forceQueued(t, s, job.ID)
	if c, err := s.store.Jobs.Claim(ctx, job.ID, "crashed-worker", 200*time.Millisecond); err != nil || c == nil {
		t.Fatalf("claim: %v", err)
	}

	// The reconciler sees the expired lease, counts the attempt, and the
	// real worker runs the retry.
	done := s.waitStatus(t, job.ID, models.StatusCompleted)
	if done.RetryCount != 1 || done.LeaseOwner != "test-worker" {
		t.Errorf("retry_count=%d worker=%q, want 1 and test-worker", done.RetryCount, done.LeaseOwner)
	}
}

func TestRedisDataLossIsRebuiltFromMongo(t *testing.T) {
	s := start(t, false)
	ctx := context.Background()
	job := s.create(t, `{"job_type":"echo","schedule":{"type":"delayed","delay_seconds":3600}}`, "")

	// Let the reconciler write the epoch, then simulate a Redis wipe.
	testutil.Eventually(t, 3*time.Second, "epoch", func() bool { ok, _ := s.queue.EpochExists(ctx); return ok })
	_ = s.queue.Remove(ctx, job.ID)
	wipeEpoch(t, s)

	testutil.Eventually(t, 3*time.Second, "rebuild", func() bool {
		delayed, _, _ := s.queue.Lengths(ctx)
		ok, _ := s.queue.EpochExists(ctx)
		return delayed == 1 && ok
	})
}

func TestRecurringJobReschedulesAfterSuccess(t *testing.T) {
	s := start(t, true)
	ctx := context.Background()

	// Insert directly with a due occurrence, instead of waiting for the next minute.
	now := time.Now().UTC().Truncate(time.Millisecond)
	job := &models.Job{
		ID: idgen.New("job"), JobType: "echo", Payload: map[string]any{},
		Schedule: models.ScheduleRecurring, CronExpression: "* * * * *", Timezone: "Asia/Kolkata",
		MaxRetries: 3, TimeoutSeconds: 60, Status: models.StatusScheduled,
		ScheduledAt: now, NextRunAt: now, OccurrenceAt: now, CreatedAt: now, UpdatedAt: now,
	}
	if err := s.store.Jobs.Insert(ctx, job); err != nil {
		t.Fatal(err)
	}
	_ = s.queue.Schedule(ctx, job.ID, now)

	testutil.Eventually(t, 5*time.Second, "next occurrence scheduled", func() bool {
		got, err := s.store.Jobs.Get(ctx, job.ID)
		return err == nil && got.Status == models.StatusScheduled && got.NextRunAt.After(now)
	})
	got, _ := s.store.Jobs.Get(ctx, job.ID)
	if got.NextRunAt.Second() != 0 || got.NextRunAt.Sub(now) > time.Minute {
		t.Errorf("next_run_at = %s, want the next whole minute after %s", got.NextRunAt, now)
	}
	if !got.OccurrenceAt.Equal(got.NextRunAt) || got.RetryCount != 0 {
		t.Errorf("occurrence_at=%s retry_count=%d", got.OccurrenceAt, got.RetryCount)
	}
}

// forceQueued moves a scheduled job to queued, as a leader would.
func forceQueued(t *testing.T, s *system, id string) {
	t.Helper()
	ok, err := s.store.Jobs.MarkQueued(context.Background(), id, 1, time.Now().Add(2*time.Hour))
	if err != nil || !ok {
		t.Fatalf("mark queued: ok=%v err=%v", ok, err)
	}
}

func wipeEpoch(t *testing.T, s *system) {
	t.Helper()
	// Queue has no method to delete the epoch on purpose, so the test opens
	// its own connection to the same Redis server and deletes the key.
	rdb, _ := testutil.Redis(t)
	if err := rdb.Del(context.Background(), s.queue.Prefix()+"epoch").Err(); err != nil {
		t.Fatal(err)
	}
}
