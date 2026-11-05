package repository_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/anushah13/distributed-job-scheduler/internal/idgen"
	"github.com/anushah13/distributed-job-scheduler/internal/models"
	"github.com/anushah13/distributed-job-scheduler/internal/repository"
	"github.com/anushah13/distributed-job-scheduler/internal/testutil"
)

func newStore(t *testing.T) *repository.Store {
	t.Helper()
	s := repository.New(testutil.Mongo(t))
	if err := s.EnsureIndexes(context.Background()); err != nil {
		t.Fatalf("ensure indexes: %v", err)
	}
	return s
}

func newJob(status models.JobStatus) *models.Job {
	now := time.Now().UTC().Truncate(time.Millisecond)
	return &models.Job{
		ID: idgen.New("job"), JobType: "echo", Payload: map[string]any{"n": 1, "nested": map[string]any{"a": "b"}},
		Schedule: models.ScheduleImmediate, MaxRetries: 3, TimeoutSeconds: 60,
		Status: status, ScheduledAt: now, NextRunAt: now, OccurrenceAt: now, CreatedAt: now, UpdatedAt: now,
	}
}

func TestIdempotencyKeyIsUnique(t *testing.T) {
	s, ctx := newStore(t), context.Background()

	a, b := newJob(models.StatusScheduled), newJob(models.StatusScheduled)
	a.IdempotencyKey, b.IdempotencyKey = "order-42", "order-42"
	if err := s.Jobs.Insert(ctx, a); err != nil {
		t.Fatal(err)
	}
	if err := s.Jobs.Insert(ctx, b); !errors.Is(err, repository.ErrDuplicateIdempotencyKey) {
		t.Fatalf("second insert: err = %v, want ErrDuplicateIdempotencyKey", err)
	}

	// Jobs without a key never collide (partial index + omitempty).
	for range 3 {
		if err := s.Jobs.Insert(ctx, newJob(models.StatusScheduled)); err != nil {
			t.Fatalf("insert without key: %v", err)
		}
	}
}

func TestPayloadRoundTripsAsMaps(t *testing.T) {
	s, ctx := newStore(t), context.Background()
	j := newJob(models.StatusScheduled)
	if err := s.Jobs.Insert(ctx, j); err != nil {
		t.Fatal(err)
	}
	got, err := s.Jobs.Get(ctx, j.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := got.Payload["nested"].(map[string]any); !ok {
		t.Errorf("nested payload decoded as %T, want map[string]any", got.Payload["nested"])
	}
	if _, err := s.Jobs.Get(ctx, "job_missing"); !errors.Is(err, repository.ErrNotFound) {
		t.Errorf("missing job: err = %v, want ErrNotFound", err)
	}
}

func TestOnlyOneWorkerWinsTheClaim(t *testing.T) {
	s, ctx := newStore(t), context.Background()
	j := newJob(models.StatusQueued)
	if err := s.Jobs.Insert(ctx, j); err != nil {
		t.Fatal(err)
	}

	var wins atomic.Int32
	var wg sync.WaitGroup
	for i := range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			got, err := s.Jobs.Claim(ctx, j.ID, "worker-"+string(rune('a'+i)), 30*time.Second)
			if err != nil {
				t.Errorf("claim: %v", err)
			}
			if got != nil {
				wins.Add(1)
			}
		}()
	}
	wg.Wait()
	if wins.Load() != 1 {
		t.Fatalf("%d workers claimed the job, want exactly 1", wins.Load())
	}
}

func TestClaimTokenFencesStaleWorkers(t *testing.T) {
	s, ctx := newStore(t), context.Background()
	j := newJob(models.StatusQueued)
	if err := s.Jobs.Insert(ctx, j); err != nil {
		t.Fatal(err)
	}
	claimed, err := s.Jobs.Claim(ctx, j.ID, "w1", 30*time.Second)
	if err != nil || claimed == nil {
		t.Fatalf("claim: %v %v", claimed, err)
	}
	if claimed.LeaseExpiresAt == nil || time.Until(*claimed.LeaseExpiresAt) < 20*time.Second {
		t.Errorf("lease_expires_at = %v, want about 30s from now", claimed.LeaseExpiresAt)
	}

	stale := repository.Claim{JobID: j.ID, Token: "not-the-token"}
	if err := s.Jobs.Complete(ctx, stale, time.Now()); !errors.Is(err, repository.ErrLeaseLost) {
		t.Errorf("complete with wrong token: err = %v, want ErrLeaseLost", err)
	}
	if _, err := s.Jobs.RenewLease(ctx, stale, time.Minute); !errors.Is(err, repository.ErrLeaseLost) {
		t.Errorf("renew with wrong token: err = %v, want ErrLeaseLost", err)
	}

	// The reconciler's Expired condition must not take a live lease.
	live := repository.Claim{JobID: j.ID, Token: claimed.ClaimToken, Expired: true}
	if err := s.Jobs.Retry(ctx, live, time.Now(), "x"); !errors.Is(err, repository.ErrLeaseLost) {
		t.Errorf("reconciler took a live lease: err = %v", err)
	}

	owner := repository.Claim{JobID: j.ID, Token: claimed.ClaimToken}
	if err := s.Jobs.Complete(ctx, owner, time.Now()); err != nil {
		t.Fatalf("complete: %v", err)
	}
	// A finished job cannot be finished again with the old token.
	if err := s.Jobs.Complete(ctx, owner, time.Now()); !errors.Is(err, repository.ErrLeaseLost) {
		t.Errorf("second complete: err = %v, want ErrLeaseLost", err)
	}
}

func TestExpiredLeaseIsFoundByServerTime(t *testing.T) {
	s, ctx := newStore(t), context.Background()
	j := newJob(models.StatusQueued)
	if err := s.Jobs.Insert(ctx, j); err != nil {
		t.Fatal(err)
	}
	claimed, err := s.Jobs.Claim(ctx, j.ID, "w1", 100*time.Millisecond)
	if err != nil || claimed == nil {
		t.Fatal(err)
	}
	time.Sleep(300 * time.Millisecond)

	expired, err := s.Jobs.ExpiredLeases(ctx, 10)
	if err != nil || len(expired) != 1 || expired[0].ID != j.ID {
		t.Fatalf("ExpiredLeases = %v, %v", expired, err)
	}
	claim := repository.Claim{JobID: j.ID, Token: expired[0].ClaimToken, Expired: true}
	if err := s.Jobs.Retry(ctx, claim, time.Now(), "lease expired"); err != nil {
		t.Fatalf("retry expired: %v", err)
	}
	got, _ := s.Jobs.Get(ctx, j.ID)
	if got.Status != models.StatusRetrying || got.RetryCount != 1 || got.ClaimToken != "" {
		t.Errorf("after retry: status=%s retry_count=%d token=%q", got.Status, got.RetryCount, got.ClaimToken)
	}
}

func TestMarkQueuedRespectsFencingToken(t *testing.T) {
	s, ctx := newStore(t), context.Background()
	j := newJob(models.StatusScheduled)
	if err := s.Jobs.Insert(ctx, j); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	if ok, err := s.Jobs.MarkQueued(ctx, j.ID, 5, now); err != nil || !ok {
		t.Fatalf("leader 5: ok=%v err=%v", ok, err)
	}

	// Put it back to scheduled (as a retry would), then try as an older leader.
	if _, err := s.Jobs.Claim(ctx, j.ID, "w", time.Minute); err != nil {
		t.Fatal(err)
	}
	got, _ := s.Jobs.Get(ctx, j.ID)
	if err := s.Jobs.Retry(ctx, repository.Claim{JobID: j.ID, Token: got.ClaimToken}, now, "x"); err != nil {
		t.Fatal(err)
	}
	if ok, _ := s.Jobs.MarkQueued(ctx, j.ID, 4, now); ok {
		t.Error("a stale leader (token 4) queued a job already touched by leader 5")
	}
	if ok, _ := s.Jobs.MarkQueued(ctx, j.ID, 6, now); !ok {
		t.Error("a newer leader (token 6) should be allowed")
	}
}

func TestCancelOutcomes(t *testing.T) {
	s, ctx := newStore(t), context.Background()
	now := time.Now()

	waiting := newJob(models.StatusScheduled)
	running := newJob(models.StatusRunning)
	done := newJob(models.StatusCompleted)
	for _, j := range []*models.Job{waiting, running, done} {
		if err := s.Jobs.Insert(ctx, j); err != nil {
			t.Fatal(err)
		}
	}

	check := func(id string, want repository.CancelOutcome, wantErr error) {
		t.Helper()
		got, err := s.Jobs.Cancel(ctx, id, now)
		if !errors.Is(err, wantErr) || (wantErr == nil && got != want) {
			t.Errorf("Cancel(%s) = %v, %v; want %v, %v", id, got, err, want, wantErr)
		}
	}
	check(waiting.ID, repository.Cancelled, nil)
	check(waiting.ID, repository.AlreadyCancelled, nil) // idempotent
	check(running.ID, repository.CancelRequested, nil)
	check(done.ID, 0, repository.ErrInvalidState)
	check("job_missing", 0, repository.ErrNotFound)
}

func TestListPaginatesWithoutGapsOrDuplicates(t *testing.T) {
	s, ctx := newStore(t), context.Background()
	base := time.Now().UTC().Truncate(time.Millisecond)
	for i := range 7 {
		j := newJob(models.StatusScheduled)
		// Three jobs share a timestamp, so _id must break the tie.
		j.CreatedAt = base.Add(time.Duration(i/3) * time.Second)
		if err := s.Jobs.Insert(ctx, j); err != nil {
			t.Fatal(err)
		}
	}

	seen := map[string]bool{}
	var after *repository.Cursor
	for page := 0; page < 10; page++ {
		jobs, err := s.Jobs.List(ctx, repository.ListFilter{Limit: 3, After: after})
		if err != nil {
			t.Fatal(err)
		}
		if len(jobs) == 0 {
			break
		}
		for _, j := range jobs {
			if seen[j.ID] {
				t.Fatalf("job %s returned twice", j.ID)
			}
			if j.Payload != nil {
				t.Error("list results must not include payloads")
			}
			seen[j.ID] = true
		}
		last := jobs[len(jobs)-1]
		after = &repository.Cursor{CreatedAt: last.CreatedAt, ID: last.ID}
	}
	if len(seen) != 7 {
		t.Errorf("saw %d jobs across pages, want 7", len(seen))
	}
}

func TestFencingTokensOnlyGrow(t *testing.T) {
	s, ctx := newStore(t), context.Background()
	var last int64
	for range 3 {
		tok, err := s.Leases.NextFencingToken(ctx, "sched-a")
		if err != nil {
			t.Fatal(err)
		}
		if tok <= last {
			t.Fatalf("token %d not greater than %d", tok, last)
		}
		last = tok
	}
}

func TestDeadLetterInsertIsIdempotent(t *testing.T) {
	s, ctx := newStore(t), context.Background()
	occ := time.Now().UTC().Truncate(time.Millisecond)
	for range 2 {
		err := s.DeadLetters.Insert(ctx, &models.DeadLetter{
			ID: idgen.New("dlq"), JobID: "job_1", OccurrenceAt: occ, ResolutionStatus: models.ResolutionPending,
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	pending, err := s.DeadLetters.ListPending(ctx, 10)
	if err != nil || len(pending) != 1 {
		t.Fatalf("pending = %d (%v), want 1", len(pending), err)
	}
}
