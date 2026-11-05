package queue_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/anushah13/distributed-job-scheduler/internal/queue"
	"github.com/anushah13/distributed-job-scheduler/internal/testutil"
)

func TestDueReturnsOnlyDueJobsInOrder(t *testing.T) {
	rdb, prefix := testutil.Redis(t)
	q, ctx := queue.New(rdb, prefix), context.Background()
	now := time.Now()

	_ = q.Schedule(ctx, "later", now.Add(time.Hour))
	_ = q.Schedule(ctx, "second", now.Add(-time.Second))
	_ = q.Schedule(ctx, "first", now.Add(-time.Minute))

	ids, err := q.Due(ctx, now, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 2 || ids[0] != "first" || ids[1] != "second" {
		t.Errorf("Due = %v, want [first second]", ids)
	}
}

func TestMoveToReadyHappensOnce(t *testing.T) {
	rdb, prefix := testutil.Redis(t)
	q, ctx := queue.New(rdb, prefix), context.Background()
	_ = q.Schedule(ctx, "job_1", time.Now())

	// Many schedulers racing: the Lua script lets exactly one move it.
	var moved atomic.Int32
	var wg sync.WaitGroup
	for range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if ok, err := q.MoveToReady(ctx, "job_1"); err == nil && ok {
				moved.Add(1)
			}
		}()
	}
	wg.Wait()
	if moved.Load() != 1 {
		t.Fatalf("moved %d times, want 1", moved.Load())
	}
	delayed, ready, _ := q.Lengths(ctx)
	if delayed != 0 || ready != 1 {
		t.Errorf("delayed=%d ready=%d, want 0 and 1", delayed, ready)
	}

	id, err := q.PopReady(ctx, time.Second)
	if err != nil || id != "job_1" {
		t.Errorf("PopReady = %q, %v", id, err)
	}
	id, err = q.PopReady(ctx, 100*time.Millisecond)
	if err != nil || id != "" {
		t.Errorf("empty PopReady = %q, %v; want \"\", nil", id, err)
	}
}

func TestScheduleIsIdempotent(t *testing.T) {
	rdb, prefix := testutil.Redis(t)
	q, ctx := queue.New(rdb, prefix), context.Background()
	for range 3 {
		_ = q.Schedule(ctx, "job_1", time.Now())
	}
	if delayed, _, _ := q.Lengths(ctx); delayed != 1 {
		t.Errorf("delayed = %d, want 1", delayed)
	}
}

func TestEpoch(t *testing.T) {
	rdb, prefix := testutil.Redis(t)
	q, ctx := queue.New(rdb, prefix), context.Background()
	if ok, _ := q.EpochExists(ctx); ok {
		t.Fatal("fresh prefix should have no epoch")
	}
	_ = q.SetEpoch(ctx, "now")
	if ok, _ := q.EpochExists(ctx); !ok {
		t.Error("epoch should exist after SetEpoch")
	}
}
