package locks_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/anushah13/distributed-job-scheduler/internal/locks"
	"github.com/anushah13/distributed-job-scheduler/internal/repository"
	"github.com/anushah13/distributed-job-scheduler/internal/testutil"
)

var fast = locks.Options{TTL: 600 * time.Millisecond, RenewEvery: 100 * time.Millisecond, MaxSilence: 400 * time.Millisecond}

func TestOnlyOneLeaderAtATimeAndFailover(t *testing.T) {
	rdb, prefix := testutil.Redis(t)
	store := repository.New(testutil.Mongo(t))
	logger := testutil.Logger()

	var leaders atomic.Int32
	var maxLeaders atomic.Int32
	var mu sync.Mutex
	var tokens []int64

	lead := func(ctx context.Context, token int64) {
		n := leaders.Add(1)
		for {
			m := maxLeaders.Load()
			if n <= m || maxLeaders.CompareAndSwap(m, n) {
				break
			}
		}
		mu.Lock()
		tokens = append(tokens, token)
		mu.Unlock()
		<-ctx.Done()
		leaders.Add(-1)
	}

	ctxA, stopA := context.WithCancel(context.Background())
	ctxB, stopB := context.WithCancel(context.Background())
	defer stopB()
	a := locks.NewElector(rdb, prefix+"leader", "a", store.Leases, logger, fast)
	b := locks.NewElector(rdb, prefix+"leader", "b", store.Leases, logger, fast)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); _ = a.Run(ctxA, lead) }()
	go func() { defer wg.Done(); _ = b.Run(ctxB, lead) }()

	testutil.Eventually(t, 3*time.Second, "a leader", func() bool { return a.IsLeader() || b.IsLeader() })
	time.Sleep(time.Second) // several renewal rounds

	// Stop whichever leads; the other must take over with a higher token.
	first, stopFirst, other := a, stopA, b
	if b.IsLeader() {
		first, stopFirst, other = b, stopB, a
	}
	stopFirst()
	testutil.Eventually(t, 3*time.Second, "failover", func() bool { return other.IsLeader() && !first.IsLeader() })

	stopA()
	stopB()
	wg.Wait()

	if maxLeaders.Load() != 1 {
		t.Errorf("%d leaders at the same time, want 1", maxLeaders.Load())
	}
	mu.Lock()
	defer mu.Unlock()
	if len(tokens) < 2 || tokens[1] <= tokens[0] {
		t.Errorf("fencing tokens %v must increase on each new leadership", tokens)
	}
}

func TestLeaderStepsDownWhenLeaseIsStolen(t *testing.T) {
	rdb, prefix := testutil.Redis(t)
	store := repository.New(testutil.Mongo(t))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	e := locks.NewElector(rdb, prefix+"leader", "a", store.Leases, testutil.Logger(), fast)
	stepped := make(chan struct{})
	go func() {
		_ = e.Run(ctx, func(ctx context.Context, _ int64) {
			<-ctx.Done()
			close(stepped)
			<-make(chan struct{}) // keep this goroutine from leading again in the test
		})
	}()
	testutil.Eventually(t, 3*time.Second, "leadership", e.IsLeader)

	// Simulate another instance owning the lease (for example after a pause).
	rdb.Set(context.Background(), prefix+"leader", "someone-else", time.Minute)

	select {
	case <-stepped:
	case <-time.After(2 * time.Second):
		t.Fatal("leader did not step down after losing the lease")
	}
}
