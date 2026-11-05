// Package locks implements scheduler leader election with a Redis lease.
//
// Only one scheduler replica (the leader) moves due jobs and reconciles.
// Two defences protect against a leader that pauses (GC, VM freeze) and
// wakes up after its lease expired:
//
//  1. Self-check: the leader steps down if it has not renewed successfully
//     for MaxSilence, measured with Go's monotonic clock, well before the
//     Redis TTL runs out.
//  2. Fencing: every new leader gets a larger token from MongoDB, and job
//     writes require "stored token <= my token", so a stale leader's
//     writes are rejected on documents a newer leader has touched.
package locks

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

// renewScript extends the lease only if we still own it.
var renewScript = goredis.NewScript(`
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('PEXPIRE', KEYS[1], ARGV[2])
end
return 0
`)

// releaseScript deletes the lease only if we still own it, so a slow
// leader can never delete the lease of the leader that replaced it.
var releaseScript = goredis.NewScript(`
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0
`)

// TokenSource hands out fencing tokens. *repository.Leases implements it.
type TokenSource interface {
	NextFencingToken(ctx context.Context, owner string) (int64, error)
}

// Options configure an Elector. Zero values use the defaults from the design.
type Options struct {
	TTL        time.Duration // lease lifetime in Redis (default 15s)
	RenewEvery time.Duration // renewal and campaign interval (default 5s)
	MaxSilence time.Duration // step down if not renewed for this long (default 10s)
}

func (o Options) withDefaults() Options {
	if o.TTL == 0 {
		o.TTL = 15 * time.Second
	}
	if o.RenewEvery == 0 {
		o.RenewEvery = 5 * time.Second
	}
	if o.MaxSilence == 0 {
		o.MaxSilence = 10 * time.Second
	}
	return o
}

// Elector campaigns for leadership and runs a function while it leads.
type Elector struct {
	rdb    *goredis.Client
	key    string
	id     string
	tokens TokenSource
	logger *slog.Logger
	opts   Options
	leader atomic.Bool
}

// NewElector returns an Elector that competes for key as id.
func NewElector(rdb *goredis.Client, key, id string, tokens TokenSource, logger *slog.Logger, opts Options) *Elector {
	return &Elector{rdb: rdb, key: key, id: id, tokens: tokens, logger: logger, opts: opts.withDefaults()}
}

// IsLeader reports whether this instance currently leads.
func (e *Elector) IsLeader() bool { return e.leader.Load() }

// Run campaigns until ctx is cancelled. Each time it wins, it calls lead
// with a context that is cancelled when leadership ends, and waits for lead
// to return before campaigning again.
func (e *Elector) Run(ctx context.Context, lead func(ctx context.Context, fencingToken int64)) error {
	ticker := time.NewTicker(e.opts.RenewEvery)
	defer ticker.Stop()

	for {
		started := time.Now()
		ok, err := e.rdb.SetNX(ctx, e.key, e.id, e.opts.TTL).Result()
		switch {
		case ctx.Err() != nil:
			return nil
		case err != nil:
			e.logger.Warn("leader election: cannot reach Redis", "error", err)
		case ok:
			e.leadWhileOwner(ctx, started, lead)
			if ctx.Err() != nil {
				return nil
			}
		}

		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

func (e *Elector) leadWhileOwner(ctx context.Context, acquiredAt time.Time, lead func(context.Context, int64)) {
	token, err := e.tokens.NextFencingToken(ctx, e.id)
	if err != nil {
		e.logger.Warn("leader election: cannot get fencing token; releasing lease", "error", err)
		e.release()
		return
	}

	e.leader.Store(true)
	defer e.leader.Store(false)
	e.logger.Info("became scheduler leader", "fencing_token", token)

	leaderCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		lead(leaderCtx, token)
	}()

	lastRenewed := acquiredAt
	ticker := time.NewTicker(e.opts.RenewEvery)
	defer ticker.Stop()

loop:
	for {
		select {
		case <-ctx.Done():
			break loop
		case <-done:
			break loop
		case <-ticker.C:
			// time.Since uses the monotonic clock, so wall-clock jumps
			// (NTP corrections) cannot fool this check.
			if time.Since(lastRenewed) > e.opts.MaxSilence {
				e.logger.Warn("stepping down: lease not renewed in time", "since_last_renewal", time.Since(lastRenewed))
				break loop
			}
			sent := time.Now()
			renewed, err := e.renew(ctx)
			if err != nil {
				e.logger.Warn("leader lease renewal failed", "error", err)
				continue // the self-check above decides when to give up
			}
			if !renewed {
				e.logger.Warn("lost scheduler leadership: lease owned by another instance")
				break loop
			}
			lastRenewed = sent // measure from when we asked, to be conservative
		}
	}

	cancel()
	<-done
	e.release()
	e.logger.Info("stopped leading", "fencing_token", token)
}

func (e *Elector) renew(ctx context.Context) (bool, error) {
	n, err := renewScript.Run(ctx, e.rdb, []string{e.key}, e.id, e.opts.TTL.Milliseconds()).Int()
	if err != nil {
		return false, fmt.Errorf("renew lease: %w", err)
	}
	return n == 1, nil
}

// release uses its own context: it must run even while shutting down.
func (e *Elector) release() {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := releaseScript.Run(ctx, e.rdb, []string{e.key}, e.id).Err(); err != nil && !errors.Is(err, goredis.Nil) {
		e.logger.Warn("release leader lease", "error", err)
	}
}
