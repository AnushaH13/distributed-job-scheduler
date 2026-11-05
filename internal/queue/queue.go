// Package queue keeps the scheduler's timers and work list in Redis.
//
// Redis holds job IDs only, never payloads, and everything here can be
// rebuilt from MongoDB. Keys (with the default "js:" prefix):
//
//	js:delayed              sorted set: member = job ID, score = run time (Unix ms)
//	js:ready                list of job IDs that are due; workers pop from it
//	js:epoch                exists while Redis still has our data (wipe detection)
//	js:workers:heartbeats   sorted set: member = worker ID, score = last heartbeat (Unix ms)
//	js:worker:{id}          hash with worker details, expires if the worker stops
package queue

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

// DefaultPrefix is the key prefix used by the services.
const DefaultPrefix = "js:"

// moveScript moves one job from the delayed set to the ready list only if
// it was still in the delayed set. Running both commands in one Lua script
// makes the move atomic: two scheduler instances can never both push it.
var moveScript = goredis.NewScript(`
if redis.call('ZREM', KEYS[1], ARGV[1]) == 1 then
  redis.call('LPUSH', KEYS[2], ARGV[1])
  return 1
end
return 0
`)

// Queue wraps the Redis keys used for scheduling.
type Queue struct {
	rdb          *goredis.Client
	prefix       string
	delayed      string
	ready        string
	epoch        string
	heartbeats   string
	workerPrefix string
}

// New returns a Queue whose keys start with prefix.
func New(rdb *goredis.Client, prefix string) *Queue {
	return &Queue{
		rdb:          rdb,
		prefix:       prefix,
		delayed:      prefix + "delayed",
		ready:        prefix + "ready",
		epoch:        prefix + "epoch",
		heartbeats:   prefix + "workers:heartbeats",
		workerPrefix: prefix + "worker:",
	}
}

// Prefix returns the key prefix.
func (q *Queue) Prefix() string { return q.prefix }

// Schedule sets (or moves) a job's run time. ZADD is idempotent: calling it
// twice leaves one entry with the latest time.
func (q *Queue) Schedule(ctx context.Context, jobID string, at time.Time) error {
	err := q.rdb.ZAdd(ctx, q.delayed, goredis.Z{Score: float64(at.UnixMilli()), Member: jobID}).Err()
	if err != nil {
		return fmt.Errorf("schedule %s: %w", jobID, err)
	}
	return nil
}

// Remove deletes a job from the delayed set (for example after cancellation).
func (q *Queue) Remove(ctx context.Context, jobID string) error {
	if err := q.rdb.ZRem(ctx, q.delayed, jobID).Err(); err != nil {
		return fmt.Errorf("remove %s: %w", jobID, err)
	}
	return nil
}

// Due returns up to limit job IDs whose run time is at or before now, oldest first.
func (q *Queue) Due(ctx context.Context, now time.Time, limit int) ([]string, error) {
	ids, err := q.rdb.ZRangeArgs(ctx, goredis.ZRangeArgs{
		Key:     q.delayed,
		Start:   "-inf",
		Stop:    strconv.FormatInt(now.UnixMilli(), 10),
		ByScore: true,
		Count:   int64(limit),
	}).Result()
	if err != nil {
		return nil, fmt.Errorf("read due jobs: %w", err)
	}
	return ids, nil
}

// MoveToReady atomically moves a job from the delayed set to the ready list.
// It returns false when the job was no longer in the delayed set.
func (q *Queue) MoveToReady(ctx context.Context, jobID string) (bool, error) {
	n, err := moveScript.Run(ctx, q.rdb, []string{q.delayed, q.ready}, jobID).Int()
	if err != nil {
		return false, fmt.Errorf("move %s to ready: %w", jobID, err)
	}
	return n == 1, nil
}

// PushReady adds a job to the ready list directly. The reconciler uses it
// for queued jobs whose ID was lost. Duplicates are harmless: claiming is
// atomic in MongoDB, so only one worker can run the job.
func (q *Queue) PushReady(ctx context.Context, jobID string) error {
	if err := q.rdb.LPush(ctx, q.ready, jobID).Err(); err != nil {
		return fmt.Errorf("push %s to ready: %w", jobID, err)
	}
	return nil
}

// PopReady waits up to timeout for a ready job and returns its ID, or ""
// when none arrived in time.
func (q *Queue) PopReady(ctx context.Context, timeout time.Duration) (string, error) {
	res, err := q.rdb.BRPop(ctx, timeout, q.ready).Result()
	if errors.Is(err, goredis.Nil) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("pop ready job: %w", err)
	}
	return res[1], nil // res is [key, value]
}

// Lengths returns the sizes of the delayed set and the ready list.
func (q *Queue) Lengths(ctx context.Context) (delayed, ready int64, err error) {
	pipe := q.rdb.Pipeline()
	d := pipe.ZCard(ctx, q.delayed)
	r := pipe.LLen(ctx, q.ready)
	if _, err := pipe.Exec(ctx); err != nil {
		return 0, 0, fmt.Errorf("read queue lengths: %w", err)
	}
	return d.Val(), r.Val(), nil
}

// EpochExists reports whether the epoch marker is present. If Redis lost
// its data (restart without persistence, FLUSHALL), the marker is gone and
// the reconciler rebuilds everything from MongoDB.
func (q *Queue) EpochExists(ctx context.Context) (bool, error) {
	n, err := q.rdb.Exists(ctx, q.epoch).Result()
	if err != nil {
		return false, fmt.Errorf("check epoch: %w", err)
	}
	return n == 1, nil
}

// SetEpoch writes the epoch marker after a rebuild.
func (q *Queue) SetEpoch(ctx context.Context, value string) error {
	if err := q.rdb.Set(ctx, q.epoch, value, 0).Err(); err != nil {
		return fmt.Errorf("set epoch: %w", err)
	}
	return nil
}

// Heartbeat records that a worker is alive. This is for visibility only:
// correctness comes from job leases in MongoDB.
func (q *Queue) Heartbeat(ctx context.Context, workerID string, info map[string]any, ttl time.Duration) error {
	key := q.workerPrefix + workerID
	pipe := q.rdb.TxPipeline()
	pipe.ZAdd(ctx, q.heartbeats, goredis.Z{Score: float64(time.Now().UnixMilli()), Member: workerID})
	pipe.HSet(ctx, key, info)
	pipe.Expire(ctx, key, ttl)
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("worker heartbeat: %w", err)
	}
	return nil
}

// RemoveWorker deletes a worker's heartbeat on clean shutdown.
func (q *Queue) RemoveWorker(ctx context.Context, workerID string) error {
	pipe := q.rdb.TxPipeline()
	pipe.ZRem(ctx, q.heartbeats, workerID)
	pipe.Del(ctx, q.workerPrefix+workerID)
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("remove worker: %w", err)
	}
	return nil
}
