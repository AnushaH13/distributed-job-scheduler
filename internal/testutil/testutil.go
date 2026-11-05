// Package testutil gives integration tests isolated MongoDB databases and
// Redis key prefixes on the local containers (ports 27018 and 6380), and
// removes them afterwards. It is imported only from _test.go files.
package testutil

import (
	"context"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/anushah13/distributed-job-scheduler/internal/config"
	"github.com/anushah13/distributed-job-scheduler/internal/database/mongodb"
	"github.com/anushah13/distributed-job-scheduler/internal/database/redis"
	"github.com/anushah13/distributed-job-scheduler/internal/idgen"
)

// RequireIntegration skips the test unless DJS_INTEGRATION=1.
func RequireIntegration(t *testing.T) {
	t.Helper()
	if os.Getenv("DJS_INTEGRATION") != "1" {
		t.Skip("set DJS_INTEGRATION=1 and start deployments/compose.deps.yml to run")
	}
}

// Mongo returns a fresh database named djs_test_<random>, dropped when the test ends.
func Mongo(t *testing.T) *mongo.Database {
	t.Helper()
	RequireIntegration(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	name := "djs_test_" + idgen.Token()[:12]
	c, err := mongodb.Connect(ctx, config.MongoConfig{URI: config.DefaultMongoURI, Database: name}, "integration-test")
	if err != nil {
		t.Fatalf("connect to MongoDB: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := c.Database().Drop(ctx); err != nil {
			t.Errorf("drop %s: %v", name, err)
		}
		_ = c.Close(ctx)
	})
	return c.Database()
}

// Redis returns a client and a unique key prefix. Every key with that prefix
// is deleted when the test ends.
func Redis(t *testing.T) (*goredis.Client, string) {
	t.Helper()
	RequireIntegration(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	c, err := redis.Connect(ctx, config.RedisConfig{Addr: config.DefaultRedisAddr}, "integration-test")
	if err != nil {
		t.Fatalf("connect to Redis: %v", err)
	}
	prefix := "djs-test:" + idgen.Token()[:12] + ":"
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		// SCAN, never KEYS: KEYS blocks Redis while it walks every key.
		iter := c.Redis().Scan(ctx, 0, prefix+"*", 100).Iterator()
		for iter.Next(ctx) {
			c.Redis().Del(ctx, iter.Val())
		}
		if err := iter.Err(); err != nil {
			t.Errorf("clean up %s*: %v", prefix, err)
		}
		_ = c.Close()
	})
	return c.Redis(), prefix
}

// Logger returns a logger that discards output unless DJS_TEST_LOGS=1.
func Logger() *slog.Logger {
	if os.Getenv("DJS_TEST_LOGS") == "1" {
		return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
	}
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// Eventually polls cond until it returns true or the timeout passes.
func Eventually(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timed out after %s waiting for %s", timeout, what)
}
