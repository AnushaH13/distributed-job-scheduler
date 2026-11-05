package redis

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/anushah13/distributed-job-scheduler/internal/config"
)

// These tests need no database. Nothing listens on port 1, so every
// connection attempt is refused.

func TestConnectUnreachableFailsFast(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	cfg := config.RedisConfig{Addr: "127.0.0.1:1", Password: "supersecret"}

	start := time.Now()
	c, err := Connect(ctx, cfg, "test")
	elapsed := time.Since(start)

	if err == nil {
		_ = c.Close()
		t.Fatal("expected an error connecting to an unreachable server")
	}
	if elapsed > 3*time.Second {
		t.Errorf("took %s; it should fail quickly", elapsed)
	}
	if strings.Contains(err.Error(), "supersecret") {
		t.Errorf("error leaks the password: %v", err)
	}
	if !strings.Contains(err.Error(), "127.0.0.1:1") {
		t.Errorf("error should name the address, got: %v", err)
	}
}

// Integration tests need the containers from deployments/compose.deps.yml.
// They are skipped unless DJS_INTEGRATION=1.

func requireIntegration(t *testing.T) {
	t.Helper()
	if os.Getenv("DJS_INTEGRATION") != "1" {
		t.Skip("set DJS_INTEGRATION=1 and start deployments/compose.deps.yml to run")
	}
}

func TestIntegrationSetGet(t *testing.T) {
	requireIntegration(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	c, err := Connect(ctx, config.RedisConfig{Addr: config.DefaultRedisAddr}, "integration-test")
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	const key = "djs-test:client:hello"
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		c.Redis().Del(cleanupCtx, key)
	})

	if err := c.Redis().Set(ctx, key, "world", time.Minute).Err(); err != nil {
		t.Fatalf("set: %v", err)
	}
	got, err := c.Redis().Get(ctx, key).Result()
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got != "world" {
		t.Errorf("value = %q, want %q", got, "world")
	}

	// A missing key returns the sentinel error redis.Nil, found with errors.Is.
	if _, err := c.Redis().Get(ctx, "djs-test:client:missing").Result(); !errors.Is(err, goredis.Nil) {
		t.Errorf("missing key: err = %v, want redis.Nil", err)
	}
}

func TestIntegrationCloseStopsClient(t *testing.T) {
	requireIntegration(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	c, err := Connect(ctx, config.RedisConfig{Addr: config.DefaultRedisAddr}, "integration-test")
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := c.Ping(ctx); !errors.Is(err, goredis.ErrClosed) {
		t.Errorf("ping after close: err = %v, want redis.ErrClosed", err)
	}
}
