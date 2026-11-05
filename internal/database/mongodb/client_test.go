package mongodb

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/anushah13/distributed-job-scheduler/internal/config"
)

// These tests need no database. Nothing listens on port 1, so every
// connection attempt is refused.

func TestConnectUnreachableFailsFast(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	cfg := config.MongoConfig{
		URI:      "mongodb://admin:supersecret@127.0.0.1:1/",
		Database: "job_scheduler",
	}

	start := time.Now()
	c, err := Connect(ctx, cfg, "test")
	elapsed := time.Since(start)

	if err == nil {
		_ = c.Close(context.Background())
		t.Fatal("expected an error connecting to an unreachable server")
	}
	// 1s deadline plus up to 5s cleanup would be the worst case; a refused
	// connection should be far quicker than that.
	if elapsed > 3*time.Second {
		t.Errorf("took %s; the context deadline should stop it after about 1s", elapsed)
	}
	if strings.Contains(err.Error(), "supersecret") {
		t.Errorf("error leaks the password: %v", err)
	}
	// The error should say where we tried to connect, with the password redacted.
	if !strings.Contains(err.Error(), "admin:xxxxx@127.0.0.1:1") {
		t.Errorf("error should name the redacted URI, got: %v", err)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("error should wrap context.DeadlineExceeded, got: %v", err)
	}
}

func TestConnectInvalidURIDoesNotLeakPassword(t *testing.T) {
	cfg := config.MongoConfig{
		URI:      "mongodb://admin:supersecret@localhost:notaport/",
		Database: "job_scheduler",
	}

	c, err := Connect(context.Background(), cfg, "test")
	if err == nil {
		_ = c.Close(context.Background())
		t.Fatal("expected an error for an invalid URI")
	}
	if strings.Contains(err.Error(), "supersecret") {
		t.Errorf("error leaks the password: %v", err)
	}
	if !strings.HasPrefix(err.Error(), "create client for ") {
		t.Errorf("error should come from mongo.Connect, got: %v", err)
	}
}

// Integration tests need the containers from deployments/compose.deps.yml.
// They are skipped unless DJS_INTEGRATION=1, so `go test ./...` stays fast
// and works without Docker.

func requireIntegration(t *testing.T) {
	t.Helper()
	if os.Getenv("DJS_INTEGRATION") != "1" {
		t.Skip("set DJS_INTEGRATION=1 and start deployments/compose.deps.yml to run")
	}
}

func TestIntegrationConnectWriteRead(t *testing.T) {
	requireIntegration(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// A separate database, dropped at the end, so tests never touch real data.
	cfg := config.MongoConfig{URI: config.DefaultMongoURI, Database: "djs_integration_test"}

	c, err := Connect(ctx, cfg, "integration-test")
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := c.Database().Drop(cleanupCtx); err != nil {
			t.Errorf("drop test database: %v", err)
		}
		if err := c.Close(cleanupCtx); err != nil {
			t.Errorf("close: %v", err)
		}
	})

	coll := c.Database().Collection("ping")
	if _, err := coll.InsertOne(ctx, bson.M{"_id": "hello", "value": "world"}); err != nil {
		t.Fatalf("insert: %v", err)
	}

	var got struct {
		Value string `bson:"value"`
	}
	if err := coll.FindOne(ctx, bson.M{"_id": "hello"}).Decode(&got); err != nil {
		t.Fatalf("find: %v", err)
	}
	if got.Value != "world" {
		t.Errorf("value = %q, want %q", got.Value, "world")
	}
}

func TestIntegrationCloseStopsClient(t *testing.T) {
	requireIntegration(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cfg := config.MongoConfig{URI: config.DefaultMongoURI, Database: "djs_integration_test"}

	c, err := Connect(ctx, cfg, "integration-test")
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	if err := c.Close(ctx); err != nil {
		t.Fatalf("close: %v", err)
	}

	// After Close, the client must refuse work instead of silently reconnecting.
	if err := c.Ping(ctx); !errors.Is(err, mongo.ErrClientDisconnected) {
		t.Errorf("ping after close: err = %v, want mongo.ErrClientDisconnected", err)
	}
}
