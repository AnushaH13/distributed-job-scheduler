package logging

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/anushah13/distributed-job-scheduler/internal/config"
)

// testConfig builds a valid configuration without touching real environment variables.
func testConfig(t *testing.T, vars map[string]string) config.Config {
	t.Helper()

	cfg, err := config.LoadFrom(config.ServiceAPI, func(key string) (string, bool) {
		v, ok := vars[key]
		return v, ok
	})
	if err != nil {
		t.Fatalf("load test config: %v", err)
	}
	return cfg
}

func TestDevelopmentWritesText(t *testing.T) {
	var buf bytes.Buffer

	logger, err := New(&buf, testConfig(t, nil))
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	logger.Info("service starting", "http_addr", ":8080")

	out := buf.String()
	for _, want := range []string{
		"level=INFO",
		`msg="service starting"`,
		"service=api",
		"http_addr=:8080",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q:\n%s", want, out)
		}
	}
}

func TestProductionWritesJSON(t *testing.T) {
	var buf bytes.Buffer

	logger, err := New(&buf, testConfig(t, map[string]string{
		"APP_ENV":    "production",
		"MONGO_URI":  "mongodb://db.internal:27017",
		"REDIS_ADDR": "cache.internal:6379",
	}))
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	logger.Info("service starting", "http_addr", ":8080")

	var line map[string]any
	if err := json.Unmarshal(buf.Bytes(), &line); err != nil {
		t.Fatalf("output is not one JSON object: %v\n%s", err, buf.String())
	}

	if line["level"] != "INFO" || line["msg"] != "service starting" || line["service"] != "api" {
		t.Errorf("unexpected fields: %v", line)
	}
}

func TestLevelFiltering(t *testing.T) {
	var buf bytes.Buffer

	logger, err := New(&buf, testConfig(t, map[string]string{
		"LOG_LEVEL": "warn",
	}))
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	logger.Debug("debug message")
	logger.Info("info message")
	logger.Warn("warn message")
	logger.Error("error message")

	out := buf.String()

	for _, hidden := range []string{"debug message", "info message"} {
		if strings.Contains(out, hidden) {
			t.Errorf("LOG_LEVEL=warn should hide %q:\n%s", hidden, out)
		}
	}

	for _, shown := range []string{"warn message", "error message"} {
		if !strings.Contains(out, shown) {
			t.Errorf("LOG_LEVEL=warn should show %q:\n%s", shown, out)
		}
	}
}

func TestSecretsNeverLogged(t *testing.T) {
	vars := map[string]string{
		"MONGO_URI":      "mongodb://admin:hunter2@db1:27017,db2:27017/?replicaSet=rs0",
		"REDIS_ADDR":     "cache.internal:6379",
		"REDIS_PASSWORD": "redis-pass-123",
	}

	for _, env := range []string{config.EnvDevelopment, config.EnvProduction} {
		t.Run(env, func(t *testing.T) {
			vars["APP_ENV"] = env
			cfg := testConfig(t, vars)

			var buf bytes.Buffer
			logger, err := New(&buf, cfg)
			if err != nil {
				t.Fatalf("New: %v", err)
			}

			logger.Info("service starting", "config", cfg)
			logger.Info("secrets", "mongo_uri", cfg.Mongo.URI, "redis_password", cfg.Redis.Password)

			out := buf.String()

			for _, secret := range []string{"hunter2", "redis-pass-123"} {
				if strings.Contains(out, secret) {
					t.Errorf("log output leaks %q:\n%s", secret, out)
				}
			}

			if !strings.Contains(out, "db1:27017,db2:27017") {
				t.Errorf("hosts should stay visible for debugging:\n%s", out)
			}
		})
	}
}
