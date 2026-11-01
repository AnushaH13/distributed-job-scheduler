package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

// env turns a map into a LookupFunc, so tests never touch real environment variables.
func env(vars map[string]string) LookupFunc {
	return func(key string) (string, bool) {
		v, ok := vars[key]
		return v, ok
	}
}

func TestLoadDefaults(t *testing.T) {
	cfg, err := LoadFrom(ServiceAPI, env(nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if cfg.Env != EnvDevelopment {
		t.Errorf("Env = %q, want %q", cfg.Env, EnvDevelopment)
	}
	if cfg.LogLevel != "info" {
		t.Errorf("LogLevel = %q, want %q", cfg.LogLevel, "info")
	}
	if cfg.HTTPAddr != ":8080" {
		t.Errorf("HTTPAddr = %q, want %q", cfg.HTTPAddr, ":8080")
	}
	if cfg.ShutdownTimeout != 15*time.Second {
		t.Errorf("ShutdownTimeout = %s, want 15s", cfg.ShutdownTimeout)
	}
	if cfg.Mongo.URI.Value() != "mongodb://localhost:27017" {
		t.Errorf("Mongo.URI = %q, want local default", cfg.Mongo.URI.Value())
	}
	if cfg.Mongo.Database != "job_scheduler" {
		t.Errorf("Mongo.Database = %q, want %q", cfg.Mongo.Database, "job_scheduler")
	}
	if cfg.Redis.Addr != "localhost:6379" || cfg.Redis.DB != 0 {
		t.Errorf("Redis = %+v, want localhost:6379 db 0", cfg.Redis)
	}
}

func TestDefaultHTTPAddrPerService(t *testing.T) {
	tests := []struct {
		service string
		want    string
	}{
		{ServiceAPI, ":8080"},
		{ServiceScheduler, ":8081"},
		{ServiceWorker, ":8082"},
	}
	for _, tt := range tests {
		t.Run(tt.service, func(t *testing.T) {
			cfg, err := LoadFrom(tt.service, env(nil))
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if cfg.HTTPAddr != tt.want {
				t.Errorf("HTTPAddr = %q, want %q", cfg.HTTPAddr, tt.want)
			}
		})
	}
}

func TestLoadOverrides(t *testing.T) {
	cfg, err := LoadFrom(ServiceWorker, env(map[string]string{
		"APP_ENV":          "Production", // case-insensitive
		"LOG_LEVEL":        "debug",
		"HTTP_ADDR":        "0.0.0.0:9090",
		"SHUTDOWN_TIMEOUT": "30s",
		"MONGO_URI":        "mongodb://db.internal:27017",
		"MONGO_DATABASE":   "scheduler_prod",
		"REDIS_ADDR":       "cache.internal:6379",
		"REDIS_PASSWORD":   "s3cret",
		"REDIS_DB":         "2",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if cfg.Env != EnvProduction || cfg.LogLevel != "debug" || cfg.HTTPAddr != "0.0.0.0:9090" {
		t.Errorf("unexpected app settings: %+v", cfg)
	}
	if cfg.ShutdownTimeout != 30*time.Second {
		t.Errorf("ShutdownTimeout = %s, want 30s", cfg.ShutdownTimeout)
	}
	if cfg.Redis.Password.Value() != "s3cret" || cfg.Redis.DB != 2 {
		t.Errorf("unexpected redis settings")
	}
}

func TestLoadReportsAllErrorsAtOnce(t *testing.T) {
	_, err := LoadFrom(ServiceAPI, env(map[string]string{
		"LOG_LEVEL":        "verbose",
		"REDIS_DB":         "abc",
		"SHUTDOWN_TIMEOUT": "15",
		"HTTP_ADDR":        "8080",
		"MONGO_URI":        "localhost:27017",
	}))
	if err == nil {
		t.Fatal("expected an error, got nil")
	}

	for _, key := range []string{"LOG_LEVEL", "REDIS_DB", "SHUTDOWN_TIMEOUT", "HTTP_ADDR", "MONGO_URI"} {
		if !strings.Contains(err.Error(), key) {
			t.Errorf("error does not mention %s:\n%v", key, err)
		}
	}
}

func TestLoadRangeChecks(t *testing.T) {
	tests := []struct {
		name string
		key  string
		val  string
	}{
		{"redis db too high", "REDIS_DB", "16"},
		{"redis db negative", "REDIS_DB", "-1"},
		{"shutdown too short", "SHUTDOWN_TIMEOUT", "500ms"},
		{"shutdown too long", "SHUTDOWN_TIMEOUT", "10m"},
		{"port zero", "HTTP_ADDR", ":0"},
		{"port too high", "HTTP_ADDR", ":70000"},
		{"unknown env", "APP_ENV", "staging"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := LoadFrom(ServiceAPI, env(map[string]string{tt.key: tt.val}))
			if err == nil {
				t.Fatalf("%s=%q: expected an error, got nil", tt.key, tt.val)
			}
			if !strings.Contains(err.Error(), tt.key) {
				t.Errorf("error does not mention %s: %v", tt.key, err)
			}
		})
	}
}

func TestProductionRequiresConnectionSettings(t *testing.T) {
	_, err := LoadFrom(ServiceAPI, env(map[string]string{"APP_ENV": "production"}))
	if err == nil {
		t.Fatal("expected an error in production without MONGO_URI and REDIS_ADDR")
	}
	for _, key := range []string{"MONGO_URI", "REDIS_ADDR"} {
		if !strings.Contains(err.Error(), key) {
			t.Errorf("error does not mention %s: %v", key, err)
		}
	}
}

func TestBlankValueUsesDefault(t *testing.T) {
	cfg, err := LoadFrom(ServiceAPI, env(map[string]string{"LOG_LEVEL": "   "}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.LogLevel != "info" {
		t.Errorf("LogLevel = %q, want default %q", cfg.LogLevel, "info")
	}
}

func TestUnknownService(t *testing.T) {
	_, err := LoadFrom("billing", env(nil))
	if !errors.Is(err, ErrUnknownService) {
		t.Fatalf("err = %v, want it to wrap ErrUnknownService", err)
	}

	// Wrapping with %w keeps the original error findable.
	wrapped := fmt.Errorf("load configuration: %w", err)
	if !errors.Is(wrapped, ErrUnknownService) {
		t.Errorf("errors.Is lost ErrUnknownService through wrapping: %v", wrapped)
	}
}

func TestSecretsNeverPrint(t *testing.T) {
	cfg, err := LoadFrom(ServiceAPI, env(map[string]string{
		"MONGO_URI":      "mongodb://admin:hunter2@db1:27017,db2:27017/?replicaSet=rs0",
		"REDIS_PASSWORD": "redis-pass-123",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	jsonBytes, err := json.Marshal(cfg)
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}

	outputs := map[string]string{
		"%v":           fmt.Sprintf("%v", cfg),
		"%+v":          fmt.Sprintf("%+v", cfg),
		"%#v":          fmt.Sprintf("%#v", cfg),
		"json.Marshal": string(jsonBytes),
	}
	for format, out := range outputs {
		for _, secret := range []string{"hunter2", "redis-pass-123"} {
			if strings.Contains(out, secret) {
				t.Errorf("%s output leaks %q:\n%s", format, secret, out)
			}
		}
	}

	if got := cfg.Mongo.SafeURI(); !strings.Contains(got, "admin:xxxxx@db1:27017,db2:27017") {
		t.Errorf("SafeURI = %q, want password replaced and hosts kept", got)
	}
}
