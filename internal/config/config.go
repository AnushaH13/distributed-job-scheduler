// Package config loads and validates service configuration from environment variables.
package config

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

// Service names. Each binary passes its own name to Load.
const (
	ServiceAPI       = "api"
	ServiceScheduler = "scheduler"
	ServiceWorker    = "worker"
)

// Environments.
const (
	EnvDevelopment = "development"
	EnvProduction  = "production"
)

// defaultHTTPAddr gives each service its own port, so all three can run
// on one laptop at the same time without a port conflict.
var defaultHTTPAddr = map[string]string{
	ServiceAPI:       ":8080",
	ServiceScheduler: ":8081",
	ServiceWorker:    ":8082",
}

// Config is the complete configuration for one service process.
type Config struct {
	Service         string
	Env             string
	LogLevel        string
	HTTPAddr        string
	ShutdownTimeout time.Duration
	Mongo           MongoConfig
	Redis           RedisConfig
}

// MongoConfig holds MongoDB connection settings.
type MongoConfig struct {
	URI      Secret
	Database string
}

// RedisConfig holds Redis connection settings.
type RedisConfig struct {
	Addr     string
	Password Secret
	DB       int
}

// Secret is a string that never prints its value.
// Use Value() only where the real value is needed, such as when connecting.
type Secret string

// String implements fmt.Stringer, so %v and %s never show the value.
func (s Secret) String() string {
	if s == "" {
		return ""
	}
	return "[REDACTED]"
}

// GoString implements fmt.GoStringer, so %#v never shows the value either.
func (s Secret) GoString() string {
	return `config.Secret("[REDACTED]")`
}

// Value returns the real secret value.
func (s Secret) Value() string {
	return string(s)
}

// LookupFunc finds an environment variable. os.LookupEnv has this shape,
// and tests pass a map-based function instead.
type LookupFunc func(key string) (string, bool)

// Load reads configuration for the given service from the real environment.
func Load(service string) (Config, error) {
	return LoadFrom(service, os.LookupEnv)
}

// LoadFrom reads configuration using the given lookup function.
// It reports every problem at once instead of stopping at the first one.
func LoadFrom(service string, lookup LookupFunc) (Config, error) {
	r := reader{lookup: lookup}

	addr, known := defaultHTTPAddr[service]
	if !known {
		return Config{}, fmt.Errorf("unknown service %q", service)
	}

	cfg := Config{
		Service:         service,
		Env:             r.oneOf("APP_ENV", EnvDevelopment, EnvDevelopment, EnvProduction),
		LogLevel:        r.oneOf("LOG_LEVEL", "info", "debug", "info", "warn", "error"),
		HTTPAddr:        r.addr("HTTP_ADDR", addr),
		ShutdownTimeout: r.duration("SHUTDOWN_TIMEOUT", 15*time.Second, time.Second, 2*time.Minute),
		Mongo: MongoConfig{
			URI:      Secret(r.mongoURI("MONGO_URI", "mongodb://localhost:27017")),
			Database: r.str("MONGO_DATABASE", "job_scheduler"),
		},
		Redis: RedisConfig{
			Addr:     r.addr("REDIS_ADDR", "localhost:6379"),
			Password: Secret(r.str("REDIS_PASSWORD", "")),
			DB:       r.intRange("REDIS_DB", 0, 0, 15),
		},
	}

	// Local defaults point at localhost. In production that is almost
	// certainly a mistake, so connection settings must be set explicitly.
	if cfg.Env == EnvProduction {
		r.require("MONGO_URI")
		r.require("REDIS_ADDR")
	}

	if len(r.errs) > 0 {
		return Config{}, errors.Join(r.errs...)
	}
	return cfg, nil
}

// Summary returns a one-line description that is safe to print or log.
func (c Config) Summary() string {
	redisPassword := "empty"
	if c.Redis.Password != "" {
		redisPassword = "set"
	}
	return fmt.Sprintf(
		"service=%s env=%s log_level=%s http_addr=%s shutdown_timeout=%s mongo_uri=%s mongo_database=%s redis_addr=%s redis_db=%d redis_password=%s",
		c.Service, c.Env, c.LogLevel, c.HTTPAddr, c.ShutdownTimeout,
		c.Mongo.SafeURI(), c.Mongo.Database, c.Redis.Addr, c.Redis.DB, redisPassword,
	)
}

// SafeURI returns the MongoDB URI with any password replaced by "xxxxx".
func (m MongoConfig) SafeURI() string {
	u, err := url.Parse(m.URI.Value())
	if err != nil {
		return "[unparseable]"
	}
	return u.Redacted()
}

// reader collects validation errors while reading variables.
type reader struct {
	lookup LookupFunc
	errs   []error
}

func (r *reader) fail(key string, format string, args ...any) {
	r.errs = append(r.errs, fmt.Errorf("%s: "+format, append([]any{key}, args...)...))
}

// get returns the trimmed value and whether it was set to something non-empty.
func (r *reader) get(key string) (string, bool) {
	v, ok := r.lookup(key)
	v = strings.TrimSpace(v)
	return v, ok && v != ""
}

func (r *reader) str(key, def string) string {
	if v, ok := r.get(key); ok {
		return v
	}
	return def
}

func (r *reader) require(key string) {
	if _, ok := r.get(key); !ok {
		r.fail(key, "must be set when APP_ENV=%s", EnvProduction)
	}
}

func (r *reader) oneOf(key, def string, allowed ...string) string {
	v := strings.ToLower(r.str(key, def))
	for _, a := range allowed {
		if v == a {
			return v
		}
	}
	r.fail(key, "%q is not one of %s", v, strings.Join(allowed, ", "))
	return def
}

func (r *reader) intRange(key string, def, minimum, maximum int) int {
	raw, ok := r.get(key)
	if !ok {
		return def
	}
	n, err := strconv.Atoi(raw)
	if err != nil {
		r.fail(key, "%q is not a whole number", raw)
		return def
	}
	if n < minimum || n > maximum {
		r.fail(key, "%d is outside the allowed range %d to %d", n, minimum, maximum)
		return def
	}
	return n
}

func (r *reader) duration(key string, def, minimum, maximum time.Duration) time.Duration {
	raw, ok := r.get(key)
	if !ok {
		return def
	}
	d, err := time.ParseDuration(raw)
	if err != nil {
		r.fail(key, "%q is not a duration; use a unit, such as 15s or 1m", raw)
		return def
	}
	if d < minimum || d > maximum {
		r.fail(key, "%s is outside the allowed range %s to %s", d, minimum, maximum)
		return def
	}
	return d
}

// addr validates a "host:port" or ":port" network address.
func (r *reader) addr(key, def string) string {
	v := r.str(key, def)
	_, port, err := net.SplitHostPort(v)
	if err != nil {
		r.fail(key, "%q is not a host:port address", v)
		return def
	}
	if p, err := strconv.Atoi(port); err != nil || p < 1 || p > 65535 {
		r.fail(key, "port %q must be a number from 1 to 65535", port)
		return def
	}
	return v
}

// mongoURI does a cheap sanity check. The MongoDB driver performs full
// validation when it connects (Lesson 2.4).
func (r *reader) mongoURI(key, def string) string {
	v := r.str(key, def)
	if !strings.HasPrefix(v, "mongodb://") && !strings.HasPrefix(v, "mongodb+srv://") {
		r.fail(key, "must start with mongodb:// or mongodb+srv://")
		return def
	}
	return v
}
