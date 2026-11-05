// Package health serves liveness and readiness endpoints.
//
//   - /healthz (liveness): "is the process alive?" It never checks
//     dependencies: if MongoDB is down, restarting our pod would not help,
//     and Kubernetes would restart every pod at once.
//   - /readyz (readiness): "should this pod receive traffic?" It pings every
//     dependency and returns 503 while any is down or while shutting down,
//     so the load balancer stops sending requests here.
package health

import (
	"context"
	"encoding/json"
	"net/http"
	"sync/atomic"
	"time"
)

// Pinger is anything that can check its connection. *mongodb.Client and
// *redis.Client satisfy it without knowing this interface exists: Go
// interfaces are implicit, so they are defined where they are used.
type Pinger interface {
	Ping(ctx context.Context) error
}

// Dependency is one named Pinger.
type Dependency struct {
	Name   string
	Pinger Pinger
}

// Checker answers health requests.
type Checker struct {
	deps     []Dependency
	timeout  time.Duration
	draining atomic.Bool
}

// New returns a Checker for deps.
func New(deps ...Dependency) *Checker {
	return &Checker{deps: deps, timeout: 2 * time.Second}
}

// SetDraining makes readiness fail from now on. Call it when shutdown begins.
func (c *Checker) SetDraining() { c.draining.Store(true) }

// Draining reports whether SetDraining was called.
func (c *Checker) Draining() bool { return c.draining.Load() }

// Liveness handles /healthz.
func (c *Checker) Liveness(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// Readiness handles /readyz.
func (c *Checker) Readiness(w http.ResponseWriter, r *http.Request) {
	if c.draining.Load() {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"status": "draining"})
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), c.timeout)
	defer cancel()

	checks := make(map[string]string, len(c.deps))
	status, code := "ok", http.StatusOK
	for _, d := range c.deps {
		if err := d.Pinger.Ping(ctx); err != nil {
			checks[d.Name] = "unavailable"
			status, code = "unavailable", http.StatusServiceUnavailable
			continue
		}
		checks[d.Name] = "ok"
	}
	// Error details stay in logs; the response only says which dependency failed.
	writeJSON(w, code, map[string]any{"status": status, "checks": checks})
}

// Register adds /healthz and /readyz to mux.
func (c *Checker) Register(mux *http.ServeMux) {
	mux.HandleFunc("GET /healthz", c.Liveness)
	mux.HandleFunc("GET /readyz", c.Readiness)
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
