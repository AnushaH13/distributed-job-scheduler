// Package middleware wraps HTTP handlers with request IDs, access logs and
// panic recovery.
package middleware

import (
	"context"
	"log/slog"
	"net/http"
	"regexp"
	"runtime/debug"
	"time"

	"github.com/anushah13/distributed-job-scheduler/internal/idgen"
)

// RequestIDHeader carries the request ID in both directions.
const RequestIDHeader = "X-Request-ID"

type ctxKey struct{}

// validRequestID accepts IDs from callers (for tracing across services) only
// if they are short and harmless to put in logs.
var validRequestID = regexp.MustCompile(`^[A-Za-z0-9._:-]{1,64}$`)

// RequestIDFrom returns the ID stored by the RequestID middleware.
func RequestIDFrom(ctx context.Context) string {
	id, _ := ctx.Value(ctxKey{}).(string)
	return id
}

// RequestID gives every request an ID, returns it in X-Request-ID, and
// stores it in the context for logs and error bodies.
func RequestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get(RequestIDHeader)
		if !validRequestID.MatchString(id) {
			id = idgen.New("req")
		}
		w.Header().Set(RequestIDHeader, id)
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), ctxKey{}, id)))
	})
}

// statusRecorder remembers the status code for the access log.
type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (s *statusRecorder) WriteHeader(code int) {
	s.status = code
	s.ResponseWriter.WriteHeader(code)
}

// Logger writes one log line per request. Health checks are logged at debug
// level, because Kubernetes calls them every few seconds.
func Logger(logger *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
			next.ServeHTTP(rec, r)

			level := slog.LevelInfo
			if r.URL.Path == "/healthz" || r.URL.Path == "/readyz" {
				level = slog.LevelDebug
			}
			logger.Log(r.Context(), level, "http request",
				"method", r.Method,
				"path", r.URL.Path,
				"status", rec.status,
				"duration_ms", time.Since(start).Milliseconds(),
				"request_id", RequestIDFrom(r.Context()),
			)
		})
	}
}

// Recover turns a panic into a 500 response, so one bad request cannot
// crash the server and every other request in flight.
func Recover(logger *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer func() {
				if v := recover(); v != nil {
					if v == http.ErrAbortHandler {
						panic(v) // the server uses this to abort a response on purpose
					}
					logger.Error("panic in handler", "panic", v, "stack", string(debug.Stack()),
						"request_id", RequestIDFrom(r.Context()))
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(http.StatusInternalServerError)
					_, _ = w.Write([]byte(`{"error":{"code":"INTERNAL_ERROR","message":"internal error","request_id":"` + RequestIDFrom(r.Context()) + `"}}`))
				}
			}()
			next.ServeHTTP(w, r)
		})
	}
}

// Chain applies middleware so the first one listed runs first.
func Chain(h http.Handler, mws ...func(http.Handler) http.Handler) http.Handler {
	for i := len(mws) - 1; i >= 0; i-- {
		h = mws[i](h)
	}
	return h
}
