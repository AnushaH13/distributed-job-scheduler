package health

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type fakePinger struct{ err error }

func (f fakePinger) Ping(context.Context) error { return f.err }

func get(t *testing.T, c *Checker, path string) *httptest.ResponseRecorder {
	t.Helper()
	mux := http.NewServeMux()
	c.Register(mux)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
	return rec
}

func TestReadyWhenAllDependenciesAnswer(t *testing.T) {
	c := New(Dependency{"mongodb", fakePinger{}}, Dependency{"redis", fakePinger{}})
	if rec := get(t, c, "/readyz"); rec.Code != http.StatusOK {
		t.Errorf("status = %d, want 200: %s", rec.Code, rec.Body)
	}
}

func TestNotReadyWhenOneDependencyFails(t *testing.T) {
	c := New(Dependency{"mongodb", fakePinger{}}, Dependency{"redis", fakePinger{errors.New("connection refused")}})
	rec := get(t, c, "/readyz")
	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), `"redis":"unavailable"`) {
		t.Errorf("body should name the failed dependency: %s", rec.Body)
	}
	if strings.Contains(rec.Body.String(), "connection refused") {
		t.Errorf("body must not expose error details: %s", rec.Body)
	}
}

func TestLivenessIgnoresDependencies(t *testing.T) {
	c := New(Dependency{"mongodb", fakePinger{errors.New("down")}})
	if rec := get(t, c, "/healthz"); rec.Code != http.StatusOK {
		t.Errorf("liveness must not depend on MongoDB: status = %d", rec.Code)
	}
}

func TestDrainingFailsReadiness(t *testing.T) {
	c := New(Dependency{"mongodb", fakePinger{}})
	c.SetDraining()
	if rec := get(t, c, "/readyz"); rec.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503 while draining", rec.Code)
	}
}

// Compile-time proof that both clients satisfy Pinger lives in the app
// package, which imports them; health must not depend on database code.
