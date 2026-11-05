package app

import (
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/anushah13/distributed-job-scheduler/internal/config"
	"github.com/anushah13/distributed-job-scheduler/internal/health"
)

func testApp(t *testing.T) *App {
	t.Helper()
	// Reserve a free port, then release it for the server to use.
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	_ = l.Close()
	return &App{
		Config: config.Config{HTTPAddr: addr, ShutdownTimeout: 2 * time.Second},
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		Health: health.New(),
	}
}

// Regression test: without a background loop, Serve once returned at once.
func TestServeWithoutBackgroundRunsUntilCancelled(t *testing.T) {
	a := testApp(t)
	mux := http.NewServeMux()
	a.Health.Register(mux)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- a.Serve(ctx, mux, nil) }()

	var resp *http.Response
	var err error
	for range 50 {
		if resp, err = http.Get("http://" + a.Config.HTTPAddr + "/healthz"); err == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("server never answered: %v", err)
	}
	_ = resp.Body.Close()

	select {
	case err := <-done:
		t.Fatalf("Serve returned before cancellation: %v", err)
	default:
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Serve returned %v after cancellation, want nil", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Serve did not stop after cancellation")
	}
}

func TestServeWaitsForBackgroundAndMarksDraining(t *testing.T) {
	a := testApp(t)
	ctx, cancel := context.WithCancel(context.Background())

	stopped := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- a.Serve(ctx, http.NewServeMux(), func(ctx context.Context) error {
			<-ctx.Done()
			time.Sleep(200 * time.Millisecond) // finishing current work
			close(stopped)
			return nil
		})
	}()
	time.Sleep(100 * time.Millisecond)
	cancel()

	if err := <-done; err != nil {
		t.Fatalf("Serve: %v", err)
	}
	select {
	case <-stopped:
	default:
		t.Error("Serve returned before the background loop finished")
	}
	if !a.Health.Draining() {
		t.Error("readiness should report draining after shutdown starts")
	}
}
