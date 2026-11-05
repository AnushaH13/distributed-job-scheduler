package handlers

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/anushah13/distributed-job-scheduler/internal/models"
)

func registry() *Registry {
	return Default(slog.New(slog.NewTextHandler(io.Discard, nil)))
}

func TestDefaultTypes(t *testing.T) {
	got := fmt.Sprint(registry().Types())
	if got != "[echo fail sleep]" {
		t.Errorf("Types() = %s", got)
	}
}

func TestPermanentSurvivesWrapping(t *testing.T) {
	err := fmt.Errorf("charge card: %w", Permanent(errors.New("card number invalid")))
	if !IsPermanent(err) {
		t.Error("IsPermanent lost the marker through wrapping")
	}
	if IsPermanent(errors.New("timeout")) {
		t.Error("plain errors must not be permanent")
	}
}

func TestFailHandler(t *testing.T) {
	h, _ := registry().Get("fail")
	if err := h(context.Background(), &models.Job{Payload: map[string]any{}}); err == nil || IsPermanent(err) {
		t.Errorf("default fail should be transient, got %v", err)
	}
	err := h(context.Background(), &models.Job{Payload: map[string]any{"permanent": true}})
	if !IsPermanent(err) {
		t.Errorf("permanent=true should be permanent, got %v", err)
	}
}

func TestSleepHonoursCancellation(t *testing.T) {
	h, _ := registry().Get("sleep")
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	start := time.Now()
	err := h(ctx, &models.Job{Payload: map[string]any{"duration_ms": float64(10_000)}})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("err = %v, want context.DeadlineExceeded", err)
	}
	if time.Since(start) > time.Second {
		t.Error("sleep did not stop when the context was cancelled")
	}
}

func TestRegisterTwicePanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Error("expected a panic")
		}
	}()
	r := NewRegistry()
	r.Register("x", nil)
	r.Register("x", nil)
}
