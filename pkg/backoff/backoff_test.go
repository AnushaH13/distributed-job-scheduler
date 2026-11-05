package backoff

import (
	"testing"
	"time"
)

func TestCeilingDoublesUntilCap(t *testing.T) {
	p := Policy{Base: 2 * time.Second, Cap: 5 * time.Minute}
	tests := []struct {
		attempt int
		want    time.Duration
	}{
		{0, 2 * time.Second}, // treated as attempt 1
		{1, 2 * time.Second},
		{2, 4 * time.Second},
		{3, 8 * time.Second},
		{8, 256 * time.Second},
		{9, 5 * time.Minute}, // 512s capped to 300s
		{1000, 5 * time.Minute},
	}
	for _, tt := range tests {
		if got := p.Ceiling(tt.attempt); got != tt.want {
			t.Errorf("Ceiling(%d) = %s, want %s", tt.attempt, got, tt.want)
		}
	}
}

func TestDelayUsesFullRange(t *testing.T) {
	p := Policy{Base: time.Second, Cap: time.Minute}

	p.Rand = func(n int64) int64 { return 0 }
	if got := p.Delay(3); got != 0 {
		t.Errorf("lowest random value: Delay = %s, want 0", got)
	}

	p.Rand = func(n int64) int64 { return n - 1 }
	if got := p.Delay(3); got != 4*time.Second {
		t.Errorf("highest random value: Delay = %s, want 4s", got)
	}
}

func TestDelayStaysWithinCeiling(t *testing.T) {
	p := Default
	for attempt := 1; attempt <= 12; attempt++ {
		for range 200 {
			if d := p.Delay(attempt); d < 0 || d > p.Ceiling(attempt) {
				t.Fatalf("Delay(%d) = %s, outside [0, %s]", attempt, d, p.Ceiling(attempt))
			}
		}
	}
}
