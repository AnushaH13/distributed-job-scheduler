package idgen

import (
	"regexp"
	"strings"
	"testing"
)

var uuidV4 = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

func TestUUIDFormat(t *testing.T) {
	for range 100 {
		if id := UUID(); !uuidV4.MatchString(id) {
			t.Fatalf("UUID() = %q, not a version 4 UUID", id)
		}
	}
}

func TestNewIsUnique(t *testing.T) {
	seen := make(map[string]bool)
	for range 1000 {
		id := New("job")
		if !strings.HasPrefix(id, "job_") {
			t.Fatalf("New(job) = %q, want prefix job_", id)
		}
		if seen[id] {
			t.Fatalf("duplicate id %q", id)
		}
		seen[id] = true
	}
}

func TestTokenLength(t *testing.T) {
	if got := len(Token()); got != 32 {
		t.Errorf("len(Token()) = %d, want 32", got)
	}
}
