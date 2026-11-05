package services

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

var now = time.Date(2026, 11, 3, 12, 0, 0, 0, time.UTC)

func registered(t string) bool { return t == "echo" || t == "sleep" }

func ptr[T any](v T) *T { return &v }

func TestValidateDefaults(t *testing.T) {
	v, err := validate(CreateRequest{JobType: "echo", Schedule: ScheduleRequest{Type: "immediate"}}, registered, now)
	if err != nil {
		t.Fatal(err)
	}
	if v.maxRetries != 3 || v.timeoutSeconds != 300 || !v.runAt.Equal(now) || v.payload == nil {
		t.Errorf("unexpected defaults: %+v", v)
	}
}

func TestValidateReportsEveryProblem(t *testing.T) {
	_, err := validate(CreateRequest{
		JobType:        "unknown",
		Payload:        json.RawMessage(`[1,2]`),
		MaxRetries:     ptr(11),
		TimeoutSeconds: ptr(0),
		Schedule:       ScheduleRequest{Type: "delayed"},
	}, registered, now)

	var verr *ValidationError
	if !errors.As(err, &verr) {
		t.Fatalf("err = %v, want *ValidationError", err)
	}
	for _, field := range []string{"job_type", "payload", "max_retries", "timeout_seconds", "schedule"} {
		if !strings.Contains(err.Error(), field+":") {
			t.Errorf("missing issue for %s in %v", field, err)
		}
	}
}

func TestValidateSchedules(t *testing.T) {
	tests := []struct {
		name    string
		sched   ScheduleRequest
		wantErr bool
		wantRun time.Time
	}{
		{"delay", ScheduleRequest{Type: "delayed", DelaySeconds: ptr[int64](90)}, false, now.Add(90 * time.Second)},
		{"run_at", ScheduleRequest{Type: "delayed", RunAt: "2026-11-03T18:30:00+05:30"}, false, time.Date(2026, 11, 3, 13, 0, 0, 0, time.UTC)},
		{"run_at far past", ScheduleRequest{Type: "delayed", RunAt: "2026-11-03T11:00:00Z"}, true, time.Time{}},
		{"run_at too far", ScheduleRequest{Type: "delayed", RunAt: "2028-01-01T00:00:00Z"}, true, time.Time{}},
		{"both", ScheduleRequest{Type: "delayed", RunAt: "2026-11-03T13:00:00Z", DelaySeconds: ptr[int64](5)}, true, time.Time{}},
		{"delay zero", ScheduleRequest{Type: "delayed", DelaySeconds: ptr[int64](0)}, true, time.Time{}},
		{"cron", ScheduleRequest{Type: "recurring", Cron: "0 9 * * *", Timezone: "Asia/Kolkata"}, false, time.Date(2026, 11, 4, 3, 30, 0, 0, time.UTC)},
		{"cron bad", ScheduleRequest{Type: "recurring", Cron: "every day"}, true, time.Time{}},
		{"unknown type", ScheduleRequest{Type: "weekly"}, true, time.Time{}},
		{"immediate with extras", ScheduleRequest{Type: "immediate", Cron: "* * * * *"}, true, time.Time{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v, err := validate(CreateRequest{JobType: "echo", Schedule: tt.sched}, registered, now)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected an error")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if !v.runAt.Equal(tt.wantRun) {
				t.Errorf("runAt = %s, want %s", v.runAt, tt.wantRun)
			}
		})
	}
}

func TestPayloadTooLarge(t *testing.T) {
	big := `{"x":"` + strings.Repeat("a", MaxPayloadBytes) + `"}`
	_, err := validate(CreateRequest{JobType: "echo", Payload: json.RawMessage(big), Schedule: ScheduleRequest{Type: "immediate"}}, registered, now)
	if !errors.Is(err, ErrPayloadTooLarge) {
		t.Errorf("err = %v, want ErrPayloadTooLarge", err)
	}
}

func TestFingerprint(t *testing.T) {
	base := CreateRequest{JobType: "echo", Payload: json.RawMessage(`{"a":1,"b":2}`), Schedule: ScheduleRequest{Type: "delayed", DelaySeconds: ptr[int64](60)}}
	reordered := base
	reordered.Payload = json.RawMessage(`{ "b": 2, "a": 1 }`)
	different := base
	different.Payload = json.RawMessage(`{"a":1,"b":3}`)

	fp := func(req CreateRequest, at time.Time) string {
		v, err := validate(req, registered, at)
		if err != nil {
			t.Fatal(err)
		}
		return v.fingerprint
	}

	if fp(base, now) != fp(reordered, now) {
		t.Error("key order in the payload must not change the fingerprint")
	}
	if fp(base, now) != fp(base, now.Add(3*time.Second)) {
		t.Error("retrying the same delay_seconds request later must keep the fingerprint")
	}
	if fp(base, now) == fp(different, now) {
		t.Error("a different payload must change the fingerprint")
	}
}

func TestCursorRoundTrip(t *testing.T) {
	if _, err := decodeCursor("not-base64!"); err == nil {
		t.Error("expected an error for a garbage cursor")
	}
}
