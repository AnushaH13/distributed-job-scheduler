package schedule

import (
	"errors"
	"testing"
	"time"
)

func TestParseCronRejectsBadInput(t *testing.T) {
	tests := []struct {
		name, expr, tz string
		want           error
	}{
		{"six fields", "0 0 * * * *", "UTC", ErrInvalidCron},
		{"descriptor", "@daily", "UTC", ErrInvalidCron},
		{"out of range", "61 * * * *", "UTC", ErrInvalidCron},
		{"unknown zone", "* * * * *", "Mars/Olympus", ErrInvalidTimezone},
		{"empty zone", "* * * * *", "", ErrInvalidTimezone},
		{"local zone", "* * * * *", "Local", ErrInvalidTimezone},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := ParseCron(tt.expr, tt.tz); !errors.Is(err, tt.want) {
				t.Errorf("err = %v, want %v", err, tt.want)
			}
		})
	}
}

func TestNextUsesJobTimezone(t *testing.T) {
	// 09:00 every day in India (UTC+05:30) is 03:30 UTC.
	c, err := ParseCron("0 9 * * *", "Asia/Kolkata")
	if err != nil {
		t.Fatal(err)
	}
	from := time.Date(2026, 11, 3, 0, 0, 0, 0, time.UTC)
	want := time.Date(2026, 11, 3, 3, 30, 0, 0, time.UTC)
	if got := c.Next(from); !got.Equal(want) {
		t.Errorf("Next = %s, want %s", got, want)
	}
	if got := c.Next(from); got.Location() != time.UTC {
		t.Errorf("Next should return UTC, got %s", got.Location())
	}
}

func TestNextHandlesDaylightSaving(t *testing.T) {
	// New York moves from UTC-4 to UTC-5 on 2026-11-01, so 09:00 local
	// is 13:00 UTC before the change and 14:00 UTC after it.
	c, err := ParseCron("0 9 * * *", "America/New_York")
	if err != nil {
		t.Fatal(err)
	}
	before := c.Next(time.Date(2026, 10, 31, 0, 0, 0, 0, time.UTC))
	after := c.Next(time.Date(2026, 11, 2, 0, 0, 0, 0, time.UTC))
	if before.Hour() != 13 || after.Hour() != 14 {
		t.Errorf("before = %s, after = %s; want 13:00 and 14:00 UTC", before, after)
	}
}
