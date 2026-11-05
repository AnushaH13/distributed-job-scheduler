// Package schedule validates cron expressions and computes next run times.
package schedule

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/robfig/cron/v3"
)

// ErrInvalidCron is returned for expressions that are not standard 5-field cron.
var ErrInvalidCron = errors.New("invalid cron expression")

// ErrInvalidTimezone is returned for names that are not IANA time zones.
var ErrInvalidTimezone = errors.New("invalid timezone")

// Cron is a parsed recurring schedule in a specific time zone.
type Cron struct {
	schedule cron.Schedule
	location *time.Location
}

// ParseCron parses a standard 5-field expression ("minute hour day month weekday")
// evaluated in the IANA time zone tz, such as "Asia/Kolkata".
//
// Descriptors like "@daily" are rejected on purpose: the API documents 5 fields.
func ParseCron(expr, tz string) (*Cron, error) {
	if len(strings.Fields(expr)) != 5 || strings.ContainsAny(expr, "@") {
		return nil, fmt.Errorf("%w: %q must have exactly 5 fields", ErrInvalidCron, expr)
	}
	if tz == "" || strings.EqualFold(tz, "local") {
		// "Local" would mean the server's zone, which differs between machines.
		return nil, fmt.Errorf("%w: %q", ErrInvalidTimezone, tz)
	}
	loc, err := time.LoadLocation(tz)
	if err != nil {
		return nil, fmt.Errorf("%w: %q", ErrInvalidTimezone, tz)
	}

	// Without a CRON_TZ prefix, robfig/cron would use the server's local zone.
	sched, err := cron.ParseStandard("CRON_TZ=" + tz + " " + expr)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidCron, err)
	}
	return &Cron{schedule: sched, location: loc}, nil
}

// Next returns the first run time strictly after t, in UTC.
func (c *Cron) Next(t time.Time) time.Time {
	return c.schedule.Next(t.In(c.location)).UTC()
}
