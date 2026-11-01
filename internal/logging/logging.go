// Package logging creates the structured logger used by every service.
package logging

import (
	"fmt"
	"io"
	"log/slog"

	"github.com/anushah13/distributed-job-scheduler/internal/config"
)

// New returns a logger that writes to w.
func New(w io.Writer, cfg config.Config) (*slog.Logger, error) {
	var level slog.Level
	if err := level.UnmarshalText([]byte(cfg.LogLevel)); err != nil {
		return nil, fmt.Errorf("parse log level %q: %w", cfg.LogLevel, err)
	}

	opts := &slog.HandlerOptions{Level: level}

	var handler slog.Handler
	if cfg.Env == config.EnvProduction {
		handler = slog.NewJSONHandler(w, opts)
	} else {
		handler = slog.NewTextHandler(w, opts)
	}

	return slog.New(handler).With("service", cfg.Service), nil
}
