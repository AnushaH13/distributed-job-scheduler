package main

import (
	"fmt"
	"log/slog"
	"os"

	"github.com/anushah13/distributed-job-scheduler/internal/config"
	"github.com/anushah13/distributed-job-scheduler/internal/logging"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "api service: %v\n", err)
		os.Exit(1)
	}
}

// run does the real work and returns an error instead of exiting,
// so main is the only place that decides how the program ends.
func run() error {
	cfg, err := config.Load(config.ServiceAPI)
	if err != nil {
		return fmt.Errorf("invalid configuration:\n%w", err)
	}

	logger, err := logging.New(os.Stdout, cfg)
	if err != nil {
		return fmt.Errorf("create logger: %w", err)
	}

	slog.SetDefault(logger)

	logger.Info("service starting", "config", cfg)

	return nil
}
