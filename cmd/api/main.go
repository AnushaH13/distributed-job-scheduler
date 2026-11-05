package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/anushah13/distributed-job-scheduler/internal/api"
	"github.com/anushah13/distributed-job-scheduler/internal/app"
	"github.com/anushah13/distributed-job-scheduler/internal/config"
	"github.com/anushah13/distributed-job-scheduler/internal/handlers"
	"github.com/anushah13/distributed-job-scheduler/internal/services"
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
	// SIGTERM is what Kubernetes and Docker send; SIGINT is Ctrl+C.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	a, err := app.Start(ctx, config.ServiceAPI)
	if err != nil {
		return err
	}
	defer a.Close()

	// The API never runs jobs, but it validates job types against the same
	// registry the workers use.
	registry := handlers.Default(a.Logger)
	svc := services.NewJobService(a.Store, a.Queue, registry.Has, a.Logger)

	return a.Serve(ctx, api.NewRouter(svc, a.Health, a.Logger), nil)
}
