package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/anushah13/distributed-job-scheduler/internal/app"
	"github.com/anushah13/distributed-job-scheduler/internal/config"
	"github.com/anushah13/distributed-job-scheduler/internal/idgen"
	"github.com/anushah13/distributed-job-scheduler/internal/lifecycle"
	"github.com/anushah13/distributed-job-scheduler/internal/locks"
	"github.com/anushah13/distributed-job-scheduler/internal/middleware"
	"github.com/anushah13/distributed-job-scheduler/internal/scheduler"
	"github.com/anushah13/distributed-job-scheduler/pkg/backoff"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "scheduler service: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	a, err := app.Start(ctx, config.ServiceScheduler)
	if err != nil {
		return err
	}
	defer a.Close()

	host, _ := os.Hostname()
	instanceID := host + "-" + idgen.Token()[:8]

	lc := lifecycle.New(a.Store, a.Queue, backoff.Default, a.Logger)
	sched := scheduler.New(a.Store, a.Queue, lc, a.Logger, scheduler.Options{})
	elector := locks.NewElector(a.Redis.Redis(), a.Queue.Prefix()+"leader", instanceID, a.Store.Leases, a.Logger, locks.Options{})

	// Every replica runs the elector; only the leader runs sched.Lead.
	mux := http.NewServeMux()
	a.Health.Register(mux)
	handler := middleware.Chain(mux, middleware.RequestID, middleware.Logger(a.Logger))

	return a.Serve(ctx, handler, func(ctx context.Context) error {
		return elector.Run(ctx, sched.Lead)
	})
}
