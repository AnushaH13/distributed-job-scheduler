package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/anushah13/distributed-job-scheduler/internal/app"
	"github.com/anushah13/distributed-job-scheduler/internal/config"
	"github.com/anushah13/distributed-job-scheduler/internal/handlers"
	"github.com/anushah13/distributed-job-scheduler/internal/lifecycle"
	"github.com/anushah13/distributed-job-scheduler/internal/middleware"
	"github.com/anushah13/distributed-job-scheduler/internal/worker"
	"github.com/anushah13/distributed-job-scheduler/pkg/backoff"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "worker service: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	a, err := app.Start(ctx, config.ServiceWorker)
	if err != nil {
		return err
	}
	defer a.Close()

	lc := lifecycle.New(a.Store, a.Queue, backoff.Default, a.Logger)
	w := worker.New(a.Store, a.Queue, lc, handlers.Default(a.Logger), a.Logger, worker.Options{
		Concurrency: a.Config.Worker.Concurrency,
	})

	mux := http.NewServeMux()
	a.Health.Register(mux)
	handler := middleware.Chain(mux, middleware.RequestID, middleware.Logger(a.Logger))

	// On shutdown the worker stops fetching at once, lets running jobs finish
	// for most of SHUTDOWN_TIMEOUT, then cancels them, keeping a few seconds
	// to record their results. Jobs cut off this way are retried.
	grace := max(a.Config.ShutdownTimeout-3*time.Second, time.Second)

	return a.Serve(ctx, handler, func(fetchCtx context.Context) error {
		execCtx, cancelExec := context.WithCancel(context.WithoutCancel(fetchCtx))
		defer cancelExec()
		stopTimer := context.AfterFunc(fetchCtx, func() { time.AfterFunc(grace, cancelExec) })
		defer stopTimer()
		return w.Run(fetchCtx, execCtx)
	})
}
