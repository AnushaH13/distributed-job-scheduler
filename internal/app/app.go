// Package app holds the startup and shutdown steps every service shares:
// configuration, logging, database connections, indexes, and an HTTP server
// that stops gracefully.
package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/anushah13/distributed-job-scheduler/internal/config"
	"github.com/anushah13/distributed-job-scheduler/internal/database/mongodb"
	"github.com/anushah13/distributed-job-scheduler/internal/database/redis"
	"github.com/anushah13/distributed-job-scheduler/internal/health"
	"github.com/anushah13/distributed-job-scheduler/internal/logging"
	"github.com/anushah13/distributed-job-scheduler/internal/queue"
	"github.com/anushah13/distributed-job-scheduler/internal/repository"
)

// Both clients satisfy health.Pinger; this fails to compile if that changes.
var (
	_ health.Pinger = (*mongodb.Client)(nil)
	_ health.Pinger = (*redis.Client)(nil)
)

const (
	startupTimeout = 15 * time.Second
	closeTimeout   = 5 * time.Second
)

// App is a started service with its dependencies.
type App struct {
	Config config.Config
	Logger *slog.Logger
	Mongo  *mongodb.Client
	Redis  *redis.Client
	Store  *repository.Store
	Queue  *queue.Queue
	Health *health.Checker
}

// Start loads configuration and connects to MongoDB and Redis. Both are
// required at startup (a clear error beats a half-working service); after
// startup, outages are tolerated and reported through /readyz.
func Start(ctx context.Context, service string) (*App, error) {
	cfg, err := config.Load(service)
	if err != nil {
		return nil, fmt.Errorf("invalid configuration:\n%w", err)
	}
	logger, err := logging.New(os.Stdout, cfg)
	if err != nil {
		return nil, fmt.Errorf("create logger: %w", err)
	}
	slog.SetDefault(logger)
	logger.Info("service starting", "config", cfg)

	startCtx, cancel := context.WithTimeout(ctx, startupTimeout)
	defer cancel()

	mc, err := mongodb.Connect(startCtx, cfg.Mongo, cfg.Service)
	if err != nil {
		return nil, fmt.Errorf("connect to MongoDB: %w", err)
	}
	rc, err := redis.Connect(startCtx, cfg.Redis, cfg.Service)
	if err != nil {
		// Close MongoDB with a fresh context: startCtx may be the reason Redis failed.
		closeCtx, cancel := context.WithTimeout(context.Background(), closeTimeout)
		defer cancel()
		_ = mc.Close(closeCtx)
		return nil, fmt.Errorf("connect to Redis: %w", err)
	}

	a := &App{
		Config: cfg,
		Logger: logger,
		Mongo:  mc,
		Redis:  rc,
		Store:  repository.New(mc.Database()),
		Queue:  queue.New(rc.Redis(), queue.DefaultPrefix),
		Health: health.New(
			health.Dependency{Name: "mongodb", Pinger: mc},
			health.Dependency{Name: "redis", Pinger: rc},
		),
	}

	if err := a.Store.EnsureIndexes(startCtx); err != nil {
		a.Close()
		return nil, fmt.Errorf("ensure indexes: %w", err)
	}

	logger.Info("connected to dependencies", "mongo", cfg.Mongo.SafeURI(), "redis", cfg.Redis.Addr)
	return a, nil
}

// Close closes both connections, reporting errors in the log.
func (a *App) Close() {
	ctx, cancel := context.WithTimeout(context.Background(), closeTimeout)
	defer cancel()
	if err := errors.Join(a.Redis.Close(), a.Mongo.Close(ctx)); err != nil {
		a.Logger.Warn("close dependencies", "error", err)
	}
}

// Serve runs the HTTP server and the optional background loop until ctx is
// cancelled (SIGINT/SIGTERM), then shuts both down within SHUTDOWN_TIMEOUT:
//
//  1. readiness turns 503 so load balancers stop sending new requests
//  2. the HTTP server stops accepting and waits for in-flight requests
//  3. the background loop (scheduler or worker) finishes its current work
func (a *App) Serve(ctx context.Context, handler http.Handler, background func(context.Context) error) error {
	srv := &http.Server{
		Addr:              a.Config.HTTPAddr,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	serverErr := make(chan error, 1)
	go func() {
		a.Logger.Info("http server listening", "addr", srv.Addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErr <- err
		}
		close(serverErr)
	}()

	bgCtx, stopBackground := context.WithCancel(ctx)
	defer stopBackground()
	// A nil channel never receives, so without a background loop the select
	// below waits only for the signal or a server error. (A closed channel
	// would be "ready" at once and shut the service down immediately.)
	var bgDone chan error
	if background != nil {
		bgDone = make(chan error, 1)
		go func() { bgDone <- background(bgCtx) }()
	}

	var runErr error
	select {
	case <-ctx.Done():
		a.Logger.Info("shutdown signal received")
	case err := <-serverErr:
		runErr = fmt.Errorf("http server: %w", err)
	case err := <-bgDone:
		if err != nil {
			runErr = fmt.Errorf("background loop: %w", err)
		}
		bgDone = nil
	}

	a.Health.SetDraining()
	stopBackground()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), a.Config.ShutdownTimeout)
	defer cancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		a.Logger.Warn("http server did not stop cleanly", "error", err)
	}
	if bgDone != nil {
		select {
		case err := <-bgDone:
			if err != nil && runErr == nil {
				runErr = fmt.Errorf("background loop: %w", err)
			}
		case <-shutdownCtx.Done():
			a.Logger.Warn("background work did not stop within the shutdown timeout")
		}
	}

	a.Logger.Info("service stopped")
	return runErr
}
