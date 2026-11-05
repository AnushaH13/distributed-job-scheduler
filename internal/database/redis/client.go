// Package redis provides Redis connection management.
package redis

import (
	"context"
	"fmt"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/anushah13/distributed-job-scheduler/internal/config"
)

const (
	dialTimeout  = 5 * time.Second
	readTimeout  = 3 * time.Second
	writeTimeout = 3 * time.Second
)

// Client holds the Redis client. Create one per process and share it:
// go-redis manages a connection pool.
type Client struct {
	rdb *goredis.Client
}

// Connect creates a Redis client and verifies that the server answers.
// ctx limits how long the check may take. clientName appears in Redis's
// CLIENT LIST, so you can tell which service opened a connection.
func Connect(ctx context.Context, cfg config.RedisConfig, clientName string) (*Client, error) {
	// NewClient prepares the pool but does not contact the server. Ping does.
	rdb := goredis.NewClient(&goredis.Options{
		Addr:         cfg.Addr,
		Password:     cfg.Password.Value(),
		DB:           cfg.DB,
		ClientName:   clientName,
		DialTimeout:  dialTimeout,
		ReadTimeout:  readTimeout,
		WriteTimeout: writeTimeout,
		// Respect context deadlines and cancellation on every command, so a
		// shutting-down service is never stuck inside a blocking call.
		ContextTimeoutEnabled: true,
	})

	c := &Client{rdb: rdb}

	if err := c.Ping(ctx); err != nil {
		_ = rdb.Close()
		return nil, fmt.Errorf("ping %s: %w", cfg.Addr, err)
	}

	return c, nil
}

// Redis returns the underlying go-redis client.
func (c *Client) Redis() *goredis.Client {
	return c.rdb
}

// Ping checks that the server answers.
func (c *Client) Ping(ctx context.Context) error {
	return c.rdb.Ping(ctx).Err()
}

// Close closes every pooled connection.
func (c *Client) Close() error {
	if err := c.rdb.Close(); err != nil {
		return fmt.Errorf("close Redis: %w", err)
	}
	return nil
}
