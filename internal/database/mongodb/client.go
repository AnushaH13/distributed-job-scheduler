// Package mongodb provides MongoDB connection management.
package mongodb

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"

	"github.com/anushah13/distributed-job-scheduler/internal/config"
)

const (
	// serverSelectionTimeout limits how long any operation waits to find a
	// usable server. The driver default is 30s, which is too long for an API
	// that should answer 503 quickly when MongoDB is down.
	serverSelectionTimeout = 5 * time.Second

	// connectTimeout limits how long opening one TCP connection may take.
	connectTimeout = 5 * time.Second

	// cleanupTimeout limits how long Connect waits to disconnect after a failed ping.
	cleanupTimeout = 5 * time.Second
)

// Client holds the MongoDB client and the application's database.
// Create one per process and share it: the driver manages a connection pool.
type Client struct {
	client *mongo.Client
	db     *mongo.Database
}

// Connect creates a MongoDB client and verifies that the primary server answers.
// ctx limits how long the check may take. appName appears in MongoDB's own
// logs, so you can tell which service opened a connection.
func Connect(ctx context.Context, cfg config.MongoConfig, appName string) (*Client, error) {
	opts := options.Client().
		ApplyURI(cfg.URI.Value()).
		SetAppName(appName).
		SetServerSelectionTimeout(serverSelectionTimeout).
		SetConnectTimeout(connectTimeout)

	// mongo.Connect validates the options and starts background monitoring,
	// but does not wait for the server. Ping does.
	mc, err := mongo.Connect(opts)
	if err != nil {
		return nil, fmt.Errorf("create client for %s: %w", cfg.SafeURI(), err)
	}

	c := &Client{client: mc, db: mc.Database(cfg.Database)}

	if err := c.Ping(ctx); err != nil {
		// Stop the background goroutines. ctx may already have expired,
		// so cleanup gets its own short deadline.
		closeCtx, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
		defer cancel()
		_ = mc.Disconnect(closeCtx)
		return nil, fmt.Errorf("ping %s: %w", cfg.SafeURI(), err)
	}

	return c, nil
}

// Database returns the application's database.
func (c *Client) Database() *mongo.Database {
	return c.db
}

// Ping checks that the primary server answers.
func (c *Client) Ping(ctx context.Context) error {
	return c.client.Ping(ctx, readpref.Primary())
}

// Close closes every pooled connection and stops background monitoring.
// ctx limits how long it waits for in-use connections to be returned.
func (c *Client) Close(ctx context.Context) error {
	if err := c.client.Disconnect(ctx); err != nil {
		return fmt.Errorf("disconnect MongoDB: %w", err)
	}
	return nil
}
