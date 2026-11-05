// Package repository reads and writes the scheduler's MongoDB collections.
//
// Every state change is a conditional update: the filter states what must
// still be true (status, claim token, fencing token), and a zero match count
// means another process got there first.
package repository

import (
	"errors"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// Collection names.
const (
	JobsCollection        = "jobs"
	ExecutionsCollection  = "job_executions"
	DeadLettersCollection = "dead_letter_jobs"
	LeasesCollection      = "scheduler_leases"
)

var (
	// ErrNotFound means the document does not exist.
	ErrNotFound = errors.New("not found")
	// ErrDuplicateIdempotencyKey means another job already uses the key.
	ErrDuplicateIdempotencyKey = errors.New("idempotency key already used")
	// ErrInvalidState means the job is not in a state that allows the change.
	ErrInvalidState = errors.New("invalid state transition")
	// ErrLeaseLost means the caller no longer owns the job: its claim token
	// no longer matches, usually because the lease expired and the
	// reconciler gave the job to someone else.
	ErrLeaseLost = errors.New("job lease lost")
	// ErrUnavailable means MongoDB could not be reached in time.
	ErrUnavailable = errors.New("database unavailable")
)

// Store groups the repositories. All share one *mongo.Database.
type Store struct {
	Jobs        *Jobs
	Executions  *Executions
	DeadLetters *DeadLetters
	Leases      *Leases
}

// New returns a Store for db.
func New(db *mongo.Database) *Store {
	return &Store{
		Jobs:        &Jobs{coll: collection(db, JobsCollection)},
		Executions:  &Executions{coll: collection(db, ExecutionsCollection)},
		DeadLetters: &DeadLetters{coll: collection(db, DeadLettersCollection)},
		Leases:      &Leases{coll: collection(db, LeasesCollection)},
	}
}

// collection decodes nested documents (job payloads) as map[string]any instead
// of the driver's default bson.D, so they encode to JSON as normal objects.
func collection(db *mongo.Database, name string) *mongo.Collection {
	return db.Collection(name, options.Collection().SetBSONOptions(&options.BSONOptions{
		DefaultDocumentMap: true,
	}))
}

// wrap adds context to err and marks connectivity problems with
// ErrUnavailable, so the API can answer 503 instead of 500.
func wrap(op string, err error) error {
	if err == nil {
		return nil
	}
	if mongo.IsTimeout(err) || mongo.IsNetworkError(err) || errors.Is(err, mongo.ErrClientDisconnected) {
		return fmt.Errorf("%s: %w: %w", op, ErrUnavailable, err)
	}
	return fmt.Errorf("%s: %w", op, err)
}
