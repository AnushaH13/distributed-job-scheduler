package repository

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// EnsureIndexes creates every index the scheduler relies on. It is safe to
// call on every startup and from several processes at once: creating an
// index that already exists with the same definition does nothing.
func (s *Store) EnsureIndexes(ctx context.Context) error {
	sets := []struct {
		coll    *mongo.Collection
		indexes []mongo.IndexModel
	}{
		{s.Jobs.coll, []mongo.IndexModel{
			{
				// Unique only for documents that have a string key, so jobs
				// without a key never collide with each other.
				Keys: bson.D{{Key: "idempotency_key", Value: 1}},
				Options: options.Index().SetName("idempotency_key_unique").SetUnique(true).
					SetPartialFilterExpression(bson.D{{Key: "idempotency_key", Value: bson.D{{Key: "$type", Value: "string"}}}}),
			},
			// ESR order: Equality (status), then Sort/Range fields.
			{Keys: bson.D{{Key: "status", Value: 1}, {Key: "next_run_at", Value: 1}}},
			{Keys: bson.D{{Key: "status", Value: 1}, {Key: "lease_expires_at", Value: 1}}},
			{Keys: bson.D{{Key: "status", Value: 1}, {Key: "queued_at", Value: 1}}},
			{Keys: bson.D{{Key: "status", Value: 1}, {Key: "created_at", Value: -1}, {Key: "_id", Value: -1}}},
			{Keys: bson.D{{Key: "job_type", Value: 1}, {Key: "created_at", Value: -1}, {Key: "_id", Value: -1}}},
			{Keys: bson.D{{Key: "created_at", Value: -1}, {Key: "_id", Value: -1}}},
		}},
		{s.Executions.coll, []mongo.IndexModel{
			{Keys: bson.D{{Key: "job_id", Value: 1}, {Key: "started_at", Value: -1}}},
			{
				Keys:    bson.D{{Key: "job_id", Value: 1}, {Key: "occurrence_at", Value: 1}, {Key: "attempt_number", Value: 1}},
				Options: options.Index().SetUnique(true),
			},
			{Keys: bson.D{{Key: "execution_status", Value: 1}, {Key: "started_at", Value: -1}}},
		}},
		{s.DeadLetters.coll, []mongo.IndexModel{
			{
				// At most one open incident per job occurrence.
				Keys: bson.D{{Key: "job_id", Value: 1}, {Key: "occurrence_at", Value: 1}},
				Options: options.Index().SetName("pending_occurrence_unique").SetUnique(true).
					SetPartialFilterExpression(bson.D{{Key: "resolution_status", Value: "pending"}}),
			},
			{Keys: bson.D{{Key: "resolution_status", Value: 1}, {Key: "dead_lettered_at", Value: -1}}},
			{Keys: bson.D{{Key: "job_type", Value: 1}, {Key: "dead_lettered_at", Value: -1}}},
		}},
	}

	for _, set := range sets {
		if _, err := set.coll.Indexes().CreateMany(ctx, set.indexes); err != nil {
			return wrap(fmt.Sprintf("create indexes on %s", set.coll.Name()), err)
		}
	}
	return nil
}
