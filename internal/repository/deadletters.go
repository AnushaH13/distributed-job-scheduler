package repository

import (
	"context"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/anushah13/distributed-job-scheduler/internal/models"
)

// DeadLetters is the repository for dead_letter_jobs.
type DeadLetters struct {
	coll *mongo.Collection
}

// Insert records an incident. A second insert for the same pending job
// occurrence is ignored, so callers can safely repeat it after a crash.
func (r *DeadLetters) Insert(ctx context.Context, dl *models.DeadLetter) error {
	_, err := r.coll.InsertOne(ctx, dl)
	if mongo.IsDuplicateKeyError(err) {
		return nil
	}
	return wrap("insert dead letter", err)
}

// ListPending returns open incidents, newest first.
func (r *DeadLetters) ListPending(ctx context.Context, limit int) ([]models.DeadLetter, error) {
	cur, err := r.coll.Find(ctx,
		bson.D{{Key: "resolution_status", Value: models.ResolutionPending}},
		options.Find().SetSort(bson.D{{Key: "dead_lettered_at", Value: -1}}).SetLimit(int64(limit)),
	)
	if err != nil {
		return nil, wrap("list dead letters", err)
	}
	out := []models.DeadLetter{}
	if err := cur.All(ctx, &out); err != nil {
		return nil, wrap("list dead letters", err)
	}
	return out, nil
}

// MarkRetried resolves every pending incident of a job as "retried".
func (r *DeadLetters) MarkRetried(ctx context.Context, jobID, note string, now time.Time) error {
	_, err := r.coll.UpdateMany(ctx,
		bson.D{{Key: "job_id", Value: jobID}, {Key: "resolution_status", Value: models.ResolutionPending}},
		bson.D{{Key: "$set", Value: bson.D{
			{Key: "resolution_status", Value: models.ResolutionRetried},
			{Key: "resolved_at", Value: now},
			{Key: "resolution_note", Value: note},
		}}},
	)
	return wrap("resolve dead letters", err)
}
