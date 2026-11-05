package repository

import (
	"context"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/anushah13/distributed-job-scheduler/internal/models"
)

// Executions is the repository for job_executions (one record per attempt).
type Executions struct {
	coll *mongo.Collection
}

// Start records the beginning of an attempt.
func (r *Executions) Start(ctx context.Context, exec *models.Execution) error {
	_, err := r.coll.InsertOne(ctx, exec)
	return wrap("start execution", err)
}

// Finish records the outcome of a running attempt.
func (r *Executions) Finish(ctx context.Context, id string, status models.ExecutionStatus,
	startedAt, completedAt time.Time, errMsg string, errType models.ErrorType) error {
	set := bson.D{
		{Key: "execution_status", Value: status},
		{Key: "completed_at", Value: completedAt},
		{Key: "duration_ms", Value: completedAt.Sub(startedAt).Milliseconds()},
	}
	if errMsg != "" {
		set = append(set, bson.E{Key: "error_message", Value: errMsg}, bson.E{Key: "error_type", Value: errType})
	}
	_, err := r.coll.UpdateOne(ctx,
		bson.D{{Key: "_id", Value: id}, {Key: "execution_status", Value: models.ExecRunning}},
		bson.D{{Key: "$set", Value: set}},
	)
	return wrap("finish execution", err)
}

// Abandon marks the running attempt of a claim as abandoned. The reconciler
// calls it when a worker stopped renewing its lease (crash, network loss).
func (r *Executions) Abandon(ctx context.Context, jobID, claimToken string, now time.Time) error {
	_, err := r.coll.UpdateMany(ctx,
		bson.D{
			{Key: "job_id", Value: jobID},
			{Key: "claim_token", Value: claimToken},
			{Key: "execution_status", Value: models.ExecRunning},
		},
		bson.D{{Key: "$set", Value: bson.D{
			{Key: "execution_status", Value: models.ExecAbandoned},
			{Key: "completed_at", Value: now},
			{Key: "error_message", Value: "worker stopped renewing its lease"},
			{Key: "error_type", Value: models.ErrorTransient},
		}}},
	)
	return wrap("abandon execution", err)
}

// ListByJob returns a job's attempts, newest first.
func (r *Executions) ListByJob(ctx context.Context, jobID string, limit int) ([]models.Execution, error) {
	cur, err := r.coll.Find(ctx,
		bson.D{{Key: "job_id", Value: jobID}},
		options.Find().SetSort(bson.D{{Key: "started_at", Value: -1}}).SetLimit(int64(limit)),
	)
	if err != nil {
		return nil, wrap("list executions", err)
	}
	execs := []models.Execution{}
	if err := cur.All(ctx, &execs); err != nil {
		return nil, wrap("list executions", err)
	}
	return execs, nil
}
