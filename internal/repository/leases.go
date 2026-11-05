package repository

import (
	"context"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// leaderDocID is the single document that records scheduler leadership.
const leaderDocID = "scheduler-leader"

// Leases is the repository for scheduler_leases.
type Leases struct {
	coll *mongo.Collection
}

// NextFencingToken increments and returns the leadership fencing token.
// Each new leader calls it once; tokens only ever grow, so a write
// conditioned on "my token >= the stored token" rejects stale leaders.
func (r *Leases) NextFencingToken(ctx context.Context, owner string) (int64, error) {
	var doc struct {
		FencingToken int64 `bson:"fencing_token"`
	}
	err := r.coll.FindOneAndUpdate(ctx,
		bson.D{{Key: "_id", Value: leaderDocID}},
		bson.D{
			{Key: "$inc", Value: bson.D{{Key: "fencing_token", Value: int64(1)}}},
			{Key: "$set", Value: bson.D{{Key: "lease_owner", Value: owner}}},
			{Key: "$currentDate", Value: bson.D{{Key: "acquired_at", Value: true}, {Key: "renewed_at", Value: true}}},
		},
		options.FindOneAndUpdate().SetUpsert(true).SetReturnDocument(options.After),
	).Decode(&doc)
	if err != nil {
		return 0, wrap("next fencing token", err)
	}
	return doc.FencingToken, nil
}

// RecordRenewal updates renewed_at for visibility. Redis holds the real timer.
func (r *Leases) RecordRenewal(ctx context.Context, owner string, token int64) error {
	_, err := r.coll.UpdateOne(ctx,
		bson.D{{Key: "_id", Value: leaderDocID}, {Key: "lease_owner", Value: owner}, {Key: "fencing_token", Value: token}},
		bson.D{{Key: "$currentDate", Value: bson.D{{Key: "renewed_at", Value: true}}}},
	)
	return wrap("record lease renewal", err)
}
