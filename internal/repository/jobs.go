package repository

import (
	"context"
	"errors"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/anushah13/distributed-job-scheduler/internal/idgen"
	"github.com/anushah13/distributed-job-scheduler/internal/models"
)

// Jobs is the repository for the jobs collection.
type Jobs struct {
	coll *mongo.Collection
}

// Claim identifies one worker's ownership of a running job.
type Claim struct {
	JobID string
	Token string
	// Expired adds the condition "the lease has expired (by MongoDB's clock)".
	// The reconciler sets it, so it can never take a job whose worker has
	// just renewed the lease.
	Expired bool
}

func (c Claim) filter() bson.D {
	f := bson.D{
		{Key: "_id", Value: c.JobID},
		{Key: "status", Value: models.StatusRunning},
		{Key: "claim_token", Value: c.Token},
	}
	if c.Expired {
		f = append(f, bson.E{Key: "$expr", Value: bson.D{{Key: "$lt", Value: bson.A{"$lease_expires_at", "$$NOW"}}}})
	}
	return f
}

// Insert stores a new job. It returns ErrDuplicateIdempotencyKey when the
// unique idempotency index rejects it.
func (r *Jobs) Insert(ctx context.Context, job *models.Job) error {
	_, err := r.coll.InsertOne(ctx, job)
	if mongo.IsDuplicateKeyError(err) {
		return ErrDuplicateIdempotencyKey
	}
	return wrap("insert job", err)
}

// Get returns one job.
func (r *Jobs) Get(ctx context.Context, id string) (*models.Job, error) {
	return r.findOne(ctx, bson.D{{Key: "_id", Value: id}})
}

// GetByIdempotencyKey returns the job created with key.
func (r *Jobs) GetByIdempotencyKey(ctx context.Context, key string) (*models.Job, error) {
	return r.findOne(ctx, bson.D{{Key: "idempotency_key", Value: key}})
}

func (r *Jobs) findOne(ctx context.Context, filter bson.D) (*models.Job, error) {
	var job models.Job
	err := r.coll.FindOne(ctx, filter).Decode(&job)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, wrap("find job", err)
	}
	return &job, nil
}

// Cursor marks a position in the (created_at desc, _id desc) ordering.
type Cursor struct {
	CreatedAt time.Time `json:"t"`
	ID        string    `json:"id"`
}

// ListFilter selects jobs for List. Zero values mean "no filter".
type ListFilter struct {
	Status        models.JobStatus
	JobType       string
	CreatedAfter  time.Time
	CreatedBefore time.Time
	After         *Cursor
	Limit         int
}

// List returns up to filter.Limit jobs, newest first, without payloads.
func (r *Jobs) List(ctx context.Context, filter ListFilter) ([]models.Job, error) {
	q := bson.D{}
	if filter.Status != "" {
		q = append(q, bson.E{Key: "status", Value: filter.Status})
	}
	if filter.JobType != "" {
		q = append(q, bson.E{Key: "job_type", Value: filter.JobType})
	}
	created := bson.D{}
	if !filter.CreatedAfter.IsZero() {
		created = append(created, bson.E{Key: "$gte", Value: filter.CreatedAfter})
	}
	if !filter.CreatedBefore.IsZero() {
		created = append(created, bson.E{Key: "$lt", Value: filter.CreatedBefore})
	}
	if len(created) > 0 {
		q = append(q, bson.E{Key: "created_at", Value: created})
	}
	if c := filter.After; c != nil {
		// Keyset pagination: rows strictly after the cursor in sort order.
		// _id breaks ties between jobs created in the same millisecond.
		q = append(q, bson.E{Key: "$or", Value: bson.A{
			bson.D{{Key: "created_at", Value: bson.D{{Key: "$lt", Value: c.CreatedAt}}}},
			bson.D{{Key: "created_at", Value: c.CreatedAt}, {Key: "_id", Value: bson.D{{Key: "$lt", Value: c.ID}}}},
		}})
	}

	opts := options.Find().
		SetSort(bson.D{{Key: "created_at", Value: -1}, {Key: "_id", Value: -1}}).
		SetLimit(int64(filter.Limit)).
		SetProjection(bson.D{{Key: "payload", Value: 0}})

	cur, err := r.coll.Find(ctx, q, opts)
	if err != nil {
		return nil, wrap("list jobs", err)
	}
	var jobs []models.Job
	if err := cur.All(ctx, &jobs); err != nil {
		return nil, wrap("list jobs", err)
	}
	return jobs, nil
}

// MarkQueued moves a due job from scheduled or retrying to queued.
// The fencing condition stops a stale leader from overwriting a job that a
// newer leader (with a higher token) has already handled.
func (r *Jobs) MarkQueued(ctx context.Context, id string, fencingToken int64, now time.Time) (bool, error) {
	res, err := r.coll.UpdateOne(ctx,
		bson.D{
			{Key: "_id", Value: id},
			{Key: "status", Value: bson.D{{Key: "$in", Value: bson.A{models.StatusScheduled, models.StatusRetrying}}}},
			{Key: "next_run_at", Value: bson.D{{Key: "$lte", Value: now}}},
			{Key: "last_scheduler_token", Value: bson.D{{Key: "$lte", Value: fencingToken}}},
		},
		bson.D{
			{Key: "$set", Value: bson.D{
				{Key: "status", Value: models.StatusQueued},
				{Key: "last_scheduler_token", Value: fencingToken},
			}},
			{Key: "$currentDate", Value: bson.D{{Key: "queued_at", Value: true}, {Key: "updated_at", Value: true}}},
		},
	)
	if err != nil {
		return false, wrap("mark job queued", err)
	}
	return res.ModifiedCount == 1, nil
}

// Claim atomically moves a queued job to running and gives it to workerID.
// It returns (nil, nil) when the job is no longer queued (another worker
// claimed it, or it was cancelled). The lease expiry is computed with
// MongoDB's clock ($$NOW), so clock differences between machines don't matter.
func (r *Jobs) Claim(ctx context.Context, id, workerID string, lease time.Duration) (*models.Job, error) {
	update := mongo.Pipeline{
		{{Key: "$set", Value: bson.D{
			{Key: "status", Value: models.StatusRunning},
			{Key: "lease_owner", Value: bson.D{{Key: "$literal", Value: workerID}}},
			{Key: "claim_token", Value: idgen.Token()},
			{Key: "lease_expires_at", Value: bson.D{{Key: "$add", Value: bson.A{"$$NOW", lease.Milliseconds()}}}},
			{Key: "updated_at", Value: "$$NOW"},
		}}},
		{{Key: "$unset", Value: "queued_at"}},
	}
	var job models.Job
	err := r.coll.FindOneAndUpdate(ctx,
		bson.D{{Key: "_id", Value: id}, {Key: "status", Value: models.StatusQueued}},
		update,
		options.FindOneAndUpdate().SetReturnDocument(options.After),
	).Decode(&job)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, nil
	}
	if err != nil {
		return nil, wrap("claim job", err)
	}
	return &job, nil
}

// RenewLease extends the lease of a running job and reports whether
// cancellation has been requested. It returns ErrLeaseLost when the claim
// is no longer valid; the worker must then stop the job.
func (r *Jobs) RenewLease(ctx context.Context, claim Claim, lease time.Duration) (cancelRequested bool, err error) {
	var got struct {
		CancelRequested bool `bson:"cancel_requested"`
	}
	err = r.coll.FindOneAndUpdate(ctx, claim.filter(),
		mongo.Pipeline{{{Key: "$set", Value: bson.D{
			{Key: "lease_expires_at", Value: bson.D{{Key: "$add", Value: bson.A{"$$NOW", lease.Milliseconds()}}}},
			{Key: "updated_at", Value: "$$NOW"},
		}}}},
		options.FindOneAndUpdate().
			SetReturnDocument(options.After).
			SetProjection(bson.D{{Key: "cancel_requested", Value: 1}}),
	).Decode(&got)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return false, ErrLeaseLost
	}
	if err != nil {
		return false, wrap("renew lease", err)
	}
	return got.CancelRequested, nil
}

// finish applies a transition that ends the current run of a claimed job.
func (r *Jobs) finish(ctx context.Context, op string, claim Claim, set bson.D, inc bson.D) error {
	update := bson.D{
		{Key: "$set", Value: set},
		{Key: "$unset", Value: bson.D{{Key: "lease_expires_at", Value: ""}, {Key: "claim_token", Value: ""}}},
		{Key: "$currentDate", Value: bson.D{{Key: "updated_at", Value: true}}},
	}
	if len(inc) > 0 {
		update = append(update, bson.E{Key: "$inc", Value: inc})
	}
	res, err := r.coll.UpdateOne(ctx, claim.filter(), update)
	if err != nil {
		return wrap(op, err)
	}
	if res.MatchedCount == 0 {
		return ErrLeaseLost
	}
	return nil
}

// Complete marks a one-time job as completed.
func (r *Jobs) Complete(ctx context.Context, claim Claim, now time.Time) error {
	return r.finish(ctx, "complete job", claim, bson.D{
		{Key: "status", Value: models.StatusCompleted},
		{Key: "completed_at", Value: now},
		{Key: "last_error", Value: ""},
	}, nil)
}

// Retry schedules another attempt of the same occurrence at next.
func (r *Jobs) Retry(ctx context.Context, claim Claim, next time.Time, lastErr string) error {
	return r.finish(ctx, "retry job", claim, bson.D{
		{Key: "status", Value: models.StatusRetrying},
		{Key: "next_run_at", Value: next},
		{Key: "last_error", Value: lastErr},
	}, bson.D{{Key: "retry_count", Value: 1}})
}

// Reschedule starts the next occurrence of a recurring job at next.
// lastErr is empty after success, or the error of a dead-lettered occurrence.
func (r *Jobs) Reschedule(ctx context.Context, claim Claim, next time.Time, lastErr string) error {
	return r.finish(ctx, "reschedule job", claim, bson.D{
		{Key: "status", Value: models.StatusScheduled},
		{Key: "next_run_at", Value: next},
		{Key: "occurrence_at", Value: next},
		{Key: "retry_count", Value: 0},
		{Key: "last_error", Value: lastErr},
	}, nil)
}

// DeadLetter marks a one-time job as dead-lettered.
func (r *Jobs) DeadLetter(ctx context.Context, claim Claim, lastErr string) error {
	return r.finish(ctx, "dead-letter job", claim, bson.D{
		{Key: "status", Value: models.StatusDeadLettered},
		{Key: "last_error", Value: lastErr},
	}, nil)
}

// MarkCancelled finishes a running job whose cancellation was requested.
func (r *Jobs) MarkCancelled(ctx context.Context, claim Claim, now time.Time) error {
	return r.finish(ctx, "cancel job", claim, bson.D{
		{Key: "status", Value: models.StatusCancelled},
		{Key: "completed_at", Value: now},
	}, nil)
}

// CancelOutcome describes what Cancel did.
type CancelOutcome int

// Cancel outcomes.
const (
	// Cancelled: the job was waiting and is now cancelled.
	Cancelled CancelOutcome = iota
	// CancelRequested: the job is running; its worker will stop it.
	CancelRequested
	// AlreadyCancelled: nothing to do (cancel is idempotent).
	AlreadyCancelled
)

// Cancel cancels a waiting job immediately, or asks the worker running it
// to stop. Completed and dead-lettered jobs return ErrInvalidState.
func (r *Jobs) Cancel(ctx context.Context, id string, now time.Time) (CancelOutcome, error) {
	res, err := r.coll.UpdateOne(ctx,
		bson.D{
			{Key: "_id", Value: id},
			{Key: "status", Value: bson.D{{Key: "$in", Value: bson.A{models.StatusScheduled, models.StatusRetrying, models.StatusQueued}}}},
		},
		bson.D{
			{Key: "$set", Value: bson.D{{Key: "status", Value: models.StatusCancelled}, {Key: "completed_at", Value: now}}},
			{Key: "$currentDate", Value: bson.D{{Key: "updated_at", Value: true}}},
		},
	)
	if err != nil {
		return 0, wrap("cancel job", err)
	}
	if res.MatchedCount == 1 {
		return Cancelled, nil
	}

	res, err = r.coll.UpdateOne(ctx,
		bson.D{{Key: "_id", Value: id}, {Key: "status", Value: models.StatusRunning}},
		bson.D{
			{Key: "$set", Value: bson.D{{Key: "cancel_requested", Value: true}}},
			{Key: "$currentDate", Value: bson.D{{Key: "updated_at", Value: true}}},
		},
	)
	if err != nil {
		return 0, wrap("request cancellation", err)
	}
	if res.MatchedCount == 1 {
		return CancelRequested, nil
	}

	job, err := r.Get(ctx, id)
	if err != nil {
		return 0, err
	}
	if job.Status == models.StatusCancelled {
		return AlreadyCancelled, nil
	}
	return 0, ErrInvalidState
}

// ManualRetry restarts a dead-lettered job as a fresh occurrence at now.
func (r *Jobs) ManualRetry(ctx context.Context, id string, now time.Time) error {
	res, err := r.coll.UpdateOne(ctx,
		bson.D{{Key: "_id", Value: id}, {Key: "status", Value: models.StatusDeadLettered}},
		bson.D{
			{Key: "$set", Value: bson.D{
				{Key: "status", Value: models.StatusScheduled},
				{Key: "retry_count", Value: 0},
				{Key: "next_run_at", Value: now},
				{Key: "occurrence_at", Value: now},
				{Key: "cancel_requested", Value: false},
			}},
			{Key: "$unset", Value: bson.D{{Key: "completed_at", Value: ""}}},
			{Key: "$currentDate", Value: bson.D{{Key: "updated_at", Value: true}}},
		},
	)
	if err != nil {
		return wrap("retry dead-lettered job", err)
	}
	if res.MatchedCount == 0 {
		if _, err := r.Get(ctx, id); err != nil {
			return err
		}
		return ErrInvalidState
	}
	return nil
}

// ExpiredLeases returns running jobs whose lease has expired by MongoDB's clock.
func (r *Jobs) ExpiredLeases(ctx context.Context, limit int) ([]models.Job, error) {
	return r.find(ctx, "find expired leases", bson.D{
		{Key: "status", Value: models.StatusRunning},
		{Key: "$expr", Value: bson.D{{Key: "$lt", Value: bson.A{"$lease_expires_at", "$$NOW"}}}},
	}, limit)
}

// StuckQueued returns jobs that have been queued since before olderThan.
// Their ID was probably lost from the Redis ready list (for example a worker
// crashed right after popping it).
func (r *Jobs) StuckQueued(ctx context.Context, olderThan time.Time, limit int) ([]models.Job, error) {
	return r.find(ctx, "find stuck queued jobs", bson.D{
		{Key: "status", Value: models.StatusQueued},
		{Key: "queued_at", Value: bson.D{{Key: "$lt", Value: olderThan}}},
	}, limit)
}

// TouchQueued resets queued_at after the reconciler re-pushes a job, so the
// same job is not re-pushed on every scan.
func (r *Jobs) TouchQueued(ctx context.Context, id string) error {
	_, err := r.coll.UpdateOne(ctx,
		bson.D{{Key: "_id", Value: id}, {Key: "status", Value: models.StatusQueued}},
		bson.D{{Key: "$currentDate", Value: bson.D{{Key: "queued_at", Value: true}}}},
	)
	return wrap("touch queued job", err)
}

// Overdue returns waiting jobs whose run time passed before `before`.
// A healthy scheduler moves due jobs within a second, so these are probably
// missing from Redis (a failed write after MongoDB accepted the job).
func (r *Jobs) Overdue(ctx context.Context, before time.Time, limit int) ([]models.Job, error) {
	return r.find(ctx, "find overdue jobs", bson.D{
		{Key: "status", Value: bson.D{{Key: "$in", Value: bson.A{models.StatusScheduled, models.StatusRetrying}}}},
		{Key: "next_run_at", Value: bson.D{{Key: "$lt", Value: before}}},
	}, limit)
}

// EachPending calls fn for every scheduled, retrying or queued job. The
// reconciler uses it to rebuild Redis after Redis lost its data.
func (r *Jobs) EachPending(ctx context.Context, fn func(models.Job) error) error {
	cur, err := r.coll.Find(ctx,
		bson.D{{Key: "status", Value: bson.D{{Key: "$in", Value: bson.A{
			models.StatusScheduled, models.StatusRetrying, models.StatusQueued,
		}}}}},
		options.Find().SetProjection(bson.D{{Key: "_id", Value: 1}, {Key: "status", Value: 1}, {Key: "next_run_at", Value: 1}}),
	)
	if err != nil {
		return wrap("scan pending jobs", err)
	}
	defer cur.Close(ctx)
	for cur.Next(ctx) {
		var job models.Job
		if err := cur.Decode(&job); err != nil {
			return wrap("decode pending job", err)
		}
		if err := fn(job); err != nil {
			return err
		}
	}
	return wrap("scan pending jobs", cur.Err())
}

func (r *Jobs) find(ctx context.Context, op string, filter bson.D, limit int) ([]models.Job, error) {
	cur, err := r.coll.Find(ctx, filter, options.Find().SetLimit(int64(limit)))
	if err != nil {
		return nil, wrap(op, err)
	}
	var jobs []models.Job
	if err := cur.All(ctx, &jobs); err != nil {
		return nil, wrap(op, err)
	}
	return jobs, nil
}
