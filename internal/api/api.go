// Package api serves the REST API under /api/v1.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/anushah13/distributed-job-scheduler/internal/health"
	"github.com/anushah13/distributed-job-scheduler/internal/middleware"
	"github.com/anushah13/distributed-job-scheduler/internal/models"
	"github.com/anushah13/distributed-job-scheduler/internal/repository"
	"github.com/anushah13/distributed-job-scheduler/internal/services"
)

// maxBodyBytes allows a 64 KB payload plus the rest of the request.
const maxBodyBytes = services.MaxPayloadBytes + 16*1024

// JobService is what the handlers need. *services.JobService implements it;
// tests use a fake.
type JobService interface {
	Create(ctx context.Context, req services.CreateRequest, idempotencyKey string) (*models.Job, bool, error)
	Get(ctx context.Context, id string) (*models.Job, error)
	List(ctx context.Context, p services.ListParams) (services.ListResult, error)
	Cancel(ctx context.Context, id string) (*models.Job, repository.CancelOutcome, error)
	Retry(ctx context.Context, id string) (*models.Job, error)
	Executions(ctx context.Context, id string, limit int) ([]models.Execution, error)
}

type handler struct {
	svc    JobService
	logger *slog.Logger
}

// NewRouter returns the API's HTTP handler, including health endpoints.
func NewRouter(svc JobService, checker *health.Checker, logger *slog.Logger) http.Handler {
	h := &handler{svc: svc, logger: logger}
	mux := http.NewServeMux()

	// Go 1.22+ patterns: method and path wildcards, no router library needed.
	mux.HandleFunc("POST /api/v1/jobs", h.createJob)
	mux.HandleFunc("GET /api/v1/jobs", h.listJobs)
	mux.HandleFunc("GET /api/v1/jobs/{job_id}", h.getJob)
	mux.HandleFunc("POST /api/v1/jobs/{job_id}/cancel", h.cancelJob)
	mux.HandleFunc("POST /api/v1/jobs/{job_id}/retry", h.retryJob)
	mux.HandleFunc("GET /api/v1/jobs/{job_id}/executions", h.listExecutions)
	checker.Register(mux)

	return middleware.Chain(mux,
		middleware.RequestID,
		middleware.Logger(logger),
		middleware.Recover(logger),
	)
}

func (h *handler) createJob(w http.ResponseWriter, r *http.Request) {
	var req services.CreateRequest
	if err := decodeJSON(w, r, &req); err != nil {
		writeError(w, r, h.logger, err)
		return
	}

	job, replayed, err := h.svc.Create(r.Context(), req, r.Header.Get("Idempotency-Key"))
	if err != nil {
		writeError(w, r, h.logger, err)
		return
	}

	w.Header().Set("Location", "/api/v1/jobs/"+job.ID)
	if replayed {
		w.Header().Set("Idempotent-Replayed", "true")
		writeJSON(w, http.StatusOK, job)
		return
	}
	writeJSON(w, http.StatusCreated, job)
}

func (h *handler) getJob(w http.ResponseWriter, r *http.Request) {
	job, err := h.svc.Get(r.Context(), r.PathValue("job_id"))
	if err != nil {
		writeError(w, r, h.logger, err)
		return
	}
	writeJSON(w, http.StatusOK, job)
}

func (h *handler) listJobs(w http.ResponseWriter, r *http.Request) {
	p, err := parseListParams(r)
	if err != nil {
		writeError(w, r, h.logger, err)
		return
	}
	res, err := h.svc.List(r.Context(), p)
	if err != nil {
		writeError(w, r, h.logger, err)
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func (h *handler) cancelJob(w http.ResponseWriter, r *http.Request) {
	job, outcome, err := h.svc.Cancel(r.Context(), r.PathValue("job_id"))
	if err != nil {
		writeError(w, r, h.logger, err)
		return
	}
	status := http.StatusOK
	if outcome == repository.CancelRequested {
		// The worker stops the job cooperatively; the client polls for the result.
		status = http.StatusAccepted
	}
	writeJSON(w, status, job)
}

func (h *handler) retryJob(w http.ResponseWriter, r *http.Request) {
	job, err := h.svc.Retry(r.Context(), r.PathValue("job_id"))
	if err != nil {
		writeError(w, r, h.logger, err)
		return
	}
	writeJSON(w, http.StatusOK, job)
}

func (h *handler) listExecutions(w http.ResponseWriter, r *http.Request) {
	limit, err := parseLimit(r.URL.Query().Get("limit"))
	if err != nil {
		writeError(w, r, h.logger, err)
		return
	}
	execs, err := h.svc.Executions(r.Context(), r.PathValue("job_id"), limit)
	if err != nil {
		writeError(w, r, h.logger, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"executions": execs})
}

// decodeJSON reads exactly one JSON object and rejects unknown fields, so a
// typo like "max_retry" fails loudly instead of being silently ignored.
func decodeJSON(w http.ResponseWriter, r *http.Request, v any) error {
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			return err
		}
		if errors.Is(err, io.EOF) {
			return errInvalidJSON{"request body is empty"}
		}
		return errInvalidJSON{fmt.Sprintf("request body is not valid JSON: %v", err)}
	}
	if dec.More() {
		return errInvalidJSON{"request body must contain a single JSON object"}
	}
	return nil
}

func parseListParams(r *http.Request) (services.ListParams, error) {
	q := r.URL.Query()
	verr := &services.ValidationError{}
	p := services.ListParams{JobType: q.Get("job_type"), Cursor: q.Get("cursor")}

	if s := q.Get("status"); s != "" {
		p.Status = models.JobStatus(s)
		if !p.Status.Valid() {
			verr.Details = append(verr.Details, services.FieldIssue{Field: "status", Issue: "is not a known job status"})
		}
	}
	for _, f := range []struct {
		name string
		dst  *time.Time
	}{{"created_after", &p.CreatedAfter}, {"created_before", &p.CreatedBefore}} {
		if s := q.Get(f.name); s != "" {
			t, err := time.Parse(time.RFC3339, s)
			if err != nil {
				verr.Details = append(verr.Details, services.FieldIssue{Field: f.name, Issue: "must be an RFC 3339 time"})
				continue
			}
			*f.dst = t.UTC()
		}
	}
	limit, err := parseLimit(q.Get("limit"))
	if err != nil {
		verr.Details = append(verr.Details, err.(*services.ValidationError).Details...)
	}
	p.Limit = limit

	if len(verr.Details) > 0 {
		return p, verr
	}
	return p, nil
}

func parseLimit(s string) (int, error) {
	if s == "" {
		return 0, nil
	}
	n, err := strconv.Atoi(s)
	if err != nil || n < 1 || n > services.MaxListLimit {
		return 0, &services.ValidationError{Details: []services.FieldIssue{
			{Field: "limit", Issue: fmt.Sprintf("must be a whole number from 1 to %d", services.MaxListLimit)},
		}}
	}
	return n, nil
}
