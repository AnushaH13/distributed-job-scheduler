package api

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"

	"github.com/anushah13/distributed-job-scheduler/internal/middleware"
	"github.com/anushah13/distributed-job-scheduler/internal/services"
)

// Error codes. Clients branch on these, never on messages.
const (
	CodeValidationFailed       = "VALIDATION_FAILED"
	CodeInvalidJSON            = "INVALID_JSON"
	CodePayloadTooLarge        = "PAYLOAD_TOO_LARGE"
	CodeJobNotFound            = "JOB_NOT_FOUND"
	CodeInvalidStateTransition = "INVALID_STATE_TRANSITION"
	CodeIdempotencyKeyReused   = "IDEMPOTENCY_KEY_REUSED"
	CodeServiceUnavailable     = "SERVICE_UNAVAILABLE"
	CodeInternalError          = "INTERNAL_ERROR"
)

type errorBody struct {
	Error errorDetail `json:"error"`
}

type errorDetail struct {
	Code      string                `json:"code"`
	Message   string                `json:"message"`
	Details   []services.FieldIssue `json:"details,omitempty"`
	RequestID string                `json:"request_id"`
}

// errInvalidJSON wraps a body decoding problem.
type errInvalidJSON struct{ msg string }

func (e errInvalidJSON) Error() string { return e.msg }

// writeError maps an error to a status code and the standard error body.
// Unexpected errors are logged here (and only here) and hidden from clients.
func writeError(w http.ResponseWriter, r *http.Request, logger *slog.Logger, err error) {
	var (
		verr    *services.ValidationError
		jsonErr errInvalidJSON
		maxErr  *http.MaxBytesError
	)
	status, d := http.StatusInternalServerError, errorDetail{Code: CodeInternalError, Message: "internal error"}

	switch {
	case errors.As(err, &verr):
		status, d = http.StatusBadRequest, errorDetail{Code: CodeValidationFailed, Message: "request validation failed", Details: verr.Details}
	case errors.As(err, &jsonErr):
		status, d = http.StatusBadRequest, errorDetail{Code: CodeInvalidJSON, Message: jsonErr.msg}
	case errors.Is(err, services.ErrPayloadTooLarge), errors.As(err, &maxErr):
		status, d = http.StatusRequestEntityTooLarge, errorDetail{Code: CodePayloadTooLarge, Message: "payload must be at most 64 KB"}
	case errors.Is(err, services.ErrNotFound):
		status, d = http.StatusNotFound, errorDetail{Code: CodeJobNotFound, Message: "job not found"}
	case errors.Is(err, services.ErrInvalidState):
		status, d = http.StatusConflict, errorDetail{Code: CodeInvalidStateTransition, Message: "the job's current status does not allow this operation"}
	case errors.Is(err, services.ErrIdempotencyKeyReused):
		status, d = http.StatusConflict, errorDetail{Code: CodeIdempotencyKeyReused, Message: "this Idempotency-Key was already used with a different request"}
	case errors.Is(err, services.ErrUnavailable):
		logger.WarnContext(r.Context(), "dependency unavailable", "error", err, "request_id", middleware.RequestIDFrom(r.Context()))
		status, d = http.StatusServiceUnavailable, errorDetail{Code: CodeServiceUnavailable, Message: "the service is temporarily unavailable; retry later"}
		w.Header().Set("Retry-After", "5")
	default:
		logger.ErrorContext(r.Context(), "request failed", "error", err, "request_id", middleware.RequestIDFrom(r.Context()))
	}

	d.RequestID = middleware.RequestIDFrom(r.Context())
	writeJSON(w, status, errorBody{Error: d})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
