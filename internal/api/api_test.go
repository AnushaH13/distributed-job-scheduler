package api

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/anushah13/distributed-job-scheduler/internal/health"
	"github.com/anushah13/distributed-job-scheduler/internal/models"
	"github.com/anushah13/distributed-job-scheduler/internal/repository"
	"github.com/anushah13/distributed-job-scheduler/internal/services"
)

// fakeService returns canned results, so these tests cover only HTTP:
// routing, decoding, status codes, headers and the error format.
type fakeService struct {
	createErr    error
	replayed     bool
	getErr       error
	cancel       repository.CancelOutcome
	gotKey       string
	gotListLimit int
}

func (f *fakeService) Create(_ context.Context, req services.CreateRequest, key string) (*models.Job, bool, error) {
	f.gotKey = key
	if f.createErr != nil {
		return nil, false, f.createErr
	}
	return &models.Job{ID: "job_1", JobType: req.JobType, Status: models.StatusScheduled, ClaimToken: "secret-token"}, f.replayed, nil
}
func (f *fakeService) Get(context.Context, string) (*models.Job, error) {
	if f.getErr != nil {
		return nil, f.getErr
	}
	return &models.Job{ID: "job_1"}, nil
}
func (f *fakeService) List(_ context.Context, p services.ListParams) (services.ListResult, error) {
	f.gotListLimit = p.Limit
	return services.ListResult{Jobs: []models.Job{}}, nil
}
func (f *fakeService) Cancel(context.Context, string) (*models.Job, repository.CancelOutcome, error) {
	return &models.Job{ID: "job_1"}, f.cancel, nil
}
func (f *fakeService) Retry(context.Context, string) (*models.Job, error) {
	return &models.Job{ID: "job_1"}, nil
}
func (f *fakeService) Executions(context.Context, string, int) ([]models.Execution, error) {
	return []models.Execution{}, nil
}

func do(t *testing.T, svc JobService, method, path, body string, headers map[string]string) *httptest.ResponseRecorder {
	t.Helper()
	router := NewRouter(svc, health.New(), slog.New(slog.NewTextHandler(io.Discard, nil)))
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	return rec
}

func errorCode(t *testing.T, rec *httptest.ResponseRecorder) string {
	t.Helper()
	var body errorBody
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("error body is not JSON: %s", rec.Body)
	}
	if body.Error.RequestID == "" {
		t.Error("error body has no request_id")
	}
	return body.Error.Code
}

const validBody = `{"job_type":"echo","schedule":{"type":"immediate"}}`

func TestCreateReturns201WithLocation(t *testing.T) {
	svc := &fakeService{}
	rec := do(t, svc, "POST", "/api/v1/jobs", validBody, map[string]string{"Idempotency-Key": "abc"})
	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201: %s", rec.Code, rec.Body)
	}
	if rec.Header().Get("Location") != "/api/v1/jobs/job_1" {
		t.Errorf("Location = %q", rec.Header().Get("Location"))
	}
	if svc.gotKey != "abc" {
		t.Errorf("Idempotency-Key not passed through: %q", svc.gotKey)
	}
	if rec.Header().Get("X-Request-ID") == "" {
		t.Error("missing X-Request-ID")
	}
	if strings.Contains(rec.Body.String(), "secret-token") {
		t.Error("response exposes the claim token")
	}
}

func TestCreateReplayReturns200(t *testing.T) {
	rec := do(t, &fakeService{replayed: true}, "POST", "/api/v1/jobs", validBody, nil)
	if rec.Code != http.StatusOK || rec.Header().Get("Idempotent-Replayed") != "true" {
		t.Errorf("status = %d, Idempotent-Replayed = %q", rec.Code, rec.Header().Get("Idempotent-Replayed"))
	}
}

func TestErrorMapping(t *testing.T) {
	tests := []struct {
		name     string
		svc      *fakeService
		method   string
		path     string
		body     string
		wantCode int
		wantErr  string
	}{
		{"bad json", &fakeService{}, "POST", "/api/v1/jobs", `{"job_type":`, 400, CodeInvalidJSON},
		{"unknown field", &fakeService{}, "POST", "/api/v1/jobs", `{"job_type":"echo","max_retry":3}`, 400, CodeInvalidJSON},
		{"empty body", &fakeService{}, "POST", "/api/v1/jobs", ``, 400, CodeInvalidJSON},
		{"validation", &fakeService{createErr: &services.ValidationError{Details: []services.FieldIssue{{Field: "job_type", Issue: "is required"}}}}, "POST", "/api/v1/jobs", validBody, 400, CodeValidationFailed},
		{"too large", &fakeService{}, "POST", "/api/v1/jobs", `{"payload":{"x":"` + strings.Repeat("a", maxBodyBytes) + `"}}`, 413, CodePayloadTooLarge},
		{"key reused", &fakeService{createErr: services.ErrIdempotencyKeyReused}, "POST", "/api/v1/jobs", validBody, 409, CodeIdempotencyKeyReused},
		{"mongo down", &fakeService{createErr: fmt.Errorf("insert: %w", services.ErrUnavailable)}, "POST", "/api/v1/jobs", validBody, 503, CodeServiceUnavailable},
		{"not found", &fakeService{getErr: services.ErrNotFound}, "GET", "/api/v1/jobs/job_x", "", 404, CodeJobNotFound},
		{"unexpected", &fakeService{getErr: fmt.Errorf("boom")}, "GET", "/api/v1/jobs/job_x", "", 500, CodeInternalError},
		{"bad limit", &fakeService{}, "GET", "/api/v1/jobs?limit=500", "", 400, CodeValidationFailed},
		{"bad status", &fakeService{}, "GET", "/api/v1/jobs?status=done", "", 400, CodeValidationFailed},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := do(t, tt.svc, tt.method, tt.path, tt.body, nil)
			if rec.Code != tt.wantCode {
				t.Fatalf("status = %d, want %d: %s", rec.Code, tt.wantCode, rec.Body)
			}
			if got := errorCode(t, rec); got != tt.wantErr {
				t.Errorf("code = %s, want %s", got, tt.wantErr)
			}
		})
	}
}

func TestUnexpectedErrorsAreHidden(t *testing.T) {
	rec := do(t, &fakeService{getErr: fmt.Errorf("mongo: secret internals")}, "GET", "/api/v1/jobs/job_x", "", nil)
	if strings.Contains(rec.Body.String(), "secret internals") {
		t.Errorf("500 response leaks internal error: %s", rec.Body)
	}
}

func TestCancelRunningReturns202(t *testing.T) {
	rec := do(t, &fakeService{cancel: repository.CancelRequested}, "POST", "/api/v1/jobs/job_1/cancel", "", nil)
	if rec.Code != http.StatusAccepted {
		t.Errorf("status = %d, want 202", rec.Code)
	}
	rec = do(t, &fakeService{cancel: repository.AlreadyCancelled}, "POST", "/api/v1/jobs/job_1/cancel", "", nil)
	if rec.Code != http.StatusOK {
		t.Errorf("already cancelled: status = %d, want 200", rec.Code)
	}
}

func TestListPassesLimit(t *testing.T) {
	svc := &fakeService{}
	rec := do(t, svc, "GET", "/api/v1/jobs?limit=7&status=queued", "", nil)
	if rec.Code != http.StatusOK || svc.gotListLimit != 7 {
		t.Errorf("status = %d, limit = %d", rec.Code, svc.gotListLimit)
	}
}

func TestWrongMethodIs405(t *testing.T) {
	rec := do(t, &fakeService{}, "DELETE", "/api/v1/jobs/job_1", "", nil)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("status = %d, want 405", rec.Code)
	}
}

func TestIncomingRequestIDIsKept(t *testing.T) {
	rec := do(t, &fakeService{}, "GET", "/api/v1/jobs/job_1", "", map[string]string{"X-Request-ID": "trace-123"})
	if rec.Header().Get("X-Request-ID") != "trace-123" {
		t.Errorf("X-Request-ID = %q, want trace-123", rec.Header().Get("X-Request-ID"))
	}
}
