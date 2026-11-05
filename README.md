# Distributed Job Scheduler

A distributed job scheduler in Go: immediate, delayed and recurring (cron) jobs,
a bounded worker pool, retries with exponential backoff and jitter, a
dead-letter queue, cancellation, idempotency keys, leader election with
fencing tokens, and graceful shutdown. MongoDB is the source of truth;
Redis holds timers and the work list, and can always be rebuilt from MongoDB.

How the pieces fit together: [Architecture](#architecture) below.
Cloud deployment and costs: [docs/deploy-eks.md](docs/deploy-eks.md).

## Architecture

```
            POST /api/v1/jobs
client ───────────────▶  api (N)  ──1. insert──▶  MongoDB (jobs, executions, DLQ, leases)
                           │                       ▲      ▲
                           └──2. ZADD js:delayed──▶ Redis │      │
                                                   │      │      │
       scheduler (N, 1 leader) ──moves due jobs────┤      │ claim / lease / result
         - leader lease in Redis + fencing token   │      │
         - reconciler repairs Redis from MongoDB   ▼      │
                                         js:ready ──BRPOP── worker (N) × WORKER_CONCURRENCY
```

- Services never call each other; they coordinate only through MongoDB and Redis.
- Writes go to MongoDB first, Redis second. A lost Redis write is repaired by the reconciler.
- Execution is **at-least-once**. Handlers must be idempotent (use job ID + `occurrence_at` as the key).

| Package | Responsibility |
|---|---|
| `cmd/{api,scheduler,worker}` | Entry points: signals, wiring, `run()` pattern |
| `internal/app` | Shared startup (config, logger, connections, indexes) and graceful HTTP shutdown |
| `internal/api`, `internal/middleware` | REST handlers, error format, request IDs, access logs, panic recovery |
| `internal/services` | Validation, idempotency fingerprints, cursor pagination |
| `internal/repository` | MongoDB collections; every state change is a conditional update |
| `internal/queue` | Redis delayed set, ready list (atomic Lua move), epoch, heartbeats |
| `internal/locks` | Leader election: `SET NX PX`, Lua renew/release, monotonic self-check |
| `internal/scheduler` | Due-job mover and reconciler (leader only) |
| `internal/worker` | Bounded pool, atomic claim, lease renewal, cancellation, results |
| `internal/lifecycle` | Retry / dead-letter / reschedule decisions shared by worker and reconciler |
| `internal/handlers` | `job_type` → Go code; built-in `echo`, `sleep`, `fail` |
| `internal/health` | `/healthz` (liveness) and `/readyz` (readiness) |
| `pkg/backoff`, `internal/schedule` | Full-jitter backoff; 5-field cron with IANA time zones |

## Running locally

Requirements: Go 1.24 and Docker. Run every Go command with `GOTOOLCHAIN=local` (the Makefile does it for you).

```bash
make deps-up                 # MongoDB on 127.0.0.1:27018, Redis on 127.0.0.1:6380
make test                    # unit tests, no Docker needed
make test-integration        # + integration and end-to-end tests
make build
bin/api & bin/scheduler & bin/worker &
```

## API

| Method | Path | Result |
|---|---|---|
| POST | `/api/v1/jobs` | 201 + `Location`; 200 + `Idempotent-Replayed: true` on replay; 409 if the key was reused |
| GET | `/api/v1/jobs/{id}` | The job |
| GET | `/api/v1/jobs?status=&job_type=&created_after=&created_before=&limit=&cursor=` | One page, newest first |
| POST | `/api/v1/jobs/{id}/cancel` | 200 cancelled, 202 running job asked to stop, 409 finished |
| POST | `/api/v1/jobs/{id}/retry` | Restart a dead-lettered job |
| GET | `/api/v1/jobs/{id}/executions` | Attempts, newest first |

```bash
curl -s -X POST localhost:8080/api/v1/jobs -H 'Idempotency-Key: order-42' \
  -d '{"job_type":"echo","payload":{"order":42},"schedule":{"type":"delayed","delay_seconds":30}}'

curl -s -X POST localhost:8080/api/v1/jobs \
  -d '{"job_type":"echo","schedule":{"type":"recurring","cron":"0 9 * * *","timezone":"Asia/Kolkata"}}'
```

Errors always look like
`{"error":{"code":"VALIDATION_FAILED","message":"...","details":[{"field":"...","issue":"..."}],"request_id":"..."}}`.

## Deployment

- **Containers:** `deployments/Dockerfile` (one distroless image, three binaries), `deployments/compose.app.yml`.
- **Kubernetes:** `deployments/k8s/base` (probes, PDBs, HPA, zone spreading, non-root, read-only FS);
  `deployments/k8s/dev-deps` for a local cluster; `deployments/k8s/overlays/eks` for EKS.
- **CI:** `.github/workflows/ci.yml`: gofmt, vet, tidy, unit tests, integration tests
  against real MongoDB and Redis, image build, optional ECR push through OIDC.
