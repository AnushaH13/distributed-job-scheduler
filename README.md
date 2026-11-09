# Distributed Job Scheduler

A production-style distributed job scheduler written in Go, coordinating through
MongoDB and Redis. It runs immediate, delayed, and recurring (cron) jobs with
retries, a dead-letter queue, leader election, and graceful shutdown — split
across three independently-scalable services (`api`, `scheduler`, `worker`)
that never call each other directly.

## Table of Contents

- [Overview](#overview)
- [Key Features](#key-features)
- [Technology Stack](#technology-stack)
- [Architecture](#architecture)
- [MongoDB and Redis Leases](#mongodb-and-redis-leases)
- [Getting Started](#getting-started)
- [API Reference](#api-reference)
- [Health and Readiness Endpoints](#health-and-readiness-endpoints)
- [Testing and CI](#testing-and-ci)
- [Deployment](#deployment)
- [License](#license)

## Overview

Most backend systems eventually need to run work outside the request/response
cycle: "send this email in 10 minutes", "retry this payment with backoff", or
"run this report every night at 9 AM". Two common shortcuts both fall short
at any real scale, for different reasons. An **in-process timer** (a
goroutine sleeping until a job is due) is lost the moment the process
crashes or redeploys — nothing else remembers the job existed, so it simply
never runs. **Traditional cron** survives an application crash, since it's a
separate OS daemon, but it's tied to one machine: if that machine is down,
nothing runs, and running cron on several machines for redundancy makes
every job run multiple times, because cron has no way to coordinate across
hosts. Neither gives you retries, a record of what ran, or a way to create a
job dynamically from a user's request.

This project solves that problem with three cooperating Go services that
share **MongoDB** as the source of truth and **Redis** as a fast, disposable
coordination layer:

- **`api`** accepts job requests over HTTP and writes them to MongoDB.
- **`scheduler`** decides when a job is due and hands it off to a worker — with
  only one replica active at a time, even when several are running.
- **`worker`** claims due jobs, runs them, and records the result.

It was built to demonstrate, end to end and with tests, the problems a real
distributed job scheduler has to solve: atomic job claiming (so two workers
cannot claim the same job simultaneously), safe leader election, crash
recovery, retries with backoff, a dead-letter queue, idempotent API
requests, and graceful shutdown.

## Key Features

**Scheduling**
- Immediate, delayed (`run_at` or `delay_seconds`), and recurring jobs
- Standard 5-field cron expressions evaluated in any IANA time zone (e.g.
  `Asia/Kolkata`), including correct handling of daylight-saving changes

**Reliability**
- At-least-once execution with automatic retries (exponential backoff with
  full jitter)
- A dead-letter queue for jobs that exhaust their retries or fail
  permanently, plus a manual-retry API endpoint
- Atomic job claiming in MongoDB, so only one worker ever holds the valid
  claim on a given job at a time (see
  [MongoDB and Redis Leases](#mongodb-and-redis-leases))
- A background reconciler that continuously repairs any drift between
  MongoDB (source of truth) and Redis, including a full rebuild if Redis
  data is lost

**API**
- A REST API with idempotent job creation (`Idempotency-Key` header), cursor
  pagination for listing jobs, and cooperative cancellation of running jobs
- Consistent JSON error responses with a machine-readable error code and a
  request ID on every response

**Operations**
- Bounded, configurable worker concurrency per process
- Leader election for the scheduler service with a Redis lease and MongoDB
  fencing tokens
- Liveness and readiness HTTP endpoints designed for Kubernetes
- Structured logging (JSON in production, text in development) with secrets
  automatically redacted
- Graceful shutdown on `SIGINT`/`SIGTERM` with a configurable timeout

**Extensibility**
- A pluggable job-handler registry — add a new job type by registering a Go
  function; ships with three demo handlers (`echo`, `sleep`, `fail`) used by
  the test suite

## Technology Stack

| Layer | Technology |
|---|---|
| Language | Go 1.24 (module targets `go 1.24.0`; built and tested with Go 1.24.12) |
| HTTP | Standard library `net/http`, using Go 1.22+ method/path routing — no router framework |
| Primary datastore | MongoDB — source of truth for jobs, execution history, and the dead-letter queue |
| Coordination layer | Redis — delayed-job timers, the ready queue, the scheduler's leader lease, worker heartbeats |
| MongoDB driver | [`go.mongodb.org/mongo-driver/v2`](https://pkg.go.dev/go.mongodb.org/mongo-driver/v2) v2.8.2 |
| Redis client | [`github.com/redis/go-redis/v9`](https://pkg.go.dev/github.com/redis/go-redis/v9) v9.22.0 |
| Cron parsing | [`github.com/robfig/cron/v3`](https://pkg.go.dev/github.com/robfig/cron/v3) v3.0.1 |
| Dependency management | Go modules, vendored (`vendor/`) for reproducible, network-free builds |
| Containers | Docker — a multi-stage build producing one [distroless](https://github.com/GoogleContainerTools/distroless) runtime image shared by all three binaries |
| Local orchestration | Docker Compose |
| Cluster orchestration | Kubernetes manifests managed with [Kustomize](https://kustomize.io/) |
| CI/CD | GitHub Actions — formatting/vet/tidy checks, unit tests, integration tests against real MongoDB and Redis, Kustomize validation, and a Docker image build |

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

**How a job flows through the system:**

1. A client calls `POST /api/v1/jobs`. The API validates the request and
   inserts the job into MongoDB with status `scheduled` — this insert is what
   actually creates the job; everything after this point is the system
   catching up.
2. The API then adds the job's ID and run time to a Redis sorted set
   (`js:delayed`). If this write fails, nothing is lost: the job already
   exists in MongoDB, and the scheduler's reconciler will notice and add it
   to Redis on its own.
3. The scheduler leader polls the Redis sorted set once a second. For every
   job whose time has come, it marks the job `queued` in MongoDB (a
   conditional write, fenced by its leadership token) and only then moves the
   job's ID into a Redis list (`js:ready`) that workers pull from.
4. An idle worker pops a job ID off `js:ready` and immediately tries to claim
   it in MongoDB with a single atomic update (`queued` → `running`, checked
   and set in one operation). Only one worker can win that race for a given
   job.
5. The worker runs the registered handler for the job's `job_type` inside its
   bounded worker pool, periodically renewing its job lease in MongoDB while
   the handler runs.
6. On success the job becomes `completed` (or reschedules itself for its next
   cron occurrence, if recurring). On failure it goes back to `retrying` with
   a backoff delay, or — once retries are exhausted or the failure is marked
   permanent — into the dead-letter queue.

**Design rules:**

- Services never call each other directly; they coordinate only by reading
  and writing MongoDB and Redis.
- Every write to MongoDB happens before the matching write to Redis. Redis
  only tracks "what's pending" — it can be wiped and rebuilt entirely from
  MongoDB at any time, and the reconciler does exactly that if it detects
  Redis has lost its data.
- Execution is **at-least-once**, never exactly-once: a crash between
  finishing a job and recording that fact can make it run again. Handlers
  should be idempotent — use the job ID plus its `occurrence_at` timestamp as
  a stable key for any downstream side effect.

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

## MongoDB and Redis Leases

The system uses two different kinds of time-limited "lease", for two
different purposes. They're easy to confuse, so here's exactly what each one
does and where it lives in the code.

### 1. Job execution lease — stored in MongoDB

When a worker claims a job, it writes a `lease_owner`, a random
`claim_token`, and a `lease_expires_at` timestamp directly onto the job
document (`internal/repository/jobs.go`, `Jobs.Claim`). The worker then
renews `lease_expires_at` every few seconds while it runs the handler
(`RenewLease`).

- **Purpose:** guarantee that only one worker holds the *valid* claim on a
  job at a time, and guarantee that a job whose worker has gone silent gets
  retried instead of stuck forever in `running`. This is a lease on the
  claim, not a guarantee of exclusive execution: if a worker pauses for a
  long garbage collection or loses network connectivity without actually
  crashing, its lease can expire — and be reassigned to another worker — while
  the first worker's handler is still physically running in the background.
- **Clock:** `lease_expires_at` is computed with MongoDB's own server clock
  (`$$NOW`), never the worker's local clock, so clock drift between machines
  never matters.
- **Recovery:** if a worker stops renewing — it crashes, pauses for a long
  garbage collection, or loses its network connection — the lease simply
  expires. The scheduler's reconciler periodically looks for jobs whose
  `lease_expires_at` has passed and treats them as failed, so they are
  retried or dead-lettered through the normal rules. The job is never stuck.
- **Safety:** every write that finishes a job (complete, retry, dead-letter)
  is conditioned on the exact `claim_token` the worker was given, so a stale
  worker — one whose lease has already expired, even if it's still running —
  can never overwrite a result the reconciler or a new claim holder has
  already recorded. Because the lease protects job *state*, not execution
  itself, handlers must still be idempotent (see [Architecture](#architecture)).

### 2. Scheduler leader lease — stored in Redis

All `scheduler` replicas race to acquire a Redis key with `SET key id NX PX
<ttl>` (`internal/locks/elector.go`). Only one of them — the leader — moves
due jobs and runs the reconciler; the rest simply keep retrying the `SET`.

- **Purpose:** avoid two scheduler replicas duplicating work, and provide a
  safe, automatic handover if the active leader disappears.
- **Renewal:** the leader renews its lease on a fixed interval using a Lua
  script that only succeeds if it still owns the key — so it can never
  accidentally extend a lease another replica has already taken over.
- **Self-check:** the leader also tracks, using Go's monotonic clock, how
  long it's been since its last successful renewal. If that exceeds a safety
  margin, it steps down on its own — even before the Redis key would expire
  — which protects against a leader that merely paused (e.g. a long GC
  pause) and might otherwise keep acting after a new leader has taken over.
- **Fencing:** every time a replica becomes leader, it asks MongoDB for a new
  fencing token — a number that only ever increases, stored in the
  `scheduler_leases` collection. Every write the leader makes while moving
  due jobs carries this token, and MongoDB rejects writes carrying a lower
  token than one already recorded. This stops a "zombie" leader — one that
  still believes it's in charge after losing its Redis lease — from
  corrupting state.

### Side by side

| | Job execution lease | Scheduler leader lease |
|---|---|---|
| Stored in | MongoDB (`jobs.lease_expires_at`) | Redis (a string key with a TTL) |
| Held by | The worker currently running a job | The one scheduler replica currently leading |
| Default duration | 30s, renewed every 10s | 15s, renewed every 5s |
| Clock used | MongoDB's server clock (`$$NOW`) | Local monotonic clock as a self-check; Redis TTL as the hard limit |
| If the holder disappears | The reconciler finds the expired lease and retries/dead-letters the job | Another replica's `SET NX` succeeds once the Redis key expires |
| Extra protection | Every write must match the current `claim_token` | MongoDB fencing token rejects stale writes |

These durations are currently fixed in code (`internal/worker/worker.go`,
`internal/locks/elector.go`), not yet exposed as environment variables.

## Getting Started

### Prerequisites

- [Go 1.24.x](https://go.dev/dl/) (tested with 1.24.12)
- [Docker](https://docs.docker.com/get-docker/) and Docker Compose v2
  (`docker compose`, not the standalone `docker-compose`)
- `make` (optional — every command below has a [Makefile](Makefile) target)

### 1. Clone and configure

```bash
git clone https://github.com/AnushaH13/distributed-job-scheduler.git
cd distributed-job-scheduler
cp .env.example .env
```

The defaults in `.env.example` already point at the MongoDB and Redis
containers started in the next step, so no changes are needed for local
development.

### 2. Start MongoDB and Redis

```bash
make deps-up        # MongoDB on 127.0.0.1:27018, Redis on 127.0.0.1:6380
```

This uses `deployments/compose.deps.yml` and only creates or touches
containers, a network, and volumes belonging to this project.

### 3. Run the tests

```bash
make test               # unit tests only — no Docker required
make test-integration   # unit + integration + end-to-end tests (needs make deps-up)
```

### 4. Build and run the three services

```bash
make build               # produces bin/api, bin/scheduler, bin/worker
set -a; source .env; set +a
bin/api &
bin/scheduler &
bin/worker &
```

- `api` listens on `:8080` and serves the REST API plus `/healthz` and `/readyz`.
- `scheduler` and `worker` only serve `/healthz` and `/readyz`, on `:8081` and `:8082`.

### 5. Or run everything with Docker

```bash
make up      # builds the image, then starts mongo, redis, api, scheduler, and worker
make down    # stops everything
```

See [Deployment](#deployment) for what `make up` actually builds and runs.

### 6. Try it

```bash
curl -s localhost:8080/healthz

curl -s -X POST localhost:8080/api/v1/jobs \
  -H 'Content-Type: application/json' \
  -d '{"job_type":"echo","payload":{"hello":"world"},"schedule":{"type":"immediate"}}'
```

## API Reference

All endpoints are served by the `api` service under `/api/v1`
(`internal/api`, `internal/services`).

### Create a job

```
POST /api/v1/jobs
Content-Type: application/json
Idempotency-Key: <optional, max 255 characters>
```

| Field | Type | Required | Notes |
|---|---|---|---|
| `job_type` | string | yes | Must be a registered handler — `echo`, `sleep`, and `fail` are built in |
| `payload` | object | no | Passed to the handler; a JSON object up to 64 KB |
| `schedule.type` | string | yes | `immediate`, `delayed`, or `recurring` |
| `schedule.run_at` | RFC 3339 string | for `delayed` | Up to 5 minutes in the past, up to 365 days in the future |
| `schedule.delay_seconds` | integer | for `delayed` | 1 to 31,536,000 (365 days) — use exactly one of `run_at`/`delay_seconds` |
| `schedule.cron` | string | for `recurring` | A standard 5-field cron expression, e.g. `0 9 * * *` |
| `schedule.timezone` | string | for `recurring` | An IANA time zone, e.g. `Asia/Kolkata` (default `UTC`) |
| `max_retries` | integer | no | 0–10 (default 3) |
| `timeout_seconds` | integer | no | 1–3600 (default 300) |

The `Idempotency-Key` header makes retries safe: replaying the exact same
request with the same key returns the original job (`200` with
`Idempotent-Replayed: true`) instead of creating a duplicate. Reusing the key
with a **different** request body returns `409 IDEMPOTENCY_KEY_REUSED`.

```bash
# Delayed job, with an idempotency key
curl -s -X POST localhost:8080/api/v1/jobs \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: order-42' \
  -d '{
    "job_type": "echo",
    "payload": {"order": 42},
    "schedule": {"type": "delayed", "delay_seconds": 30}
  }'
```

```bash
# Recurring job: every day at 09:00 in Asia/Kolkata
curl -s -X POST localhost:8080/api/v1/jobs \
  -H 'Content-Type: application/json' \
  -d '{
    "job_type": "echo",
    "schedule": {"type": "recurring", "cron": "0 9 * * *", "timezone": "Asia/Kolkata"}
  }'
```

A successful response (`201 Created`, with a `Location` header pointing at
the new job):

```json
{
  "job_id": "job_3f1c9c2e-8f1a-4e8e-9c2e-1a2b3c4d5e6f",
  "job_type": "echo",
  "payload": {"order": 42},
  "schedule_type": "delayed",
  "max_retries": 3,
  "timeout_seconds": 300,
  "status": "scheduled",
  "scheduled_at": "<RFC 3339 timestamp, UTC>",
  "next_run_at": "<RFC 3339 timestamp, UTC — 30s after created_at>",
  "occurrence_at": "<RFC 3339 timestamp, UTC — same as next_run_at>",
  "retry_count": 0,
  "cancel_requested": false,
  "created_at": "<RFC 3339 timestamp, UTC>",
  "updated_at": "<RFC 3339 timestamp, UTC>"
}
```

### Other endpoints

| Method | Path | Description |
|---|---|---|
| `GET` | `/api/v1/jobs/{job_id}` | Fetch one job |
| `GET` | `/api/v1/jobs` | List jobs, newest first. Filters: `status`, `job_type`, `created_after`, `created_before`. Pagination: `limit` (default 20, max 100), `cursor` |
| `POST` | `/api/v1/jobs/{job_id}/cancel` | Cancel a waiting job, or ask a running one to stop cooperatively |
| `POST` | `/api/v1/jobs/{job_id}/retry` | Re-queue a job that is currently in the dead-letter queue |
| `GET` | `/api/v1/jobs/{job_id}/executions` | List this job's execution attempts, newest first |

```bash
curl -s localhost:8080/api/v1/jobs/job_3f1c9c2e-8f1a-4e8e-9c2e-1a2b3c4d5e6f

curl -s "localhost:8080/api/v1/jobs?status=completed&limit=10"

curl -s -X POST localhost:8080/api/v1/jobs/job_3f1c9c2e-8f1a-4e8e-9c2e-1a2b3c4d5e6f/cancel

curl -s -X POST localhost:8080/api/v1/jobs/job_3f1c9c2e-8f1a-4e8e-9c2e-1a2b3c4d5e6f/retry

curl -s localhost:8080/api/v1/jobs/job_3f1c9c2e-8f1a-4e8e-9c2e-1a2b3c4d5e6f/executions
```

### Status codes

| Code | Meaning |
|---|---|
| `200 OK` | Request succeeded — including an idempotent replay, and cancelling a job that was already waiting or already cancelled |
| `201 Created` | Job created |
| `202 Accepted` | Cancellation requested for a job that is currently running — the worker will stop it cooperatively |
| `400 Bad Request` | Invalid JSON, or a request that fails validation (`VALIDATION_FAILED`) |
| `404 Not Found` | No job with that ID (`JOB_NOT_FOUND`) |
| `409 Conflict` | The job's current status doesn't allow this operation, or an idempotency key was reused with a different request |
| `413 Payload Too Large` | Payload over 64 KB |
| `503 Service Unavailable` | MongoDB is unreachable; a `Retry-After` header is included |

### Error format

Every error response has the same shape, with a request ID that also appears
in the server's logs and in the `X-Request-ID` response header:

```json
{
  "error": {
    "code": "VALIDATION_FAILED",
    "message": "request validation failed",
    "details": [{"field": "job_type", "issue": "is required"}],
    "request_id": "req_a1b2c3d4"
  }
}
```

## Health and Readiness Endpoints

Every service (`api`, `scheduler`, `worker`) exposes the same two endpoints,
each on its own HTTP port (`:8080`, `:8081`, `:8082` by default):

| Endpoint | Purpose | Checks dependencies? |
|---|---|---|
| `GET /healthz` | **Liveness** — "is the process alive?" | No. Restarting the process would not fix a MongoDB/Redis outage, so this never fails because of one |
| `GET /readyz` | **Readiness** — "should this instance receive traffic or work?" | Yes. Pings MongoDB and Redis and returns `503` if either is unreachable, or if the service is shutting down |

```bash
curl -s localhost:8080/healthz
# {"status":"ok"}

curl -s localhost:8080/readyz
# {"status":"ok","checks":{"mongodb":"ok","redis":"ok"}}
```

This split matches how Kubernetes uses the two probes in
`deployments/k8s/base`: a failing liveness probe restarts the pod, while a
failing readiness probe only removes it from traffic — so a single
dependency outage doesn't restart every pod at once.

## Testing and CI

### Running tests locally

| Command | What it runs | Needs Docker? |
|---|---|---|
| `make test` | Unit tests only (`go test -race -count=1 ./...`) | No |
| `make test-integration` | The same, plus integration and end-to-end tests (`DJS_INTEGRATION=1`) against real MongoDB and Redis | Yes (`make deps-up` first) |

The codebase has 16 test files across its packages. The end-to-end suite
(`tests/e2e_test.go`) runs the actual scheduler and worker loops against real
MongoDB and Redis containers and covers, among other things:

- a job completing successfully
- delayed and recurring jobs
- retries with backoff, ending in the dead-letter queue
- permanent failures skipping straight to the dead-letter queue
- manually retrying a dead-lettered job
- cancelling a running job vs. a waiting job
- idempotent job creation and idempotency-key reuse
- recovering a job after its worker "crashes" (stops renewing its lease)
- rebuilding Redis state entirely from MongoDB after simulated data loss

### Continuous integration

[`.github/workflows/ci.yml`](.github/workflows/ci.yml) runs on every push to
`main` and on every pull request, as four jobs:

| Job | What it does |
|---|---|
| `test` | Confirms `go.mod` still targets Go 1.24, then runs `gofmt`, `go vet`, `go mod tidy -diff`, and the unit tests |
| `kustomize` | Renders all three Kustomize directories (`base`, `dev-deps`, `overlays/eks`) with the real `kubectl kustomize` — nothing is applied to a cluster |
| `integration` | Runs the full test suite, including integration and end-to-end tests, against real `mongo:7.0.45-jammy` and `redis:8.8.4-alpine` service containers |
| `image` | Builds the Docker image from `deployments/Dockerfile`; if the repository variable `AWS_ROLE_ARN` is set, it also pushes the image to Amazon ECR using GitHub OIDC (no AWS credentials stored in GitHub) |

`image` only starts once `test`, `integration`, and `kustomize` have all
passed.

## Deployment

### Docker

`deployments/Dockerfile` is a two-stage build: Go 1.24.12 compiles all three
binaries from the vendored dependencies (no network access needed during the
build), and the result is copied into a `distroless` base image with no
shell and a non-root user — one image that all three services run from,
selected by `ENTRYPOINT`/`command`.

```bash
make image     # docker build -f deployments/Dockerfile -t djs:dev .
make up        # the image + mongo + redis + api + scheduler + worker, via Docker Compose
make down
```

`deployments/compose.app.yml` runs two replicas each of `scheduler` and
`worker` alongside a single `api`, so leader election is visible locally:
only one `scheduler` replica will log that it became leader at any time.

### Kubernetes

`deployments/k8s/` is organized with [Kustomize](https://kustomize.io/):

| Directory | Purpose |
|---|---|
| `base/` | Deployments for `api`, `scheduler`, and `worker`; a ConfigMap; an example Secret; a Service for `api`; a `PodDisruptionBudget` for `api` and `worker`; and a `HorizontalPodAutoscaler` for `worker` (CPU-based, 2–10 replicas) |
| `dev-deps/` | MongoDB and Redis as StatefulSets, for trying the manifests on a local cluster (e.g. kind or minikube) — not intended for production |
| `overlays/eks/` | Patches the image reference and the Redis address for an EKS deployment with managed MongoDB/Redis |

The base manifests also include: liveness and readiness probes wired to
`/healthz` and `/readyz`, a non-root numeric user, a read-only root
filesystem, dropped Linux capabilities, resource requests/limits, rolling
updates with zero unavailable replicas, and pod topology spread across
availability zones.

Validate the manifests without a cluster:

```bash
kubectl kustomize deployments/k8s/base
kubectl kustomize deployments/k8s/dev-deps
kubectl kustomize deployments/k8s/overlays/eks
```

### AWS EKS

[`docs/deploy-eks.md`](docs/deploy-eks.md) documents how to run this project
on EKS, including a rough monthly cost estimate (around $250–320 if left
running) and the exact `eksctl`/`kubectl` commands. That figure is only a
starting point — the actual cost depends on your AWS region and how much
compute, storage, and data transfer you actually use; see that document for
the per-resource breakdown and current pricing sources. **Nothing in this
repository creates AWS resources automatically** — it's a manual,
cost-incurring process that has to be started and torn down deliberately; no
cluster is created as part of building or testing this project.

## License

This project is licensed under the [MIT License](LICENSE).
