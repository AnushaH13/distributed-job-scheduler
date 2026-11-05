# Common commands. Every Go command runs with GOTOOLCHAIN=local, so an
# incompatible dependency fails instead of downloading a newer Go.
export GOTOOLCHAIN := local

COMPOSE_DEPS := docker compose -f deployments/compose.deps.yml
COMPOSE_ALL  := $(COMPOSE_DEPS) -f deployments/compose.app.yml

.PHONY: check fmt vet test test-integration build deps-up deps-down image up down

check: fmt vet test            ## everything CI's first job runs

fmt:
	@files=$$(gofmt -l $$(git ls-files '*.go' | grep -v '^vendor/')); \
	test -z "$$files" || { echo "$$files"; echo "run: gofmt -w <file>"; exit 1; }

vet:
	go vet ./...

test:                          ## unit tests only; no Docker needed
	go test -race -count=1 ./...

test-integration:              ## needs: make deps-up
	DJS_INTEGRATION=1 go test -race -count=1 ./...

build:
	@mkdir -p bin
	for s in api scheduler worker; do go build -o bin/$$s ./cmd/$$s || exit 1; done

deps-up:                       ## MongoDB 127.0.0.1:27018, Redis 127.0.0.1:6380
	$(COMPOSE_DEPS) up -d --wait

deps-down:                     ## keeps data volumes (no -v)
	$(COMPOSE_DEPS) down

image:
	docker build -f deployments/Dockerfile -t djs:dev .

up: image                      ## whole system in containers; API on 127.0.0.1:8080
	$(COMPOSE_ALL) up -d --wait

down:
	$(COMPOSE_ALL) down
