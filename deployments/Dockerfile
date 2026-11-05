# One image with all three services; Kubernetes and Compose pick the binary
# with `command`. Build from the repository root:
#
#   docker build -f deployments/Dockerfile -t djs:dev .
#
# The build uses Go 1.24.12 inside the image, which matches the Go version in CI.

# ---- build stage ------------------------------------------------------------
FROM golang:1.24.12-bookworm AS build

WORKDIR /src
# Never let the build download a different Go toolchain.
ENV GOTOOLCHAIN=local CGO_ENABLED=0

# Dependencies come from vendor/ (their source, in the repository), so the
# build never contacts proxy.golang.org. -mod=vendor and GOPROXY=off make
# a missing or outdated vendor/ fail loudly instead of downloading.
ENV GOFLAGS=-mod=vendor GOPROXY=off
COPY go.mod go.sum ./
COPY vendor ./vendor

COPY cmd ./cmd
COPY internal ./internal
COPY pkg ./pkg

# -trimpath removes local paths from binaries; -s -w drops debug symbols.
RUN for svc in api scheduler worker; do \
      go build -trimpath -ldflags="-s -w" -o /out/$svc ./cmd/$svc || exit 1; \
    done

# ---- runtime stage ----------------------------------------------------------
# distroless/static: no shell, no package manager, runs as a non-root user.
# Less software in the image means fewer vulnerabilities to patch.
FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=build /out/ /usr/local/bin/
# Numeric UID:GID of distroless's "nonroot" user. Kubernetes runAsNonRoot can
# only verify a numeric user; a name like "nonroot" makes the pod fail to start.
USER 65532:65532
EXPOSE 8080 8081 8082
ENTRYPOINT ["/usr/local/bin/api"]
