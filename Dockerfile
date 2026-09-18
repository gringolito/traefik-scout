# Build stage: compile a static binary for the requested platform.
FROM golang:1.27 AS build

WORKDIR /src

# Cache module downloads independently of source changes.
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download

COPY . .
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" \
    -o /out/traefik-scout ./cmd/traefik-scout

# Final stage: distroless static image — no shell, no package manager, non-root.
FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=build /out/traefik-scout /traefik-scout

USER 65532:65532

ENTRYPOINT ["/traefik-scout"]
