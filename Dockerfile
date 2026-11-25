# Build stage: compile a static binary for the requested platform.
FROM golang:1.27@sha256:5f1b98fa02fe396ebce2a0227a0e08309e11792915b9dd2e1f71053d7e7e6666 AS build

WORKDIR /src

# Cache module downloads independently of source changes.
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download

COPY . .
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" \
    -o /out/traefik-scout ./cmd/traefik-scout

# Final stage: distroless static image, no shell, no package manager, non-root.
FROM gcr.io/distroless/static-debian12:nonroot@sha256:afa5c872c891853ca7fcf1f12c3edb23f7eeef36189728842dd51042ff57f7ab

COPY --from=build /out/traefik-scout /traefik-scout

USER 65532:65532

# Runs with the config the server uses (-config falls back to CONFIG_PATH).
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 CMD ["/traefik-scout", "-healthcheck"]

ENTRYPOINT ["/traefik-scout"]
