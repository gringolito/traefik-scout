package cli

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/gringolito/traefik-scout/internal/config"
)

// FlagHealthcheckName is the -healthcheck flag name.
const FlagHealthcheckName = "healthcheck"

// probeTimeout bounds the healthcheck's HTTP request so a hung server cannot
// hang the probe forever.
const probeTimeout = 3 * time.Second

// runHealthcheck self-probes the running server's /healthz endpoint and
// returns the process exit code: 0 on a 200 response, 1 otherwise. It targets
// /healthz, not /readyz: readiness is legitimately 503 while no downstream
// has ever answered.
func runHealthcheck(cfg *config.Config, log *slog.Logger) int {
	addr, err := loopbackAddr(cfg.Listen)
	if err != nil {
		log.Error("healthcheck failed", "error", err)
		return 1
	}

	url := "http://" + addr + config.PathHealthz
	ctx, cancel := context.WithTimeout(context.Background(), probeTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		log.Error("healthcheck failed", "error", err)
		return 1
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Error("healthcheck failed", "url", url, "error", err)
		return 1
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		log.Error("healthcheck failed", "url", url, "status", resp.StatusCode)
		return 1
	}
	return 0
}

// loopbackAddr converts a configured listen address into a client-dialable
// address. A wildcard host (0.0.0.0, ::, empty) is a bind target rather than
// a dial target, so it is rewritten to the loopback; any explicit host already
// is a valid dial target and is kept as configured.
func loopbackAddr(listen string) (string, error) {
	host, port, err := net.SplitHostPort(listen)
	if err != nil {
		return "", fmt.Errorf("listen %q: %w", listen, err)
	}
	switch host {
	case "", "0.0.0.0", "::":
		host = "127.0.0.1"
	}
	return net.JoinHostPort(host, port), nil
}
