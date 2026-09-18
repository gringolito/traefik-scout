package cli

import (
	"context"
	"io"
	"net"
	"net/http"

	"github.com/gringolito/traefik-scout/internal/config"
)

// ServeForTest exposes the unexported serve seam so external tests can drive
// the serving lifecycle with an ephemeral listener and capture its address.
func ServeForTest(ctx context.Context, cfg *config.Config, stderr io.Writer, onListen func(net.Listener)) int {
	return serve(ctx, cfg, stderr, onListen, nil)
}

// ServeHandlerForTest is ServeForTest with a handler override, letting tests
// gate requests to observe shutdown drain behavior.
func ServeHandlerForTest(ctx context.Context, cfg *config.Config, stderr io.Writer, handler http.Handler, onListen func(net.Listener)) int {
	return serve(ctx, cfg, stderr, onListen, handler)
}
