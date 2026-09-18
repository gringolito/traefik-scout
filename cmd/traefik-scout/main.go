// Command traefik-scout polls downstream Traefik instances and serves their
// merged route configuration as a single Traefik file-provider document.
//
// The config file path comes from the -config flag or the CONFIG_PATH
// environment variable (the flag wins when both are set). The process exits
// non-zero without serving when the config cannot be loaded or validated,
// and shuts down gracefully, finishing in-flight requests, on SIGTERM or
// SIGINT.
package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/gringolito/traefik-scout/internal/cli"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)

	code := cli.Run(ctx, os.Args[1:], os.Stderr)
	stop()
	os.Exit(code)
}
