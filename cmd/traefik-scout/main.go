// Command traefik-scout polls downstream Traefik instances and serves their
// merged route configuration as a single Traefik file-provider document.
//
// See internal/cli.Run for how the config path is resolved and how the
// process exits or shuts down.
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
