// Package cli wires configuration loading and the App's HTTP surface into a
// runnable command, testable independently of process signals and exit.
package cli

import (
	"context"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/gringolito/traefik-scout/internal/app"
	"github.com/gringolito/traefik-scout/internal/config"
)

// EnvConfigPath is the environment variable supplying the config file path.
// The -config flag takes precedence when both are set.
const EnvConfigPath = "CONFIG_PATH"

// shutdownTimeout bounds how long graceful shutdown waits for in-flight
// requests after SIGTERM/SIGINT.
const shutdownTimeout = 10 * time.Second

// FlagConfigName is the -config flag name, used in usage text and tests.
const FlagConfigName = "config"

// Run is the command's main wiring, returning the process exit code.
//
// The config path resolves per EnvConfigPath and FlagConfigName. A load or
// validation failure prints the error to stderr and returns 1 without
// starting the HTTP server.
//
// ctx is the process-lifetime context, canceled by SIGTERM/SIGINT in main.
// On cancellation Run stops polling and shuts the server down per
// shutdownTimeout.
func Run(ctx context.Context, args []string, stderr io.Writer) int {
	fs := flag.NewFlagSet("traefik-scout", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Usage = func() {
		_, _ = fmt.Fprintf(stderr, "usage: traefik-scout [-config <path>]\n\n"+
			"serves merged Traefik route configuration from the downstreams in the\n"+
			"config file, refreshing every poll_interval.\n\n"+
			"flags:\n")
		fs.PrintDefaults()
		_, _ = fmt.Fprintf(stderr, "\nThe config path comes from -config or the %s environment\n"+
			"variable; the flag takes precedence when both are set.\n", EnvConfigPath)
	}
	cfgFlag := fs.String(FlagConfigName, "", "path to the YAML config file (overrides $"+EnvConfigPath+")")
	if err := fs.Parse(args); err != nil {
		return 2
	}

	cfgPath := *cfgFlag
	if cfgPath == "" {
		cfgPath = os.Getenv(EnvConfigPath)
	}
	if cfgPath == "" {
		_, _ = fmt.Fprintf(stderr, "traefik-scout: no config file: use -config or set %s\n", EnvConfigPath)
		return 1
	}

	cfg, err := config.Load(cfgPath)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "traefik-scout: %v\n", err)
		return 1
	}

	return serve(ctx, cfg, stderr, func(l net.Listener) {
		_, _ = fmt.Fprintf(stderr, "traefik-scout: listening on %s\n", l.Addr())
	}, nil)
}

// serve constructs the App, runs one synchronous refresh, serves the handler
// on cfg.Listen, and polls on cfg.PollInterval until ctx is canceled.
// onListen is called with the bound listener (use nil to ignore it).
// handler overrides theApp.Handler() when non-nil (test seam only).
func serve(ctx context.Context, cfg *config.Config, stderr io.Writer, onListen func(net.Listener), handler http.Handler) int {
	theApp, err := app.New(*cfg)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "traefik-scout: %v\n", err)
		return 1
	}

	// Downstreams may be down at boot; the App serves an empty snapshot until
	// a poll succeeds, so log and continue rather than failing startup.
	if err := theApp.Refresh(ctx); err != nil {
		_, _ = fmt.Fprintf(stderr, "traefik-scout: initial refresh failed: %v\n", err)
	}

	pollCtx, stopPoll := context.WithCancel(ctx)
	defer stopPoll()
	go pollLoop(pollCtx, theApp, cfg.PollInterval, stderr)

	var lc net.ListenConfig
	listener, err := lc.Listen(ctx, "tcp", cfg.Listen)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "traefik-scout: listen %s: %v\n", cfg.Listen, err)
		return 1
	}
	if onListen != nil {
		onListen(listener)
	}

	if handler == nil {
		handler = theApp.Handler()
	}
	server := &http.Server{
		Handler:           handler,
		ReadHeaderTimeout: shutdownTimeout,
	}
	go func() { _ = server.Serve(listener) }()

	<-ctx.Done()
	stopPoll()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	_ = server.Shutdown(shutdownCtx)
	return 0
}

// pollLoop calls Refresh on every tick until ctx is canceled.  The first tick
// fires after one interval; the initial snapshot comes from the synchronous
// refresh in serve.
func pollLoop(ctx context.Context, theApp *app.App, interval time.Duration, stderr io.Writer) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := theApp.Refresh(ctx); err != nil {
				_, _ = fmt.Fprintf(stderr, "traefik-scout: refresh failed: %v\n", err)
			}
		}
	}
}
