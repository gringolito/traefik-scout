// Package cli wires configuration loading and the App's HTTP surface into a
// runnable command, testable independently of process signals and exit.
package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
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

// readHeaderTimeout bounds how long the server waits to read a request's
// headers, guarding against clients that open connections and never send a
// complete request (slow-loris style). It is a per-connection guard,
// unrelated to graceful shutdown.
const readHeaderTimeout = 5 * time.Second

// FlagConfigName is the -config flag name, used in usage text and tests.
const FlagConfigName = "config"

// newLogger builds the process logger from the validated configuration:
// level from log_level, handler format from log_format, writing to stderr.
// Values are already validated by config.Load.
func newLogger(stderr io.Writer, level, format string) *slog.Logger {
	var lvl slog.Level
	switch level {
	case "debug":
		lvl = slog.LevelDebug
	case "warn":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}
	opts := &slog.HandlerOptions{Level: lvl}
	var h slog.Handler
	if format == "json" {
		h = slog.NewJSONHandler(stderr, opts)
	} else {
		h = slog.NewTextHandler(stderr, opts)
	}
	return slog.New(h)
}

// resolveConfigPath parses command-line flags and the CONFIG_PATH environment
// variable into the config file path. On a flag-parse error it prints usage
// to stderr and returns ok=false with code 2; on a missing config path it
// prints a hint and returns ok=false with code 1. ok=true carries the
// resolved path. Messages go through log, the pre-config default logger.
func resolveConfigPath(args []string, stderr io.Writer, log *slog.Logger) (path string, code int, ok bool) {
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
		return "", 2, false
	}

	cfgPath := *cfgFlag
	if cfgPath == "" {
		cfgPath = os.Getenv(EnvConfigPath)
	}
	if cfgPath == "" {
		log.ErrorContext(context.Background(), "no config file: use -config or set "+EnvConfigPath)
		return "", 1, false
	}
	return cfgPath, 0, true
}

// Option configures optional behavior of Run beyond the config file.
type Option func(*runOptions)

// runOptions carries the optional behavior Run applies after the config file
// has been loaded.
type runOptions struct {
	onListen func(net.Listener)
	handler  http.Handler
}

// WithOnListen registers a callback invoked with the bound listener once Run
// binds it, in addition to the "listening on" message Run always writes to
// stderr. Useful for discovering an ephemeral port (cfg.Listen ending in
// ":0") without parsing stderr.
func WithOnListen(f func(net.Listener)) Option {
	return func(o *runOptions) { o.onListen = f }
}

// WithHandler overrides the handler Run serves instead of the App's own
// Handler(). Intended for tests that need to observe or control individual
// requests (for example, verifying graceful-shutdown drain behavior), since
// App's production Handler() always answers instantly from a cached
// snapshot and cannot itself be made to block.
func WithHandler(h http.Handler) Option {
	return func(o *runOptions) { o.handler = h }
}

// Run is the command's main wiring, returning the process exit code.
//
// The config path resolves per EnvConfigPath and FlagConfigName. A load or
// validation failure prints the error to stderr and returns 1 without
// starting the HTTP server.
//
// ctx is the process-lifetime context, canceled by SIGTERM/SIGINT in main.
// On cancellation Run stops polling and shuts the server down per
// shutdownTimeout. opts applies optional behavior (WithOnListen, WithHandler);
// production callers pass none.
func Run(ctx context.Context, args []string, stderr io.Writer, opts ...Option) int {
	// Pre-config logger: config-driven level/format are unknowable before
	// the file loads, so early diagnostics use the text default at info.
	log := newLogger(stderr, config.DefaultLogLevel, config.DefaultLogFormat)

	cfgPath, code, ok := resolveConfigPath(args, stderr, log)
	if !ok {
		return code
	}

	cfg, err := config.Load(cfgPath)
	if err != nil {
		log.ErrorContext(ctx, "failed to load config", "error", err)
		return 1
	}

	// The config-provided logger serves everything from here on: cli's own
	// startup, listen, and shutdown messages and the App's diagnostics share
	// one handler, so a process never emits two log formats.
	log = newLogger(stderr, cfg.LogLevel, cfg.LogFormat)

	var ro runOptions
	for _, o := range opts {
		o(&ro)
	}

	return serve(ctx, cfg, log, func(l net.Listener) {
		log.InfoContext(ctx, "listening on", "addr", l.Addr().String())
		if ro.onListen != nil {
			ro.onListen(l)
		}
	}, ro.handler)
}

// serve constructs the App with the shared logger, runs one synchronous
// refresh, serves the handler on cfg.Listen, and polls on cfg.PollInterval
// until ctx is canceled.
// onListen is called with the bound listener (use nil to ignore it).
// handler, when non-nil, overrides theApp.Handler().
func serve(ctx context.Context, cfg *config.Config, log *slog.Logger, onListen func(net.Listener), handler http.Handler) int {
	theApp, err := app.New(*cfg, app.WithLogger(log))
	if err != nil {
		log.ErrorContext(ctx, "failed to construct app", "error", err)
		return 1
	}

	// App.Refresh never fails on a downstream being down or slow: it retries,
	// falls back to the last-known-good snapshot, and logs per-poll failures
	// itself. The only error it returns is a failure to marshal the merged
	// snapshot, which would be a bug; surface it without killing startup.
	if err := theApp.Refresh(ctx); err != nil {
		log.ErrorContext(ctx, "initial refresh failed", "error", err)
	}

	pollCtx, stopPoll := context.WithCancel(ctx)
	defer stopPoll()
	go pollLoop(pollCtx, theApp, cfg.PollInterval, log)

	var lc net.ListenConfig
	listener, err := lc.Listen(ctx, "tcp", cfg.Listen)
	if err != nil {
		log.ErrorContext(ctx, "listen failed", "addr", cfg.Listen, "error", err)
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
		ReadHeaderTimeout: readHeaderTimeout,
	}
	go func() {
		if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.ErrorContext(ctx, "serve failed", "error", err)
		}
	}()

	<-ctx.Done()
	stopPoll()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	_ = server.Shutdown(shutdownCtx)
	log.InfoContext(ctx, "shutdown complete")
	return 0
}

// pollLoop calls Refresh on every tick until ctx is canceled.  The first tick
// fires after one interval; the initial snapshot comes from the synchronous
// refresh in serve.
func pollLoop(ctx context.Context, theApp *app.App, interval time.Duration, log *slog.Logger) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// As with the initial refresh, Refresh returns non-nil only when
			// the merged snapshot cannot be marshaled; per-downstream poll
			// failures are logged and recovered inside the App. Surface the
			// marshal error and keep polling.
			if err := theApp.Refresh(ctx); err != nil {
				log.ErrorContext(ctx, "refresh failed", "error", err)
			}
		}
	}
}
