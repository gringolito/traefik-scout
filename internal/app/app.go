package app

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"regexp"
	"strings"
	"sync/atomic"

	"github.com/traefik/genconf/dynamic"

	"github.com/gringolito/traefik-scout/internal/config"
)

// unsafeChars matches any character not in the safe set [a-zA-Z0-9_-].
// One or more consecutive unsafe characters are collapsed to a single dash.
var unsafeChars = regexp.MustCompile(`[^a-zA-Z0-9_-]+`)

type snapshot struct {
	data []byte
	etag string
}

// Option configures an App at construction time.
type Option func(*App)

// WithLogger sets the logger used by App for diagnostic output.
// Defaults to slog.Default() when not supplied.
func WithLogger(l *slog.Logger) Option {
	return func(a *App) { a.log = l }
}

// App polls downstream Traefik instances and serves their merged route
// configuration as a single Traefik file-provider JSON document.
type App struct {
	cfg    config.Config
	client *http.Client
	log    *slog.Logger
	// lastGood holds the most recent successful rawdata response per downstream.
	// It is only accessed during Refresh, which the caller must not invoke
	// concurrently.
	lastGood map[string]*rawdataResponse
	snap     atomic.Pointer[snapshot]
}

// New returns a ready App configured from cfg.  Config is already validated by
// config.Load; New always returns a nil error but keeps the error return for
// future validation at construction time.
func New(cfg config.Config, opts ...Option) (*App, error) {
	a := &App{
		cfg:      cfg,
		client:   &http.Client{Timeout: cfg.RequestTimeout},
		log:      slog.Default(),
		lastGood: make(map[string]*rawdataResponse),
	}
	for _, o := range opts {
		o(a)
	}
	return a, nil
}

// mergeResult accumulates routers, services, and rule-contributor tracking
// across a single Refresh cycle.
type mergeResult struct {
	out              *dynamic.HTTPConfiguration
	ruleContributors map[string][]string // rule string -> downstream names that contributed it
}

func newMergeResult() *mergeResult {
	return &mergeResult{
		out: &dynamic.HTTPConfiguration{
			Routers:  make(map[string]*dynamic.Router),
			Services: make(map[string]*dynamic.Service),
		},
		ruleContributors: make(map[string][]string),
	}
}

// Refresh polls every downstream, falls back to last-known-good data on
// failure, and atomically replaces the snapshot that Handler serves.
//
// A downstream that fails to respond is logged at WARN; its most recent
// successful response is used instead so its routes remain in the merged
// output.  Only when a downstream has never responded successfully does it
// contribute nothing for that cycle.  If no downstream contributes any data
// (all failed, none have prior data), the existing snapshot is kept rather
// than replacing it with an empty configuration.
//
// After merging, identical routing rules contributed by different downstreams
// are logged at WARN naming both contributors; the configuration is still
// served so Traefik can resolve the conflict by priority.
func (a *App) Refresh(ctx context.Context) error {
	result := newMergeResult()
	contributed := false

	for _, ds := range a.cfg.Downstreams {
		raw, err := a.fetchRawData(ctx, ds)
		if err != nil {
			a.log.WarnContext(ctx, "poll failed, using last-known-good",
				"downstream", ds.Name, "error", err)
			raw = a.lastGood[ds.Name] // nil when this downstream has never succeeded
		} else {
			a.lastGood[ds.Name] = raw // store even when zero routers (valid empty result)
		}

		if raw == nil {
			continue
		}
		contributed = true
		a.mergeDownstream(ds, raw, result)
	}

	// Nothing to merge: all downstreams failed and none have prior data.
	// Retain the existing snapshot instead of overwriting it with empty output.
	if !contributed {
		return nil
	}

	for rule, contributors := range result.ruleContributors {
		if len(contributors) > 1 {
			a.log.WarnContext(ctx, "identical routing rule detected",
				"rule", rule,
				"downstreams", contributors,
			)
		}
	}

	data, err := json.Marshal(&dynamic.Configuration{HTTP: result.out})
	if err != nil {
		return fmt.Errorf("marshal config: %w", err)
	}

	sum := sha256.Sum256(data)
	a.snap.Store(&snapshot{
		data: data,
		etag: fmt.Sprintf(`"%x"`, sum[:8]),
	})
	return nil
}

// fetchRawData fetches and decodes one downstream's /api/rawdata.
func (a *App) fetchRawData(ctx context.Context, ds config.Downstream) (*rawdataResponse, error) {
	url := ds.APIAddress + "/api/rawdata"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil) //nolint:gosec // URL is operator config, not user input
	if err != nil {
		return nil, fmt.Errorf("build request for %s: %w", ds.Name, err)
	}

	resp, err := a.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetch %s rawdata: %w", ds.Name, err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("downstream %s: unexpected status %d", ds.Name, resp.StatusCode)
	}

	body := io.LimitReader(resp.Body, a.cfg.MaxResponseSize)

	var raw rawdataResponse
	if err := json.NewDecoder(body).Decode(&raw); err != nil {
		return nil, fmt.Errorf("decode %s rawdata: %w", ds.Name, err)
	}
	return &raw, nil
}

// mergeDownstream transforms raw's routers and emits one service into result.
func (a *App) mergeDownstream(ds config.Downstream, raw *rawdataResponse, result *mergeResult) {
	allow := entrypointSet(ds.AllowedEntrypoints)

	for name, r := range raw.Routers {
		if isInternalProvider(name, r.Provider) {
			continue
		}
		if r.Status == "disabled" {
			continue
		}
		// A nil allow set means no entrypoints were configured: pass the router
		// through unconditionally.  Empty AllowedEntrypoints = accept all.
		if allow != nil && !hasAllowedEntrypoint(r.EntryPoints, allow) {
			continue
		}

		base := stripProvider(name)
		outName := ds.Name + "-" + unsafeChars.ReplaceAllString(base, "-")

		result.out.Routers[outName] = &dynamic.Router{
			EntryPoints: a.cfg.EdgeEntrypoints,
			Service:     ds.Name,
			Rule:        r.Rule,
			Priority:    r.Priority + ds.PriorityOffset,
		}
		result.ruleContributors[r.Rule] = append(result.ruleContributors[r.Rule], ds.Name)
	}

	// One service per downstream; all its routers point here.
	passHostHeader := true
	result.out.Services[ds.Name] = &dynamic.Service{
		LoadBalancer: &dynamic.ServersLoadBalancer{
			Servers:        []dynamic.Server{{URL: ds.TrafficAddress}},
			PassHostHeader: &passHostHeader,
		},
	}
}

// isInternalProvider returns true when the router belongs to Traefik's
// built-in "internal" provider, detected either from the provider field or
// the "@internal" suffix on the key.
func isInternalProvider(name, provider string) bool {
	return provider == "internal" || strings.HasSuffix(name, "@internal")
}

// stripProvider removes the "@provider" qualifier from a Traefik router key.
func stripProvider(name string) string {
	if i := strings.LastIndex(name, "@"); i >= 0 {
		return name[:i]
	}
	return name
}

func entrypointSet(eps []string) map[string]struct{} {
	if len(eps) == 0 {
		return nil
	}
	s := make(map[string]struct{}, len(eps))
	for _, ep := range eps {
		s[ep] = struct{}{}
	}
	return s
}

func hasAllowedEntrypoint(eps []string, allow map[string]struct{}) bool {
	for _, ep := range eps {
		if _, ok := allow[ep]; ok {
			return true
		}
	}
	return false
}

// Handler returns an http.Handler that serves the current snapshot at the
// configured path.  Reads from an atomically-swapped pointer so an in-flight
// Refresh never blocks a request.
func (a *App) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc(a.cfg.ConfigPath, a.serveSnapshot)
	return mux
}

func (a *App) serveSnapshot(w http.ResponseWriter, r *http.Request) {
	snap := a.snap.Load()
	if snap == nil {
		http.Error(w, "not ready", http.StatusServiceUnavailable)
		return
	}

	if r.Header.Get("If-None-Match") == snap.etag {
		w.WriteHeader(http.StatusNotModified)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("ETag", snap.etag)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(snap.data)
}
