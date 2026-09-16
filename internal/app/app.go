package app

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strings"
	"sync/atomic"

	"github.com/gringolito/traefik-scout/internal/config"
)

// unsafeChars matches any character not in the safe set [a-zA-Z0-9_-].
// One or more consecutive unsafe characters are collapsed to a single dash.
var unsafeChars = regexp.MustCompile(`[^a-zA-Z0-9_-]+`)

type snapshot struct {
	data []byte
	etag string
}

// App polls downstream Traefik instances and serves their merged route
// configuration as a single Traefik file-provider JSON document.
type App struct {
	cfg    config.Config
	client *http.Client
	snap   atomic.Pointer[snapshot]
}

// New validates cfg and returns a ready App.  The App makes no network calls
// until Refresh is invoked.
func New(cfg config.Config) (*App, error) {
	return &App{
		cfg:    cfg,
		client: &http.Client{Timeout: cfg.RequestTimeout},
	}, nil
}

// Refresh fetches rawdata from every downstream, transforms the routers and
// services, and atomically replaces the snapshot that Handler serves.
func (a *App) Refresh(ctx context.Context) error {
	out := &httpConfig{
		Routers:  make(map[string]*outRouter),
		Services: make(map[string]*outService),
	}

	for _, ds := range a.cfg.Downstreams {
		if err := a.fetchAndMerge(ctx, ds, out); err != nil {
			return err
		}
	}

	data, err := json.Marshal(&configOutput{HTTP: out})
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

func (a *App) fetchAndMerge(ctx context.Context, ds config.Downstream, out *httpConfig) error {
	url := ds.APIAddress + "/api/rawdata"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil) //nolint:gosec // URL is operator config, not user input
	if err != nil {
		return fmt.Errorf("build request for %s: %w", ds.Name, err)
	}

	resp, err := a.client.Do(req)
	if err != nil {
		return fmt.Errorf("fetch %s rawdata: %w", ds.Name, err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("downstream %s: unexpected status %d", ds.Name, resp.StatusCode)
	}

	body := io.LimitReader(resp.Body, a.cfg.MaxResponseSize)

	var raw rawdataResponse
	if err := json.NewDecoder(body).Decode(&raw); err != nil {
		return fmt.Errorf("decode %s rawdata: %w", ds.Name, err)
	}

	allow := entrypointSet(ds.AllowedEntrypoints)

	for name, r := range raw.Routers {
		if isInternalProvider(name, r.Provider) {
			continue
		}
		if r.Status == "disabled" {
			continue
		}
		if allow != nil && !hasAllowedEntrypoint(r.EntryPoints, allow) {
			continue
		}

		base := stripProvider(name)
		outName := ds.Name + "-" + unsafeChars.ReplaceAllString(base, "-")

		out.Routers[outName] = &outRouter{
			EntryPoints: a.cfg.EdgeEntrypoints,
			Service:     ds.Name,
			Rule:        r.Rule,
		}
	}

	// One service per downstream; all its routers point here.
	out.Services[ds.Name] = &outService{
		LoadBalancer: &loadBalancer{
			Servers:        []server{{URL: ds.TrafficAddress}},
			PassHostHeader: true,
		},
	}

	return nil
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
