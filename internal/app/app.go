package app

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"regexp"
	"strings"
	"sync/atomic"
	"time"

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

// Fetch-retry timing for a failing downstream, applied entirely within a
// single Refresh cycle so resilience does not depend on any external poll
// cadence.  A failing downstream is attempted fetchRetryMaxAttempts times,
// sleeping fetchRetryBaseDelay (doubled per attempt, capped at
// fetchRetryMaxDelay) between attempts.  500ms doubling to 2s rides out
// brief blips (connection resets, one bad response) while bounding the
// worst-case added latency for a permanently-dead downstream at about 1.5s
// per cycle, small enough to sit inside a normal poll budget.
const (
	fetchRetryMaxAttempts = 3
	fetchRetryBaseDelay   = 500 * time.Millisecond
	fetchRetryMaxDelay    = 2 * time.Second
)

// pollState tracks one downstream's staleness bookkeeping across Refresh
// cycles.
type pollState struct {
	// lastSuccess is the time of the most recent successful poll; zero when
	// the downstream has never succeeded.
	lastSuccess time.Time
	// stale records whether the downstream is currently withdrawn for
	// staleness, so the withdrawal WARN fires only on the transition into
	// withdrawal instead of on every subsequent cycle.
	stale bool
}

// Option configures an App at construction time.
type Option func(*App)

// WithLogger sets the logger used by App for diagnostic output.
// Defaults to slog.Default() when not supplied.
func WithLogger(l *slog.Logger) Option {
	return func(a *App) { a.log = l }
}

// withClock overrides the App's time source.  It is unexported on purpose:
// only tests inject a clock; production always uses time.Now.
func withClock(now func() time.Time) Option {
	return func(a *App) { a.now = now }
}

// withSleep overrides the delay mechanism between fetch-retry attempts.  It
// is unexported on purpose: only tests inject a sleep; production always
// uses time.Sleep.
func withSleep(sleep func(time.Duration)) Option {
	return func(a *App) { a.sleep = sleep }
}

// App polls downstream Traefik instances and serves their merged route
// configuration as a single Traefik file-provider JSON document.
type App struct {
	cfg     config.Config
	clients map[string]*http.Client // one per downstream, keyed by name
	log     *slog.Logger
	// lastGood holds the most recent successful rawdata response per downstream.
	// It is only accessed during Refresh, which the caller must not invoke
	// concurrently.
	lastGood map[string]*rawdataResponse
	// state holds per-downstream retry bookkeeping, accessed under the same
	// single-caller constraint as lastGood.
	state map[string]*pollState
	// now is the time source, injectable for deterministic tests.
	now func() time.Time
	// sleep pauses between fetch-retry attempts, injectable for
	// deterministic tests.
	sleep func(time.Duration)
	snap  atomic.Pointer[snapshot]
}

// New returns a ready App configured from cfg.  Config is already validated by
// config.Load; New returns an error only when a downstream's TLS configuration
// cannot be loaded.
func New(cfg config.Config, opts ...Option) (*App, error) {
	clients := make(map[string]*http.Client, len(cfg.Downstreams))
	for _, ds := range cfg.Downstreams {
		c, err := buildClient(ds, cfg.RequestTimeout)
		if err != nil {
			return nil, fmt.Errorf("downstream %q: %w", ds.Name, err)
		}
		clients[ds.Name] = c
	}
	a := &App{
		cfg:      cfg,
		clients:  clients,
		log:      slog.Default(),
		lastGood: make(map[string]*rawdataResponse),
		state:    make(map[string]*pollState),
		now:      time.Now,
		sleep:    time.Sleep,
	}
	for _, o := range opts {
		o(a)
	}
	return a, nil
}

// buildClient constructs an http.Client for one downstream.  When the
// downstream carries no TLS config the default transport is used unchanged.
func buildClient(ds config.Downstream, timeout time.Duration) (*http.Client, error) {
	if ds.TLS == nil {
		return &http.Client{Timeout: timeout}, nil
	}

	tlsCfg := &tls.Config{
		InsecureSkipVerify: ds.TLS.InsecureSkipVerify, //nolint:gosec // operator-controlled setting
	}

	if ds.TLS.CA != "" {
		caPEM, err := os.ReadFile(ds.TLS.CA)
		if err != nil {
			return nil, fmt.Errorf("read CA cert %q: %w", ds.TLS.CA, err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caPEM) {
			return nil, fmt.Errorf("CA file %q: no valid PEM certificates found", ds.TLS.CA)
		}
		tlsCfg.RootCAs = pool
	}

	if ds.TLS.Cert != "" || ds.TLS.Key != "" {
		cert, err := tls.LoadX509KeyPair(ds.TLS.Cert, ds.TLS.Key)
		if err != nil {
			return nil, fmt.Errorf("load client certificate: %w", err)
		}
		tlsCfg.Certificates = []tls.Certificate{cert}
	}

	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.TLSClientConfig = tlsCfg
	return &http.Client{
		Timeout:   timeout,
		Transport: transport,
	}, nil
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
// Failed polls are retried within the same cycle: each downstream is
// attempted up to fetchRetryMaxAttempts times, with a capped exponential
// delay (fetchRetryBaseDelay doubling per attempt, capped at
// fetchRetryMaxDelay) between attempts, before falling back to
// last-known-good data (or contributing nothing, if the downstream has
// never succeeded).
//
// A downstream may also configure a staleness limit (zero, the default, means
// unlimited).  When the time since that downstream's last successful poll
// exceeds the limit, its routes are withdrawn from the merged output instead
// of being served from last-known-good data.  The check uses the
// last-success timestamp, which is only advanced by successful polls.  A
// withdrawn downstream's routes reappear on its next successful poll
// without a restart.
//
// A downstream that responds HTTP 200 with zero routers is a valid empty
// result: its snapshot is replaced with an empty one and its routes drop out
// of the merged output, without being treated as a failure.
//
// After merging, identical routing rules contributed by different downstreams
// are logged at WARN naming both contributors; the configuration is still
// served so Traefik can resolve the conflict by priority.
func (a *App) Refresh(ctx context.Context) error {
	result := newMergeResult()
	contributed := false
	// withdrew records that a staleness limit removed previously-served data
	// this cycle: the snapshot must be republished (possibly empty) even when
	// no downstream contributed, so withdrawal takes effect.
	withdrew := false

	for _, ds := range a.cfg.Downstreams {
		st := a.state[ds.Name]
		if st == nil {
			st = &pollState{}
			a.state[ds.Name] = st
		}

		var raw *rawdataResponse
		if r, err := a.fetchWithRetry(ctx, ds); err != nil {
			a.log.WarnContext(ctx, "poll failed, using last-known-good",
				"downstream", ds.Name, "error", err, "attempts", fetchRetryMaxAttempts)
			raw = a.lastGood[ds.Name] // nil when this downstream has never succeeded
		} else {
			if st.stale {
				st.stale = false
				a.log.InfoContext(ctx, "stale downstream recovered, reinstating routes",
					"downstream", ds.Name)
			}
			st.lastSuccess = a.now()
			a.lastGood[ds.Name] = r // store even when zero routers (valid empty result)
			raw = r
		}

		// Enforce the optional per-downstream staleness limit: if no poll has
		// succeeded within the limit, withdraw this downstream's routes for
		// this cycle instead of serving last-known-good data indefinitely.
		// The check uses the last-success timestamp, which is only advanced
		// by successful polls.
		if raw != nil && ds.StalenessLimit > 0 {
			if st.lastSuccess.IsZero() || a.now().Sub(st.lastSuccess) > ds.StalenessLimit {
				// Warn only on the transition into withdrawal; while the
				// downstream stays stale it is logged once, not once per
				// cycle.
				if !st.stale {
					st.stale = true
					a.log.WarnContext(ctx, "downstream stale, withdrawing routes",
						"downstream", ds.Name,
						"last_success", st.lastSuccess,
						"staleness_limit", ds.StalenessLimit)
				}
				raw = nil
				withdrew = true
			}
		}

		if raw == nil {
			continue
		}
		contributed = true
		a.mergeDownstream(ds, raw, result)
	}

	// Nothing to merge: all downstreams failed and none have prior data, and
	// no staleness withdrawal occurred.  Retain the existing snapshot instead
	// of overwriting it with empty output.
	if !contributed && !withdrew {
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

// fetchWithRetry calls fetchRawData up to fetchRetryMaxAttempts times for
// one downstream, sleeping for a capped exponential delay between attempts.
// On success the response is returned immediately; when every attempt fails,
// the last error is returned.  A cancelled or expired context is checked
// before each sleep so it is never blocked by a retry; on that path the
// context error is returned.
func (a *App) fetchWithRetry(ctx context.Context, ds config.Downstream) (*rawdataResponse, error) {
	var (
		raw *rawdataResponse
		err error
	)
	for attempt := 1; attempt <= fetchRetryMaxAttempts; attempt++ {
		if raw, err = a.fetchRawData(ctx, ds); err == nil {
			return raw, nil
		}
		if attempt == fetchRetryMaxAttempts {
			break
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		delay := min(fetchRetryBaseDelay<<uint(attempt-1), fetchRetryMaxDelay)
		a.log.DebugContext(ctx, "retrying downstream poll",
			"downstream", ds.Name, "attempt", attempt+1, "delay", delay, "error", err)
		a.sleep(delay)
	}
	return nil, err
}

// fetchRawData fetches and decodes one downstream's /api/rawdata.
func (a *App) fetchRawData(ctx context.Context, ds config.Downstream) (*rawdataResponse, error) {
	url := ds.APIAddress + "/api/rawdata"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil) //nolint:gosec // URL is operator config, not user input
	if err != nil {
		return nil, fmt.Errorf("build request for %s: %w", ds.Name, err)
	}

	if ds.Auth != nil {
		for k, v := range ds.Auth.Headers {
			req.Header.Set(k, v)
		}
		if ds.Auth.Username != "" {
			req.SetBasicAuth(ds.Auth.Username, ds.Auth.Password)
		}
	}

	resp, err := a.clients[ds.Name].Do(req)
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
