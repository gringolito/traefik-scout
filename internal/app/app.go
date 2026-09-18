package app

import (
	"bytes"
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

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
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

// fetchRetryMaxAttempts bounds in-cycle retries for a failing downstream;
// fetchRetryBaseDelay doubles per attempt up to fetchRetryMaxDelay.
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

// labelName is the per-downstream metric label.
const labelName = "name"

// metrics bundles the Prometheus collectors owned by one App.  They live on a
// dedicated registry (not the global default) so multiple App instances do
// not collide.
type metrics struct {
	reg         *prometheus.Registry
	lastSuccess *prometheus.GaugeVec
	attempts    *prometheus.CounterVec
	failures    *prometheus.CounterVec
	duration    *prometheus.HistogramVec
	routers     prometheus.Gauge
	generation  prometheus.Counter
}

func newMetrics() *metrics {
	m := &metrics{
		reg: prometheus.NewRegistry(),
		lastSuccess: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "scout_downstream_last_success_timestamp_seconds",
				Help: "Unix timestamp of the most recent successful poll per downstream.",
			},
			[]string{labelName},
		),
		attempts: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "scout_downstream_poll_attempts_total",
				Help: "Total downstream poll attempts, including retries.",
			},
			[]string{labelName},
		),
		failures: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "scout_downstream_poll_failures_total",
				Help: "Total downstream polls that failed all retry attempts.",
			},
			[]string{labelName},
		),
		duration: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "scout_downstream_poll_duration_seconds",
				Help:    "Duration of one full poll (including retries) per downstream.",
				Buckets: prometheus.DefBuckets,
			},
			[]string{labelName},
		),
		routers: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name: "scout_routers",
				Help: "Number of routers in the latest served merged snapshot.",
			},
		),
		generation: prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: "scout_merged_output_generation_total",
				Help: "Times the merged output actually changed across Refresh cycles.",
			},
		),
	}
	m.reg.MustRegister(m.lastSuccess, m.attempts, m.failures, m.duration, m.routers, m.generation)
	return m
}

// Option configures an App at construction time.
type Option func(*App)

// WithLogger sets the logger used by App for diagnostic output.
// Defaults to slog.Default() when not supplied.
func WithLogger(l *slog.Logger) Option {
	return func(a *App) { a.log = l }
}

// WithClock overrides the App's time source, defaulting to time.Now.
// Intended for injecting a deterministic clock in tests.
func WithClock(now func() time.Time) Option {
	return func(a *App) { a.now = now }
}

// WithSleep overrides the delay mechanism used between fetch-retry attempts,
// defaulting to time.Sleep. Intended for injecting a fast, deterministic
// sleep in tests.
func WithSleep(sleep func(time.Duration)) Option {
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
	// everSucceeded records whether any downstream has ever been polled
	// successfully.  Atomic so /readyz can read it from handler goroutines.
	everSucceeded atomic.Bool
	// metrics owns this App's Prometheus collectors and registry.
	metrics *metrics
	snap    atomic.Pointer[snapshot]
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
		metrics:  newMetrics(),
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

// Refresh polls every downstream, merges their routes, and atomically
// replaces the snapshot Handler serves.
//
// A downstream that fails is retried up to fetchRetryMaxAttempts times with
// capped exponential backoff, then falls back to its last-known-good data,
// or nothing if it has never succeeded. HTTP 200 with zero routers is a
// valid empty result, not a failure.
//
// An optional per-downstream StalenessLimit withdraws a downstream's routes
// once its last successful poll is older than the limit, and reinstates them
// on the next success without a restart. If nothing was contributed or
// withdrawn, the existing snapshot is kept.
//
// Duplicate routing rules across downstreams are logged at WARN but still
// served, letting Traefik resolve the conflict by priority.
//
// Poll attempts, failures, duration, last-success time, and the served router
// count are exported as Prometheus metrics on /metrics.
func (a *App) Refresh(ctx context.Context) error {
	result := newMergeResult()
	contributed := false
	// withdrew forces a republish when staleness removed the only contributor.
	withdrew := false

	for _, ds := range a.cfg.Downstreams {
		st := a.state[ds.Name]
		if st == nil {
			st = &pollState{}
			a.state[ds.Name] = st
		}

		raw := a.pollDownstream(ctx, ds, st)

		// Withdraw routes once the last successful poll is older than the limit.
		if raw != nil && ds.StalenessLimit > 0 {
			if st.lastSuccess.IsZero() || a.now().Sub(st.lastSuccess) > ds.StalenessLimit {
				// Log once on the transition into withdrawal, not every cycle.
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
		a.mergeDownstream(ctx, ds, raw, result)
	}

	// Nothing changed: keep the existing snapshot instead of clearing it.
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
	prev := a.snap.Load()
	if prev == nil || !bytes.Equal(prev.data, data) {
		// Publish a new generation only when the merged output changed.
		a.metrics.generation.Inc()
		a.log.InfoContext(ctx, "merged configuration changed",
			"routers", len(result.out.Routers))
	}
	a.snap.Store(&snapshot{
		data: data,
		etag: fmt.Sprintf(`"%x"`, sum[:8]),
	})
	a.metrics.routers.Set(float64(len(result.out.Routers)))
	return nil
}

// pollDownstream polls one downstream and returns its data, falling back to
// last-known-good on failure.
func (a *App) pollDownstream(ctx context.Context, ds config.Downstream, st *pollState) *rawdataResponse {
	// Touching the failure counter pre-creates the per-downstream child.
	a.metrics.failures.WithLabelValues(ds.Name)
	start := a.now()
	var raw *rawdataResponse
	if r, err := a.fetchWithRetry(ctx, ds); err != nil {
		a.metrics.failures.WithLabelValues(ds.Name).Inc()
		a.log.WarnContext(ctx, "poll failed, using last-known-good",
			"downstream", ds.Name, "error", err, "attempts", fetchRetryMaxAttempts)
		raw = a.lastGood[ds.Name] // nil when this downstream has never succeeded
		a.log.DebugContext(ctx, "downstream polled",
			"downstream", ds.Name, "success", false)
	} else {
		a.everSucceeded.Store(true)
		a.metrics.lastSuccess.WithLabelValues(ds.Name).Set(float64(a.now().Unix()))
		a.log.DebugContext(ctx, "downstream polled",
			"downstream", ds.Name, "success", true)
		if st.stale {
			st.stale = false
			a.log.InfoContext(ctx, "stale downstream recovered, reinstating routes",
				"downstream", ds.Name)
		}
		st.lastSuccess = a.now()
		a.lastGood[ds.Name] = r // store even when zero routers (valid empty result)
		raw = r
	}
	a.metrics.duration.WithLabelValues(ds.Name).Observe(a.now().Sub(start).Seconds())
	return raw
}

// fetchWithRetry retries fetchRawData up to fetchRetryMaxAttempts times with
// capped exponential backoff, returning early on success or context
// cancellation.
func (a *App) fetchWithRetry(ctx context.Context, ds config.Downstream) (*rawdataResponse, error) {
	var (
		raw *rawdataResponse
		err error
	)
	for attempt := 1; attempt <= fetchRetryMaxAttempts; attempt++ {
		a.metrics.attempts.WithLabelValues(ds.Name).Inc()
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
func (a *App) mergeDownstream(ctx context.Context, ds config.Downstream, raw *rawdataResponse, result *mergeResult) {
	allow := entrypointSet(ds.AllowedEntrypoints)

	for name, r := range raw.Routers {
		if isInternalProvider(name, r.Provider) {
			continue
		}
		if r.Status != "enabled" {
			a.log.DebugContext(ctx, "skipping router with non-enabled status",
				"router", name, "status", r.Status)
			continue
		}
		// Config validation requires at least one allowed entrypoint per
		// downstream, so the filter is unconditional.
		if !hasAllowedEntrypoint(r.EntryPoints, allow) {
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

// Handler returns an http.Handler serving:
//
//   - the configured snapshot path (a.serveSnapshot)
//   - /healthz: always 200 while the process is serving, independent of
//     downstream health
//   - /readyz: 200 once at least one downstream has ever been polled
//     successfully (cumulative), 503 before that
//   - /metrics: Prometheus exposition from the App's dedicated registry
//
// Reads from an atomically-swapped pointer so an in-flight Refresh never
// blocks a request.
func (a *App) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc(a.cfg.ConfigPath, a.serveSnapshot)
	mux.HandleFunc(config.PathHealthz, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc(config.PathReadyz, func(w http.ResponseWriter, _ *http.Request) {
		if !a.everSucceeded.Load() {
			http.Error(w, "no downstream polled successfully yet", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	mux.Handle(config.PathMetrics, promhttp.HandlerFor(a.metrics.reg, promhttp.HandlerOpts{}))
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
