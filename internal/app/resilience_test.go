package app_test

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gringolito/traefik-scout/internal/app"
	"github.com/gringolito/traefik-scout/internal/config"
)

// Issue #6 tests that need to control the passage of time inject a fake clock
// through the WithClock test hook exported from export_test.go, and stay in
// the external test package so fixture constants are shared with app_test.go.

// fakeClock is a manually-advanced clock for deterministic staleness tests.
type fakeClock struct {
	now atomic.Int64 // unix nanoseconds
}

func (c *fakeClock) Now() time.Time { return time.Unix(0, c.now.Load()) }
func (c *fakeClock) Advance(d time.Duration) {
	c.now.Add(int64(d))
}

// fakeDS is a downstream whose /api/rawdata response is switched between a
// valid routers payload and failures, and which counts every rawdata request.
type fakeDS struct {
	srv      *httptest.Server
	hits     atomic.Int32
	mode     atomic.Int32 // modeOK, modeFail, or modeFailNext
	failLeft atomic.Int32 // requests still failing when mode is modeFailNext
}

const (
	modeOK   = 0
	modeFail = 1
	// modeFailNext fails the next failLeft requests, then serves the OK
	// payload again.
	modeFailNext = 2
)

func newFakeDS(t *testing.T) *fakeDS {
	t.Helper()
	ds := &fakeDS{}
	ds.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != rawdataPath {
			http.NotFound(w, r)
			return
		}
		ds.hits.Add(1)
		fail := ds.mode.Load() == modeFail ||
			(ds.mode.Load() == modeFailNext && ds.failLeft.Add(-1) >= 0)
		if fail {
			http.Error(w, "upstream error", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			jsonFieldRouters: map[string]any{
				"app-a@docker": map[string]any{
					"entryPoints": []string{valueWeb},
					"rule":        `Host("a.example.com")`,
					"status":      "enabled",
					"provider":    "docker",
				},
			},
		})
	}))
	t.Cleanup(ds.srv.Close)
	return ds
}

func (ds *fakeDS) url() string { return ds.srv.URL }

// failNext puts the downstream into a mode where the next n rawdata requests
// fail with HTTP 500 and subsequent requests succeed again.
func (ds *fakeDS) failNext(n int32) {
	ds.mode.Store(modeFailNext)
	ds.failLeft.Store(n)
}

// healthDS is a permanently-healthy downstream serving one router.
func healthDS(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != rawdataPath {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			jsonFieldRouters: map[string]any{
				"app-b@docker": map[string]any{
					"entryPoints": []string{valueWeb},
					"rule":        `Host("b.example.com")`,
					"status":      "enabled",
					"provider":    "docker",
				},
			},
		})
	}))
	t.Cleanup(srv.Close)
	return srv
}

// resilienceConfig builds a two-downstream Config: "primary" pointing at
// primaryURL (with the given staleness limit) and "gpu" pointing at healthyURL.
func resilienceConfig(primaryURL, healthyURL string, staleness time.Duration) config.Config {
	return config.Config{
		ConfigPath:      configPath,
		RequestTimeout:  5 * time.Second,
		MaxResponseSize: 10 * 1024 * 1024,
		EdgeEntrypoints: []string{valueWeb},
		Downstreams: []config.Downstream{
			{Name: "primary", APIAddress: primaryURL, TrafficAddress: "http://primary:80", AllowedEntrypoints: []string{valueWeb}, StalenessLimit: staleness},
			{Name: "gpu", APIAddress: healthyURL, TrafficAddress: "http://gpu:80", AllowedEntrypoints: []string{valueWeb}},
		},
	}
}

// resilienceApp builds an App with the fake clock injected and runs one
// Refresh, failing the test on error.  Sleep injection is optional: when
// omitted, sleeps between retry attempts are no-ops so failing cycles stay
// instant and deterministic.
func resilienceApp(t *testing.T, cfg config.Config, clock *fakeClock, sleep ...func(time.Duration)) *app.App {
	t.Helper()
	opts := []app.Option{app.WithClock(clock.Now)}
	if len(sleep) > 0 {
		opts = append(opts, app.WithSleep(sleep[0]))
	} else {
		opts = append(opts, app.WithSleep(func(time.Duration) {}))
	}
	a, err := app.New(cfg, opts...)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := a.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	return a
}

// servedRouters queries the handler and returns the served router keys.
func servedRouters(t *testing.T, a *app.App) map[string]bool {
	t.Helper()
	srv := httptest.NewServer(a.Handler())
	t.Cleanup(srv.Close)
	resp, err := http.Get(srv.URL + configPath) //nolint:gosec,noctx // test-only
	if err != nil {
		t.Fatalf("GET handler: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 from handler, got %d", resp.StatusCode)
	}
	var env struct {
		HTTP struct {
			Routers map[string]json.RawMessage `json:"routers"`
		} `json:"http"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&env); err != nil {
		t.Fatalf("decode handler response: %v", err)
	}
	keys := make(map[string]bool, len(env.HTTP.Routers))
	for k := range env.HTTP.Routers {
		keys[k] = true
	}
	return keys
}

// ---- In-call fetch retries --------------------------------------------------

// A downstream that fails once then succeeds within the retry budget has its
// routes contributed in the same Refresh cycle that started the retries.
func TestRefresh_Retry_FailsOnceThenSucceeds(t *testing.T) {
	clock := &fakeClock{}
	ds := newFakeDS(t)
	healthy := healthDS(t)

	cfg := resilienceConfig(ds.url(), healthy.URL, 0)
	a := resilienceApp(t, cfg, clock) // success at T0

	ds.failNext(1) // fail exactly one request, then recover mid-cycle
	if err := a.Refresh(context.Background()); err != nil {
		t.Fatalf("cycle 2 Refresh: %v", err)
	}

	// First attempt fails, the retried attempt succeeds (plus the initial
	// successful cycle's one hit).
	if got := ds.hits.Load(); got != 3 {
		t.Errorf("a single transient failure must be retried in the same cycle: hits = %d, want 3", got)
	}
	keys := servedRouters(t, a)
	if !keys["primary-app-a"] {
		t.Errorf("recovered routes must be contributed in the same Refresh cycle; got %v", keys)
	}
}

// A downstream that fails on every attempt exhausts the retry budget and
// falls back to last-known-good data for that cycle.
func TestRefresh_Retry_ExhaustsAttemptsThenFallsBack(t *testing.T) {
	clock := &fakeClock{}
	ds := newFakeDS(t)
	healthy := healthDS(t)

	cfg := resilienceConfig(ds.url(), healthy.URL, 0)
	a := resilienceApp(t, cfg, clock) // success at T0: 1 hit

	ds.mode.Store(modeFail)
	if err := a.Refresh(context.Background()); err != nil {
		t.Fatalf("cycle 2 Refresh: %v", err)
	}

	// 3 fetch attempts (fetchRetryMaxAttempts), all failing.
	if got := ds.hits.Load(); got != 4 {
		t.Errorf("a permanently-failing downstream must be attempted exactly 3 times per cycle: hits = %d, want 4", got)
	}
	keys := servedRouters(t, a)
	if !keys["primary-app-a"] {
		t.Errorf("exhausted retries must fall back to last-known-good routes; got %v", keys)
	}
}

// The delay before each retry grows exponentially and stays within the cap.
// The sequence is observed through the injected sleep hook, no wall-clock
// sleeping in tests.
func TestRefresh_Retry_DelayGrowsExponentiallyAndStaysCapped(t *testing.T) {
	clock := &fakeClock{}
	ds := newFakeDS(t)
	healthy := healthDS(t)

	var sleeps []time.Duration
	record := func(d time.Duration) { sleeps = append(sleeps, d) }

	cfg := resilienceConfig(ds.url(), healthy.URL, 0)
	a := resilienceApp(t, cfg, clock, record) // success at T0

	ds.mode.Store(modeFail)
	if err := a.Refresh(context.Background()); err != nil {
		t.Fatalf("cycle 2 Refresh: %v", err)
	}

	// One sleep between each pair of the 3 attempts: base 500ms doubled to
	// 1s, both within the 2s cap.
	want := []time.Duration{500 * time.Millisecond, 1 * time.Second}
	if len(sleeps) != len(want) {
		t.Fatalf("sleep calls = %v, want %v", sleeps, want)
	}
	for i, d := range want {
		if sleeps[i] != d {
			t.Errorf("sleep[%d] = %v, want %v", i, sleeps[i], d)
		}
		if sleeps[i] > 2*time.Second {
			t.Errorf("sleep[%d] = %v exceeds the 2s cap", i, sleeps[i])
		}
	}
}

// A cancelled context is not blocked by retry sleeps: after the first failed
// attempt the refresh bails out immediately instead of sleeping and retrying.
func TestRefresh_Retry_CancelledContextBailsOutEarly(t *testing.T) {
	clock := &fakeClock{}
	ds := newFakeDS(t)
	healthy := healthDS(t)

	var sleeps []time.Duration
	record := func(d time.Duration) { sleeps = append(sleeps, d) }

	cfg := resilienceConfig(ds.url(), healthy.URL, 0)
	a := resilienceApp(t, cfg, clock, record) // success at T0

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	ds.mode.Store(modeFail)
	if err := a.Refresh(ctx); err != nil {
		t.Fatalf("cancelled-context Refresh must still fall back, not fail: %v", err)
	}

	// Only the first attempt runs: no retry sleep and no further attempt
	// happen once the context is done.
	if len(sleeps) != 0 {
		t.Errorf("cancelled context must bail out before the first retry sleep: sleeps = %v, want none", sleeps)
	}
	keys := servedRouters(t, a)
	if !keys["primary-app-a"] {
		t.Errorf("cancelled-context failure must still fall back to last-known-good; got %v", keys)
	}
}

// ---- Staleness --------------------------------------------------------------

// With a staleness limit configured and exceeded, a persistently-dead
// downstream's routes are withdrawn from the merged output; a healthy
// downstream is unaffected.
func TestRefresh_StalenessLimit_WithdrawsStaleRoutes(t *testing.T) {
	clock := &fakeClock{}
	ds := newFakeDS(t)
	healthy := healthDS(t)

	cfg := resilienceConfig(ds.url(), healthy.URL, 10*time.Second)
	a := resilienceApp(t, cfg, clock) // success at T0

	// Persistent failure from T0 on; every cycle exhausts the retry budget.
	ds.mode.Store(modeFail)
	if err := a.Refresh(context.Background()); err != nil { // failure at T0
		t.Fatalf("failure 1 Refresh: %v", err)
	}
	clock.Advance(5 * time.Second)
	if err := a.Refresh(context.Background()); err != nil { // failure at T0+5s
		t.Fatalf("failure 2 Refresh: %v", err)
	}

	// 5s < 10s limit: last-good routes still served.
	keys := servedRouters(t, a)
	if !keys["primary-app-a"] {
		t.Errorf("routes must be retained while within the staleness limit; got %v", keys)
	}

	clock.Advance(6 * time.Second) // T0+11s, beyond the 10s limit
	if err := a.Refresh(context.Background()); err != nil {
		t.Fatalf("stale Refresh: %v", err)
	}

	keys = servedRouters(t, a)
	if keys["primary-app-a"] {
		t.Errorf("stale routes must be withdrawn after the staleness limit; got %v", keys)
	}
	if !keys["gpu-app-b"] {
		t.Errorf("healthy downstream's routes must be unaffected by withdrawal; got %v", keys)
	}
}

// With no staleness limit (the default), routes are retained indefinitely.
func TestRefresh_StalenessDisabled_RetainsIndefinitely(t *testing.T) {
	clock := &fakeClock{}
	ds := newFakeDS(t)
	healthy := healthDS(t)

	cfg := resilienceConfig(ds.url(), healthy.URL, 0) // 0 = unlimited
	a := resilienceApp(t, cfg, clock)                 // success at T0

	ds.mode.Store(modeFail)
	if err := a.Refresh(context.Background()); err != nil {
		t.Fatalf("failure 1 Refresh: %v", err)
	}

	clock.Advance(24 * time.Hour)
	if err := a.Refresh(context.Background()); err != nil {
		t.Fatalf("far-future Refresh: %v", err)
	}

	keys := servedRouters(t, a)
	if !keys["primary-app-a"] {
		t.Errorf("with staleness disabled, last-good routes must be retained indefinitely; got %v", keys)
	}
}

// A downstream that recovers after being withdrawn has its routes reappear on
// the next successful Refresh, no restart required.
func TestRefresh_Staleness_RecoveryAfterWithdrawal(t *testing.T) {
	clock := &fakeClock{}
	ds := newFakeDS(t)
	healthy := healthDS(t)

	cfg := resilienceConfig(ds.url(), healthy.URL, 10*time.Second)
	a := resilienceApp(t, cfg, clock) // success at T0

	// Fail long enough to exceed the staleness limit and be withdrawn.
	ds.mode.Store(modeFail)
	for _, step := range []time.Duration{0, 5 * time.Second, 6 * time.Second} {
		if step > 0 {
			clock.Advance(step)
		}
		if err := a.Refresh(context.Background()); err != nil {
			t.Fatalf("failure Refresh: %v", err)
		}
	}
	if keys := servedRouters(t, a); keys["primary-app-a"] {
		t.Fatalf("routes should have been withdrawn before recovery; got %v", keys)
	}

	// Recover: the next poll succeeds and routes reappear.
	clock.Advance(5 * time.Second) // T0+16s
	ds.mode.Store(modeOK)
	if err := a.Refresh(context.Background()); err != nil {
		t.Fatalf("recovery Refresh: %v", err)
	}

	keys := servedRouters(t, a)
	if !keys["primary-app-a"] {
		t.Errorf("routes must reappear after recovery without a restart; got %v", keys)
	}
}

// A staleness withdrawal must publish even when the withdrawn downstream was
// the only contributor: the merged output drops its routes instead of
// silently retaining the previous snapshot.
func TestRefresh_StalenessLimit_SingleDownstream_WithdrawalPublished(t *testing.T) {
	clock := &fakeClock{}
	ds := newFakeDS(t)

	cfg := config.Config{
		ConfigPath:      configPath,
		RequestTimeout:  5 * time.Second,
		MaxResponseSize: 10 * 1024 * 1024,
		EdgeEntrypoints: []string{valueWeb},
		Downstreams: []config.Downstream{
			{Name: "primary", APIAddress: ds.url(), TrafficAddress: "http://primary:80", AllowedEntrypoints: []string{valueWeb}, StalenessLimit: 5 * time.Second},
		},
	}
	a := resilienceApp(t, cfg, clock) // success at T0

	ds.mode.Store(modeFail)
	if err := a.Refresh(context.Background()); err != nil { // failure at T0
		t.Fatalf("failure 1 Refresh: %v", err)
	}
	clock.Advance(6 * time.Second)
	if err := a.Refresh(context.Background()); err != nil { // failure at T0+6s, past the 5s limit
		t.Fatalf("stale Refresh: %v", err)
	}

	keys := servedRouters(t, a)
	if keys["primary-app-a"] {
		t.Errorf("withdrawn downstream's routes must drop from the merged output even as sole contributor; got %v", keys)
	}
}

// ---- Staleness WARN throttling ---------------------------------------------

// The staleness-withdrawal WARN fires only on the transition into withdrawal:
// a downstream that stays stale across multiple cycles logs exactly one
// staleness WARN, not one per cycle.  A recovery re-arms the transition, so
// going stale again warns once more.
func TestRefresh_Staleness_WarnsOnlyOnTransition(t *testing.T) {
	clock := &fakeClock{}
	ds := newFakeDS(t)
	healthy := healthDS(t)

	var logBuf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logBuf, nil))

	cfg := resilienceConfig(ds.url(), healthy.URL, 3*time.Second)
	a, err := app.New(cfg,
		app.WithClock(clock.Now),
		app.WithSleep(func(time.Duration) {}),
		app.WithLogger(logger),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := a.Refresh(context.Background()); err != nil {
		t.Fatalf("initial Refresh: %v", err)
	}

	staleCount := func() int { return strings.Count(logBuf.String(), "downstream stale, withdrawing routes") }

	// Fail through the staleness limit: the withdrawal WARN fires once.
	ds.mode.Store(modeFail)
	for _, step := range []time.Duration{0, 2 * time.Second, 2 * time.Second} {
		if step > 0 {
			clock.Advance(step)
		}
		if err := a.Refresh(context.Background()); err != nil {
			t.Fatalf("stale Refresh: %v", err)
		}
	}
	if got := staleCount(); got != 1 {
		t.Fatalf("after going stale, staleness WARN count = %d, want 1 (log: %s)", got, logBuf.String())
	}

	// Three more stale cycles: still exactly one WARN, no per-cycle spam.
	for i := range 3 {
		clock.Advance(2 * time.Second)
		if err := a.Refresh(context.Background()); err != nil {
			t.Fatalf("still-stale Refresh %d: %v", i+1, err)
		}
	}
	if got := staleCount(); got != 1 {
		t.Errorf("remaining stale must not re-warn: staleness WARN count = %d, want 1", got)
	}

	// Recover, then go stale again: the transition re-arms and warns once.
	clock.Advance(2 * time.Second)
	ds.mode.Store(modeOK)
	if err := a.Refresh(context.Background()); err != nil {
		t.Fatalf("recovery Refresh: %v", err)
	}
	if got := staleCount(); got != 1 {
		t.Errorf("recovery must not emit a staleness WARN: count = %d, want 1", got)
	}

	ds.mode.Store(modeFail)
	clock.Advance(4 * time.Second) // past the 3s limit again
	if err := a.Refresh(context.Background()); err != nil {
		t.Fatalf("second staleness Refresh: %v", err)
	}
	if got := staleCount(); got != 2 {
		t.Errorf("going stale a second time must warn exactly once more: count = %d, want 2", got)
	}
}
