//nolint:testpackage // needs the unexported withClock Option to inject a fake clock; no clock knob in the public API
package app

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gringolito/traefik-scout/internal/config"
)

// Issue #6 tests that need to control the passage of time live in the internal
// test package so they can inject a fake clock through the unexported
// withClock Option without exposing a clock knob in the public API.

// test-local copies of fixture constants defined in the external test package.
const (
	rawdataPath      = "/api/rawdata"
	jsonFieldRouters = "routers"
	testWeb          = "web"
)

// fakeClock is a manually-advanced clock for deterministic staleness/backoff
// tests.
type fakeClock struct {
	now atomic.Int64 // unix nanoseconds
}

func (c *fakeClock) Now() time.Time { return time.Unix(0, c.now.Load()) }
func (c *fakeClock) Advance(d time.Duration) {
	c.now.Add(int64(d))
}

// fakeDS is a downstream whose /api/rawdata response is switched between a
// valid routers payload (modeOK) and HTTP 500 (modeFail), and which counts
// every rawdata request.
type fakeDS struct {
	srv  *httptest.Server
	hits atomic.Int32
	mode atomic.Int32 // modeOK or modeFail
}

const (
	modeOK   = 0
	modeFail = 1
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
		if ds.mode.Load() == modeFail {
			http.Error(w, "upstream error", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			jsonFieldRouters: map[string]any{
				"app-a@docker": map[string]any{
					"entryPoints": []string{testWeb},
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
					"entryPoints": []string{testWeb},
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
		ConfigPath:      "/config",
		RequestTimeout:  5 * time.Second,
		MaxResponseSize: 10 * 1024 * 1024,
		EdgeEntrypoints: []string{testWeb},
		Downstreams: []config.Downstream{
			{Name: "primary", APIAddress: primaryURL, TrafficAddress: "http://primary:80", AllowedEntrypoints: []string{testWeb}, StalenessLimit: staleness},
			{Name: "gpu", APIAddress: healthyURL, TrafficAddress: "http://gpu:80", AllowedEntrypoints: []string{testWeb}},
		},
	}
}

// resilienceApp builds an App with the fake clock injected and runs one
// Refresh, failing the test on error.
func resilienceApp(t *testing.T, cfg config.Config, clock *fakeClock) *App {
	t.Helper()
	a, err := New(cfg, withClock(clock.Now))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := a.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	return a
}

// servedRouters queries the handler and returns the served router keys.
func servedRouters(t *testing.T, a *App) map[string]bool {
	t.Helper()
	srv := httptest.NewServer(a.Handler())
	t.Cleanup(srv.Close)
	resp, err := http.Get(srv.URL + "/config") //nolint:gosec,noctx // test-only
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

// ---- Backoff ----------------------------------------------------------------

// After a failure, the downstream is not re-polled until the backoff window
// has elapsed; last-known-good routes keep being served in the meantime.
func TestRefresh_Backoff_SkipsFetchWithinWindow(t *testing.T) {
	clock := &fakeClock{}
	ds := newFakeDS(t)
	healthy := healthDS(t)

	cfg := resilienceConfig(ds.url(), healthy.URL, 0)
	a := resilienceApp(t, cfg, clock) // cycle 1 at T0: success, 1 hit

	ds.mode.Store(modeFail)
	if err := a.Refresh(context.Background()); err != nil {
		t.Fatalf("cycle 2 Refresh: %v", err)
	}
	if got := ds.hits.Load(); got != 2 {
		t.Fatalf("after failing cycle: hits = %d, want 2", got)
	}

	// Half the initial window later: the poll must be skipped entirely.
	clock.Advance(500 * time.Millisecond)
	if err := a.Refresh(context.Background()); err != nil {
		t.Fatalf("cycle 3 Refresh: %v", err)
	}
	if got := ds.hits.Load(); got != 2 {
		t.Errorf("refresh inside backoff window must skip the fetch: hits = %d, want 2", got)
	}

	// Last-known-good routes must still be served while backing off.
	keys := servedRouters(t, a)
	if !keys["primary-app-a"] {
		t.Errorf("last-good routes must be served during backoff; got %v", keys)
	}

	// Once the window elapses, the poll is retried.
	clock.Advance(500 * time.Millisecond) // T0+1s
	if err := a.Refresh(context.Background()); err != nil {
		t.Fatalf("cycle 4 Refresh: %v", err)
	}
	if got := ds.hits.Load(); got != 3 {
		t.Errorf("refresh after backoff window must retry the fetch: hits = %d, want 3", got)
	}
}

// Consecutive failures double the backoff window.
func TestRefresh_Backoff_ExponentialGrowth(t *testing.T) {
	clock := &fakeClock{}
	ds := newFakeDS(t)
	healthy := healthDS(t)

	cfg := resilienceConfig(ds.url(), healthy.URL, 0)
	a := resilienceApp(t, cfg, clock) // success at T0

	ds.mode.Store(modeFail)
	if err := a.Refresh(context.Background()); err != nil { // failure 1 at T0: window 1s
		t.Fatalf("failure 1 Refresh: %v", err)
	}
	clock.Advance(1 * time.Second)
	if err := a.Refresh(context.Background()); err != nil { // failure 2 at T0+1s: window 2s
		t.Fatalf("failure 2 Refresh: %v", err)
	}
	if got := ds.hits.Load(); got != 3 {
		t.Fatalf("hits = %d, want 3", got)
	}

	// At T0+2.9s (before the doubled window ends at T0+3s): skipped.
	clock.Advance(1900 * time.Millisecond)
	if err := a.Refresh(context.Background()); err != nil {
		t.Fatalf("skipped-cycle Refresh: %v", err)
	}
	if got := ds.hits.Load(); got != 3 {
		t.Errorf("second consecutive failure must double the window: hits = %d, want 3", got)
	}

	// At T0+3s: retried.
	clock.Advance(100 * time.Millisecond)
	if err := a.Refresh(context.Background()); err != nil {
		t.Fatalf("retry Refresh: %v", err)
	}
	if got := ds.hits.Load(); got != 4 {
		t.Errorf("doubled window must end after 2s: hits = %d, want 4", got)
	}
}

// The backoff window is capped; retries keep happening at least every cap
// period no matter how many consecutive failures accumulate.
func TestRefresh_Backoff_CappedAtMax(t *testing.T) {
	clock := &fakeClock{}
	ds := newFakeDS(t)
	healthy := healthDS(t)

	cfg := resilienceConfig(ds.url(), healthy.URL, 0)
	a := resilienceApp(t, cfg, clock) // success at T0

	ds.mode.Store(modeFail)
	// 12 consecutive failures, advancing past the cap each cycle.  Uncapped
	// doubling would exceed any 5-minute advance after ~9 failures.
	const failures = 12
	for i := range failures {
		if err := a.Refresh(context.Background()); err != nil {
			t.Fatalf("failure %d Refresh: %v", i+1, err)
		}
		clock.Advance(5 * time.Minute)
		if got := ds.hits.Load(); got != int32(2+i) { // success + i+1 failures
			t.Fatalf("failure %d: hits = %d, want %d (backoff window must be capped)", i+1, got, 2+i)
		}
	}
}

// A success resets the consecutive-failure count, so the next failure starts
// again from the base window.
func TestRefresh_Backoff_ResetOnSuccess(t *testing.T) {
	clock := &fakeClock{}
	ds := newFakeDS(t)
	healthy := healthDS(t)

	cfg := resilienceConfig(ds.url(), healthy.URL, 0)
	a := resilienceApp(t, cfg, clock) // success at T0

	// Failure 1 at T0 → window 1s.
	ds.mode.Store(modeFail)
	if err := a.Refresh(context.Background()); err != nil {
		t.Fatalf("failure 1 Refresh: %v", err)
	}
	// Recovery at T0+1s → resets backoff.
	clock.Advance(1 * time.Second)
	ds.mode.Store(modeOK)
	if err := a.Refresh(context.Background()); err != nil {
		t.Fatalf("recovery Refresh: %v", err)
	}
	// Failure again at T0+1s → window must be 1s (reset), due at T0+2s.
	ds.mode.Store(modeFail)
	if err := a.Refresh(context.Background()); err != nil {
		t.Fatalf("failure 2 Refresh: %v", err)
	}
	hitsAfterFailure := ds.hits.Load()

	// At T0+1.5s: still inside the reset 1s window → skipped.
	clock.Advance(500 * time.Millisecond)
	if err := a.Refresh(context.Background()); err != nil {
		t.Fatalf("skipped-cycle Refresh: %v", err)
	}
	if got := ds.hits.Load(); got != hitsAfterFailure {
		t.Errorf("backoff must reset to base after success: hits = %d, want %d", got, hitsAfterFailure)
	}

	// At T0+2s: retried.
	clock.Advance(500 * time.Millisecond)
	if err := a.Refresh(context.Background()); err != nil {
		t.Fatalf("retry Refresh: %v", err)
	}
	if got := ds.hits.Load(); got != hitsAfterFailure+1 {
		t.Errorf("reset window must end 1s after failure: hits = %d, want %d", got, hitsAfterFailure+1)
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

	// Persistent failure from T0 on.
	ds.mode.Store(modeFail)
	if err := a.Refresh(context.Background()); err != nil { // failure 1 at T0
		t.Fatalf("failure 1 Refresh: %v", err)
	}
	clock.Advance(5 * time.Second)
	if err := a.Refresh(context.Background()); err != nil { // failure 2 at T0+5s
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

	// Recover: the next due poll succeeds and routes reappear.
	clock.Advance(5 * time.Second) // T0+16s, past the T0+11s backoff window
	ds.mode.Store(modeOK)
	if err := a.Refresh(context.Background()); err != nil {
		t.Fatalf("recovery Refresh: %v", err)
	}

	keys := servedRouters(t, a)
	if !keys["primary-app-a"] {
		t.Errorf("routes must reappear after recovery without a restart; got %v", keys)
	}
}

// A downstream deferred by backoff is still subject to staleness: if its
// last-success timestamp ages past the limit while the retry window is still
// open, its routes are withdrawn without another fetch attempt.
func TestRefresh_Staleness_EnforcedDuringBackoffSkip(t *testing.T) {
	clock := &fakeClock{}
	ds := newFakeDS(t)
	healthy := healthDS(t)

	cfg := resilienceConfig(ds.url(), healthy.URL, 3*time.Second)
	a := resilienceApp(t, cfg, clock) // success at T0, lastSuccess=T0

	ds.mode.Store(modeFail)
	if err := a.Refresh(context.Background()); err != nil { // failure 1 at T0, window 1s
		t.Fatalf("failure 1 Refresh: %v", err)
	}
	clock.Advance(500 * time.Millisecond)
	if err := a.Refresh(context.Background()); err != nil { // skipped at T0+0.5s, not stale yet
		t.Fatalf("early skip Refresh: %v", err)
	}
	clock.Advance(500 * time.Millisecond)
	if err := a.Refresh(context.Background()); err != nil { // failure 2 at T0+1s, window 2s (due T0+3s)
		t.Fatalf("failure 2 Refresh: %v", err)
	}
	clock.Advance(2 * time.Second)
	if err := a.Refresh(context.Background()); err != nil { // failure 3 at T0+3s, window 4s (due T0+7s)
		t.Fatalf("failure 3 Refresh: %v", err)
	}
	hitsBefore := ds.hits.Load()

	// T0+4.5s: inside the 4s backoff window AND 4.5s > 3s staleness limit.
	// No fetch may happen, yet the routes must be withdrawn.
	clock.Advance(1500 * time.Millisecond)
	if err := a.Refresh(context.Background()); err != nil {
		t.Fatalf("deferred stale Refresh: %v", err)
	}
	if got := ds.hits.Load(); got != hitsBefore {
		t.Errorf("staleness check during backoff must not trigger a fetch: hits = %d, want %d", got, hitsBefore)
	}

	keys := servedRouters(t, a)
	if keys["primary-app-a"] {
		t.Errorf("stale routes must be withdrawn even while backing off; got %v", keys)
	}
	if !keys["gpu-app-b"] {
		t.Errorf("healthy downstream's routes must be unaffected; got %v", keys)
	}
}

// A staleness withdrawal must publish even when the withdrawn downstream was
// the only contributor: the merged output drops its routes instead of
// silently retaining the previous snapshot.
func TestRefresh_StalenessLimit_SingleDownstream_WithdrawalPublished(t *testing.T) {
	clock := &fakeClock{}
	ds := newFakeDS(t)

	cfg := config.Config{
		ConfigPath:      "/config",
		RequestTimeout:  5 * time.Second,
		MaxResponseSize: 10 * 1024 * 1024,
		EdgeEntrypoints: []string{testWeb},
		Downstreams: []config.Downstream{
			{Name: "primary", APIAddress: ds.url(), TrafficAddress: "http://primary:80", AllowedEntrypoints: []string{testWeb}, StalenessLimit: 5 * time.Second},
		},
	}
	a := resilienceApp(t, cfg, clock) // success at T0

	ds.mode.Store(modeFail)
	if err := a.Refresh(context.Background()); err != nil { // failure 1 at T0
		t.Fatalf("failure 1 Refresh: %v", err)
	}
	clock.Advance(6 * time.Second)
	if err := a.Refresh(context.Background()); err != nil { // failure 2 at T0+6s, past the 5s limit
		t.Fatalf("stale Refresh: %v", err)
	}

	keys := servedRouters(t, a)
	if keys["primary-app-a"] {
		t.Errorf("withdrawn downstream's routes must drop from the merged output even as sole contributor; got %v", keys)
	}
}
