package app_test

import (
	"bytes"
	"context"
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

// queryEndpoint issues GET path against h and returns the recorder without
// decoding the body, for endpoint tests that assert status codes or raw text.
func queryEndpoint(t *testing.T, h http.Handler, path string) *httptest.ResponseRecorder {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, path, nil)
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

// ---- Issue #7, Cycle 1 ------------------------------------------------------

// /healthz returns 200 whenever the process is serving, independent of
// downstream health: it must respond 200 before any Refresh has run at all.
func TestHandler_Healthz_AlwaysOK(t *testing.T) {
	cfg := testConfig("http://unreachable.invalid", "http://traffic.invalid")
	a, err := app.New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	rec := queryEndpoint(t, a.Handler(), "/healthz")
	if rec.Code != http.StatusOK {
		t.Errorf("/healthz status = %d, want 200 (no Refresh needed)", rec.Code)
	}
}

// /healthz must also be 200 when every downstream has failed, i.e. after
// Refresh cycles that never succeeded.
func TestHandler_Healthz_WithAllDownstreamsDown(t *testing.T) {
	cfg := testConfig("http://unreachable.invalid", "http://traffic.invalid")
	a, err := app.New(cfg, app.WithSleep(func(time.Duration) {}))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := a.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh: %v", err)
	}

	rec := queryEndpoint(t, a.Handler(), "/healthz")
	if rec.Code != http.StatusOK {
		t.Errorf("/healthz status = %d, want 200 even when every downstream is unreachable", rec.Code)
	}
}

// ---- Issue #7, Cycle 2 ------------------------------------------------------

// /readyz returns non-200 before any downstream has ever been polled
// successfully, including after Refresh cycles that all failed.
func TestHandler_Readyz_NotReadyBeforeFirstSuccess(t *testing.T) {
	cfg := testConfig("http://unreachable.invalid", "http://traffic.invalid")
	a, err := app.New(cfg, app.WithSleep(func(time.Duration) {}))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := a.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh: %v", err)
	}

	rec := queryEndpoint(t, a.Handler(), "/readyz")
	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("/readyz status = %d before any successful poll, want 503", rec.Code)
	}
}

// /readyz returns 200 once at least one downstream has ever been polled
// successfully. Readiness is cumulative: later failures do not un-ready
// the process.
func TestHandler_Readyz_ReadyAfterFirstSuccess(t *testing.T) {
	// First cycle succeeds, second returns 500: readiness must persist.
	ds := sequencedDownstream(t, []map[string]any{
		{valueMyRouter: map[string]any{fieldEntryPoints: []string{valueWeb}, fieldRule: valueExampleRule, fieldStatus: valueEnabled, fieldProvider: valueDocker}},
		nil,
	})
	defer ds.Close()

	cfg := testConfig(ds.URL, "http://traffic.invalid")
	a, err := app.New(cfg, app.WithSleep(func(time.Duration) {}))
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	if rec := queryEndpoint(t, a.Handler(), "/readyz"); rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("/readyz status = %d before any Refresh, want 503", rec.Code)
	}
	if err := a.Refresh(context.Background()); err != nil {
		t.Fatalf("first Refresh: %v", err)
	}
	if rec := queryEndpoint(t, a.Handler(), "/readyz"); rec.Code != http.StatusOK {
		t.Fatalf("/readyz status = %d after first successful poll, want 200", rec.Code)
	}

	// Second cycle fails everywhere; readiness is cumulative, not current.
	if err := a.Refresh(context.Background()); err != nil {
		t.Fatalf("second Refresh: %v", err)
	}
	if rec := queryEndpoint(t, a.Handler(), "/readyz"); rec.Code != http.StatusOK {
		t.Errorf("/readyz status = %d after later failed poll, want 200 (readiness is cumulative)", rec.Code)
	}
}

// ---- Issue #7, Cycle 3 ------------------------------------------------------

// metricValue scrapes /metrics and returns the value of the line whose metric
// name and label set match exactly (Prometheus text exposition format).
func metricValue(t *testing.T, a *app.App, name string) string {
	t.Helper()
	rec := queryEndpoint(t, a.Handler(), "/metrics")
	if rec.Code != http.StatusOK {
		t.Fatalf("/metrics status = %d, want 200", rec.Code)
	}
	for line := range strings.SplitSeq(rec.Body.String(), "\n") {
		if strings.HasPrefix(line, name+" ") {
			fields := strings.Fields(line)
			if len(fields) != 2 {
				t.Fatalf("malformed metric line %q", line)
			}
			return fields[1]
		}
	}
	t.Fatalf("metric %q not found in /metrics output:\n%s", name, rec.Body.String())
	return ""
}

// After one successful Refresh of a one-router downstream, /metrics exposes
// every per-downstream and global observability metric with correct values.
func TestHandler_Metrics_AfterFirstRefresh(t *testing.T) {
	ds := fakeDownstream(map[string]any{
		valueMyRouter: map[string]any{fieldEntryPoints: []string{valueWeb}, fieldRule: valueExampleRule, fieldStatus: valueEnabled, fieldProvider: valueDocker},
	})
	defer ds.Close()

	cfg := testConfig(ds.URL, "http://traffic.invalid")
	a, err := app.New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := a.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh: %v", err)
	}

	// The test never tells the App what time it is, so the last-success gauge
	// just has to be a plausible recent unix timestamp.
	if got := metricValue(t, a, `scout_downstream_last_success_timestamp_seconds{name="primary"}`); got == "" {
		t.Fatal("missing last-success gauge")
	}
	if got := metricValue(t, a, `scout_downstream_poll_attempts_total{name="primary"}`); got != "1" {
		t.Errorf("poll attempts = %q, want 1 after one successful poll", got)
	}
	if got := metricValue(t, a, `scout_downstream_poll_failures_total{name="primary"}`); got != "0" {
		t.Errorf("poll failures = %q, want 0", got)
	}
	if got := metricValue(t, a, `scout_downstream_poll_duration_seconds_count{name="primary"}`); got != "1" {
		t.Errorf("poll duration observations = %q, want 1", got)
	}
	if got := metricValue(t, a, "scout_routers"); got != "1" {
		t.Errorf("router gauge = %q, want 1", got)
	}
	if got := metricValue(t, a, "scout_merged_output_generation_total"); got != "1" {
		t.Errorf("generation counter = %q, want 1 after first changed output", got)
	}
}

// ---- Issue #7, Cycle 4 ------------------------------------------------------

// switchableDS is a downstream whose routers payload can be swapped between
// Refresh cycles to drive merged-output changes.
type switchableDS struct {
	srv     *httptest.Server
	payload atomic.Pointer[map[string]any]
}

func newSwitchableDS(t *testing.T, initial map[string]any) *switchableDS {
	t.Helper()
	ds := &switchableDS{}
	ds.payload.Store(&initial)
	ds.srv = httptest.NewServer(rawdataHandler(func(w http.ResponseWriter, _ *http.Request) {
		serveRawdata(w, *ds.payload.Load())
	}))
	t.Cleanup(ds.srv.Close)
	return ds
}

func (ds *switchableDS) set(p map[string]any) { ds.payload.Store(&p) }

// The generation counter increments only when the merged output actually
// changes across Refresh cycles: unchanged output republishes the same
// generation, a real change bumps it by exactly one, and the router gauge
// tracks the latest snapshot.
func TestHandler_Metrics_GenerationOnlyOnOutputChange(t *testing.T) {
	oneRouter := map[string]any{
		valueMyRouter: map[string]any{fieldEntryPoints: []string{valueWeb}, fieldRule: valueExampleRule, fieldStatus: valueEnabled, fieldProvider: valueDocker},
	}
	twoRouters := map[string]any{
		valueMyRouter:  map[string]any{fieldEntryPoints: []string{valueWeb}, fieldRule: valueExampleRule, fieldStatus: valueEnabled, fieldProvider: valueDocker},
		"other@docker": map[string]any{fieldEntryPoints: []string{valueWeb}, fieldRule: valueHostA, fieldStatus: valueEnabled, fieldProvider: valueDocker},
	}

	ds := newSwitchableDS(t, oneRouter)
	cfg := testConfig(ds.srv.URL, "http://traffic.invalid")
	a, err := app.New(cfg, app.WithSleep(func(time.Duration) {}))
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	if err := a.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh 1: %v", err)
	}
	if got := metricValue(t, a, "scout_merged_output_generation_total"); got != "1" {
		t.Fatalf("after first publish, generation = %q, want 1", got)
	}

	// Cycle 2 over identical downstream data: no output change, no bump.
	if err := a.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh 2: %v", err)
	}
	if got := metricValue(t, a, "scout_merged_output_generation_total"); got != "1" {
		t.Errorf("unchanged output must not bump generation; got %q, want 1", got)
	}

	// Cycle 3 with changed downstream data: exactly one more generation.
	ds.set(twoRouters)
	if err := a.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh 3: %v", err)
	}
	if got := metricValue(t, a, "scout_merged_output_generation_total"); got != "2" {
		t.Errorf("changed output must bump generation by one; got %q, want 2", got)
	}
	if got := metricValue(t, a, "scout_routers"); got != "2" {
		t.Errorf("router gauge = %q, want 2 after output change", got)
	}

	// Every poll attempt is counted even when output never changes.
	if got := metricValue(t, a, `scout_downstream_poll_attempts_total{name="primary"}`); got != "3" {
		t.Errorf("poll attempts = %q, want 3 after three cycles", got)
	}
	if got := metricValue(t, a, `scout_downstream_poll_failures_total{name="primary"}`); got != "0" {
		t.Errorf("poll failures = %q, want 0", got)
	}
}

// ---- Issue #7, Cycle 5 ------------------------------------------------------

// An INFO log line appears exactly when the merged output changes across two
// Refresh cycles, and not when the output is unchanged.
func TestRefresh_LogsInfoExactlyOnOutputChange(t *testing.T) {
	oneRouter := map[string]any{
		valueMyRouter: map[string]any{fieldEntryPoints: []string{valueWeb}, fieldRule: valueExampleRule, fieldStatus: valueEnabled, fieldProvider: valueDocker},
	}
	twoRouters := map[string]any{
		valueMyRouter:  map[string]any{fieldEntryPoints: []string{valueWeb}, fieldRule: valueExampleRule, fieldStatus: valueEnabled, fieldProvider: valueDocker},
		"other@docker": map[string]any{fieldEntryPoints: []string{valueWeb}, fieldRule: valueHostA, fieldStatus: valueEnabled, fieldProvider: valueDocker},
	}

	ds := newSwitchableDS(t, oneRouter)
	cfg := testConfig(ds.srv.URL, "http://traffic.invalid")

	var logBuf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelInfo}))
	a, err := app.New(cfg, app.WithLogger(logger), app.WithSleep(func(time.Duration) {}))
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	count := func() int { return strings.Count(logBuf.String(), "merged configuration changed") }

	if err := a.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh 1: %v", err)
	}
	if got := count(); got != 1 {
		t.Fatalf("first publish must log exactly one INFO change line; got %d (log: %s)", got, logBuf.String())
	}

	if err := a.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh 2: %v", err)
	}
	if got := count(); got != 1 {
		t.Errorf("unchanged output must not log a change line; got %d total (log: %s)", got, logBuf.String())
	}

	ds.set(twoRouters)
	if err := a.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh 3: %v", err)
	}
	if got := count(); got != 2 {
		t.Errorf("changed output must log exactly one more change line; got %d total (log: %s)", got, logBuf.String())
	}
}

// ---- Issue #7, Cycle 6 ------------------------------------------------------

// Each Refresh cycle emits one DEBUG line per downstream regardless of poll
// outcome, so operators can trace per-downstream activity at debug level.
func TestRefresh_LogsDebugPerDownstreamPerCycle(t *testing.T) {
	healthy := healthDS(t)
	defer healthy.Close()
	dead := newFakeDS(t)
	dead.mode.Store(modeFail)

	cfg := multiConfig([]config.Downstream{
		{Name: valueDownstreamHostA, APIAddress: healthy.URL, TrafficAddress: valueTrafficHostA, AllowedEntrypoints: []string{valueWeb}},
		{Name: valueDownstreamHostB, APIAddress: dead.url(), TrafficAddress: valueTrafficHostB, AllowedEntrypoints: []string{valueWeb}},
	})

	var logBuf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	a, err := app.New(cfg, app.WithLogger(logger), app.WithSleep(func(time.Duration) {}))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := a.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh: %v", err)
	}

	logOut := logBuf.String()
	for _, name := range []string{valueDownstreamHostA, valueDownstreamHostB} {
		lines := 0
		for l := range strings.SplitSeq(logOut, "\n") {
			if strings.Contains(l, "downstream polled") && strings.Contains(l, `downstream=`+name) {
				lines++
			}
		}
		if lines != 1 {
			t.Errorf("expected exactly one DEBUG 'downstream polled' line for %s, got %d (log:\n%s)", name, lines, logOut)
		}
	}
}

// ---- Issue #7, Cycle 7 ------------------------------------------------------

// A poll failure produces a warning-level log line naming the downstream and
// the retry budget, without failing Refresh itself.
func TestRefresh_PollFailure_WarnLog(t *testing.T) {
	ds := newFakeDS(t)
	ds.mode.Store(modeFail)

	cfg := testConfig(ds.url(), "http://traffic.invalid")

	var logBuf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelWarn}))
	a, err := app.New(cfg, app.WithLogger(logger), app.WithSleep(func(time.Duration) {}))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := a.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh must not fail on downstream errors: %v", err)
	}

	logOut := logBuf.String()
	if !strings.Contains(logOut, "level=WARN") || !strings.Contains(logOut, "poll failed, using last-known-good") {
		t.Errorf("poll failure must produce a WARN 'poll failed, using last-known-good' line; log:\n%s", logOut)
	}
	if !strings.Contains(logOut, "downstream=primary") {
		t.Errorf("poll-failure WARN must name the downstream; log:\n%s", logOut)
	}
}
