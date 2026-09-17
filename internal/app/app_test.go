package app_test

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"io"
	"log/slog"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gringolito/traefik-scout/internal/app"
	"github.com/gringolito/traefik-scout/internal/config"
)

// rawdata fixture field names and common values used across multiple tests.
const (
	fieldEntryPoints = "entryPoints"
	fieldService     = "service"
	fieldRule        = "rule"
	fieldStatus      = "status"
	fieldProvider    = "provider"

	valueEnabled         = "enabled"
	valueDocker          = "docker"
	valueWeb             = "web"
	valueWebsecure       = "websecure"
	valueMyRouter        = "my-router@docker"
	valueSvcDocker       = "svc@docker"
	valueExampleRule     = `Host("example.com")`
	valueHostA           = `Host("a.example.com")`
	valueHostB           = `Host("b.example.com")`
	valueAppA            = "app-a@docker"
	valueAppB            = "app-b@docker"
	valuePrimary         = "primary"
	valueGpu             = "gpu"
	valueTrafficPrimary  = "http://primary:80"
	valueTrafficGpu      = "http://gpu:80"
	valueTrafficExample  = "http://traffic.example.com:80"
	valueDownstreamHostA = "host-a"
	valueDownstreamHostB = "host-b"
	valuePrimaryMyRouter = "primary-my-router"
	valuePrimaryAppA     = "primary-app-a"
	valueGpuAppB         = "gpu-app-b"
)

// shared string literals used in multiple helpers.
const (
	rawdataPath      = "/api/rawdata"
	jsonFieldRouters = "routers"
	pemTypeCert      = "CERTIFICATE"
	pemTypeECKey     = "EC PRIVATE KEY"
)

// testConfig returns a minimal Config with one downstream pointed at apiURL,
// serving traffic to trafficURL.  EdgeEntrypoints are ["web", "websecure"].
func testConfig(apiURL, trafficURL string) config.Config {
	return config.Config{
		Listen:          ":0",
		ConfigPath:      "/config",
		PollInterval:    30 * time.Second,
		RequestTimeout:  5 * time.Second,
		MaxResponseSize: 10 * 1024 * 1024,
		EdgeEntrypoints: []string{valueWeb, valueWebsecure},
		LogLevel:        "info",
		Downstreams: []config.Downstream{
			{
				Name:               valuePrimary,
				APIAddress:         apiURL,
				TrafficAddress:     trafficURL,
				AllowedEntrypoints: []string{valueWeb},
			},
		},
	}
}

// multiConfig returns a Config suited for multi-downstream tests.
// EdgeEntrypoints are ["websecure"]; all other timing/size fields are test defaults.
func multiConfig(downstreams []config.Downstream) config.Config {
	return config.Config{
		ConfigPath:      "/config",
		RequestTimeout:  5 * time.Second,
		MaxResponseSize: 10 * 1024 * 1024,
		EdgeEntrypoints: []string{valueWebsecure},
		Downstreams:     downstreams,
	}
}

// fakeDownstream starts an httptest.Server that serves the given routers at
// GET /api/rawdata in Traefik rawdata format.
func fakeDownstream(routers map[string]any) *httptest.Server {
	body, err := json.Marshal(map[string]any{jsonFieldRouters: routers})
	if err != nil {
		panic("fakeDownstream: json.Marshal: " + err.Error())
	}
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != rawdataPath {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(body)
	}))
}

// sequencedDownstream starts an httptest.Server whose /api/rawdata responses
// cycle through responses in order.  A nil entry produces a 500.
func sequencedDownstream(t *testing.T, responses []map[string]any) *httptest.Server {
	t.Helper()
	var n atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != rawdataPath {
			http.NotFound(w, r)
			return
		}
		i := int(n.Add(1)-1) % len(responses)
		routers := responses[i]
		if routers == nil {
			http.Error(w, "upstream error", http.StatusInternalServerError)
			return
		}
		body, err := json.Marshal(map[string]any{jsonFieldRouters: routers})
		if err != nil {
			http.Error(w, "marshal error", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(body)
	}))
	t.Cleanup(srv.Close)
	return srv
}

// httpEnvelope is the top-level shape we expect the handler to serve.
type httpEnvelope struct {
	HTTP struct {
		Routers  map[string]json.RawMessage `json:"routers"`
		Services map[string]json.RawMessage `json:"services"`
	} `json:"http"`
}

// queryHandler wraps h in a fresh httptest.Server, issues GET path, decodes
// the JSON envelope, and fails the test immediately on any error.
func queryHandler(t *testing.T, h http.Handler, path string) *httpEnvelope {
	t.Helper()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)

	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, srv.URL+path, nil) //nolint:gosec
	if err != nil {
		t.Fatalf("build request %s: %v", path, err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET %s: %v", path, err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	var env httpEnvelope
	if err := json.NewDecoder(resp.Body).Decode(&env); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	return &env
}

func routerKeys(env *httpEnvelope) []string {
	keys := make([]string, 0, len(env.HTTP.Routers))
	for k := range env.HTTP.Routers {
		keys = append(keys, k)
	}
	return keys
}

// refreshAndQuery builds an App from ds using the default test config, calls
// Refresh, queries the handler, and fatals on any error.
func refreshAndQuery(t *testing.T, ds *httptest.Server) *httpEnvelope {
	t.Helper()
	cfg := testConfig(ds.URL, valueTrafficExample)
	a, err := app.New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := a.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	return queryHandler(t, a.Handler(), cfg.ConfigPath)
}

// Cycle 1: New with a valid Config must return a non-nil *App without error.
func TestNew_ValidConfig(t *testing.T) {
	cfg := testConfig("http://localhost:8080", "http://localhost:80")
	a, err := app.New(cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if a == nil {
		t.Fatal("expected non-nil *App")
	}
}

// Cycle 2: Refresh against one fake downstream produces the expected
// router and service entries when Handler is queried.
func TestRefreshAndHandler_HappyPath(t *testing.T) {
	ds := fakeDownstream(map[string]any{
		valueMyRouter: map[string]any{
			fieldEntryPoints: []string{valueWeb},
			fieldService:     "my-svc@docker",
			fieldRule:        valueExampleRule,
			fieldStatus:      valueEnabled,
			fieldProvider:    valueDocker,
		},
	})
	defer ds.Close()

	cfg := testConfig(ds.URL, valueTrafficExample)
	a, err := app.New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := a.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh: %v", err)
	}

	env := queryHandler(t, a.Handler(), cfg.ConfigPath)

	if _, ok := env.HTTP.Routers[valuePrimaryMyRouter]; !ok {
		t.Errorf("expected router %q; got %v", valuePrimaryMyRouter, routerKeys(env))
	}
	if _, ok := env.HTTP.Services[valuePrimary]; !ok {
		t.Errorf("expected service %q", valuePrimary)
	}
}

// Cycle 3: Routers from the internal provider must not appear in the output.
func TestRefreshAndHandler_ExcludesInternalProvider(t *testing.T) {
	ds := fakeDownstream(map[string]any{
		"dashboard@internal": map[string]any{
			fieldEntryPoints: []string{valueWeb},
			fieldService:     "dashboard@internal",
			fieldRule:        `PathPrefix("/api")`,
			fieldStatus:      valueEnabled,
			fieldProvider:    "internal",
		},
		"good-router@docker": map[string]any{
			fieldEntryPoints: []string{valueWeb},
			fieldService:     valueSvcDocker,
			fieldRule:        valueExampleRule,
			fieldStatus:      valueEnabled,
			fieldProvider:    valueDocker,
		},
	})
	defer ds.Close()

	env := refreshAndQuery(t, ds)

	if _, ok := env.HTTP.Routers["primary-dashboard"]; ok {
		t.Error("internal-provider router must not appear in output")
	}
	if _, ok := env.HTTP.Routers["primary-good-router"]; !ok {
		t.Error("non-internal router must appear in output")
	}
}

// Cycle 4: Routers with status=disabled must not appear in the output.
func TestRefreshAndHandler_ExcludesDisabled(t *testing.T) {
	ds := fakeDownstream(map[string]any{
		"off-router@docker": map[string]any{
			fieldEntryPoints: []string{valueWeb},
			fieldService:     valueSvcDocker,
			fieldRule:        `Host("off.example.com")`,
			fieldStatus:      "disabled",
			fieldProvider:    valueDocker,
		},
		"on-router@docker": map[string]any{
			fieldEntryPoints: []string{valueWeb},
			fieldService:     valueSvcDocker,
			fieldRule:        `Host("on.example.com")`,
			fieldStatus:      valueEnabled,
			fieldProvider:    valueDocker,
		},
	})
	defer ds.Close()

	env := refreshAndQuery(t, ds)

	if _, ok := env.HTTP.Routers["primary-off-router"]; ok {
		t.Error("disabled router must not appear in output")
	}
	if _, ok := env.HTTP.Routers["primary-on-router"]; !ok {
		t.Error("enabled router must appear in output")
	}
}

// Cycle 5: Routers whose entrypoints are not in the downstream allow-list must
// be excluded.
func TestRefreshAndHandler_ExcludesNonAllowedEntrypoints(t *testing.T) {
	ds := fakeDownstream(map[string]any{
		"tcp-router@docker": map[string]any{
			fieldEntryPoints: []string{"tcpep"},
			fieldService:     valueSvcDocker,
			fieldRule:        `Host("tcp.example.com")`,
			fieldStatus:      valueEnabled,
			fieldProvider:    valueDocker,
		},
		"web-router@docker": map[string]any{
			fieldEntryPoints: []string{valueWeb},
			fieldService:     valueSvcDocker,
			fieldRule:        `Host("web.example.com")`,
			fieldStatus:      valueEnabled,
			fieldProvider:    valueDocker,
		},
	})
	defer ds.Close()

	// testConfig sets AllowedEntrypoints: ["web"]; "tcpep" is not in the list.
	env := refreshAndQuery(t, ds)

	if _, ok := env.HTTP.Routers["primary-tcp-router"]; ok {
		t.Error("router with non-allowed entrypoint must not appear in output")
	}
	if _, ok := env.HTTP.Routers["primary-web-router"]; !ok {
		t.Error("router with allowed entrypoint must appear in output")
	}
}

// Cycle 6: The emitted router must carry the edge entrypoints from Config,
// not the downstream's original entrypoints.
func TestRefreshAndHandler_UsesEdgeEntrypoints(t *testing.T) {
	ds := fakeDownstream(map[string]any{
		valueMyRouter: map[string]any{
			fieldEntryPoints: []string{valueWeb}, // downstream entrypoint
			fieldService:     valueSvcDocker,
			fieldRule:        valueExampleRule,
			fieldStatus:      valueEnabled,
			fieldProvider:    valueDocker,
		},
	})
	defer ds.Close()

	// testConfig sets EdgeEntrypoints to ["web", "websecure"]; the output must
	// carry those, not the downstream's original ["web"].
	env := refreshAndQuery(t, ds)

	raw, ok := env.HTTP.Routers[valuePrimaryMyRouter]
	if !ok {
		t.Fatal("expected router primary-my-router")
	}

	var r struct {
		EntryPoints []string `json:"entryPoints"`
	}
	if err := json.Unmarshal(raw, &r); err != nil {
		t.Fatalf("unmarshal router: %v", err)
	}

	want := []string{valueWeb, valueWebsecure}
	if len(r.EntryPoints) != len(want) {
		t.Fatalf("entryPoints: got %v, want %v", r.EntryPoints, want)
	}
	for i, ep := range want {
		if r.EntryPoints[i] != ep {
			t.Errorf("entryPoints[%d]: got %q, want %q", i, r.EntryPoints[i], ep)
		}
	}
}

// Cycle 7: Two Refresh cycles over unchanged fixtures produce byte-identical
// output; a conditional GET with the ETag from the first cycle receives 304.
func TestRefreshAndHandler_ETagStability(t *testing.T) {
	ds := fakeDownstream(map[string]any{
		valueMyRouter: map[string]any{
			fieldEntryPoints: []string{valueWeb},
			fieldService:     valueSvcDocker,
			fieldRule:        valueExampleRule,
			fieldStatus:      valueEnabled,
			fieldProvider:    valueDocker,
		},
	})
	defer ds.Close()

	cfg := testConfig(ds.URL, valueTrafficExample)
	a, _ := app.New(cfg)

	srv := httptest.NewServer(a.Handler())
	defer srv.Close()

	get := func(label string) (body []byte, etag string) {
		t.Helper()
		req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, srv.URL+cfg.ConfigPath, nil) //nolint:gosec
		if err != nil {
			t.Fatalf("%s build request: %v", label, err)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("%s GET: %v", label, err)
		}
		defer func() { _ = resp.Body.Close() }()
		b, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatalf("%s read body: %v", label, err)
		}
		return b, resp.Header.Get("ETag")
	}

	if err := a.Refresh(context.Background()); err != nil {
		t.Fatalf("first Refresh: %v", err)
	}
	body1, etag1 := get("first")
	if etag1 == "" {
		t.Fatal("expected non-empty ETag after first Refresh")
	}

	if err := a.Refresh(context.Background()); err != nil {
		t.Fatalf("second Refresh: %v", err)
	}
	body2, etag2 := get("second")

	if string(body1) != string(body2) {
		t.Errorf("bodies differ across Refresh cycles:\n  first:  %s\n  second: %s", body1, body2)
	}
	if etag1 != etag2 {
		t.Errorf("ETag changed across Refresh cycles: %q -> %q", etag1, etag2)
	}

	// Conditional GET with the stable ETag must yield 304.
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, srv.URL+cfg.ConfigPath, nil) //nolint:gosec
	if err != nil {
		t.Fatalf("conditional GET request: %v", err)
	}
	req.Header.Set("If-None-Match", etag1)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("conditional GET: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusNotModified {
		t.Errorf("expected 304 Not Modified, got %d", resp.StatusCode)
	}
}

// Spec note: an empty AllowedEntrypoints passes every router through.  The
// issue AC says "routers without an allow-listed entrypoint are excluded", but
// an empty list is treated as no filter rather than "exclude all".  This test
// documents and pins that decision.
func TestRefreshAndHandler_EmptyAllowListPassesAll(t *testing.T) {
	ds := fakeDownstream(map[string]any{
		"any-router@docker": map[string]any{
			fieldEntryPoints: []string{"whatever"},
			fieldService:     valueSvcDocker,
			fieldRule:        valueExampleRule,
			fieldStatus:      valueEnabled,
			fieldProvider:    valueDocker,
		},
	})
	defer ds.Close()

	cfg := testConfig(ds.URL, valueTrafficExample)
	cfg.Downstreams[0].AllowedEntrypoints = nil // empty = no filter; all routers pass
	a, err := app.New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := a.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh: %v", err)
	}

	env := queryHandler(t, a.Handler(), cfg.ConfigPath)
	if _, ok := env.HTTP.Routers["primary-any-router"]; !ok {
		t.Error("empty AllowedEntrypoints must pass all routers through")
	}
}

// Issue #4 — Cycle 1: three downstreams with distinct routes all appear in the
// merged Handler response.
func TestRefresh_ThreeDownstreams_AllRoutesAppear(t *testing.T) {
	ds1 := fakeDownstream(map[string]any{
		valueAppA: map[string]any{fieldEntryPoints: []string{valueWeb}, fieldRule: valueHostA, fieldStatus: valueEnabled, fieldProvider: valueDocker},
	})
	ds2 := fakeDownstream(map[string]any{
		valueAppB: map[string]any{fieldEntryPoints: []string{valueWeb}, fieldRule: valueHostB, fieldStatus: valueEnabled, fieldProvider: valueDocker},
	})
	ds3 := fakeDownstream(map[string]any{
		"app-c@docker": map[string]any{fieldEntryPoints: []string{valueWeb}, fieldRule: `Host("c.example.com")`, fieldStatus: valueEnabled, fieldProvider: valueDocker},
	})
	defer ds1.Close()
	defer ds2.Close()
	defer ds3.Close()

	cfg := multiConfig([]config.Downstream{
		{Name: valuePrimary, APIAddress: ds1.URL, TrafficAddress: valueTrafficPrimary, AllowedEntrypoints: []string{valueWeb}},
		{Name: valueGpu, APIAddress: ds2.URL, TrafficAddress: valueTrafficGpu, AllowedEntrypoints: []string{valueWeb}},
		{Name: "k8s", APIAddress: ds3.URL, TrafficAddress: "http://k8s:80", AllowedEntrypoints: []string{valueWeb}},
	})

	a, err := app.New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := a.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh: %v", err)
	}

	env := queryHandler(t, a.Handler(), cfg.ConfigPath)

	for _, key := range []string{valuePrimaryAppA, valueGpuAppB, "k8s-app-c"} {
		if _, ok := env.HTTP.Routers[key]; !ok {
			t.Errorf("missing router %q in merged config; got %v", key, routerKeys(env))
		}
	}
}

// Issue #4 — Cycle 2: two downstreams that both define a router with the same
// base name each appear under their own prefixed key; neither overwrites the other.
func TestRefresh_RouterNameCollision_BothPrefixed(t *testing.T) {
	ds1 := fakeDownstream(map[string]any{
		"dashboard@docker": map[string]any{fieldEntryPoints: []string{valueWeb}, fieldRule: valueHostA, fieldStatus: valueEnabled, fieldProvider: valueDocker},
	})
	ds2 := fakeDownstream(map[string]any{
		"dashboard@docker": map[string]any{fieldEntryPoints: []string{valueWeb}, fieldRule: valueHostB, fieldStatus: valueEnabled, fieldProvider: valueDocker},
	})
	defer ds1.Close()
	defer ds2.Close()

	cfg := multiConfig([]config.Downstream{
		{Name: valueDownstreamHostA, APIAddress: ds1.URL, TrafficAddress: "http://host-a:80", AllowedEntrypoints: []string{valueWeb}},
		{Name: valueDownstreamHostB, APIAddress: ds2.URL, TrafficAddress: "http://host-b:80", AllowedEntrypoints: []string{valueWeb}},
	})

	a, err := app.New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := a.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh: %v", err)
	}

	env := queryHandler(t, a.Handler(), cfg.ConfigPath)

	if _, ok := env.HTTP.Routers["host-a-dashboard"]; !ok {
		t.Error("missing router host-a-dashboard")
	}
	if _, ok := env.HTTP.Routers["host-b-dashboard"]; !ok {
		t.Error("missing router host-b-dashboard")
	}
	if len(env.HTTP.Routers) != 2 {
		t.Errorf("want exactly 2 routers, got %d: %v", len(env.HTTP.Routers), routerKeys(env))
	}
}

// Issue #4 — Cycle 3: a configured priority offset appears on the emitted router.
func TestRefresh_PriorityOffset(t *testing.T) {
	ds := fakeDownstream(map[string]any{
		"app-x@docker": map[string]any{fieldEntryPoints: []string{valueWeb}, fieldRule: `Host("x.example.com")`, fieldStatus: valueEnabled, fieldProvider: valueDocker},
	})
	defer ds.Close()

	cfg := multiConfig([]config.Downstream{
		{Name: "edge", APIAddress: ds.URL, TrafficAddress: "http://edge:80", AllowedEntrypoints: []string{valueWeb}, PriorityOffset: 100},
	})

	a, err := app.New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := a.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh: %v", err)
	}

	env := queryHandler(t, a.Handler(), cfg.ConfigPath)

	raw, ok := env.HTTP.Routers["edge-app-x"]
	if !ok {
		t.Fatal("missing router edge-app-x")
	}
	var r struct {
		Priority int `json:"priority"`
	}
	if err := json.Unmarshal(raw, &r); err != nil {
		t.Fatalf("unmarshal router: %v", err)
	}
	if r.Priority != 100 {
		t.Errorf("Priority: got %d, want 100", r.Priority)
	}
}

// Issue #4 — Cycle 4: two downstreams contributing an identical rule produce a
// WARN log naming both downstream names, and both routers are still served.
func TestRefresh_IdenticalRule_WarnAndServe(t *testing.T) {
	sharedRule := `Host("shared.example.com")`
	ds1 := fakeDownstream(map[string]any{
		"app-shared@docker": map[string]any{fieldEntryPoints: []string{valueWeb}, fieldRule: sharedRule, fieldStatus: valueEnabled, fieldProvider: valueDocker},
	})
	ds2 := fakeDownstream(map[string]any{
		"app-shared2@docker": map[string]any{fieldEntryPoints: []string{valueWeb}, fieldRule: sharedRule, fieldStatus: valueEnabled, fieldProvider: valueDocker},
	})
	defer ds1.Close()
	defer ds2.Close()

	var logBuf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelWarn}))

	cfg := multiConfig([]config.Downstream{
		{Name: valueDownstreamHostA, APIAddress: ds1.URL, TrafficAddress: "http://host-a:80", AllowedEntrypoints: []string{valueWeb}},
		{Name: valueDownstreamHostB, APIAddress: ds2.URL, TrafficAddress: "http://host-b:80", AllowedEntrypoints: []string{valueWeb}},
	})

	a, err := app.New(cfg, app.WithLogger(logger))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := a.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh: %v", err)
	}

	logOut := logBuf.String()
	if !strings.Contains(logOut, valueDownstreamHostA) {
		t.Errorf("warning log does not name downstream host-a; log:\n%s", logOut)
	}
	if !strings.Contains(logOut, valueDownstreamHostB) {
		t.Errorf("warning log does not name downstream host-b; log:\n%s", logOut)
	}

	env := queryHandler(t, a.Handler(), cfg.ConfigPath)
	if _, ok := env.HTTP.Routers["host-a-app-shared"]; !ok {
		t.Error("missing router host-a-app-shared")
	}
	if _, ok := env.HTTP.Routers["host-b-app-shared2"]; !ok {
		t.Error("missing router host-b-app-shared2")
	}
}

// Issue #4 / #16: a downstream that has never succeeded contributes nothing
// for that cycle; routes from healthy downstreams still appear.
func TestRefresh_FailingDownstream_HealthyDownstreamsStillServed(t *testing.T) {
	// A closed server simulates a downstream that has never been reachable
	// (no last-known-good data).
	dead := fakeDownstream(map[string]any{})
	dead.Close()

	live := fakeDownstream(map[string]any{
		valueAppA: map[string]any{fieldEntryPoints: []string{valueWeb}, fieldRule: valueHostA, fieldStatus: valueEnabled, fieldProvider: valueDocker},
	})
	defer live.Close()

	cfg := multiConfig([]config.Downstream{
		{Name: "dead", APIAddress: dead.URL, TrafficAddress: "http://dead:80", AllowedEntrypoints: []string{valueWeb}},
		{Name: "live", APIAddress: live.URL, TrafficAddress: "http://live:80", AllowedEntrypoints: []string{valueWeb}},
	})

	a, err := app.New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := a.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh must not fail when a downstream is unreachable: %v", err)
	}

	env := queryHandler(t, a.Handler(), cfg.ConfigPath)
	if _, ok := env.HTTP.Routers["live-app-a"]; !ok {
		t.Error("healthy downstream's router must appear when another downstream is unreachable")
	}
}

// Issue #16: when a downstream fails after having been healthy, its
// last-known-good routes are retained in the merged output.  Routes from
// healthy downstreams reflect the latest poll.
func TestRefresh_FailingDownstream_LastGoodRoutesRetained(t *testing.T) {
	// primary: healthy in cycle 1, returns 500 in cycle 2.
	primary := sequencedDownstream(t, []map[string]any{
		{valueAppA: map[string]any{fieldEntryPoints: []string{valueWeb}, fieldRule: valueHostA, fieldStatus: valueEnabled, fieldProvider: valueDocker}},
		nil, // cycle 2: 500
	})
	// gpu: healthy in both cycles.
	gpu := sequencedDownstream(t, []map[string]any{
		{valueAppB: map[string]any{fieldEntryPoints: []string{valueWeb}, fieldRule: valueHostB, fieldStatus: valueEnabled, fieldProvider: valueDocker}},
		{valueAppB: map[string]any{fieldEntryPoints: []string{valueWeb}, fieldRule: valueHostB, fieldStatus: valueEnabled, fieldProvider: valueDocker}},
	})

	cfg := multiConfig([]config.Downstream{
		{Name: valuePrimary, APIAddress: primary.URL, TrafficAddress: valueTrafficPrimary, AllowedEntrypoints: []string{valueWeb}},
		{Name: valueGpu, APIAddress: gpu.URL, TrafficAddress: valueTrafficGpu, AllowedEntrypoints: []string{valueWeb}},
	})

	a, err := app.New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	if err := a.Refresh(context.Background()); err != nil {
		t.Fatalf("cycle 1 Refresh: %v", err)
	}
	if err := a.Refresh(context.Background()); err != nil {
		t.Fatalf("cycle 2 Refresh: %v", err)
	}

	env := queryHandler(t, a.Handler(), cfg.ConfigPath)
	if _, ok := env.HTTP.Routers[valuePrimaryAppA]; !ok {
		t.Error("failing downstream's last-good routes must still be served after a poll failure")
	}
	if _, ok := env.HTTP.Routers[valueGpuAppB]; !ok {
		t.Error("healthy downstream's routes must still be served")
	}
}

// Issue #16: when all downstreams fail but each has last-known-good data,
// the previous routes are retained for every downstream.
func TestRefresh_AllDownstreamsFail_LastGoodRoutesRetained(t *testing.T) {
	primary := sequencedDownstream(t, []map[string]any{
		{valueAppA: map[string]any{fieldEntryPoints: []string{valueWeb}, fieldRule: valueHostA, fieldStatus: valueEnabled, fieldProvider: valueDocker}},
		nil, // cycle 2: 500
	})
	gpu := sequencedDownstream(t, []map[string]any{
		{valueAppB: map[string]any{fieldEntryPoints: []string{valueWeb}, fieldRule: valueHostB, fieldStatus: valueEnabled, fieldProvider: valueDocker}},
		nil, // cycle 2: 500
	})

	cfg := multiConfig([]config.Downstream{
		{Name: valuePrimary, APIAddress: primary.URL, TrafficAddress: valueTrafficPrimary, AllowedEntrypoints: []string{valueWeb}},
		{Name: valueGpu, APIAddress: gpu.URL, TrafficAddress: valueTrafficGpu, AllowedEntrypoints: []string{valueWeb}},
	})

	a, err := app.New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	if err := a.Refresh(context.Background()); err != nil {
		t.Fatalf("cycle 1 Refresh: %v", err)
	}
	if err := a.Refresh(context.Background()); err != nil {
		t.Fatalf("cycle 2 Refresh: %v", err)
	}

	env := queryHandler(t, a.Handler(), cfg.ConfigPath)
	if _, ok := env.HTTP.Routers[valuePrimaryAppA]; !ok {
		t.Error("primary routes must be retained when all downstreams fail")
	}
	if _, ok := env.HTTP.Routers[valueGpuAppB]; !ok {
		t.Error("gpu routes must be retained when all downstreams fail")
	}
}

// ---- Issue #5 helpers -------------------------------------------------------

// testCA holds a generated CA certificate and key for test TLS scenarios.
type testCA struct {
	cert    *x509.Certificate
	key     *ecdsa.PrivateKey
	certPEM []byte
}

// newTestCA generates a fresh self-signed CA cert valid for one hour.
func newTestCA(t *testing.T) *testCA {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate CA key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
	}
	certDER, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create CA cert: %v", err)
	}
	cert, err := x509.ParseCertificate(certDER)
	if err != nil {
		t.Fatalf("parse CA cert: %v", err)
	}
	return &testCA{
		cert:    cert,
		key:     key,
		certPEM: pem.EncodeToMemory(&pem.Block{Type: pemTypeCert, Bytes: certDER}),
	}
}

// signServerCert issues a server cert for 127.0.0.1 signed by the CA.
func (ca *testCA) signServerCert(t *testing.T) tls.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate server key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "127.0.0.1"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	certDER, err := x509.CreateCertificate(rand.Reader, tmpl, ca.cert, &key.PublicKey, ca.key)
	if err != nil {
		t.Fatalf("create server cert: %v", err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("marshal server key: %v", err)
	}
	pair, err := tls.X509KeyPair(
		pem.EncodeToMemory(&pem.Block{Type: pemTypeCert, Bytes: certDER}),
		pem.EncodeToMemory(&pem.Block{Type: pemTypeECKey, Bytes: keyDER}),
	)
	if err != nil {
		t.Fatalf("build server tls.Certificate: %v", err)
	}
	return pair
}

// signClientCert issues a client cert signed by the CA and returns PEM-encoded cert and key.
func (ca *testCA) signClientCert(t *testing.T) (certPEM, keyPEM []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate client key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(3),
		Subject:      pkix.Name{CommonName: "test-client"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	certDER, err := x509.CreateCertificate(rand.Reader, tmpl, ca.cert, &key.PublicKey, ca.key)
	if err != nil {
		t.Fatalf("create client cert: %v", err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("marshal client key: %v", err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: pemTypeCert, Bytes: certDER}),
		pem.EncodeToMemory(&pem.Block{Type: pemTypeECKey, Bytes: keyDER})
}

// writePEMFile writes content to a temp file and returns the path.
func writePEMFile(t *testing.T, content []byte) string {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "*.pem")
	if err != nil {
		t.Fatalf("create temp file: %v", err)
	}
	defer func() { _ = f.Close() }()
	if _, err := f.Write(content); err != nil {
		t.Fatalf("write pem file: %v", err)
	}
	return f.Name()
}

// requestCheckingDownstream starts a fake downstream that calls checkFn on
// every /api/rawdata request before serving an empty rawdata JSON response.
func requestCheckingDownstream(t *testing.T, checkFn func(t *testing.T, r *http.Request)) *httptest.Server {
	t.Helper()
	body, _ := json.Marshal(map[string]any{jsonFieldRouters: map[string]any{}})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != rawdataPath {
			http.NotFound(w, r)
			return
		}
		checkFn(t, r)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(body)
	}))
	t.Cleanup(srv.Close)
	return srv
}

// tlsDownstreamWith starts an HTTPS fake downstream using the given TLS config.
func tlsDownstreamWith(t *testing.T, tlsCfg *tls.Config) *httptest.Server {
	t.Helper()
	body, _ := json.Marshal(map[string]any{jsonFieldRouters: map[string]any{}})
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != rawdataPath {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(body)
	}))
	srv.TLS = tlsCfg
	srv.StartTLS()
	t.Cleanup(srv.Close)
	return srv
}

// refreshWith builds an App from cfg, calls Refresh, and fatals on any error.
func refreshWith(t *testing.T, cfg config.Config) {
	t.Helper()
	a, err := app.New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := a.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh: %v", err)
	}
}

// ---- Issue #5, Cycle 1 ------------------------------------------------------

// A downstream configured with a bearer token must send that exact
// Authorization header on every poll request.
func TestRefresh_BearerToken_SendsAuthorizationHeader(t *testing.T) {
	const token = "my-secret-bearer-token"
	ds := requestCheckingDownstream(t, func(t *testing.T, r *http.Request) {
		t.Helper()
		got := r.Header.Get("Authorization")
		want := "Bearer " + token
		if got != want {
			t.Errorf("Authorization = %q, want %q", got, want)
		}
	})
	cfg := testConfig(ds.URL, valueTrafficExample)
	cfg.Downstreams[0].Auth = &config.Auth{Token: token}
	refreshWith(t, cfg)
}

// ---- Issue #5, Cycle 2 ------------------------------------------------------

// A downstream configured with basic auth credentials must send valid HTTP
// basic auth on every poll request.
func TestRefresh_BasicAuth_SendsCredentials(t *testing.T) {
	const (
		wantUser = "alice"
		wantPass = "hunter2"
	)
	ds := requestCheckingDownstream(t, func(t *testing.T, r *http.Request) {
		t.Helper()
		user, pass, ok := r.BasicAuth()
		if !ok {
			t.Error("request carries no HTTP basic auth credentials")
			return
		}
		if user != wantUser || pass != wantPass {
			t.Errorf("basic auth = (%q, %q), want (%q, %q)", user, pass, wantUser, wantPass)
		}
	})
	cfg := testConfig(ds.URL, valueTrafficExample)
	cfg.Downstreams[0].Auth = &config.Auth{Username: wantUser, Password: wantPass}
	refreshWith(t, cfg)
}

// ---- Issue #5, Cycle 3 ------------------------------------------------------

// A downstream configured with a custom CA cert must successfully poll a
// server whose certificate is signed by that CA.
func TestRefresh_CustomCA_PollsHTTPSServer(t *testing.T) {
	ca := newTestCA(t)
	serverCert := ca.signServerCert(t)

	ds := tlsDownstreamWith(t, &tls.Config{Certificates: []tls.Certificate{serverCert}})

	caPath := writePEMFile(t, ca.certPEM)
	cfg := testConfig(ds.URL, valueTrafficExample)
	cfg.Downstreams[0].TLS = &config.TLS{CA: caPath}
	refreshWith(t, cfg)
}

// ---- Issue #5, Cycle 4 ------------------------------------------------------

// A downstream configured with insecure-skip-verify must successfully poll a
// server presenting a self-signed certificate that the system trust store does
// not recognise.
func TestRefresh_InsecureSkipVerify_PollsSelfSignedServer(t *testing.T) {
	// httptest.NewTLSServer uses a built-in self-signed cert not in any system
	// trust store; a plain client would reject it.
	ds := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != rawdataPath {
			http.NotFound(w, r)
			return
		}
		body, _ := json.Marshal(map[string]any{jsonFieldRouters: map[string]any{}})
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(body)
	}))
	t.Cleanup(ds.Close)

	cfg := testConfig(ds.URL, valueTrafficExample)
	cfg.Downstreams[0].TLS = &config.TLS{InsecureSkipVerify: true}
	refreshWith(t, cfg)
}

// ---- Issue #5, Cycle 5 ------------------------------------------------------

// A downstream configured with a client certificate must present it during the
// TLS handshake.  The server requires and verifies a client cert; if none is
// presented the handshake fails and Refresh returns an error.
func TestRefresh_ClientCert_PresentedDuringTLS(t *testing.T) {
	ca := newTestCA(t)
	serverCert := ca.signServerCert(t)
	clientCertPEM, clientKeyPEM := ca.signClientCert(t)

	clientCAs := x509.NewCertPool()
	clientCAs.AddCert(ca.cert)

	ds := tlsDownstreamWith(t, &tls.Config{
		Certificates: []tls.Certificate{serverCert},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    clientCAs,
	})

	certPath := writePEMFile(t, clientCertPEM)
	keyPath := writePEMFile(t, clientKeyPEM)

	cfg := testConfig(ds.URL, valueTrafficExample)
	cfg.Downstreams[0].TLS = &config.TLS{CA: writePEMFile(t, ca.certPEM), Cert: certPath, Key: keyPath}
	refreshWith(t, cfg)
}
