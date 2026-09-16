package app_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gringolito/traefik-scout/internal/app"
	"github.com/gringolito/traefik-scout/internal/config"
)

// rawdata fixture field names and common values used across multiple tests.
const (
	fEntryPoints = "entryPoints"
	fService     = "service"
	fRule        = "rule"
	fStatus      = "status"
	fProvider    = "provider"

	vEnabled     = "enabled"
	vDocker      = "docker"
	vWeb         = "web"
	vMyRouter    = "my-router@docker"
	vSvcDocker   = "svc@docker"
	vExampleRule = `Host("example.com")`
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
		EdgeEntrypoints: []string{vWeb, "websecure"},
		LogLevel:        "info",
		Downstreams: []config.Downstream{
			{
				Name:               "primary",
				APIAddress:         apiURL,
				TrafficAddress:     trafficURL,
				AllowedEntrypoints: []string{vWeb},
			},
		},
	}
}

// fakeDownstream starts an httptest.Server that serves the given routers at
// GET /api/rawdata in Traefik rawdata format.
func fakeDownstream(routers map[string]any) *httptest.Server {
	body, err := json.Marshal(map[string]any{"routers": routers})
	if err != nil {
		panic("fakeDownstream: json.Marshal: " + err.Error())
	}
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/rawdata" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(body)
	}))
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
		vMyRouter: map[string]any{
			fEntryPoints: []string{vWeb},
			fService:     "my-svc@docker",
			fRule:        vExampleRule,
			fStatus:      vEnabled,
			fProvider:    vDocker,
		},
	})
	defer ds.Close()

	cfg := testConfig(ds.URL, "http://traffic.example.com:80")
	a, err := app.New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := a.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh: %v", err)
	}

	env := queryHandler(t, a.Handler(), cfg.ConfigPath)

	if _, ok := env.HTTP.Routers["primary-my-router"]; !ok {
		t.Errorf("expected router %q; got %v", "primary-my-router", routerKeys(env))
	}
	if _, ok := env.HTTP.Services["primary"]; !ok {
		t.Errorf("expected service %q", "primary")
	}
}

// Cycle 3: Routers from the internal provider must not appear in the output.
func TestRefreshAndHandler_ExcludesInternalProvider(t *testing.T) {
	ds := fakeDownstream(map[string]any{
		"dashboard@internal": map[string]any{
			fEntryPoints: []string{vWeb},
			fService:     "dashboard@internal",
			fRule:        `PathPrefix("/api")`,
			fStatus:      vEnabled,
			fProvider:    "internal",
		},
		"good-router@docker": map[string]any{
			fEntryPoints: []string{vWeb},
			fService:     vSvcDocker,
			fRule:        vExampleRule,
			fStatus:      vEnabled,
			fProvider:    vDocker,
		},
	})
	defer ds.Close()

	cfg := testConfig(ds.URL, "http://traffic.example.com:80")
	a, _ := app.New(cfg)
	_ = a.Refresh(context.Background())

	env := queryHandler(t, a.Handler(), cfg.ConfigPath)

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
			fEntryPoints: []string{vWeb},
			fService:     vSvcDocker,
			fRule:        `Host("off.example.com")`,
			fStatus:      "disabled",
			fProvider:    vDocker,
		},
		"on-router@docker": map[string]any{
			fEntryPoints: []string{vWeb},
			fService:     vSvcDocker,
			fRule:        `Host("on.example.com")`,
			fStatus:      vEnabled,
			fProvider:    vDocker,
		},
	})
	defer ds.Close()

	cfg := testConfig(ds.URL, "http://traffic.example.com:80")
	a, _ := app.New(cfg)
	_ = a.Refresh(context.Background())

	env := queryHandler(t, a.Handler(), cfg.ConfigPath)

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
			fEntryPoints: []string{"tcpep"},
			fService:     vSvcDocker,
			fRule:        `Host("tcp.example.com")`,
			fStatus:      vEnabled,
			fProvider:    vDocker,
		},
		"web-router@docker": map[string]any{
			fEntryPoints: []string{vWeb},
			fService:     vSvcDocker,
			fRule:        `Host("web.example.com")`,
			fStatus:      vEnabled,
			fProvider:    vDocker,
		},
	})
	defer ds.Close()

	// testConfig sets AllowedEntrypoints: ["web"]; "tcpep" is not in the list.
	cfg := testConfig(ds.URL, "http://traffic.example.com:80")
	a, _ := app.New(cfg)
	_ = a.Refresh(context.Background())

	env := queryHandler(t, a.Handler(), cfg.ConfigPath)

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
		vMyRouter: map[string]any{
			fEntryPoints: []string{vWeb}, // downstream entrypoint
			fService:     vSvcDocker,
			fRule:        vExampleRule,
			fStatus:      vEnabled,
			fProvider:    vDocker,
		},
	})
	defer ds.Close()

	cfg := testConfig(ds.URL, "http://traffic.example.com:80")
	// EdgeEntrypoints = ["web", "websecure"] (set by testConfig)
	a, _ := app.New(cfg)
	_ = a.Refresh(context.Background())

	env := queryHandler(t, a.Handler(), cfg.ConfigPath)

	raw, ok := env.HTTP.Routers["primary-my-router"]
	if !ok {
		t.Fatal("expected router primary-my-router")
	}

	var r struct {
		EntryPoints []string `json:"entryPoints"`
	}
	if err := json.Unmarshal(raw, &r); err != nil {
		t.Fatalf("unmarshal router: %v", err)
	}

	want := cfg.EdgeEntrypoints
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
		vMyRouter: map[string]any{
			fEntryPoints: []string{vWeb},
			fService:     vSvcDocker,
			fRule:        vExampleRule,
			fStatus:      vEnabled,
			fProvider:    vDocker,
		},
	})
	defer ds.Close()

	cfg := testConfig(ds.URL, "http://traffic.example.com:80")
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
