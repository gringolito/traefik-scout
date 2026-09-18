// Tests drive the command through its public Run entry point: WithOnListen
// hands the test the bound listener (so tests need not parse stderr), and
// WithHandler substitutes a request handler when a test must control
// individual requests. Both are real functional options, so no test-only
// export shim or in-package test file is needed.
package cli_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gringolito/traefik-scout/internal/cli"
)

// fakeDownstream starts an httptest.Server serving the given routers at
// GET /api/rawdata in Traefik rawdata format.
func fakeDownstream(t *testing.T, routers map[string]any) *httptest.Server {
	t.Helper()
	ds := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/rawdata" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(map[string]any{"routers": routers}); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	}))
	t.Cleanup(ds.Close)
	return ds
}

// getResponse fetches url and returns the response; the caller closes Body.
func getResponse(t *testing.T, url string) *http.Response {
	t.Helper()
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, url, nil)
	if err != nil {
		t.Fatalf("build request %s: %v", url, err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	return resp
}

// getJSON fetches url and decodes the JSON object body, failing on error.
func getJSON(t *testing.T, url string) map[string]any {
	t.Helper()
	resp := getResponse(t, url)
	defer func() { _ = resp.Body.Close() }()
	var env map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&env); err != nil {
		t.Fatalf("decode %s: %v", url, err)
	}
	return env
}

// getStatus fetches url and returns the status code.
func getStatus(t *testing.T, url string) int {
	t.Helper()
	resp := getResponse(t, url)
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp.StatusCode
}

// getBody fetches url and returns the body as a string.
func getBody(t *testing.T, url string) string {
	t.Helper()
	resp := getResponse(t, url)
	defer func() { _ = resp.Body.Close() }()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return string(b)
}

// A config file that cannot be loaded or validated must exit
// non-zero with the offending error on stderr, before any server starts.
func TestRun_ConfigLoadError_ExitsNonZero(t *testing.T) {
	var stderr strings.Builder

	code := cli.Run(context.Background(), []string{"-" + cli.FlagConfigName, "testdata/does_not_exist.yaml"}, &stderr)

	if code != 1 {
		t.Errorf("exit code: got %d, want 1", code)
	}
	if out := stderr.String(); !strings.Contains(out, "does_not_exist.yaml") {
		t.Errorf("stderr must name the config path, got %q", out)
	}
}

// The -config flag wins over CONFIG_PATH when both are set.
func TestRun_FlagOverridesEnv(t *testing.T) {
	var stderr strings.Builder
	t.Setenv("CONFIG_PATH", "testdata/env_path_bad.yaml")

	code := cli.Run(context.Background(), []string{"-" + cli.FlagConfigName, "testdata/does_not_exist.yaml"}, &stderr)

	if code != 1 {
		t.Errorf("exit code: got %d, want 1", code)
	}
	if out := stderr.String(); !strings.Contains(out, "does_not_exist.yaml") {
		t.Errorf("flag path must take precedence; stderr: %q", out)
	}
}

// CONFIG_PATH alone supplies the config path.
func TestRun_EnvConfigPathUsed(t *testing.T) {
	var stderr strings.Builder
	t.Setenv("CONFIG_PATH", "testdata/does_not_exist.yaml")

	code := cli.Run(context.Background(), nil, &stderr)

	if code != 1 {
		t.Errorf("exit code: got %d, want 1", code)
	}
	if out := stderr.String(); !strings.Contains(out, "does_not_exist.yaml") {
		t.Errorf("env path must be used; stderr: %q", out)
	}
}

// slowDownstream serves an immediate first rawdata response (for the initial
// refresh), then blocks every subsequent fetch until released or the request
// context is canceled.
func slowDownstream(t *testing.T) (*httptest.Server, <-chan struct{}) {
	t.Helper()
	var n atomic.Int32
	inFlight := make(chan struct{}, 1)
	ds := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if int(n.Add(1)) > 1 {
			select {
			case inFlight <- struct{}{}:
			default:
			}
			<-r.Context().Done() // held until the poller gives up
			return
		}
		serveRawdataOK(w)
	}))
	t.Cleanup(ds.Close)
	return ds, inFlight
}

func serveRawdataOK(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write([]byte(`{"routers": {}}`))
}

// Cancelling the run context while a periodic refresh is in flight
// must abort the fetch through ctx, return 0 promptly, and close the listener.
func TestRun_CancelDuringInFlightRefresh(t *testing.T) {
	ds, inFlight := slowDownstream(t)

	path := filepath.Join(t.TempDir(), "config.yaml")
	cfgYAML := fmt.Sprintf("listen: %s\nconfig_path: %s\npoll_interval: 10ms\nrequest_timeout: 5s\nmax_response_size: 10485760\nlog_level: %s\nedge_entrypoints: [%s]\ndownstreams:\n  - name: %s\n    api_address: %s\n    traffic_address: %s\n    allowed_entrypoints: [%s]\n", testListen, testConfigPath, testLogLevel, entrypointWeb, testDownstream, ds.URL, ds.URL, entrypointWeb)
	if err := os.WriteFile(path, []byte(cfgYAML), 0o600); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	listenerCh := make(chan net.Listener, 1)
	done := make(chan int, 1)
	go func() {
		done <- cli.Run(ctx, []string{"-" + cli.FlagConfigName, path}, io.Discard, cli.WithOnListen(func(l net.Listener) { listenerCh <- l }))
	}()

	var addr string
	select {
	case l := <-listenerCh:
		addr = l.Addr().String()
	case <-time.After(5 * time.Second):
		t.Fatal("Run never bound a listener")
	}

	// Wait until the initial refresh succeeded and the next poll is in flight.
	base := "http://" + addr
	waitFor(t, 5*time.Second, func() bool { return getStatus(t, base+"/readyz") == http.StatusOK })
	select {
	case <-inFlight:
	case <-time.After(5 * time.Second):
		t.Fatal("no periodic refresh entered the slow downstream")
	}

	start := time.Now()
	cancel()
	select {
	case code := <-done:
		if code != 0 {
			t.Errorf("exit code: got %d, want 0", code)
		}
		if elapsed := time.Since(start); elapsed > 5*time.Second {
			t.Errorf("shutdown took %v; in-flight refresh did not abort via ctx", elapsed)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after cancellation")
	}

	// The listener must be closed: new connections are refused.
	var dialer net.Dialer
	conn, err := dialer.DialContext(context.Background(), "tcp", addr)
	if err == nil {
		_ = conn.Close()
		t.Error("connection succeeded after shutdown; listener still open")
	}
}

// A YAML file whose content fails validation surfaces the offending
// field through Run.
func TestRun_InvalidYAMLContent_SurfacesValidationError(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	err := os.WriteFile(path, []byte("edge_entrypoints: [websecure]\ndownstreams:\n  - name: primary\n    allowed_entrypoints: [web]\n"), 0o600)
	if err != nil {
		t.Fatal(err)
	}
	var stderr strings.Builder

	code := cli.Run(context.Background(), []string{"-" + cli.FlagConfigName, path}, &stderr)

	if code != 1 {
		t.Errorf("exit code: got %d, want 1", code)
	}
	if out := stderr.String(); !strings.Contains(out, "api_address") {
		t.Errorf("stderr must name the offending field, got %q", out)
	}
}

// safeStderr is an io.Writer collecting stderr into a string. Writes come
// from the goroutine running Run while the test goroutine reads inside
// waitFor, so access is guarded by a mutex (strings.Builder is not safe for
// concurrent use).
type safeStderr struct {
	mu  sync.Mutex
	buf strings.Builder
}

func (s *safeStderr) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.Write(p)
}

func (s *safeStderr) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.String()
}

// Run end to end with a valid config file. It serves the merged
// snapshot on the bound address (announced on stderr), then exits 0 cleanly
// on cancellation.
func TestRun_ValidConfig_ServesAndShutsDownCleanly(t *testing.T) {
	ds := fakeDownstream(t, map[string]any{
		"whoami@file": map[string]any{
			"entryPoints": []string{entrypointWeb},
			"service":     "whoami",
			"rule":        "Host(`example.com`)",
			"status":      "enabled",
			"provider":    "file",
			"priority":    10,
		},
	})
	path := filepath.Join(t.TempDir(), "config.yaml")
	cfgYAML := fmt.Sprintf("listen: 127.0.0.1:0\npoll_interval: 50ms\nlog_level: error\nedge_entrypoints: [%s]\ndownstreams:\n  - name: primary\n    api_address: %s\n    traffic_address: %s\n    allowed_entrypoints: [%s]\n", entrypointWeb, ds.URL, ds.URL, entrypointWeb)
	if err := os.WriteFile(path, []byte(cfgYAML), 0o600); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var stderr safeStderr
	done := make(chan int, 1)
	go func() {
		done <- cli.Run(ctx, []string{"-" + cli.FlagConfigName, path}, &stderr)
	}()

	var addr string
	waitFor(t, 5*time.Second, func() bool {
		for line := range strings.SplitSeq(stderr.String(), "\n") {
			if after, ok := strings.CutPrefix(line, "traefik-scout: listening on "); ok {
				addr = strings.TrimSpace(after)
				return true
			}
		}
		return false
	})

	env := getJSON(t, "http://"+addr+"/config")
	httpCfg := env["http"].(map[string]any)
	routers := httpCfg["routers"].(map[string]any)
	if _, ok := routers["primary-whoami"]; !ok {
		t.Errorf("merged config missing router primary-whoami: %v", routers)
	}

	cancel()
	select {
	case code := <-done:
		if code != 0 {
			t.Errorf("exit code: got %d, want 0", code)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after cancellation")
	}
}

// Shared test fixture values reused in the YAML config fixtures below.
const (
	testListen      = "127.0.0.1:0"
	testConfigPath  = "/config"
	testLogLevel    = "error"
	testDownstream  = "primary"
	testInitialPoll = 50 * time.Millisecond
)

// gatedHandler returns a handler that signals entered on the first request,
// then blocks it until release is closed, and finally writes a full response.
func gatedHandler(entered, release chan struct{}) http.Handler {
	var once sync.Once
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		once.Do(func() { close(entered) })
		<-release
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("drained"))
	})
}

// An HTTP request already in flight when the run context is
// canceled must complete successfully. Shutdown waits for active handlers
// instead of dropping the connection.
func TestRun_InFlightRequestCompletesDuringShutdown(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})

	path := filepath.Join(t.TempDir(), "config.yaml")
	// The downstream is unreachable on purpose: the failed initial refresh is
	// logged, not fatal, and the server must still serve. poll_interval of 1h
	// effectively disables periodic refreshes so the poller cannot interfere
	// with the in-flight request.
	cfgYAML := fmt.Sprintf("listen: %s\nconfig_path: %s\npoll_interval: 1h\nrequest_timeout: 5s\nmax_response_size: 10485760\nlog_level: %s\nedge_entrypoints: [%s]\ndownstreams:\n  - name: %s\n    api_address: http://127.0.0.1:1\n    traffic_address: http://127.0.0.1:80\n    allowed_entrypoints: [%s]\n", testListen, testConfigPath, testLogLevel, entrypointWeb, testDownstream, entrypointWeb)
	if err := os.WriteFile(path, []byte(cfgYAML), 0o600); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	listenerCh := make(chan net.Listener, 1)
	done := make(chan int, 1)
	go func() {
		done <- cli.Run(ctx, []string{"-" + cli.FlagConfigName, path}, io.Discard,
			cli.WithOnListen(func(l net.Listener) { listenerCh <- l }),
			cli.WithHandler(gatedHandler(entered, release)))
	}()

	var addr string
	select {
	case l := <-listenerCh:
		addr = l.Addr().String()
	case <-time.After(5 * time.Second):
		t.Fatal("Run never bound a listener")
	}

	// A failed initial refresh is logged, not fatal; the server still serves.
	type result struct {
		status int
		body   string
	}
	respCh := make(chan result, 1)
	go func() {
		resp := getResponse(t, "http://"+addr+"/anything")
		body, err := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if err != nil {
			t.Errorf("read body: %v", err)
			respCh <- result{status: resp.StatusCode}
			return
		}
		respCh <- result{status: resp.StatusCode, body: string(body)}
	}()

	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("handler never entered")
	}

	cancel()
	// Give an abrupt close (Close vs Shutdown) a chance to kill the connection
	// if the implementation is wrong, then release the handler.
	time.Sleep(100 * time.Millisecond)
	close(release)

	select {
	case r := <-respCh:
		if r.status != http.StatusOK {
			t.Errorf("in-flight request status: got %d, want 200", r.status)
		}
		if r.body != "drained" {
			t.Errorf("in-flight request body: got %q, want %q", r.body, "drained")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("in-flight request never completed; shutdown dropped the connection")
	}

	select {
	case code := <-done:
		if code != 0 {
			t.Errorf("exit code: got %d, want 0", code)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after cancellation")
	}
}

// waitFor polls check until it returns true or the timeout elapses.
func waitFor(t *testing.T, timeout time.Duration, check func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if check() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("condition never became true")
}

// entrypointWeb is the allowed edge entrypoint used across test fixtures.
const entrypointWeb = "web"

// With a valid config and a live downstream, Run binds an ephemeral
// listener, runs an initial refresh, and serves the merged config, /healthz,
// /readyz, and /metrics.
func TestRun_ServesMergedConfigAndHealth(t *testing.T) {
	ds := fakeDownstream(t, map[string]any{
		"whoami@file": map[string]any{
			"entryPoints": []string{entrypointWeb},
			"service":     "whoami",
			"rule":        "Host(`example.com`)",
			"status":      "enabled",
			"provider":    "file",
			"priority":    10,
		},
	})

	path := filepath.Join(t.TempDir(), "config.yaml")
	cfgYAML := fmt.Sprintf("listen: %s\nconfig_path: %s\npoll_interval: %s\nrequest_timeout: 5s\nmax_response_size: 10485760\nlog_level: %s\nedge_entrypoints: [%s]\ndownstreams:\n  - name: %s\n    api_address: %s\n    traffic_address: %s\n    allowed_entrypoints: [%s]\n", testListen, testConfigPath, testInitialPoll, testLogLevel, entrypointWeb, testDownstream, ds.URL, ds.URL, entrypointWeb)
	if err := os.WriteFile(path, []byte(cfgYAML), 0o600); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	listenerCh := make(chan net.Listener, 1)
	done := make(chan int, 1)
	go func() {
		done <- cli.Run(ctx, []string{"-" + cli.FlagConfigName, path}, io.Discard, cli.WithOnListen(func(l net.Listener) { listenerCh <- l }))
	}()

	var addr string
	select {
	case l := <-listenerCh:
		addr = l.Addr().String()
	case <-time.After(5 * time.Second):
		t.Fatal("Run never bound a listener")
	}
	// Stop the server no matter how the test exits; the goroutine's exit code
	// is read in the body when the run reaches it normally.
	t.Cleanup(cancel)

	base := "http://" + addr

	// The initial synchronous refresh must have populated the snapshot.  The
	// merged document is a Traefik file-provider dynamic config: http.routers.
	env := getJSON(t, base+testConfigPath)
	httpCfg, ok := env["http"].(map[string]any)
	if !ok {
		t.Fatalf("merged config missing http section: %v", env)
	}
	routers, ok := httpCfg["routers"].(map[string]any)
	if !ok {
		t.Fatalf("merged config missing http.routers: %v", httpCfg)
	}
	if _, ok := routers["primary-whoami"]; !ok {
		t.Errorf("merged config missing router primary-whoami: %v", routers)
	}

	if code := getStatus(t, base+"/healthz"); code != http.StatusOK {
		t.Errorf("/healthz: got %d, want 200", code)
	}
	if code := getStatus(t, base+"/readyz"); code != http.StatusOK {
		t.Errorf("/readyz: got %d, want 200 after successful initial refresh", code)
	}
	metrics := getBody(t, base+"/metrics")
	if !strings.Contains(metrics, "scout_routers") {
		t.Errorf("/metrics missing scout_routers: %q", metrics)
	}

	// Cancellation must shut down gracefully: serve returns 0.
	cancel()
	select {
	case code := <-done:
		if code != 0 {
			t.Errorf("exit code after cancellation: got %d, want 0", code)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after context cancellation")
	}
}
