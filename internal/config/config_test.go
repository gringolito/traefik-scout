package config_test

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gringolito/traefik-scout/internal/config"
)

// Cycle 1: a minimal YAML (only required downstream fields) must load without
// error and produce non-zero defaults for every global duration field.
func TestLoad_MinimalValid(t *testing.T) {
	cfg, err := config.Load("testdata/minimal_valid.yaml")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Global scalar defaults must be non-empty.
	if cfg.Listen == "" {
		t.Error("Listen must have a default")
	}
	if cfg.LogLevel == "" {
		t.Error("LogLevel must have a default")
	}
	if cfg.ConfigPath == "" {
		t.Error("ConfigPath must have a default")
	}

	// No duration field silently defaults to zero / empty string.
	if cfg.PollInterval <= 0 {
		t.Errorf("PollInterval must default to a positive duration, got %v", cfg.PollInterval)
	}
	if cfg.RequestTimeout <= 0 {
		t.Errorf("RequestTimeout must default to a positive duration, got %v", cfg.RequestTimeout)
	}
	if cfg.MaxResponseSize <= 0 {
		t.Errorf("MaxResponseSize must have a positive default, got %d", cfg.MaxResponseSize)
	}

	// The single downstream must be present.
	if len(cfg.Downstreams) != 1 {
		t.Fatalf("expected 1 downstream, got %d", len(cfg.Downstreams))
	}
	d := cfg.Downstreams[0]
	if d.Name != "primary" {
		t.Errorf("downstream name: got %q, want %q", d.Name, "primary")
	}
	if d.APIAddress != "http://traefik-primary:8080" {
		t.Errorf("downstream api_address: got %q", d.APIAddress)
	}
	if d.TrafficAddress != "http://traefik-primary:80" {
		t.Errorf("downstream traffic_address: got %q", d.TrafficAddress)
	}
}

// Cycle 2: missing required downstream address fields must produce an error
// that names the specific field.
func TestLoad_MissingAPIAddress(t *testing.T) {
	_, err := config.Load("testdata/missing_api_address.yaml")
	if err == nil {
		t.Fatal("expected error for missing api_address, got nil")
	}
	if !containsField(err, "api_address") {
		t.Errorf("error must name 'api_address', got: %v", err)
	}
}

func TestLoad_MissingTrafficAddress(t *testing.T) {
	_, err := config.Load("testdata/missing_traffic_address.yaml")
	if err == nil {
		t.Fatal("expected error for missing traffic_address, got nil")
	}
	if !containsField(err, "traffic_address") {
		t.Errorf("error must name 'traffic_address', got: %v", err)
	}
}

// Cycle 3: duplicate downstream names must produce an error that names the
// duplicated value.
func TestLoad_DuplicateDownstreamName(t *testing.T) {
	_, err := config.Load("testdata/duplicate_name.yaml")
	if err == nil {
		t.Fatal("expected error for duplicate downstream name, got nil")
	}
	if !containsField(err, "primary") {
		t.Errorf("error must name the duplicate (%q), got: %v", "primary", err)
	}
}

// Cycle 4: an unknown YAML key must produce an error that names the field.
func TestLoad_UnknownField(t *testing.T) {
	_, err := config.Load("testdata/unknown_field.yaml")
	if err == nil {
		t.Fatal("expected error for unknown field, got nil")
	}
	if !containsField(err, "typo_field") {
		t.Errorf("error must name 'typo_field', got: %v", err)
	}
}

// Cycle 5: an unparseable duration must produce an error that names the field.
func TestLoad_BadDuration(t *testing.T) {
	_, err := config.Load("testdata/bad_duration.yaml")
	if err == nil {
		t.Fatal("expected error for bad duration, got nil")
	}
	if !containsField(err, "poll_interval") {
		t.Errorf("error must name 'poll_interval', got: %v", err)
	}
}

// Lint cleanup (#12): parseDuration must wrap the underlying time.ParseDuration
// error with %w so callers can unwrap/errors.Is through the chain.
func TestLoad_BadDuration_WrapsUnderlyingError(t *testing.T) {
	_, err := config.Load("testdata/bad_duration.yaml")
	if err == nil {
		t.Fatal("expected error for bad duration, got nil")
	}

	unwrapped := errors.Unwrap(err)
	if unwrapped == nil {
		t.Fatal("errors.Unwrap(err) = nil, want the wrapped time.ParseDuration error")
	}

	_, wantErr := time.ParseDuration("not-a-duration")
	if wantErr == nil {
		t.Fatal("time.ParseDuration(\"not-a-duration\") unexpectedly succeeded")
	}
	if unwrapped.Error() != wantErr.Error() {
		t.Errorf("unwrapped error = %q, want %q", unwrapped.Error(), wantErr.Error())
	}
}

// Cycle 6: a downstream name with unsafe characters must produce an error
// that names the offending value.
func TestLoad_InvalidDownstreamName(t *testing.T) {
	_, err := config.Load("testdata/invalid_name.yaml")
	if err == nil {
		t.Fatal("expected error for invalid downstream name, got nil")
	}
	if !containsField(err, "bad name!") {
		t.Errorf("error must name the invalid value, got: %v", err)
	}
}

// Review fix: auth, tls, allowed_entrypoints, priority_offset, and
// staleness_limit must parse cleanly and not be rejected by KnownFields.
func TestLoad_DownstreamOptionalFields(t *testing.T) {
	cfg, err := config.Load("testdata/downstream_optional_fields.yaml")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(cfg.Downstreams) != 1 {
		t.Fatalf("expected 1 downstream, got %d", len(cfg.Downstreams))
	}
	d := cfg.Downstreams[0]

	if len(d.AllowedEntrypoints) != 2 {
		t.Errorf("allowed_entrypoints: got %v, want [web websecure]", d.AllowedEntrypoints)
	}
	if d.PriorityOffset != 10 {
		t.Errorf("priority_offset: got %d, want 10", d.PriorityOffset)
	}
	if d.StalenessLimit != 2*time.Minute {
		t.Errorf("staleness_limit: got %v, want 2m", d.StalenessLimit)
	}
	if d.Auth == nil {
		t.Fatal("auth must be populated")
	}
	if d.Auth.Username != "admin" {
		t.Errorf("auth.username: got %q, want %q", d.Auth.Username, "admin")
	}
	if d.TLS == nil {
		t.Fatal("tls must be populated")
	}
	if d.TLS.CA != "/etc/ssl/ca.crt" {
		t.Errorf("tls.ca: got %q, want %q", d.TLS.CA, "/etc/ssl/ca.crt")
	}
}

// URL validation: a non-absolute api_address must produce an error that names
// the field and the offending value.
func TestLoad_InvalidAPIURL(t *testing.T) {
	_, err := config.Load("testdata/invalid_api_url.yaml")
	if err == nil {
		t.Fatal("expected error for non-absolute api_address, got nil")
	}
	if !containsField(err, "api_address") {
		t.Errorf("error must name the field, got: %v", err)
	}
}

// Issue #25: a non-positive poll_interval must fail at startup with a message
// naming the field, before the listener binds or the ticker panics.
func TestLoad_NonPositivePollInterval_ReturnsError(t *testing.T) {
	path := writeYAML(t, `
poll_interval: 0s
edge_entrypoints:
  - websecure
downstreams:
  - name: primary
    api_address: http://traefik:8080
    traffic_address: http://traefik:80
    allowed_entrypoints:
      - web
`)
	_, err := config.Load(path)
	if err == nil {
		t.Fatal("expected error for zero poll_interval, got nil")
	}
	if !containsField(err, "poll_interval") {
		t.Errorf("error must name 'poll_interval', got: %v", err)
	}
}

// Issue #25: a non-positive request_timeout must fail at startup naming the field.
func TestLoad_NonPositiveRequestTimeout_ReturnsError(t *testing.T) {
	path := writeYAML(t, `
request_timeout: -3s
edge_entrypoints:
  - websecure
downstreams:
  - name: primary
    api_address: http://traefik:8080
    traffic_address: http://traefik:80
    allowed_entrypoints:
      - web
`)
	_, err := config.Load(path)
	if err == nil {
		t.Fatal("expected error for negative request_timeout, got nil")
	}
	if !containsField(err, "request_timeout") {
		t.Errorf("error must name 'request_timeout', got: %v", err)
	}
}

// Issue #25: a non-positive max_response_size must fail at startup naming the
// field: a zero limit makes every poll fail.
func TestLoad_NonPositiveMaxResponseSize_ReturnsError(t *testing.T) {
	path := writeYAML(t, `
max_response_size: 0
edge_entrypoints:
  - websecure
downstreams:
  - name: primary
    api_address: http://traefik:8080
    traffic_address: http://traefik:80
    allowed_entrypoints:
      - web
`)
	_, err := config.Load(path)
	if err == nil {
		t.Fatal("expected error for zero max_response_size, got nil")
	}
	if !containsField(err, "max_response_size") {
		t.Errorf("error must name 'max_response_size', got: %v", err)
	}
}

// Issue #25: config_path must start with / and must not collide with the
// health/metrics endpoints the App registers itself; a collision is a startup
// error, not a ServeMux panic after the listener binds.
func TestLoad_ConfigPathCollision_ReturnsError(t *testing.T) {
	for _, reserved := range []string{"healthz", "readyz", "metrics"} {
		path := writeYAML(t, fmt.Sprintf(`
config_path: /%s
edge_entrypoints:
  - websecure
downstreams:
  - name: primary
    api_address: http://traefik:8080
    traffic_address: http://traefik:80
    allowed_entrypoints:
      - web
`, reserved))
		_, err := config.Load(path)
		if err == nil {
			t.Fatalf("config_path /%s: expected error, got nil", reserved)
		}
		if !containsField(err, "config_path") {
			t.Errorf("config_path /%s: error must name 'config_path', got: %v", reserved, err)
		}
	}
}

// Issue #25: config_path must be an absolute path.
func TestLoad_ConfigPathRelative_ReturnsError(t *testing.T) {
	path := writeYAML(t, `
config_path: config
edge_entrypoints:
  - websecure
downstreams:
  - name: primary
    api_address: http://traefik:8080
    traffic_address: http://traefik:80
    allowed_entrypoints:
      - web
`)
	_, err := config.Load(path)
	if err == nil {
		t.Fatal("expected error for relative config_path, got nil")
	}
	if !containsField(err, "config_path") {
		t.Errorf("error must name 'config_path', got: %v", err)
	}
}

// Issue #25 (user story 18): at least one edge_entrypoints value is required;
// an empty list makes emitted routers carry no entryPoints key, so Traefik
// binds them to every entrypoint including plain HTTP.
func TestLoad_EmptyEdgeEntrypoints_ReturnsError(t *testing.T) {
	path := writeYAML(t, `
downstreams:
  - name: primary
    api_address: http://traefik:8080
    traffic_address: http://traefik:80
    allowed_entrypoints:
      - web
`)
	_, err := config.Load(path)
	if err == nil {
		t.Fatal("expected error for empty edge_entrypoints, got nil")
	}
	if !containsField(err, "edge_entrypoints") {
		t.Errorf("error must name 'edge_entrypoints', got: %v", err)
	}
}

// Issue #25: log_level must accept only supported level names.
func TestLoad_InvalidLogLevel_ReturnsError(t *testing.T) {
	path := writeYAML(t, `
log_level: bananas
edge_entrypoints:
  - websecure
downstreams:
  - name: primary
    api_address: http://traefik:8080
    traffic_address: http://traefik:80
    allowed_entrypoints:
      - web
`)
	_, err := config.Load(path)
	if err == nil {
		t.Fatal("expected error for invalid log_level, got nil")
	}
	if !containsField(err, "log_level") {
		t.Errorf("error must name 'log_level', got: %v", err)
	}
}

// Valid level names must load without error.
func TestLoad_ValidLogLevels(t *testing.T) {
	for _, level := range []string{"debug", "info", "warn", "error"} {
		path := writeYAML(t, fmt.Sprintf(`
log_level: %s
edge_entrypoints:
  - websecure
downstreams:
  - name: primary
    api_address: http://traefik:8080
    traffic_address: http://traefik:80
    allowed_entrypoints:
      - web
`, level))
		cfg, err := config.Load(path)
		if err != nil {
			t.Fatalf("log_level %q: unexpected error: %v", level, err)
		}
		if cfg.LogLevel != level {
			t.Errorf("log_level: got %q, want %q", cfg.LogLevel, level)
		}
	}
}

// Issue #26: log_format must default to "text" when unset.
func TestLoad_LogFormatDefault(t *testing.T) {
	path := writeYAML(t, `
edge_entrypoints:
  - websecure
downstreams:
  - name: primary
    api_address: http://traefik:8080
    traffic_address: http://traefik:80
    allowed_entrypoints:
      - web
`)
	cfg, err := config.Load(path)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.LogFormat != "text" {
		t.Errorf("log_format: got %q, want %q", cfg.LogFormat, "text")
	}
}

// Issue #26: valid log_format values must load without error.
func TestLoad_ValidLogFormats(t *testing.T) {
	for _, format := range []string{"text", "json"} {
		path := writeYAML(t, fmt.Sprintf(`
log_format: %s
edge_entrypoints:
  - websecure
downstreams:
  - name: primary
    api_address: http://traefik:8080
    traffic_address: http://traefik:80
    allowed_entrypoints:
      - web
`, format))
		cfg, err := config.Load(path)
		if err != nil {
			t.Fatalf("log_format %q: unexpected error: %v", format, err)
		}
		if cfg.LogFormat != format {
			t.Errorf("log_format: got %q, want %q", cfg.LogFormat, format)
		}
	}
}

// Issue #26: an unsupported log_format must fail at startup naming the field.
func TestLoad_InvalidLogFormat_ReturnsError(t *testing.T) {
	path := writeYAML(t, `
log_format: csv
edge_entrypoints:
  - websecure
downstreams:
  - name: primary
    api_address: http://traefik:8080
    traffic_address: http://traefik:80
    allowed_entrypoints:
      - web
`)
	_, err := config.Load(path)
	if err == nil {
		t.Fatal("expected error for invalid log_format, got nil")
	}
	if !containsField(err, "log_format") {
		t.Errorf("error must name 'log_format', got: %v", err)
	}
}

// Issue #25 (user story 13): each downstream must name at least one
// allowed_entrypoints value; "empty means publish everything" would publish
// host-local routes at the edge.
func TestLoad_MissingAllowedEntrypoints_ReturnsError(t *testing.T) {
	path := writeYAML(t, `
edge_entrypoints:
  - websecure
downstreams:
  - name: primary
    api_address: http://traefik:8080
    traffic_address: http://traefik:80
`)
	_, err := config.Load(path)
	if err == nil {
		t.Fatal("expected error for missing allowed_entrypoints, got nil")
	}
	if !containsField(err, "allowed_entrypoints") {
		t.Errorf("error must name 'allowed_entrypoints', got: %v", err)
	}
}

func containsField(err error, field string) bool {
	return err != nil && strings.Contains(err.Error(), field)
}

// writeYAML writes content to a temp file and returns the path.
func writeYAML(t *testing.T, content string) string {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "*.yaml")
	if err != nil {
		t.Fatalf("create temp file: %v", err)
	}
	defer func() { _ = f.Close() }()
	if _, err := f.WriteString(content); err != nil {
		t.Fatalf("write yaml: %v", err)
	}
	return f.Name()
}

// Auth.headers must parse from YAML and be accessible on Downstream.Auth.
func TestLoad_DownstreamAuthHeaders(t *testing.T) {
	path := writeYAML(t, `
edge_entrypoints:
  - websecure
downstreams:
  - name: primary
    api_address: http://traefik:8080
    traffic_address: http://traefik:80
    allowed_entrypoints:
      - web
    auth:
      headers:
        Authorization: "Bearer mytoken"
        X-Api-Key: "my-api-key"
`)
	cfg, err := config.Load(path)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	d := cfg.Downstreams[0]
	if d.Auth == nil {
		t.Fatal("auth must be populated")
	}
	if d.Auth.Headers["Authorization"] != "Bearer mytoken" {
		t.Errorf("Authorization header: got %q, want %q", d.Auth.Headers["Authorization"], "Bearer mytoken")
	}
	if d.Auth.Headers["X-Api-Key"] != "my-api-key" {
		t.Errorf("X-Api-Key header: got %q, want %q", d.Auth.Headers["X-Api-Key"], "my-api-key")
	}
}

// auth.username set without auth.password must fail at startup.
func TestLoad_AuthUsernameWithoutPassword_ReturnsError(t *testing.T) {
	path := writeYAML(t, `
edge_entrypoints:
  - websecure
downstreams:
  - name: primary
    api_address: http://traefik:8080
    traffic_address: http://traefik:80
    allowed_entrypoints:
      - web
    auth:
      username: alice
`)
	_, err := config.Load(path)
	if err == nil {
		t.Fatal("expected error for username without password, got nil")
	}
	if !containsField(err, "username") && !containsField(err, "password") {
		t.Errorf("error must name username or password, got: %v", err)
	}
}

// auth.password set without auth.username must fail at startup.
func TestLoad_AuthPasswordWithoutUsername_ReturnsError(t *testing.T) {
	path := writeYAML(t, `
edge_entrypoints:
  - websecure
downstreams:
  - name: primary
    api_address: http://traefik:8080
    traffic_address: http://traefik:80
    allowed_entrypoints:
      - web
    auth:
      password: secret
`)
	_, err := config.Load(path)
	if err == nil {
		t.Fatal("expected error for password without username, got nil")
	}
	if !containsField(err, "username") && !containsField(err, "password") {
		t.Errorf("error must name username or password, got: %v", err)
	}
}

// tls.cert set without tls.key must fail at startup with a message naming the fields.
func TestLoad_TLSCertWithoutKey_ReturnsError(t *testing.T) {
	path := writeYAML(t, `
edge_entrypoints:
  - websecure
downstreams:
  - name: primary
    api_address: http://traefik:8080
    traffic_address: http://traefik:80
    allowed_entrypoints:
      - web
    tls:
      cert: /etc/ssl/client.crt
`)
	_, err := config.Load(path)
	if err == nil {
		t.Fatal("expected error for cert without key, got nil")
	}
	if !containsField(err, "cert") && !containsField(err, "key") {
		t.Errorf("error must name cert or key, got: %v", err)
	}
}

// tls.key set without tls.cert must fail at startup with a message naming the fields.
func TestLoad_TLSKeyWithoutCert_ReturnsError(t *testing.T) {
	path := writeYAML(t, `
edge_entrypoints:
  - websecure
downstreams:
  - name: primary
    api_address: http://traefik:8080
    traffic_address: http://traefik:80
    allowed_entrypoints:
      - web
    tls:
      key: /etc/ssl/client.key
`)
	_, err := config.Load(path)
	if err == nil {
		t.Fatal("expected error for key without cert, got nil")
	}
	if !containsField(err, "cert") && !containsField(err, "key") {
		t.Errorf("error must name cert or key, got: %v", err)
	}
}

// The shipped config.example.yaml must load cleanly against the real loader
// and must exercise every optional downstream field category, so the file
// doubles as living schema documentation: if the loader gains a field or the
// example drifts, this fails before release.
func TestLoad_ExampleConfig(t *testing.T) {
	cfg, err := config.Load(filepath.Join("..", "..", "config.example.yaml"))
	if err != nil {
		t.Fatalf("config.example.yaml must load without validation errors: %v", err)
	}

	if len(cfg.Downstreams) < 1 {
		t.Fatal("config.example.yaml must contain at least one downstream")
	}
	// At least one downstream must exercise each optional field category, so
	// the file doubles as schema documentation.
	var seenAuth, seenTLS, seenPriority, seenStaleness bool
	for _, d := range cfg.Downstreams {
		seenAuth = seenAuth || d.Auth != nil
		seenTLS = seenTLS || d.TLS != nil
		seenPriority = seenPriority || d.PriorityOffset != 0
		seenStaleness = seenStaleness || d.StalenessLimit > 0
	}
	if !seenAuth {
		t.Error("example must demonstrate auth on at least one downstream")
	}
	if !seenTLS {
		t.Error("example must demonstrate tls on at least one downstream")
	}
	if !seenPriority {
		t.Error("example must demonstrate a non-zero priority_offset on at least one downstream")
	}
	if !seenStaleness {
		t.Error("example must demonstrate a staleness_limit on at least one downstream")
	}
	for i, d := range cfg.Downstreams {
		if len(d.AllowedEntrypoints) == 0 {
			t.Errorf("downstream[%d] (%q): example must demonstrate allowed_entrypoints", i, d.Name)
		}
	}
}

// auth.headers and auth.username/password cannot both be set.
func TestLoad_AuthHeadersAndBasicAuth_ReturnsError(t *testing.T) {
	path := writeYAML(t, `
edge_entrypoints:
  - websecure
downstreams:
  - name: primary
    api_address: http://traefik:8080
    traffic_address: http://traefik:80
    allowed_entrypoints:
      - web
    auth:
      headers:
        X-Api-Key: mykey
      username: alice
      password: secret
`)
	_, err := config.Load(path)
	if err == nil {
		t.Fatal("expected error for headers combined with username/password, got nil")
	}
	if !containsField(err, "headers") && !containsField(err, "username") {
		t.Errorf("error must name headers or username, got: %v", err)
	}
}
