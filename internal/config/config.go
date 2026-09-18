package config

import (
	"fmt"
	"net/url"
	"os"
	"regexp"
	"slices"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

var validName = regexp.MustCompile(`^[a-zA-Z0-9_-]+$`)

// supportedLogLevels lists the accepted log_level values, matching the
// slog level names the App's logger understands.
var supportedLogLevels = []string{"debug", "info", "warn", "error"}

func validLogLevel(s string) bool {
	return slices.Contains(supportedLogLevels, s)
}

const (
	DefaultListen          = ":8080"
	DefaultConfigPath      = "/config"
	DefaultLogLevel        = "info"
	DefaultPollInterval    = 30 * time.Second
	DefaultRequestTimeout  = 5 * time.Second
	DefaultMaxResponseSize = int64(10 * 1024 * 1024) // 10 MiB

	// PathHealthz, PathReadyz, and PathMetrics are the endpoint paths the
	// App's Handler always registers alongside the configured config_path.
	// validate rejects a config_path that collides with any of them; app.go's
	// Handler registers them under these same names, so the two stay in sync.
	PathHealthz = "/healthz"
	PathReadyz  = "/readyz"
	PathMetrics = "/metrics"
)

// ReservedPaths lists the endpoint paths config_path must not collide with.
var ReservedPaths = []string{PathHealthz, PathReadyz, PathMetrics}

// Config holds the complete, validated application configuration.
type Config struct {
	Listen          string `yaml:"listen"`
	ConfigPath      string `yaml:"config_path"`
	PollInterval    time.Duration
	RequestTimeout  time.Duration
	MaxResponseSize int64
	EdgeEntrypoints []string
	LogLevel        string
	Downstreams     []Downstream
}

// Downstream describes one upstream Traefik instance.
type Downstream struct {
	Name               string
	APIAddress         string
	TrafficAddress     string
	AllowedEntrypoints []string
	Auth               *Auth
	TLS                *TLS
	PriorityOffset     int
	StalenessLimit     time.Duration
}

// Auth holds credentials for accessing a downstream's Traefik API.
type Auth struct {
	Username string            `yaml:"username"`
	Password string            `yaml:"password"`
	Headers  map[string]string `yaml:"headers"`
}

// TLS holds TLS configuration for connecting to a downstream's Traefik API.
type TLS struct {
	CA                 string `yaml:"ca"`
	Cert               string `yaml:"cert"`
	Key                string `yaml:"key"`
	InsecureSkipVerify bool   `yaml:"insecure_skip_verify"`
}

// rawConfig mirrors Config with duration fields as plain strings so that
// yaml.v3 decodes them without interpretation; we parse them ourselves to
// produce error messages that name the offending field.
type rawConfig struct {
	Listen          string          `yaml:"listen"`
	ConfigPath      string          `yaml:"config_path"`
	PollInterval    string          `yaml:"poll_interval"`
	RequestTimeout  string          `yaml:"request_timeout"`
	MaxResponseSize int64           `yaml:"max_response_size"`
	EdgeEntrypoints []string        `yaml:"edge_entrypoints"`
	LogLevel        string          `yaml:"log_level"`
	Downstreams     []rawDownstream `yaml:"downstreams"`
}

type rawDownstream struct {
	Name               string   `yaml:"name"`
	APIAddress         string   `yaml:"api_address"`
	TrafficAddress     string   `yaml:"traffic_address"`
	AllowedEntrypoints []string `yaml:"allowed_entrypoints"`
	Auth               *Auth    `yaml:"auth"`
	TLS                *TLS     `yaml:"tls"`
	PriorityOffset     int      `yaml:"priority_offset"`
	StalenessLimit     string   `yaml:"staleness_limit"`
}

func rawDefaults() *rawConfig {
	return &rawConfig{
		Listen:          DefaultListen,
		ConfigPath:      DefaultConfigPath,
		LogLevel:        DefaultLogLevel,
		PollInterval:    DefaultPollInterval.String(),
		RequestTimeout:  DefaultRequestTimeout.String(),
		MaxResponseSize: DefaultMaxResponseSize,
		EdgeEntrypoints: []string{},
	}
}

// Load reads the YAML file at path, applies defaults, and returns a validated Config.
func Load(path string) (*Config, error) {
	f, err := os.Open(path) //nolint:gosec // path is operator-supplied (CLI/config flag), not untrusted input
	if err != nil {
		return nil, fmt.Errorf("open config: %w", err)
	}
	defer func() { _ = f.Close() }()

	raw := rawDefaults()

	dec := yaml.NewDecoder(f)
	dec.KnownFields(true)
	if err := dec.Decode(raw); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}

	cfg, err := convert(raw)
	if err != nil {
		return nil, err
	}

	if err := validate(cfg); err != nil {
		return nil, err
	}

	return cfg, nil
}

// convert parses duration strings and builds the public Config type.
// Each error names the offending field.
func convert(r *rawConfig) (*Config, error) {
	pollInterval, err := parseDuration("poll_interval", r.PollInterval)
	if err != nil {
		return nil, err
	}
	requestTimeout, err := parseDuration("request_timeout", r.RequestTimeout)
	if err != nil {
		return nil, err
	}

	downstreams := make([]Downstream, len(r.Downstreams))
	for i, rd := range r.Downstreams {
		// zero means no limit; callers should treat a zero StalenessLimit as unlimited.
		sl, err := parseOptionalDuration(fmt.Sprintf("downstream[%d].staleness_limit", i), rd.StalenessLimit)
		if err != nil {
			return nil, err
		}
		downstreams[i] = Downstream{
			Name:               rd.Name,
			APIAddress:         rd.APIAddress,
			TrafficAddress:     rd.TrafficAddress,
			AllowedEntrypoints: rd.AllowedEntrypoints,
			Auth:               rd.Auth,
			TLS:                rd.TLS,
			PriorityOffset:     rd.PriorityOffset,
			StalenessLimit:     sl,
		}
	}

	return &Config{
		Listen:          r.Listen,
		ConfigPath:      r.ConfigPath,
		PollInterval:    pollInterval,
		RequestTimeout:  requestTimeout,
		MaxResponseSize: r.MaxResponseSize,
		EdgeEntrypoints: r.EdgeEntrypoints,
		LogLevel:        r.LogLevel,
		Downstreams:     downstreams,
	}, nil
}

func parseDuration(field, s string) (time.Duration, error) {
	d, err := time.ParseDuration(s)
	if err != nil {
		return 0, fmt.Errorf("%s: invalid duration %q: %w", field, s, err)
	}
	return d, nil
}

// parseOptionalDuration allows an empty string (meaning "not set") and returns
// zero in that case, which callers interpret as "no limit".
func parseOptionalDuration(field, s string) (time.Duration, error) {
	if s == "" {
		return 0, nil
	}
	return parseDuration(field, s)
}

func validate(cfg *Config) error {
	if cfg.PollInterval <= 0 {
		return fmt.Errorf("poll_interval must be positive, got %s", cfg.PollInterval)
	}
	if cfg.RequestTimeout <= 0 {
		return fmt.Errorf("request_timeout must be positive, got %s", cfg.RequestTimeout)
	}
	if cfg.MaxResponseSize <= 0 {
		return fmt.Errorf("max_response_size must be positive, got %d", cfg.MaxResponseSize)
	}
	if !strings.HasPrefix(cfg.ConfigPath, "/") {
		return fmt.Errorf("config_path %q must be an absolute path starting with /", cfg.ConfigPath)
	}
	if slices.Contains(ReservedPaths, cfg.ConfigPath) {
		return fmt.Errorf("config_path %q collides with a reserved endpoint (%s)", cfg.ConfigPath, strings.Join(ReservedPaths, ", "))
	}
	if len(cfg.EdgeEntrypoints) == 0 {
		return fmt.Errorf("edge_entrypoints: at least one entrypoint is required, got none")
	}
	if !validLogLevel(cfg.LogLevel) {
		return fmt.Errorf("log_level %q is not a supported level (supported: %s)", cfg.LogLevel, strings.Join(supportedLogLevels, ", "))
	}
	seen := make(map[string]int, len(cfg.Downstreams))
	for i, d := range cfg.Downstreams {
		if !validName.MatchString(d.Name) {
			return fmt.Errorf("downstream[%d]: name %q contains invalid characters (allowed: a-z, A-Z, 0-9, _, -)", i, d.Name)
		}
		if err := requireAbsoluteURL("api_address", d.APIAddress); err != nil {
			return fmt.Errorf("downstream[%d] (%q): %w", i, d.Name, err)
		}
		if err := requireAbsoluteURL("traffic_address", d.TrafficAddress); err != nil {
			return fmt.Errorf("downstream[%d] (%q): %w", i, d.Name, err)
		}
		if len(d.AllowedEntrypoints) == 0 {
			return fmt.Errorf("downstream[%d] (%q): allowed_entrypoints: at least one entrypoint is required; an empty list must not be treated as \"publish all\"", i, d.Name)
		}
		if d.Auth != nil {
			if (d.Auth.Username == "") != (d.Auth.Password == "") {
				return fmt.Errorf("downstream[%d] (%q): auth.username and auth.password must both be set together", i, d.Name)
			}
			if len(d.Auth.Headers) > 0 && d.Auth.Username != "" {
				return fmt.Errorf("downstream[%d] (%q): auth.headers and auth.username/password are mutually exclusive", i, d.Name)
			}
		}
		if d.TLS != nil {
			if (d.TLS.Cert == "") != (d.TLS.Key == "") {
				return fmt.Errorf("downstream[%d] (%q): tls.cert and tls.key must both be set together", i, d.Name)
			}
		}
		if prev, ok := seen[d.Name]; ok {
			return fmt.Errorf("downstream[%d]: name %q duplicates downstream[%d]", i, d.Name, prev)
		}
		seen[d.Name] = i
	}
	return nil
}

// requireAbsoluteURL returns an error when s is empty or is not an absolute URL
// (scheme + host required).  Malformed addresses caught here produce a startup
// error rather than a runtime fetch failure after deploy.
func requireAbsoluteURL(field, s string) error {
	if s == "" {
		return fmt.Errorf("%s is required", field)
	}
	u, err := url.Parse(s)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return fmt.Errorf("%s %q must be an absolute URL (scheme://host[:port])", field, s)
	}
	return nil
}
