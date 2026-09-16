package config_test

import (
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

	// Verify the type so callers can use cfg.PollInterval directly as time.Duration.
	var _ time.Duration = cfg.PollInterval
	var _ time.Duration = cfg.RequestTimeout
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

func containsField(err error, field string) bool {
	return err != nil && strings.Contains(err.Error(), field)
}
