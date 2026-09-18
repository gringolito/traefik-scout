package cli

import "testing"

// loopbackTarget is the dial target every wildcard listen host maps to.
const loopbackTarget = "127.0.0.1:8080"

func TestLoopbackAddr(t *testing.T) {
	tests := []struct {
		name   string
		listen string
		want   string
	}{
		{"empty host dials loopback", ":8080", loopbackTarget},
		{"ipv4 wildcard dials loopback", "0.0.0.0:8080", loopbackTarget},
		{"ipv6 wildcard dials loopback", "[::]:8080", loopbackTarget},
		{"explicit ipv4 host kept", "10.0.0.5:8080", "10.0.0.5:8080"},
		{"explicit hostname kept", "scout.internal:8080", "scout.internal:8080"},
		{"explicit ipv6 loopback kept", "[::1]:8080", "[::1]:8080"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := loopbackAddr(tt.listen)
			if err != nil {
				t.Fatalf("loopbackAddr(%q): %v", tt.listen, err)
			}
			if got != tt.want {
				t.Errorf("loopbackAddr(%q) = %q, want %q", tt.listen, got, tt.want)
			}
		})
	}
}

func TestLoopbackAddr_UnparseableListen(t *testing.T) {
	if _, err := loopbackAddr("no-port-here"); err == nil {
		t.Error("loopbackAddr(\"no-port-here\"): got nil error, want error")
	}
}
