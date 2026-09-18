package imagetags_test

import (
	"slices"
	"testing"

	"github.com/gringolito/traefik-scout/internal/imagetags"
)

const (
	latest = "latest"
	v12    = "v1.2"
)

// Cycle 1: tagging the newest release in every series must publish the exact
// version tag plus its major.minor and major aliases and latest, all at the
// same digest, so the workflow pushes them as one tag list.
func TestTags_NewestReleaseGetsAllAliases(t *testing.T) {
	got, err := imagetags.Tags("v1.2.3", []string{"v1.2.2", "v1.1.0", "v0.9.0"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	want := []string{"v1.2.3", v12, "v1", latest}
	if !slices.Equal(got, want) {
		t.Errorf("Tags(v1.2.3) = %v, want %v", got, want)
	}
}

// Cycle 2: publishing an older-series backport (e.g. a v1 patch while v2.3.1
// is out) must not move the newer major tag or latest backward. The backport
// still owns the minor tag of its own series if it is the newest patch there.
func TestTags_BackportDoesNotMoveNewerAliases(t *testing.T) {
	got, err := imagetags.Tags("v1.2.9", []string{"v2.3.1", "v1.2.4", "v1.2.10"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// v1.2.10 is already published, so v1.2.9 is not even the newest patch in
	// its own minor series: no aliases at all.
	want := []string{"v1.2.9"}
	if !slices.Equal(got, want) {
		t.Errorf("Tags(v1.2.9) = %v, want %v", got, want)
	}

	got, err = imagetags.Tags("v1.2.11", []string{"v2.3.1", "v1.2.4", "v1.2.10"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Newest patch in the v1.2 series and the newest v1.x overall, so it
	// moves v1.2 and v1 forward. But v2.3.1 outranks it, so latest is never
	// touched.
	want = []string{"v1.2.11", "v1.2", "v1"}
	if !slices.Equal(got, want) {
		t.Errorf("Tags(v1.2.11) = %v, want %v", got, want)
	}
}

// Cycle 3: latest reflects the newest release by semver, not by chronological
// tag date. Publishing v2.0.0 after v1.9.9 must move latest forward; the same
// comparison governs every alias.
func TestTags_LatestFollowsSemverNotTagDate(t *testing.T) {
	// Chronologically latest published tag is v1.9.9, but v2.0.0 is greater.
	got, err := imagetags.Tags("v2.0.0", []string{"v1.9.9", "v1.0.0"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// v2.0.0 is the newest release in the v2 series (and overall), so it
	// seeds the fresh v2.0 and v2 aliases and moves latest forward.
	want := []string{"v2.0.0", "v2.0", "v2", latest}
	if !slices.Equal(got, want) {
		t.Errorf("Tags(v2.0.0) = %v, want %v", got, want)
	}
}

// Cycle 4: unparseable v-prefixed tags can exist in the registry for reasons
// outside this workflow; they carry no semver, must never block an alias
// decision, and the new tag itself must still be strictly validated.
func TestTags_TagValidation(t *testing.T) {
	// Junk in the existing set is ignored.
	got, err := imagetags.Tags("v1.2.3", []string{"vlatest", "v1", "edge", "sha-abc123"})
	if err != nil {
		t.Fatalf("unexpected error for junk existing tags: %v", err)
	}

	want := []string{"v1.2.3", v12, "v1", latest}
	if !slices.Equal(got, want) {
		t.Errorf("Tags(v1.2.3) = %v, want %v", got, want)
	}

	// The tag being published must be a full X.Y.Z semver: aliases computed
	// from a partial version would be wrong, so refuse.
	if _, err := imagetags.Tags("v1.2", nil); err == nil {
		t.Error("expected error for partial version tag v1.2, got nil")
	}
	if _, err := imagetags.Tags("banana", nil); err == nil {
		t.Error("expected error for non-semver tag banana, got nil")
	}
}
