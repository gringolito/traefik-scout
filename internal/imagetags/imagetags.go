// Package imagetags decides which registry tags a release build publishes.
//
// A release tag vX.Y.Z always publishes itself, plus the vX.Y and vX aliases
// and latest, but only when it is the newest release in the corresponding
// series. Publishing an older patch release (a backport) must never move a
// newer major/minor alias or latest backward, so the existing published
// release tags are compared by semver before any alias is added.
package imagetags

import (
	"fmt"
	"strconv"
	"strings"
)

// Tags returns the registry tags to publish for newTag, given the set of
// already-published tags on the image. The exact newTag is always included;
// vMAJOR.MINOR, vMAJOR, and latest are included only when newTag is greater
// than or equal to every already-published release in that series (or overall,
// for latest). Tags that do not parse as full vX.Y.Z semver in existing are
// ignored; a non-semver newTag is an error.
func Tags(newTag string, existing []string) ([]string, error) {
	newVersion, err := parseVersion(newTag)
	if err != nil {
		return nil, fmt.Errorf("new tag: %w", err)
	}

	var maxSameMinor, maxSameMajor, maxOverall *version
	for _, tag := range existing {
		ver, err := parseVersion(tag)
		if err != nil {
			continue
		}
		if ver.sameMinor(newVersion) && (maxSameMinor == nil || ver.atLeast(maxSameMinor)) {
			maxSameMinor = &ver
		}
		if ver.sameMajor(newVersion) && (maxSameMajor == nil || ver.atLeast(maxSameMajor)) {
			maxSameMajor = &ver
		}
		if maxOverall == nil || ver.atLeast(maxOverall) {
			maxOverall = &ver
		}
	}

	tags := []string{newTag}
	if maxSameMinor == nil || newVersion.atLeast(maxSameMinor) {
		tags = append(tags, newVersion.alias(2))
	}
	if maxSameMajor == nil || newVersion.atLeast(maxSameMajor) {
		tags = append(tags, newVersion.alias(1))
	}
	if maxOverall == nil || newVersion.atLeast(maxOverall) {
		tags = append(tags, "latest")
	}
	return tags, nil
}

// version is a parsed vX.Y.Z semver with an optional prerelease.
type version struct {
	major, minor, patch uint64
	pre                 string
}

func parseVersion(tag string) (version, error) {
	rest, ok := strings.CutPrefix(tag, "v")
	if !ok {
		return version{}, fmt.Errorf("%q: not a v-prefixed tag", tag)
	}
	rest, _, _ = strings.Cut(rest, "+") // build metadata never affects precedence
	core, pre, _ := strings.Cut(rest, "-")

	parts := strings.Split(core, ".")
	if len(parts) != 3 {
		return version{}, fmt.Errorf("%q: want vX.Y.Z, got %q", tag, core)
	}
	numbers := make([]uint64, 3)
	for i, part := range parts {
		n, err := strconv.ParseUint(part, 10, 64)
		if err != nil {
			return version{}, fmt.Errorf("%q: invalid version component %q", tag, part)
		}
		numbers[i] = n
	}
	return version{major: numbers[0], minor: numbers[1], patch: numbers[2], pre: pre}, nil
}

// atLeast reports whether v has semver precedence greater than or equal to
// other, per the prerelease rules of semver.org (a release outranks any
// prerelease of the same core version).
func (v version) atLeast(other *version) bool {
	if v.major != other.major {
		return v.major > other.major
	}
	if v.minor != other.minor {
		return v.minor > other.minor
	}
	if v.patch != other.patch {
		return v.patch > other.patch
	}
	// Equal core: a release (no prerelease) outranks a prerelease.
	if v.pre == "" || other.pre == "" {
		return v.pre == ""
	}
	return comparePrerelease(v.pre, other.pre) >= 0
}

func comparePrerelease(a, b string) int {
	as, bs := strings.Split(a, "."), strings.Split(b, ".")
	for i := 0; i < len(as) && i < len(bs); i++ {
		if c := comparePrereleaseIdent(as[i], bs[i]); c != 0 {
			return c
		}
	}
	switch {
	case len(as) < len(bs):
		return -1
	case len(as) > len(bs):
		return 1
	default:
		return 0
	}
}

func comparePrereleaseIdent(a, b string) int {
	an, aerr := strconv.ParseUint(a, 10, 64)
	bn, berr := strconv.ParseUint(b, 10, 64)
	switch {
	case aerr == nil && berr == nil: // numeric identifiers compare numerically
	case aerr == nil: // numeric identifiers rank below alphanumeric ones
		return -1
	case berr == nil:
		return 1
	default: // both alphanumeric: lexical ASCII order
		return strings.Compare(a, b)
	}
	switch {
	case an < bn:
		return -1
	case an > bn:
		return 1
	default:
		return 0
	}
}

func (v version) sameMajor(other version) bool {
	return v.major == other.major
}

func (v version) sameMinor(other version) bool {
	return v.major == other.major && v.minor == other.minor
}

// alias renders the tag keeping the leading n version components.
func (v version) alias(n int) string {
	parts := []string{strconv.FormatUint(v.major, 10)}
	if n >= 2 {
		parts = append(parts, strconv.FormatUint(v.minor, 10))
	}
	return "v" + strings.Join(parts, ".")
}
