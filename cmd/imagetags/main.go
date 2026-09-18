// Command imagetags prints the registry tags to publish for a release.
//
// Usage: imagetags <newTag> < existing-tags.txt
//
// The new release tag (e.g. v1.2.3) is the only argument; the already
// published tags are read from stdin, one per line. It writes the resolved
// tag list to stdout, one per line: the exact version tag plus the vX.Y, vX,
// and latest aliases when the new release is the newest in the corresponding
// semver series. The release workflow feeds this list to
// docker/build-push-action so every tag points at the same digest, and an
// out-of-order backport can never move a newer alias or latest backward.
//
// It exits non-zero if the new tag is not a full vX.Y.Z semver tag; junk in
// the stdin set is ignored.
package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/gringolito/traefik-scout/internal/imagetags"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: imagetags <newTag> < existing-tags.txt")
		os.Exit(2)
	}

	existing, err := readTags(os.Stdin)
	if err != nil {
		fmt.Fprintf(os.Stderr, "imagetags: %v\n", err)
		os.Exit(1)
	}

	tags, err := imagetags.Tags(os.Args[1], existing)
	if err != nil {
		fmt.Fprintf(os.Stderr, "imagetags: %v\n", err)
		os.Exit(1)
	}

	for _, tag := range tags {
		fmt.Println(tag)
	}
}

func readTags(r io.Reader) ([]string, error) {
	var tags []string
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		if tag := strings.TrimSpace(scanner.Text()); tag != "" {
			tags = append(tags, tag)
		}
	}
	return tags, scanner.Err()
}
