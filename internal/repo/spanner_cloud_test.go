//go:build cloud

package repo

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/VishiATChoudhary/graft/internal/store"
)

func init() {
	spannerBackend = func(t *testing.T, blobs bool) store.Backend {
		url := ""
		if blobs { // file contents in a bucket, as with cloud storage
			dir := filepath.ToSlash(t.TempDir())
			if !strings.HasPrefix(dir, "/") {
				dir = "/" + dir // file:///C:/...
			}
			url = "file://" + dir
		}
		s, closeFn, err := store.OpenFakeSpanner(url)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(closeFn)
		return s
	}
}
