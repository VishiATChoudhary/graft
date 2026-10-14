//go:build cloud

package repo

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/VishiATChoudhary/graft/internal/store"
)

func init() {
	spannerBackend = func(t *testing.T, blobs bool) store.Backend {
		// GRAFT_SPANNER_INSTANCE=projects/P/instances/I runs against real
		// Cloud Spanner: a fresh database per test, dropped afterwards.
		if inst := os.Getenv("GRAFT_SPANNER_INSTANCE"); inst != "" {
			ctx := context.Background()
			name := fmt.Sprintf("t%d%04d", time.Now().Unix()%1000000, rand.Intn(10000))
			db, err := store.CreateSpannerDatabase(ctx, inst, name)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { store.DropSpannerDatabase(context.Background(), db) })
			url := ""
			if blobs {
				url = "file://" + filepath.ToSlash(t.TempDir())
			}
			s, err := store.OpenSpanner(ctx, db, 64<<20, url)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { s.Close() })
			return s
		}
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
