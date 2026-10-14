//go:build cloud

package store

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/VishiATChoudhary/graft/internal/object"
)

// TestSpannerBlobsInBucket checks file contents go to the bucket and
// everything else to Spanner rows.
func TestSpannerBlobsInBucket(t *testing.T) {
	dir := filepath.ToSlash(t.TempDir())
	if !strings.HasPrefix(dir, "/") {
		dir = "/" + dir
	}
	s, closeFn, err := OpenFakeSpanner("file://" + dir)
	if err != nil {
		t.Fatal(err)
	}
	defer closeFn()
	bid, err := WritePayload(s, object.KindBlob, []byte("hello, bucket\n"))
	if err != nil {
		t.Fatal(err)
	}
	tid, err := WritePayload(s, object.KindTree, (&object.Tree{Entries: []object.TreeEntry{{Name: "f", Mode: object.ModeFile, ID: bid}}}).Encode())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(filepath.FromSlash(dir), blobKey(bid))); err != nil {
		t.Fatalf("blob not in bucket: %v", err)
	}
	if _, err := os.Stat(filepath.Join(filepath.FromSlash(dir), blobKey(tid))); err == nil {
		t.Fatal("tree stored in the bucket")
	}
	// A fresh client (empty caches) reads both back.
	s.hot, s.payloads = NewMemLRU(1<<20), NewMemLRU(1<<20)
	for _, id := range []object.ID{bid, tid} {
		b, err := s.Get(id)
		if err != nil {
			t.Fatal(err)
		}
		if _, _, err := object.Verify(id, b); err != nil {
			t.Fatal(err)
		}
	}
}
