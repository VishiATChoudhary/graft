//go:build cloud

package store

import (
	"fmt"
	"math/rand"
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

// TestSpannerDeltas checks versions are stored as deltas when smaller and
// read back exactly with cold caches, through a chain.
func TestSpannerDeltas(t *testing.T) {
	for _, blobs := range []bool{false, true} {
		url := ""
		if blobs {
			dir := filepath.ToSlash(t.TempDir())
			if !strings.HasPrefix(dir, "/") {
				dir = "/" + dir
			}
			url = "file://" + dir
		}
		s, closeFn, err := OpenFakeSpanner(url)
		if err != nil {
			t.Fatal(err)
		}
		rng := rand.New(rand.NewSource(1))
		var sb strings.Builder
		for i := 0; i < 400; i++ {
			fmt.Fprintf(&sb, "func f%d(x int) int { return x*%d + %d }\n", rng.Intn(1e6), rng.Intn(1e6), rng.Intn(1e6))
		}
		content := []byte(sb.String())
		var ids []object.ID
		prev := object.Zero
		for v := 0; v < 40; v++ {
			content = append(content, []byte("version line "+strings.Repeat("x", v)+"\n")...)
			id, stored := object.Encode(object.KindBlob, content)
			if err := s.PutBase(id, stored, prev); err != nil {
				t.Fatal(err)
			}
			ids = append(ids, id)
			prev = id
		}
		rec, err := s.record(ids[len(ids)-1])
		if err != nil {
			t.Fatal(err)
		}
		_, full := object.Encode(object.KindBlob, content)
		if !isDeltaRec(rec) || len(rec) >= len(full)/4 {
			t.Fatalf("last version not stored as a small delta: %d bytes vs %d full", len(rec), len(full))
		}
		s.hot, s.payloads = NewMemLRU(1), NewMemLRU(1) // cold reads
		for _, id := range ids {
			b, err := s.Get(id)
			if err != nil {
				t.Fatal(err)
			}
			if _, _, err := object.Verify(id, b); err != nil {
				t.Fatalf("version %s: %v", id.Short(), err)
			}
		}
		closeFn()
	}
}
