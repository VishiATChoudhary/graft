package store

import (
	"bytes"
	"fmt"
	"math/rand"
	"testing"

	"github.com/VishiATChoudhary/graft/internal/object"
)

// obj returns an incompressible object unique to i.
func obj(i int, size int) (object.ID, []byte) {
	b := make([]byte, size)
	rand.New(rand.NewSource(int64(i))).Read(b)
	copy(b, fmt.Sprint(i))
	return object.Encode(object.KindBlob, b)
}

func TestDiskCacheReopenAndTornWrite(t *testing.T) {
	dir := t.TempDir()
	c, err := OpenDiskCache(dir, 0)
	if err != nil {
		t.Fatal(err)
	}
	ids := map[object.ID][]byte{}
	for i := 0; i < 500; i++ {
		id, b := obj(i, 1000)
		ids[id] = b
		if err := c.Put(id, b); err != nil {
			t.Fatal(err)
		}
	}
	// Simulate a crash mid-append: a partial record at the end.
	c.active.f.WriteAt([]byte("partial-record"), c.active.size)
	c.Close()

	c2, err := OpenDiskCache(dir, 0)
	if err != nil {
		t.Fatal(err)
	}
	for id, want := range ids {
		got, err := c2.Get(id)
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("lost %s after reopen: %v", id.Short(), err)
		}
	}
	id, b := obj(9999, 100)
	if err := c2.Put(id, b); err != nil {
		t.Fatal(err)
	}
	if got, _ := c2.Get(id); !bytes.Equal(got, b) {
		t.Fatal("append after torn record")
	}
}

func TestDiskCacheBoundedWithPromotion(t *testing.T) {
	c, err := OpenDiskCache(t.TempDir(), 8<<20) // 1 MiB segments
	if err != nil {
		t.Fatal(err)
	}
	hotID, hot := obj(-1, 4000)
	c.Put(hotID, hot)
	for i := 0; i < 20000; i++ {
		id, b := obj(i, 4000)
		c.Put(id, b)
		if i%500 == 0 {
			if _, err := c.Get(hotID); err != nil {
				t.Fatalf("hot object evicted at %d", i)
			}
		}
	}
	_, _, bytes, _ := c.Stats()
	if bytes > 8<<20 {
		t.Fatalf("cache exceeds budget: %d", bytes)
	}
	if _, err := c.Get(hotID); err != nil {
		t.Fatal("hot object should survive via promotion")
	}
	first, _ := obj(0, 4000)
	if ok, _ := c.Has(first); ok {
		t.Fatal("cold object should have been evicted")
	}
}

func TestMemLRUBudget(t *testing.T) {
	m := NewMemLRU(1 << 20)
	for i := 0; i < 10000; i++ {
		id, b := obj(i, 1000)
		m.Put(id, b)
	}
	_, _, n := m.Stats()
	if n > 1<<20 {
		t.Fatalf("over budget: %d", n)
	}
}

// TestDeltaChainsStayBounded writes many versions of one object family and
// checks every stored chain respects the depth cap (regression: a byte
// depth counter once wrapped at 255, making chains unbounded).
func TestDeltaChainsStayBounded(t *testing.T) {
	db, err := OpenDB(t.TempDir(), 32<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var prev object.ID
	var ids []object.ID
	payload := bytes.Repeat([]byte("entry-name 0123456789abcdef0123456789abcdef\n"), 40)
	for i := 0; i < 700; i++ {
		p := append([]byte(fmt.Sprintf("version %d\n", i)), payload...)
		id, stored := object.Encode(object.KindTree, p)
		if err := db.PutBase(id, stored, prev); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
		prev = id
	}
	for _, id := range ids {
		hops := 0
		for cur := id; ; hops++ {
			b, err := db.raw(cur)
			if err != nil {
				t.Fatal(err)
			}
			r, _ := parseRec(b)
			if r.codec != codecDelta {
				break
			}
			cur = r.base
		}
		if hops > maxDeltaDepth {
			t.Fatalf("chain of %d deltas exceeds the cap of %d", hops, maxDeltaDepth)
		}
	}
	db.payloads = NewMemLRU(1 << 20) // force real chain walks
	for _, id := range []object.ID{ids[0], ids[350], ids[699]} {
		if _, _, err := db.Payload(id); err != nil {
			t.Fatal(err)
		}
	}
}
