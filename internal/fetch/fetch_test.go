package fetch

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/VishiATChoudhary/graft/internal/object"
	"github.com/VishiATChoudhary/graft/internal/store"
)

type countingSource struct {
	inner Source
	calls atomic.Int64
	ids   atomic.Int64
}

func (c *countingSource) Fetch(ctx context.Context, ids []object.ID, fn func(object.ID, []byte) error) error {
	c.calls.Add(1)
	c.ids.Add(int64(len(ids)))
	time.Sleep(20 * time.Millisecond) // make overlap likely
	return c.inner.Fetch(ctx, ids, fn)
}

func seed(n int) (*store.MemLRU, []object.ID) {
	s := store.NewMemLRU(1 << 30)
	var ids []object.ID
	for i := 0; i < n; i++ {
		id, b := object.Encode(object.KindBlob, []byte{byte(i), byte(i >> 8), 'x'})
		s.Put(id, b)
		ids = append(ids, id)
	}
	return s, ids
}

func TestTierCoalescesConcurrentMisses(t *testing.T) {
	origin, ids := seed(1000)
	up := &countingSource{inner: StoreSource{Store: origin}}
	tier := NewTier(store.NewMemLRU(1<<30), up)
	var wg sync.WaitGroup
	for j := 0; j < 32; j++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			n := 0
			if err := tier.Fetch(context.Background(), ids, func(object.ID, []byte) error { n++; return nil }); err != nil || n != len(ids) {
				t.Errorf("fetch: %v, got %d", err, n)
			}
		}()
	}
	wg.Wait()
	if got := up.ids.Load(); got != int64(len(ids)) {
		t.Fatalf("upstream asked for %d ids; want each of %d exactly once", got, len(ids))
	}
}

func TestHTTPTiersAndMissing(t *testing.T) {
	origin, ids := seed(5000)
	srv := httptest.NewServer(http.HandlerFunc(Handler(StoreSource{Store: origin})))
	defer srv.Close()
	cache := NewTier(store.NewMemLRU(1<<30), NewRemote(srv.URL))
	csrv := httptest.NewServer(http.HandlerFunc(Handler(cache)))
	defer csrv.Close()
	client := NewRemote(csrv.URL)

	got := map[object.ID]bool{}
	err := client.Fetch(context.Background(), ids, func(id object.ID, b []byte) error {
		if _, _, err := object.Verify(id, b); err != nil {
			return err
		}
		got[id] = true
		return nil
	})
	if err != nil || len(got) != len(ids) {
		t.Fatalf("fetch through cache: %v (%d/%d)", err, len(got), len(ids))
	}
	bogus := object.Hash(object.KindBlob, []byte("nope"))
	err = client.Fetch(context.Background(), []object.ID{ids[0], bogus}, func(object.ID, []byte) error { return nil })
	var me *MissingError
	if !errors.As(err, &me) || len(me.IDs) != 1 || me.IDs[0] != bogus {
		t.Fatalf("expected missing error for bogus id, got %v", err)
	}
}
