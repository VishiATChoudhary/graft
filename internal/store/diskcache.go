package store

import (
	"container/list"
	"encoding/hex"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/VishiATChoudhary/graft/internal/object"
)

// DiskCache is a bounded on-disk object store with LRU eviction. Clients and
// node daemons use it: they keep a bounded cache rather than a full copy of
// the repository, and need no maintenance (no packing, no gc).
type DiskCache struct {
	dir    string
	budget int64

	mu    sync.Mutex
	items map[object.ID]*list.Element
	order list.List
	bytes int64

	hits, misses atomic.Uint64
}

type diskItem struct {
	id   object.ID
	size int64
}

// OpenDiskCache opens (or creates) a cache directory. budget <= 0 means
// unbounded.
func OpenDiskCache(dir string, budget int64) (*DiskCache, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	c := &DiskCache{dir: dir, budget: budget, items: make(map[object.ID]*list.Element)}
	type found struct {
		id   object.ID
		size int64
		mod  int64
	}
	var all []found
	err := filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		name := filepath.Base(filepath.Dir(p)) + d.Name()
		if len(name) != 64 {
			return nil
		}
		var id object.ID
		if _, err := hex.Decode(id[:], []byte(name)); err != nil {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return nil
		}
		all = append(all, found{id, info.Size(), info.ModTime().UnixNano()})
		return nil
	})
	if err != nil {
		return nil, err
	}
	// Oldest at the back.
	sort.Slice(all, func(i, j int) bool { return all[i].mod > all[j].mod })
	for _, f := range all {
		c.items[f.id] = c.order.PushBack(&diskItem{f.id, f.size})
		c.bytes += f.size
	}
	c.mu.Lock()
	c.evictLocked()
	c.mu.Unlock()
	return c, nil
}

func (c *DiskCache) path(id object.ID) string {
	h := id.String()
	return filepath.Join(c.dir, h[:2], h[2:])
}

func (c *DiskCache) Get(id object.ID) ([]byte, error) {
	c.mu.Lock()
	e, ok := c.items[id]
	if ok {
		c.order.MoveToFront(e)
	}
	c.mu.Unlock()
	if !ok {
		c.misses.Add(1)
		return nil, object.ErrNotFound
	}
	b, err := os.ReadFile(c.path(id))
	if errors.Is(err, fs.ErrNotExist) {
		c.mu.Lock()
		if e, ok := c.items[id]; ok {
			c.order.Remove(e)
			delete(c.items, id)
			c.bytes -= e.Value.(*diskItem).size
		}
		c.mu.Unlock()
		c.misses.Add(1)
		return nil, object.ErrNotFound
	}
	if err == nil {
		c.hits.Add(1)
	}
	return b, err
}

func (c *DiskCache) Has(id object.ID) (bool, error) {
	c.mu.Lock()
	_, ok := c.items[id]
	c.mu.Unlock()
	return ok, nil
}

func (c *DiskCache) Put(id object.ID, data []byte) error {
	c.mu.Lock()
	_, ok := c.items[id]
	c.mu.Unlock()
	if ok {
		return nil
	}
	p := c.path(id)
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(p), ".tmp-*")
	if err != nil {
		return err
	}
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		os.Remove(tmp.Name())
		return err
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmp.Name())
		return err
	}
	if err := os.Rename(tmp.Name(), p); err != nil {
		os.Remove(tmp.Name())
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.items[id]; ok {
		return nil
	}
	c.items[id] = c.order.PushFront(&diskItem{id, int64(len(data))})
	c.bytes += int64(len(data))
	c.evictLocked()
	return nil
}

func (c *DiskCache) evictLocked() {
	if c.budget <= 0 {
		return
	}
	for c.bytes > c.budget && c.order.Len() > 0 {
		back := c.order.Back()
		it := back.Value.(*diskItem)
		c.order.Remove(back)
		delete(c.items, it.id)
		c.bytes -= it.size
		os.Remove(c.path(it.id))
	}
}

func (c *DiskCache) Stats() (hits, misses uint64, bytes int64, count int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.hits.Load(), c.misses.Load(), c.bytes, len(c.items)
}
