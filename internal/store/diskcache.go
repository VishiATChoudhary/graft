package store

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/VishiATChoudhary/graft/internal/object"
)

// DiskCache is a bounded on-disk object cache for clients and node daemons.
//
// Objects are appended to large segment files (no file per object, so
// caching 100k objects costs 100k appends, not 100k file creations). An
// in-memory index maps IDs to segment offsets. When the cache exceeds its
// budget the oldest segment is deleted wholesale; objects read from the
// oldest segments are re-appended first so hot data survives (segmented FIFO
// with promotion, a cheap LRU approximation). There is no packing, gc or
// other maintenance.
//
// Segment record: [32-byte id][u32 length][stored bytes]. Snapshot files
// produced by `gft snapshot` use the same format.
type DiskCache struct {
	dir     string
	budget  int64
	segSize int64

	mu       sync.RWMutex
	index    map[object.ID]loc
	segs     map[uint32]*segment
	order    []uint32 // oldest first
	active   *segment
	bytes    int64
	nextSeg  uint32
	hits     atomic.Uint64
	misses   atomic.Uint64
	promoted atomic.Uint64
}

type loc struct {
	seg uint32
	off int64
	n   uint32
}

type segment struct {
	id   uint32
	f    *os.File
	size int64
}

const recHdr = 36

// DefaultSegmentSize is the size at which a segment is sealed.
const DefaultSegmentSize = 64 << 20

// OpenDiskCache opens (or creates) a cache directory. budget <= 0 means
// unbounded.
func OpenDiskCache(dir string, budget int64) (*DiskCache, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	c := &DiskCache{dir: dir, budget: budget, segSize: DefaultSegmentSize, index: map[object.ID]loc{}, segs: map[uint32]*segment{}}
	if budget > 0 && budget/8 < c.segSize {
		c.segSize = max(budget/8, 1<<20)
	}
	ents, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var ids []uint32
	for _, e := range ents {
		name, ok := strings.CutSuffix(e.Name(), ".seg")
		if !ok {
			continue
		}
		v, err := strconv.ParseUint(name, 10, 32)
		if err != nil {
			continue
		}
		ids = append(ids, uint32(v))
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	for _, id := range ids {
		if err := c.loadSegment(id); err != nil {
			return nil, err
		}
		c.nextSeg = id + 1
	}
	if n := len(c.order); n > 0 {
		c.active = c.segs[c.order[n-1]]
	}
	c.mu.Lock()
	c.evictLocked()
	c.mu.Unlock()
	return c, nil
}

func (c *DiskCache) segPath(id uint32) string {
	return filepath.Join(c.dir, fmt.Sprintf("%08d.seg", id))
}

// loadSegment scans a segment, indexing its records and truncating a torn
// final record.
func (c *DiskCache) loadSegment(id uint32) error {
	f, err := os.OpenFile(c.segPath(id), os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	br := bufio.NewReaderSize(f, 1<<20)
	var off int64
	var hdr [recHdr]byte
	for {
		if _, err := io.ReadFull(br, hdr[:]); err != nil {
			break
		}
		n := binary.BigEndian.Uint32(hdr[32:])
		if _, err := br.Discard(int(n)); err != nil {
			break
		}
		var oid object.ID
		copy(oid[:], hdr[:32])
		c.index[oid] = loc{id, off + recHdr, n}
		off += recHdr + int64(n)
	}
	if err := f.Truncate(off); err != nil {
		f.Close()
		return err
	}
	s := &segment{id: id, f: f, size: off}
	c.segs[id] = s
	c.order = append(c.order, id)
	c.bytes += off
	return nil
}

func (c *DiskCache) Get(id object.ID) ([]byte, error) {
	c.mu.RLock()
	l, ok := c.index[id]
	var s *segment
	old := false
	if ok {
		s = c.segs[l.seg]
		// Promote objects living in the oldest quarter of segments.
		old = len(c.order) >= 4 && l.seg <= c.order[len(c.order)/4]
	}
	c.mu.RUnlock()
	if !ok || s == nil {
		c.misses.Add(1)
		return nil, object.ErrNotFound
	}
	b := make([]byte, l.n)
	if _, err := s.f.ReadAt(b, l.off); err != nil {
		// The segment may have been evicted concurrently.
		c.misses.Add(1)
		return nil, object.ErrNotFound
	}
	c.hits.Add(1)
	if old && c.budget > 0 {
		c.promoted.Add(1)
		c.put(id, b, true)
	}
	return b, nil
}

func (c *DiskCache) Has(id object.ID) (bool, error) {
	c.mu.RLock()
	_, ok := c.index[id]
	c.mu.RUnlock()
	return ok, nil
}

func (c *DiskCache) Put(id object.ID, data []byte) error { return c.put(id, data, false) }

func (c *DiskCache) put(id object.ID, data []byte, promote bool) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if l, ok := c.index[id]; ok && (!promote || l.seg == c.active.id) {
		return nil
	}
	if c.active == nil || c.active.size >= c.segSize {
		if err := c.rollLocked(); err != nil {
			return err
		}
	}
	s := c.active
	rec := make([]byte, recHdr+len(data))
	copy(rec, id[:])
	binary.BigEndian.PutUint32(rec[32:], uint32(len(data)))
	copy(rec[recHdr:], data)
	if _, err := s.f.WriteAt(rec, s.size); err != nil {
		return err
	}
	c.index[id] = loc{s.id, s.size + recHdr, uint32(len(data))}
	s.size += int64(len(rec))
	c.bytes += int64(len(rec))
	c.evictLocked()
	return nil
}

func (c *DiskCache) rollLocked() error {
	id := c.nextSeg
	f, err := os.OpenFile(c.segPath(id), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	c.nextSeg++
	s := &segment{id: id, f: f}
	c.segs[id] = s
	c.order = append(c.order, id)
	c.active = s
	return nil
}

func (c *DiskCache) evictLocked() {
	if c.budget <= 0 {
		return
	}
	for c.bytes > c.budget && len(c.order) > 1 {
		id := c.order[0]
		c.order = c.order[1:]
		s := c.segs[id]
		delete(c.segs, id)
		for oid, l := range c.index {
			if l.seg == id {
				delete(c.index, oid)
			}
		}
		c.bytes -= s.size
		s.f.Close()
		os.Remove(c.segPath(id))
	}
}

// Close releases segment files.
func (c *DiskCache) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	var first error
	for _, s := range c.segs {
		if err := s.f.Close(); err != nil && first == nil {
			first = err
		}
	}
	return first
}

func (c *DiskCache) Stats() (hits, misses uint64, bytes int64, count int) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.hits.Load(), c.misses.Load(), c.bytes, len(c.index)
}
