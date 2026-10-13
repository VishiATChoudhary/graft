package store

import (
	"container/list"
	"sync"
	"sync/atomic"

	"github.com/VishiATChoudhary/graft/internal/object"
)

const memShards = 64

// MemLRU is a sharded, byte-bounded in-memory LRU of stored objects. It backs
// the regional cache tier, which streams objects directly from memory.
type MemLRU struct {
	shards [memShards]memShard
	hits   atomic.Uint64
	misses atomic.Uint64
}

type memShard struct {
	mu     sync.Mutex
	items  map[object.ID]*list.Element
	order  list.List
	bytes  int64
	budget int64
}

type memItem struct {
	id   object.ID
	data []byte
}

func NewMemLRU(budgetBytes int64) *MemLRU {
	m := &MemLRU{}
	for i := range m.shards {
		m.shards[i].items = make(map[object.ID]*list.Element)
		m.shards[i].budget = budgetBytes / memShards
	}
	return m
}

func (m *MemLRU) shard(id object.ID) *memShard { return &m.shards[id[0]%memShards] }

func (m *MemLRU) Get(id object.ID) ([]byte, error) {
	s := m.shard(id)
	s.mu.Lock()
	e, ok := s.items[id]
	if ok {
		s.order.MoveToFront(e)
	}
	s.mu.Unlock()
	if !ok {
		m.misses.Add(1)
		return nil, object.ErrNotFound
	}
	m.hits.Add(1)
	return e.Value.(*memItem).data, nil
}

func (m *MemLRU) Has(id object.ID) (bool, error) {
	s := m.shard(id)
	s.mu.Lock()
	_, ok := s.items[id]
	s.mu.Unlock()
	return ok, nil
}

func (m *MemLRU) Put(id object.ID, data []byte) error {
	s := m.shard(id)
	sz := int64(len(data)) + 64
	if sz > s.budget {
		return nil // too large to cache; caller still has it
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if e, ok := s.items[id]; ok {
		s.order.MoveToFront(e)
		return nil
	}
	s.items[id] = s.order.PushFront(&memItem{id: id, data: data})
	s.bytes += sz
	for s.bytes > s.budget {
		back := s.order.Back()
		it := back.Value.(*memItem)
		s.order.Remove(back)
		delete(s.items, it.id)
		s.bytes -= int64(len(it.data)) + 64
	}
	return nil
}

// Stats returns hit/miss counters and resident bytes.
func (m *MemLRU) Stats() (hits, misses uint64, bytes int64) {
	for i := range m.shards {
		m.shards[i].mu.Lock()
		bytes += m.shards[i].bytes
		m.shards[i].mu.Unlock()
	}
	return m.hits.Load(), m.misses.Load(), bytes
}
