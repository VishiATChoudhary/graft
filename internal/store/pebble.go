package store

import (
	"bytes"
	"errors"

	"github.com/cockroachdb/pebble"

	"github.com/VishiATChoudhary/graft/internal/object"
)

// DB is the authoritative server store. It plays the role Spanner plays in
// Scion: objects, derived data, the changeset log and refs all live here,
// under disjoint key prefixes. A small in-memory LRU fronts object reads.
type DB struct {
	db  *pebble.DB
	hot *MemLRU
}

const (
	pfxObject  = 'o'
	pfxDerived = 'd'
	pfxRef     = 'r'
	pfxLog     = 'w'
	pfxLogIdx  = 's'
	pfxMeta    = 'm'
)

// OpenDB opens a pebble database at dir. cacheBytes sizes both pebble's
// block cache and the hot object cache.
func OpenDB(dir string, cacheBytes int64) (*DB, error) {
	if cacheBytes <= 0 {
		cacheBytes = 512 << 20
	}
	cache := pebble.NewCache(cacheBytes / 2)
	defer cache.Unref()
	opts := &pebble.Options{
		Cache:                       cache,
		MemTableSize:                64 << 20,
		MaxConcurrentCompactions:    func() int { return 4 },
		L0CompactionThreshold:       4,
		L0StopWritesThreshold:       64,
		LBaseMaxBytes:               256 << 20,
		MaxOpenFiles:                4096,
		DisableWAL:                  false,
		BytesPerSync:                1 << 20,
		MemTableStopWritesThreshold: 4,
	}
	opts.Levels = make([]pebble.LevelOptions, 7)
	for i := range opts.Levels {
		opts.Levels[i].BlockSize = 32 << 10
		opts.Levels[i].TargetFileSize = 64 << 20
	}
	db, err := pebble.Open(dir, opts)
	if err != nil {
		return nil, err
	}
	return &DB{db: db, hot: NewMemLRU(cacheBytes / 2)}, nil
}

func (d *DB) Close() error { return d.db.Close() }

func key(pfx byte, rest ...[]byte) []byte {
	n := 1
	for _, r := range rest {
		n += len(r)
	}
	k := make([]byte, 1, n)
	k[0] = pfx
	for _, r := range rest {
		k = append(k, r...)
	}
	return k
}

func (d *DB) get(k []byte) ([]byte, error) {
	v, closer, err := d.db.Get(k)
	if errors.Is(err, pebble.ErrNotFound) {
		return nil, object.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	out := bytes.Clone(v)
	closer.Close()
	return out, nil
}

// Objects interface.

func (d *DB) Get(id object.ID) ([]byte, error) {
	if b, err := d.hot.Get(id); err == nil {
		return b, nil
	}
	b, err := d.get(key(pfxObject, id[:]))
	if err == nil {
		d.hot.Put(id, b)
	}
	return b, err
}

func (d *DB) Has(id object.ID) (bool, error) {
	if ok, _ := d.hot.Has(id); ok {
		return true, nil
	}
	_, closer, err := d.db.Get(key(pfxObject, id[:]))
	if errors.Is(err, pebble.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	closer.Close()
	return true, nil
}

func (d *DB) Put(id object.ID, stored []byte) error {
	d.hot.Put(id, stored)
	return d.db.Set(key(pfxObject, id[:]), stored, pebble.NoSync)
}

// Derived data.

func (d *DB) GetDerived(kind string, id object.ID) ([]byte, error) {
	return d.get(key(pfxDerived, []byte(kind), []byte{0}, id[:]))
}

func (d *DB) PutDerived(kind string, id object.ID, v []byte) error {
	return d.db.Set(key(pfxDerived, []byte(kind), []byte{0}, id[:]), v, pebble.NoSync)
}

// Refs.

func (d *DB) GetRef(name string) (object.ID, error) {
	v, err := d.get(key(pfxRef, []byte(name)))
	if err != nil {
		return object.Zero, err
	}
	var id object.ID
	copy(id[:], v)
	return id, nil
}

// SetRef writes a ref durably. Callers serialize per-ref CAS above this.
func (d *DB) SetRef(name string, id object.ID) error {
	if id.IsZero() {
		return d.db.Delete(key(pfxRef, []byte(name)), pebble.Sync)
	}
	return d.db.Set(key(pfxRef, []byte(name)), id[:], pebble.Sync)
}

func (d *DB) ListRefs() (map[string]object.ID, error) {
	it, err := d.db.NewIter(&pebble.IterOptions{LowerBound: []byte{pfxRef}, UpperBound: []byte{pfxRef + 1}})
	if err != nil {
		return nil, err
	}
	defer it.Close()
	out := map[string]object.ID{}
	for it.First(); it.Valid(); it.Next() {
		var id object.ID
		copy(id[:], it.Value())
		out[string(it.Key()[1:])] = id
	}
	return out, it.Error()
}

// Changeset log.

func be64(v uint64) []byte {
	return []byte{byte(v >> 56), byte(v >> 48), byte(v >> 40), byte(v >> 32), byte(v >> 24), byte(v >> 16), byte(v >> 8), byte(v)}
}

// LastLogSeq returns the highest assigned log sequence, or 0.
func (d *DB) LastLogSeq() (uint64, error) {
	it, err := d.db.NewIter(&pebble.IterOptions{LowerBound: []byte{pfxLog}, UpperBound: []byte{pfxLog + 1}})
	if err != nil {
		return 0, err
	}
	defer it.Close()
	if !it.Last() {
		return 0, it.Error()
	}
	k := it.Key()[1:]
	var v uint64
	for _, b := range k {
		v = v<<8 | uint64(b)
	}
	return v, nil
}

// AppendLog records a changeset at a sequence number.
func (d *DB) AppendLog(seq uint64, id object.ID) error {
	b := d.db.NewBatch()
	b.Set(key(pfxLog, be64(seq)), id[:], nil)
	b.Set(key(pfxLogIdx, id[:]), be64(seq), nil)
	return b.Commit(pebble.NoSync)
}

// LogSeq returns the log sequence of a changeset, or ErrNotFound.
func (d *DB) LogSeq(id object.ID) (uint64, error) {
	v, err := d.get(key(pfxLogIdx, id[:]))
	if err != nil {
		return 0, err
	}
	var s uint64
	for _, b := range v {
		s = s<<8 | uint64(b)
	}
	return s, nil
}

// ScanLog calls fn for each log entry with seq > after, in order.
func (d *DB) ScanLog(after uint64, fn func(seq uint64, id object.ID) bool) error {
	it, err := d.db.NewIter(&pebble.IterOptions{LowerBound: key(pfxLog, be64(after+1)), UpperBound: []byte{pfxLog + 1}})
	if err != nil {
		return err
	}
	defer it.Close()
	for it.First(); it.Valid(); it.Next() {
		var s uint64
		for _, b := range it.Key()[1:] {
			s = s<<8 | uint64(b)
		}
		var id object.ID
		copy(id[:], it.Value())
		if !fn(s, id) {
			break
		}
	}
	return it.Error()
}

// ScanLogIdxPrefix iterates logged changeset IDs starting with prefix bytes.
func (d *DB) ScanLogIdxPrefix(prefix []byte, fn func(id object.ID) bool) error {
	lo := key(pfxLogIdx, prefix)
	hi := key(pfxLogIdx, prefix, bytes.Repeat([]byte{0xff}, 33-len(prefix)))
	it, err := d.db.NewIter(&pebble.IterOptions{LowerBound: lo, UpperBound: hi})
	if err != nil {
		return err
	}
	defer it.Close()
	for it.First(); it.Valid(); it.Next() {
		var id object.ID
		copy(id[:], it.Key()[1:])
		if !fn(id) {
			break
		}
	}
	return it.Error()
}

// Meta key/value.

func (d *DB) GetMeta(k string) ([]byte, error) { return d.get(key(pfxMeta, []byte(k))) }
func (d *DB) SetMeta(k string, v []byte) error {
	return d.db.Set(key(pfxMeta, []byte(k)), v, pebble.Sync)
}

// Flush forces buffered writes to stable storage.
func (d *DB) Flush() error { return d.db.Flush() }

// HotStats exposes the hot object cache counters.
func (d *DB) HotStats() (uint64, uint64, int64) { return d.hot.Stats() }
