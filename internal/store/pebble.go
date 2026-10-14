package store

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/cockroachdb/pebble"
	"github.com/klauspost/compress/zstd"

	"github.com/VishiATChoudhary/graft/internal/object"
)

// DB is the local authoritative store: objects, derived data, the changeset
// log and refs in one pebble database under disjoint key prefixes.
//
// Objects are served in their standard stored form, but on disk the DB may
// keep them in a denser storage form, decided once at write time:
//
//   - delta: a new version of a path is stored as copy/insert ops against
//     the previous version of the same path (chains are capped, so reads
//     stay bounded and nothing is ever repacked);
//   - dictionary: full blobs are compressed with a zstd dictionary trained
//     on the repository's own content.
//
// Neither changes object IDs or what clients receive; both are internals
// that can change without touching clients.
type DB struct {
	bg sync.WaitGroup // background dictionary training
	// TrainInline trains dictionaries in the writer that collects the last
	// sample (bulk import: one writer, densest result) instead of in the
	// background (servers: no writer ever stalls).
	TrainInline bool
	db          *pebble.DB
	hot         *MemLRU // standard stored form, for serving
	payloads    *MemLRU // [kind]+payload, for server-internal reads

	seqMu    sync.Mutex
	seq      uint64
	refLocks sync.Map

	// Trained zstd dictionaries, one per kind of object that benefits:
	// blobs, and changesets (many small records sharing paths, authors
	// and message phrasing).
	dicts    map[object.Kind]*dictState
	decMu    sync.RWMutex
	dictDecs map[uint32]*zstd.Decoder

	deltas, dicted atomic.Uint64
	peers          BaseResolver
	why            [8]atomic.Uint64 // Deltify outcomes, for diagnostics
}

const (
	pfxObject  = 'o'
	pfxDerived = 'd'
	pfxRef     = 'r'
	pfxLog     = 'w'
	pfxLogIdx  = 's'
	pfxMeta    = 'm'

	// Storage-only codecs (never sent to clients).
	codecDict  = 2
	codecDelta = 3

	// Chains are capped so reads stay bounded. Trees and unodes are small
	// and cheap to rebuild, so their chains can be long (and dense); blobs
	// are larger, so theirs are shorter (git's default pack depth is 50).
	maxDeltaDepth     = 255
	maxBlobDeltaDepth = 64

	maxChainRead = 1 << 14

	dictSamples = 2000

	// Object keys and delta-base references use the first 16 bytes of the
	// ID: 2^64 objects before a birthday collision is likely, and it saves
	// 16 bytes per object and per delta.
	keyIDLen = 16
)

var (
	plainEnc, _ = zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault), zstd.WithEncoderConcurrency(runtime.GOMAXPROCS(0)))
	bigEnc, _   = zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedBetterCompression), zstd.WithEncoderConcurrency(1))
	bestEnc, _  = zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedBestCompression), zstd.WithEncoderConcurrency(1))
	plainDec, _ = zstd.NewReader(nil, zstd.WithDecoderConcurrency(runtime.GOMAXPROCS(0)))
)

// OpenDB opens a pebble database at dir. cacheBytes sizes pebble's block
// cache and the object caches.
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
		BytesPerSync:                1 << 20,
		MemTableStopWritesThreshold: 4,
		Logger:                      quietLogger{},
	}
	opts.Levels = make([]pebble.LevelOptions, 7)
	for i := range opts.Levels {
		opts.Levels[i].BlockSize = 32 << 10
		opts.Levels[i].TargetFileSize = 64 << 20
		// Values are already compressed (zstd, dictionaries, deltas); Snappy
		// blocks cover keys and small records. (Pebble's cgo zstd block path
		// rejected some of our blocks: "decompressed into unexpected buffer".)
		opts.Levels[i].Compression = pebble.SnappyCompression
	}
	pdb, err := pebble.Open(dir, opts)
	if err != nil {
		return nil, err
	}
	d := &DB{db: pdb, hot: NewMemLRU(cacheBytes / 4), payloads: NewMemLRU(cacheBytes / 4), dictDecs: map[uint32]*zstd.Decoder{},
		dicts: map[object.Kind]*dictState{
			object.KindBlob:      {kind: object.KindBlob, base: 0, samples: dictSamples, history: 112 << 10, maxLen: 64 << 10},
			object.KindChangeset: {kind: object.KindChangeset, base: 1 << 20, samples: 1000, history: 32 << 10, maxLen: 64 << 10},
		}}
	if d.seq, err = d.lastLogSeq(); err != nil {
		pdb.Close()
		return nil, err
	}
	if err := d.loadDicts(); err != nil {
		pdb.Close()
		return nil, err
	}
	return d, nil
}

type quietLogger struct{}

func (quietLogger) Infof(string, ...interface{})      {}
func (quietLogger) Errorf(f string, a ...interface{}) { fmt.Printf("pebble: "+f+"\n", a...) }
func (quietLogger) Fatalf(f string, a ...interface{}) { panic(fmt.Sprintf(f, a...)) }

func (d *DB) Close() error {
	d.bg.Wait()
	return d.db.Close()
}

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

// ---------------------------------------------------------------- objects

// rec is a parsed storage record.
type rec struct {
	kind  object.Kind
	codec byte
	depth byte
	base  object.ID // delta records: only the first keyIDLen bytes are set
	size  uint64
	dict  uint32
	zstd  bool
	data  []byte
	raw   []byte
}

func parseRec(b []byte) (rec, error) {
	if len(b) < 2 {
		return rec{}, object.ErrCorrupt
	}
	r := rec{kind: object.Kind(b[0]), codec: b[1], raw: b}
	switch r.codec {
	case 0, 1:
		r.data = b[2:]
	case codecDict:
		dct := object.Decoder{B: b[2:]}
		r.dict = uint32(dct.Uvarint())
		r.size = dct.Uvarint()
		r.data = dct.B
		if dct.Err != nil {
			return rec{}, dct.Err
		}
	case codecDelta:
		if len(b) < 3+keyIDLen {
			return rec{}, object.ErrCorrupt
		}
		r.depth = b[2]
		copy(r.base[:], b[3:3+keyIDLen])
		dct := object.Decoder{B: b[3+keyIDLen:]}
		r.size = dct.Uvarint()
		r.zstd = dct.Byte() == 1
		r.data = dct.B
		if dct.Err != nil {
			return rec{}, dct.Err
		}
	default:
		return rec{}, fmt.Errorf("%w: storage codec %d", object.ErrCorrupt, r.codec)
	}
	return r, nil
}

func objKey(id object.ID) []byte { return key(pfxObject, id[:keyIDLen]) }

func (d *DB) raw(id object.ID) ([]byte, error) { return d.get(objKey(id)) }

// pkey is the payload-cache key: the 16-byte storage prefix, so delta
// bases (which are referenced by prefix) share cache entries with full IDs.
func pkey(id object.ID) object.ID {
	var k object.ID
	copy(k[:keyIDLen], id[:keyIDLen])
	return k
}

func (d *DB) Has(id object.ID) (bool, error) {
	if ok, _ := d.hot.Has(id); ok {
		return true, nil
	}
	if ok, _ := d.payloads.Has(pkey(id)); ok {
		return true, nil
	}
	_, closer, err := d.db.Get(objKey(id))
	if errors.Is(err, pebble.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	closer.Close()
	return true, nil
}

// Get returns the standard stored form (what every tier serves).
func (d *DB) Get(id object.ID) ([]byte, error) {
	if b, err := d.hot.Get(id); err == nil {
		return b, nil
	}
	b, err := d.raw(id)
	if err != nil {
		return nil, err
	}
	if len(b) >= 2 && b[1] <= 1 {
		d.hot.Put(id, b)
		return b, nil
	}
	k, p, err := d.Payload(id)
	if err != nil {
		return nil, err
	}
	out := object.Pack(k, p)
	d.hot.Put(id, out)
	return out, nil
}

// Payload returns kind and uncompressed payload, resolving delta chains
// (including bases held by peer shards).
func (d *DB) Payload(id object.ID) (object.Kind, []byte, error) {
	if v, err := d.payloads.Get(pkey(id)); err == nil {
		return object.Kind(v[0]), v[1:], nil
	}
	var chain []rec
	var ids []object.ID
	cur := id
	var base []byte
	var kind object.Kind
	for {
		if v, err := d.payloads.Get(pkey(cur)); err == nil && len(chain) > 0 {
			kind, base = object.Kind(v[0]), v[1:]
			break
		}
		b, err := d.raw(cur)
		if err != nil {
			if len(chain) > 0 && errors.Is(err, object.ErrNotFound) {
				if d.peers != nil {
					if info, perr := d.peers(cur, false); perr == nil {
						kind, base = info.Kind, info.Payload
						d.cachePayload(cur, kind, base)
						break
					}
				}
				return 0, nil, fmt.Errorf("%w: delta base %s of %s missing", object.ErrCorrupt, cur.Short(), id.Short())
			}
			return 0, nil, err
		}
		r, err := parseRec(b)
		if err != nil {
			return 0, nil, err
		}
		if r.codec != codecDelta {
			if kind, base, err = d.decodeFull(r); err != nil {
				return 0, nil, err
			}
			d.cachePayload(cur, kind, base)
			break
		}
		// Reverse deltas grow chains after the fact (an old version's base
		// may later become a delta itself), so the cap here is a safety
		// net against corruption, not the write-time depth limit.
		if len(chain) > maxChainRead {
			return 0, nil, fmt.Errorf("%w: delta chain too long at %s", object.ErrCorrupt, id.Short())
		}
		chain = append(chain, r)
		ids = append(ids, cur)
		cur = r.base
	}
	for i := len(chain) - 1; i >= 0; i-- {
		r := chain[i]
		delta := r.data
		if r.zstd {
			var err error
			if delta, err = plainDec.DecodeAll(r.data, nil); err != nil {
				return 0, nil, fmt.Errorf("%w: %v", object.ErrCorrupt, err)
			}
		}
		out, err := applyDelta(base, delta, r.size)
		if err != nil {
			return 0, nil, fmt.Errorf("%w: %s", err, ids[i].Short())
		}
		base, kind = out, r.kind
		d.cachePayload(ids[i], kind, base)
	}
	return kind, base, nil
}

func (d *DB) cachePayload(id object.ID, k object.Kind, p []byte) {
	v := make([]byte, 1+len(p))
	v[0] = byte(k)
	copy(v[1:], p)
	d.payloads.Put(pkey(id), v)
}

func (d *DB) decodeFull(r rec) (object.Kind, []byte, error) {
	switch r.codec {
	case 0, 1:
		return object.Unpack(r.raw)
	case codecDict:
		d.decMu.RLock()
		dec := d.dictDecs[r.dict]
		d.decMu.RUnlock()
		if dec == nil {
			return 0, nil, fmt.Errorf("%w: unknown dictionary %d", object.ErrCorrupt, r.dict)
		}
		p, err := dec.DecodeAll(r.data, make([]byte, 0, r.size))
		if err != nil {
			return 0, nil, fmt.Errorf("%w: %v", object.ErrCorrupt, err)
		}
		return r.kind, p, nil
	}
	return 0, nil, object.ErrCorrupt
}

func (d *DB) PayloadSize(id object.ID) (uint64, error) {
	if b, err := d.hot.Get(id); err == nil {
		return object.PayloadSize(b)
	}
	b, err := d.raw(id)
	if err != nil {
		return 0, err
	}
	r, err := parseRec(b)
	if err != nil {
		return 0, err
	}
	if r.codec <= 1 {
		return object.PayloadSize(b)
	}
	return r.size, nil
}

// Put stores an object. Full blobs are re-encoded with the repository
// dictionary (or, if too large for it, at a stronger zstd level).
func (d *DB) Put(id object.ID, stored []byte) error {
	if ok, err := d.Has(id); err != nil || ok {
		return err
	}
	return d.write(id, d.denseFull(id, stored))
}

func (d *DB) write(id object.ID, record []byte) error {
	return d.db.Set(objKey(id), record, pebble.NoSync)
}

func (d *DB) denseFull(id object.ID, stored []byte) []byte {
	if len(stored) < 80 {
		return stored
	}
	ds := d.dicts[object.Kind(stored[0])]
	if ds == nil {
		return stored
	}
	_, p, err := object.Unpack(stored)
	if err != nil {
		return stored
	}
	if len(p) > 1<<20 {
		// Too large for the dictionary: recompress harder instead (still
		// the standard zstd stored form).
		out := append([]byte{stored[0], 1}, bigEnc.EncodeAll(p, nil)...)
		if len(out) < len(stored) {
			return out
		}
		return stored
	}
	enc, no := ds.encoder(d, id, p)
	if enc == nil {
		return stored
	}
	var e object.Encoder
	e.Byte(stored[0])
	e.Byte(codecDict)
	e.Uvarint(uint64(no))
	e.Uvarint(uint64(len(p)))
	out := enc.EncodeAll(p, e.B)
	if len(out) >= len(stored) {
		return stored
	}
	d.dicted.Add(1)
	return out
}

// PutBase stores an object, as a delta against base when that is smaller.
func (d *DB) PutBase(id object.ID, stored []byte, base object.ID) error {
	if base.IsZero() || base == id {
		return d.Put(id, stored)
	}
	if ok, err := d.Has(id); err != nil || ok {
		return err
	}
	k, p, err := object.Unpack(stored)
	if err != nil {
		return err
	}
	if r := d.deltaRecord(id, k, p, base, false); r != nil && len(r) < len(stored) {
		d.deltas.Add(1)
		d.cachePayload(id, k, p)
		return d.write(id, r)
	}
	return d.write(id, d.denseFull(id, stored))
}

// Deltify re-stores an existing object as a delta against base if that is
// smaller than its current record (including an existing delta against a
// different base, so callers may offer several candidates).
func (d *DB) Deltify(id, base object.ID) error {
	if base.IsZero() || base == id {
		return nil
	}
	b, err := d.raw(id)
	if err != nil {
		return err
	}
	if len(b) < 2 {
		return nil
	}
	if b[1] == codecDelta && len(b) >= 3+keyIDLen && [keyIDLen]byte(b[3:3+keyIDLen]) == [keyIDLen]byte(base[:keyIDLen]) {
		return nil
	}
	k, p, err := d.Payload(id)
	if err != nil {
		return err
	}
	r := d.deltaRecord(id, k, p, base, true)
	switch {
	case r == nil:
		d.why[1].Add(1)
	case len(r) >= len(b):
		d.why[2].Add(1)
	default:
		d.why[0].Add(1)
		d.deltas.Add(1)
		return d.write(id, r)
	}
	return nil
}

// deltaRecord builds a delta storage record for (id, p) against base, or
// returns nil. existing is set when id is already stored (Deltify): only
// then could base's chain lead back to id, so only then is it checked.
func (d *DB) deltaRecord(id object.ID, k object.Kind, p []byte, base object.ID, existing bool) []byte {
	if len(p) < 64 {
		return nil
	}
	limit := byte(maxDeltaDepth)
	if k == object.KindBlob {
		limit = maxBlobDeltaDepth
	}
	var info *BaseInfo
	var err error
	if ok, _ := d.Has(base); ok {
		info, err = d.localBase(base, existing)
	} else if d.peers != nil {
		info, err = d.peers(base, existing)
	} else {
		return nil
	}
	if err != nil || info.Kind != k {
		d.why[3].Add(1)
		return nil
	}
	if int(info.Depth)+1 > int(limit) { // int: a byte would wrap at 255
		d.why[4].Add(1)
		return nil
	}
	if existing {
		for _, c := range info.Chain {
			if c == [keyIDLen]byte(id[:keyIDLen]) {
				d.why[5].Add(1)
				return nil
			}
		}
	}
	delta := makeDelta(info.Payload, p)
	if delta == nil {
		return nil
	}
	var e object.Encoder
	e.Byte(byte(k))
	e.Byte(codecDelta)
	e.Byte(info.Depth + 1)
	e.B = append(e.B, base[:keyIDLen]...)
	e.Uvarint(uint64(len(p)))
	if len(delta) > 128 {
		if z := plainEnc.EncodeAll(delta, nil); len(z) < len(delta) {
			e.Byte(1)
			return append(e.B, z...)
		}
	}
	e.Byte(0)
	return append(e.B, delta...)
}

// ---------------------------------------------------------------- dictionary

// dictState trains and holds the dictionary for one kind of object. The
// first samples objects of the kind are collected; then the dictionary is
// trained synchronously (concurrent writers wait, so the bulk of an import
// is dictionary-compressed) and the samples are re-encoded with it.
type dictState struct {
	kind    object.Kind
	base    uint32 // dictionary numbers for this kind start above base
	samples int
	history int // dictionary content size
	maxLen  int // objects larger than this are not sampled

	mu              sync.RWMutex
	enc             *zstd.Encoder
	no              uint32
	sample          [][]byte
	sampleIDs       []object.ID
	trainingStarted bool
	late            []object.ID // written plain while training; re-encoded after
	failed          atomic.Bool
}

// encoder returns the dictionary encoder for p, sampling p (and training
// once enough samples arrived) if there is none yet.
func (ds *dictState) encoder(d *DB, id object.ID, p []byte) (*zstd.Encoder, uint32) {
	ds.mu.Lock()
	if ds.enc != nil {
		enc, no := ds.enc, ds.no
		ds.mu.Unlock()
		return enc, no
	}
	if ds.failed.Load() {
		ds.mu.Unlock()
		return nil, 0
	}
	if ds.trainingStarted {
		// Training runs in the background; this object is stored plain
		// now and re-encoded with the dictionary once it exists.
		if len(ds.late) < 1<<16 {
			ds.late = append(ds.late, id)
		}
		ds.mu.Unlock()
		return nil, 0
	}
	if len(p) >= 32 && len(p) <= ds.maxLen {
		ds.sample = append(ds.sample, bytes.Clone(p))
		ds.sampleIDs = append(ds.sampleIDs, id)
	}
	if len(ds.sample) < ds.samples {
		ds.mu.Unlock()
		return nil, 0
	}
	// Enough samples: train in the background so writers never stall on
	// it (training takes about a second of CPU).
	ds.trainingStarted = true
	sample, ids := ds.sample, ds.sampleIDs
	if d.TrainInline {
		ds.mu.Unlock()
		d.train(ds, sample, ids)
		ds.mu.RLock()
		defer ds.mu.RUnlock()
		return ds.enc, ds.no
	}
	ds.late = append(ds.late, id) // this object is written plain too
	ds.mu.Unlock()
	d.bg.Add(1)
	go func() {
		defer d.bg.Done()
		d.train(ds, sample, ids)
	}()
	return nil, 0
}

func (d *DB) train(ds *dictState, sample [][]byte, ids []object.ID) {
	hist := bytes.Join(sample, nil)
	if len(hist) > ds.history {
		hist = hist[len(hist)-ds.history:]
	}
	no := ds.base + 1
	dict, err := zstd.BuildDict(zstd.BuildDictOptions{ID: no, Contents: sample, History: hist, Offsets: [3]int{1, 4, 8}})
	if err == nil {
		err = d.db.Set(key(pfxMeta, []byte("dict/"+strconv.Itoa(int(no)))), dict, pebble.Sync)
	}
	if err == nil {
		err = d.installDict(no, dict)
	}
	if err != nil {
		ds.failed.Store(true) // keep plain compression
		return
	}
	// Re-encode the sampled objects still stored as plain zstd.
	for i, id := range ids {
		b, err := d.raw(id)
		if err != nil || len(b) < 2 || b[1] > 1 { // plain (raw or zstd) records only
			continue
		}
		if r := d.denseFull(id, object.Pack(ds.kind, sample[i])); len(r) > 1 && r[1] == codecDict {
			d.write(id, r)
		}
	}
	ds.mu.Lock()
	late := ds.late
	ds.sample, ds.sampleIDs, ds.late = nil, nil, nil
	ds.mu.Unlock()
	for _, id := range late {
		d.redense(id)
	}
}

// redense re-encodes a whole plain object with the current dictionary
// when that is smaller (deltas and dictionary records are left alone).
func (d *DB) redense(id object.ID) {
	b, err := d.raw(id)
	if err != nil || len(b) < 2 || b[1] > 1 {
		return
	}
	k, p, err := d.Payload(id)
	if err != nil {
		return
	}
	if r := d.denseFull(id, object.Pack(k, p)); len(r) > 1 && r[1] == codecDict && len(r) < len(b) {
		d.write(id, r)
	}
}

func (d *DB) installDict(no uint32, dict []byte) error {
	enc, err := zstd.NewWriter(nil, zstd.WithEncoderDict(dict), zstd.WithEncoderLevel(zstd.SpeedBetterCompression),
		zstd.WithEncoderConcurrency(runtime.GOMAXPROCS(0)))
	if err != nil {
		return err
	}
	dec, err := zstd.NewReader(nil, zstd.WithDecoderDicts(dict), zstd.WithDecoderConcurrency(runtime.GOMAXPROCS(0)))
	if err != nil {
		return err
	}
	d.decMu.Lock()
	d.dictDecs[no] = dec
	d.decMu.Unlock()
	for _, ds := range d.dicts {
		if no > ds.base && no <= ds.base+1<<20-1 {
			ds.mu.Lock()
			if no >= ds.no {
				ds.no, ds.enc = no, enc
				ds.trainingStarted = true
			}
			ds.mu.Unlock()
		}
	}
	return nil
}

func (d *DB) loadDicts() error {
	lo := key(pfxMeta, []byte("dict/"))
	it, err := d.db.NewIter(&pebble.IterOptions{LowerBound: lo, UpperBound: key(pfxMeta, []byte("dict0"))})
	if err != nil {
		return err
	}
	defer it.Close()
	for it.First(); it.Valid(); it.Next() {
		no, err := strconv.Atoi(string(it.Key()[len(lo):]))
		if err != nil {
			continue
		}
		if err := d.installDict(uint32(no), bytes.Clone(it.Value())); err != nil {
			return err
		}
	}
	return it.Error()
}

// ---------------------------------------------------------------- derived

func (d *DB) GetDerived(kind string, id object.ID) ([]byte, error) {
	return d.get(key(pfxDerived, []byte(kind), []byte{0}, id[:keyIDLen]))
}

func (d *DB) PutDerived(kind string, id object.ID, v []byte) error {
	return d.db.Set(key(pfxDerived, []byte(kind), []byte{0}, id[:keyIDLen]), v, pebble.NoSync)
}

// ---------------------------------------------------------------- refs

func (d *DB) GetRef(name string) (object.ID, error) {
	v, err := d.get(key(pfxRef, []byte(name)))
	if err != nil {
		return object.Zero, err
	}
	var id object.ID
	copy(id[:], v)
	return id, nil
}

// CASRef is a compare-and-swap on a single row, serialized per ref only.
func (d *DB) CASRef(name string, old, new object.ID) error {
	m, _ := d.refLocks.LoadOrStore(name, &sync.Mutex{})
	mu := m.(*sync.Mutex)
	mu.Lock()
	defer mu.Unlock()
	cur, err := d.GetRef(name)
	if errors.Is(err, object.ErrNotFound) {
		cur, err = object.Zero, nil
	}
	if err != nil {
		return err
	}
	if cur != old {
		return &RefMismatch{Name: name, Current: cur, Expected: old}
	}
	if new.IsZero() {
		return d.db.Delete(key(pfxRef, []byte(name)), pebble.Sync)
	}
	// Sync makes every earlier unsynced write durable too (single WAL).
	return d.db.Set(key(pfxRef, []byte(name)), new[:], pebble.Sync)
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

// ---------------------------------------------------------------- log

func be64(v uint64) []byte {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], v)
	return b[:]
}

func (d *DB) lastLogSeq() (uint64, error) {
	it, err := d.db.NewIter(&pebble.IterOptions{LowerBound: []byte{pfxLog}, UpperBound: []byte{pfxLog + 1}})
	if err != nil {
		return 0, err
	}
	defer it.Close()
	if !it.Last() {
		return 0, it.Error()
	}
	return binary.BigEndian.Uint64(it.Key()[1:]), nil
}

func (d *DB) LastLogSeq() (uint64, error) {
	d.seqMu.Lock()
	defer d.seqMu.Unlock()
	return d.seq, nil
}

// AppendLog assigns the next sequence number. Only the counter increment
// and one small batch are serialized.
func (d *DB) AppendLog(id object.ID) (uint64, error) {
	d.seqMu.Lock()
	defer d.seqMu.Unlock()
	if s, err := d.LogSeq(id); err == nil {
		return s, nil
	}
	seq := d.seq + 1
	b := d.db.NewBatch()
	b.Set(key(pfxLog, be64(seq)), id[:], nil)
	b.Set(key(pfxLogIdx, id[:keyIDLen]), be64(seq), nil)
	if err := b.Commit(pebble.NoSync); err != nil {
		return 0, err
	}
	d.seq = seq
	return seq, nil
}

func (d *DB) LogSeq(id object.ID) (uint64, error) {
	v, err := d.get(key(pfxLogIdx, id[:keyIDLen]))
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint64(v), nil
}

func (d *DB) LogID(seq uint64) (object.ID, error) {
	v, err := d.get(key(pfxLog, be64(seq)))
	if err != nil {
		return object.Zero, err
	}
	var id object.ID
	copy(id[:], v)
	return id, nil
}

// ScanLogIdxPrefix iterates logged changeset IDs starting with prefix bytes
// (the index is keyed by the 16-byte storage prefix; full IDs come from the
// log).
func (d *DB) ScanLogIdxPrefix(prefix []byte, fn func(id object.ID) bool) error {
	if len(prefix) > keyIDLen {
		prefix = prefix[:keyIDLen]
	}
	lo := key(pfxLogIdx, prefix)
	hi := key(pfxLogIdx, prefix, bytes.Repeat([]byte{0xff}, keyIDLen+1-len(prefix)))
	it, err := d.db.NewIter(&pebble.IterOptions{LowerBound: lo, UpperBound: hi})
	if err != nil {
		return err
	}
	defer it.Close()
	for it.First(); it.Valid(); it.Next() {
		id, err := d.LogID(binary.BigEndian.Uint64(it.Value()))
		if err != nil {
			return err
		}
		if !fn(id) {
			break
		}
	}
	return it.Error()
}

// ---------------------------------------------------------------- misc

func (d *DB) Flush() error { return d.db.Flush() }

// Compact flushes and compacts the whole keyspace (used after bulk imports
// so on-disk size reflects live data).
func (d *DB) Compact() error {
	if err := d.db.Flush(); err != nil {
		return err
	}
	return d.db.Compact([]byte{0}, []byte{0xff}, true)
}

func (d *DB) Stats() map[string]any {
	hh, hm, hb := d.hot.Stats()
	ph, pm, pb := d.payloads.Stats()
	dict := map[string]uint32{}
	for k, ds := range d.dicts {
		ds.mu.RLock()
		dict[string(k)] = ds.no
		ds.mu.RUnlock()
	}
	return map[string]any{
		"hot_cache":     map[string]any{"hits": hh, "misses": hm, "bytes": hb},
		"payload_cache": map[string]any{"hits": ph, "misses": pm, "bytes": pb},
		"dictionary":    dict,
		"delta_objects": d.deltas.Load(),
		"dict_objects":  d.dicted.Load(),
		"disk_bytes":    d.db.Metrics().DiskSpaceUsage(),
		"deltify":       fmt.Sprintf("ok=%d nil=%d notsmaller=%d base=%d depth=%d cycle=%d", d.why[0].Load(), d.why[1].Load(), d.why[2].Load(), d.why[3].Load(), d.why[4].Load(), d.why[5].Load()),
	}
}

// ---------------------------------------------------------------- shards

// BaseInfo describes a delta base for a peer shard: its kind, chain depth,
// the prefixes of its own base chain (for cycle checks) and its payload.
type BaseInfo struct {
	Kind    object.Kind
	Depth   byte
	Chain   [][keyIDLen]byte
	Payload []byte
}

func (b *BaseInfo) encode() []byte {
	var e object.Encoder
	e.Byte(byte(b.Kind))
	e.Byte(b.Depth)
	e.Uvarint(uint64(len(b.Chain)))
	for _, c := range b.Chain {
		e.B = append(e.B, c[:]...)
	}
	return append(e.B, b.Payload...)
}

func decodeBaseInfo(b []byte) (*BaseInfo, error) {
	d := object.Decoder{B: b}
	info := &BaseInfo{Kind: object.Kind(d.Byte()), Depth: d.Byte()}
	n := d.Uvarint()
	if d.Err != nil || n > 1024 || uint64(len(d.B)) < n*keyIDLen {
		return nil, object.ErrCorrupt
	}
	for i := uint64(0); i < n; i++ {
		var c [keyIDLen]byte
		copy(c[:], d.B[i*keyIDLen:])
		info.Chain = append(info.Chain, c)
	}
	info.Payload = d.B[n*keyIDLen:]
	return info, nil
}

// BaseResolver finds a delta base stored on another shard, by prefix. The
// base's own chain (needed only for cycle checks) is included on request.
type BaseResolver func(prefix object.ID, withChain bool) (*BaseInfo, error)

// SetPeers lets this shard use delta bases stored on peer shards, so
// storage stays dense when one file's history is spread across shards.
func (d *DB) SetPeers(r BaseResolver) { d.peers = r }

// localBase describes an object stored on this shard. The base chain is
// only walked when withChain is set (it is needed for cycle checks).
func (d *DB) localBase(id object.ID, withChain bool) (*BaseInfo, error) {
	b, err := d.raw(id)
	if err != nil {
		return nil, err
	}
	r, err := parseRec(b)
	if err != nil {
		return nil, err
	}
	info := &BaseInfo{Kind: r.kind}
	if r.codec == codecDelta {
		info.Depth = r.depth
	}
	if r.codec == codecDelta && withChain {
		cur := r
		for n := 0; cur.codec == codecDelta && n <= maxChainRead; n++ {
			var c [keyIDLen]byte
			copy(c[:], cur.base[:keyIDLen])
			info.Chain = append(info.Chain, c)
			nb, err := d.raw(cur.base)
			if errors.Is(err, object.ErrNotFound) && d.peers != nil {
				pi, err := d.peers(cur.base, true)
				if err != nil {
					return nil, err
				}
				info.Chain = append(info.Chain, pi.Chain...)
				break
			}
			if err != nil {
				return nil, err
			}
			if cur, err = parseRec(nb); err != nil {
				return nil, err
			}
		}
	}
	_, info.Payload, err = d.Payload(id)
	return info, err
}

var _ Backend = (*DB)(nil)

// StorageInfo reports how an object is stored (codec) and its record size.
func (d *DB) StorageInfo(id object.ID) (byte, int, error) {
	b, err := d.raw(id)
	if err != nil {
		return 0, 0, err
	}
	if len(b) < 2 {
		return 0, len(b), object.ErrCorrupt
	}
	return b[1], len(b), nil
}

// Redictionary trains a new dictionary for kind from the given objects (the
// whole ones among them, up to a sample) and re-encodes every whole object
// in ids with it when that is smaller. Used by optional compaction, so the
// dictionary reflects current content rather than the oldest history.
func (d *DB) Redictionary(kind object.Kind, ids []object.ID) (improved int, err error) {
	ds := d.dicts[kind]
	if ds == nil {
		return 0, nil
	}
	var sample [][]byte
	step := max(1, len(ids)/4000)
	for i := 0; i < len(ids); i += step {
		b, err := d.raw(ids[i])
		if err != nil || len(b) < 2 || b[1] > codecDict {
			continue
		}
		k, p, err := d.Payload(ids[i])
		if err != nil || k != kind || len(p) < 32 || len(p) > ds.maxLen {
			continue
		}
		sample = append(sample, p)
	}
	if len(sample) < 100 {
		return 0, nil
	}
	hist := bytes.Join(sample, nil)
	if len(hist) > ds.history {
		hist = hist[len(hist)-ds.history:]
	}
	ds.mu.RLock()
	no := max(ds.no, ds.base) + 1
	ds.mu.RUnlock()
	dict, err := zstd.BuildDict(zstd.BuildDictOptions{ID: no, Contents: sample, History: hist, Offsets: [3]int{1, 4, 8}})
	if err != nil {
		return 0, err
	}
	// Compaction is offline, so it can afford zstd's strongest level.
	enc, err := zstd.NewWriter(nil, zstd.WithEncoderDict(dict), zstd.WithEncoderLevel(zstd.SpeedBestCompression))
	if err != nil {
		return 0, err
	}
	if err := d.db.Set(key(pfxMeta, []byte("dict/"+strconv.Itoa(int(no)))), dict, pebble.Sync); err != nil {
		return 0, err
	}
	if err := d.installDict(no, dict); err != nil {
		return 0, err
	}
	for _, id := range ids {
		b, err := d.raw(id)
		if err != nil || len(b) < 2 || b[1] > codecDict {
			continue
		}
		k, p, err := d.Payload(id)
		if err != nil || k != kind {
			continue
		}
		if len(p) > 1<<20 { // too large for a dictionary: strongest plain zstd
			if out := append([]byte{byte(kind), 1}, bestEnc.EncodeAll(p, nil)...); len(out) < len(b) {
				if err := d.write(id, out); err != nil {
					return improved, err
				}
				improved++
			}
			continue
		}
		var e object.Encoder
		e.Byte(byte(kind))
		e.Byte(codecDict)
		e.Uvarint(uint64(no))
		e.Uvarint(uint64(len(p)))
		out := enc.EncodeAll(p, e.B)
		if len(out) < len(b) {
			if err := d.write(id, out); err != nil {
				return improved, err
			}
			improved++
		}
	}
	return improved, nil
}
