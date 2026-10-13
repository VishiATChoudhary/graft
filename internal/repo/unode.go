package repo

import (
	"container/heap"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"sort"
	"strings"

	"github.com/VishiATChoudhary/graft/internal/object"
	"github.com/VishiATChoudhary/graft/internal/store"
)

// Unodes are graft's path-history manifest (after Mononoke's unodes). There
// is one directory unode per directory version. Each entry records, for a
// file or subdirectory, the log sequence number of the changeset that last
// modified it ("link"); subdirectory entries also point at the child unode.
//
// A path version is therefore identified by (path, link seq), and a
// directory unode is stored under versionKey(path, link): parents find
// children by name and link, with no 32-byte child pointers. Its previous
// versions are found by looking the path up at the link changeset's parents,
// which reproduces git's path-limited history (including TREESAME merge
// simplification) while visiting only the commits that touched the path.
//
// Deletions leave tombstone entries (link = the deleting changeset), so
// history survives delete/re-add and `log` works on deleted paths. Renames
// found by the rename index are stored separately as derived data.
//
// Links are sequence numbers (a few bytes) rather than 32-byte IDs, and each
// directory unode is stored as a delta against its previous version, so the
// whole history index costs a small fraction of the repository.
type Unode struct {
	Deleted bool
	Link    uint64
	Entries []UnodeEntry // sorted by name, including tombstones
}

type UnodeEntry struct {
	Name    string
	Dir     bool
	Deleted bool
	Link    uint64
}

const (
	uflagDir     = 1
	uflagDeleted = 2
)

func (u *Unode) encode() []byte {
	var e object.Encoder
	var f byte
	if u.Deleted {
		f |= uflagDeleted
	}
	e.Byte(f)
	e.Uvarint(u.Link)
	e.Uvarint(uint64(len(u.Entries)))
	for _, en := range u.Entries {
		e.String(en.Name)
		var ef byte
		if en.Dir {
			ef |= uflagDir
		}
		if en.Deleted {
			ef |= uflagDeleted
		}
		e.Byte(ef)
		e.Uvarint(en.Link)
	}
	return e.B
}

func decodeUnode(b []byte) (*Unode, error) {
	d := object.Decoder{B: b}
	f := d.Byte()
	u := &Unode{Deleted: f&uflagDeleted != 0, Link: d.Uvarint()}
	n := d.Uvarint()
	if n > uint64(len(b)) {
		return nil, object.ErrCorrupt
	}
	u.Entries = make([]UnodeEntry, 0, n)
	for i := uint64(0); i < n && d.Err == nil; i++ {
		en := UnodeEntry{Name: d.String()}
		ef := d.Byte()
		en.Dir, en.Deleted = ef&uflagDir != 0, ef&uflagDeleted != 0
		en.Link = d.Uvarint()
		u.Entries = append(u.Entries, en)
	}
	return u, d.Err
}

// Storage. All directory versions written by one changeset are stored as a
// single derived record keyed by the changeset's log sequence ("ub"). A
// version is stored whole or as a delta against the previous version of the
// same directory, which is referenced by that version's sequence number, so
// a typical version costs a few dozen bytes and no key of its own. Chains
// are capped at unodeKeyframe deltas.

const unodeKeyframe = 64

type batchEntry struct {
	u    *Unode
	prev uint64 // link of the previous version of this directory (0: none)
}

// unodeBatch collects the directory versions written by one changeset.
type unodeBatch struct {
	link    uint64
	entries map[string]*batchEntry
}

func newBatch(link uint64) *unodeBatch {
	return &unodeBatch{link: link, entries: map[string]*batchEntry{}}
}

func (b *unodeBatch) put(path string, u *Unode, prev uint64) {
	b.entries[path] = &batchEntry{u, prev}
}

func batchKey(link uint64) object.ID { return versionKey("\x00unodes", link) }

type storedEntry struct {
	prev, size uint64
	depth      int
	delta      bool
	data       []byte
}

func (r *Repo) writeBatch(b *unodeBatch) error {
	if len(b.entries) == 0 {
		return nil
	}
	paths := make([]string, 0, len(b.entries))
	for p := range b.entries {
		paths = append(paths, p)
	}
	sort.Strings(paths)
	var e object.Encoder
	e.Uvarint(uint64(len(paths)))
	for _, p := range paths {
		be := b.entries[p]
		payload := be.u.encode()
		e.String(p)
		e.Uvarint(be.prev)
		e.Uvarint(uint64(len(payload)))
		if be.prev != 0 {
			if base, depth, err := r.unodePayload(p, be.prev); err == nil && depth+1 < unodeKeyframe {
				if d := store.MakeDelta(base, payload); d != nil && len(d) < len(payload) {
					e.Uvarint(uint64(depth + 1))
					e.Byte(1)
					e.Bytes(d)
					r.cacheUnode(p, b.link, payload, depth+1)
					continue
				}
			}
		}
		e.Uvarint(0)
		e.Byte(0)
		e.Bytes(payload)
		r.cacheUnode(p, b.link, payload, 0)
	}
	return r.DB.PutDerived("ub", batchKey(b.link), object.Pack(object.KindUnode, e.B))
}

func (r *Repo) loadBatch(link uint64) (map[string]storedEntry, error) {
	if v, ok := r.batches.Load(link); ok {
		return v.(map[string]storedEntry), nil
	}
	raw, err := r.DB.GetDerived("ub", batchKey(link))
	if err != nil {
		return nil, err
	}
	_, b, err := object.Unpack(raw)
	if err != nil {
		return nil, err
	}
	d := object.Decoder{B: b}
	n := d.Uvarint()
	if n > uint64(len(b)) {
		return nil, object.ErrCorrupt
	}
	out := make(map[string]storedEntry, n)
	for i := uint64(0); i < n && d.Err == nil; i++ {
		p := d.String()
		se := storedEntry{prev: d.Uvarint(), size: d.Uvarint()}
		se.depth = int(d.Uvarint())
		se.delta = d.Byte() == 1
		se.data = d.Bytes()
		out[p] = se
	}
	if d.Err != nil {
		return nil, d.Err
	}
	if r.nbatches.Add(1) > 1<<14 {
		r.batches.Range(func(k, _ any) bool { r.batches.Delete(k); return true })
		r.nbatches.Store(0)
	}
	r.batches.Store(link, out)
	return out, nil
}

func (r *Repo) cacheUnode(path string, link uint64, payload []byte, depth int) {
	v := make([]byte, 1+len(payload))
	v[0] = byte(depth)
	copy(v[1:], payload)
	r.unodes.Put(versionKey(path, link), v)
}

// unodePayload returns the encoded unode for directory path at link, and
// its delta depth.
func (r *Repo) unodePayload(path string, link uint64) ([]byte, int, error) {
	if v, err := r.unodes.Get(versionKey(path, link)); err == nil {
		return v[1:], int(v[0]), nil
	}
	b, err := r.loadBatch(link)
	if err != nil {
		return nil, 0, err
	}
	se, ok := b[path]
	if !ok {
		return nil, 0, fmt.Errorf("%w: unode %q@%d", object.ErrNotFound, path, link)
	}
	payload := se.data
	if se.delta {
		base, _, err := r.unodePayload(path, se.prev)
		if err != nil {
			return nil, 0, err
		}
		if payload, err = store.ApplyDelta(base, se.data, se.size); err != nil {
			return nil, 0, err
		}
	}
	r.cacheUnode(path, link, payload, se.depth)
	return payload, se.depth, nil
}

func (r *Repo) readUnode(path string, link uint64) (*Unode, error) {
	p, _, err := r.unodePayload(path, link)
	if err != nil {
		return nil, err
	}
	return decodeUnode(p)
}

func (u *Unode) find(name string) int {
	i := sort.Search(len(u.Entries), func(i int) bool { return u.Entries[i].Name >= name })
	if i < len(u.Entries) && u.Entries[i].Name == name {
		return i
	}
	return -1
}

// tombstoneDir writes a version of directory path at link in which every
// live entry beneath it is deleted. cur is the directory's current link.
func (r *Repo) tombstoneDir(path string, cur, link uint64, b *unodeBatch) error {
	u, err := r.readUnode(path, cur)
	if err != nil {
		return err
	}
	if u.Deleted {
		return nil
	}
	nu := &Unode{Deleted: true, Link: link, Entries: make([]UnodeEntry, len(u.Entries))}
	for i, e := range u.Entries {
		if !e.Deleted {
			if e.Dir {
				if err := r.tombstoneDir(join(path, e.Name), e.Link, link, b); err != nil {
					return err
				}
			}
			e.Deleted, e.Link = true, link
		}
		nu.Entries[i] = e
	}
	b.put(path, nu, cur)
	return nil
}

// applyUnode writes the new version of directory dir (previous version at
// prev, 0 if none) with changes applied. It reports whether the directory
// still exists (possibly as a tombstone) and whether it is deleted.
func (r *Repo) applyUnode(dir string, prev uint64, changes []object.Change, link uint64, b *unodeBatch) (bool, bool, error) {
	base := &Unode{}
	if prev != 0 {
		var err error
		if base, err = r.readUnode(dir, prev); err != nil {
			return false, false, err
		}
	}
	groups, err := groupChanges(changes)
	if err != nil {
		return false, false, err
	}
	entries := make(map[string]UnodeEntry, len(base.Entries)+len(groups))
	for _, e := range base.Entries {
		entries[e.Name] = e
	}
	for _, g := range groups {
		cur, exists := entries[g.name]
		cp := join(dir, g.name)
		if g.direct != nil && g.direct.Mode == object.ModeNone {
			if exists && !cur.Deleted {
				if cur.Dir {
					if err := r.tombstoneDir(cp, cur.Link, link, b); err != nil {
						return false, false, err
					}
				}
				cur.Deleted, cur.Link = true, link
				entries[g.name] = cur
			}
			if len(g.nested) == 0 {
				continue
			}
		}
		if g.direct != nil && g.direct.Mode != object.ModeNone {
			entries[g.name] = UnodeEntry{Name: g.name, Link: link}
			continue
		}
		var sub uint64
		if exists && cur.Dir {
			sub = cur.Link
		}
		present, deleted, err := r.applyUnode(cp, sub, g.nested, link, b)
		if err != nil {
			return false, false, err
		}
		if !present {
			delete(entries, g.name)
			continue
		}
		entries[g.name] = UnodeEntry{Name: g.name, Dir: true, Deleted: deleted, Link: link}
	}
	if len(entries) == 0 {
		return false, false, nil
	}
	nu := &Unode{Deleted: true, Link: link}
	for _, e := range entries {
		nu.Entries = append(nu.Entries, e)
		if !e.Deleted {
			nu.Deleted = false
		}
	}
	sort.Slice(nu.Entries, func(i, j int) bool { return nu.Entries[i].Name < nu.Entries[j].Name })
	b.put(dir, nu, prev)
	return true, nu.Deleted, nil
}

// pathVersion identifies one version of a path.
type pathVersion struct {
	path    string
	link    uint64
	dir     bool
	deleted bool
}

// lookupVersion resolves path at a changeset to the version that is current
// there. If live is set, deleted paths are reported as not found.
func (r *Repo) lookupVersion(cs object.ID, path string, live bool) (pathVersion, error) {
	path = strings.Trim(path, "/")
	notFound := fmt.Errorf("%w: path %q", object.ErrNotFound, path)
	c, err := r.csInfo(cs)
	if err != nil {
		return pathVersion{}, err
	}
	if c.Link == 0 {
		return pathVersion{}, notFound
	}
	u, err := r.readUnode("", c.Link)
	if err != nil {
		return pathVersion{}, err
	}
	if path == "" {
		return pathVersion{path: "", link: u.Link, dir: true, deleted: u.Deleted}, nil
	}
	segs := strings.Split(path, "/")
	for i, seg := range segs {
		j := u.find(seg)
		if j < 0 {
			return pathVersion{}, notFound
		}
		e := u.Entries[j]
		if live && e.Deleted {
			return pathVersion{}, notFound
		}
		if i == len(segs)-1 {
			return pathVersion{path: path, link: e.Link, dir: e.Dir, deleted: e.Deleted}, nil
		}
		if !e.Dir {
			return pathVersion{}, notFound
		}
		if u, err = r.readUnode(strings.Join(segs[:i+1], "/"), e.Link); err != nil {
			return pathVersion{}, err
		}
	}
	return pathVersion{}, notFound
}

// previousVersions returns the versions of v.path just before v: the path
// looked up at each parent of v's link changeset (deduplicated).
func (r *Repo) previousVersions(v pathVersion, live bool) ([]pathVersion, error) {
	id, err := r.DB.LogID(v.link)
	if err != nil {
		return nil, err
	}
	g, err := r.Graph(id)
	if err != nil {
		return nil, err
	}
	var out []pathVersion
	for _, p := range g.Parents {
		pv, err := r.lookupVersion(p, v.path, live)
		if err != nil {
			continue
		}
		dup := false
		for _, o := range out {
			dup = dup || o.link == pv.link
		}
		if !dup {
			out = append(out, pv)
		}
	}
	return out, nil
}

// versionKey is a stable synthetic ID for a path version, used to key
// per-version derived data (rename sources, blame).
func versionKey(path string, link uint64) object.ID {
	h := sha256.New()
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], link)
	h.Write(b[:])
	h.Write([]byte(path))
	var id object.ID
	h.Sum(id[:0])
	return id
}

func encodeVersion(path string, link uint64) []byte {
	var e object.Encoder
	e.String(path)
	e.Uvarint(link)
	return e.B
}

func decodeVersion(b []byte) (string, uint64, error) {
	d := object.Decoder{B: b}
	p := d.String()
	l := d.Uvarint()
	return p, l, d.Err
}

// copySource returns the rename source recorded for a path version.
func (r *Repo) copySource(path string, link uint64) (pathVersion, bool, error) {
	v, err := r.DB.GetDerived("copy", versionKey(path, link))
	if err == object.ErrNotFound {
		return pathVersion{}, false, nil
	}
	if err != nil {
		return pathVersion{}, false, err
	}
	p, l, err := decodeVersion(v)
	if err != nil {
		return pathVersion{}, false, err
	}
	return pathVersion{path: p, link: l}, true, nil
}

// PathLog returns up to limit changesets that modified path (a file or
// directory, live or deleted) at or before rev, newest first. Cost is
// proportional to the number of results, not the length of history. With
// follow, file history continues across detected renames.
func (r *Repo) PathLog(rev object.ID, path string, limit int, follow bool) ([]object.ID, error) {
	start, err := r.lookupVersion(rev, path, false)
	if err != nil {
		return nil, err
	}
	gen := func(link uint64) (uint64, object.ID, error) {
		id, err := r.DB.LogID(link)
		if err != nil {
			return 0, id, err
		}
		g, err := r.Graph(id)
		if err != nil {
			return 0, id, err
		}
		return g.Gen, id, nil
	}
	type key struct {
		path string
		link uint64
	}
	pending := map[key]pathVersion{}
	h := &versionHeap{}
	seen := map[key]bool{}
	push := func(v pathVersion) error {
		k := key{v.path, v.link}
		if seen[k] {
			return nil
		}
		seen[k] = true
		g, _, err := gen(v.link)
		if err != nil {
			return err
		}
		pending[k] = v
		heap.Push(h, versionItem{k.path, k.link, g})
		return nil
	}
	if err := push(start); err != nil {
		return nil, err
	}
	seenLink := map[uint64]bool{}
	var out []object.ID
	for h.Len() > 0 && (limit <= 0 || len(out) < limit) {
		it := heap.Pop(h).(versionItem)
		v := pending[key{it.path, it.link}]
		if !seenLink[v.link] {
			seenLink[v.link] = true
			id, err := r.DB.LogID(v.link)
			if err != nil {
				return nil, err
			}
			out = append(out, id)
		}
		prev, err := r.previousVersions(v, false)
		if err != nil {
			return nil, err
		}
		for _, p := range prev {
			if err := push(p); err != nil {
				return nil, err
			}
		}
		if follow && !v.dir {
			if src, ok, err := r.copySource(v.path, v.link); err != nil {
				return nil, err
			} else if ok {
				if err := push(src); err != nil {
					return nil, err
				}
			}
		}
	}
	return out, nil
}

type versionItem struct {
	path string
	link uint64
	gen  uint64
}

type versionHeap []versionItem

func (h versionHeap) Len() int { return len(h) }
func (h versionHeap) Less(i, j int) bool {
	if h[i].gen != h[j].gen {
		return h[i].gen > h[j].gen
	}
	return h[i].link > h[j].link
}
func (h versionHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *versionHeap) Push(x interface{}) { *h = append(*h, x.(versionItem)) }
func (h *versionHeap) Pop() interface{} {
	old := *h
	x := old[len(old)-1]
	*h = old[:len(old)-1]
	return x
}
