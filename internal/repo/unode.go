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

func (r *Repo) readUnode(id object.ID) (*Unode, error) {
	p, err := store.ReadPayload(r.DB, id, object.KindUnode)
	if err != nil {
		return nil, err
	}
	return decodeUnode(p)
}

// writeUnode stores the unode for directory path at versionKey(path,
// u.Link); base (the previous version of the directory) lets the store keep
// it as a small delta. Unodes are server-internal, so they are keyed by
// version rather than by content hash.
func (r *Repo) writeUnode(path string, u *Unode, base object.ID) (object.ID, error) {
	id := versionKey(path, u.Link)
	return id, r.DB.PutBase(id, object.Pack(object.KindUnode, u.encode()), base)
}

func (u *Unode) find(name string) int {
	i := sort.Search(len(u.Entries), func(i int) bool { return u.Entries[i].Name >= name })
	if i < len(u.Entries) && u.Entries[i].Name == name {
		return i
	}
	return -1
}

// tombstoneDir marks every live entry beneath a directory unode deleted.
func (r *Repo) tombstoneDir(path string, id object.ID, link uint64) (object.ID, error) {
	u, err := r.readUnode(id)
	if err != nil {
		return object.Zero, err
	}
	if u.Deleted {
		return id, nil
	}
	nu := &Unode{Deleted: true, Link: link, Entries: make([]UnodeEntry, len(u.Entries))}
	for i, e := range u.Entries {
		if !e.Deleted {
			if e.Dir {
				cp := join(path, e.Name)
				if _, err := r.tombstoneDir(cp, versionKey(cp, e.Link), link); err != nil {
					return object.Zero, err
				}
			}
			e.Deleted, e.Link = true, link
		}
		nu.Entries[i] = e
	}
	return r.writeUnode(path, nu, id)
}

func (r *Repo) applyUnode(dir string, p1 object.ID, changes []object.Change, link uint64) (object.ID, error) {
	base := &Unode{}
	if !p1.IsZero() {
		var err error
		if base, err = r.readUnode(p1); err != nil {
			return object.Zero, err
		}
	}
	groups, err := groupChanges(changes)
	if err != nil {
		return object.Zero, err
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
					if _, err := r.tombstoneDir(cp, versionKey(cp, cur.Link), link); err != nil {
						return object.Zero, err
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
		var sub object.ID
		if exists && cur.Dir {
			sub = versionKey(cp, cur.Link)
		}
		child, err := r.applyUnode(cp, sub, g.nested, link)
		if err != nil {
			return object.Zero, err
		}
		if child.IsZero() {
			delete(entries, g.name)
			continue
		}
		cu, err := r.readUnode(child)
		if err != nil {
			return object.Zero, err
		}
		entries[g.name] = UnodeEntry{Name: g.name, Dir: true, Deleted: cu.Deleted, Link: link}
	}
	if len(entries) == 0 {
		return object.Zero, nil
	}
	nu := &Unode{Deleted: true, Link: link}
	for _, e := range entries {
		nu.Entries = append(nu.Entries, e)
		if !e.Deleted {
			nu.Deleted = false
		}
	}
	sort.Slice(nu.Entries, func(i, j int) bool { return nu.Entries[i].Name < nu.Entries[j].Name })
	return r.writeUnode(dir, nu, p1)
}

// RootUnode returns the root history unode of a changeset.
func (r *Repo) RootUnode(id object.ID) (object.ID, error) {
	c, err := r.csInfo(id)
	if err != nil || c.Link == 0 {
		return object.Zero, err
	}
	return versionKey("", c.Link), nil
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
	cur, err := r.RootUnode(cs)
	if err != nil {
		return pathVersion{}, err
	}
	if cur.IsZero() {
		return pathVersion{}, notFound
	}
	u, err := r.readUnode(cur)
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
		if u, err = r.readUnode(versionKey(strings.Join(segs[:i+1], "/"), e.Link)); err != nil {
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
