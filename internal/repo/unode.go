package repo

import (
	"container/heap"
	"fmt"
	"sort"
	"strings"

	"github.com/VishiATChoudhary/graft/internal/object"
	"github.com/VishiATChoudhary/graft/internal/store"
)

// Unodes are graft's path-history structure (after Mononoke's unodes). Like
// a tree, a unode exists per directory and file, but instead of being keyed
// purely by content it records Link, the changeset that last modified the
// path, and Parents, the previous unode(s) of the same path. Following
// Parents enumerates exactly the changesets that touched a path, without
// scanning unrelated history.
//
// Deletions leave tombstone unodes (Deleted = true) so that history
// survives a delete/re-add cycle and `log` works on deleted paths, matching
// git's path-limited log. CopyFrom records rename sources found by the
// rename-detection index; blame follows it, path log follows it on request.
type Unode struct {
	Dir      bool
	Deleted  bool
	Mode     object.Mode // files only
	Blob     object.ID   // files only; zero when deleted
	Link     object.ID
	Parents  []object.ID
	CopyFrom object.ID    // files only; rename source
	Entries  []UnodeEntry // dirs only, sorted (includes tombstones)
}

type UnodeEntry struct {
	Name    string
	Dir     bool
	Deleted bool
	ID      object.ID
}

const (
	uflagDir     = 1
	uflagDeleted = 2
	uflagCopy    = 4
)

func (u *Unode) encode() []byte {
	var e object.Encoder
	var flags byte
	if u.Dir {
		flags |= uflagDir
	}
	if u.Deleted {
		flags |= uflagDeleted
	}
	if !u.CopyFrom.IsZero() {
		flags |= uflagCopy
	}
	e.Byte(flags)
	if !u.Dir {
		e.Byte(byte(u.Mode))
		e.ID(u.Blob)
	}
	e.ID(u.Link)
	e.Uvarint(uint64(len(u.Parents)))
	for _, p := range u.Parents {
		e.ID(p)
	}
	if flags&uflagCopy != 0 {
		e.ID(u.CopyFrom)
	}
	if u.Dir {
		e.Uvarint(uint64(len(u.Entries)))
		for _, en := range u.Entries {
			e.String(en.Name)
			var f byte
			if en.Dir {
				f |= uflagDir
			}
			if en.Deleted {
				f |= uflagDeleted
			}
			e.Byte(f)
			e.ID(en.ID)
		}
	}
	return e.B
}

func decodeUnode(b []byte) (*Unode, error) {
	d := object.Decoder{B: b}
	flags := d.Byte()
	u := &Unode{Dir: flags&uflagDir != 0, Deleted: flags&uflagDeleted != 0}
	if !u.Dir {
		u.Mode = object.Mode(d.Byte())
		u.Blob = d.ID()
	}
	u.Link = d.ID()
	n := d.Uvarint()
	if n > uint64(len(b)) {
		return nil, object.ErrCorrupt
	}
	for i := uint64(0); i < n && d.Err == nil; i++ {
		u.Parents = append(u.Parents, d.ID())
	}
	if flags&uflagCopy != 0 {
		u.CopyFrom = d.ID()
	}
	if u.Dir {
		n = d.Uvarint()
		if n > uint64(len(b)) {
			return nil, object.ErrCorrupt
		}
		u.Entries = make([]UnodeEntry, 0, n)
		for i := uint64(0); i < n && d.Err == nil; i++ {
			name := d.String()
			f := d.Byte()
			u.Entries = append(u.Entries, UnodeEntry{name, f&uflagDir != 0, f&uflagDeleted != 0, d.ID()})
		}
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

func (r *Repo) writeUnode(u *Unode) (object.ID, error) {
	return store.WritePayload(r.DB, object.KindUnode, u.encode())
}

func (u *Unode) find(name string) int {
	i := sort.Search(len(u.Entries), func(i int) bool { return u.Entries[i].Name >= name })
	if i < len(u.Entries) && u.Entries[i].Name == name {
		return i
	}
	return -1
}

func (r *Repo) computeUnode(id object.ID, parents []object.ID, vals [][]byte) ([]byte, error) {
	cs, err := r.Changeset(id)
	if err != nil {
		return nil, err
	}
	roots := make([]object.ID, len(vals))
	for i, v := range vals {
		copy(roots[i][:], v)
	}
	var p1 object.ID
	var others []object.ID
	if len(roots) > 0 {
		p1, others = roots[0], roots[1:]
	}
	if len(cs.Changes) == 0 && len(parents) <= 1 {
		return p1[:], nil
	}
	var renames map[string]object.ID
	if len(parents) > 0 {
		if renames, err = r.detectRenames(parents[0], p1, cs.Changes); err != nil {
			return nil, err
		}
	}
	u, err := r.applyUnode("", p1, others, cs.Changes, id, renames)
	if err != nil {
		return nil, err
	}
	return u[:], nil
}

// childUnode returns the named child of a dir unode (Zero if absent or of
// the wrong kind), including tombstones.
func (r *Repo) childUnode(dir object.ID, name string, wantDir bool) (object.ID, error) {
	if dir.IsZero() {
		return object.Zero, nil
	}
	u, err := r.readUnode(dir)
	if err != nil {
		return object.Zero, err
	}
	if i := u.find(name); i >= 0 && u.Entries[i].Dir == wantDir {
		return u.Entries[i].ID, nil
	}
	return object.Zero, nil
}

func historyParents(p1 object.ID, others []object.ID) []object.ID {
	var ps []object.ID
	if !p1.IsZero() {
		ps = append(ps, p1)
	}
	for _, o := range others {
		if o.IsZero() {
			continue
		}
		dup := false
		for _, x := range ps {
			dup = dup || x == o
		}
		if !dup {
			ps = append(ps, o)
		}
	}
	return ps
}

// tombstone marks a unode (and, for directories, everything live beneath
// it) as deleted by link. Already-deleted unodes are returned unchanged.
func (r *Repo) tombstone(id object.ID, link object.ID) (object.ID, error) {
	u, err := r.readUnode(id)
	if err != nil {
		return object.Zero, err
	}
	if u.Deleted {
		return id, nil
	}
	nu := &Unode{Dir: u.Dir, Deleted: true, Mode: u.Mode, Link: link, Parents: []object.ID{id}}
	if u.Dir {
		nu.Entries = make([]UnodeEntry, len(u.Entries))
		for i, e := range u.Entries {
			if !e.Deleted {
				if e.ID, err = r.tombstone(e.ID, link); err != nil {
					return object.Zero, err
				}
				e.Deleted = true
			}
			nu.Entries[i] = e
		}
	}
	return r.writeUnode(nu)
}

func (r *Repo) applyUnode(dir string, p1 object.ID, others []object.ID, changes []object.Change, link object.ID, renames map[string]object.ID) (object.ID, error) {
	base := &Unode{Dir: true}
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
		path := join(dir, g.name)
		// Deletion (possibly followed by nested re-creation).
		if g.direct != nil && g.direct.Mode == object.ModeNone {
			if exists && !cur.Deleted {
				tid, err := r.tombstone(cur.ID, link)
				if err != nil {
					return object.Zero, err
				}
				cur = UnodeEntry{g.name, cur.Dir, true, tid}
				entries[g.name] = cur
			}
			if len(g.nested) == 0 {
				continue
			}
		}
		if g.direct != nil && g.direct.Mode != object.ModeNone {
			var fp1 object.ID
			if exists && !cur.Dir {
				fp1 = cur.ID
			}
			var fo []object.ID
			for _, o := range others {
				c, err := r.childUnode(o, g.name, false)
				if err != nil {
					return object.Zero, err
				}
				fo = append(fo, c)
			}
			fu := &Unode{Mode: g.direct.Mode, Blob: g.direct.Blob, Link: link, Parents: historyParents(fp1, fo)}
			if src, ok := renames[path]; ok && (fp1.IsZero() || cur.Deleted) {
				fu.CopyFrom = src
			}
			fid, err := r.writeUnode(fu)
			if err != nil {
				return object.Zero, err
			}
			entries[g.name] = UnodeEntry{g.name, false, false, fid}
			continue
		}
		var sp1 object.ID
		if exists && cur.Dir {
			sp1 = cur.ID
		}
		var so []object.ID
		for _, o := range others {
			c, err := r.childUnode(o, g.name, true)
			if err != nil {
				return object.Zero, err
			}
			so = append(so, c)
		}
		sub, err := r.applyUnode(path, sp1, so, g.nested, link, renames)
		if err != nil {
			return object.Zero, err
		}
		if sub.IsZero() {
			delete(entries, g.name)
			continue
		}
		su, err := r.readUnode(sub)
		if err != nil {
			return object.Zero, err
		}
		entries[g.name] = UnodeEntry{g.name, true, su.Deleted, sub}
	}
	if len(entries) == 0 {
		return object.Zero, nil
	}
	nu := &Unode{Dir: true, Deleted: true, Link: link, Parents: historyParents(p1, others)}
	for _, e := range entries {
		nu.Entries = append(nu.Entries, e)
		if !e.Deleted {
			nu.Deleted = false
		}
	}
	sort.Slice(nu.Entries, func(i, j int) bool { return nu.Entries[i].Name < nu.Entries[j].Name })
	return r.writeUnode(nu)
}

// RootUnode returns the root history unode of a changeset.
func (r *Repo) RootUnode(id object.ID) (object.ID, error) {
	v, err := r.eng.Get(r.unode, id)
	if err != nil {
		return object.Zero, err
	}
	var u object.ID
	copy(u[:], v)
	return u, nil
}

// lookupUnodeIn resolves a path from a root unode. If live is set,
// tombstones are treated as absent.
func (r *Repo) lookupUnodeIn(cur object.ID, path string, live bool) (object.ID, *Unode, error) {
	path = strings.Trim(path, "/")
	notFound := fmt.Errorf("%w: path %q", object.ErrNotFound, path)
	if cur.IsZero() {
		return object.Zero, nil, notFound
	}
	u, err := r.readUnode(cur)
	if err != nil {
		return object.Zero, nil, err
	}
	if path != "" {
		for _, seg := range strings.Split(path, "/") {
			if !u.Dir {
				return object.Zero, nil, notFound
			}
			i := u.find(seg)
			if i < 0 || (live && u.Entries[i].Deleted) {
				return object.Zero, nil, notFound
			}
			cur = u.Entries[i].ID
			if u, err = r.readUnode(cur); err != nil {
				return object.Zero, nil, err
			}
		}
	}
	if live && u.Deleted && path != "" {
		return object.Zero, nil, notFound
	}
	return cur, u, nil
}

func (r *Repo) lookupUnode(cs object.ID, path string, live bool) (object.ID, *Unode, error) {
	root, err := r.RootUnode(cs)
	if err != nil {
		return object.Zero, nil, err
	}
	return r.lookupUnodeIn(root, path, live)
}

// PathLog returns up to limit changesets that modified path (a file or
// directory, live or deleted) at or before rev, newest first. Cost is
// proportional to the number of results, not the length of history. With
// follow, file history continues across detected renames.
func (r *Repo) PathLog(rev object.ID, path string, limit int, follow bool) ([]object.ID, error) {
	start, su, err := r.lookupUnode(rev, path, false)
	if err != nil {
		return nil, err
	}
	g, err := r.Graph(su.Link)
	if err != nil {
		return nil, err
	}
	h := &genHeap{{start, g.Gen}}
	pending := map[object.ID]*Unode{start: su}
	seenUnode := map[object.ID]bool{start: true}
	seenLink := map[object.ID]bool{}
	var out []object.ID
	for h.Len() > 0 && (limit <= 0 || len(out) < limit) {
		it := heap.Pop(h).(genItem)
		u := pending[it.id]
		delete(pending, it.id)
		if !seenLink[u.Link] {
			seenLink[u.Link] = true
			out = append(out, u.Link)
		}
		next := u.Parents
		if follow && !u.CopyFrom.IsZero() {
			next = append(append([]object.ID{}, next...), u.CopyFrom)
		}
		for _, p := range next {
			if seenUnode[p] {
				continue
			}
			seenUnode[p] = true
			pu, err := r.readUnode(p)
			if err != nil {
				return nil, err
			}
			pg, err := r.Graph(pu.Link)
			if err != nil {
				return nil, err
			}
			pending[p] = pu
			heap.Push(h, genItem{p, pg.Gen})
		}
	}
	return out, nil
}
