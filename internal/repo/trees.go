package repo

import (
	"encoding/binary"
	"fmt"
	"path"
	"sort"
	"strings"

	"github.com/VishiATChoudhary/graft/internal/object"
	"github.com/VishiATChoudhary/graft/internal/store"
)

// splitFirst splits "a/b/c" into "a", "b/c".
func splitFirst(p string) (string, string) {
	if i := strings.IndexByte(p, '/'); i >= 0 {
		return p[:i], p[i+1:]
	}
	return p, ""
}

type group struct {
	name   string
	direct *object.Change
	nested []object.Change
}

// groupChanges buckets changes (paths relative to one directory) by their
// first path component.
func groupChanges(changes []object.Change) ([]*group, error) {
	idx := map[string]*group{}
	var out []*group
	for i := range changes {
		ch := changes[i]
		name, rest := splitFirst(ch.Path)
		g := idx[name]
		if g == nil {
			g = &group{name: name}
			idx[name] = g
			out = append(out, g)
		}
		if rest == "" {
			g.direct = &changes[i]
		} else {
			ch.Path = rest
			g.nested = append(g.nested, ch)
		}
	}
	for _, g := range out {
		if g.direct != nil && len(g.nested) > 0 && g.direct.Mode != object.ModeNone {
			return nil, fmt.Errorf("%w: %q is both a file and a directory", ErrBadChange, g.name)
		}
	}
	return out, nil
}

// applyTree returns the ID of tree base with changes applied. Only the
// directories on changed paths are rewritten: O(changes x depth).
// Directories that become empty are removed; an empty root is Zero.
func (r *Repo) applyTree(base object.ID, changes []object.Change) (object.ID, error) {
	t := &object.Tree{}
	if !base.IsZero() {
		var err error
		if t, err = store.ReadTree(r.DB, base); err != nil {
			return object.Zero, err
		}
	}
	if len(changes) == 0 {
		return base, nil
	}
	groups, err := groupChanges(changes)
	if err != nil {
		return object.Zero, err
	}
	entries := make(map[string]object.TreeEntry, len(t.Entries)+len(groups))
	for _, e := range t.Entries {
		entries[e.Name] = e
	}
	for _, g := range groups {
		cur, exists := entries[g.name]
		if g.direct != nil && len(g.nested) == 0 {
			if g.direct.Mode == object.ModeNone {
				if exists && (cur.Mode == object.ModeFile || cur.Mode == object.ModeExec) {
					// The deleted file's last version has no newer version
					// to delta against; offer similar surviving siblings.
					delete(entries, g.name)
					sib := make([]object.TreeEntry, 0, len(entries))
					for _, e := range entries {
						sib = append(sib, e)
					}
					r.deltifyNewFile(g.name, cur.ID, cur.Size, sib)
					continue
				}
				delete(entries, g.name)
			} else {
				size, err := r.blobSize(g.direct.Blob)
				if err != nil {
					return object.Zero, err
				}
				if exists && cur.Mode != object.ModeDir && cur.ID != g.direct.Blob && g.direct.Mode != object.ModeGitlink {
					// Storage hint, like git: the newest version stays
					// whole and the previous one becomes a reverse delta
					// against it (code mostly grows, so these are small).
					r.reverseDeltify(cur.ID, g.direct.Blob, cur.Size)
				} else if !exists && g.direct.Mode == object.ModeFile {
					sib := make([]object.TreeEntry, 0, len(entries))
					for _, e := range entries {
						sib = append(sib, e)
					}
					r.deltifyNewFile(g.name, g.direct.Blob, size, sib)
				}
				entries[g.name] = object.TreeEntry{Name: g.name, Mode: g.direct.Mode, ID: g.direct.Blob, Size: size}
			}
			continue
		}
		// A delete of the directory itself plus nested writes: start fresh.
		var sub object.ID
		if exists && cur.Mode == object.ModeDir && g.direct == nil {
			sub = cur.ID
		}
		nsub, err := r.applyTree(sub, g.nested)
		if err != nil {
			return object.Zero, err
		}
		if nsub.IsZero() {
			delete(entries, g.name)
		} else {
			entries[g.name] = object.TreeEntry{Name: g.name, Mode: object.ModeDir, ID: nsub}
		}
	}
	if len(entries) == 0 {
		return object.Zero, nil
	}
	nt := &object.Tree{Entries: make([]object.TreeEntry, 0, len(entries))}
	for _, e := range entries {
		nt.Entries = append(nt.Entries, e)
	}
	nt.Sort()
	return store.WritePayloadBase(r.DB, object.KindTree, nt.Encode(), base)
}

// deltifyNewFile picks delta bases for a file with no previous version:
// up to three siblings with the same extension and the closest sizes (a
// cheap stand-in for git's similarity window). Deltify keeps the smallest.
func (r *Repo) deltifyNewFile(name string, blob object.ID, size uint64, siblings []object.TreeEntry) {
	ext := path.Ext(name)
	type cand struct {
		id   object.ID
		dist uint64
	}
	var cs []cand
	for _, e := range siblings {
		if e.Mode != object.ModeFile && e.Mode != object.ModeExec || e.ID == blob || path.Ext(e.Name) != ext {
			continue
		}
		if e.Size*2 < size || size*2 < e.Size {
			continue
		}
		dist := e.Size - size
		if size > e.Size {
			dist = size - e.Size
		}
		cs = append(cs, cand{e.ID, dist})
	}
	sort.Slice(cs, func(i, j int) bool { return cs[i].dist < cs[j].dist })
	for i := 0; i < len(cs) && i < 3; i++ {
		_ = r.DB.Deltify(blob, cs[i].id)
	}
}

// reverseDeltify stores old as a delta against new (like git, the newest
// version stays whole). Chains to ancient versions grow as history does, so
// some versions are kept whole as keyframes, chosen pseudo-randomly by ID
// with probability size/keyframeBudget: the expected bytes rebuilt to read
// any version stay around keyframeBudget, and small files (cheap to
// rebuild) almost never pay for a keyframe.
func (r *Repo) reverseDeltify(old, new object.ID, size uint64) {
	if size > 0 && binary.BigEndian.Uint32(old[1:5])%uint32(max(2, keyframeBudget/size)) == 0 {
		return
	}
	_ = r.DB.Deltify(old, new)
}

const keyframeBudget = 64 << 20

func (r *Repo) blobSize(id object.ID) (uint64, error) {
	n, err := r.DB.PayloadSize(id)
	if err != nil {
		return 0, fmt.Errorf("blob %s: %w", id.Short(), err)
	}
	return n, nil
}

// Lookup resolves a path within a root tree. An empty path returns the root
// as a directory entry.
func (r *Repo) Lookup(root object.ID, path string) (object.TreeEntry, error) {
	path = strings.Trim(path, "/")
	cur := object.TreeEntry{Mode: object.ModeDir, ID: root}
	if path == "" {
		return cur, nil
	}
	for _, seg := range strings.Split(path, "/") {
		if cur.Mode != object.ModeDir || cur.ID.IsZero() {
			return object.TreeEntry{}, fmt.Errorf("%w: path %q", object.ErrNotFound, path)
		}
		t, err := store.ReadTree(r.DB, cur.ID)
		if err != nil {
			return object.TreeEntry{}, err
		}
		i := t.Find(seg)
		if i < 0 {
			return object.TreeEntry{}, fmt.Errorf("%w: path %q", object.ErrNotFound, path)
		}
		cur = t.Entries[i]
	}
	return cur, nil
}

// FileChange describes one path that differs between two trees.
type FileChange struct {
	Path    string      `json:"path"`
	OldMode object.Mode `json:"old_mode"`
	OldBlob object.ID   `json:"old_blob,omitempty"`
	NewMode object.Mode `json:"new_mode"`
	NewBlob object.ID   `json:"new_blob,omitempty"`
}

// DiffTrees lists file-level differences between two trees, restricted to
// the given path prefixes (all paths if empty). Identical subtrees are
// skipped by ID without being read, so cost scales with the difference.
func (r *Repo) DiffTrees(a, b object.ID, prefixes []string) ([]FileChange, error) {
	var out []FileChange
	err := r.diffTrees("", a, b, normPrefixes(prefixes), &out)
	sort.Slice(out, func(i, j int) bool { return out[i].Path < out[j].Path })
	return out, err
}

func normPrefixes(ps []string) []string {
	var out []string
	for _, p := range ps {
		p = strings.Trim(p, "/")
		if p == "" {
			return nil
		}
		out = append(out, p)
	}
	return out
}

// prefixState reports whether dir is inside a prefix (all descendants match)
// or on the way to one (some descendants may match).
func prefixState(dir string, prefixes []string) (inside, toward bool) {
	if len(prefixes) == 0 {
		return true, true
	}
	for _, p := range prefixes {
		if dir == p || strings.HasPrefix(dir, p+"/") {
			return true, true
		}
		if dir == "" || strings.HasPrefix(p, dir+"/") {
			toward = true
		}
	}
	return false, toward
}

func join(dir, name string) string {
	if dir == "" {
		return name
	}
	return dir + "/" + name
}

func (r *Repo) diffTrees(dir string, a, b object.ID, prefixes []string, out *[]FileChange) error {
	if a == b {
		return nil
	}
	inside, toward := prefixState(dir, prefixes)
	if !toward {
		return nil
	}
	ta, tb := &object.Tree{}, &object.Tree{}
	var err error
	if !a.IsZero() {
		if ta, err = store.ReadTree(r.DB, a); err != nil {
			return err
		}
	}
	if !b.IsZero() {
		if tb, err = store.ReadTree(r.DB, b); err != nil {
			return err
		}
	}
	i, j := 0, 0
	for i < len(ta.Entries) || j < len(tb.Entries) {
		var ea, eb *object.TreeEntry
		switch {
		case j >= len(tb.Entries) || (i < len(ta.Entries) && ta.Entries[i].Name < tb.Entries[j].Name):
			ea = &ta.Entries[i]
			i++
		case i >= len(ta.Entries) || tb.Entries[j].Name < ta.Entries[i].Name:
			eb = &tb.Entries[j]
			j++
		default:
			ea, eb = &ta.Entries[i], &tb.Entries[j]
			i++
			j++
		}
		if ea != nil && eb != nil && ea.Mode == eb.Mode && ea.ID == eb.ID {
			continue
		}
		var name string
		if ea != nil {
			name = ea.Name
		} else {
			name = eb.Name
		}
		p := join(dir, name)
		var aSub, bSub object.ID
		var aFile, bFile *object.TreeEntry
		if ea != nil {
			if ea.Mode == object.ModeDir {
				aSub = ea.ID
			} else {
				aFile = ea
			}
		}
		if eb != nil {
			if eb.Mode == object.ModeDir {
				bSub = eb.ID
			} else {
				bFile = eb
			}
		}
		if aFile != nil || bFile != nil {
			if fileInside, _ := prefixState(p, prefixes); inside || fileInside {
				fc := FileChange{Path: p}
				if aFile != nil {
					fc.OldMode, fc.OldBlob = aFile.Mode, aFile.ID
				}
				if bFile != nil {
					fc.NewMode, fc.NewBlob = bFile.Mode, bFile.ID
				}
				*out = append(*out, fc)
			}
		}
		if !aSub.IsZero() || !bSub.IsZero() {
			if err := r.diffTrees(p, aSub, bSub, prefixes, out); err != nil {
				return err
			}
		}
	}
	return nil
}

// ListTree returns the entries of the directory at path.
func (r *Repo) ListTree(root object.ID, path string) ([]object.TreeEntry, error) {
	e, err := r.Lookup(root, path)
	if err != nil {
		return nil, err
	}
	if e.Mode != object.ModeDir {
		return []object.TreeEntry{e}, nil
	}
	if e.ID.IsZero() {
		return nil, nil
	}
	t, err := store.ReadTree(r.DB, e.ID)
	if err != nil {
		return nil, err
	}
	return t.Entries, nil
}

// ReadBlob returns the contents of a blob.
func (r *Repo) ReadBlob(id object.ID) ([]byte, error) {
	return store.ReadPayload(r.DB, id, object.KindBlob)
}

// FindPaths returns up to limit file paths under dir whose path contains q
// (case-insensitive), walking the tree server-side.
func (r *Repo) FindPaths(root object.ID, dir, q string, limit int) ([]string, error) {
	e, err := r.Lookup(root, dir)
	if err != nil {
		return nil, err
	}
	q = strings.ToLower(q)
	var out []string
	var walk func(prefix string, id object.ID) error
	walk = func(prefix string, id object.ID) error {
		t, err := store.ReadTree(r.DB, id)
		if err != nil {
			return err
		}
		for _, en := range t.Entries {
			if limit > 0 && len(out) >= limit {
				return nil
			}
			p := join(prefix, en.Name)
			if en.Mode == object.ModeDir {
				if err := walk(p, en.ID); err != nil {
					return err
				}
			} else if strings.Contains(strings.ToLower(p), q) {
				out = append(out, p)
			}
		}
		return nil
	}
	if e.Mode != object.ModeDir {
		return []string{strings.Trim(dir, "/")}, nil
	}
	if e.ID.IsZero() {
		return nil, nil
	}
	return out, walk(strings.Trim(dir, "/"), e.ID)
}
