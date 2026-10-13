// Package repo implements the authoritative server-side repository: the
// changeset log, derived data, refs, and every history/rewrite operation.
// Clients compute almost nothing locally; they ask this package (via the API)
// for history, blame, diffs, merge bases, and to create, rebase, merge and
// land changesets.
package repo

import (
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/VishiATChoudhary/graft/internal/derive"
	"github.com/VishiATChoudhary/graft/internal/object"
	"github.com/VishiATChoudhary/graft/internal/store"
)

// Repo is one repository backed by a store.DB.
type Repo struct {
	DB  *store.DB
	eng *derive.Engine

	root  *derive.Type // changeset -> root tree ID
	graph *derive.Type // changeset -> generation, first-parent depth, parents, skip list
	unode *derive.Type // changeset -> root history unode
	blame *derive.Type // file unode -> blame runs (lazy)

	seq      atomic.Uint64
	refLocks sync.Map // name -> *sync.Mutex; CAS is per ref, never global
}

var (
	ErrConflict  = errors.New("conflict")
	ErrStaleRef  = errors.New("ref changed concurrently")
	ErrBadRev    = errors.New("unknown revision")
	ErrMissing   = errors.New("missing object")
	ErrBadChange = errors.New("invalid change")
)

// Open opens a repository over an existing DB.
func Open(db *store.DB) (*Repo, error) {
	r := &Repo{DB: db, eng: derive.NewEngine(db)}
	csParents := func(id object.ID) ([]object.ID, error) {
		cs, err := r.Changeset(id)
		if err != nil {
			return nil, err
		}
		return cs.Parents, nil
	}
	r.graph = r.eng.Register(&derive.Type{Name: "graph", Eager: true, Parents: csParents, Compute: r.computeGraph}, 64<<20)
	r.root = r.eng.Register(&derive.Type{Name: "root", Eager: true, Parents: csParents, Compute: r.computeRoot}, 16<<20)
	r.unode = r.eng.Register(&derive.Type{Name: "unode", Eager: true, Parents: csParents, Compute: r.computeUnode}, 16<<20)
	r.blame = r.eng.Register(&derive.Type{Name: "blame", Eager: false, Parents: r.blameParents, Compute: r.computeBlame}, 128<<20)
	last, err := db.LastLogSeq()
	if err != nil {
		return nil, err
	}
	r.seq.Store(last)
	return r, nil
}

// Changeset loads a changeset by ID.
func (r *Repo) Changeset(id object.ID) (*object.Changeset, error) {
	cs, err := store.ReadChangeset(r.DB, id)
	if err == object.ErrNotFound {
		return nil, fmt.Errorf("%w: changeset %s", ErrBadRev, id.Short())
	}
	return cs, err
}

// LogSize is the number of changesets in the write-ahead log.
func (r *Repo) LogSize() uint64 { return r.seq.Load() }

// Create validates and appends a changeset to the log, then computes eager
// derived data. Cost scales with the size of the change, not the repo.
// It takes no global lock: concurrent creations proceed in parallel.
func (r *Repo) Create(cs *object.Changeset) (object.ID, error) {
	if err := cs.Normalize(); err != nil {
		return object.Zero, fmt.Errorf("%w: %v", ErrBadChange, err)
	}
	for _, p := range cs.Parents {
		if _, err := r.DB.LogSeq(p); err != nil {
			return object.Zero, fmt.Errorf("%w: parent %s", ErrBadRev, p.Short())
		}
	}
	for _, ch := range cs.Changes {
		if ch.Mode == object.ModeNone || ch.Mode == object.ModeGitlink {
			continue
		}
		ok, err := r.DB.Has(ch.Blob)
		if err != nil {
			return object.Zero, err
		}
		if !ok {
			return object.Zero, fmt.Errorf("%w: blob %s for %s", ErrMissing, ch.Blob.Short(), ch.Path)
		}
	}
	return r.append(cs)
}

// append stores a pre-validated changeset.
func (r *Repo) append(cs *object.Changeset) (object.ID, error) {
	id, stored := object.Encode(object.KindChangeset, cs.Encode())
	if _, err := r.DB.LogSeq(id); err == nil {
		return id, nil // already in the log
	}
	if err := r.DB.Put(id, stored); err != nil {
		return object.Zero, err
	}
	for _, t := range r.eng.EagerTypes() {
		if _, err := r.eng.Get(t, id); err != nil {
			return object.Zero, err
		}
	}
	if err := r.DB.AppendLog(r.seq.Add(1), id); err != nil {
		return object.Zero, err
	}
	return id, nil
}

// RootTree returns the root tree of a changeset (Zero for an empty tree).
func (r *Repo) RootTree(id object.ID) (object.ID, error) {
	v, err := r.eng.Get(r.root, id)
	if err != nil {
		return object.Zero, err
	}
	var t object.ID
	copy(t[:], v)
	return t, nil
}

func (r *Repo) computeRoot(id object.ID, parents []object.ID, vals [][]byte) ([]byte, error) {
	cs, err := r.Changeset(id)
	if err != nil {
		return nil, err
	}
	var base object.ID
	if len(vals) > 0 {
		copy(base[:], vals[0])
	}
	t, err := r.applyTree(base, cs.Changes)
	if err != nil {
		return nil, err
	}
	return t[:], nil
}

// Refs.

func (r *Repo) refLock(name string) *sync.Mutex {
	m, _ := r.refLocks.LoadOrStore(name, &sync.Mutex{})
	return m.(*sync.Mutex)
}

// UpdateRef performs a compare-and-swap on a single ref. old == Zero means
// the ref must not exist; new == Zero deletes it.
func (r *Repo) UpdateRef(name string, old, new object.ID) error {
	if err := validRefName(name); err != nil {
		return err
	}
	if !new.IsZero() {
		if _, err := r.DB.LogSeq(new); err != nil {
			return fmt.Errorf("%w: %s", ErrBadRev, new.Short())
		}
	}
	mu := r.refLock(name)
	mu.Lock()
	defer mu.Unlock()
	cur, err := r.DB.GetRef(name)
	if err == object.ErrNotFound {
		cur, err = object.Zero, nil
	}
	if err != nil {
		return err
	}
	if cur != old {
		return fmt.Errorf("%w: %s is %s, expected %s", ErrStaleRef, name, cur.Short(), old.Short())
	}
	return r.DB.SetRef(name, new)
}

func (r *Repo) Ref(name string) (object.ID, error) { return r.DB.GetRef(name) }

func (r *Repo) Refs() (map[string]object.ID, error) { return r.DB.ListRefs() }

func validRefName(name string) error {
	if name == "" || strings.ContainsAny(name, " ~^:\\\x00") || strings.Contains(name, "..") || len(name) > 255 {
		return fmt.Errorf("invalid ref name %q", name)
	}
	return nil
}

// Resolve turns a revision expression into a changeset ID. Supported forms:
// a ref name, a full or abbreviated (>= 8 hex chars) ID, and a "~N" suffix
// for the Nth first-parent ancestor (resolved in O(log N) via skip lists).
func (r *Repo) Resolve(rev string) (object.ID, error) {
	back := 0
	if i := strings.LastIndexByte(rev, '~'); i >= 0 {
		n := 1
		if s := rev[i+1:]; s != "" {
			v, err := strconv.Atoi(s)
			if err != nil || v < 0 {
				return object.Zero, fmt.Errorf("%w: %q", ErrBadRev, rev)
			}
			n = v
		}
		back, rev = n, rev[:i]
	}
	id, err := r.resolveBase(rev)
	if err != nil {
		return object.Zero, err
	}
	if back == 0 {
		return id, nil
	}
	g, err := r.Graph(id)
	if err != nil {
		return object.Zero, err
	}
	if uint64(back) > g.Depth {
		return object.Zero, fmt.Errorf("%w: %q goes past the root", ErrBadRev, rev)
	}
	return r.jump(id, g.Depth-uint64(back))
}

func (r *Repo) resolveBase(rev string) (object.ID, error) {
	if id, err := r.DB.GetRef(rev); err == nil {
		return id, nil
	}
	if len(rev) == 64 {
		id, err := object.ParseID(rev)
		if err == nil {
			if _, err := r.DB.LogSeq(id); err == nil {
				return id, nil
			}
		}
	}
	if len(rev) >= 8 && len(rev) < 64 {
		if id, err := r.resolvePrefix(rev); err == nil {
			return id, nil
		}
	}
	return object.Zero, fmt.Errorf("%w: %q", ErrBadRev, rev)
}

func (r *Repo) resolvePrefix(p string) (object.ID, error) {
	even := p[:len(p)&^1]
	pb, err := hex.DecodeString(even)
	if err != nil {
		return object.Zero, err
	}
	var found []object.ID
	err = r.DB.ScanLogIdxPrefix(pb, func(id object.ID) bool {
		if strings.HasPrefix(id.String(), p) {
			found = append(found, id)
		}
		return len(found) < 2
	})
	if err != nil {
		return object.Zero, err
	}
	if len(found) != 1 {
		return object.Zero, fmt.Errorf("%w: ambiguous or unknown prefix %q", ErrBadRev, p)
	}
	return found[0], nil
}
