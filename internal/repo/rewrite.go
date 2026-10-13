package repo

import (
	"bytes"
	"errors"
	"fmt"
	"strings"

	"github.com/VishiATChoudhary/graft/internal/diff"
	"github.com/VishiATChoudhary/graft/internal/object"
	"github.com/VishiATChoudhary/graft/internal/store"
)

// ConflictError lists the paths that could not be merged automatically.
type ConflictError struct{ Paths []string }

func (e *ConflictError) Error() string {
	return fmt.Sprintf("conflict in %d path(s): %s", len(e.Paths), strings.Join(e.Paths, ", "))
}

func (e *ConflictError) Unwrap() error { return ErrConflict }

// fileAt returns the file entry at path in root, or a zero entry if absent
// or not a file.
func (r *Repo) fileAt(root object.ID, path string) object.TreeEntry {
	e, err := r.Lookup(root, path)
	if err != nil || e.Mode == object.ModeDir {
		return object.TreeEntry{}
	}
	return e
}

// merge3File merges one path. base/ours/theirs are file entries (Mode
// ModeNone when absent). It returns the resulting change for the path, and
// whether a change is needed at all relative to ours.
func (r *Repo) merge3File(path string, base, ours, theirs object.TreeEntry, labelOurs, labelTheirs string) (object.Change, bool, error) {
	same := func(a, b object.TreeEntry) bool { return a.Mode == b.Mode && a.ID == b.ID }
	asChange := func(e object.TreeEntry) object.Change {
		return object.Change{Path: path, Mode: e.Mode, Blob: e.ID}
	}
	switch {
	case same(ours, theirs):
		return object.Change{}, false, nil
	case same(ours, base):
		return asChange(theirs), true, nil
	case same(theirs, base):
		return object.Change{}, false, nil
	}
	// Both sides changed the path differently.
	if base.Mode == object.ModeNone || ours.Mode == object.ModeNone || theirs.Mode == object.ModeNone ||
		ours.Mode != theirs.Mode || ours.Mode == object.ModeGitlink || ours.Mode == object.ModeSymlink {
		return object.Change{}, false, &ConflictError{Paths: []string{path}}
	}
	bc, err := r.ReadBlob(base.ID)
	if err != nil {
		return object.Change{}, false, err
	}
	oc, err := r.ReadBlob(ours.ID)
	if err != nil {
		return object.Change{}, false, err
	}
	tc, err := r.ReadBlob(theirs.ID)
	if err != nil {
		return object.Change{}, false, err
	}
	if diff.IsBinary(bc) || diff.IsBinary(oc) || diff.IsBinary(tc) {
		return object.Change{}, false, &ConflictError{Paths: []string{path}}
	}
	merged, conflict := diff.Merge3(bc, oc, tc, labelOurs, labelTheirs)
	if conflict {
		return object.Change{}, false, &ConflictError{Paths: []string{path}}
	}
	if bytes.Equal(merged, oc) {
		return object.Change{}, false, nil
	}
	id, err := store.WritePayload(r.DB, object.KindBlob, merged)
	if err != nil {
		return object.Change{}, false, err
	}
	return object.Change{Path: path, Mode: ours.Mode, Blob: id}, true, nil
}

func collectConflicts(err error, acc *[]string) error {
	var ce *ConflictError
	if errors.As(err, &ce) {
		*acc = append(*acc, ce.Paths...)
		return nil
	}
	return err
}

// Rebase replays a single-parent changeset onto a new parent, server-side,
// returning the new changeset ID. Only the paths the changeset touched are
// examined, so the cost is proportional to the change.
func (r *Repo) Rebase(id, onto object.ID) (object.ID, error) {
	cs, err := r.Changeset(id)
	if err != nil {
		return object.Zero, err
	}
	if len(cs.Parents) != 1 {
		return object.Zero, fmt.Errorf("rebase: %s has %d parents; only single-parent changesets can be rebased", id.Short(), len(cs.Parents))
	}
	if cs.Parents[0] == onto {
		return id, nil
	}
	baseRoot, err := r.RootTree(cs.Parents[0])
	if err != nil {
		return object.Zero, err
	}
	ontoRoot, err := r.RootTree(onto)
	if err != nil {
		return object.Zero, err
	}
	out := &object.Changeset{Parents: []object.ID{onto}, Author: cs.Author, Time: cs.Time, TZ: cs.TZ, Message: cs.Message}
	var conflicts []string
	for _, ch := range cs.Changes {
		base := r.fileAt(baseRoot, ch.Path)
		ours := r.fileAt(ontoRoot, ch.Path)
		theirs := object.TreeEntry{Mode: ch.Mode, ID: ch.Blob}
		c, need, err := r.merge3File(ch.Path, base, ours, theirs, "onto "+onto.Short(), id.Short())
		if err = collectConflicts(err, &conflicts); err != nil {
			return object.Zero, err
		}
		if need {
			out.Changes = append(out.Changes, c)
		}
	}
	if len(conflicts) > 0 {
		return object.Zero, &ConflictError{Paths: conflicts}
	}
	return r.append(out)
}

// Merge creates a two-parent changeset joining b into a. If b is already
// contained in a, a is returned; if a is an ancestor of b, b is returned
// (fast-forward).
func (r *Repo) Merge(a, b object.ID, author, message string, when int64) (object.ID, error) {
	if ok, err := r.IsAncestor(b, a); err != nil || ok {
		return a, err
	}
	if ok, err := r.IsAncestor(a, b); err != nil || ok {
		return b, err
	}
	base, err := r.MergeBase(a, b)
	if err != nil {
		return object.Zero, err
	}
	var roots [3]object.ID
	for i, id := range []object.ID{base, a, b} {
		if roots[i], err = r.RootTree(id); err != nil {
			return object.Zero, err
		}
	}
	changed, err := r.DiffTrees(roots[0], roots[2], nil)
	if err != nil {
		return object.Zero, err
	}
	out := &object.Changeset{Parents: []object.ID{a, b}, Author: author, Time: when, Message: message}
	var conflicts []string
	for _, fc := range changed {
		baseE := object.TreeEntry{Mode: fc.OldMode, ID: fc.OldBlob}
		theirs := object.TreeEntry{Mode: fc.NewMode, ID: fc.NewBlob}
		ours := r.fileAt(roots[1], fc.Path)
		c, need, err := r.merge3File(fc.Path, baseE, ours, theirs, a.Short(), b.Short())
		if err = collectConflicts(err, &conflicts); err != nil {
			return object.Zero, err
		}
		if need {
			out.Changes = append(out.Changes, c)
		}
	}
	if len(conflicts) > 0 {
		return object.Zero, &ConflictError{Paths: conflicts}
	}
	return r.append(out)
}

// stackOnto returns the first-parent chain of changesets in head that are
// not on trunk's first-parent chain, oldest first. It uses skip-list jumps,
// so the cost is O(stack length x log(history)), independent of how far
// trunk has advanced.
func (r *Repo) stackOnto(head, trunk object.ID) ([]object.ID, bool, error) {
	tg, err := r.Graph(trunk)
	if err != nil {
		return nil, false, err
	}
	var stack []object.ID
	const maxStack = 10000
	for x := head; len(stack) < maxStack; {
		g, err := r.Graph(x)
		if err != nil {
			return nil, false, err
		}
		if g.Depth <= tg.Depth {
			y, err := r.jump(trunk, g.Depth)
			if err != nil {
				return nil, false, err
			}
			if y == x {
				for i, j := 0, len(stack)-1; i < j; i, j = i+1, j-1 {
					stack[i], stack[j] = stack[j], stack[i]
				}
				return stack, true, nil
			}
		}
		if len(g.Parents) != 1 {
			return nil, false, nil // a merge or a root: not a simple stack
		}
		stack = append(stack, x)
		x = g.Parents[0]
	}
	return nil, false, nil
}

// LandResult reports how a land completed.
type LandResult struct {
	Old      object.ID `json:"old"`
	New      object.ID `json:"new"`
	Rebased  int       `json:"rebased"`
	Attempts int       `json:"attempts"`
	Merged   bool      `json:"merged"`
}

// Land atomically advances ref to include head. If ref has moved past
// head's base, head's stack is rebased server-side onto the current tip
// (or merged, if it is not a simple stack) and the ref is updated with a
// compare-and-swap. There is no global lock or merge queue: concurrent
// lands race on the CAS of a single ref and the loser simply retries.
// check, if set, may veto moving the ref from old to new (authorization).
func (r *Repo) Land(ref string, head object.ID, author string, when int64, maxAttempts int, check ...func(old, new object.ID) error) (*LandResult, error) {
	if maxAttempts <= 0 {
		maxAttempts = 64
	}
	res := &LandResult{}
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		res.Attempts = attempt
		tip, err := r.DB.GetRef(ref)
		if err == object.ErrNotFound {
			tip, err = object.Zero, nil
		}
		if err != nil {
			return nil, err
		}
		res.Old = tip
		target := head
		res.Rebased, res.Merged = 0, false
		if !tip.IsZero() {
			ff, err := r.IsAncestor(tip, head)
			if err != nil {
				return nil, err
			}
			if !ff {
				stack, ok, err := r.stackOnto(head, tip)
				if err != nil {
					return nil, err
				}
				if ok {
					target = tip
					for _, id := range stack {
						if target, err = r.Rebase(id, target); err != nil {
							return nil, err
						}
					}
					res.Rebased = len(stack)
				} else {
					if target, err = r.Merge(tip, head, author, "Merge "+head.Short()+" into "+ref, when); err != nil {
						return nil, err
					}
					res.Merged = true
				}
			}
		}
		for _, c := range check {
			if err := c(tip, target); err != nil {
				return nil, err
			}
		}
		err = r.UpdateRef(ref, tip, target)
		if errors.Is(err, ErrStaleRef) {
			continue
		}
		if err != nil {
			return nil, err
		}
		res.New = target
		return res, nil
	}
	return nil, fmt.Errorf("%w: gave up after %d attempts", ErrStaleRef, maxAttempts)
}
