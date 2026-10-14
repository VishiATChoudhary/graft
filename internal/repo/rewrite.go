package repo

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand/v2"
	"strings"
	"sync"
	"time"

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
	out, err := r.rebased(id, onto)
	if err != nil || out == nil {
		return id, err
	}
	return r.append(out)
}

// rebased builds the changeset replaying id onto onto (nil if id's parent
// already is onto).
func (r *Repo) rebased(id, onto object.ID) (*object.Changeset, error) {
	cs, err := r.Changeset(id)
	if err != nil {
		return nil, err
	}
	if len(cs.Parents) != 1 {
		return nil, fmt.Errorf("rebase: %s has %d parents; only single-parent changesets can be rebased", id.Short(), len(cs.Parents))
	}
	if cs.Parents[0] == onto {
		return nil, nil
	}
	baseRoot, err := r.RootTree(cs.Parents[0])
	if err != nil {
		return nil, err
	}
	ontoRoot, err := r.RootTree(onto)
	if err != nil {
		return nil, err
	}
	out := &object.Changeset{Parents: []object.ID{onto}, Author: cs.Author, Time: cs.Time, TZ: cs.TZ, Message: cs.Message}
	var conflicts []string
	for _, ch := range cs.Changes {
		base := r.fileAt(baseRoot, ch.Path)
		ours := r.fileAt(ontoRoot, ch.Path)
		theirs := object.TreeEntry{Mode: ch.Mode, ID: ch.Blob}
		c, need, err := r.merge3File(ch.Path, base, ours, theirs, "onto "+onto.Short(), id.Short())
		if err = collectConflicts(err, &conflicts); err != nil {
			return nil, err
		}
		if need {
			out.Changes = append(out.Changes, c)
		}
	}
	if len(conflicts) > 0 {
		return nil, &ConflictError{Paths: conflicts}
	}
	return out, nil
}

// rebaseDeferred is Rebase without publishing: the changeset is stored and
// its root tree computed and remembered, but it is not appended to the log
// yet. A land batch appends all of them in one request (appendDeferred).
func (r *Repo) rebaseDeferred(id, onto object.ID) (object.ID, bool, error) {
	out, err := r.rebased(id, onto)
	if err != nil || out == nil {
		return id, false, err
	}
	ontoRoot, err := r.RootTree(onto)
	if err != nil {
		return object.Zero, false, err
	}
	nid, stored := object.Encode(object.KindChangeset, out.Encode())
	if err := r.DB.Put(nid, stored); err != nil {
		return object.Zero, false, err
	}
	root, err := r.applyTree(ontoRoot, out.Changes)
	if err != nil {
		return object.Zero, false, err
	}
	r.rootHints.Store(nid, root)
	return nid, true, nil
}

// logBatcher is implemented by backends that append several changesets
// in one request.
type logBatcher interface {
	AppendLogs(ids []object.ID) ([]uint64, error)
}

// appendDeferred publishes deferred changesets in order and computes their
// eager derived data.
func (r *Repo) appendDeferred(ids []object.ID) error {
	if len(ids) == 0 {
		return nil
	}
	if lb, ok := r.DB.(logBatcher); ok {
		if _, err := lb.AppendLogs(ids); err != nil {
			return err
		}
	} else {
		for _, id := range ids {
			if _, err := r.DB.AppendLog(id); err != nil {
				return err
			}
		}
	}
	for _, id := range ids {
		if err := r.finishAppend(id); err != nil {
			return err
		}
	}
	return nil
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
		g, err := r.stackGraph(x, 64)
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
//
// Lands for the same ref arriving at one server while another is in
// flight are coalesced: one goroutine rebases each onto the previous
// result and publishes them all with a single CAS. Each request is still
// checked and can fail on its own; servers still race only on the CAS.
// Under contention, losers back off for a random, exponentially growing
// interval before retrying. maxAttempts <= 0 means no attempt limit within
// landTimeout.
func (r *Repo) Land(ref string, head object.ID, author string, when int64, maxAttempts int, check ...func(old, new object.ID) error) (*LandResult, error) {
	req := &landReq{head: head, author: author, when: when, max: maxAttempts, check: check, done: make(chan struct{})}
	v, _ := r.landers.LoadOrStore(ref, &lander{})
	l := v.(*lander)
	l.mu.Lock()
	l.pending = append(l.pending, req)
	lead := !l.running
	l.running = true
	l.mu.Unlock()
	if lead {
		r.landLoop(ref, l)
	}
	<-req.done
	return req.res, req.err
}

type landReq struct {
	head     object.ID
	author   string
	when     int64
	max      int
	check    []func(old, new object.ID) error
	attempts int
	res      *LandResult
	err      error
	done     chan struct{}
}

type lander struct {
	mu      sync.Mutex
	pending []*landReq
	running bool
}

func (l *lander) take() []*landReq {
	l.mu.Lock()
	defer l.mu.Unlock()
	p := l.pending
	l.pending = nil
	return p
}

// landLoop lands one batch, then hands leadership to a new goroutine if
// more requests arrived, so the caller returns promptly.
func (r *Repo) landLoop(ref string, l *lander) {
	r.landBatch(ref, l, l.take())
	l.mu.Lock()
	if len(l.pending) == 0 {
		l.running = false
		l.mu.Unlock()
		return
	}
	l.mu.Unlock()
	go r.landLoop(ref, l)
}

func (r *Repo) landBatch(ref string, l *lander, batch []*landReq) {
	finish := func(q *landReq, res *LandResult, err error) {
		q.res, q.err = res, err
		close(q.done)
	}
	deadline := time.Now().Add(landTimeout)
	for attempt := 1; len(batch) > 0; attempt++ {
		if attempt > 1 {
			if time.Now().After(deadline) {
				for _, q := range batch {
					finish(q, nil, fmt.Errorf("%w: gave up after %d attempts", ErrStaleRef, q.attempts))
				}
				return
			}
			backoff := time.Duration(1<<min(attempt-2, 6)) * time.Millisecond
			time.Sleep(time.Duration(rand.Int64N(int64(backoff) + 1)))
			batch = append(batch, l.take()...) // late arrivals join the retry
		}
		tip, err := r.DB.GetRef(ref)
		if err == object.ErrNotFound {
			tip, err = object.Zero, nil
		}
		if err != nil {
			for _, q := range batch {
				finish(q, nil, err)
			}
			return
		}
		r.prefetchLands(tip, batch)
		cur, logged := tip, tip
		var deferred []object.ID
		flush := func() error {
			err := r.appendDeferred(deferred)
			deferred = deferred[:0]
			logged = cur
			return err
		}
		var ok []*landReq
		var results []*LandResult
		for _, q := range batch {
			q.attempts++
			if len(deferred) == 0 {
				logged = cur
			}
			target, rebased, merged, newly, err := r.landOnto(logged, cur, q.head, q.author, q.when, ref, len(deferred) > 0, flush)
			deferred = append(deferred, newly...)
			if err == nil {
				for _, c := range q.check {
					if err = c(cur, target); err != nil {
						break
					}
				}
			}
			if err != nil {
				finish(q, nil, err)
				continue
			}
			ok = append(ok, q)
			results = append(results, &LandResult{Old: cur, New: target, Rebased: rebased, Merged: merged})
			cur = target
		}
		if err := flush(); err != nil {
			for _, q := range ok {
				finish(q, nil, err)
			}
			return
		}
		if len(ok) == 0 {
			return
		}
		err = r.UpdateRef(ref, tip, cur)
		if err == nil {
			for i, q := range ok {
				results[i].Attempts = q.attempts
				finish(q, results[i], nil)
			}
			return
		}
		if !errors.Is(err, ErrStaleRef) {
			for _, q := range ok {
				finish(q, nil, err)
			}
			return
		}
		batch = batch[:0]
		for _, q := range ok {
			if q.max > 0 && q.attempts >= q.max {
				finish(q, nil, fmt.Errorf("%w: gave up after %d attempts", ErrStaleRef, q.attempts))
				continue
			}
			batch = append(batch, q)
		}
	}
}

// stackGraph returns x's depth and parents for stack walks. A changeset
// just created for landing has no derived record yet, and computing one
// (root tree, history) would be wasted on a changeset about to be rebased;
// its depth is its parent's plus one. Falls back to the full record when x
// is not a simple child or the walk gets long.
func (r *Repo) stackGraph(x object.ID, budget int) (*GraphNode, error) {
	if g := r.nodes.get(x); g != nil {
		return g, nil
	}
	if budget > 0 {
		if _, ok := r.eng.Cached(r.cs, x); !ok {
			if cs, err := r.Changeset(x); err == nil && len(cs.Parents) == 1 {
				pg, err := r.stackGraph(cs.Parents[0], budget-1)
				if err != nil {
					return nil, err
				}
				return &GraphNode{Gen: pg.Gen + 1, Depth: pg.Depth + 1, Parents: cs.Parents}, nil
			}
		}
	}
	return r.Graph(x)
}

// prefetchLands reads, in parallel, what rebasing each request onto tip
// will need (graph records, the stacked changesets, their base trees), so
// the serial rebase chain inside a batch runs from cache. On a sharded
// backend this turns a few sequential round trips per request into a
// handful for the whole batch.
func (r *Repo) prefetchLands(tip object.ID, batch []*landReq) {
	if tip.IsZero() || len(batch) < 2 {
		return
	}
	sem := make(chan struct{}, 32)
	var wg sync.WaitGroup
	for _, q := range batch {
		wg.Add(1)
		sem <- struct{}{}
		go func(head object.ID) {
			defer func() { <-sem; wg.Done() }()
			stack, ok, err := r.stackOnto(head, tip)
			if err != nil || !ok {
				return
			}
			for _, id := range stack {
				cs, err := r.Changeset(id)
				if err != nil || len(cs.Parents) != 1 {
					return
				}
				base, err := r.RootTree(cs.Parents[0])
				if err != nil {
					return
				}
				for _, ch := range cs.Changes {
					r.fileAt(base, ch.Path)
				}
			}
		}(q.head)
	}
	wg.Wait()
}

// landOnto computes the changeset that lands head on top of cur. logged
// is the newest logged changeset on the batch's chain (cur itself unless
// pending is set, meaning cur and some of its ancestors are deferred
// rebases not yet in the log). Rebases are deferred and returned in newly;
// anything needing cur's ancestry (a merge) flushes the deferred ones first.
func (r *Repo) landOnto(logged, cur, head object.ID, author string, when int64, ref string, pending bool, flush func() error) (target object.ID, rebased int, merged bool, newly []object.ID, err error) {
	if cur.IsZero() {
		return head, 0, false, nil, nil
	}
	stack, ok, err := r.stackOnto(head, logged)
	if err != nil {
		return object.Zero, 0, false, nil, err
	}
	if ok && len(stack) > 0 && !pending {
		// Fast-forward: head's own changesets already sit on cur.
		first, err := r.Changeset(stack[0])
		if err != nil {
			return object.Zero, 0, false, nil, err
		}
		if len(first.Parents) == 1 && first.Parents[0] == cur {
			return head, 0, false, nil, nil
		}
	}
	if !ok {
		if pending {
			if err := flush(); err != nil {
				return object.Zero, 0, false, nil, err
			}
		}
		target, err = r.Merge(cur, head, author, "Merge "+head.Short()+" into "+ref, when)
		return target, 0, true, nil, err
	}
	target = cur
	for _, id := range stack {
		nid, isNew, err := r.rebaseDeferred(id, target)
		if err != nil {
			return object.Zero, 0, false, newly, err
		}
		if isNew {
			newly = append(newly, nid)
		}
		target = nid
	}
	return target, len(stack), false, newly, nil
}

// landTimeout bounds how long a land keeps retrying under contention.
const landTimeout = 2 * time.Minute
