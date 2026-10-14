package repo

import (
	"container/heap"
	"fmt"
	"sync"

	"github.com/VishiATChoudhary/graft/internal/object"
)

// GraphNode is the derived commit-graph entry for a changeset.
//
//   - Gen is the generation number: 1 + max(parent generations). An ancestor
//     always has a strictly smaller generation, which bounds graph walks.
//   - Depth is the first-parent depth (root = 0).
//   - Skips[k] is the log sequence number of the first-parent ancestor 2^k
//     steps back, giving O(log n) jumps along the trunk. Sequence numbers
//     (a few bytes as varints) rather than 32-byte IDs keep the commit graph
//     small.
type GraphNode struct {
	Gen     uint64
	Depth   uint64
	Parents []object.ID
	Skips   []uint64
}

// csInfo is the single derived record kept per changeset: its root tree,
// the link (log sequence) of its root history unode, and its commit-graph
// entry. Parents and skips are log sequence numbers, so the record is a few
// dozen bytes.
type csInfo struct {
	Root       object.ID
	Link       uint64 // 0: empty history
	Gen, Depth uint64
	Parents    []uint64
	Skips      []uint64
}

func (c *csInfo) encode() []byte {
	var e object.Encoder
	if c.Root.IsZero() {
		e.Byte(0)
	} else {
		e.Byte(1)
		e.ID(c.Root)
	}
	e.Uvarint(c.Link)
	e.Uvarint(c.Gen)
	e.Uvarint(c.Depth)
	e.Uvarint(uint64(len(c.Parents)))
	for _, p := range c.Parents {
		e.Uvarint(p)
	}
	e.Uvarint(uint64(len(c.Skips)))
	for _, s := range c.Skips {
		e.Uvarint(s)
	}
	return e.B
}

func decodeCS(b []byte) (*csInfo, error) {
	d := object.Decoder{B: b}
	c := &csInfo{}
	if d.Byte() == 1 {
		c.Root = d.ID()
	}
	c.Link, c.Gen, c.Depth = d.Uvarint(), d.Uvarint(), d.Uvarint()
	n := d.Uvarint()
	if n > uint64(len(b)) {
		return nil, object.ErrCorrupt
	}
	for i := uint64(0); i < n && d.Err == nil; i++ {
		c.Parents = append(c.Parents, d.Uvarint())
	}
	n = d.Uvarint()
	if n > uint64(len(b)) {
		return nil, object.ErrCorrupt
	}
	for i := uint64(0); i < n && d.Err == nil; i++ {
		c.Skips = append(c.Skips, d.Uvarint())
	}
	return c, d.Err
}

// computeCS derives a changeset's record from its parents' records.
func (r *Repo) computeCS(id object.ID, parents []object.ID, vals [][]byte) ([]byte, error) {
	cs, err := r.Changeset(id)
	if err != nil {
		return nil, err
	}
	seq, err := r.DB.LogSeq(id)
	if err != nil {
		return nil, err
	}
	ps := make([]*csInfo, len(vals))
	for i, v := range vals {
		if ps[i], err = decodeCS(v); err != nil {
			return nil, err
		}
	}
	c := &csInfo{Gen: 1}
	for i, p := range parents {
		s, err := r.DB.LogSeq(p)
		if err != nil {
			return nil, err
		}
		c.Parents = append(c.Parents, s)
		if ps[i].Gen+1 > c.Gen {
			c.Gen = ps[i].Gen + 1
		}
	}
	var p1 *csInfo
	if len(ps) > 0 {
		p1 = ps[0]
		c.Depth = p1.Depth + 1
		c.Skips = append(c.Skips, c.Parents[0])
		// Deterministic skip list: a changeset at first-parent depth d keeps
		// skips 2^k back only for 2^k dividing d (about two per changeset
		// on average), which still allows O(log n) jumps. skip[k] is
		// skip[k-1] of skip[k-1]; that ancestor's depth is divisible by
		// 2^(k-1), so it has the pointer needed.
		for k := 1; uint64(1)<<k <= c.Depth && c.Depth%(uint64(1)<<k) == 0; k++ {
			mid, err := r.graphAtSeq(c.Skips[k-1])
			if err != nil {
				return nil, err
			}
			if len(mid.Skips) < k {
				break
			}
			c.Skips = append(c.Skips, mid.Skips[k-1])
		}
	}
	// Root tree.
	var baseRoot object.ID
	if p1 != nil {
		baseRoot = p1.Root
	}
	if v, ok := r.rootHints.Load(id); ok { // computed when it was rebased
		c.Root = v.(object.ID)
		defer r.rootHints.Delete(id)
	} else if c.Root, err = r.applyTree(baseRoot, cs.Changes); err != nil {
		return nil, err
	}
	// Merges: the other parents' versions of merged paths would otherwise
	// never get a newer version to be stored as deltas against.
	for _, p := range ps[min(1, len(ps)):] {
		for _, ch := range cs.Changes {
			if ch.Mode != object.ModeFile && ch.Mode != object.ModeExec {
				continue
			}
			if e := r.fileAt(p.Root, ch.Path); (e.Mode == object.ModeFile || e.Mode == object.ModeExec) && e.ID != ch.Blob {
				r.reverseDeltify(e.ID, ch.Blob, e.Size)
			}
		}
	}
	// History unodes.
	var p1Link uint64
	if p1 != nil {
		c.Link = p1.Link
		p1Link = p1.Link
	}
	if len(cs.Changes) > 0 {
		if len(parents) > 0 {
			if err := r.recordRenames(parents[0], seq, cs.Changes); err != nil {
				return nil, err
			}
		}
		b := newBatch(seq)
		present, _, err := r.applyUnode("", p1Link, cs.Changes, seq, b)
		if err != nil {
			return nil, err
		}
		if err := r.writeBatch(b); err != nil {
			return nil, err
		}
		c.Link = 0
		if present {
			c.Link = seq
		}
	}
	return c.encode(), nil
}

func (r *Repo) csInfo(id object.ID) (*csInfo, error) {
	v, err := r.eng.Get(r.cs, id)
	if err != nil {
		return nil, err
	}
	return decodeCS(v)
}

// Graph returns the commit-graph node for a changeset, with parents as IDs.
func (r *Repo) Graph(id object.ID) (*GraphNode, error) {
	if g := r.nodes.get(id); g != nil {
		return g, nil
	}
	c, err := r.csInfo(id)
	if err != nil {
		return nil, err
	}
	g := &GraphNode{Gen: c.Gen, Depth: c.Depth, Skips: c.Skips}
	for _, s := range c.Parents {
		pid, err := r.DB.LogID(s)
		if err != nil {
			return nil, err
		}
		g.Parents = append(g.Parents, pid)
	}
	r.nodes.put(id, g)
	return g, nil
}

// nodeCache keeps decoded graph nodes (parents resolved to IDs) for graph
// walks. It is bounded; eviction is arbitrary, which suits a cache of
// immutable values.
type nodeCache struct {
	mu sync.Mutex
	m  map[object.ID]*GraphNode
}

const nodeCacheMax = 1 << 20

func (c *nodeCache) get(id object.ID) *GraphNode {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.m[id]
}

func (c *nodeCache) put(id object.ID, g *GraphNode) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.m == nil {
		c.m = map[object.ID]*GraphNode{}
	}
	if len(c.m) >= nodeCacheMax {
		for k := range c.m {
			delete(c.m, k)
			if len(c.m) < nodeCacheMax*3/4 {
				break
			}
		}
	}
	c.m[id] = g
}

func (r *Repo) graphAtSeq(seq uint64) (*GraphNode, error) {
	id, err := r.DB.LogID(seq)
	if err != nil {
		return nil, err
	}
	return r.Graph(id)
}

// jump walks the first-parent chain from id back to the given depth.
func (r *Repo) jump(id object.ID, depth uint64) (object.ID, error) {
	g, err := r.Graph(id)
	if err != nil {
		return object.Zero, err
	}
	for g.Depth > depth {
		dist := g.Depth - depth
		k := 0
		for k+1 < len(g.Skips) && uint64(1)<<(k+1) <= dist {
			k++
		}
		if id, err = r.DB.LogID(g.Skips[k]); err != nil {
			return object.Zero, err
		}
		if g, err = r.Graph(id); err != nil {
			return object.Zero, err
		}
	}
	return id, nil
}

// IsAncestor reports whether a is an ancestor of (or equal to) b.
func (r *Repo) IsAncestor(a, b object.ID) (bool, error) {
	if a == b {
		return true, nil
	}
	ga, err := r.Graph(a)
	if err != nil {
		return false, err
	}
	gb, err := r.Graph(b)
	if err != nil {
		return false, err
	}
	if ga.Gen >= gb.Gen {
		return false, nil
	}
	// Fast path: a on b's first-parent chain (the trunk), O(log n).
	if ga.Depth < gb.Depth {
		x, err := r.jump(b, ga.Depth)
		if err != nil {
			return false, err
		}
		if x == a {
			return true, nil
		}
	}
	// General case: walk b's ancestry, pruning anything older than a.
	seen := map[object.ID]bool{b: true}
	stack := []object.ID{b}
	for len(stack) > 0 {
		x := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		gx, err := r.Graph(x)
		if err != nil {
			return false, err
		}
		for _, p := range gx.Parents {
			if p == a {
				return true, nil
			}
			if seen[p] {
				continue
			}
			seen[p] = true
			gp, err := r.Graph(p)
			if err != nil {
				return false, err
			}
			if gp.Gen > ga.Gen {
				stack = append(stack, p)
			}
		}
	}
	return false, nil
}

type genItem struct {
	id  object.ID
	gen uint64
}

type genHeap []genItem

func (h genHeap) Len() int            { return len(h) }
func (h genHeap) Less(i, j int) bool  { return h[i].gen > h[j].gen }
func (h genHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *genHeap) Push(x interface{}) { *h = append(*h, x.(genItem)) }
func (h *genHeap) Pop() interface{} {
	old := *h
	x := old[len(old)-1]
	*h = old[:len(old)-1]
	return x
}

// MergeBases returns the best common ancestors of a and b (usually one).
func (r *Repo) MergeBases(a, b object.ID) ([]object.ID, error) {
	if ok, err := r.IsAncestor(a, b); err != nil || ok {
		return []object.ID{a}, err
	}
	if ok, err := r.IsAncestor(b, a); err != nil || ok {
		return []object.ID{b}, err
	}
	const (
		fa    = 1
		fb    = 2
		stale = 4
	)
	flags := map[object.ID]uint8{}
	h := &genHeap{}
	hasNonStale := func() bool {
		for _, it := range *h {
			if flags[it.id]&stale == 0 {
				return true
			}
		}
		return false
	}
	push := func(id object.ID, f uint8) error {
		old, had := flags[id]
		if had && old&f == f {
			return nil
		}
		g, err := r.Graph(id)
		if err != nil {
			return err
		}
		flags[id] = old | f
		heap.Push(h, genItem{id, g.Gen})
		return nil
	}
	if err := push(a, fa); err != nil {
		return nil, err
	}
	if err := push(b, fb); err != nil {
		return nil, err
	}
	var results []object.ID
	inResult := map[object.ID]bool{}
	for h.Len() > 0 && hasNonStale() {
		it := heap.Pop(h).(genItem)
		f := flags[it.id]
		if f&(fa|fb) == fa|fb && f&stale == 0 {
			if !inResult[it.id] {
				inResult[it.id] = true
				results = append(results, it.id)
			}
			f |= stale
			flags[it.id] = f
		}
		g, err := r.Graph(it.id)
		if err != nil {
			return nil, err
		}
		for _, p := range g.Parents {
			if err := push(p, f); err != nil {
				return nil, err
			}
		}
	}
	// Drop results that are ancestors of other results.
	var best []object.ID
	for i, x := range results {
		redundant := false
		for j, y := range results {
			if i == j {
				continue
			}
			if ok, err := r.IsAncestor(x, y); err != nil {
				return nil, err
			} else if ok {
				redundant = true
				break
			}
		}
		if !redundant {
			best = append(best, x)
		}
	}
	if len(best) == 0 {
		return nil, fmt.Errorf("%w: no common ancestor", object.ErrNotFound)
	}
	return best, nil
}

// MergeBase returns a single best common ancestor.
func (r *Repo) MergeBase(a, b object.ID) (object.ID, error) {
	bs, err := r.MergeBases(a, b)
	if err != nil {
		return object.Zero, err
	}
	return bs[0], nil
}

// Log walks history from rev in generation order (newest first), returning
// up to limit changeset IDs. If firstParent, only the trunk is followed.
func (r *Repo) Log(rev object.ID, limit int, firstParent bool) ([]object.ID, error) {
	var out []object.ID
	if firstParent {
		for id := rev; !id.IsZero() && (limit <= 0 || len(out) < limit); {
			out = append(out, id)
			g, err := r.Graph(id)
			if err != nil {
				return nil, err
			}
			if len(g.Parents) == 0 {
				break
			}
			id = g.Parents[0]
		}
		return out, nil
	}
	g, err := r.Graph(rev)
	if err != nil {
		return nil, err
	}
	h := &genHeap{{rev, g.Gen}}
	seen := map[object.ID]bool{rev: true}
	for h.Len() > 0 && (limit <= 0 || len(out) < limit) {
		it := heap.Pop(h).(genItem)
		out = append(out, it.id)
		g, err := r.Graph(it.id)
		if err != nil {
			return nil, err
		}
		for _, p := range g.Parents {
			if seen[p] {
				continue
			}
			seen[p] = true
			pg, err := r.Graph(p)
			if err != nil {
				return nil, err
			}
			heap.Push(h, genItem{p, pg.Gen})
		}
	}
	return out, nil
}
