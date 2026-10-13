package repo

import (
	"container/heap"
	"fmt"

	"github.com/VishiATChoudhary/graft/internal/object"
)

// GraphNode is the derived commit-graph entry for a changeset.
//
//   - Gen is the generation number: 1 + max(parent generations). An ancestor
//     always has a strictly smaller generation, which bounds graph walks.
//   - Depth is the first-parent depth (root = 0).
//   - Skips[k] is the first-parent ancestor 2^k steps back, giving O(log n)
//     jumps along the trunk.
type GraphNode struct {
	Gen     uint64
	Depth   uint64
	Parents []object.ID
	Skips   []object.ID
}

func encodeGraph(g *GraphNode) []byte {
	var e object.Encoder
	e.Uvarint(g.Gen)
	e.Uvarint(g.Depth)
	e.Uvarint(uint64(len(g.Parents)))
	for _, p := range g.Parents {
		e.ID(p)
	}
	e.Uvarint(uint64(len(g.Skips)))
	for _, s := range g.Skips {
		e.ID(s)
	}
	return e.B
}

func decodeGraph(b []byte) (*GraphNode, error) {
	d := object.Decoder{B: b}
	g := &GraphNode{Gen: d.Uvarint(), Depth: d.Uvarint()}
	n := d.Uvarint()
	for i := uint64(0); i < n && d.Err == nil; i++ {
		g.Parents = append(g.Parents, d.ID())
	}
	n = d.Uvarint()
	for i := uint64(0); i < n && d.Err == nil; i++ {
		g.Skips = append(g.Skips, d.ID())
	}
	return g, d.Err
}

func (r *Repo) computeGraph(id object.ID, parents []object.ID, vals [][]byte) ([]byte, error) {
	g := &GraphNode{Gen: 1, Parents: parents}
	for _, v := range vals {
		pg, err := decodeGraph(v)
		if err != nil {
			return nil, err
		}
		if pg.Gen+1 > g.Gen {
			g.Gen = pg.Gen + 1
		}
	}
	if len(parents) > 0 {
		p1, err := decodeGraph(vals[0])
		if err != nil {
			return nil, err
		}
		g.Depth = p1.Depth + 1
		g.Skips = append(g.Skips, parents[0])
		// skip[k] = skip[k-1] of skip[k-1]. Ancestors are always derived
		// before descendants, so these lookups never recurse deeply.
		for k := 1; ; k++ {
			if uint64(1)<<k > g.Depth {
				break
			}
			mid, err := r.Graph(g.Skips[k-1])
			if err != nil {
				return nil, err
			}
			if len(mid.Skips) < k {
				break
			}
			g.Skips = append(g.Skips, mid.Skips[k-1])
		}
	}
	return encodeGraph(g), nil
}

// Graph returns the commit-graph node for a changeset.
func (r *Repo) Graph(id object.ID) (*GraphNode, error) {
	v, err := r.eng.Get(r.graph, id)
	if err != nil {
		return nil, err
	}
	return decodeGraph(v)
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
		id = g.Skips[k]
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
