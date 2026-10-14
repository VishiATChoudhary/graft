// Package derive is graft's generic derived-data mechanism.
//
// Anything that is not part of a changeset itself (root trees, history
// unodes, commit-graph skip lists, blame) is "derived data". Each type is
// defined recursively: its value for a node is computed from the node and its
// parents' values. Types are either computed eagerly when a changeset is
// created or lazily on first use. Because clients never see these
// structures, their internals can change without client changes.
package derive

import (
	"fmt"
	"sync"

	"github.com/VishiATChoudhary/graft/internal/object"
	"github.com/VishiATChoudhary/graft/internal/store"
)

// Store persists derived values.
type Store interface {
	GetDerived(kind string, id object.ID) ([]byte, error)
	PutDerived(kind string, id object.ID, v []byte) error
}

// Type describes one kind of derived data.
type Type struct {
	Name  string
	Eager bool
	// Parents returns the nodes whose values this node's value depends on.
	// For changeset-keyed types these are changeset parents; other types
	// (like blame, keyed by file unode) may walk a different graph.
	Parents func(id object.ID) ([]object.ID, error)
	// Compute derives the value for id given its parents' values, in the
	// order Parents returned them.
	Compute func(id object.ID, parents []object.ID, parentVals [][]byte) ([]byte, error)

	mem *store.MemLRU
}

// Engine computes and memoizes derived data.
type Engine struct {
	st    Store
	mu    sync.Mutex
	types map[string]*Type
	// inflight dedupes concurrent derivations of the same (type, id).
	inflight map[flightKey]*flight
}

type flightKey struct {
	t  string
	id object.ID
}

type flight struct {
	done chan struct{}
	val  []byte
	err  error
}

func NewEngine(st Store) *Engine {
	return &Engine{st: st, types: map[string]*Type{}, inflight: map[flightKey]*flight{}}
}

// Register adds a type. memBudget bounds its in-memory value cache.
func (e *Engine) Register(t *Type, memBudget int64) *Type {
	t.mem = store.NewMemLRU(memBudget)
	e.mu.Lock()
	e.types[t.Name] = t
	e.mu.Unlock()
	return t
}

// EagerTypes returns the types to compute at changeset creation.
func (e *Engine) EagerTypes() []*Type {
	e.mu.Lock()
	defer e.mu.Unlock()
	var out []*Type
	for _, t := range e.types {
		if t.Eager {
			out = append(out, t)
		}
	}
	return out
}

// Peek returns a stored value without deriving it.
func (e *Engine) Peek(t *Type, id object.ID) ([]byte, bool, error) {
	if v, err := t.mem.Get(id); err == nil {
		return v, true, nil
	}
	v, err := e.st.GetDerived(t.Name, id)
	if err == object.ErrNotFound {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	t.mem.Put(id, v)
	return v, true, nil
}

// Derive computes and stores t at id without first looking for a stored
// value: for a changeset just added, where none can exist yet, this saves
// a store lookup per type. Values are deterministic, so recomputing an
// existing one is harmless.
func (e *Engine) Derive(t *Type, id object.ID) ([]byte, error) {
	if v, err := t.mem.Get(id); err == nil {
		return v, nil
	}
	return e.derive(t, id)
}

// Cached returns t at id if it is in memory, without any store lookup.
func (e *Engine) Cached(t *Type, id object.ID) ([]byte, bool) {
	v, err := t.mem.Get(id)
	return v, err == nil
}

// Recompute derives t at id again even if a value is cached, rewriting
// it (values are deterministic). Used to regenerate lost side records.
func (e *Engine) Recompute(t *Type, id object.ID) ([]byte, error) { return e.derive(t, id) }

// Get returns the value of t at id, deriving it (and any missing ancestor
// values) if needed. Derivation is iterative so arbitrarily long histories
// do not overflow the stack.
func (e *Engine) Get(t *Type, id object.ID) ([]byte, error) {
	if v, ok, err := e.Peek(t, id); ok || err != nil {
		return v, err
	}
	key := flightKey{t.Name, id}
	e.mu.Lock()
	if f, ok := e.inflight[key]; ok {
		e.mu.Unlock()
		<-f.done
		return f.val, f.err
	}
	f := &flight{done: make(chan struct{})}
	e.inflight[key] = f
	e.mu.Unlock()

	f.val, f.err = e.derive(t, id)

	e.mu.Lock()
	delete(e.inflight, key)
	e.mu.Unlock()
	close(f.done)
	return f.val, f.err
}

func (e *Engine) derive(t *Type, root object.ID) ([]byte, error) {
	type frame struct {
		id      object.ID
		parents []object.ID
		vals    [][]byte
		next    int
	}
	var stack []*frame
	push := func(id object.ID) error {
		ps, err := t.Parents(id)
		if err != nil {
			return fmt.Errorf("derive %s %s: parents: %w", t.Name, id.Short(), err)
		}
		stack = append(stack, &frame{id: id, parents: ps, vals: make([][]byte, len(ps))})
		return nil
	}
	if err := push(root); err != nil {
		return nil, err
	}
	var result []byte
	for len(stack) > 0 {
		f := stack[len(stack)-1]
		descended := false
		for f.next < len(f.parents) {
			p := f.parents[f.next]
			v, ok, err := e.Peek(t, p)
			if err != nil {
				return nil, err
			}
			if !ok {
				if err := push(p); err != nil {
					return nil, err
				}
				descended = true
				break
			}
			f.vals[f.next] = v
			f.next++
		}
		if descended {
			continue
		}
		v, err := t.Compute(f.id, f.parents, f.vals)
		if err != nil {
			return nil, fmt.Errorf("derive %s %s: %w", t.Name, f.id.Short(), err)
		}
		if err := e.st.PutDerived(t.Name, f.id, v); err != nil {
			return nil, err
		}
		t.mem.Put(f.id, v)
		stack = stack[:len(stack)-1]
		result = v
	}
	return result, nil
}
