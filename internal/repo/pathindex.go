package repo

import (
	"encoding/binary"
	"errors"
	"slices"
	"strings"

	"github.com/VishiATChoudhary/graft/internal/object"
	"github.com/VishiATChoudhary/graft/internal/store"
)

// Path search index. Each tree has, as derived data keyed by its ID, the
// exact set of (lowercased) trigrams of every entry name in its subtree.
// A path query without "/" must lie inside one path component, so a
// subtree whose names lack one of the query's trigrams cannot hold a match
// unless an enclosing directory's name matches, and is skipped. Sets are
// built bottom-up from the children's sets, so a new revision computes
// them only for directories it changed.

const nameTrigramKind = "nt"

func (r *Repo) nameTrigrams(id object.ID) ([]uint32, error) {
	if v, err := r.blooms.Get(nameKey(id)); err == nil {
		return decodeSet(v), nil
	}
	if v, err := r.DB.GetDerived(nameTrigramKind, id); err == nil {
		r.blooms.Put(nameKey(id), v)
		return decodeSet(v), nil
	} else if !errors.Is(err, object.ErrNotFound) {
		return nil, err
	}
	t, err := store.ReadTree(r.DB, id)
	if err != nil {
		return nil, err
	}
	var set []uint32
	for _, e := range t.Entries {
		trigramsOf([]byte(e.Name), func(x uint32) { set = append(set, x) })
		if e.Mode == object.ModeDir {
			sub, err := r.nameTrigrams(e.ID)
			if err != nil {
				return nil, err
			}
			set = append(set, sub...)
		}
	}
	slices.Sort(set)
	set = slices.Compact(set)
	v := encodeSet(set)
	r.blooms.Put(nameKey(id), v)
	if err := r.DB.PutDerived(nameTrigramKind, id, v); err != nil {
		return nil, err
	}
	return set, nil
}

// nameKey keeps name sets apart from content filters in the shared cache.
func nameKey(id object.ID) object.ID {
	id[len(id)-1] ^= 0x5a
	id[0] ^= 0xa5
	return id
}

func encodeSet(set []uint32) []byte {
	out := binary.AppendUvarint(nil, uint64(len(set)))
	prev := uint32(0)
	for _, x := range set {
		out = binary.AppendUvarint(out, uint64(x-prev))
		prev = x
	}
	return out
}

func decodeSet(b []byte) []uint32 {
	n, k := binary.Uvarint(b)
	if k <= 0 || n > uint64(len(b)) {
		return nil
	}
	b = b[k:]
	out := make([]uint32, 0, n)
	prev := uint32(0)
	for i := uint64(0); i < n; i++ {
		d, k := binary.Uvarint(b)
		if k <= 0 {
			return nil
		}
		b = b[k:]
		prev += uint32(d)
		out = append(out, prev)
	}
	return out
}

func containsAll(set, want []uint32) bool {
	for _, x := range want {
		if _, ok := slices.BinarySearch(set, x); !ok {
			return false
		}
	}
	return true
}

// pathQueryTrigrams returns the trigrams subtrees must contain for q, or
// nil if the index cannot be used for q.
func pathQueryTrigrams(q string) []uint32 {
	if strings.Contains(q, "/") {
		return nil
	}
	return queryTrigrams([]string{q})
}
