package repo

import (
	"errors"
	"math/bits"
	"regexp/syntax"
	"strings"

	"github.com/VishiATChoudhary/graft/internal/diff"
	"github.com/VishiATChoudhary/graft/internal/object"
)

// Search index. Each directory version (tree ID) gets a Bloom filter of
// the trigrams of the files directly in it, lowercased so it also serves
// case-insensitive searches. It is derived data keyed by the tree ID:
// computed the first time a search reaches that directory, then stored,
// and since trees are content-addressed a new revision only needs filters
// for the directories it changed. A search extracts the literal text any
// match must contain and scans only directories whose filter has all of
// its trigrams; the filter can have false positives, never false
// negatives, so results are exactly those of a full scan.

const (
	bloomK        = 3
	bloomBitsPer  = 16 // bits per distinct trigram: about 0.1% false positives
	bloomMaxLog2  = 23 // at most 1 MB per directory
	indexMaxBlob  = 1 << 20
	trigramKind   = "tg"
	trigramFormat = 1
)

type bloom struct {
	log2     uint8
	bits     []byte
	complete bool // false if some text file was too large to index: never prune
}

func mix(x uint32) uint64 {
	h := uint64(x) * 0x9E3779B97F4A7C15
	h ^= h >> 29
	h *= 0xBF58476D1CE4E5B9
	return h ^ h>>32
}

func (b *bloom) positions(t uint32, fn func(uint64)) {
	h := mix(t)
	h1, h2 := h&0xffffffff, h>>32|1
	mask := uint64(1)<<b.log2 - 1
	for i := uint64(0); i < bloomK; i++ {
		fn((h1 + i*h2) & mask)
	}
}

func (b *bloom) add(t uint32) { b.positions(t, func(p uint64) { b.bits[p>>3] |= 1 << (p & 7) }) }

func (b *bloom) has(t uint32) bool {
	ok := true
	b.positions(t, func(p uint64) { ok = ok && b.bits[p>>3]&(1<<(p&7)) != 0 })
	return ok
}

func lower(c byte) byte {
	if 'A' <= c && c <= 'Z' {
		return c + 'a' - 'A'
	}
	return c
}

func trigramsOf(b []byte, fn func(uint32)) {
	for i := 0; i+3 <= len(b); i++ {
		fn(uint32(lower(b[i]))<<16 | uint32(lower(b[i+1]))<<8 | uint32(lower(b[i+2])))
	}
}

// dirBloom returns the trigram filter of the files directly in tree id.
func (r *Repo) dirBloom(id object.ID, files []object.ID) (*bloom, error) {
	if v, err := r.blooms.Get(id); err == nil {
		return decodeBloom(v), nil
	}
	if v, err := r.DB.GetDerived(trigramKind, id); err == nil {
		r.blooms.Put(id, v)
		return decodeBloom(v), nil
	} else if !errors.Is(err, object.ErrNotFound) {
		return nil, err
	}
	set := map[uint32]struct{}{}
	complete := byte(1)
	for _, f := range files {
		b, err := r.ReadBlob(f)
		if err != nil {
			return nil, err
		}
		if diff.IsBinary(b) {
			continue // grep never matches binary files
		}
		if len(b) > indexMaxBlob {
			complete = 0
			continue
		}
		trigramsOf(b, func(t uint32) { set[t] = struct{}{} })
	}
	log2 := uint8(6)
	if len(set) > 0 {
		log2 = uint8(bits.Len(uint(len(set)*bloomBitsPer - 1)))
		log2 = max(6, min(log2, bloomMaxLog2))
	}
	bl := &bloom{log2: log2, bits: make([]byte, 1<<log2/8)}
	for t := range set {
		bl.add(t)
	}
	v := append([]byte{trigramFormat, log2, complete}, bl.bits...)
	r.blooms.Put(id, v)
	if err := r.DB.PutDerived(trigramKind, id, v); err != nil {
		return nil, err
	}
	return decodeBloom(v), nil
}

func decodeBloom(v []byte) *bloom {
	if len(v) < 3 || v[0] != trigramFormat || v[1] < 3 || len(v)-3 != 1<<v[1]/8 {
		return nil
	}
	return &bloom{log2: v[1], complete: v[2] == 1, bits: v[3:]}
}

// requiredLiterals returns strings every match of the pattern must
// contain (case-insensitively), or nil if none can be derived.
func requiredLiterals(pattern string, fixed bool) []string {
	if fixed {
		return []string{pattern}
	}
	re, err := syntax.Parse(pattern, syntax.Perl)
	if err != nil {
		return nil
	}
	var out []string
	var walk func(re *syntax.Regexp)
	walk = func(re *syntax.Regexp) {
		switch re.Op {
		case syntax.OpLiteral:
			out = append(out, string(re.Rune))
		case syntax.OpCapture:
			walk(re.Sub[0])
		case syntax.OpPlus:
			walk(re.Sub[0]) // at least one occurrence
		case syntax.OpConcat:
			var run strings.Builder
			flush := func() {
				if run.Len() > 0 {
					out = append(out, run.String())
					run.Reset()
				}
			}
			for _, s := range re.Sub {
				if s.Op == syntax.OpLiteral {
					run.WriteString(string(s.Rune))
					continue
				}
				flush()
				walk(s)
			}
			flush()
		}
	}
	walk(re.Simplify())
	return out
}

// queryTrigrams are the (lowercased) trigrams of the required literals.
func queryTrigrams(lits []string) []uint32 {
	seen := map[uint32]bool{}
	var out []uint32
	for _, l := range lits {
		trigramsOf([]byte(l), func(t uint32) {
			if !seen[t] {
				seen[t] = true
				out = append(out, t)
			}
		})
	}
	return out
}

// mayContain reports whether the directory's files may contain all of ts.
func (b *bloom) mayContain(ts []uint32) bool {
	if b == nil || !b.complete {
		return true
	}
	for _, t := range ts {
		if !b.has(t) {
			return false
		}
	}
	return true
}
