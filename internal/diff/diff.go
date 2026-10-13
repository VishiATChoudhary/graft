// Package diff implements line diffs (linear-space Myers), three-way merge,
// and unified diff formatting.
package diff

import (
	"bytes"
)

// Match is a run of equal lines: A[A:A+N] == B[B:B+N].
type Match struct{ A, B, N int }

// SplitLines splits text into lines, keeping the trailing newline on each.
func SplitLines(b []byte) [][]byte {
	if len(b) == 0 {
		return nil
	}
	n := bytes.Count(b, []byte{'\n'})
	out := make([][]byte, 0, n+1)
	for len(b) > 0 {
		i := bytes.IndexByte(b, '\n')
		if i < 0 {
			out = append(out, b)
			break
		}
		out = append(out, b[:i+1])
		b = b[i+1:]
	}
	return out
}

// IsBinary reports whether content looks binary (NUL in the first 8 KiB).
func IsBinary(b []byte) bool {
	if len(b) > 8000 {
		b = b[:8000]
	}
	return bytes.IndexByte(b, 0) >= 0
}

// maxCost bounds the edit distance explored per middle-snake search. Beyond
// it, the region is reported as fully replaced (non-minimal but bounded).
const maxCost = 1 << 14

// Lines computes matching runs between two line slices.
func Lines(a, b [][]byte) []Match {
	ids := make(map[string]int, len(a)+len(b))
	ai := intern(a, ids)
	bi := intern(b, ids)
	ca, cb := xdiff(ai, bi)
	return compactChanges(ai, bi, a, b, ca, cb)
}

// MinimalLines computes a minimal diff (linear-space Myers). It is used
// where minimality matters more than matching git's choices.
func MinimalLines(a, b [][]byte) []Match {
	ids := make(map[string]int, len(a)+len(b))
	ai := intern(a, ids)
	bi := intern(b, ids)
	var out []Match
	rec(ai, bi, 0, 0, &out)
	return coalesce(out)
}

func intern(lines [][]byte, ids map[string]int) []int {
	out := make([]int, len(lines))
	for i, l := range lines {
		id, ok := ids[string(l)]
		if !ok {
			id = len(ids)
			ids[string(l)] = id
		}
		out[i] = id
	}
	return out
}

func coalesce(ms []Match) []Match {
	var out []Match
	for _, m := range ms {
		if m.N == 0 {
			continue
		}
		if n := len(out); n > 0 && out[n-1].A+out[n-1].N == m.A && out[n-1].B+out[n-1].N == m.B {
			out[n-1].N += m.N
			continue
		}
		out = append(out, m)
	}
	return out
}

func rec(a, b []int, ao, bo int, out *[]Match) {
	// Common prefix.
	p := 0
	for p < len(a) && p < len(b) && a[p] == b[p] {
		p++
	}
	if p > 0 {
		*out = append(*out, Match{ao, bo, p})
	}
	a, b, ao, bo = a[p:], b[p:], ao+p, bo+p
	// Common suffix.
	s := 0
	for s < len(a) && s < len(b) && a[len(a)-1-s] == b[len(b)-1-s] {
		s++
	}
	a2, b2 := a[:len(a)-s], b[:len(b)-s]
	if len(a2) > 0 && len(b2) > 0 {
		x, y, u, v, ok := midSnake(a2, b2)
		if ok && (x+y > 0 || u+v < len(a2)+len(b2)) && !(x == 0 && y == 0 && u == 0 && v == 0) {
			rec(a2[:x], b2[:y], ao, bo, out)
			if u > x {
				*out = append(*out, Match{ao + x, bo + y, u - x})
			}
			rec(a2[u:], b2[v:], ao+u, bo+v, out)
		}
	}
	if s > 0 {
		*out = append(*out, Match{ao + len(a2), bo + len(b2), s})
	}
}

// midSnake finds the middle snake of an optimal edit path (Myers 1986, 4b).
func midSnake(a, b []int) (x0, y0, x1, y1 int, ok bool) {
	n, m := len(a), len(b)
	max := (n + m + 1) / 2
	if max > maxCost {
		max = maxCost
	}
	off := max + 1
	vf := make([]int, 2*max+3)
	vb := make([]int, 2*max+3)
	delta := n - m
	odd := delta&1 != 0
	for d := 0; d <= max; d++ {
		for k := -d; k <= d; k += 2 {
			var x int
			if k == -d || (k != d && vf[off+k-1] < vf[off+k+1]) {
				x = vf[off+k+1]
			} else {
				x = vf[off+k-1] + 1
			}
			y := x - k
			sx, sy := x, y
			for x < n && y < m && a[x] == b[y] {
				x++
				y++
			}
			vf[off+k] = x
			if odd {
				kr := delta - k
				if kr >= -(d-1) && kr <= d-1 && x+vb[off+kr] >= n {
					return sx, sy, x, y, true
				}
			}
		}
		for k := -d; k <= d; k += 2 {
			var x int
			if k == -d || (k != d && vb[off+k-1] < vb[off+k+1]) {
				x = vb[off+k+1]
			} else {
				x = vb[off+k-1] + 1
			}
			y := x - k
			sx, sy := x, y
			for x < n && y < m && a[n-1-x] == b[m-1-y] {
				x++
				y++
			}
			vb[off+k] = x
			if !odd {
				kf := delta - k
				if kf >= -d && kf <= d && x+vf[off+kf] >= n {
					return n - x, m - y, n - sx, m - sy, true
				}
			}
		}
	}
	return 0, 0, 0, 0, false
}
