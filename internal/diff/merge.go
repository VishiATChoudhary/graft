package diff

import (
	"bytes"
	"fmt"
)

// Merge3 performs a line-based three-way merge of a and b against base.
// It returns the merged content and whether any conflicts were found. On
// conflict the output contains conflict markers labelled with la and lb.
func Merge3(base, a, b []byte, la, lb string) ([]byte, bool) {
	if bytes.Equal(a, b) {
		return a, false
	}
	if bytes.Equal(base, a) {
		return b, false
	}
	if bytes.Equal(base, b) {
		return a, false
	}
	O, A, B := SplitLines(base), SplitLines(a), SplitLines(b)
	ma := matchMap(len(O), Lines(O, A))
	mb := matchMap(len(O), Lines(O, B))

	var out bytes.Buffer
	conflict := false
	emit := func(ls [][]byte) {
		for _, l := range ls {
			out.Write(l)
		}
	}
	resolve := func(o, x, y [][]byte) {
		switch {
		case eqLines(x, o):
			emit(y)
		case eqLines(y, o):
			emit(x)
		case eqLines(x, y):
			emit(x)
		default:
			conflict = true
			fmt.Fprintf(&out, "<<<<<<< %s\n", la)
			emitNL(&out, x)
			out.WriteString("=======\n")
			emitNL(&out, y)
			fmt.Fprintf(&out, ">>>>>>> %s\n", lb)
		}
	}
	i, ja, jb := 0, 0, 0
	for {
		k := 0
		for i+k < len(O) && ma[i+k] == ja+k && mb[i+k] == jb+k {
			k++
		}
		if k > 0 {
			emit(O[i : i+k])
			i, ja, jb = i+k, ja+k, jb+k
			continue
		}
		next := -1
		for t := i; t < len(O); t++ {
			if ma[t] >= ja && mb[t] >= jb {
				next = t
				break
			}
		}
		if next < 0 {
			resolve(O[i:], A[ja:], B[jb:])
			break
		}
		resolve(O[i:next], A[ja:ma[next]], B[jb:mb[next]])
		i, ja, jb = next, ma[next], mb[next]
	}
	return out.Bytes(), conflict
}

func emitNL(out *bytes.Buffer, ls [][]byte) {
	for _, l := range ls {
		out.Write(l)
	}
	if n := len(ls); n > 0 && !bytes.HasSuffix(ls[n-1], []byte{'\n'}) {
		out.WriteByte('\n')
	}
}

func matchMap(n int, ms []Match) []int {
	m := make([]int, n)
	for i := range m {
		m[i] = -1
	}
	for _, r := range ms {
		for t := 0; t < r.N; t++ {
			m[r.A+t] = r.B + t
		}
	}
	return m
}

func eqLines(x, y [][]byte) bool {
	if len(x) != len(y) {
		return false
	}
	for i := range x {
		if !bytes.Equal(x[i], y[i]) {
			return false
		}
	}
	return true
}
