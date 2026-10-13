package diff

import (
	"bytes"
	"fmt"
	"io"
)

// Unified writes a unified diff of a and b with the given context lines.
func Unified(w io.Writer, nameA, nameB string, a, b []byte, context int) {
	if bytes.Equal(a, b) {
		return
	}
	if IsBinary(a) || IsBinary(b) {
		fmt.Fprintf(w, "Binary files %s and %s differ\n", nameA, nameB)
		return
	}
	A, B := SplitLines(a), SplitLines(b)
	ms := Lines(A, B)
	ms = append(ms, Match{len(A), len(B), 0}) // sentinel
	fmt.Fprintf(w, "--- %s\n+++ %s\n", nameA, nameB)

	type op struct {
		kind byte
		line []byte
	}
	// Build a flat edit script, then cut it into hunks.
	var ops []op
	var aLine, bLine []int
	ia, ib := 0, 0
	for _, m := range ms {
		for ; ia < m.A; ia++ {
			ops = append(ops, op{'-', A[ia]})
			aLine, bLine = append(aLine, ia), append(bLine, ib)
		}
		for ; ib < m.B; ib++ {
			ops = append(ops, op{'+', B[ib]})
			aLine, bLine = append(aLine, ia), append(bLine, ib)
		}
		for t := 0; t < m.N; t++ {
			ops = append(ops, op{' ', A[ia]})
			aLine, bLine = append(aLine, ia), append(bLine, ib)
			ia++
			ib++
		}
	}
	i := 0
	for i < len(ops) {
		for i < len(ops) && ops[i].kind == ' ' {
			i++
		}
		if i >= len(ops) {
			break
		}
		start := i - context
		if start < 0 {
			start = 0
		}
		end := i
		for {
			for end < len(ops) && ops[end].kind != ' ' {
				end++
			}
			run := end
			for run < len(ops) && ops[run].kind == ' ' && run-end < 2*context+1 {
				run++
			}
			if run < len(ops) && ops[run].kind != ' ' {
				end = run
				continue
			}
			end += context
			if end > len(ops) {
				end = len(ops)
			}
			break
		}
		na, nb := 0, 0
		for _, o := range ops[start:end] {
			if o.kind != '+' {
				na++
			}
			if o.kind != '-' {
				nb++
			}
		}
		sa, sb := aLine[start]+1, bLine[start]+1
		if na == 0 {
			sa--
		}
		if nb == 0 {
			sb--
		}
		fmt.Fprintf(w, "@@ -%d,%d +%d,%d @@\n", sa, na, sb, nb)
		for _, o := range ops[start:end] {
			w.Write([]byte{o.kind})
			w.Write(o.line)
			if !bytes.HasSuffix(o.line, []byte{'\n'}) {
				io.WriteString(w, "\n\\ No newline at end of file\n")
			}
		}
		i = end
	}
}
