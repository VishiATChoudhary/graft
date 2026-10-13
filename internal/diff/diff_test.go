package diff

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"strings"
	"testing"
)

func lcsDP(a, b [][]byte) int {
	dp := make([][]int, len(a)+1)
	for i := range dp {
		dp[i] = make([]int, len(b)+1)
	}
	for i := len(a) - 1; i >= 0; i-- {
		for j := len(b) - 1; j >= 0; j-- {
			if bytes.Equal(a[i], b[j]) {
				dp[i][j] = dp[i+1][j+1] + 1
			} else {
				dp[i][j] = max(dp[i+1][j], dp[i][j+1])
			}
		}
	}
	return dp[0][0]
}

func randLines(r *rand.Rand, n, alpha int) [][]byte {
	out := make([][]byte, n)
	for i := range out {
		out[i] = []byte{byte('a' + r.Intn(alpha)), '\n'}
	}
	return out
}

func TestMinimalLinesOptimal(t *testing.T) {
	r := rand.New(rand.NewSource(1))
	for iter := 0; iter < 3000; iter++ {
		a := randLines(r, r.Intn(40), 1+r.Intn(6))
		b := randLines(r, r.Intn(40), 1+r.Intn(6))
		ms := MinimalLines(a, b)
		total, pa, pb := 0, 0, 0
		for _, m := range ms {
			if m.A < pa || m.B < pb {
				t.Fatalf("non-monotonic matches %v", ms)
			}
			for k := 0; k < m.N; k++ {
				if !bytes.Equal(a[m.A+k], b[m.B+k]) {
					t.Fatalf("bad match")
				}
			}
			pa, pb = m.A+m.N, m.B+m.N
			total += m.N
		}
		if want := lcsDP(a, b); total != want {
			t.Fatalf("iter %d: lcs %d want %d", iter, total, want)
		}
	}
}

func TestMerge3(t *testing.T) {
	base := "a\nb\nc\nd\ne\n"
	a := "a\nB\nc\nd\ne\n"
	b := "a\nb\nc\nd\nE\n"
	got, conf := Merge3([]byte(base), []byte(a), []byte(b), "x", "y")
	if conf || string(got) != "a\nB\nc\nd\nE\n" {
		t.Fatalf("got %q conflict=%v", got, conf)
	}
	got, conf = Merge3([]byte(base), []byte("a\nX\nc\nd\ne\n"), []byte("a\nY\nc\nd\ne\n"), "x", "y")
	if !conf || !strings.Contains(string(got), "<<<<<<< x") {
		t.Fatalf("expected conflict, got %q", got)
	}
	// Insertions at both ends.
	got, conf = Merge3([]byte(base), []byte("top\n"+base), []byte(base+"bottom\n"), "x", "y")
	if conf || string(got) != "top\n"+base+"bottom\n" {
		t.Fatalf("got %q", got)
	}
}

func TestUnified(t *testing.T) {
	var buf bytes.Buffer
	Unified(&buf, "a/f", "b/f", []byte("1\n2\n3\n4\n5\n6\n7\n8\n9\n"), []byte("1\n2\n3\n4\nfive\n6\n7\n8\n9\n"), 3)
	want := "--- a/f\n+++ b/f\n@@ -2,7 +2,7 @@\n 2\n 3\n 4\n-5\n+five\n 6\n 7\n 8\n"
	if buf.String() != want {
		t.Fatalf("got:\n%s\nwant:\n%s", buf.String(), want)
	}
}

// TestMatchesGit checks that unified diffs are identical to git's on random
// inputs shaped like source code (repeated braces, blank lines, indents).
func TestMatchesGit(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not installed")
	}
	dir := t.TempDir()
	r := rand.New(rand.NewSource(7))
	vocab := []string{"}", "", "\treturn nil", "\tif err != nil {", "\t\treturn err", "\t}", "func f() {", "x := 1", "// comment", "\tfoo()", "\tbar()"}
	gen := func(n int) []byte {
		var b strings.Builder
		for i := 0; i < n; i++ {
			if r.Intn(3) == 0 {
				fmt.Fprintf(&b, "line %d\n", r.Intn(40))
			} else {
				b.WriteString(vocab[r.Intn(len(vocab))] + "\n")
			}
		}
		return []byte(b.String())
	}
	mutate := func(src []byte) []byte {
		ls := SplitLines(src)
		for k := r.Intn(8); k >= 0; k-- {
			pos := r.Intn(len(ls) + 1)
			switch r.Intn(3) {
			case 0:
				ins := SplitLines(gen(1 + r.Intn(6)))
				ls = append(ls[:pos], append(ins, ls[pos:]...)...)
			case 1:
				if pos < len(ls) {
					ls = append(ls[:pos], ls[min(len(ls), pos+1+r.Intn(4)):]...)
				}
			case 2:
				if pos < len(ls) {
					ls[pos] = gen(1)
				}
			}
		}
		return bytes.Join(ls, nil)
	}
	same := 0
	const iters = 3000
	for it := 0; it < iters; it++ {
		a := gen(5 + r.Intn(80))
		b := mutate(a)
		pa, pb := dir+"/a", dir+"/b"
		os.WriteFile(pa, a, 0o644)
		os.WriteFile(pb, b, 0o644)
		out, _ := exec.Command("git", "diff", "--no-index", "--no-color", "-U3", "--", pa, pb).Output()
		want := string(out)
		if i := strings.Index(want, "@@"); i >= 0 {
			want = want[i:]
		} else {
			want = ""
		}
		var buf bytes.Buffer
		Unified(&buf, "a", "b", a, b, 3)
		got := buf.String()
		if i := strings.Index(got, "@@"); i >= 0 {
			got = got[i:]
		} else {
			got = ""
		}
		if got == want {
			same++
		} else if it-same < 3 {
			t.Logf("iteration %d differs:\n--- git\n%s\n--- graft\n%s", it, want, got)
		}
	}
	t.Logf("identical to git on %d/%d random diffs", same, iters)
	if same != iters {
		t.Fatalf("only %d/%d diffs identical to git", same, iters)
	}
}
