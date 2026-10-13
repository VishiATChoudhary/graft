package diff

import (
	"bytes"
	"math/rand"
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

func TestLinesOptimal(t *testing.T) {
	r := rand.New(rand.NewSource(1))
	for iter := 0; iter < 3000; iter++ {
		a := randLines(r, r.Intn(40), 1+r.Intn(6))
		b := randLines(r, r.Intn(40), 1+r.Intn(6))
		ms := Lines(a, b)
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
