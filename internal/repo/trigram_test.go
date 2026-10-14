package repo

import (
	"fmt"
	"math/rand"
	"reflect"
	"strings"
	"testing"

	"github.com/VishiATChoudhary/graft/internal/object"
)

// TestIndexedGrepMatchesFullScan checks that searching with the directory
// filters returns exactly what a full scan returns, for literal, regular
// expression and case-insensitive patterns, including files too large to
// index and a later revision that reuses unchanged directories' filters.
func TestIndexedGrepMatchesFullScan(t *testing.T) {
	r := newRepo(t)
	rng := rand.New(rand.NewSource(7))
	words := []string{"alpha", "Bravo", "charlie", "DELTA", "echo", "foxtrot", "golf", "hotel", "India", "juliet", "kilo", "Lima", "func", "return", "nil", "err"}
	gen := func(n int) string {
		var b strings.Builder
		for i := 0; i < n; i++ {
			b.WriteString(words[rng.Intn(len(words))])
			if rng.Intn(8) == 0 {
				b.WriteString("\n")
			} else {
				b.WriteString(" ")
			}
		}
		return b.String()
	}
	files := map[string]string{}
	for d := 0; d < 30; d++ {
		for f := 0; f < 4; f++ {
			files[fmt.Sprintf("pkg%d/sub%d/f%d.txt", d%7, d, f)] = gen(40)
		}
	}
	files["big/large.txt"] = strings.Repeat("filler line\n", (indexMaxBlob/12)+10) + "needle in a large haystack\n"
	files["pkg3/sub3/special.txt"] = "the unique zanzibar marker\n"
	c1 := commit(t, r, nil, "one", files)
	c2 := commit(t, r, []object.ID{c1}, "two", map[string]string{"pkg1/sub1/f0.txt": "now mentions zanzibar too\n"})

	patterns := []GrepOptions{
		{Pattern: "zanzibar", Fixed: true},
		{Pattern: "needle", Fixed: true},
		{Pattern: "ZANZIBAR", Fixed: true, IgnoreCase: true},
		{Pattern: "alpha bravo", Fixed: true, IgnoreCase: true},
		{Pattern: "alpha Bravo", Fixed: true},
		{Pattern: `fox(trot)+ golf`},
		{Pattern: `(?i)lima\s+INDIA`},
		{Pattern: `kilo|zulu`},
		{Pattern: `[a-z]+ nil err`},
		{Pattern: "nothing-like-this", Fixed: true},
	}
	for _, rev := range []object.ID{c1, c2} {
		root, _ := r.RootTree(rev)
		for _, opt := range patterns {
			opt.Limit = 100000
			full := opt
			full.NoIndex = true
			want, _, err := r.Grep(root, full)
			if err != nil {
				t.Fatal(err)
			}
			got, _, err := r.Grep(root, opt)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("%q (rev %s): indexed %d matches, full scan %d", opt.Pattern, rev.Short(), len(got), len(want))
			}
		}
	}
	if lits := requiredLiterals(`kilo|zulu`, false); len(queryTrigrams(lits)) != 0 {
		t.Fatalf("alternation must not prune: %v", lits)
	}
}
