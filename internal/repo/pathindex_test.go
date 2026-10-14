package repo

import (
	"fmt"
	"math/rand"
	"slices"
	"strings"
	"testing"

	"github.com/VishiATChoudhary/graft/internal/object"
)

// TestFindPathsMatchesBruteForce checks indexed path search against
// matching every path, for queries inside file names, inside directory
// names, spanning "/" and shorter than a trigram, on two revisions.
func TestFindPathsMatchesBruteForce(t *testing.T) {
	r := newRepo(t)
	rng := rand.New(rand.NewSource(3))
	parts := []string{"Server", "client", "Cache", "shard", "index", "util", "Handler", "main", "test", "README"}
	files := map[string]string{}
	for i := 0; i < 400; i++ {
		depth := 1 + rng.Intn(4)
		var comps []string
		for d := 0; d < depth; d++ {
			comps = append(comps, parts[rng.Intn(len(parts))]+fmt.Sprint(rng.Intn(5)))
		}
		files[strings.Join(comps, "/")+fmt.Sprintf("/f%d.go", i)] = "x\n"
	}
	c1 := commit(t, r, nil, "one", files)
	c2 := commit(t, r, []object.ID{c1}, "two", map[string]string{"util3/zebraHandler.go": "y\n"})
	for _, rev := range []object.ID{c1, c2} {
		root, _ := r.RootTree(rev)
		all, err := r.FindPaths(root, "", "", 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, q := range []string{"handler", "cache2", "f12.go", "server1/cli", "zebra", "ex", "nomatch", "SHARD", "test4"} {
			got, err := r.FindPaths(root, "", q, 0)
			if err != nil {
				t.Fatal(err)
			}
			var want []string
			for _, p := range all {
				if strings.Contains(strings.ToLower(p), strings.ToLower(q)) {
					want = append(want, p)
				}
			}
			slices.Sort(got)
			slices.Sort(want)
			if !slices.Equal(got, want) {
				t.Fatalf("query %q on %s: got %d paths, want %d", q, rev.Short(), len(got), len(want))
			}
		}
	}
}
