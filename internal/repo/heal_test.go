package repo

import (
	"fmt"
	"testing"

	"github.com/VishiATChoudhary/graft/internal/object"
	"github.com/VishiATChoudhary/graft/internal/store"
)

// TestLostHistoryBatchIsRecomputed drops a stored history batch and checks
// that path history still works on a fresh repository handle: the batch is
// regenerated from its changeset.
func TestLostHistoryBatchIsRecomputed(t *testing.T) {
	r := newRepo(t)
	db, ok := r.DB.(*store.DB)
	if !ok {
		t.Skip("needs a local store to delete from")
	}
	var parent []object.ID
	for i := 0; i < 5; i++ {
		parent = []object.ID{commit(t, r, parent, fmt.Sprint(i), map[string]string{"a.txt": fmt.Sprint(i, "\n")})}
	}
	if err := db.DeleteDerived("ub", batchKey(3)); err != nil {
		t.Fatal(err)
	}
	r2, _ := Open(r.DB)
	hist, err := r2.PathLog(parent[0], "a.txt", 0, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(hist) != 5 {
		t.Fatalf("history of a.txt has %d entries, want 5", len(hist))
	}
}
