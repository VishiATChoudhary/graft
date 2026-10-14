package client_test

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/VishiATChoudhary/graft/internal/acl"
	"github.com/VishiATChoudhary/graft/internal/api"
	"github.com/VishiATChoudhary/graft/internal/repo"
	"github.com/VishiATChoudhary/graft/internal/store"
)

// TestChangeRequestsAndMergeQueue covers change requests end to end:
// comments (including concurrent ones), the merge queue waiting for an
// approval and then landing on its own, revisions dequeuing, and the
// review UI being served.
func TestChangeRequestsAndMergeQueue(t *testing.T) {
	ctx := context.Background()
	db, _ := store.OpenDB(t.TempDir(), 32<<20)
	defer db.Close()
	r, _ := repo.Open(db)
	s := &api.Server{Repo: r}
	s.EnableACL(&acl.Policy{
		Tokens:  map[string]string{"a": "alice", "b": "bob", "c": "carol"},
		Default: acl.RoleWrite,
		Reviews: []acl.ReviewRule{{Path: "svc", Approvals: 1, Approvers: []string{"alice", "carol"}}},
	})
	mux := http.NewServeMux()
	s.Routes(mux)
	srv := httptest.NewServer(mux)
	defer srv.Close()
	alice, bob, carol := api.NewClientToken(srv.URL, "a"), api.NewClientToken(srv.URL, "b"), api.NewClientToken(srv.URL, "c")

	if _, err := alice.Write(ctx, &api.WriteRequest{Author: "alice", Message: "seed", Land: "main",
		Files: []api.WriteFile{{Path: "README", Content: []byte("hi\n")}}}); err != nil {
		t.Fatal(err)
	}
	w, err := bob.Write(ctx, &api.WriteRequest{Parent: "main", Author: "bob", Message: "Add service\n\nDetails.",
		Files: []api.WriteFile{{Path: "svc/main.go", Content: []byte("package main\n\nfunc main() {}\n")}}})
	if err != nil {
		t.Fatal(err)
	}
	cr, err := bob.CreateCR(ctx, w.ID.String(), "main", "", "please review")
	if err != nil {
		t.Fatal(err)
	}
	if cr.Title != "Add service" || cr.Author != "bob" || cr.State != "open" {
		t.Fatalf("unexpected record: %+v", cr)
	}

	// Concurrent comments from several reviewers are all kept.
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			c := []*api.Client{alice, carol}[i%2]
			if _, err := c.CommentCR(ctx, cr.ID, "svc/main.go", 3, fmt.Sprint("note ", i)); err != nil {
				t.Error(err)
			}
		}(i)
	}
	wg.Wait()
	got, err := alice.GetCR(ctx, cr.ID)
	if err != nil || len(got.Comments) != 20 {
		t.Fatalf("want 20 comments, got %d (%v)", len(got.Comments), err)
	}

	// Queued without approval: waits.
	q, err := bob.QueueCR(ctx, cr.ID)
	if err != nil {
		t.Fatal(err)
	}
	if q.State != "queued" || !strings.Contains(q.Waiting, "review required") {
		t.Fatalf("queued request should wait for review: %+v", q)
	}
	// Approval lands it without anyone landing by hand.
	if _, err := carol.Approve(ctx, w.ID.String()); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		got, _ = bob.GetCR(ctx, cr.ID)
		if got.State == "landed" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("not landed after approval: %+v", got)
		}
		time.Sleep(20 * time.Millisecond)
	}
	main, _ := bob.Resolve(ctx, "main")
	if main.ID != got.Landed {
		t.Fatalf("main %s is not the landed changeset %s", main.ID.Short(), got.Landed.Short())
	}

	// A second change request: a new revision takes it out of the queue
	// (its approvals belong to the old revision).
	w2, _ := bob.Write(ctx, &api.WriteRequest{Parent: "main", Author: "bob", Message: "Tweak",
		Files: []api.WriteFile{{Path: "svc/main.go", Content: []byte("package main\n\nfunc main() { println() }\n")}}})
	cr2, err := bob.CreateCR(ctx, w2.ID.String(), "main", "Tweak", "")
	if err != nil {
		t.Fatal(err)
	}
	bob.QueueCR(ctx, cr2.ID)
	w3, _ := bob.Write(ctx, &api.WriteRequest{Parent: w2.ID.String(), Author: "bob", Message: "Tweak v2",
		Files: []api.WriteFile{{Path: "svc/main.go", Content: []byte("package main\n\nfunc main() { println(1) }\n")}}})
	rv, err := bob.ReviseCR(ctx, cr2.ID, w3.ID.String())
	if err != nil || rv.State != "open" || len(rv.Revisions) != 2 {
		t.Fatalf("revision should dequeue: %+v %v", rv, err)
	}
	if list, _ := alice.ListCRs(ctx, "open"); len(list) != 1 || list[0].ID != cr2.ID {
		t.Fatalf("open list: %+v", list)
	}
	// Records are not ordinary refs.
	refs, _ := alice.Refs(ctx)
	for name := range refs {
		if strings.HasPrefix(name, "meta/") {
			t.Fatalf("metadata ref listed: %s", name)
		}
	}
	// The review UI is served.
	resp, err := http.Get(srv.URL + "/review")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if !strings.Contains(string(body), "graft review") {
		t.Fatal("review UI not served")
	}
}
