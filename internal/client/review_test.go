package client_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/VishiATChoudhary/graft/internal/acl"
	"github.com/VishiATChoudhary/graft/internal/api"
	"github.com/VishiATChoudhary/graft/internal/object"
	"github.com/VishiATChoudhary/graft/internal/repo"
	"github.com/VishiATChoudhary/graft/internal/store"
)

func TestReviewGates(t *testing.T) {
	ctx := context.Background()
	db, _ := store.OpenDB(t.TempDir(), 32<<20)
	defer db.Close()
	r, _ := repo.Open(db)
	srvAPI := &api.Server{Repo: r}
	srvAPI.EnableACL(&acl.Policy{
		Tokens:  map[string]string{"a": "alice", "b": "bob", "c": "carol"},
		Groups:  map[string][]string{"payments-owners": {"carol", "alice"}},
		Default: acl.RoleWrite,
		Reviews: []acl.ReviewRule{{Path: "payments", Approvals: 1, Approvers: []string{"group:payments-owners"}, Exempt: []string{"comments", "docs"}}},
	})
	mux := http.NewServeMux()
	srvAPI.Routes(mux)
	srv := httptest.NewServer(mux)
	defer srv.Close()
	alice, bob, carol := api.NewClientToken(srv.URL, "a"), api.NewClientToken(srv.URL, "b"), api.NewClientToken(srv.URL, "c")

	seed, err := alice.Write(ctx, &api.WriteRequest{Author: "alice", Message: "seed", Files: []api.WriteFile{
		{Path: "payments/charge.go", Content: []byte("package payments\n\nfunc Charge() int { return 1 }\n")},
		{Path: "web/page.go", Content: []byte("package web\n")}}})
	if err != nil {
		t.Fatal(err)
	}
	// Adding payments code is a logic change: alice (an owner) cannot
	// approve her own change, carol can.
	if _, err := alice.Land(ctx, "main", seed.ID.String(), "alice"); !errors.Is(err, api.ErrForbidden) {
		t.Fatalf("unreviewed seed should not land: %v", err)
	}
	if _, err := carol.Approve(ctx, seed.ID.String()); err != nil {
		t.Fatal(err)
	}
	if _, err := alice.Land(ctx, "main", seed.ID.String(), "alice"); err != nil {
		t.Fatal(err)
	}

	// A comment-only change to payments lands without review.
	if _, err := bob.Write(ctx, &api.WriteRequest{Parent: "main", Author: "bob", Message: "doc comment", Land: "main", Files: []api.WriteFile{
		{Path: "payments/charge.go", Content: []byte("package payments\n\n// Charge charges.\nfunc Charge() int { return 1 }\n")}}}); err != nil {
		t.Fatalf("comment-only change should be exempt: %v", err)
	}
	// Changes elsewhere are not gated.
	if _, err := bob.Write(ctx, &api.WriteRequest{Parent: "main", Author: "bob", Message: "web", Land: "main", Files: []api.WriteFile{
		{Path: "web/page.go", Content: []byte("package web\n\nvar X = 1\n")}}}); err != nil {
		t.Fatalf("ungated path: %v", err)
	}
	// A logic change needs an owner's approval.
	logic, err := bob.Write(ctx, &api.WriteRequest{Parent: "main", Author: "bob", Message: "charge 2", Files: []api.WriteFile{
		{Path: "payments/charge.go", Content: []byte("package payments\n\n// Charge charges.\nfunc Charge() int { return 2 }\n")}}})
	if err != nil {
		t.Fatal(err)
	}
	_, err = bob.Land(ctx, "main", logic.ID.String(), "bob")
	if !errors.Is(err, api.ErrForbidden) || !strings.Contains(err.Error(), "review required") {
		t.Fatalf("expected review requirement, got %v", err)
	}
	if _, err := bob.Approve(ctx, logic.ID.String()); !errors.Is(err, api.ErrForbidden) {
		t.Fatalf("authors cannot approve their own change: %v", err)
	}
	st, err := carol.Approve(ctx, logic.ID.String())
	if err != nil || !st.Satisfied {
		t.Fatalf("carol's approval should satisfy the rule: %+v %v", st, err)
	}
	if _, err := bob.Land(ctx, "main", logic.ID.String(), "bob"); err != nil {
		t.Fatalf("approved change should land: %v", err)
	}
	// A new changeset dismisses the approval (it was for the old one).
	again, _ := bob.Write(ctx, &api.WriteRequest{Parent: logic.ID.String(), Author: "bob", Message: "charge 3", Files: []api.WriteFile{
		{Path: "payments/charge.go", Content: []byte("package payments\n\n// Charge charges.\nfunc Charge() int { return 3 }\n")}}})
	if _, err := bob.Land(ctx, "main", again.ID.String(), "bob"); !errors.Is(err, api.ErrForbidden) {
		t.Fatalf("new changeset should need a fresh approval: %v", err)
	}
	var _ = object.Zero
}
