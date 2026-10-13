package client_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/VishiATChoudhary/graft/internal/acl"
	"github.com/VishiATChoudhary/graft/internal/api"
	"github.com/VishiATChoudhary/graft/internal/client"
	"github.com/VishiATChoudhary/graft/internal/fetch"
	"github.com/VishiATChoudhary/graft/internal/object"
	"github.com/VishiATChoudhary/graft/internal/repo"
	"github.com/VishiATChoudhary/graft/internal/store"
)

func TestAccessControl(t *testing.T) {
	ctx := context.Background()
	db, err := store.OpenDB(t.TempDir(), 64<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	r, _ := repo.Open(db)
	srvAPI := &api.Server{Repo: r}
	pol := &acl.Policy{
		Tokens:  map[string]string{"alice-token": "alice", "bob-token": "bob"},
		Groups:  map[string][]string{"security": {"alice"}, "payments": {"alice"}},
		Default: acl.RoleWrite,
		Rules: []acl.Rule{
			{Path: "secrets", Read: []string{"group:security"}, Write: []string{"group:security"}},
			{Path: "services/payments", Write: []string{"group:payments"}},
		},
	}
	// Seed history before enabling the policy, to exercise backfill.
	seed := &object.Changeset{Message: "seed", Author: "seed", Time: 1}
	for p, c := range map[string]string{"README": "hi\n", "secrets/key.pem": "TOP SECRET\n", "secrets/deep/x.txt": "x\n", "services/payments/pay.go": "package pay\n", "services/web/web.go": "package web\n"} {
		id, _ := store.WritePayload(db, object.KindBlob, []byte(c))
		seed.Changes = append(seed.Changes, object.Change{Path: p, Mode: object.ModeFile, Blob: id})
	}
	base, err := r.Create(seed)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.UpdateRef("main", object.Zero, base); err != nil {
		t.Fatal(err)
	}
	if err := srvAPI.EnableACL(pol); err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	srvAPI.Routes(mux)
	srv := httptest.NewServer(mux)
	defer srv.Close()
	// A shared regional cache in front of the server.
	cmux := http.NewServeMux()
	cmux.HandleFunc("/v1/objects", fetch.Handler(fetch.NewTier(store.NewMemLRU(64<<20), fetch.NewRemote(srv.URL))))
	cache := httptest.NewServer(cmux)
	defer cache.Close()

	alice := api.NewClientToken(srv.URL, "alice-token")
	bob := api.NewClientToken(srv.URL, "bob-token")
	anon := api.NewClient(srv.URL)

	if _, err := anon.Resolve(ctx, "main"); !errors.Is(err, api.ErrForbidden) {
		t.Fatalf("anonymous should be refused, got %v", err)
	}

	// Alice clones everything through the cache (warming it).
	aroot := filepath.Join(t.TempDir(), "alice")
	aw, _ := client.Init(aroot, client.Config{Server: srv.URL, Objects: cache.URL, Token: "alice-token", Author: "alice"})
	if _, err := aw.Checkout(ctx, "main", false); err != nil {
		t.Fatal(err)
	}
	if b, _ := os.ReadFile(filepath.Join(aroot, "secrets/key.pem")); string(b) != "TOP SECRET\n" {
		t.Fatal("alice should see secrets")
	}

	// Bob clones through the same cache: the secrets directory is skipped.
	broot := filepath.Join(t.TempDir(), "bob")
	bw, _ := client.Init(broot, client.Config{Server: srv.URL, Objects: cache.URL, Token: "bob-token", Author: "bob"})
	st, err := bw.Checkout(ctx, "main", false)
	if err != nil {
		t.Fatal(err)
	}
	if len(st.Restricted) != 1 {
		t.Fatalf("expected one restricted directory, got %d", len(st.Restricted))
	}
	if _, err := os.Stat(filepath.Join(broot, "secrets")); !os.IsNotExist(err) {
		t.Fatal("bob must not get secrets")
	}
	if b, _ := os.ReadFile(filepath.Join(broot, "services/web/web.go")); string(b) != "package web\n" {
		t.Fatal("bob should get public files")
	}

	// Path-based queries.
	var sb strings.Builder
	if err := bob.Cat(ctx, &sb, "main", "secrets/key.pem"); !errors.Is(err, api.ErrForbidden) {
		t.Fatalf("cat secrets: %v", err)
	}
	if _, err := bob.Log(ctx, "main", "secrets", 0, false, false); !errors.Is(err, api.ErrForbidden) {
		t.Fatalf("log secrets: %v", err)
	}
	ci, err := bob.Changeset(ctx, "main")
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range ci.Changes {
		if strings.HasPrefix(c.Path, "secrets") {
			t.Fatal("changeset listing leaked a secrets path")
		}
	}
	d, _ := bob.Diff(ctx, "", "main", nil)
	for _, c := range d.Changes {
		if strings.HasPrefix(c.Path, "secrets") {
			t.Fatal("diff leaked a secrets path")
		}
	}

	// Writes.
	if _, err := bob.Write(ctx, &api.WriteRequest{Parent: "main", Author: "bob", Message: "x", Files: []api.WriteFile{{Path: "secrets/new", Content: []byte("x")}}}); !errors.Is(err, api.ErrForbidden) {
		t.Fatalf("bob writing secrets: %v", err)
	}
	if _, err := bob.Write(ctx, &api.WriteRequest{Parent: "main", Author: "bob", Message: "x", Files: []api.WriteFile{{Path: "services/payments/pay.go", Content: []byte("evil")}}}); !errors.Is(err, api.ErrForbidden) {
		t.Fatalf("bob writing payments: %v", err)
	}
	if _, err := bob.Write(ctx, &api.WriteRequest{Parent: "main", Author: "bob", Message: "ok", Land: "main", Files: []api.WriteFile{{Path: "services/web/web.go", Content: []byte("package web // bob\n")}}}); err != nil {
		t.Fatalf("bob writing web: %v", err)
	}
	// Alice prepares a secrets change; bob may not land it (ref moves are
	// checked against the whole difference).
	resp, err := alice.Write(ctx, &api.WriteRequest{Parent: "main", Author: "alice", Message: "rotate", Files: []api.WriteFile{{Path: "secrets/key.pem", Content: []byte("NEW SECRET\n")}}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := bob.Land(ctx, "main", resp.ID.String(), "bob"); !errors.Is(err, api.ErrForbidden) {
		t.Fatalf("bob landing alice's secrets change: %v", err)
	}
	if _, err := alice.Land(ctx, "main", resp.ID.String(), "alice"); err != nil {
		t.Fatalf("alice landing: %v", err)
	}

	// The new secrets tree is indexed at creation and refused to bob even
	// when asked for directly, through the shared cache, by ID.
	ar, _ := alice.Resolve(ctx, "main")
	ents, _ := alice.Ls(ctx, ar.ID.String(), "")
	var secretsTree object.ID
	for _, e := range ents {
		if e.Name == "secrets" {
			secretsTree = e.ID
		}
	}
	bobCache := fetch.NewRemote(cache.URL)
	bobCache.Token = "bob-token"
	err = bobCache.Fetch(ctx, []object.ID{secretsTree}, func(object.ID, []byte) error { return nil })
	var fe *fetch.ForbiddenError
	if !errors.As(err, &fe) {
		t.Fatalf("bob fetching the secrets tree by ID through the cache: %v", err)
	}
	aliceCache := fetch.NewRemote(cache.URL)
	aliceCache.Token = "alice-token"
	if err := aliceCache.Fetch(ctx, []object.ID{secretsTree}, func(object.ID, []byte) error { return nil }); err != nil {
		t.Fatalf("alice fetching the secrets tree: %v", err)
	}
	// And again for bob after alice's fetch: the cache must not have kept it.
	if err := bobCache.Fetch(ctx, []object.ID{secretsTree}, func(object.ID, []byte) error { return nil }); !errors.As(err, &fe) {
		t.Fatalf("cache served a private object to bob: %v", err)
	}
}
