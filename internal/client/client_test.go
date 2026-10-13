package client_test

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/VishiATChoudhary/graft/internal/api"
	"github.com/VishiATChoudhary/graft/internal/client"
	"github.com/VishiATChoudhary/graft/internal/fetch"
	"github.com/VishiATChoudhary/graft/internal/object"
	"github.com/VishiATChoudhary/graft/internal/repo"
	"github.com/VishiATChoudhary/graft/internal/store"
	"github.com/VishiATChoudhary/graft/internal/watch"
)

type env struct {
	server string
	daemon string
	cache  *store.MemLRU
	api    *api.Client
}

// newEnv starts server, cache and daemon tiers, wired strictly in order.
func newEnv(t *testing.T) *env {
	t.Helper()
	db, err := store.OpenDB(t.TempDir(), 64<<20)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	r, err := repo.Open(db)
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	(&api.Server{Repo: r}).Routes(mux)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	lru := store.NewMemLRU(64 << 20)
	cmux := http.NewServeMux()
	cmux.HandleFunc("/v1/objects", fetch.Handler(fetch.NewTier(lru, fetch.NewRemote(srv.URL))))
	csrv := httptest.NewServer(cmux)
	t.Cleanup(csrv.Close)

	dc, err := store.OpenDiskCache(t.TempDir(), 64<<20)
	if err != nil {
		t.Fatal(err)
	}
	sock := filepath.Join(t.TempDir(), "d.sock")
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	dmux := http.NewServeMux()
	dmux.HandleFunc("/v1/objects", fetch.Handler(fetch.NewTier(dc, fetch.NewRemote(csrv.URL))))
	dsrv := &http.Server{Handler: dmux}
	go dsrv.Serve(ln)
	t.Cleanup(func() { dsrv.Close() })
	return &env{server: srv.URL, daemon: "unix://" + sock, cache: lru, api: api.NewClient(srv.URL)}
}

func write(t *testing.T, e *env, parent, land, msg string, files map[string]string) string {
	t.Helper()
	req := &api.WriteRequest{Parent: parent, Author: "seed", Message: msg, Land: land}
	for p, c := range files {
		if c == "" {
			req.Files = append(req.Files, api.WriteFile{Path: p, Delete: true})
		} else {
			req.Files = append(req.Files, api.WriteFile{Path: p, Content: []byte(c)})
		}
	}
	resp, err := e.api.Write(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	return resp.ID.String()
}

func readFile(t *testing.T, root, p string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(root, p))
	if err != nil {
		return "<missing>"
	}
	return string(b)
}

func TestCloneCommitLand(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t)
	if err := e.api.UpdateRef(ctx, "main", [32]byte{}, mustID(t, write(t, e, "", "", "init", map[string]string{
		"svc/a/main.go": "package a\n", "svc/b/main.go": "package b\n", "README": "hi\n", "tools/run.sh": "echo\n",
	}))); err != nil {
		t.Fatal(err)
	}

	// Agent 1: sparse clone through the daemon.
	root1 := filepath.Join(t.TempDir(), "wc1")
	w1, err := client.Init(root1, client.Config{Server: e.server, Objects: e.daemon, Sparse: []string{"svc/a"}, Author: "a1"})
	if err != nil {
		t.Fatal(err)
	}
	st, err := w1.Checkout(ctx, "main", false)
	if err != nil {
		t.Fatal(err)
	}
	if st.Files != 1 || readFile(t, root1, "svc/a/main.go") != "package a\n" || readFile(t, root1, "README") != "<missing>" {
		t.Fatalf("sparse checkout wrote %d files", st.Files)
	}

	// Agent 2: full clone direct to server with a local cache.
	root2 := filepath.Join(t.TempDir(), "wc2")
	w2, err := client.Init(root2, client.Config{Server: e.server, Author: "a2"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w2.Checkout(ctx, "main", false); err != nil {
		t.Fatal(err)
	}

	// Both modify different files and land; the second is rebased server-side.
	os.WriteFile(filepath.Join(root1, "svc/a/main.go"), []byte("package a\n\nfunc A() {}\n"), 0o644)
	os.WriteFile(filepath.Join(root1, "svc/a/new.go"), []byte("package a\n"), 0o644)
	if _, _, err := w1.Commit(ctx, "a1 change", nil); err != nil {
		t.Fatal(err)
	}
	os.WriteFile(filepath.Join(root2, "README"), []byte("hello\n"), 0o644)
	os.Remove(filepath.Join(root2, "tools/run.sh"))
	if st, _ := w2.Status(ctx); len(st) != 2 {
		t.Fatalf("status = %+v", st)
	}
	if _, _, err := w2.Commit(ctx, "a2 change", nil); err != nil {
		t.Fatal(err)
	}
	if _, err := w1.Land(ctx, "main"); err != nil {
		t.Fatal(err)
	}
	res, err := w2.Land(ctx, "main")
	if err != nil {
		t.Fatal(err)
	}
	if res.Rebased != 1 {
		t.Fatalf("expected rebase, got %+v", res)
	}
	// w2's working copy now has agent 1's change too, and tools/ is gone.
	if readFile(t, root2, "svc/a/main.go") != "package a\n\nfunc A() {}\n" || readFile(t, root2, "svc/a/new.go") != "package a\n" {
		t.Fatal("landed working copy missing other agent's change")
	}
	if _, err := os.Stat(filepath.Join(root2, "tools")); !os.IsNotExist(err) {
		t.Fatal("empty directory should be pruned")
	}
	if st, _ := w2.Status(ctx); len(st) != 0 {
		t.Fatalf("clean after land, got %+v", st)
	}

	// Agent 1 moves forward; only changed files are written.
	st, err = w1.Checkout(ctx, "main", false)
	if err != nil {
		t.Fatal(err)
	}
	if st.Files != 0 { // README is outside the sparse spec
		t.Fatalf("update wrote %d files", st.Files)
	}

	// Local edits that would be clobbered block checkout.
	os.WriteFile(filepath.Join(root2, "README"), []byte("dirty\n"), 0o644)
	write(t, e, "main", "main", "server edit", map[string]string{"README": "server\n"})
	if _, err := w2.Checkout(ctx, "main", false); err == nil || !strings.Contains(err.Error(), "overwritten") {
		t.Fatalf("expected dirty refusal, got %v", err)
	}

	// Widen the sparse spec.
	w1.Config.Sparse = append(w1.Config.Sparse, "svc/b")
	if _, err := w1.ApplySparse(ctx); err != nil {
		t.Fatal(err)
	}
	if readFile(t, root1, "svc/b/main.go") != "package b\n" {
		t.Fatal("sparse add did not materialize svc/b")
	}

	// History through the API.
	log, err := e.api.Log(ctx, "main", "svc/a/main.go", 0, false, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(log) != 2 {
		t.Fatalf("path log has %d entries", len(log))
	}
	bl, err := e.api.Blame(ctx, "main", "svc/a/main.go")
	if err != nil || len(bl) != 3 || bl[0].Author != "seed" || bl[2].Author != "a1" {
		t.Fatalf("blame = %+v, %v", bl, err)
	}
}

func TestConflictingLand(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t)
	base := write(t, e, "", "", "init", map[string]string{"f.txt": "one\ntwo\nthree\n"})
	if err := e.api.UpdateRef(ctx, "main", [32]byte{}, mustID(t, base)); err != nil {
		t.Fatal(err)
	}
	root := filepath.Join(t.TempDir(), "wc")
	w, _ := client.Init(root, client.Config{Server: e.server, Author: "a"})
	if _, err := w.Checkout(ctx, "main", false); err != nil {
		t.Fatal(err)
	}
	write(t, e, "main", "main", "theirs", map[string]string{"f.txt": "one\nTWO\nthree\n"})
	os.WriteFile(filepath.Join(root, "f.txt"), []byte("one\nzwei\nthree\n"), 0o644)
	if _, _, err := w.Commit(ctx, "mine", nil); err != nil {
		t.Fatal(err)
	}
	_, err := w.Land(ctx, "main")
	paths, ok := api.IsConflict(err)
	if !ok || len(paths) != 1 || paths[0] != "f.txt" || !errors.Is(err, repo.ErrConflict) {
		t.Fatalf("expected conflict on f.txt, got %v", err)
	}
}

func mustID(t *testing.T, s string) object.ID {
	t.Helper()
	id, err := object.ParseID(s)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func TestWatchedStatusMatchesFullScan(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t)
	files := map[string]string{}
	for i := 0; i < 60; i++ {
		files[fmt.Sprintf("d%d/sub%d/f%d.txt", i%4, i%3, i)] = fmt.Sprint(i, "\n")
	}
	if err := e.api.UpdateRef(ctx, "main", object.Zero, mustID(t, write(t, e, "", "", "init", files))); err != nil {
		t.Fatal(err)
	}
	root := filepath.Join(t.TempDir(), "wc")
	w, _ := client.Init(root, client.Config{Server: e.server, Author: "a"})
	if _, err := w.Checkout(ctx, "main", false); err != nil {
		t.Fatal(err)
	}
	wctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		if err := watch.Run(wctx, root); err != nil {
			t.Error("watch:", err)
		}
	}()
	for i := 0; i < 200 && watch.Dial(root) == nil; i++ {
		time.Sleep(10 * time.Millisecond)
	}
	if st, _ := w.Status(ctx); len(st) != 0 { // full scan, adopts the token
		t.Fatalf("not clean: %v", st)
	}
	if _, err := os.Stat(filepath.Join(root, ".gft", "fsmonitor")); err != nil {
		ents, _ := os.ReadDir(filepath.Join(root, ".gft"))
		var names []string
		for _, e := range ents {
			names = append(names, e.Name())
		}
		t.Fatalf("clean status should record the watcher token; .gft has %v", names)
	}
	r := rand.New(rand.NewSource(3))
	for round := 0; round < 8; round++ {
		for k := 0; k < 5; k++ {
			p := filepath.Join(root, fmt.Sprintf("d%d/sub%d/f%d.txt", r.Intn(4), r.Intn(3), r.Intn(60)))
			switch r.Intn(4) {
			case 0:
				os.WriteFile(p, []byte(fmt.Sprint("edit ", round, k)), 0o644)
			case 1:
				os.Remove(p)
			case 2:
				os.MkdirAll(filepath.Dir(p), 0o755)
				os.WriteFile(p+".new", []byte("new"), 0o644)
			case 3:
				d := filepath.Join(root, fmt.Sprintf("d%d", r.Intn(4)))
				os.Rename(d, d+"x")
				os.Rename(d+"x", d+fmt.Sprint("r", round))
			}
		}
		watched, err := w.Status(ctx)
		if err != nil {
			t.Fatal(err)
		}
		tok, _ := os.ReadFile(filepath.Join(root, ".gft", "fsmonitor"))
		os.Remove(filepath.Join(root, ".gft", "fsmonitor"))
		full, err := w.Status(ctx)
		if err != nil {
			t.Fatal(err)
		}
		os.WriteFile(filepath.Join(root, ".gft", "fsmonitor"), tok, 0o644)
		if fmt.Sprint(statusLines(watched)) != fmt.Sprint(statusLines(full)) {
			t.Fatalf("round %d: watched status differs\nwatched %v\nfull    %v", round, statusLines(watched), statusLines(full))
		}
	}
}

func statusLines(st []client.FileStatus) []string {
	var out []string
	for _, s := range st {
		out = append(out, string(s.State)+" "+s.Path)
	}
	return out
}
