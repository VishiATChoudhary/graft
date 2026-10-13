package repo

import (
	"errors"
	"fmt"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/VishiATChoudhary/graft/internal/object"
	"github.com/VishiATChoudhary/graft/internal/store"
)

func newRepo(t *testing.T) *Repo {
	t.Helper()
	r, err := Open(newBackend(t))
	if err != nil {
		t.Fatal(err)
	}
	return r
}

// newBackend returns a local DB, or a sharded fleet when GRAFT_TEST_SHARDS
// is set (CI runs the suite both ways).
func newBackend(t *testing.T) store.Backend {
	t.Helper()
	if n, _ := strconv.Atoi(os.Getenv("GRAFT_TEST_SHARDS")); n > 0 {
		return newFleet(t, n)
	}
	db, err := store.OpenDB(t.TempDir(), 64<<20)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

// newFleet starts n storage nodes and returns the shared URLs' client.
func newFleet(t *testing.T, n int) *store.Sharded {
	t.Helper()
	urls := fleetURLs(t, n)
	return store.NewSharded(urls, 64<<20)
}

func fleetURLs(t *testing.T, n int) []string {
	t.Helper()
	var urls []string
	var dbs []*store.DB
	var srvs []*httptest.Server
	for i := 0; i < n; i++ {
		db, err := store.OpenDB(t.TempDir(), 32<<20)
		if err != nil {
			t.Fatal(err)
		}
		dbs = append(dbs, db)
		srv := httptest.NewServer(store.NodeHandler(db, nil))
		srvs = append(srvs, srv)
		urls = append(urls, srv.URL)
	}
	for i, db := range dbs {
		db.SetPeers(store.PeerResolver(store.ParseFleet(urls), i, db))
	}
	t.Cleanup(func() {
		for i := range srvs {
			srvs[i].Close()
			dbs[i].Close()
		}
	})
	return urls
}

func blob(t *testing.T, r *Repo, s string) object.ID {
	t.Helper()
	id, err := store.WritePayload(r.DB, object.KindBlob, []byte(s))
	if err != nil {
		t.Fatal(err)
	}
	return id
}

// commit creates a changeset; files maps path -> content ("" deletes).
func commit(t *testing.T, r *Repo, parents []object.ID, msg string, files map[string]string) object.ID {
	t.Helper()
	cs := &object.Changeset{Parents: parents, Author: "t <t@x>", Time: 1, Message: msg}
	for p, c := range files {
		if c == "" {
			cs.Changes = append(cs.Changes, object.Change{Path: p})
		} else {
			cs.Changes = append(cs.Changes, object.Change{Path: p, Mode: object.ModeFile, Blob: blob(t, r, c)})
		}
	}
	id, err := r.Create(cs)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func read(t *testing.T, r *Repo, cs object.ID, path string) string {
	t.Helper()
	root, err := r.RootTree(cs)
	if err != nil {
		t.Fatal(err)
	}
	e, err := r.Lookup(root, path)
	if err != nil {
		return "<absent>"
	}
	b, err := r.ReadBlob(e.ID)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func TestTreesAndHistory(t *testing.T) {
	r := newRepo(t)
	c1 := commit(t, r, nil, "one", map[string]string{"a/x.txt": "1\n2\n3\n", "b/y.txt": "y\n"})
	c2 := commit(t, r, []object.ID{c1}, "two", map[string]string{"a/x.txt": "1\nTWO\n3\n"})
	c3 := commit(t, r, []object.ID{c2}, "three", map[string]string{"b/y.txt": "", "c/z.txt": "z\n"})
	c4 := commit(t, r, []object.ID{c3}, "four", map[string]string{"a/x.txt": "0\n1\nTWO\n3\n"})

	if got := read(t, r, c4, "a/x.txt"); got != "0\n1\nTWO\n3\n" {
		t.Fatalf("x = %q", got)
	}
	if got := read(t, r, c4, "b/y.txt"); got != "<absent>" {
		t.Fatalf("y should be deleted, got %q", got)
	}
	root4, _ := r.RootTree(c4)
	if _, err := r.Lookup(root4, "b"); err == nil {
		t.Fatalf("empty dir b should be pruned")
	}

	log, err := r.PathLog(c4, "a/x.txt", 0, false)
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(log) != fmt.Sprint([]object.ID{c4, c2, c1}) {
		t.Fatalf("path log = %v", log)
	}
	dlog, _ := r.PathLog(c4, "c", 0, false)
	if len(dlog) != 1 || dlog[0] != c3 {
		t.Fatalf("dir log = %v", dlog)
	}

	bl, err := r.Blame(c4, "a/x.txt")
	if err != nil {
		t.Fatal(err)
	}
	want := []object.ID{c4, c1, c2, c1}
	for i, l := range bl {
		if l.Changeset != want[i] {
			t.Fatalf("blame line %d = %s want %s", i+1, l.Changeset.Short(), want[i].Short())
		}
	}
	if bl[3].Line != 3 {
		t.Fatalf("line 4 should originate at line 3, got %d", bl[3].Line)
	}

	got, err := r.Resolve(c4.String()[:10] + "~2")
	if err != nil || got != c2 {
		t.Fatalf("resolve ~2 = %v %v", got, err)
	}

	d, err := r.DiffTrees(mustRoot(t, r, c1), root4, nil)
	if err != nil {
		t.Fatal(err)
	}
	var paths []string
	for _, f := range d {
		paths = append(paths, f.Path)
	}
	if strings.Join(paths, ",") != "a/x.txt,b/y.txt,c/z.txt" {
		t.Fatalf("diff paths %v", paths)
	}
	d, _ = r.DiffTrees(mustRoot(t, r, c1), root4, []string{"c"})
	if len(d) != 1 || d[0].Path != "c/z.txt" {
		t.Fatalf("prefixed diff %v", d)
	}
}

func mustRoot(t *testing.T, r *Repo, id object.ID) object.ID {
	root, err := r.RootTree(id)
	if err != nil {
		t.Fatal(err)
	}
	return root
}

func TestLongHistorySkipLists(t *testing.T) {
	r := newRepo(t)
	var ids []object.ID
	var parent []object.ID
	for i := 0; i < 1500; i++ {
		id := commit(t, r, parent, fmt.Sprint(i), map[string]string{fmt.Sprintf("f%d", i%7): fmt.Sprint(i, "\n")})
		ids = append(ids, id)
		parent = []object.ID{id}
	}
	tip := ids[len(ids)-1]
	for _, back := range []int{1, 2, 3, 100, 777, 1499} {
		got, err := r.Resolve(fmt.Sprintf("%s~%d", tip, back))
		if err != nil || got != ids[len(ids)-1-back] {
			t.Fatalf("~%d wrong: %v", back, err)
		}
	}
	ok, _ := r.IsAncestor(ids[3], tip)
	if !ok {
		t.Fatal("ancestor")
	}
	ok, _ = r.IsAncestor(tip, ids[3])
	if ok {
		t.Fatal("not ancestor")
	}
	// Branch off the middle and check the merge base.
	br := commit(t, r, []object.ID{ids[700]}, "branch", map[string]string{"g": "g\n"})
	mb, err := r.MergeBase(br, tip)
	if err != nil || mb != ids[700] {
		t.Fatalf("merge base %v %v", mb, err)
	}
	pl, _ := r.PathLog(tip, "f3", 0, false)
	if len(pl) != 1500/7+1 && len(pl) != 1500/7 {
		t.Fatalf("path log length %d", len(pl))
	}
}

func TestRebaseMergeLand(t *testing.T) {
	r := newRepo(t)
	base := commit(t, r, nil, "base", map[string]string{"f": "a\nb\nc\nd\ne\n", "g": "g\n"})
	if err := r.UpdateRef("main", object.Zero, base); err != nil {
		t.Fatal(err)
	}
	m1 := commit(t, r, []object.ID{base}, "main edit", map[string]string{"f": "A\nb\nc\nd\ne\n"})
	if err := r.UpdateRef("main", base, m1); err != nil {
		t.Fatal(err)
	}
	if err := r.UpdateRef("main", base, m1); !errors.Is(err, ErrStaleRef) {
		t.Fatalf("expected stale ref, got %v", err)
	}
	feat := commit(t, r, []object.ID{base}, "feature", map[string]string{"f": "a\nb\nc\nd\nE\n"})
	res, err := r.Land("main", feat, "t", 1, 0)
	if err != nil {
		t.Fatal(err)
	}
	if res.Rebased != 1 || read(t, r, res.New, "f") != "A\nb\nc\nd\nE\n" {
		t.Fatalf("land result %+v content %q", res, read(t, r, res.New, "f"))
	}
	// Conflicting land fails cleanly.
	bad := commit(t, r, []object.ID{base}, "bad", map[string]string{"f": "X\nb\nc\nd\ne\n"})
	if _, err := r.Land("main", bad, "t", 1, 0); !errors.Is(err, ErrConflict) {
		t.Fatalf("expected conflict, got %v", err)
	}
	// Merge with two parents.
	side := commit(t, r, []object.ID{base}, "side", map[string]string{"g": "G\n"})
	mid, err := r.Merge(res.New, side, "t", "merge", 1)
	if err != nil {
		t.Fatal(err)
	}
	if read(t, r, mid, "g") != "G\n" || read(t, r, mid, "f") != "A\nb\nc\nd\nE\n" {
		t.Fatal("merge content")
	}
	pl, _ := r.PathLog(mid, "g", 0, false)
	if len(pl) != 3 || pl[0] != mid { // merge, side, base
		t.Fatalf("path log through merge: %v", pl)
	}
}

func TestConcurrentLand(t *testing.T) {
	r := newRepo(t)
	base := commit(t, r, nil, "base", map[string]string{"README": "hi\n"})
	if err := r.UpdateRef("main", object.Zero, base); err != nil {
		t.Fatal(err)
	}
	const n = 64
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id := commit(t, r, []object.ID{base}, fmt.Sprint("agent ", i), map[string]string{fmt.Sprintf("agents/%d/out.txt", i): fmt.Sprint(i, "\n")})
			if _, err := r.Land("main", id, "t", 1, 1000); err != nil {
				errs <- err
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	tip, _ := r.Ref("main")
	for i := 0; i < n; i++ {
		if got := read(t, r, tip, fmt.Sprintf("agents/%d/out.txt", i)); got != fmt.Sprint(i, "\n") {
			t.Fatalf("agent %d lost: %q", i, got)
		}
	}
	g, _ := r.Graph(tip)
	if g.Depth != n {
		t.Fatalf("linear history depth %d want %d", g.Depth, n)
	}
}

// TestMultipleAPIServers runs several stateless API servers (each its own
// Repo over its own Sharded client) against one storage fleet, landing
// concurrently. No lands may be lost, and history must be linear.
func TestMultipleAPIServers(t *testing.T) {
	urls := fleetURLs(t, 3)
	var servers []*Repo
	for i := 0; i < 3; i++ {
		r, err := Open(store.NewSharded(append([]string{}, urls...), 32<<20))
		if err != nil {
			t.Fatal(err)
		}
		servers = append(servers, r)
	}
	base := commit(t, servers[0], nil, "base", map[string]string{"README": "hi\n"})
	if err := servers[1].UpdateRef("main", object.Zero, base); err != nil {
		t.Fatal(err)
	}
	const n = 48
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			r := servers[i%len(servers)]
			id := commit(t, r, []object.ID{base}, fmt.Sprint("agent ", i), map[string]string{fmt.Sprintf("agents/%d.txt", i): fmt.Sprint(i, "\n")})
			if _, err := r.Land("main", id, "t", 1, 1000); err != nil {
				errs <- err
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	// Every server sees the same tip with all changes.
	tip, _ := servers[2].Ref("main")
	for _, r := range servers {
		if got, _ := r.Ref("main"); got != tip {
			t.Fatal("servers disagree on main")
		}
	}
	for i := 0; i < n; i++ {
		if got := read(t, servers[i%3], tip, fmt.Sprintf("agents/%d.txt", i)); got != fmt.Sprint(i, "\n") {
			t.Fatalf("agent %d lost: %q", i, got)
		}
	}
	g, _ := servers[0].Graph(tip)
	if g.Depth != n {
		t.Fatalf("depth %d want %d", g.Depth, n)
	}
	// History queries work from any server.
	pl, err := servers[1].PathLog(tip, "agents", 0, false)
	if err != nil || len(pl) != n {
		t.Fatalf("path log over fleet: %d %v", len(pl), err)
	}
}

func TestGrep(t *testing.T) {
	r := newRepo(t)
	c := commit(t, r, nil, "g", map[string]string{"a/x.go": "package a\nfunc Foo() {}\n", "a/y.go": "package a\n// foo bar\n", "b/z.txt": "FOO\n"})
	root, _ := r.RootTree(c)
	m, _, err := r.Grep(root, GrepOptions{Pattern: "foo", IgnoreCase: true})
	if err != nil || len(m) != 3 {
		t.Fatalf("grep -i foo: %v %v", m, err)
	}
	m, _, _ = r.Grep(root, GrepOptions{Pattern: "Foo()", Fixed: true, Prefix: "a"})
	if len(m) != 1 || m[0].Path != "a/x.go" || m[0].Line != 2 {
		t.Fatalf("fixed grep: %v", m)
	}
	m, _, _ = r.Grep(root, GrepOptions{Pattern: "o", CanRead: func(p string) bool { return p != "b" }})
	for _, x := range m {
		if strings.HasPrefix(x.Path, "b/") {
			t.Fatal("grep ignored access filter")
		}
	}
}
