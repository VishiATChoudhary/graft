package vfs

import (
	"io"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/VishiATChoudhary/graft/internal/fetch"
	"github.com/VishiATChoudhary/graft/internal/object"
	"github.com/VishiATChoudhary/graft/internal/store"
)

// buildTree stores files (path -> content) and returns the root tree.
func buildTree(t *testing.T, s store.Objects, files map[string]string) object.ID {
	t.Helper()
	type dir struct {
		files map[string]string
		dirs  map[string]*dir
	}
	root := &dir{files: map[string]string{}, dirs: map[string]*dir{}}
	for p, c := range files {
		d := root
		segs := splitPath(p)
		for _, seg := range segs[:len(segs)-1] {
			if d.dirs[seg] == nil {
				d.dirs[seg] = &dir{files: map[string]string{}, dirs: map[string]*dir{}}
			}
			d = d.dirs[seg]
		}
		d.files[segs[len(segs)-1]] = c
	}
	var write func(d *dir) object.ID
	write = func(d *dir) object.ID {
		tr := &object.Tree{}
		for name, c := range d.files {
			id, err := store.WritePayload(s, object.KindBlob, []byte(c))
			if err != nil {
				t.Fatal(err)
			}
			tr.Entries = append(tr.Entries, object.TreeEntry{Name: name, Mode: object.ModeFile, ID: id, Size: uint64(len(c))})
		}
		for name, sub := range d.dirs {
			tr.Entries = append(tr.Entries, object.TreeEntry{Name: name, Mode: object.ModeDir, ID: write(sub)})
		}
		tr.Sort()
		id, err := store.WriteTree(s, tr)
		if err != nil {
			t.Fatal(err)
		}
		return id
	}
	return write(root)
}

func splitPath(p string) []string {
	var out []string
	cur := ""
	for _, c := range p {
		if c == '/' {
			out = append(out, cur)
			cur = ""
			continue
		}
		cur += string(c)
	}
	return append(out, cur)
}

func readAll(t *testing.T, o *Overlay, p string) string {
	t.Helper()
	f, err := o.Open(p)
	if err != nil {
		return "<missing>"
	}
	defer f.Close()
	b, _ := io.ReadAll(f)
	return string(b)
}

func TestOverlay(t *testing.T) {
	s := store.NewMemLRU(1 << 30)
	root := buildTree(t, s, map[string]string{"a.txt": "a", "dir/b.txt": "b", "dir/sub/c.txt": "c", "keep/k.txt": "k"})
	base := New(fetch.StoreSource{Store: s}, root, time.Unix(1, 0), 1<<20)
	o, err := NewOverlay(base, t.TempDir(), []byte(`{"control":"x"}`))
	if err != nil {
		t.Fatal(err)
	}
	if got := readAll(t, o, "dir/b.txt"); got != "b" {
		t.Fatalf("base read: %q", got)
	}
	if got := readAll(t, o, ".gft/mount.json"); got != `{"control":"x"}` {
		t.Fatalf("meta: %q", got)
	}
	// Append to a base file (copy-up), create, delete, rename a directory.
	f, err := o.OpenFile("a.txt", os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	f.Write([]byte("+"))
	f.Close()
	nf, _ := o.Create("new/n.txt")
	nf.Write([]byte("n"))
	nf.Close()
	if err := o.Remove("keep/k.txt"); err != nil {
		t.Fatal(err)
	}
	if err := o.Rename("dir", "moved"); err != nil {
		t.Fatal(err)
	}
	if got := readAll(t, o, "a.txt"); got != "a+" {
		t.Fatalf("copy-up append: %q", got)
	}
	if got := readAll(t, o, "moved/sub/c.txt"); got != "c" {
		t.Fatalf("renamed dir content: %q", got)
	}
	if got := readAll(t, o, "dir/b.txt"); got != "<missing>" {
		t.Fatal("old directory should be gone")
	}
	infos, _ := o.ReadDir("")
	var names []string
	for _, fi := range infos {
		names = append(names, fi.Name())
	}
	if !slices.Equal(names, []string{".gft", "a.txt", "keep", "moved", "new"}) {
		t.Fatalf("root listing %v", names)
	}
	ch, err := o.Changes()
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, c := range ch {
		st := "+"
		if c.Deleted {
			st = "-"
		}
		got = append(got, st+c.Path)
	}
	want := []string{"+a.txt", "-dir/b.txt", "-dir/sub/c.txt", "-keep/k.txt", "+moved/b.txt", "+moved/sub/c.txt", "+new/n.txt"}
	if !slices.Equal(got, want) {
		t.Fatalf("changes\n got  %v\n want %v", got, want)
	}
	// Re-creating a deleted path clears its whiteout.
	kf, _ := o.Create("keep/k.txt")
	kf.Write([]byte("k"))
	kf.Close()
	ch, _ = o.Changes()
	for _, c := range ch {
		if c.Path == "keep/k.txt" {
			t.Fatalf("identical re-created file should not be a change: %+v", c)
		}
	}
	// Rebase onto a new revision clears the overlay.
	root2 := buildTree(t, s, map[string]string{"z.txt": "z"})
	if err := o.Rebase(root2, time.Unix(2, 0)); err != nil {
		t.Fatal(err)
	}
	if !o.Clean() || readAll(t, o, "z.txt") != "z" || readAll(t, o, "a.txt") != "<missing>" {
		t.Fatal("rebase should show the new revision with an empty overlay")
	}
}
