package vfs

import (
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/VishiATChoudhary/graft/internal/fetch"
	"github.com/VishiATChoudhary/graft/internal/store"
)

// TestFUSEMount mounts a writable overlay with FUSE as the current
// (unprivileged) user and works through the kernel. Skipped without FUSE.
func TestFUSEMount(t *testing.T) {
	if _, err := os.Stat("/dev/fuse"); err != nil {
		t.Skip("no /dev/fuse")
	}
	if _, err := exec.LookPath("fusermount3"); err != nil {
		if _, err := exec.LookPath("fusermount"); err != nil {
			t.Skip("no fusermount")
		}
	}
	s := store.NewMemLRU(1 << 30)
	root := buildTree(t, s, map[string]string{"a.txt": "a", "dir/b.txt": "b"})
	o, err := NewOverlay(New(fetch.StoreSource{Store: s}, root, time.Unix(1, 0), 1<<20), t.TempDir(), []byte("{}"))
	if err != nil {
		t.Fatal(err)
	}
	mnt := t.TempDir()
	unmount, err := MountFUSE(o, mnt)
	if err != nil {
		t.Fatal(err)
	}
	defer unmount()
	if b, err := os.ReadFile(filepath.Join(mnt, "dir/b.txt")); err != nil || string(b) != "b" {
		t.Fatalf("read through FUSE: %q %v", b, err)
	}
	if err := os.WriteFile(filepath.Join(mnt, "new.txt"), []byte("n"), 0o644); err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(filepath.Join(mnt, "a.txt"), os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	f.Write([]byte("+"))
	f.Close()
	if err := os.Rename(filepath.Join(mnt, "dir"), filepath.Join(mnt, "moved")); err != nil {
		t.Fatal(err)
	}
	if b, _ := os.ReadFile(filepath.Join(mnt, "a.txt")); string(b) != "a+" {
		t.Fatalf("append through FUSE: %q", b)
	}
	ch, _ := o.Changes()
	var got []string
	for _, c := range ch {
		got = append(got, c.Path)
	}
	if !slices.Equal(got, []string{"a.txt", "dir/b.txt", "moved/b.txt", "new.txt"}) {
		t.Fatalf("changes %v", got)
	}
}
