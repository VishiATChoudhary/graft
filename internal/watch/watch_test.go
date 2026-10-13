package watch

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"testing"
	"time"
)

func TestJournal(t *testing.T) {
	j := NewJournal()
	t0 := j.Token()
	j.Record("a/b.txt", false)
	j.Record("c", true)
	j.Record(".gft/index", false)
	t1 := j.Token()
	j.Record("d.txt", false)
	if got := j.Since(t0); got.Full || !slices.Equal(got.Paths, []string{"a/b.txt", "c/", "d.txt"}) {
		t.Fatalf("since t0: %+v", got)
	}
	if got := j.Since(t1); !slices.Equal(got.Paths, []string{"d.txt"}) {
		t.Fatalf("since t1: %+v", got)
	}
	if !NewJournal().Since(t1).Full {
		t.Fatal("a different epoch must force a full scan")
	}
	j.Overflow()
	if !j.Since(t1).Full {
		t.Fatal("overflow must force a full scan")
	}
}

func TestWatcher(t *testing.T) {
	if runtime.GOOS != "darwin" && runtime.GOOS != "linux" {
		t.Skip("unsupported platform")
	}
	root := t.TempDir()
	root, _ = filepath.EvalSymlinks(root)
	os.MkdirAll(filepath.Join(root, ".gft"), 0o755)
	os.MkdirAll(filepath.Join(root, "src", "pkg"), 0o755)
	os.WriteFile(filepath.Join(root, "src", "pkg", "a.go"), []byte("a"), 0o644)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go Run(ctx, root)
	var c *Client
	for i := 0; i < 200 && c == nil; i++ {
		time.Sleep(10 * time.Millisecond)
		c = Dial(root)
	}
	if c == nil {
		t.Fatal("watcher did not start")
	}
	tok, err := c.Token()
	if err != nil {
		t.Fatal(err)
	}
	os.WriteFile(filepath.Join(root, "src", "pkg", "a.go"), []byte("changed"), 0o644)
	os.WriteFile(filepath.Join(root, "new.txt"), []byte("n"), 0o644)
	os.Rename(filepath.Join(root, "src", "pkg"), filepath.Join(root, "src", "moved"))
	// No sleep: the cookie handshake must make these visible immediately.
	ch, err := c.Since(tok)
	if err != nil || ch.Full {
		t.Fatalf("since: %+v %v", ch, err)
	}
	has := func(p string) bool { return slices.Contains(ch.Paths, p) }
	if !has("new.txt") || !(has("src/moved/") || has("src/")) || !(has("src/pkg/") || has("src/pkg/a.go") || has("src/")) {
		t.Fatalf("missing changes: %v", ch.Paths)
	}
}
