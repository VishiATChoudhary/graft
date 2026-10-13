package gitimport

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/VishiATChoudhary/graft/internal/object"
	"github.com/VishiATChoudhary/graft/internal/repo"
	"github.com/VishiATChoudhary/graft/internal/store"
)

func git(t *testing.T, dir string, args ...string) string {
	t.Helper()
	cmd := exec.Command("git", append([]string{"-C", dir}, args...)...)
	cmd.Env = append(os.Environ(), "GIT_AUTHOR_DATE=1700000000 +0100", "GIT_COMMITTER_DATE=1700000000 +0100",
		"GIT_AUTHOR_NAME=T", "GIT_AUTHOR_EMAIL=t@x", "GIT_COMMITTER_NAME=T", "GIT_COMMITTER_EMAIL=t@x")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v: %v\n%s", args, err, out)
	}
	return string(out)
}

func put(t *testing.T, dir, p, content string) {
	t.Helper()
	fp := filepath.Join(dir, p)
	os.MkdirAll(filepath.Dir(fp), 0o755)
	if err := os.WriteFile(fp, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

// buildRepo makes a small history with the awkward cases.
func buildRepo(t *testing.T) string {
	d := t.TempDir()
	git(t, d, "init", "-q", "-b", "main")
	put(t, d, "a/x.txt", "1\n2\n3\n")
	put(t, d, "a/y.txt", "y\n")
	put(t, d, "weird name \"q\"/é.txt", "unicode\n")
	put(t, d, "run.sh", "#!/bin/sh\n")
	os.Chmod(filepath.Join(d, "run.sh"), 0o755)
	os.Symlink("a/x.txt", filepath.Join(d, "link"))
	git(t, d, "add", "-A")
	git(t, d, "commit", "-qm", "init")
	git(t, d, "checkout", "-qb", "side")
	put(t, d, "side.txt", "side\n")
	put(t, d, "a/x.txt", "1\n2\n3\n4\n")
	git(t, d, "commit", "-qam", "side work")
	git(t, d, "add", "-A")
	git(t, d, "commit", "-qm", "side file")
	git(t, d, "checkout", "-q", "main")
	put(t, d, "a/x.txt", "0\n1\n2\n3\n")
	os.MkdirAll(filepath.Join(d, "b"), 0o755)
	git(t, d, "mv", "a/y.txt", "b/y.txt")
	git(t, d, "commit", "-qam", "main work + rename")
	git(t, d, "merge", "-q", "--no-edit", "side")
	git(t, d, "rm", "-q", "-r", "a")
	git(t, d, "commit", "-qm", "delete a/")
	put(t, d, "a/x.txt", "reborn\n")
	git(t, d, "add", "-A")
	git(t, d, "commit", "-qm", "re-add a/x.txt")
	git(t, d, "tag", "v1")
	return d
}

func gitBlobID(content []byte) string {
	h := sha1.New()
	fmt.Fprintf(h, "blob %d\x00", len(content))
	h.Write(content)
	return hex.EncodeToString(h.Sum(nil))
}

func TestImportFidelity(t *testing.T) {
	src := buildRepo(t)
	db, err := store.OpenDB(t.TempDir(), 32<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	r, err := repo.Open(db)
	if err != nil {
		t.Fatal(err)
	}
	st, err := Import(context.Background(), r, Options{GitDir: src, Refs: []string{"--all"}})
	if err != nil {
		t.Fatal(err)
	}
	commits := strings.Fields(git(t, src, "rev-list", "--all"))
	if st.Changesets != len(commits) {
		t.Fatalf("imported %d changesets, git has %d commits", st.Changesets, len(commits))
	}
	// Map git commit -> changeset via origin, walking the whole log.
	byOrigin := map[string]object.ID{}
	tip, err := r.Ref("main")
	if err != nil {
		t.Fatal(err)
	}
	ids, err := r.Log(tip, 0, false)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range ids {
		cs, _ := r.Changeset(id)
		byOrigin[strings.TrimPrefix(cs.Origin, "git:")] = id
	}
	for _, c := range commits {
		id, ok := byOrigin[c]
		if !ok {
			t.Fatalf("commit %s not reachable from main", c)
		}
		want := map[string]string{}
		for _, line := range strings.Split(strings.TrimSpace(git(t, src, "ls-tree", "-r", "-z", c)), "\x00") {
			if line == "" {
				continue
			}
			meta, path, _ := strings.Cut(line, "\t")
			f := strings.Fields(meta)
			want[path] = f[0] + " " + f[2]
		}
		got := map[string]string{}
		root, _ := r.RootTree(id)
		var walk func(dir string, tid object.ID)
		walk = func(dir string, tid object.ID) {
			tr, err := store.ReadTree(db, tid)
			if err != nil {
				t.Fatal(err)
			}
			for _, e := range tr.Entries {
				p := e.Name
				if dir != "" {
					p = dir + "/" + e.Name
				}
				if e.Mode == object.ModeDir {
					walk(p, e.ID)
					continue
				}
				b, _ := r.ReadBlob(e.ID)
				if e.Size != uint64(len(b)) {
					t.Fatalf("%s: tree size %d != %d", p, e.Size, len(b))
				}
				mode := map[object.Mode]string{object.ModeFile: "100644", object.ModeExec: "100755", object.ModeSymlink: "120000"}[e.Mode]
				got[p] = mode + " " + gitBlobID(b)
			}
		}
		if !root.IsZero() {
			walk("", root)
		}
		if fmt.Sprint(got) != fmt.Sprint(want) {
			t.Fatalf("commit %s tree mismatch:\n got  %v\n want %v", c[:8], got, want)
		}
	}
	// Tags and branches arrive as refs.
	if _, err := r.Ref("tags/v1"); err != nil {
		t.Fatal("tag not imported")
	}
	if _, err := r.Ref("side"); err != nil {
		t.Fatal("branch not imported")
	}
	// History survives the delete/re-add, and blame follows the rename.
	pl, err := r.PathLog(tip, "a/x.txt", 0, false)
	if err != nil {
		t.Fatal(err)
	}
	want := strings.Fields(git(t, src, "log", "--format=%H", "--full-history", "--", "a/x.txt"))
	if len(pl) != len(want) {
		t.Fatalf("path log %d entries, git %d", len(pl), len(want))
	}
	bl, err := r.Blame(tip, "b/y.txt")
	if err != nil {
		t.Fatal(err)
	}
	cs, _ := r.Changeset(bl[0].Changeset)
	if !strings.Contains(git(t, src, "log", "-1", "--format=%s", strings.TrimPrefix(cs.Origin, "git:")), "init") {
		t.Fatalf("blame across rename attributed to %q", cs.Message)
	}
}
