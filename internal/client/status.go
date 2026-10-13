package client

import (
	"bufio"
	"context"
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"

	"github.com/VishiATChoudhary/graft/internal/object"
	"github.com/VishiATChoudhary/graft/internal/watch"
)

// FileStatus is one locally modified path.
type FileStatus struct {
	Path  string
	State byte // 'A' added, 'M' modified, 'D' deleted
	Mode  object.Mode
	Blob  object.ID // new content, for A and M
	// Stat information for refreshing the index after commit.
	Size  int64
	Mtime int64
}

type ignorer struct{ patterns []string }

func loadIgnore(root string) *ignorer {
	ig := &ignorer{patterns: []string{".git/", ".gft/"}}
	f, err := os.Open(filepath.Join(root, ".gftignore"))
	if err != nil {
		return ig
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		l := strings.TrimSpace(sc.Text())
		if l != "" && !strings.HasPrefix(l, "#") {
			ig.patterns = append(ig.patterns, l)
		}
	}
	return ig
}

func (ig *ignorer) match(p string, dir bool) bool {
	base := path.Base(p)
	for _, pat := range ig.patterns {
		dirOnly := strings.HasSuffix(pat, "/")
		pat = strings.TrimSuffix(pat, "/")
		if dirOnly && !dir {
			continue
		}
		target := base
		if strings.Contains(pat, "/") {
			target = p
			pat = strings.TrimPrefix(pat, "/")
		}
		if ok, _ := path.Match(pat, target); ok {
			return true
		}
	}
	return false
}

type seenFile struct {
	path string
	info fs.FileInfo
}

// walk lists every non-ignored file in the sparse region, in parallel.
func (w *Worktree) walk() ([]seenFile, error) {
	ig := loadIgnore(w.Root)
	var (
		mu    sync.Mutex
		out   []seenFile
		first error
		wg    sync.WaitGroup
	)
	sem := make(chan struct{}, runtime.GOMAXPROCS(0)*4)
	var visit func(rel string)
	visit = func(rel string) {
		defer wg.Done()
		sem <- struct{}{}
		ents, err := os.ReadDir(w.abs(rel))
		<-sem
		if err != nil {
			mu.Lock()
			if first == nil && !os.IsNotExist(err) {
				first = err
			}
			mu.Unlock()
			return
		}
		var local []seenFile
		for _, e := range ents {
			p := path.Join(rel, e.Name())
			if rel == "" {
				p = e.Name()
			}
			if e.IsDir() {
				if ig.match(p, true) || !w.towardSparse(p) {
					continue
				}
				wg.Add(1)
				go visit(p)
				continue
			}
			if ig.match(p, false) || !w.inSparse(p) {
				continue
			}
			info, err := e.Info()
			if err != nil {
				continue
			}
			local = append(local, seenFile{p, info})
		}
		mu.Lock()
		out = append(out, local...)
		mu.Unlock()
	}
	wg.Add(1)
	visit("")
	wg.Wait()
	return out, first
}

func modeOf(info fs.FileInfo) object.Mode {
	switch {
	case info.Mode()&fs.ModeSymlink != 0:
		return object.ModeSymlink
	case runtime.GOOS != "windows" && info.Mode()&0o111 != 0:
		return object.ModeExec
	}
	return object.ModeFile
}

// sameMode compares a file's mode with the index's. Windows has no
// executable bit, so there an exec file matches a regular one.
func sameMode(indexed, actual object.Mode) bool {
	return indexed == actual || runtime.GOOS == "windows" && indexed == object.ModeExec && actual == object.ModeFile
}

// readContent returns file content (the target for symlinks).
func readContent(fp string, mode object.Mode) ([]byte, error) {
	if mode == object.ModeSymlink {
		t, err := os.Readlink(fp)
		return []byte(t), err
	}
	return os.ReadFile(fp)
}

// Status reports local modifications relative to the index. Files whose
// size and mtime match the index are assumed clean; others are hashed.
// When a watcher (gft watch) is running, only paths it saw change since
// the index was written are examined.
func (w *Worktree) Status(ctx context.Context) ([]FileStatus, error) {
	idx, err := w.LoadIndex()
	if err != nil {
		return nil, err
	}
	if st, ok := w.watchedStatus(idx); ok {
		return st, nil
	}
	tok := w.watchToken()
	files, err := w.walk()
	if err != nil {
		return nil, err
	}
	var (
		mu  sync.Mutex
		out []FileStatus
		wg  sync.WaitGroup
	)
	present := make(map[string]bool, len(files))
	work := make(chan seenFile, 256)
	for i := 0; i < runtime.GOMAXPROCS(0); i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for f := range work {
				mode := modeOf(f.info)
				content, err := readContent(w.abs(f.path), mode)
				if err != nil {
					continue
				}
				id := object.Hash(object.KindBlob, content)
				e := idx[f.path]
				state := byte('A')
				if e != nil {
					if e.Blob == id && sameMode(e.Mode, mode) {
						// Content unchanged; refresh stat info lazily.
						continue
					}
					state = 'M'
				}
				mu.Lock()
				out = append(out, FileStatus{Path: f.path, State: state, Mode: mode, Blob: id, Size: f.info.Size(), Mtime: f.info.ModTime().UnixNano()})
				mu.Unlock()
			}
		}()
	}
	for _, f := range files {
		present[f.path] = true
		if e := idx[f.path]; e != nil && e.Size == f.info.Size() && e.Mtime == f.info.ModTime().UnixNano() && sameMode(e.Mode, modeOf(f.info)) {
			continue
		}
		work <- f
	}
	close(work)
	wg.Wait()
	for p, e := range idx {
		if !present[p] && w.inSparse(p) {
			out = append(out, FileStatus{Path: p, State: 'D', Mode: e.Mode})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Path < out[j].Path })
	if len(out) == 0 && tok != "" {
		w.saveWatchToken(tok) // clean as of tok: later queries can start here
	}
	return out, nil
}

// watchToken returns the watcher's current position, or "" without one.
func (w *Worktree) watchToken() string {
	c := watch.Dial(w.Root)
	if c == nil {
		return ""
	}
	tok, err := c.Token()
	if err != nil {
		if os.Getenv("GFT_WATCH_DEBUG") != "" {
			fmt.Fprintln(os.Stderr, "watch token:", err)
		}
		return ""
	}
	return tok
}

// saveWatchToken records the position the index is current as of.
func (w *Worktree) saveWatchToken(tok string) {
	p := meta(w.Root, "fsmonitor")
	if tok == "" {
		os.Remove(p)
		return
	}
	os.WriteFile(p, []byte(tok), 0o644)
}

// watchedStatus computes status from the watcher's change journal.
func (w *Worktree) watchedStatus(idx Index) ([]FileStatus, bool) {
	tok, err := os.ReadFile(meta(w.Root, "fsmonitor"))
	if err != nil {
		return nil, false
	}
	c := watch.Dial(w.Root)
	if c == nil {
		return nil, false
	}
	ch, err := c.Since(strings.TrimSpace(string(tok)))
	if err != nil || ch.Full {
		return nil, false
	}
	ig := loadIgnore(w.Root)
	cand := map[string]bool{}
	for _, p := range ch.Paths {
		if dir, ok := strings.CutSuffix(p, "/"); ok {
			// A directory appeared, vanished or moved: rescan it.
			filepath.WalkDir(w.abs(dir), func(fp string, d fs.DirEntry, err error) error {
				if err != nil {
					return nil
				}
				rel, _ := filepath.Rel(w.Root, fp)
				rel = filepath.ToSlash(rel)
				if d.IsDir() {
					if ig.match(rel, true) {
						return filepath.SkipDir
					}
					return nil
				}
				cand[rel] = true
				return nil
			})
			for ip := range idx {
				if strings.HasPrefix(ip, dir+"/") {
					cand[ip] = true
				}
			}
			continue
		}
		cand[p] = true
	}
	var out []FileStatus
	for p := range cand {
		if !w.inSparse(p) || ig.match(p, false) {
			continue
		}
		info, err := os.Lstat(w.abs(p))
		e := idx[p]
		if err != nil {
			if e != nil {
				out = append(out, FileStatus{Path: p, State: 'D', Mode: e.Mode})
			}
			continue
		}
		if info.IsDir() {
			continue
		}
		mode := modeOf(info)
		if e != nil && e.Size == info.Size() && e.Mtime == info.ModTime().UnixNano() && sameMode(e.Mode, mode) {
			continue
		}
		content, err := readContent(w.abs(p), mode)
		if err != nil {
			continue
		}
		id := object.Hash(object.KindBlob, content)
		state := byte('A')
		if e != nil {
			if e.Blob == id && sameMode(e.Mode, mode) {
				continue
			}
			state = 'M'
		}
		out = append(out, FileStatus{Path: p, State: state, Mode: mode, Blob: id, Size: info.Size(), Mtime: info.ModTime().UnixNano()})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Path < out[j].Path })
	return out, true
}
