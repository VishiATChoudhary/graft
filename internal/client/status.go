package client

import (
	"bufio"
	"context"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"

	"github.com/VishiATChoudhary/graft/internal/object"
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
	case info.Mode()&0o111 != 0:
		return object.ModeExec
	}
	return object.ModeFile
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
func (w *Worktree) Status(ctx context.Context) ([]FileStatus, error) {
	idx, err := w.LoadIndex()
	if err != nil {
		return nil, err
	}
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
					if e.Blob == id && e.Mode == mode {
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
		if e := idx[f.path]; e != nil && e.Size == f.info.Size() && e.Mtime == f.info.ModTime().UnixNano() && e.Mode == modeOf(f.info) {
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
	return out, nil
}
