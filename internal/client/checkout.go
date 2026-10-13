package client

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strconv"
	"sync"

	"github.com/VishiATChoudhary/graft/internal/object"
	"github.com/VishiATChoudhary/graft/internal/store"
)

const (
	blobBatch   = 2048
	treeBatch   = 4096
	fetchWorker = 8
)

// CheckoutStats summarises a checkout.
type CheckoutStats struct {
	Trees, Files, Deleted int
	Bytes                 int64
}

type blobJob struct {
	id    object.ID
	paths []string
	modes []object.Mode
}

// Checkout moves the working copy to rev. A fresh working copy is
// materialized by walking from the root tree, requesting only the trees
// inside the sparse spec and then their blobs. An existing working copy is
// updated using a server-computed diff, so only changed files move.
// Local modifications to paths that would change are refused unless force.
func (w *Worktree) Checkout(ctx context.Context, rev string, force bool) (*CheckoutStats, error) {
	ci, err := w.API.Resolve(ctx, rev)
	if err != nil {
		return nil, err
	}
	idx, err := w.LoadIndex()
	if err != nil {
		return nil, err
	}
	var st *CheckoutStats
	if w.Head.Changeset.IsZero() || len(idx) == 0 {
		st, err = w.materialize(ctx, ci.Root, idx)
	} else {
		st, err = w.update(ctx, w.Head.Changeset.String(), ci.ID.String(), idx, force)
	}
	if err != nil {
		return st, err
	}
	w.Head.Changeset, w.Head.Root = ci.ID, ci.Root
	if err := w.SaveIndex(idx); err != nil {
		return st, err
	}
	return st, w.SaveHead()
}

// fetchTrees fetches tree objects and decodes them.
func (w *Worktree) fetchTrees(ctx context.Context, ids []object.ID) (map[object.ID]*object.Tree, error) {
	out := make(map[object.ID]*object.Tree, len(ids))
	var mu sync.Mutex
	err := w.parallelFetch(ctx, ids, treeBatch, func(id object.ID, stored []byte) error {
		k, p, err := object.Verify(id, stored)
		if err != nil {
			return err
		}
		if k != object.KindTree {
			return fmt.Errorf("%s: expected tree", id.Short())
		}
		t, err := object.DecodeTree(p)
		if err != nil {
			return err
		}
		mu.Lock()
		out[id] = t
		mu.Unlock()
		return nil
	})
	return out, err
}

// parallelFetch splits ids into batches and fetches them concurrently. fn
// may be called from multiple goroutines.
func (w *Worktree) parallelFetch(ctx context.Context, ids []object.ID, batch int, fn func(object.ID, []byte) error) error {
	if len(ids) == 0 {
		return nil
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	work := make(chan []object.ID)
	errc := make(chan error, fetchWorker)
	var wg sync.WaitGroup
	for i := 0; i < fetchWorker; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for b := range work {
				if err := w.src.Fetch(ctx, b, fn); err != nil {
					errc <- err
					cancel()
					return
				}
			}
		}()
	}
	// Smaller batches early on keep all workers busy for small fetches.
	size := min(batch, max(64, len(ids)/fetchWorker+1))
loop:
	for start := 0; start < len(ids); start += size {
		select {
		case work <- ids[start:min(start+size, len(ids))]:
		case <-ctx.Done():
			break loop
		}
	}
	close(work)
	wg.Wait()
	select {
	case err := <-errc:
		return err
	default:
		return ctx.Err()
	}
}

func (w *Worktree) materialize(ctx context.Context, root object.ID, idx Index) (*CheckoutStats, error) {
	st := &CheckoutStats{}
	type dirRef struct {
		path string
		id   object.ID
	}
	level := []dirRef{}
	if !root.IsZero() {
		level = append(level, dirRef{"", root})
	}
	jobs := map[object.ID]*blobJob{}
	var order []object.ID
	for len(level) > 0 {
		ids := make([]object.ID, 0, len(level))
		seen := map[object.ID]bool{}
		for _, d := range level {
			if !seen[d.id] {
				seen[d.id] = true
				ids = append(ids, d.id)
			}
		}
		trees, err := w.fetchTrees(ctx, ids)
		if err != nil {
			return st, err
		}
		st.Trees += len(ids)
		var next []dirRef
		for _, d := range level {
			for _, e := range trees[d.id].Entries {
				p := path.Join(d.path, e.Name)
				if e.Mode == object.ModeDir {
					if w.towardSparse(p) {
						next = append(next, dirRef{p, e.ID})
					}
					continue
				}
				if !w.inSparse(p) || e.Mode == object.ModeGitlink {
					continue
				}
				if old := idx[p]; old != nil && old.Blob == e.ID && old.Mode == e.Mode {
					continue // already materialized
				}
				j := jobs[e.ID]
				if j == nil {
					j = &blobJob{id: e.ID}
					jobs[e.ID] = j
					order = append(order, e.ID)
				}
				j.paths = append(j.paths, p)
				j.modes = append(j.modes, e.Mode)
			}
		}
		level = next
	}
	return st, w.writeBlobs(ctx, jobs, order, idx, st)
}

// writeBlobs fetches blobs and writes every path that uses them.
func (w *Worktree) writeBlobs(ctx context.Context, jobs map[object.ID]*blobJob, order []object.ID, idx Index, st *CheckoutStats) error {
	var mu sync.Mutex
	dirs := sync.Map{}
	mkdir := func(dir string) error {
		if _, ok := dirs.Load(dir); ok {
			return nil
		}
		if err := os.MkdirAll(dir, 0o755); err != nil {
			// A file may be in the way of a new directory.
			if err2 := removeBlocking(dir, w.Root); err2 != nil {
				return err
			}
			if err := os.MkdirAll(dir, 0o755); err != nil {
				return err
			}
		}
		dirs.Store(dir, true)
		return nil
	}
	// Writes are I/O bound; overlap them with fetching.
	type write struct {
		path    string
		mode    object.Mode
		id      object.ID
		content []byte
	}
	// Each directory is owned by one writer, so concurrent creates do not
	// contend on the same directory lock.
	nw := writers()
	queues := make([]chan write, nw)
	var werr error
	var wwg sync.WaitGroup
	for i := 0; i < nw; i++ {
		queues[i] = make(chan write, 256)
		wwg.Add(1)
		go func(writes chan write) {
			defer wwg.Done()
			for wr := range writes {
				fp := w.abs(wr.path)
				err := mkdir(filepath.Dir(fp))
				var info os.FileInfo
				if err == nil {
					info, err = writeFile(fp, wr.mode, wr.content)
				}
				mu.Lock()
				if err != nil {
					if werr == nil {
						werr = err
					}
				} else {
					idx[wr.path] = &IndexEntry{Mode: wr.mode, Size: info.Size(), Mtime: info.ModTime().UnixNano(), Blob: wr.id}
					st.Files++
					st.Bytes += int64(len(wr.content))
				}
				mu.Unlock()
			}
		}(queues[i])
	}
	ferr := w.parallelFetch(ctx, order, blobBatch, func(id object.ID, stored []byte) error {
		k, p, err := object.Verify(id, stored)
		if err != nil {
			return err
		}
		if k != object.KindBlob {
			return fmt.Errorf("%s: expected blob", id.Short())
		}
		j := jobs[id]
		for i, pth := range j.paths {
			queues[dirShard(pth, nw)] <- write{pth, j.modes[i], id, p}
		}
		return nil
	})
	for _, q := range queues {
		close(q)
	}
	wwg.Wait()
	if ferr != nil {
		return ferr
	}
	return werr
}

func dirShard(p string, n int) int {
	d := path.Dir(p)
	var h uint32 = 2166136261
	for i := 0; i < len(d); i++ {
		h = (h ^ uint32(d[i])) * 16777619
	}
	return int(h % uint32(n))
}

// writers is the number of concurrent file writers. File creation is
// kernel-bound and contends on directory locks, so more is not better.
func writers() int {
	if v, err := strconv.Atoi(os.Getenv("GFT_WRITERS")); err == nil && v > 0 {
		return v
	}
	if runtime.GOOS == "darwin" {
		return 2 // APFS serializes creates per volume: more writers only burn kernel CPU
	}
	return min(runtime.GOMAXPROCS(0), 4)
}

// removeBlocking removes a file sitting where a directory must go.
func removeBlocking(dir, root string) error {
	for d := dir; len(d) > len(root); d = filepath.Dir(d) {
		if info, err := os.Lstat(d); err == nil && !info.IsDir() {
			return os.Remove(d)
		}
	}
	return errors.New("no blocking file")
}

// writeFile writes a working-copy file and returns its stat information.
// The common case (a new file) is open(O_EXCL) + write + fstat + close.
func writeFile(fp string, mode object.Mode, content []byte) (os.FileInfo, error) {
	perm := os.FileMode(0o644)
	if mode == object.ModeExec {
		perm = 0o755
	}
	if mode != object.ModeSymlink {
		f, err := os.OpenFile(fp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, perm)
		if err == nil {
			_, werr := f.Write(content)
			var info os.FileInfo
			if werr == nil {
				info, werr = f.Stat()
			}
			if cerr := f.Close(); werr == nil {
				werr = cerr
			}
			return info, werr
		}
		if !errors.Is(err, fs.ErrExist) {
			return nil, err
		}
	}
	// Something is in the way: replace it.
	if info, err := os.Lstat(fp); err == nil {
		if info.IsDir() {
			if err := os.RemoveAll(fp); err != nil {
				return nil, err
			}
		} else if err := os.Remove(fp); err != nil {
			return nil, err
		}
	}
	if mode == object.ModeSymlink {
		if err := os.Symlink(string(content), fp); err != nil {
			return nil, err
		}
		return os.Lstat(fp)
	}
	f, err := os.OpenFile(fp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, perm)
	if err != nil {
		return nil, err
	}
	_, werr := f.Write(content)
	var info os.FileInfo
	if werr == nil {
		info, werr = f.Stat()
	}
	if cerr := f.Close(); werr == nil {
		werr = cerr
	}
	return info, werr
}

func (w *Worktree) update(ctx context.Context, from, to string, idx Index, force bool) (*CheckoutStats, error) {
	st := &CheckoutStats{}
	d, err := w.API.Diff(ctx, from, to, w.Config.Sparse)
	if err != nil {
		return st, err
	}
	if len(d.Changes) == 0 {
		return st, nil
	}
	if !force {
		// Only the paths this update touches can be clobbered, so only
		// they are checked (not the whole working copy).
		var dirty []string
		for _, c := range d.Changes {
			if w.inSparse(c.Path) && w.locallyModified(c.Path, idx) {
				dirty = append(dirty, c.Path)
			}
		}
		if len(dirty) > 0 {
			return st, fmt.Errorf("local changes would be overwritten: %v (commit them or use --force)", dirty)
		}
	}
	jobs := map[object.ID]*blobJob{}
	var order []object.ID
	for _, c := range d.Changes {
		if !w.inSparse(c.Path) {
			continue
		}
		if c.NewMode == object.ModeNone || c.NewMode == object.ModeGitlink {
			fp := w.abs(c.Path)
			if err := os.Remove(fp); err != nil && !errors.Is(err, fs.ErrNotExist) {
				return st, err
			}
			pruneEmpty(filepath.Dir(fp), w.Root)
			delete(idx, c.Path)
			st.Deleted++
			continue
		}
		j := jobs[c.NewBlob]
		if j == nil {
			j = &blobJob{id: c.NewBlob}
			jobs[c.NewBlob] = j
			order = append(order, c.NewBlob)
		}
		j.paths = append(j.paths, c.Path)
		j.modes = append(j.modes, c.NewMode)
	}
	return st, w.writeBlobs(ctx, jobs, order, idx, st)
}

// locallyModified reports whether path differs from what the index
// recorded (including untracked files that a checkout would overwrite).
func (w *Worktree) locallyModified(p string, idx Index) bool {
	info, err := os.Lstat(w.abs(p))
	e := idx[p]
	if err != nil {
		return e != nil // deleted locally
	}
	if e == nil {
		return true // untracked file in the way
	}
	mode := modeOf(info)
	if e.Size == info.Size() && e.Mtime == info.ModTime().UnixNano() && e.Mode == mode {
		return false
	}
	content, err := readContent(w.abs(p), mode)
	return err != nil || object.Hash(object.KindBlob, content) != e.Blob || e.Mode != mode
}

func pruneEmpty(dir, root string) {
	for len(dir) > len(root) {
		if err := os.Remove(dir); err != nil {
			return
		}
		dir = filepath.Dir(dir)
	}
}

// ApplySparse reconciles the working copy with a changed sparse spec:
// files that left the spec are removed and newly included directories are
// materialized. Files already present are not rewritten.
func (w *Worktree) ApplySparse(ctx context.Context) (*CheckoutStats, error) {
	idx, err := w.LoadIndex()
	if err != nil {
		return nil, err
	}
	removed := 0
	for p := range idx {
		if !w.inSparse(p) {
			fp := w.abs(p)
			if err := os.Remove(fp); err != nil && !errors.Is(err, fs.ErrNotExist) {
				return nil, err
			}
			pruneEmpty(filepath.Dir(fp), w.Root)
			delete(idx, p)
			removed++
		}
	}
	st, err := w.materialize(ctx, w.Head.Root, idx)
	if st != nil {
		st.Deleted = removed
	}
	if err != nil {
		return st, err
	}
	if err := w.SaveIndex(idx); err != nil {
		return st, err
	}
	return st, w.SaveConfig()
}

// ReadObject fetches and verifies a single object through the source.
func (w *Worktree) ReadObject(ctx context.Context, id object.ID) (object.Kind, []byte, error) {
	var kind object.Kind
	var payload []byte
	err := w.src.Fetch(ctx, []object.ID{id}, func(id object.ID, b []byte) error {
		var err error
		kind, payload, err = object.Verify(id, b)
		return err
	})
	return kind, payload, err
}

var _ store.Objects = (*store.DiskCache)(nil)
