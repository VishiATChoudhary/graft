package vfs

import (
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/go-git/go-billy/v5"

	"github.com/VishiATChoudhary/graft/internal/object"
)

// Overlay is a writable virtual checkout: a copy-on-write layer over a
// lazily fetched revision. Unmodified files are served from the revision
// (fetched on first read); written files live in a local overlay directory;
// deletions are recorded as whiteouts. Committing uploads only the overlay,
// and moving to a new revision just swaps the base tree, so landing never
// rewrites files on disk.
type Overlay struct {
	base  *FS
	upper string // overlay directory
	state string // state directory (whiteouts.json lives here)

	mu        sync.RWMutex
	whiteouts map[string]bool
	prefix    string // chroot prefix
	meta      []byte // contents of the synthetic .gft/mount.json
}

// NewOverlay layers stateDir/upper over base. meta is exposed read-only at
// .gft/mount.json so gft commands run inside the mount can find it.
func NewOverlay(base *FS, stateDir string, meta []byte) (*Overlay, error) {
	upper := filepath.Join(stateDir, "upper")
	if err := os.MkdirAll(upper, 0o755); err != nil {
		return nil, err
	}
	o := &Overlay{base: base, upper: upper, state: stateDir, whiteouts: map[string]bool{}, meta: meta}
	if b, err := os.ReadFile(filepath.Join(stateDir, "whiteouts.json")); err == nil {
		json.Unmarshal(b, &o.whiteouts)
	}
	return o, nil
}

func (o *Overlay) rel(name string) string {
	p := path.Clean("/" + strings.ReplaceAll(name, "\\", "/"))
	return strings.TrimPrefix(path.Join(o.prefix, p), "/")
}

func (o *Overlay) upperPath(rel string) string {
	return filepath.Join(o.upper, filepath.FromSlash(rel))
}

func (o *Overlay) saveWhiteouts() {
	b, _ := json.Marshal(o.whiteouts)
	tmp := filepath.Join(o.state, "whiteouts.json.tmp")
	if os.WriteFile(tmp, b, 0o644) == nil {
		os.Rename(tmp, filepath.Join(o.state, "whiteouts.json"))
	}
}

// whitedOut reports whether rel or an ancestor was deleted from the base.
func (o *Overlay) whitedOut(rel string) bool {
	for p := rel; ; {
		if o.whiteouts[p] {
			return true
		}
		i := strings.LastIndexByte(p, '/')
		if i < 0 {
			return false
		}
		p = p[:i]
	}
}

func (o *Overlay) isMeta(rel string) bool { return rel == ".gft" || rel == ".gft/mount.json" }

// baseEntry returns the revision's entry for rel, unless whited out.
func (o *Overlay) baseEntry(rel string) (object.TreeEntry, bool) {
	o.mu.RLock()
	wo := o.whitedOut(rel)
	o.mu.RUnlock()
	if wo {
		return object.TreeEntry{}, false
	}
	e, err := o.base.lookup(rel)
	return e, err == nil
}

type metaInfo struct {
	name string
	dir  bool
	size int64
}

func (m metaInfo) Name() string { return m.name }
func (m metaInfo) Size() int64  { return m.size }
func (m metaInfo) Mode() fs.FileMode {
	if m.dir {
		return fs.ModeDir | 0o555
	}
	return 0o444
}
func (m metaInfo) ModTime() time.Time { return time.Unix(0, 0) }
func (m metaInfo) IsDir() bool        { return m.dir }
func (m metaInfo) Sys() any           { return nil }

func (o *Overlay) stat(name string, follow bool) (os.FileInfo, error) {
	rel := o.rel(name)
	switch rel {
	case ".gft":
		return metaInfo{".gft", true, 0}, nil
	case ".gft/mount.json":
		return metaInfo{"mount.json", false, int64(len(o.meta))}, nil
	}
	up := o.upperPath(rel)
	var info os.FileInfo
	var err error
	if follow {
		info, err = os.Stat(up)
	} else {
		info, err = os.Lstat(up)
	}
	if err == nil {
		return info, nil
	}
	if _, ok := o.baseEntry(rel); !ok && rel != "" {
		return nil, &fs.PathError{Op: "stat", Path: name, Err: fs.ErrNotExist}
	}
	if follow {
		return o.base.Stat(rel)
	}
	return o.base.Lstat(rel)
}

func (o *Overlay) Stat(name string) (os.FileInfo, error)  { return o.stat(name, true) }
func (o *Overlay) Lstat(name string) (os.FileInfo, error) { return o.stat(name, false) }

func (o *Overlay) ReadDir(name string) ([]os.FileInfo, error) {
	rel := o.rel(name)
	if rel == ".gft" {
		return []os.FileInfo{metaInfo{"mount.json", false, int64(len(o.meta))}}, nil
	}
	byName := map[string]os.FileInfo{}
	if e, ok := o.baseEntry(rel); ok && e.Mode == object.ModeDir {
		if infos, err := o.base.ReadDir(rel); err == nil {
			o.mu.RLock()
			for _, fi := range infos {
				if !o.whitedOut(path.Join(rel, fi.Name())) {
					byName[fi.Name()] = fi
				}
			}
			o.mu.RUnlock()
		}
	}
	ents, err := os.ReadDir(o.upperPath(rel))
	if err != nil && len(byName) == 0 && rel != "" {
		if _, ok := o.baseEntry(rel); !ok {
			return nil, &fs.PathError{Op: "readdir", Path: name, Err: fs.ErrNotExist}
		}
	}
	for _, e := range ents {
		if fi, err := e.Info(); err == nil {
			byName[e.Name()] = fi
		}
	}
	if rel == "" {
		byName[".gft"] = metaInfo{".gft", true, 0}
	}
	out := make([]os.FileInfo, 0, len(byName))
	for _, fi := range byName {
		out = append(out, fi)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name() < out[j].Name() })
	return out, nil
}

// copyUp materializes rel (and its parent directories) in the overlay.
func (o *Overlay) copyUp(rel string, withContent bool) error {
	up := o.upperPath(rel)
	if _, err := os.Lstat(up); err == nil {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(up), 0o755); err != nil {
		return err
	}
	e, ok := o.baseEntry(rel)
	if !ok {
		return nil
	}
	if e.Mode == object.ModeDir {
		return os.MkdirAll(up, 0o755)
	}
	var data []byte
	if withContent {
		var err error
		if data, err = o.base.blob(e.ID); err != nil {
			return err
		}
	}
	if e.Mode == object.ModeSymlink {
		b, err := o.base.blob(e.ID)
		if err != nil {
			return err
		}
		return os.Symlink(string(b), up)
	}
	perm := os.FileMode(0o644)
	if e.Mode == object.ModeExec {
		perm = 0o755
	}
	return os.WriteFile(up, data, perm)
}

func (o *Overlay) unwhite(rel string) {
	o.mu.Lock()
	changed := false
	for p := rel; ; {
		if o.whiteouts[p] {
			delete(o.whiteouts, p)
			changed = true
		}
		i := strings.LastIndexByte(p, '/')
		if i < 0 {
			break
		}
		p = p[:i]
	}
	if changed {
		o.saveWhiteouts()
	}
	o.mu.Unlock()
}

func (o *Overlay) Open(name string) (billy.File, error) { return o.OpenFile(name, os.O_RDONLY, 0) }

func (o *Overlay) Create(name string) (billy.File, error) {
	return o.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o666)
}

func (o *Overlay) OpenFile(name string, flag int, perm os.FileMode) (billy.File, error) {
	rel := o.rel(name)
	if o.isMeta(rel) {
		if flag&(os.O_WRONLY|os.O_RDWR|os.O_CREATE|os.O_TRUNC|os.O_APPEND) != 0 {
			return nil, &fs.PathError{Op: "open", Path: name, Err: errReadOnly}
		}
		return &memFile{name: name, r: strings.NewReader(string(o.meta))}, nil
	}
	write := flag&(os.O_WRONLY|os.O_RDWR|os.O_CREATE|os.O_TRUNC|os.O_APPEND) != 0
	up := o.upperPath(rel)
	if !write {
		if _, err := os.Lstat(up); err == nil {
			f, err := os.Open(up)
			if err != nil {
				return nil, err
			}
			return &osFile{f, name}, nil
		}
		if _, ok := o.baseEntry(rel); !ok {
			return nil, &fs.PathError{Op: "open", Path: name, Err: fs.ErrNotExist}
		}
		return o.base.OpenFile(rel, flag, perm)
	}
	if err := o.copyUp(rel, flag&os.O_TRUNC == 0); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Dir(up), 0o755); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(up, flag, perm)
	if err != nil {
		return nil, err
	}
	o.unwhite(rel)
	return &osFile{f, name}, nil
}

func (o *Overlay) Remove(name string) error {
	rel := o.rel(name)
	if o.isMeta(rel) || rel == "" {
		return errReadOnly
	}
	up := o.upperPath(rel)
	inUpper := false
	if _, err := os.Lstat(up); err == nil {
		inUpper = true
		if err := os.RemoveAll(up); err != nil {
			return err
		}
	}
	_, inBase := o.baseEntry(rel)
	if !inUpper && !inBase {
		return &fs.PathError{Op: "remove", Path: name, Err: fs.ErrNotExist}
	}
	if inBase {
		o.mu.Lock()
		o.whiteouts[rel] = true
		o.saveWhiteouts()
		o.mu.Unlock()
	}
	return nil
}

func (o *Overlay) Rename(from, to string) error {
	src, dst := o.rel(from), o.rel(to)
	if o.isMeta(src) || o.isMeta(dst) {
		return errReadOnly
	}
	info, err := o.Lstat(from)
	if err != nil {
		return err
	}
	// Materialize the source in the overlay (recursively for directories),
	// then move it and white out the old base path.
	if info.IsDir() {
		if err := o.copyTree(src); err != nil {
			return err
		}
	} else if err := o.copyUp(src, true); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(o.upperPath(dst)), 0o755); err != nil {
		return err
	}
	os.RemoveAll(o.upperPath(dst))
	if err := os.Rename(o.upperPath(src), o.upperPath(dst)); err != nil {
		return err
	}
	o.unwhite(dst)
	if _, inBase := o.baseEntry(src); inBase {
		o.mu.Lock()
		o.whiteouts[src] = true
		o.saveWhiteouts()
		o.mu.Unlock()
	}
	return nil
}

func (o *Overlay) copyTree(rel string) error {
	if err := o.copyUp(rel, true); err != nil {
		return err
	}
	infos, err := o.ReadDir(rel)
	if err != nil {
		return err
	}
	for _, fi := range infos {
		child := path.Join(rel, fi.Name())
		if fi.IsDir() {
			if err := o.copyTree(child); err != nil {
				return err
			}
		} else if err := o.copyUp(child, true); err != nil {
			return err
		}
	}
	return nil
}

func (o *Overlay) MkdirAll(name string, perm os.FileMode) error {
	rel := o.rel(name)
	if o.isMeta(rel) {
		return errReadOnly
	}
	if err := os.MkdirAll(o.upperPath(rel), 0o755); err != nil {
		return err
	}
	o.unwhite(rel)
	return nil
}

func (o *Overlay) Symlink(target, link string) error {
	rel := o.rel(link)
	up := o.upperPath(rel)
	if err := os.MkdirAll(filepath.Dir(up), 0o755); err != nil {
		return err
	}
	if err := os.Symlink(target, up); err != nil {
		return err
	}
	o.unwhite(rel)
	return nil
}

func (o *Overlay) Readlink(name string) (string, error) {
	rel := o.rel(name)
	if t, err := os.Readlink(o.upperPath(rel)); err == nil {
		return t, nil
	}
	if _, ok := o.baseEntry(rel); !ok {
		return "", &fs.PathError{Op: "readlink", Path: name, Err: fs.ErrNotExist}
	}
	return o.base.Readlink(rel)
}

func (o *Overlay) TempFile(dir, prefix string) (billy.File, error) {
	rel := o.rel(dir)
	if err := os.MkdirAll(o.upperPath(rel), 0o755); err != nil {
		return nil, err
	}
	f, err := os.CreateTemp(o.upperPath(rel), prefix)
	if err != nil {
		return nil, err
	}
	name, _ := filepath.Rel(o.upper, f.Name())
	return &osFile{f, "/" + filepath.ToSlash(name)}, nil
}

func (o *Overlay) Join(elem ...string) string { return path.Join(elem...) }
func (o *Overlay) Root() string               { return "/" + o.prefix }

func (o *Overlay) Chroot(p string) (billy.Filesystem, error) {
	c := &Overlay{base: o.base, upper: o.upper, state: o.state, whiteouts: o.whiteouts, prefix: o.rel(p), meta: o.meta}
	return c, nil
}

func (o *Overlay) Capabilities() billy.Capability {
	return billy.ReadCapability | billy.WriteCapability | billy.SeekCapability | billy.ReadAndWriteCapability | billy.TruncateCapability
}

func (o *Overlay) Chmod(name string, mode os.FileMode) error {
	rel := o.rel(name)
	if o.isMeta(rel) {
		return errReadOnly
	}
	if err := o.copyUp(rel, true); err != nil {
		return err
	}
	return os.Chmod(o.upperPath(rel), mode)
}

func (o *Overlay) Lchown(string, int, int) error { return nil }
func (o *Overlay) Chown(string, int, int) error  { return nil }

func (o *Overlay) Chtimes(name string, atime, mtime time.Time) error {
	rel := o.rel(name)
	if _, err := os.Lstat(o.upperPath(rel)); err != nil {
		return nil // base files keep the revision's time
	}
	return os.Chtimes(o.upperPath(rel), atime, mtime)
}

// Change is a modification in the overlay relative to the base revision.
type Change struct {
	Path    string
	Deleted bool
	Mode    object.Mode
	Content []byte
}

// Changes lists overlay files that differ from the base, and base files
// removed by whiteouts. Copy-ups whose content is unchanged are skipped.
func (o *Overlay) Changes() ([]Change, error) {
	var out []Change
	seen := map[string]bool{}
	err := filepath.WalkDir(o.upper, func(fp string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		rel, _ := filepath.Rel(o.upper, fp)
		rel = filepath.ToSlash(rel)
		if osJunk(path.Base(rel)) {
			return nil
		}
		info, err := os.Lstat(fp)
		if err != nil {
			return err
		}
		mode := object.ModeFile
		var content []byte
		switch {
		case info.Mode()&fs.ModeSymlink != 0:
			mode = object.ModeSymlink
			t, err := os.Readlink(fp)
			if err != nil {
				return err
			}
			content = []byte(t)
		default:
			if info.Mode()&0o111 != 0 {
				mode = object.ModeExec
			}
			if content, err = os.ReadFile(fp); err != nil {
				return err
			}
		}
		seen[rel] = true
		if e, ok := o.baseEntry(rel); ok && e.Mode == mode && e.ID == object.Hash(object.KindBlob, content) {
			return nil // copied up but unchanged
		}
		out = append(out, Change{Path: rel, Mode: mode, Content: content})
		return nil
	})
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return nil, err
	}
	o.mu.RLock()
	wo := make([]string, 0, len(o.whiteouts))
	for p := range o.whiteouts {
		wo = append(wo, p)
	}
	o.mu.RUnlock()
	for _, p := range wo {
		e, err := o.base.lookup(p)
		if err != nil {
			continue
		}
		var walk func(rel string, e object.TreeEntry) error
		walk = func(rel string, e object.TreeEntry) error {
			if e.Mode != object.ModeDir {
				if !seen[rel] {
					out = append(out, Change{Path: rel, Deleted: true})
				}
				return nil
			}
			t, err := o.base.tree(e.ID)
			if err != nil {
				return err
			}
			for _, c := range t.Entries {
				if err := walk(path.Join(rel, c.Name), c); err != nil {
					return err
				}
			}
			return nil
		}
		if err := walk(p, e); err != nil {
			return nil, err
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Path < out[j].Path })
	return out, nil
}

// Rebase switches the base revision and clears the overlay (after the
// overlay's changes were committed into root).
func (o *Overlay) Rebase(root object.ID, mtime time.Time) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.base.SetRoot(root, mtime)
	if err := os.RemoveAll(o.upper); err != nil {
		return err
	}
	if err := os.MkdirAll(o.upper, 0o755); err != nil {
		return err
	}
	o.whiteouts = map[string]bool{}
	o.saveWhiteouts()
	return nil
}

// Clean reports whether the overlay has no changes.
func (o *Overlay) Clean() bool {
	ch, err := o.Changes()
	return err == nil && len(ch) == 0
}

type osFile struct {
	*os.File
	name string
}

func (f *osFile) Name() string  { return f.name }
func (f *osFile) Lock() error   { return nil }
func (f *osFile) Unlock() error { return nil }

type memFile struct {
	name string
	r    *strings.Reader
}

func (m *memFile) Name() string                            { return m.name }
func (m *memFile) Read(p []byte) (int, error)              { return m.r.Read(p) }
func (m *memFile) ReadAt(p []byte, off int64) (int, error) { return m.r.ReadAt(p, off) }
func (m *memFile) Seek(off int64, w int) (int64, error)    { return m.r.Seek(off, w) }
func (m *memFile) Write([]byte) (int, error)               { return 0, errReadOnly }
func (m *memFile) Close() error                            { return nil }
func (m *memFile) Lock() error                             { return nil }
func (m *memFile) Unlock() error                           { return nil }
func (m *memFile) Truncate(int64) error                    { return errReadOnly }

var _ billy.Filesystem = (*Overlay)(nil)
var _ billy.Change = (*Overlay)(nil)
var _ io.ReaderAt = (*memFile)(nil)

// osJunk reports files operating systems create on network mounts (macOS
// AppleDouble metadata and Finder state) that never belong in a commit.
func osJunk(name string) bool {
	return strings.HasPrefix(name, "._") || name == ".DS_Store"
}
