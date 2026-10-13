// Package vfs provides a virtualized, read-only checkout of a revision.
//
// Nothing is materialized up front. Directory listings fetch the tree for
// that directory, stat is answered from tree entries (which carry sizes),
// and file contents are fetched on first read. Fetches go through the
// normal object source (daemon, cache, or server), so a job only pays for
// what it actually reads. The filesystem is exported over NFSv3 by
// `gft mount`, which works without kernel extensions on macOS and Linux.
package vfs

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-git/go-billy/v5"

	"github.com/VishiATChoudhary/graft/internal/fetch"
	"github.com/VishiATChoudhary/graft/internal/object"
	"github.com/VishiATChoudhary/graft/internal/store"
)

// FS is a billy.Filesystem over one root tree.
type FS struct {
	*shared
	prefix string // chroot prefix, slash-separated, no leading slash
}

type shared struct {
	src   fetch.Source
	root  object.ID
	mtime time.Time

	mu    sync.Mutex
	trees map[object.ID]*object.Tree
	blobs *store.MemLRU

	TreesFetched atomic.Int64
	BlobsFetched atomic.Int64
	BytesFetched atomic.Int64
}

// New creates a filesystem for a root tree. blobCache bounds the in-memory
// cache of decompressed file contents.
func New(src fetch.Source, root object.ID, mtime time.Time, blobCache int64) *FS {
	return &FS{shared: &shared{src: src, root: root, mtime: mtime, trees: map[object.ID]*object.Tree{}, blobs: store.NewMemLRU(blobCache)}}
}

// Stats reports how much was fetched lazily.
func (f *FS) Stats() (trees, blobs, bytes int64) {
	return f.TreesFetched.Load(), f.BlobsFetched.Load(), f.BytesFetched.Load()
}

var errReadOnly = errors.New("graft virtual checkout is read-only")

func (s *shared) fetch(id object.ID, want object.Kind) ([]byte, error) {
	var payload []byte
	err := s.src.Fetch(context.Background(), []object.ID{id}, func(id object.ID, b []byte) error {
		k, p, err := object.Verify(id, b)
		if err != nil {
			return err
		}
		if k != want {
			return fmt.Errorf("%s: unexpected kind", id.Short())
		}
		payload = p
		return nil
	})
	return payload, err
}

func (s *shared) tree(id object.ID) (*object.Tree, error) {
	if id.IsZero() {
		return &object.Tree{}, nil
	}
	s.mu.Lock()
	t := s.trees[id]
	s.mu.Unlock()
	if t != nil {
		return t, nil
	}
	p, err := s.fetch(id, object.KindTree)
	if err != nil {
		return nil, err
	}
	if t, err = object.DecodeTree(p); err != nil {
		return nil, err
	}
	s.TreesFetched.Add(1)
	s.mu.Lock()
	s.trees[id] = t
	s.mu.Unlock()
	return t, nil
}

func (s *shared) blob(id object.ID) ([]byte, error) {
	if b, err := s.blobs.Get(id); err == nil {
		return b, nil
	}
	p, err := s.fetch(id, object.KindBlob)
	if err != nil {
		return nil, err
	}
	s.BlobsFetched.Add(1)
	s.BytesFetched.Add(int64(len(p)))
	s.blobs.Put(id, p)
	return p, nil
}

func (f *FS) clean(name string) string {
	p := path.Clean("/" + strings.ReplaceAll(name, "\\", "/"))
	return strings.TrimPrefix(path.Join(f.prefix, p), "/")
}

// lookup resolves a slash path to a tree entry.
func (f *FS) lookup(name string) (object.TreeEntry, error) {
	p := f.clean(name)
	cur := object.TreeEntry{Name: "", Mode: object.ModeDir, ID: f.root}
	if p == "" {
		return cur, nil
	}
	for _, seg := range strings.Split(p, "/") {
		if cur.Mode != object.ModeDir {
			return object.TreeEntry{}, &fs.PathError{Op: "stat", Path: name, Err: fs.ErrNotExist}
		}
		t, err := f.tree(cur.ID)
		if err != nil {
			return object.TreeEntry{}, &fs.PathError{Op: "stat", Path: name, Err: err}
		}
		i := t.Find(seg)
		if i < 0 {
			return object.TreeEntry{}, &fs.PathError{Op: "stat", Path: name, Err: fs.ErrNotExist}
		}
		cur = t.Entries[i]
	}
	return cur, nil
}

type fileInfo struct {
	e     object.TreeEntry
	mtime time.Time
}

func (i fileInfo) Name() string {
	if i.e.Name == "" {
		return "/"
	}
	return i.e.Name
}
func (i fileInfo) Size() int64 {
	if i.e.Mode == object.ModeDir {
		return 4096
	}
	return int64(i.e.Size)
}
func (i fileInfo) Mode() fs.FileMode {
	switch i.e.Mode {
	case object.ModeDir:
		return fs.ModeDir | 0o555
	case object.ModeExec:
		return 0o555
	case object.ModeSymlink:
		return fs.ModeSymlink | 0o777
	}
	return 0o444
}
func (i fileInfo) ModTime() time.Time { return i.mtime }
func (i fileInfo) IsDir() bool        { return i.e.Mode == object.ModeDir }
func (i fileInfo) Sys() any           { return nil }

func (f *FS) Stat(name string) (os.FileInfo, error) {
	e, err := f.lookup(name)
	if err != nil {
		return nil, err
	}
	if e.Mode == object.ModeSymlink {
		// Follow one level, relative to the link's directory.
		target, err := f.Readlink(name)
		if err == nil {
			t := target
			if !path.IsAbs(t) {
				t = path.Join(path.Dir("/"+f.clean(name)), t)
			}
			if te, err := (&FS{shared: f.shared}).lookup(t); err == nil {
				te.Name = path.Base(name)
				return fileInfo{te, f.mtime}, nil
			}
		}
	}
	e.Name = path.Base("/" + f.clean(name))
	if f.clean(name) == "" {
		e.Name = ""
	}
	return fileInfo{e, f.mtime}, nil
}

func (f *FS) Lstat(name string) (os.FileInfo, error) {
	e, err := f.lookup(name)
	if err != nil {
		return nil, err
	}
	if f.clean(name) != "" {
		e.Name = path.Base("/" + f.clean(name))
	}
	return fileInfo{e, f.mtime}, nil
}

func (f *FS) ReadDir(name string) ([]os.FileInfo, error) {
	e, err := f.lookup(name)
	if err != nil {
		return nil, err
	}
	if e.Mode != object.ModeDir {
		return nil, &fs.PathError{Op: "readdir", Path: name, Err: errors.New("not a directory")}
	}
	t, err := f.tree(e.ID)
	if err != nil {
		return nil, err
	}
	out := make([]os.FileInfo, 0, len(t.Entries))
	for _, en := range t.Entries {
		out = append(out, fileInfo{en, f.mtime})
	}
	return out, nil
}

func (f *FS) Readlink(name string) (string, error) {
	e, err := f.lookup(name)
	if err != nil {
		return "", err
	}
	if e.Mode != object.ModeSymlink {
		return "", &fs.PathError{Op: "readlink", Path: name, Err: errors.New("not a symlink")}
	}
	b, err := f.blob(e.ID)
	return string(b), err
}

func (f *FS) Open(name string) (billy.File, error) { return f.OpenFile(name, os.O_RDONLY, 0) }

func (f *FS) OpenFile(name string, flag int, _ os.FileMode) (billy.File, error) {
	if flag&(os.O_WRONLY|os.O_RDWR|os.O_CREATE|os.O_TRUNC|os.O_APPEND) != 0 {
		return nil, &fs.PathError{Op: "open", Path: name, Err: errReadOnly}
	}
	e, err := f.lookup(name)
	if err != nil {
		return nil, err
	}
	if e.Mode == object.ModeDir {
		return nil, &fs.PathError{Op: "open", Path: name, Err: errors.New("is a directory")}
	}
	return &file{fs: f, name: name, e: e}, nil
}

func (f *FS) Join(elem ...string) string { return path.Join(elem...) }
func (f *FS) Root() string               { return "/" + f.prefix }

func (f *FS) Chroot(p string) (billy.Filesystem, error) {
	e, err := f.lookup(p)
	if err != nil {
		return nil, err
	}
	if e.Mode != object.ModeDir {
		return nil, errors.New("chroot: not a directory")
	}
	return &FS{shared: f.shared, prefix: f.clean(p)}, nil
}

func (f *FS) Capabilities() billy.Capability {
	return billy.ReadCapability | billy.SeekCapability
}

func (f *FS) Create(string) (billy.File, error)         { return nil, errReadOnly }
func (f *FS) Rename(string, string) error                { return errReadOnly }
func (f *FS) Remove(string) error                        { return errReadOnly }
func (f *FS) TempFile(string, string) (billy.File, error) { return nil, errReadOnly }
func (f *FS) MkdirAll(string, os.FileMode) error         { return errReadOnly }
func (f *FS) Symlink(string, string) error               { return errReadOnly }

// billy.Change, for setattr: reject everything.
func (f *FS) Chmod(string, os.FileMode) error              { return errReadOnly }
func (f *FS) Lchown(string, int, int) error                { return errReadOnly }
func (f *FS) Chown(string, int, int) error                 { return errReadOnly }
func (f *FS) Chtimes(string, time.Time, time.Time) error   { return errReadOnly }

type file struct {
	fs   *FS
	name string
	e    object.TreeEntry
	r    *bytes.Reader
}

func (fl *file) load() error {
	if fl.r != nil {
		return nil
	}
	b, err := fl.fs.blob(fl.e.ID)
	if err != nil {
		return err
	}
	fl.r = bytes.NewReader(b)
	return nil
}

func (fl *file) Name() string { return fl.name }
func (fl *file) Read(p []byte) (int, error) {
	if err := fl.load(); err != nil {
		return 0, err
	}
	return fl.r.Read(p)
}
func (fl *file) ReadAt(p []byte, off int64) (int, error) {
	if err := fl.load(); err != nil {
		return 0, err
	}
	return fl.r.ReadAt(p, off)
}
func (fl *file) Seek(off int64, whence int) (int64, error) {
	if err := fl.load(); err != nil {
		return 0, err
	}
	return fl.r.Seek(off, whence)
}
func (fl *file) Write([]byte) (int, error) { return 0, errReadOnly }
func (fl *file) Close() error              { return nil }
func (fl *file) Lock() error               { return nil }
func (fl *file) Unlock() error             { return nil }
func (fl *file) Truncate(int64) error      { return errReadOnly }

var _ billy.Filesystem = (*FS)(nil)
var _ billy.Change = (*FS)(nil)
var _ io.ReaderAt = (*file)(nil)
