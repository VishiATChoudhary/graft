package vfs

import (
	"errors"
	"io"
	"io/fs"
	"os"
	"syscall"
	"time"

	"github.com/go-git/go-billy/v5"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/hanwen/go-fuse/v2/fuse/nodefs"
	"github.com/hanwen/go-fuse/v2/fuse/pathfs"
)

// FUSE export (Linux). fusermount is setuid, so unprivileged users can
// mount: no root needed, unlike NFS on Linux.

type fuseFS struct {
	pathfs.FileSystem
	fs billy.Filesystem
}

func errno(err error) fuse.Status {
	switch {
	case err == nil:
		return fuse.OK
	case errors.Is(err, fs.ErrNotExist):
		return fuse.ENOENT
	case errors.Is(err, errReadOnly):
		return fuse.EROFS
	case errors.Is(err, fs.ErrExist):
		return fuse.Status(syscall.EEXIST)
	case errors.Is(err, fs.ErrPermission):
		return fuse.EPERM
	}
	return fuse.ToStatus(err)
}

func toAttr(fi os.FileInfo) *fuse.Attr {
	a := &fuse.Attr{Size: uint64(fi.Size()), Nlink: 1}
	perm := uint32(fi.Mode().Perm())
	switch {
	case fi.IsDir():
		a.Mode = fuse.S_IFDIR | perm
		a.Nlink = 2
	case fi.Mode()&fs.ModeSymlink != 0:
		a.Mode = fuse.S_IFLNK | 0o777
	default:
		a.Mode = fuse.S_IFREG | perm
	}
	t := fi.ModTime()
	a.SetTimes(&t, &t, &t)
	a.Blocks = (a.Size + 511) / 512
	return a
}

func (f *fuseFS) GetAttr(name string, _ *fuse.Context) (*fuse.Attr, fuse.Status) {
	fi, err := f.fs.Lstat(name)
	if err != nil {
		return nil, errno(err)
	}
	return toAttr(fi), fuse.OK
}

func (f *fuseFS) OpenDir(name string, _ *fuse.Context) ([]fuse.DirEntry, fuse.Status) {
	infos, err := f.fs.ReadDir(name)
	if err != nil {
		return nil, errno(err)
	}
	out := make([]fuse.DirEntry, 0, len(infos))
	for _, fi := range infos {
		out = append(out, fuse.DirEntry{Name: fi.Name(), Mode: toAttr(fi).Mode})
	}
	return out, fuse.OK
}

func (f *fuseFS) Open(name string, flags uint32, _ *fuse.Context) (nodefs.File, fuse.Status) {
	// The kernel computes append offsets itself, and WriteAt refuses
	// files opened with O_APPEND.
	bf, err := f.fs.OpenFile(name, int(flags)&^(syscall.O_EXCL|syscall.O_APPEND), 0)
	if err != nil {
		return nil, errno(err)
	}
	return &fuseFile{File: nodefs.NewDefaultFile(), f: bf, fs: f.fs, name: name}, fuse.OK
}

func (f *fuseFS) Create(name string, flags uint32, mode uint32, _ *fuse.Context) (nodefs.File, fuse.Status) {
	bf, err := f.fs.OpenFile(name, (int(flags)|os.O_CREATE)&^syscall.O_APPEND, os.FileMode(mode))
	if err != nil {
		return nil, errno(err)
	}
	return &fuseFile{File: nodefs.NewDefaultFile(), f: bf, fs: f.fs, name: name}, fuse.OK
}

func (f *fuseFS) Unlink(name string, _ *fuse.Context) fuse.Status { return errno(f.fs.Remove(name)) }
func (f *fuseFS) Rmdir(name string, _ *fuse.Context) fuse.Status  { return errno(f.fs.Remove(name)) }
func (f *fuseFS) Mkdir(name string, mode uint32, _ *fuse.Context) fuse.Status {
	return errno(f.fs.MkdirAll(name, os.FileMode(mode)))
}
func (f *fuseFS) Rename(from, to string, _ *fuse.Context) fuse.Status {
	return errno(f.fs.Rename(from, to))
}
func (f *fuseFS) Symlink(value, link string, _ *fuse.Context) fuse.Status {
	return errno(f.fs.Symlink(value, link))
}
func (f *fuseFS) Readlink(name string, _ *fuse.Context) (string, fuse.Status) {
	t, err := f.fs.Readlink(name)
	return t, errno(err)
}
func (f *fuseFS) Chmod(name string, mode uint32, _ *fuse.Context) fuse.Status {
	if c, ok := f.fs.(billy.Change); ok {
		return errno(c.Chmod(name, os.FileMode(mode)))
	}
	return fuse.EROFS
}
func (f *fuseFS) Utimens(name string, a, m *time.Time, _ *fuse.Context) fuse.Status {
	if c, ok := f.fs.(billy.Change); ok && a != nil && m != nil {
		return errno(c.Chtimes(name, *a, *m))
	}
	return fuse.OK
}
func (f *fuseFS) Truncate(name string, size uint64, _ *fuse.Context) fuse.Status {
	bf, err := f.fs.OpenFile(name, os.O_WRONLY, 0)
	if err != nil {
		return errno(err)
	}
	defer bf.Close()
	return errno(bf.Truncate(int64(size)))
}
func (f *fuseFS) Access(string, uint32, *fuse.Context) fuse.Status { return fuse.OK }
func (f *fuseFS) String() string                                   { return "graft" }

type fuseFile struct {
	nodefs.File
	f    billy.File
	fs   billy.Filesystem
	name string
}

func (ff *fuseFile) Read(dest []byte, off int64) (fuse.ReadResult, fuse.Status) {
	n, err := ff.f.ReadAt(dest, off)
	if err != nil && err != io.EOF {
		return nil, errno(err)
	}
	return fuse.ReadResultData(dest[:n]), fuse.OK
}

func (ff *fuseFile) Write(data []byte, off int64) (uint32, fuse.Status) {
	w, ok := ff.f.(io.WriterAt)
	if !ok {
		return 0, fuse.EROFS
	}
	n, err := w.WriteAt(data, off)
	return uint32(n), errno(err)
}

func (ff *fuseFile) GetAttr(out *fuse.Attr) fuse.Status {
	fi, err := ff.fs.Stat(ff.name)
	if err != nil {
		return errno(err)
	}
	*out = *toAttr(fi)
	return fuse.OK
}

func (ff *fuseFile) Truncate(size uint64) fuse.Status { return errno(ff.f.Truncate(int64(size))) }
func (ff *fuseFile) Flush() fuse.Status               { return fuse.OK }
func (ff *fuseFile) Fsync(int) fuse.Status            { return fuse.OK }
func (ff *fuseFile) Release()                         { ff.f.Close() }

// MountFUSE mounts fs at dir with FUSE and serves it until unmounted.
func MountFUSE(bfs billy.Filesystem, dir string) (unmount func() error, err error) {
	pfs := pathfs.NewPathNodeFs(&fuseFS{FileSystem: pathfs.NewDefaultFileSystem(), fs: bfs}, nil)
	opts := nodefs.NewOptions()
	opts.EntryTimeout, opts.AttrTimeout, opts.NegativeTimeout = 0, 0, 0 // the revision can change under us
	srv, _, err := nodefs.MountRoot(dir, pfs.Root(), opts)
	if err != nil {
		return nil, err
	}
	go srv.Serve()
	if err := srv.WaitMount(); err != nil {
		return nil, err
	}
	return srv.Unmount, nil
}
