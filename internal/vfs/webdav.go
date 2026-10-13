package vfs

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"net/http"
	"os"

	"github.com/go-git/go-billy/v5"
	"golang.org/x/net/webdav"
)

// WebDAV export. Windows (net use), macOS (Finder) and Linux (gio, davfs)
// can mount WebDAV as an ordinary user, which makes it the portable choice
// where NFS or FUSE need privileges.

type davFS struct{ fs billy.Filesystem }

// WebDAVHandler serves fs over WebDAV.
func WebDAVHandler(bfs billy.Filesystem) http.Handler {
	return &webdav.Handler{FileSystem: davFS{bfs}, LockSystem: webdav.NewMemLS()}
}

func (d davFS) Mkdir(_ context.Context, name string, perm os.FileMode) error {
	if _, err := d.fs.Stat(name); err == nil {
		return fs.ErrExist
	}
	return d.fs.MkdirAll(name, perm)
}

func (d davFS) RemoveAll(_ context.Context, name string) error { return d.fs.Remove(name) }

func (d davFS) Rename(_ context.Context, from, to string) error { return d.fs.Rename(from, to) }

func (d davFS) Stat(_ context.Context, name string) (os.FileInfo, error) { return d.fs.Stat(name) }

func (d davFS) OpenFile(_ context.Context, name string, flag int, perm os.FileMode) (webdav.File, error) {
	if flag&(os.O_CREATE|os.O_WRONLY|os.O_RDWR|os.O_TRUNC|os.O_APPEND) == 0 {
		if fi, err := d.fs.Stat(name); err == nil && fi.IsDir() {
			return &davDir{fs: d.fs, name: name, fi: fi}, nil
		}
	}
	f, err := d.fs.OpenFile(name, flag, perm)
	if err != nil {
		return nil, err
	}
	return &davFile{File: f, fs: d.fs, name: name}, nil
}

type davFile struct {
	billy.File
	fs   billy.Filesystem
	name string
}

func (f *davFile) Readdir(int) ([]fs.FileInfo, error) { return nil, errors.New("not a directory") }
func (f *davFile) Stat() (fs.FileInfo, error)         { return f.fs.Stat(f.name) }

type davDir struct {
	fs   billy.Filesystem
	name string
	fi   os.FileInfo
	read bool
}

func (d *davDir) Close() error                   { return nil }
func (d *davDir) Read([]byte) (int, error)       { return 0, io.EOF }
func (d *davDir) Seek(int64, int) (int64, error) { return 0, nil }
func (d *davDir) Write([]byte) (int, error)      { return 0, errors.New("is a directory") }
func (d *davDir) Stat() (fs.FileInfo, error)     { return d.fi, nil }
func (d *davDir) Readdir(count int) ([]fs.FileInfo, error) {
	if d.read {
		if count > 0 {
			return nil, io.EOF
		}
		return nil, nil
	}
	d.read = true
	return d.fs.ReadDir(d.name)
}
