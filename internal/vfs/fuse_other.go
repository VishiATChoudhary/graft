//go:build !linux

package vfs

import (
	"errors"

	"github.com/go-git/go-billy/v5"
)

// MountFUSE is available on Linux only (use the nfs or webdav backend).
func MountFUSE(billy.Filesystem, string) (func() error, error) {
	return nil, errors.New("the fuse backend is Linux-only; use --backend nfs or webdav")
}
