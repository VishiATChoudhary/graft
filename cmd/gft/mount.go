package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/go-git/go-billy/v5"
	nfs "github.com/willscott/go-nfs"
	nfshelper "github.com/willscott/go-nfs/helpers"

	"github.com/VishiATChoudhary/graft/internal/api"
	"github.com/VishiATChoudhary/graft/internal/fetch"
	"github.com/VishiATChoudhary/graft/internal/store"
	"github.com/VishiATChoudhary/graft/internal/vfs"
)

// cmdMount serves a virtualized checkout of a revision over a loopback
// NFSv3 server and mounts it. Files are fetched only when read.
func cmdMount(args []string) error {
	fs := flag.NewFlagSet("mount", flag.ExitOnError)
	rev := fs.String("rev", "main", "revision to expose")
	objects := fs.String("objects", "", "object endpoint (daemon socket or cache URL); default: server")
	cacheDir := fs.String("cache", "", "local object cache dir when no daemon (default: none, memory only)")
	mem := fs.String("mem", "512M", "in-memory cache of file contents")
	noMount := fs.Bool("no-mount", false, "only serve NFS and print the mount command")
	rdonly := fs.Bool("rdonly", false, "read-only view (no overlay; commit/land unavailable)")
	state := fs.String("state", "", "state directory for the writable overlay (default ~/.cache/gft/mounts/<hash>)")
	backend := fs.String("backend", "", "nfs (macOS default), fuse (Linux default; no root), webdav (Windows default)")
	pos := parse(fs, args)
	if len(pos) != 2 {
		return errors.New("usage: gft mount <server> <mountpoint> [--rev R]")
	}
	server, mnt := pos[0], pos[1]
	c := api.NewClient(server)
	ci, err := c.Resolve(bg(), *rev)
	if err != nil {
		return err
	}
	var src fetch.Source
	switch {
	case strings.HasPrefix(*objects, "unix://"):
		src = fetch.NewRemote(*objects)
	default:
		up := *objects
		if up == "" {
			up = server
		}
		var local store.Objects = store.NewMemLRU(mustSize(*mem))
		if *cacheDir != "" {
			dc, err := store.OpenDiskCache(*cacheDir, 0)
			if err != nil {
				return err
			}
			local = dc
		}
		src = fetch.NewTier(local, fetch.NewRemote(up))
	}
	vf := vfs.New(src, ci.Root, time.Unix(ci.Time, 0), mustSize(*mem))
	var served billy.Filesystem = vf
	if !*rdonly {
		absMnt, _ := filepath.Abs(mnt)
		if *state == "" {
			home, _ := os.UserHomeDir()
			h := sha256.Sum256([]byte(absMnt))
			*state = filepath.Join(home, ".cache", "gft", "mounts", hex.EncodeToString(h[:6]))
		}
		if err := os.MkdirAll(*state, 0o755); err != nil {
			return err
		}
		sock := filepath.Join(*state, "control.sock")
		meta, _ := json.Marshal(vfs.MountMeta{Control: sock, Server: server})
		ov, err := vfs.NewOverlay(vf, *state, meta)
		if err != nil {
			return err
		}
		ctl := &vfs.Control{Overlay: ov, API: c, State: *state, Author: defaultAuthor()}
		if h, ok := ctl.LoadHead(); ok && h.Server == server {
			// Resume: keep uncommitted overlay changes on their old base.
			vf.SetRoot(h.Root, time.Unix(h.Time, 0))
			ci.ID = h.Changeset
			ctl.Init(h)
		} else {
			ov.Rebase(ci.Root, time.Unix(ci.Time, 0))
			branch := ""
			if refs, err := c.Refs(bg()); err == nil {
				if _, ok := refs[*rev]; ok {
					branch = *rev
				}
			}
			ctl.Init(vfs.MountHead{Changeset: ci.ID, Root: ci.Root, Time: ci.Time, Branch: branch, Server: server})
		}
		cctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go func() {
			if err := ctl.Serve(cctx, sock); err != nil {
				log.Printf("mount control: %v", err)
			}
		}()
		served = ov
	}
	mode := "writable"
	if *rdonly {
		mode = "read-only"
	}
	be := *backend
	if be == "" {
		switch runtime.GOOS {
		case "linux":
			be = "fuse"
		case "windows":
			be = "webdav"
		default:
			be = "nfs"
		}
	}
	var unmount func() error
	switch be {
	case "fuse":
		if err := os.MkdirAll(mnt, 0o755); err != nil {
			return err
		}
		if unmount, err = vfs.MountFUSE(served, mnt); err != nil {
			return fmt.Errorf("fuse mount: %w", err)
		}
		fmt.Printf("serving %s (%s) %s over FUSE\nmounted at %s; Ctrl-C to unmount\n", *rev, ci.ID.Short(), mode, mnt)
	case "webdav", "nfs":
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			return err
		}
		port := ln.Addr().(*net.TCPAddr).Port
		var mountCmd, umountCmd []string
		if be == "webdav" {
			go http.Serve(ln, vfs.WebDAVHandler(served))
			url := fmt.Sprintf("http://127.0.0.1:%d/", port)
			if runtime.GOOS == "windows" {
				mountCmd = []string{"net", "use", mnt, url}
				umountCmd = []string{"net", "use", mnt, "/delete", "/y"}
			} else if runtime.GOOS == "darwin" {
				mountCmd = []string{"mount_webdav", url, mnt}
				umountCmd = []string{"umount", mnt}
			}
			fmt.Printf("serving %s (%s) %s over WebDAV at %s\n", *rev, ci.ID.Short(), mode, url)
		} else {
			handler := nfshelper.NewCachingHandler(nfshelper.NewNullAuthHandler(served), 1<<20)
			go func() {
				if err := nfs.Serve(ln, handler); err != nil {
					log.Printf("nfs: %v", err)
				}
			}()
			opts := fmt.Sprintf("port=%d,mountport=%d,vers=3,tcp", port, port)
			if *rdonly {
				opts += ",rdonly"
			}
			if runtime.GOOS == "darwin" {
				// The served revision can change under the kernel (checkout,
				// land), so attribute and negative-lookup caching are off.
				mountCmd = []string{"mount", "-t", "nfs", "-o", opts + ",nolocks,noresvport,locallocks,noac,nonegnamecache", "localhost:/", mnt}
			} else {
				mountCmd = []string{"mount", "-t", "nfs", "-o", opts + ",nolock,noac,lookupcache=none", "localhost:/", mnt}
			}
			umountCmd = []string{"umount", mnt}
			fmt.Printf("serving %s (%s) %s over NFS on 127.0.0.1:%d\n", *rev, ci.ID.Short(), mode, port)
		}
		switch {
		case *noMount || mountCmd == nil:
			fmt.Printf("mount with: %s\n", strings.Join(mountCmd, " "))
		default:
			if runtime.GOOS != "windows" {
				if err := os.MkdirAll(mnt, 0o755); err != nil {
					return err
				}
			}
			out, err := exec.Command(mountCmd[0], mountCmd[1:]...).CombinedOutput()
			if err != nil {
				return fmt.Errorf("%s: %v: %s", strings.Join(mountCmd, " "), err, strings.TrimSpace(string(out)))
			}
			unmount = func() error {
				if err := exec.Command(umountCmd[0], umountCmd[1:]...).Run(); err != nil && runtime.GOOS != "windows" {
					return exec.Command("umount", "-f", mnt).Run()
				}
				return nil
			}
			fmt.Printf("mounted at %s; Ctrl-C to unmount\n", mnt)
		}
	default:
		return fmt.Errorf("unknown backend %q (nfs, fuse, webdav)", be)
	}
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, stopSignals...)
	<-stop
	if unmount != nil {
		if err := unmount(); err != nil {
			log.Printf("unmount: %v", err)
		}
	}
	t, b, n := vf.Stats()
	fmt.Printf("fetched lazily: %d trees, %d blobs, %.1f MB\n", t, b, float64(n)/1e6)
	return nil
}
