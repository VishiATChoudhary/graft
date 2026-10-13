package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"runtime"
	"strings"
	"syscall"
	"time"

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
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return err
	}
	port := ln.Addr().(*net.TCPAddr).Port
	handler := nfshelper.NewCachingHandler(nfshelper.NewNullAuthHandler(vf), 1<<20)
	go func() {
		if err := nfs.Serve(ln, handler); err != nil {
			log.Printf("nfs: %v", err)
		}
	}()
	opts := fmt.Sprintf("port=%d,mountport=%d,vers=3,tcp,rdonly", port, port)
	var mountCmd []string
	if runtime.GOOS == "darwin" {
		mountCmd = []string{"mount", "-t", "nfs", "-o", opts + ",nolocks,noresvport,locallocks", "localhost:/", mnt}
	} else {
		mountCmd = []string{"mount", "-t", "nfs", "-o", opts + ",nolock", "localhost:/", mnt}
	}
	fmt.Printf("serving %s (%s) read-only over NFS on 127.0.0.1:%d\n", *rev, ci.ID.Short(), port)
	mounted := false
	if *noMount {
		fmt.Printf("mount with: %s\n", strings.Join(mountCmd, " "))
	} else {
		if err := os.MkdirAll(mnt, 0o755); err != nil {
			return err
		}
		out, err := exec.Command(mountCmd[0], mountCmd[1:]...).CombinedOutput()
		if err != nil {
			return fmt.Errorf("%s: %v: %s (on Linux this needs root; try --no-mount and sudo)", strings.Join(mountCmd, " "), err, strings.TrimSpace(string(out)))
		}
		mounted = true
		fmt.Printf("mounted at %s; Ctrl-C to unmount\n", mnt)
	}
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop
	if mounted {
		if out, err := exec.Command("umount", mnt).CombinedOutput(); err != nil {
			exec.Command("umount", "-f", mnt).Run()
			_ = out
		}
	}
	t, b, n := vf.Stats()
	fmt.Printf("fetched lazily: %d trees, %d blobs, %.1f MB\n", t, b, float64(n)/1e6)
	return nil
}
