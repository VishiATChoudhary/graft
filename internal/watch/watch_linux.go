package watch

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"unsafe"

	"golang.org/x/sys/unix"
)

// start watches root with inotify, one watch per directory.
func start(ctx context.Context, root string, j *Journal) (func(), error) {
	fd, err := unix.InotifyInit1(unix.IN_CLOEXEC)
	if err != nil {
		return nil, err
	}
	const mask = unix.IN_CREATE | unix.IN_DELETE | unix.IN_MODIFY | unix.IN_CLOSE_WRITE | unix.IN_MOVED_FROM |
		unix.IN_MOVED_TO | unix.IN_ATTRIB | unix.IN_DELETE_SELF
	var mu sync.Mutex
	dirs := map[int32]string{}
	var add func(dir string)
	add = func(dir string) {
		filepath.WalkDir(dir, func(p string, d os.DirEntry, err error) error {
			if err != nil || !d.IsDir() {
				return nil
			}
			if name := d.Name(); p != root && (name == ".git") {
				return filepath.SkipDir
			}
			wd, err := unix.InotifyAddWatch(fd, p, mask)
			if err == nil {
				mu.Lock()
				dirs[int32(wd)] = p
				mu.Unlock()
			}
			return nil
		})
	}
	add(root)
	done := make(chan struct{})
	go func() {
		buf := make([]byte, 1<<16)
		for {
			n, err := unix.Read(fd, buf)
			if err != nil || n <= 0 {
				select {
				case <-done:
					return
				default:
				}
				if err == unix.EINTR {
					continue
				}
				return
			}
			for off := 0; off+unix.SizeofInotifyEvent <= n; {
				ev := (*unix.InotifyEvent)(unsafe.Pointer(&buf[off]))
				nameLen := int(ev.Len)
				name := ""
				if nameLen > 0 {
					raw := buf[off+unix.SizeofInotifyEvent : off+unix.SizeofInotifyEvent+nameLen]
					for i, c := range raw {
						if c == 0 {
							raw = raw[:i]
							break
						}
					}
					name = string(raw)
				}
				off += unix.SizeofInotifyEvent + nameLen
				if ev.Mask&unix.IN_Q_OVERFLOW != 0 {
					j.Overflow()
					continue
				}
				mu.Lock()
				dir := dirs[ev.Wd]
				mu.Unlock()
				p := filepath.Join(dir, name)
				rel, err := filepath.Rel(root, p)
				if err != nil {
					continue
				}
				if rel == "." {
					rel = ""
				}
				isDir := ev.Mask&unix.IN_ISDIR != 0
				if isDir && ev.Mask&(unix.IN_CREATE|unix.IN_MOVED_TO) != 0 {
					add(p)
					// Entries created before the watch was added produced no
					// events: report what is already there.
					filepath.WalkDir(p, func(fp string, d os.DirEntry, err error) error {
						if err == nil && fp != p {
							if r, err := filepath.Rel(root, fp); err == nil {
								j.Record(r, d.IsDir())
							}
						}
						return nil
					})
				}
				j.Record(rel, isDir && ev.Mask&(unix.IN_CREATE|unix.IN_DELETE|unix.IN_MOVED_FROM|unix.IN_MOVED_TO) != 0)
			}
		}
	}()
	go func() {
		<-ctx.Done()
	}()
	return func() { close(done); unix.Close(fd) }, nil
}
