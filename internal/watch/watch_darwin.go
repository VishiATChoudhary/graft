package watch

import (
	"context"
	"path/filepath"
	"strings"
	"time"

	"github.com/fsnotify/fsevents"
)

// start watches root with FSEvents (file-level events, no kernel
// extension, one stream for the whole tree).
func start(ctx context.Context, root string, j *Journal) (func(), error) {
	es := &fsevents.EventStream{
		Paths:   []string{root},
		Latency: 5 * time.Millisecond,
		Flags:   fsevents.FileEvents | fsevents.NoDefer | fsevents.WatchRoot,
	}
	if err := es.Start(); err != nil {
		return nil, err
	}
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case batch, ok := <-es.Events:
				if !ok {
					return
				}
				for _, ev := range batch {
					if ev.Flags&(fsevents.MustScanSubDirs|fsevents.KernelDropped|fsevents.UserDropped|fsevents.RootChanged) != 0 {
						j.Overflow()
						continue
					}
					p := ev.Path
					if !strings.HasPrefix(p, "/") {
						p = "/" + p
					}
					rel, err := filepath.Rel(root, p)
					if err != nil || strings.HasPrefix(rel, "..") {
						continue
					}
					if rel == "." {
						rel = ""
					}
					dir := ev.Flags&fsevents.ItemIsDir != 0 && ev.Flags&(fsevents.ItemCreated|fsevents.ItemRemoved|fsevents.ItemRenamed) != 0
					j.Record(rel, dir)
				}
			}
		}
	}()
	return es.Stop, nil
}
