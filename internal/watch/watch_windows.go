package watch

import (
	"context"
	"os"
	"path/filepath"
	"unsafe"

	"golang.org/x/sys/windows"
)

// start watches root with ReadDirectoryChangesW over the whole subtree.
func start(ctx context.Context, root string, j *Journal) (func(), error) {
	p, err := windows.UTF16PtrFromString(root)
	if err != nil {
		return nil, err
	}
	h, err := windows.CreateFile(p, windows.FILE_LIST_DIRECTORY,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE, nil,
		windows.OPEN_EXISTING, windows.FILE_FLAG_BACKUP_SEMANTICS, 0)
	if err != nil {
		return nil, err
	}
	const filter = windows.FILE_NOTIFY_CHANGE_FILE_NAME | windows.FILE_NOTIFY_CHANGE_DIR_NAME |
		windows.FILE_NOTIFY_CHANGE_ATTRIBUTES | windows.FILE_NOTIFY_CHANGE_SIZE | windows.FILE_NOTIFY_CHANGE_LAST_WRITE
	done := make(chan struct{})
	go func() {
		buf := make([]byte, 1<<16)
		for {
			var n uint32
			err := windows.ReadDirectoryChanges(h, &buf[0], uint32(len(buf)), true, filter, &n, nil, 0)
			select {
			case <-done:
				return
			default:
			}
			if err != nil {
				return
			}
			if n == 0 { // buffer overflowed: changes were lost
				j.Overflow()
				continue
			}
			for off := uint32(0); ; {
				info := (*windows.FileNotifyInformation)(unsafe.Pointer(&buf[off]))
				name := windows.UTF16ToString(unsafe.Slice(&info.FileName, info.FileNameLength/2))
				rel := filepath.ToSlash(name)
				switch info.Action {
				case windows.FILE_ACTION_ADDED, windows.FILE_ACTION_RENAMED_NEW_NAME:
					st, err := os.Lstat(filepath.Join(root, name))
					j.Record(rel, err == nil && st.IsDir())
				case windows.FILE_ACTION_REMOVED, windows.FILE_ACTION_RENAMED_OLD_NAME:
					// It may have been a directory: rescan whatever was under it.
					j.Record(rel, false)
					j.Record(rel, true)
				default:
					j.Record(rel, false)
				}
				if info.NextEntryOffset == 0 {
					break
				}
				off += info.NextEntryOffset
			}
		}
	}()
	return func() {
		close(done)
		windows.CancelIoEx(h, nil)
		windows.CloseHandle(h)
	}, nil
}
