package client

import (
	"bufio"
	"errors"
	"io"
	"io/fs"
	"os"
	"sort"

	"github.com/VishiATChoudhary/graft/internal/object"
)

// IndexEntry records what the working copy last wrote (or committed) at a
// path, so unchanged files are recognised with a single lstat.
type IndexEntry struct {
	Mode  object.Mode
	Size  int64
	Mtime int64 // unix nanoseconds
	Blob  object.ID
}

type Index map[string]*IndexEntry

const indexVersion = 1

func (w *Worktree) LoadIndex() (Index, error) {
	b, err := os.ReadFile(meta(w.Root, "index"))
	if errors.Is(err, fs.ErrNotExist) {
		return Index{}, nil
	}
	if err != nil {
		return nil, err
	}
	d := object.Decoder{B: b}
	if d.Byte() != indexVersion {
		return nil, errors.New("unsupported index version; re-run gft checkout")
	}
	n := d.Uvarint()
	idx := make(Index, n)
	for i := uint64(0); i < n && d.Err == nil; i++ {
		p := d.String()
		e := &IndexEntry{Mode: object.Mode(d.Byte()), Size: int64(d.Uvarint()), Mtime: d.Varint(), Blob: d.ID()}
		idx[p] = e
	}
	return idx, d.Err
}

func (w *Worktree) SaveIndex(idx Index) error {
	paths := make([]string, 0, len(idx))
	for p := range idx {
		paths = append(paths, p)
	}
	sort.Strings(paths)
	tmp := meta(w.Root, "index.tmp")
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	bw := bufio.NewWriterSize(f, 1<<20)
	var e object.Encoder
	e.Byte(indexVersion)
	e.Uvarint(uint64(len(paths)))
	for _, p := range paths {
		en := idx[p]
		e.String(p)
		e.Byte(byte(en.Mode))
		e.Uvarint(uint64(en.Size))
		e.Varint(en.Mtime)
		e.ID(en.Blob)
		if len(e.B) > 1<<20 {
			bw.Write(e.B)
			e.B = e.B[:0]
		}
	}
	bw.Write(e.B)
	if err := bw.Flush(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, meta(w.Root, "index"))
}

var _ io.Writer = (*bufio.Writer)(nil)
