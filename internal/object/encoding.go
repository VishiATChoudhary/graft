package object

import (
	"encoding/binary"
	"fmt"
	"sort"
	"strings"
)

// Encoder is a tiny append-only binary writer using uvarints for lengths.
type Encoder struct{ B []byte }

func (e *Encoder) Uvarint(v uint64) { e.B = binary.AppendUvarint(e.B, v) }
func (e *Encoder) Varint(v int64)   { e.B = binary.AppendVarint(e.B, v) }
func (e *Encoder) Byte(b byte)      { e.B = append(e.B, b) }
func (e *Encoder) ID(id ID)         { e.B = append(e.B, id[:]...) }
func (e *Encoder) Bytes(b []byte)   { e.Uvarint(uint64(len(b))); e.B = append(e.B, b...) }
func (e *Encoder) String(s string)  { e.Uvarint(uint64(len(s))); e.B = append(e.B, s...) }

// Decoder reads what Encoder writes. The first error sticks.
type Decoder struct {
	B   []byte
	Err error
}

func (d *Decoder) fail(what string) {
	if d.Err == nil {
		d.Err = fmt.Errorf("%w: truncated %s", ErrCorrupt, what)
	}
}

func (d *Decoder) Uvarint() uint64 {
	if d.Err != nil {
		return 0
	}
	v, n := binary.Uvarint(d.B)
	if n <= 0 {
		d.fail("uvarint")
		return 0
	}
	d.B = d.B[n:]
	return v
}

func (d *Decoder) Varint() int64 {
	if d.Err != nil {
		return 0
	}
	v, n := binary.Varint(d.B)
	if n <= 0 {
		d.fail("varint")
		return 0
	}
	d.B = d.B[n:]
	return v
}

func (d *Decoder) Byte() byte {
	if d.Err != nil || len(d.B) < 1 {
		d.fail("byte")
		return 0
	}
	b := d.B[0]
	d.B = d.B[1:]
	return b
}

func (d *Decoder) ID() ID {
	var id ID
	if d.Err != nil || len(d.B) < 32 {
		d.fail("id")
		return id
	}
	copy(id[:], d.B)
	d.B = d.B[32:]
	return id
}

func (d *Decoder) Bytes() []byte {
	n := d.Uvarint()
	if d.Err != nil || uint64(len(d.B)) < n {
		d.fail("bytes")
		return nil
	}
	b := d.B[:n:n]
	d.B = d.B[n:]
	return b
}

func (d *Decoder) String() string { return string(d.Bytes()) }

// TreeEntry is one name in a directory.
type TreeEntry struct {
	Name string
	Mode Mode
	ID   ID
}

// Tree is a directory listing sorted by name.
type Tree struct {
	Entries []TreeEntry
}

func (t *Tree) Sort() {
	sort.Slice(t.Entries, func(i, j int) bool { return t.Entries[i].Name < t.Entries[j].Name })
}

// Find returns the index of name, or -1.
func (t *Tree) Find(name string) int {
	i := sort.Search(len(t.Entries), func(i int) bool { return t.Entries[i].Name >= name })
	if i < len(t.Entries) && t.Entries[i].Name == name {
		return i
	}
	return -1
}

func (t *Tree) Encode() []byte {
	var e Encoder
	e.Uvarint(uint64(len(t.Entries)))
	for _, en := range t.Entries {
		e.String(en.Name)
		e.Byte(byte(en.Mode))
		e.ID(en.ID)
	}
	return e.B
}

func DecodeTree(b []byte) (*Tree, error) {
	d := Decoder{B: b}
	n := d.Uvarint()
	if d.Err == nil && n > uint64(len(b)) {
		return nil, fmt.Errorf("%w: tree entry count", ErrCorrupt)
	}
	t := &Tree{Entries: make([]TreeEntry, 0, n)}
	for i := uint64(0); i < n && d.Err == nil; i++ {
		var en TreeEntry
		en.Name = d.String()
		en.Mode = Mode(d.Byte())
		en.ID = d.ID()
		t.Entries = append(t.Entries, en)
	}
	return t, d.Err
}

// Change is one file-level modification in a changeset, relative to the
// changeset's first parent. Mode == ModeNone means the path was deleted.
type Change struct {
	Path string `json:"path"`
	Mode Mode   `json:"mode"`
	Blob ID     `json:"blob,omitempty"`
}

// Changeset is the unit of work: an entry in the write-ahead log.
type Changeset struct {
	Parents []ID     `json:"parents"`
	Author  string   `json:"author"`
	Time    int64    `json:"time"` // unix seconds
	TZ      int32    `json:"tz"`   // minutes east of UTC
	Message string   `json:"message"`
	Changes []Change `json:"changes"` // sorted by path, unique
	// Origin optionally records an external identity (e.g. the git commit
	// this changeset was imported from).
	Origin string `json:"origin,omitempty"`
}

const changesetVersion = 1

func (c *Changeset) Encode() []byte {
	var e Encoder
	e.Byte(changesetVersion)
	e.Uvarint(uint64(len(c.Parents)))
	for _, p := range c.Parents {
		e.ID(p)
	}
	e.String(c.Author)
	e.Varint(c.Time)
	e.Varint(int64(c.TZ))
	e.String(c.Message)
	e.String(c.Origin)
	e.Uvarint(uint64(len(c.Changes)))
	for _, ch := range c.Changes {
		e.String(ch.Path)
		e.Byte(byte(ch.Mode))
		if ch.Mode != ModeNone {
			e.ID(ch.Blob)
		}
	}
	return e.B
}

func DecodeChangeset(b []byte) (*Changeset, error) {
	d := Decoder{B: b}
	if v := d.Byte(); d.Err == nil && v != changesetVersion {
		return nil, fmt.Errorf("%w: changeset version %d", ErrCorrupt, v)
	}
	c := &Changeset{}
	np := d.Uvarint()
	if np > 1<<16 {
		return nil, fmt.Errorf("%w: parent count", ErrCorrupt)
	}
	for i := uint64(0); i < np && d.Err == nil; i++ {
		c.Parents = append(c.Parents, d.ID())
	}
	c.Author = d.String()
	c.Time = d.Varint()
	c.TZ = int32(d.Varint())
	c.Message = d.String()
	c.Origin = d.String()
	nc := d.Uvarint()
	if d.Err == nil && nc > uint64(len(b)) {
		return nil, fmt.Errorf("%w: change count", ErrCorrupt)
	}
	c.Changes = make([]Change, 0, nc)
	for i := uint64(0); i < nc && d.Err == nil; i++ {
		var ch Change
		ch.Path = d.String()
		ch.Mode = Mode(d.Byte())
		if ch.Mode != ModeNone {
			ch.Blob = d.ID()
		}
		c.Changes = append(c.Changes, ch)
	}
	return c, d.Err
}

// Normalize sorts changes by path and validates them. Later duplicates win.
func (c *Changeset) Normalize() error {
	seen := make(map[string]int, len(c.Changes))
	out := c.Changes[:0]
	for _, ch := range c.Changes {
		p, err := CleanPath(ch.Path)
		if err != nil {
			return err
		}
		ch.Path = p
		if ch.Mode == ModeDir || ch.Mode > ModeGitlink {
			return fmt.Errorf("invalid mode %v for %q", ch.Mode, p)
		}
		if i, ok := seen[p]; ok {
			out[i] = ch
			continue
		}
		seen[p] = len(out)
		out = append(out, ch)
	}
	c.Changes = out
	sort.Slice(c.Changes, func(i, j int) bool { return c.Changes[i].Path < c.Changes[j].Path })
	return nil
}

// CleanPath validates a repository-relative slash path.
func CleanPath(p string) (string, error) {
	p = strings.Trim(p, "/")
	if p == "" {
		return "", fmt.Errorf("empty path")
	}
	for _, seg := range strings.Split(p, "/") {
		if seg == "" || seg == "." || seg == ".." || seg == ".gft" {
			return "", fmt.Errorf("invalid path %q", p)
		}
		if strings.ContainsRune(seg, 0) {
			return "", fmt.Errorf("invalid path %q", p)
		}
	}
	return p, nil
}
