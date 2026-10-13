// Package object defines graft's content-addressed object model.
//
// There are three object kinds:
//
//   - Blob: raw file contents.
//   - Tree: a sorted directory listing pointing at blobs and subtrees.
//   - Changeset: an entry in the write-ahead log. Unlike a git commit it does
//     not point at a root tree; it records the files that changed relative to
//     its first parent. Root trees are derived data (see package derive).
//
// Every object is identified by the SHA-256 of its kind byte followed by its
// uncompressed payload. Objects are immutable, so any tier may cache them
// without coordination.
package object

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
)

// Kind tags the type of an object. It is the first byte hashed into an ID.
type Kind byte

const (
	KindBlob      Kind = 'b'
	KindTree      Kind = 't'
	KindChangeset Kind = 'c'
	// KindUnode and KindBlame are server-side derived-data objects. They are
	// content-addressed like everything else but never sent to clients.
	KindUnode Kind = 'u'
	KindBlame Kind = 'm'
)

// ID is a content address.
type ID [32]byte

// Zero is the zero ID, used to mean "absent".
var Zero ID

func (id ID) String() string { return hex.EncodeToString(id[:]) }

// Short returns an abbreviated hex form for display.
func (id ID) Short() string { return hex.EncodeToString(id[:6]) }

func (id ID) IsZero() bool { return id == Zero }

func (id ID) MarshalText() ([]byte, error) {
	if id.IsZero() {
		return []byte{}, nil
	}
	return []byte(id.String()), nil
}

func (id *ID) UnmarshalText(b []byte) error {
	if len(b) == 0 {
		*id = Zero
		return nil
	}
	p, err := ParseID(string(b))
	if err != nil {
		return err
	}
	*id = p
	return nil
}

// ParseID parses a full 64-character hex ID.
func ParseID(s string) (ID, error) {
	var id ID
	if len(s) != 64 {
		return id, fmt.Errorf("invalid id %q: want 64 hex chars", s)
	}
	if _, err := hex.Decode(id[:], []byte(s)); err != nil {
		return id, fmt.Errorf("invalid id %q: %w", s, err)
	}
	return id, nil
}

// Hash computes the ID of a payload of the given kind.
func Hash(k Kind, payload []byte) ID {
	h := sha256.New()
	h.Write([]byte{byte(k)})
	h.Write(payload)
	var id ID
	h.Sum(id[:0])
	return id
}

// Mode is the type of a tree entry or file change.
type Mode byte

const (
	ModeNone    Mode = 0 // used in a Change to mean "deleted"
	ModeFile    Mode = 1
	ModeExec    Mode = 2
	ModeSymlink Mode = 3
	ModeDir     Mode = 4
	ModeGitlink Mode = 5 // git submodule pointer, preserved on import
)

func (m Mode) String() string {
	switch m {
	case ModeNone:
		return "deleted"
	case ModeFile:
		return "file"
	case ModeExec:
		return "exec"
	case ModeSymlink:
		return "symlink"
	case ModeDir:
		return "dir"
	case ModeGitlink:
		return "gitlink"
	}
	return fmt.Sprintf("mode(%d)", m)
}

var ErrNotFound = errors.New("object not found")
var ErrCorrupt = errors.New("object corrupt")
