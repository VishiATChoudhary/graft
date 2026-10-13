package store

import (
	"fmt"

	"github.com/VishiATChoudhary/graft/internal/object"
)

// Backend is the authoritative storage a repository runs on: the role
// Spanner and blob storage play for Scion. It is implemented by DB (a local
// pebble store) and by Sharded (a client for a fleet of storage nodes, so
// any number of stateless API servers can share one repository).
//
// Everything except refs and the log is immutable and content-addressed, so
// callers may cache it freely. Refs move only by compare-and-swap, which the
// backend performs atomically on a single row.
type Backend interface {
	Objects

	// PutBase stores an object, letting the backend store it as a delta
	// against base (a previous version of the same path) if that is smaller.
	PutBase(id object.ID, stored []byte, base object.ID) error
	// Deltify re-stores an existing object as a delta against base when
	// beneficial. It is a storage hint and never changes object contents.
	Deltify(id, base object.ID) error
	// Payload returns an object's kind and uncompressed payload.
	Payload(id object.ID) (object.Kind, []byte, error)
	// PayloadSize returns the uncompressed size without reading content
	// where possible.
	PayloadSize(id object.ID) (uint64, error)

	GetDerived(kind string, id object.ID) ([]byte, error)
	PutDerived(kind string, id object.ID, v []byte) error

	GetRef(name string) (object.ID, error)
	// CASRef sets name to new if it currently equals old (Zero = absent;
	// new == Zero deletes). On mismatch it returns *RefMismatch.
	CASRef(name string, old, new object.ID) error
	ListRefs() (map[string]object.ID, error)

	// AppendLog assigns the next log sequence to a changeset. It is
	// idempotent: appending a logged changeset returns its sequence.
	AppendLog(id object.ID) (uint64, error)
	LogSeq(id object.ID) (uint64, error)
	LogID(seq uint64) (object.ID, error)
	LastLogSeq() (uint64, error)
	ScanLogIdxPrefix(prefix []byte, fn func(object.ID) bool) error

	Stats() map[string]any
	Flush() error
	Close() error
}

// RefMismatch reports a failed compare-and-swap.
type RefMismatch struct {
	Name     string
	Current  object.ID
	Expected object.ID
}

func (e *RefMismatch) Error() string {
	return fmt.Sprintf("ref %s is %s, expected %s", e.Name, e.Current.Short(), e.Expected.Short())
}

// payloadReader is implemented by stores that can return payloads without
// re-encoding (DB, Sharded).
type payloadReader interface {
	Payload(id object.ID) (object.Kind, []byte, error)
}
