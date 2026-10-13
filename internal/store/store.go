// Package store provides object storage backends.
//
// All objects are immutable and content-addressed, so every store here is a
// plain map from ID to stored bytes. Nothing needs invalidation, and caches
// never consult each other.
package store

import (
	"github.com/VishiATChoudhary/graft/internal/object"
)

// Objects is the minimal object storage interface shared by every tier.
type Objects interface {
	// Get returns the stored form of id, or object.ErrNotFound.
	Get(id object.ID) ([]byte, error)
	// Put stores the stored form of id. Callers guarantee that stored hashes
	// to id; stores may skip the write if the object exists.
	Put(id object.ID, stored []byte) error
	Has(id object.ID) (bool, error)
}

// ReadPayload fetches and unpacks an object of the expected kind.
func ReadPayload(s Objects, id object.ID, want object.Kind) ([]byte, error) {
	b, err := s.Get(id)
	if err != nil {
		return nil, err
	}
	k, p, err := object.Unpack(b)
	if err != nil {
		return nil, err
	}
	if k != want {
		return nil, object.ErrCorrupt
	}
	return p, nil
}

// WritePayload encodes and stores a payload, returning its ID.
func WritePayload(s Objects, k object.Kind, payload []byte) (object.ID, error) {
	id, stored := object.Encode(k, payload)
	return id, s.Put(id, stored)
}

func ReadTree(s Objects, id object.ID) (*object.Tree, error) {
	p, err := ReadPayload(s, id, object.KindTree)
	if err != nil {
		return nil, err
	}
	return object.DecodeTree(p)
}

func WriteTree(s Objects, t *object.Tree) (object.ID, error) {
	return WritePayload(s, object.KindTree, t.Encode())
}

func ReadChangeset(s Objects, id object.ID) (*object.Changeset, error) {
	p, err := ReadPayload(s, id, object.KindChangeset)
	if err != nil {
		return nil, err
	}
	return object.DecodeChangeset(p)
}
