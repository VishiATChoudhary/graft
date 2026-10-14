package repo

import (
	"path"
	"sort"

	"github.com/VishiATChoudhary/graft/internal/object"
)

// compactBudget bounds the candidate bytes tried per blob.
const compactBudget = 8 << 20

// CompactStats summarises a compaction.
type CompactStats struct {
	Blobs, Full, Improved int
	Before, After         int64
}

type storageInspector interface {
	StorageInfo(id object.ID) (codec byte, size int, err error)
	Redictionary(kind object.Kind, ids []object.ID) (int, error)
	Compact() error
}

// Compact optionally re-encodes blobs as deltas against similar blobs
// anywhere in history, git-style: blobs are ordered by file name, extension
// and size, and each tries the window previous ones as bases, keeping the
// smallest encoding (whole blobs may become deltas; deltas may find better
// bases). graft never needs this (storage is
// decided at write time and nothing degrades); it trades a one-off pass for
// a few percent of disk. Objects stay readable throughout, so it can run
// against a live single-node store.
func (r *Repo) Compact(window int, progress func(done, total int)) (*CompactStats, error) {
	if window < 0 { // dictionaries only
		window = 0
	}
	si, ok := r.DB.(storageInspector)
	if !ok {
		return nil, ErrBadChange
	}
	type entry struct {
		id   object.ID
		name string
		ext  string
		size uint64
	}
	seen := map[object.ID]bool{}
	var all []entry
	var changesets []object.ID
	n, err := r.DB.LastLogSeq()
	if err != nil {
		return nil, err
	}
	for seq := uint64(1); seq <= n; seq++ {
		id, err := r.DB.LogID(seq)
		if err != nil {
			return nil, err
		}
		cs, err := r.Changeset(id)
		if err != nil {
			return nil, err
		}
		changesets = append(changesets, id)
		for _, ch := range cs.Changes {
			if (ch.Mode != object.ModeFile && ch.Mode != object.ModeExec) || seen[ch.Blob] {
				continue
			}
			seen[ch.Blob] = true
			size, _ := r.blobSize(ch.Blob)
			all = append(all, entry{ch.Blob, path.Base(ch.Path), path.Ext(ch.Path), size})
		}
	}
	sort.SliceStable(all, func(i, j int) bool {
		if all[i].name != all[j].name {
			return all[i].name < all[j].name
		}
		if all[i].ext != all[j].ext {
			return all[i].ext < all[j].ext
		}
		return all[i].size > all[j].size
	})
	st := &CompactStats{Blobs: len(all)}
	for _, e := range all {
		_, n, _ := si.StorageInfo(e.id)
		st.Before += int64(n)
	}
	for i, e := range all {
		if progress != nil && i%1000 == 0 {
			progress(i, len(all))
		}
		codec, before, err := si.StorageInfo(e.id)
		if err != nil {
			continue
		}
		if codec != 3 {
			st.Full++
		}
		// Bound the work per blob: large files try fewer candidates.
		budget := uint64(compactBudget)
		for j := i - 1; j >= 0 && j >= i-window; j-- {
			if all[j].size*2 < e.size || e.size*2 < all[j].size {
				continue
			}
			if budget < all[j].size {
				break
			}
			budget -= all[j].size
			_ = r.DB.Deltify(e.id, all[j].id)
		}
		if _, after, err := si.StorageInfo(e.id); err == nil && after < before {
			st.Improved++
		}
	}
	// Retrain the dictionaries on current content and re-encode whole
	// objects with them where that is smaller.
	ids := make([]object.ID, 0, len(all))
	for i := len(all) - 1; i >= 0; i-- {
		ids = append(ids, all[i].id)
	}
	if _, err := si.Redictionary(object.KindBlob, ids); err != nil {
		return nil, err
	}
	if _, err := si.Redictionary(object.KindChangeset, changesets); err != nil {
		return nil, err
	}
	for _, e := range all {
		_, n, _ := si.StorageInfo(e.id)
		st.After += int64(n)
	}
	return st, si.Compact()
}
