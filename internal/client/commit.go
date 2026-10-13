package client

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/VishiATChoudhary/graft/internal/api"
	"github.com/VishiATChoudhary/graft/internal/object"
	"github.com/VishiATChoudhary/graft/internal/repo"
	"github.com/VishiATChoudhary/graft/internal/store"
)

var ErrNothingToCommit = errors.New("nothing to commit")

// Commit records local modifications as a new changeset on the server.
// Only new blobs and the list of changed paths are sent; the server builds
// trees and the changeset. There is nothing to push afterwards.
// If paths is non-empty only those paths are committed.
func (w *Worktree) Commit(ctx context.Context, message string, paths []string) (object.ID, []FileStatus, error) {
	tok := w.watchToken()
	st, err := w.Status(ctx)
	if err != nil {
		return object.Zero, nil, err
	}
	if len(paths) > 0 {
		want := map[string]bool{}
		for _, p := range paths {
			want[p] = true
		}
		var sel []FileStatus
		for _, s := range st {
			if want[s.Path] {
				sel = append(sel, s)
			}
		}
		st = sel
	}
	if len(st) == 0 {
		return object.Zero, nil, ErrNothingToCommit
	}
	mem := store.NewMemLRU(1 << 40)
	var ids []object.ID
	req := &api.CreateRequest{Author: w.Config.Author, Time: time.Now().Unix(), Message: message}
	if !w.Head.Changeset.IsZero() {
		req.Parents = []object.ID{w.Head.Changeset}
	}
	_, tzOff := time.Now().Zone()
	req.TZ = int32(tzOff / 60)
	for i, s := range st {
		if s.State == 'D' {
			req.Changes = append(req.Changes, object.Change{Path: s.Path})
			continue
		}
		content, err := readContent(w.abs(s.Path), s.Mode)
		if err != nil {
			return object.Zero, nil, err
		}
		id, stored := object.Encode(object.KindBlob, content)
		st[i].Blob = id // the file may have changed since Status
		mem.Put(id, stored)
		ids = append(ids, id)
		req.Changes = append(req.Changes, object.Change{Path: s.Path, Mode: s.Mode, Blob: id})
	}
	if len(ids) > 0 {
		missing, err := w.API.Missing(ctx, ids)
		if err != nil {
			return object.Zero, nil, err
		}
		if err := w.API.PutObjects(ctx, mem, missing); err != nil {
			return object.Zero, nil, err
		}
	}
	resp, err := w.API.Create(ctx, req)
	if err != nil {
		return object.Zero, nil, err
	}
	idx, err := w.LoadIndex()
	if err != nil {
		return object.Zero, nil, err
	}
	for _, s := range st {
		if s.State == 'D' {
			delete(idx, s.Path)
			pruneEmpty(filepath.Dir(w.abs(s.Path)), w.Root)
			continue
		}
		info, err := os.Lstat(w.abs(s.Path))
		if err != nil {
			return object.Zero, nil, err
		}
		idx[s.Path] = &IndexEntry{Mode: s.Mode, Size: info.Size(), Mtime: info.ModTime().UnixNano(), Blob: s.Blob}
	}
	w.Head.Changeset, w.Head.Root = resp.ID, resp.Root
	if err := w.SaveIndex(idx); err != nil {
		return object.Zero, nil, err
	}
	if len(paths) == 0 {
		// Everything changed before tok is now committed and indexed.
		w.saveWatchToken(tok)
	}
	return resp.ID, st, w.SaveHead()
}

// Land lands the current changeset onto ref on the server (rebasing it
// server-side if ref has advanced), then moves the working copy to the
// landed changeset. Only files changed by others are updated locally.
func (w *Worktree) Land(ctx context.Context, ref string) (*repo.LandResult, error) {
	if w.Head.Changeset.IsZero() {
		return nil, fmt.Errorf("nothing to land")
	}
	res, err := w.API.Land(ctx, ref, w.Head.Changeset.String(), w.Config.Author)
	if err != nil {
		return nil, err
	}
	if res.New != w.Head.Changeset {
		if _, err := w.Checkout(ctx, res.New.String(), false); err != nil {
			return res, fmt.Errorf("landed as %s, but updating the working copy failed: %w", res.New.Short(), err)
		}
	}
	return res, nil
}
