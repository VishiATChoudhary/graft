package api

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"

	"github.com/VishiATChoudhary/graft/internal/acl"
	"github.com/VishiATChoudhary/graft/internal/object"
	"github.com/VishiATChoudhary/graft/internal/repo"
)

// Access control. With no policy configured everything is allowed.
//
// Reads through path-based APIs are checked against the path; listings that
// span paths (diffs, changeset file lists, search) are filtered. Writes are
// checked per path when changesets are created, and every ref update or
// land is checked against the full difference between the old and new
// tips, so content cannot reach a ref through a merge either.
//
// Object fetches are content-addressed, so the server indexes the tree
// object of every access-controlled directory as changesets are created
// (derived data "acl": tree ID -> directory). Fetching such a tree requires
// read access to its directory; blobs beneath can only be named through
// it. These trees are flagged private so shared cache tiers never cache
// them and always forward the caller's credentials.

var errForbidden = errors.New("permission denied")

type principalKey struct{}

func (s *Server) principal(ctx context.Context) *acl.Principal {
	if p, ok := ctx.Value(principalKey{}).(*acl.Principal); ok {
		return p
	}
	return acl.Anonymous
}

// authenticate attaches the caller's principal to the request context.
func (s *Server) authenticate(r *http.Request) *http.Request {
	if s.ACL == nil {
		return r
	}
	tok := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	return r.WithContext(context.WithValue(r.Context(), principalKey{}, s.ACL.Authenticate(tok)))
}

func (s *Server) canRead(ctx context.Context, path string) bool {
	return s.ACL == nil || s.ACL.CanRead(s.principal(ctx), path)
}

func (s *Server) canWrite(ctx context.Context, path string) bool {
	return s.ACL == nil || s.ACL.CanWrite(s.principal(ctx), path)
}

func (s *Server) requireRead(ctx context.Context, path string) error {
	if !s.canRead(ctx, path) {
		if path == "" {
			return fmt.Errorf("%w: read access to the repository required", errForbidden)
		}
		return fmt.Errorf("%w: no read access to %s", errForbidden, path)
	}
	return nil
}

func (s *Server) requireWrite(ctx context.Context, paths ...string) error {
	for _, p := range paths {
		if !s.canWrite(ctx, p) {
			return fmt.Errorf("%w: no write access to %s", errForbidden, p)
		}
	}
	return nil
}

func (s *Server) requireAdmin(ctx context.Context) error {
	if s.ACL != nil && s.ACL.RepoRole(s.principal(ctx)) != acl.RoleAdmin {
		return fmt.Errorf("%w: admin required", errForbidden)
	}
	return nil
}

// checkRefMove verifies the caller may write every path that differs
// between the old and new tips of a ref.
func (s *Server) checkRefMove(ctx context.Context) func(old, new object.ID) error {
	return func(old, new object.ID) error {
		if s.ACL == nil {
			return nil
		}
		if err := s.requireWrite(ctx, ""); err != nil {
			return err
		}
		var oldRoot, newRoot object.ID
		var err error
		if !old.IsZero() {
			if oldRoot, err = s.Repo.RootTree(old); err != nil {
				return err
			}
		}
		if !new.IsZero() {
			if newRoot, err = s.Repo.RootTree(new); err != nil {
				return err
			}
		}
		changes, err := s.Repo.DiffTrees(oldRoot, newRoot, nil)
		if err != nil {
			return err
		}
		for _, c := range changes {
			if err := s.requireWrite(ctx, c.Path); err != nil {
				return err
			}
		}
		return nil
	}
}

// restrictedIndex caches the "acl" derived marks (tree ID -> directory).
type restrictedIndex struct {
	mu    sync.RWMutex
	known map[object.ID]string
	clean map[object.ID]bool // checked, not restricted (marks only ever get added for new trees)
}

func (s *Server) restrictedDir(id object.ID) (string, bool) {
	ix := &s.restricted
	ix.mu.RLock()
	dir, ok := ix.known[id]
	clean := ix.clean[id]
	ix.mu.RUnlock()
	if ok || clean {
		return dir, ok
	}
	v, err := s.Repo.DB.GetDerived("acl", id)
	ix.mu.Lock()
	defer ix.mu.Unlock()
	if ix.known == nil {
		ix.known, ix.clean = map[object.ID]string{}, map[object.ID]bool{}
	}
	if err == nil {
		ix.known[id] = string(v)
		return string(v), true
	}
	if len(ix.clean) > 1<<20 {
		ix.clean = map[object.ID]bool{}
	}
	ix.clean[id] = true
	return "", false
}

// indexRestricted marks the trees of access-controlled directories in a
// changeset's root.
func (s *Server) indexRestricted(cs object.ID) {
	if s.ACL == nil {
		return
	}
	root, err := s.Repo.RootTree(cs)
	if err != nil || root.IsZero() {
		return
	}
	for _, dir := range s.ACL.ReadRestricted() {
		e, err := s.Repo.Lookup(root, dir)
		if err != nil || e.Mode != object.ModeDir {
			continue
		}
		if _, ok := s.restrictedDir(e.ID); ok {
			continue
		}
		if s.Repo.DB.PutDerived("acl", e.ID, []byte(dir)) == nil {
			s.restricted.mu.Lock()
			if s.restricted.known == nil {
				s.restricted.known, s.restricted.clean = map[object.ID]string{}, map[object.ID]bool{}
			}
			s.restricted.known[e.ID] = dir
			delete(s.restricted.clean, e.ID)
			s.restricted.mu.Unlock()
		}
	}
}

// EnableACL installs a policy: new changesets are indexed as they are
// created, and existing history is indexed now.
func (s *Server) EnableACL(p *acl.Policy) error {
	s.ACL = p
	s.Repo.OnCreate = s.indexRestricted
	n, err := s.Repo.DB.LastLogSeq()
	if err != nil {
		return err
	}
	for seq := uint64(1); seq <= n; seq++ {
		id, err := s.Repo.DB.LogID(seq)
		if err != nil {
			return err
		}
		s.indexRestricted(id)
	}
	return nil
}

// objectGuard authorizes object fetches.
func (s *Server) objectGuard(ctx context.Context, id object.ID, stored []byte) (bool, bool) {
	if s.ACL == nil {
		return true, false
	}
	if !s.canRead(ctx, "") {
		return false, false
	}
	if len(stored) == 0 || object.Kind(stored[0]) != object.KindTree {
		return true, false
	}
	dir, restricted := s.restrictedDir(id)
	if !restricted {
		return true, false
	}
	return s.canRead(ctx, dir), true
}

func (s *Server) filterChanges(ctx context.Context, cs []object.Change) []object.Change {
	if s.ACL == nil {
		return cs
	}
	out := cs[:0:0]
	for _, c := range cs {
		if s.canRead(ctx, c.Path) {
			out = append(out, c)
		}
	}
	return out
}

func (s *Server) filterFileChanges(ctx context.Context, cs []repo.FileChange) []repo.FileChange {
	if s.ACL == nil {
		return cs
	}
	out := cs[:0:0]
	for _, c := range cs {
		if s.canRead(ctx, c.Path) {
			out = append(out, c)
		}
	}
	return out
}
