// Package api exposes every source-control operation the client uses as an
// HTTP/JSON API, so CI jobs and tools can ask questions (history, blame,
// ancestry, diffs) and even write changes without a local repository.
package api

import (
	"bufio"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/VishiATChoudhary/graft/internal/acl"
	"github.com/VishiATChoudhary/graft/internal/diff"
	"github.com/VishiATChoudhary/graft/internal/fetch"
	"github.com/VishiATChoudhary/graft/internal/metrics"
	"github.com/VishiATChoudhary/graft/internal/object"
	"github.com/VishiATChoudhary/graft/internal/repo"
	"github.com/VishiATChoudhary/graft/internal/store"
)

// Server serves the authoritative tier.
type Server struct {
	Repo *repo.Repo
	Log  *log.Logger
	// ACL, if set (via EnableACL), enforces repository and directory
	// permissions.
	ACL        *acl.Policy
	restricted restrictedIndex

	// Peers lists every API server (including this one, at index Self)
	// for ref affinity of lands; see affinity.go. Optional.
	Peers []string
	Self  int
}

// ChangesetInfo is the JSON form of a changeset with derived context.
type ChangesetInfo struct {
	ID      object.ID       `json:"id"`
	Root    object.ID       `json:"root"`
	Gen     uint64          `json:"gen"`
	Depth   uint64          `json:"depth"`
	Parents []object.ID     `json:"parents"`
	Author  string          `json:"author"`
	Time    int64           `json:"time"`
	TZ      int32           `json:"tz"`
	Message string          `json:"message"`
	Origin  string          `json:"origin,omitempty"`
	Changes []object.Change `json:"changes,omitempty"`
}

type CreateRequest struct {
	Parents []object.ID     `json:"parents"`
	Author  string          `json:"author"`
	Time    int64           `json:"time"`
	TZ      int32           `json:"tz"`
	Message string          `json:"message"`
	Changes []object.Change `json:"changes"`
}

type CreateResponse struct {
	ID   object.ID `json:"id"`
	Root object.ID `json:"root"`
}

type RefUpdate struct {
	Name string    `json:"name"`
	Old  object.ID `json:"old"`
	New  object.ID `json:"new"`
}

type WriteFile struct {
	Path    string      `json:"path"`
	Content []byte      `json:"content,omitempty"` // base64 in JSON
	Mode    object.Mode `json:"mode,omitempty"`    // default file; 0 with Delete
	Delete  bool        `json:"delete,omitempty"`
}

// WriteRequest creates a changeset from file contents without any checkout,
// and optionally lands it.
type WriteRequest struct {
	Parent  string      `json:"parent"` // revision; empty for a root changeset
	Author  string      `json:"author"`
	Message string      `json:"message"`
	Files   []WriteFile `json:"files"`
	Land    string      `json:"land,omitempty"` // ref to land onto
}

type WriteResponse struct {
	ID   object.ID        `json:"id"`
	Land *repo.LandResult `json:"land,omitempty"`
}

type BlameEntry struct {
	Line      int       `json:"line"`
	Changeset object.ID `json:"changeset"`
	OrigLine  int       `json:"orig_line"`
	Author    string    `json:"author"`
	Time      int64     `json:"time"`
	Text      string    `json:"text"`
}

type ConflictBody struct {
	Error     string   `json:"error"`
	Conflicts []string `json:"conflicts,omitempty"`
}

// Routes registers the full API on mux.
func (s *Server) Routes(mux *http.ServeMux) {
	objects := fetch.Handler(fetch.StoreSource{Store: s.Repo.DB, Guard: s.objectGuard})
	mux.HandleFunc("/v1/objects", s.raw(objects, ""))
	mux.HandleFunc("/v1/objects/missing", s.raw(s.missing, "write"))
	mux.HandleFunc("/v1/objects/put", s.raw(s.put, "write"))
	mux.HandleFunc("/v1/info", s.wrap(s.info))
	mux.HandleFunc("/v1/refs", s.wrap(s.refs))
	mux.HandleFunc("/v1/refs/update", s.wrap(s.refUpdate))
	mux.HandleFunc("/v1/resolve", s.wrap(s.resolve))
	mux.HandleFunc("/v1/changeset", s.wrap(s.changeset))
	mux.HandleFunc("/v1/changesets", s.wrap(s.create))
	mux.HandleFunc("/v1/write", s.wrap(s.write))
	mux.HandleFunc("/v1/log", s.wrap(s.log))
	mux.HandleFunc("/v1/blame", s.wrap(s.blame))
	mux.HandleFunc("/v1/diff", s.raw(s.diff, "read"))
	mux.HandleFunc("/v1/mergebase", s.wrap(s.mergebase))
	mux.HandleFunc("/v1/isancestor", s.wrap(s.isancestor))
	mux.HandleFunc("/v1/rebase", s.wrap(s.rebase))
	mux.HandleFunc("/v1/merge", s.wrap(s.merge))
	mux.HandleFunc("/v1/land", s.wrap(s.land))
	mux.HandleFunc("/v1/ls", s.wrap(s.ls))
	mux.HandleFunc("/v1/cat", s.raw(s.cat, "read"))
	mux.HandleFunc("/v1/search", s.wrap(s.search))
	mux.HandleFunc("/v1/grep", s.wrap(s.grep))
	mux.HandleFunc("/v1/reviews/approve", s.wrap(s.approveHandler))
	mux.HandleFunc("/v1/reviews/status", s.wrap(s.reviewStatusHandler))
}

func (s *Server) approveHandler(r *http.Request) (any, error) {
	var req struct{ ID string }
	if err := decodeJSON(r, &req); err != nil {
		return nil, err
	}
	return s.approve(r.Context(), req.ID)
}

func (s *Server) reviewStatusHandler(r *http.Request) (any, error) {
	id, err := s.rev(r, "id", "")
	if err != nil {
		return nil, err
	}
	ref := r.URL.Query().Get("ref")
	if ref == "" {
		ref = "main"
	}
	tip, _ := s.Repo.Ref(ref)
	return s.reviewStatus(id, tip)
}

// GrepResult is the response of /v1/grep.
type GrepResult struct {
	Matches   []repo.GrepMatch `json:"matches"`
	Truncated bool             `json:"truncated"`
}

func (s *Server) grep(r *http.Request) (any, error) {
	id, err := s.rev(r, "rev", "main")
	if err != nil {
		return nil, err
	}
	root, err := s.Repo.RootTree(id)
	if err != nil {
		return nil, err
	}
	q := r.URL.Query()
	if q.Get("q") == "" {
		return nil, badf("missing q")
	}
	if err := s.requireRead(r.Context(), q.Get("path")); err != nil {
		return nil, err
	}
	opt := repo.GrepOptions{Pattern: q.Get("q"), Fixed: q.Get("fixed") == "1", IgnoreCase: q.Get("i") == "1",
		Prefix: q.Get("path"), Limit: intParam(r, "limit", 1000)}
	if s.ACL != nil {
		ctx := r.Context()
		opt.CanRead = func(p string) bool { return s.canRead(ctx, p) }
	}
	m, trunc, err := s.Repo.Grep(root, opt)
	if err != nil {
		return nil, badf("%v", err)
	}
	return GrepResult{m, trunc}, nil
}

// raw wraps a non-JSON handler with authentication and a repository-level
// permission check ("" leaves checks to the handler).
func (s *Server) raw(h http.HandlerFunc, need string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		r = s.authenticate(r)
		var err error
		switch need {
		case "read":
			err = s.requireRead(r.Context(), "")
		case "write":
			err = s.requireWrite(r.Context(), "")
		}
		if err != nil {
			s.writeErr(w, err)
			return
		}
		h(w, r)
	}
}

type handler func(r *http.Request) (any, error)

func (s *Server) wrap(h handler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		r = s.authenticate(r)
		if err := s.requireRead(r.Context(), ""); err != nil {
			s.writeErr(w, err)
			return
		}
		v, err := h(r)
		if err != nil {
			s.writeErr(w, err)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		enc := json.NewEncoder(w)
		enc.Encode(v)
	}
}

func (s *Server) writeErr(w http.ResponseWriter, err error) {
	var fe *forwardedError
	if errors.As(err, &fe) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(fe.code)
		w.Write(fe.body)
		return
	}
	code := http.StatusInternalServerError
	body := ConflictBody{Error: err.Error()}
	var ce *repo.ConflictError
	switch {
	case errors.As(err, &ce):
		code, body.Conflicts = http.StatusConflict, ce.Paths
	case errors.Is(err, repo.ErrStaleRef):
		code = http.StatusPreconditionFailed
	case errors.Is(err, repo.ErrBadRev), errors.Is(err, object.ErrNotFound):
		code = http.StatusNotFound
	case errors.Is(err, repo.ErrBadChange), errors.Is(err, errBadRequest):
		code = http.StatusBadRequest
	case errors.Is(err, repo.ErrMissing):
		code = http.StatusUnprocessableEntity
	case errors.Is(err, errForbidden):
		code = http.StatusForbidden
	}
	if code == http.StatusInternalServerError && s.Log != nil {
		s.Log.Printf("error: %v", err)
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(body)
}

var errBadRequest = errors.New("bad request")

func badf(format string, a ...any) error {
	return fmt.Errorf("%w: %s", errBadRequest, fmt.Sprintf(format, a...))
}

func decodeJSON(r *http.Request, v any) error {
	if r.Method != http.MethodPost {
		return badf("POST required")
	}
	dec := json.NewDecoder(io.LimitReader(r.Body, 1<<30))
	if err := dec.Decode(v); err != nil {
		return badf("decode: %v", err)
	}
	return nil
}

func (s *Server) rev(r *http.Request, param, def string) (object.ID, error) {
	v := r.URL.Query().Get(param)
	if v == "" {
		v = def
	}
	if v == "" {
		return object.Zero, badf("missing %s", param)
	}
	return s.Repo.Resolve(v)
}

func intParam(r *http.Request, name string, def int) int {
	if v, err := strconv.Atoi(r.URL.Query().Get(name)); err == nil {
		return v
	}
	return def
}

func (s *Server) Info(id object.ID, withChanges bool) (*ChangesetInfo, error) {
	cs, err := s.Repo.Changeset(id)
	if err != nil {
		return nil, err
	}
	g, err := s.Repo.Graph(id)
	if err != nil {
		return nil, err
	}
	root, err := s.Repo.RootTree(id)
	if err != nil {
		return nil, err
	}
	ci := &ChangesetInfo{ID: id, Root: root, Gen: g.Gen, Depth: g.Depth, Parents: cs.Parents,
		Author: cs.Author, Time: cs.Time, TZ: cs.TZ, Message: cs.Message, Origin: cs.Origin}
	if withChanges {
		ci.Changes = cs.Changes
	}
	return ci, nil
}

func (s *Server) changesetInfo(ctx context.Context, id object.ID, withChanges bool) (*ChangesetInfo, error) {
	ci, err := s.Info(id, withChanges)
	if err == nil && withChanges {
		ci.Changes = s.filterChanges(ctx, ci.Changes)
	}
	return ci, err
}

func (s *Server) info(r *http.Request) (any, error) {
	refs, err := s.Repo.Refs()
	if err != nil {
		return nil, err
	}
	out := map[string]any{"tier": "server", "changesets": s.Repo.LogSize(), "refs": len(refs),
		"cpu_seconds": metrics.CPUSeconds(), "max_rss": metrics.MaxRSS(), "store": s.Repo.DB.Stats()}
	return out, nil
}

func (s *Server) refs(r *http.Request) (any, error) { return s.Repo.Refs() }

func (s *Server) refUpdate(r *http.Request) (any, error) {
	var u RefUpdate
	if err := decodeJSON(r, &u); err != nil {
		return nil, err
	}
	if u.New.IsZero() {
		if err := s.requireAdmin(r.Context()); err != nil {
			return nil, err
		}
	} else if err := s.checkRefMove(r.Context())(u.Old, u.New); err != nil {
		return nil, err
	} else if !strings.HasPrefix(u.Name, "reviews/") {
		if err := s.requireReviews(u.New, u.Old); err != nil {
			return nil, err
		}
	}
	if err := s.Repo.UpdateRef(u.Name, u.Old, u.New); err != nil {
		return nil, err
	}
	return u, nil
}

func (s *Server) resolve(r *http.Request) (any, error) {
	id, err := s.rev(r, "rev", "main")
	if err != nil {
		return nil, err
	}
	return s.Info(id, false)
}

func (s *Server) changeset(r *http.Request) (any, error) {
	id, err := s.rev(r, "id", "")
	if err != nil {
		return nil, err
	}
	return s.changesetInfo(r.Context(), id, true)
}

func (s *Server) create(r *http.Request) (any, error) {
	var req CreateRequest
	if err := decodeJSON(r, &req); err != nil {
		return nil, err
	}
	if req.Time == 0 {
		req.Time = time.Now().Unix()
	}
	cs := &object.Changeset{Parents: req.Parents, Author: req.Author, Time: req.Time, TZ: req.TZ, Message: req.Message, Changes: req.Changes}
	for _, c := range cs.Changes {
		if err := s.requireWrite(r.Context(), c.Path); err != nil {
			return nil, err
		}
	}
	id, err := s.Repo.Create(cs)
	if err != nil {
		return nil, err
	}
	s.recordCreator(r.Context(), id)
	root, err := s.Repo.RootTree(id)
	if err != nil {
		return nil, err
	}
	return CreateResponse{id, root}, nil
}

func (s *Server) write(r *http.Request) (any, error) {
	var req WriteRequest
	if err := decodeJSON(r, &req); err != nil {
		return nil, err
	}
	cs := &object.Changeset{Author: req.Author, Time: time.Now().Unix(), Message: req.Message}
	if req.Parent != "" {
		p, err := s.Repo.Resolve(req.Parent)
		if err != nil {
			return nil, err
		}
		cs.Parents = []object.ID{p}
	}
	for _, f := range req.Files {
		if err := s.requireWrite(r.Context(), f.Path); err != nil {
			return nil, err
		}
		if f.Delete {
			cs.Changes = append(cs.Changes, object.Change{Path: f.Path})
			continue
		}
		mode := f.Mode
		if mode == object.ModeNone {
			mode = object.ModeFile
		}
		id, err := store.WritePayload(s.Repo.DB, object.KindBlob, f.Content)
		if err != nil {
			return nil, err
		}
		cs.Changes = append(cs.Changes, object.Change{Path: f.Path, Mode: mode, Blob: id})
	}
	id, err := s.Repo.Create(cs)
	if err != nil {
		return nil, err
	}
	s.recordCreator(r.Context(), id)
	resp := &WriteResponse{ID: id}
	if req.Land != "" {
		if resp.Land, err = s.landRouted(r, req.Land, id, req.Author, cs.Time); err != nil {
			return nil, err
		}
	}
	return resp, nil
}

func (s *Server) log(r *http.Request) (any, error) {
	id, err := s.rev(r, "rev", "main")
	if err != nil {
		return nil, err
	}
	limit := intParam(r, "limit", 50)
	var ids []object.ID
	if p := r.URL.Query().Get("path"); p != "" {
		if err := s.requireRead(r.Context(), p); err != nil {
			return nil, err
		}
		ids, err = s.Repo.PathLog(id, p, limit, r.URL.Query().Get("follow") == "1")
	} else {
		ids, err = s.Repo.Log(id, limit, r.URL.Query().Get("first_parent") == "1")
	}
	if err != nil {
		return nil, err
	}
	out := make([]*ChangesetInfo, 0, len(ids))
	for _, x := range ids {
		ci, err := s.Info(x, false)
		if err != nil {
			return nil, err
		}
		out = append(out, ci)
	}
	return out, nil
}

func (s *Server) blame(r *http.Request) (any, error) {
	id, err := s.rev(r, "rev", "main")
	if err != nil {
		return nil, err
	}
	path := r.URL.Query().Get("path")
	if err := s.requireRead(r.Context(), path); err != nil {
		return nil, err
	}
	lines, err := s.Repo.Blame(id, path)
	if err != nil {
		return nil, err
	}
	root, err := s.Repo.RootTree(id)
	if err != nil {
		return nil, err
	}
	e, err := s.Repo.Lookup(root, path)
	if err != nil {
		return nil, err
	}
	content, err := s.Repo.ReadBlob(e.ID)
	if err != nil {
		return nil, err
	}
	text := diff.SplitLines(content)
	cache := map[object.ID]*object.Changeset{}
	out := make([]BlameEntry, len(lines))
	for i, l := range lines {
		cs := cache[l.Changeset]
		if cs == nil {
			if cs, err = s.Repo.Changeset(l.Changeset); err != nil {
				return nil, err
			}
			cache[l.Changeset] = cs
		}
		out[i] = BlameEntry{Line: i + 1, Changeset: l.Changeset, OrigLine: l.Line, Author: cs.Author, Time: cs.Time}
		if i < len(text) {
			out[i].Text = strings.TrimSuffix(string(text[i]), "\n")
		}
	}
	return out, nil
}

// diff returns file-level changes between two revisions (JSON) or a unified
// patch (patch=1). "from" defaults to the first parent of "to".
func (s *Server) diff(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	to, err := s.rev(r, "to", "main")
	if err != nil {
		s.writeErr(w, err)
		return
	}
	var from object.ID
	if q.Get("from") != "" {
		if from, err = s.rev(r, "from", ""); err != nil {
			s.writeErr(w, err)
			return
		}
	} else if q.Get("mergebase") != "" {
		other, err := s.Repo.Resolve(q.Get("mergebase"))
		if err == nil {
			from, err = s.Repo.MergeBase(to, other)
		}
		if err != nil {
			s.writeErr(w, err)
			return
		}
	} else {
		g, err := s.Repo.Graph(to)
		if err != nil {
			s.writeErr(w, err)
			return
		}
		if len(g.Parents) > 0 {
			from = g.Parents[0]
		}
	}
	var fromRoot object.ID
	if !from.IsZero() {
		if fromRoot, err = s.Repo.RootTree(from); err != nil {
			s.writeErr(w, err)
			return
		}
	}
	toRoot, err := s.Repo.RootTree(to)
	if err != nil {
		s.writeErr(w, err)
		return
	}
	changes, err := s.Repo.DiffTrees(fromRoot, toRoot, q["path"])
	if err != nil {
		s.writeErr(w, err)
		return
	}
	changes = s.filterFileChanges(r.Context(), changes)
	if q.Get("patch") != "1" {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"from": from, "to": to, "changes": changes})
		return
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	bw := bufio.NewWriter(w)
	defer bw.Flush()
	for _, c := range changes {
		var a, b []byte
		if c.OldMode != object.ModeNone {
			a, _ = s.Repo.ReadBlob(c.OldBlob)
		}
		if c.NewMode != object.ModeNone {
			b, _ = s.Repo.ReadBlob(c.NewBlob)
		}
		na, nb := "a/"+c.Path, "b/"+c.Path
		if c.OldMode == object.ModeNone {
			na = "/dev/null"
		}
		if c.NewMode == object.ModeNone {
			nb = "/dev/null"
		}
		fmt.Fprintf(bw, "diff --gft a/%s b/%s\n", c.Path, c.Path)
		diff.Unified(bw, na, nb, a, b, 3)
	}
}

func (s *Server) mergebase(r *http.Request) (any, error) {
	a, err := s.rev(r, "a", "")
	if err != nil {
		return nil, err
	}
	b, err := s.rev(r, "b", "")
	if err != nil {
		return nil, err
	}
	bases, err := s.Repo.MergeBases(a, b)
	if err != nil {
		return nil, err
	}
	return map[string]any{"bases": bases}, nil
}

func (s *Server) isancestor(r *http.Request) (any, error) {
	a, err := s.rev(r, "a", "")
	if err != nil {
		return nil, err
	}
	b, err := s.rev(r, "b", "")
	if err != nil {
		return nil, err
	}
	ok, err := s.Repo.IsAncestor(a, b)
	return map[string]bool{"ancestor": ok}, err
}

func (s *Server) rebase(r *http.Request) (any, error) {
	var req struct {
		ID   string `json:"id"`
		Onto string `json:"onto"`
	}
	if err := decodeJSON(r, &req); err != nil {
		return nil, err
	}
	id, err := s.Repo.Resolve(req.ID)
	if err != nil {
		return nil, err
	}
	onto, err := s.Repo.Resolve(req.Onto)
	if err != nil {
		return nil, err
	}
	n, err := s.Repo.Rebase(id, onto)
	if err != nil {
		return nil, err
	}
	return map[string]object.ID{"id": n}, nil
}

func (s *Server) merge(r *http.Request) (any, error) {
	var req struct {
		A, B, Author, Message string
	}
	if err := decodeJSON(r, &req); err != nil {
		return nil, err
	}
	a, err := s.Repo.Resolve(req.A)
	if err != nil {
		return nil, err
	}
	b, err := s.Repo.Resolve(req.B)
	if err != nil {
		return nil, err
	}
	if req.Message == "" {
		req.Message = "Merge " + req.B + " into " + req.A
	}
	n, err := s.Repo.Merge(a, b, req.Author, req.Message, time.Now().Unix())
	if err != nil {
		return nil, err
	}
	return map[string]object.ID{"id": n}, nil
}

func (s *Server) land(r *http.Request) (any, error) {
	var req struct {
		Ref, ID, Author string
	}
	if err := decodeJSON(r, &req); err != nil {
		return nil, err
	}
	if req.Ref == "" {
		req.Ref = "main"
	}
	id, err := s.Repo.Resolve(req.ID)
	if err != nil {
		return nil, err
	}
	return s.landRouted(r, req.Ref, id, req.Author, time.Now().Unix())
}

func (s *Server) ls(r *http.Request) (any, error) {
	id, err := s.rev(r, "rev", "main")
	if err != nil {
		return nil, err
	}
	root, err := s.Repo.RootTree(id)
	if err != nil {
		return nil, err
	}
	if err := s.requireRead(r.Context(), r.URL.Query().Get("path")); err != nil {
		return nil, err
	}
	return s.Repo.ListTree(root, r.URL.Query().Get("path"))
}

func (s *Server) search(r *http.Request) (any, error) {
	id, err := s.rev(r, "rev", "main")
	if err != nil {
		return nil, err
	}
	root, err := s.Repo.RootTree(id)
	if err != nil {
		return nil, err
	}
	q := r.URL.Query().Get("q")
	if q == "" {
		return nil, badf("missing q")
	}
	paths, err := s.Repo.FindPaths(root, r.URL.Query().Get("path"), q, intParam(r, "limit", 100))
	if err != nil || s.ACL == nil {
		return paths, err
	}
	out := paths[:0:0]
	for _, p := range paths {
		if s.canRead(r.Context(), p) {
			out = append(out, p)
		}
	}
	return out, nil
}

func (s *Server) cat(w http.ResponseWriter, r *http.Request) {
	id, err := s.rev(r, "rev", "main")
	if err != nil {
		s.writeErr(w, err)
		return
	}
	root, err := s.Repo.RootTree(id)
	if err != nil {
		s.writeErr(w, err)
		return
	}
	if err := s.requireRead(r.Context(), r.URL.Query().Get("path")); err != nil {
		s.writeErr(w, err)
		return
	}
	e, err := s.Repo.Lookup(root, r.URL.Query().Get("path"))
	if err != nil {
		s.writeErr(w, err)
		return
	}
	if e.Mode == object.ModeDir {
		s.writeErr(w, badf("is a directory"))
		return
	}
	b, err := s.Repo.ReadBlob(e.ID)
	if err != nil {
		s.writeErr(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Write(b)
}

// missing returns the subset of posted IDs the server does not have.
func (s *Server) missing(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(io.LimitReader(r.Body, fetch.MaxBatch*32+1))
	if err != nil || len(body)%32 != 0 || len(body) > fetch.MaxBatch*32 {
		s.writeErr(w, badf("body must be at most 65536 ids"))
		return
	}
	var out []byte
	for i := 0; i < len(body); i += 32 {
		var id object.ID
		copy(id[:], body[i:])
		ok, err := s.Repo.DB.Has(id)
		if err != nil {
			s.writeErr(w, err)
			return
		}
		if !ok {
			out = append(out, id[:]...)
		}
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Write(out)
}

// put accepts a stream of object frames. Every object is verified against
// its ID before it is stored.
func (s *Server) put(w http.ResponseWriter, r *http.Request) {
	br := bufio.NewReaderSize(r.Body, 256<<10)
	var hdr [36]byte
	n := 0
	for {
		if _, err := io.ReadFull(br, hdr[:]); err != nil {
			if err == io.EOF {
				break
			}
			s.writeErr(w, badf("frame: %v", err))
			return
		}
		var id object.ID
		copy(id[:], hdr[:32])
		size := binary.BigEndian.Uint32(hdr[32:])
		if size > 1<<31 {
			s.writeErr(w, badf("object too large"))
			return
		}
		b := make([]byte, size)
		if _, err := io.ReadFull(br, b); err != nil {
			s.writeErr(w, badf("frame body: %v", err))
			return
		}
		k, _, err := object.Verify(id, b)
		if err != nil {
			s.writeErr(w, badf("%v", err))
			return
		}
		if k != object.KindBlob {
			s.writeErr(w, badf("only blobs may be uploaded; trees and changesets are created server-side"))
			return
		}
		if err := s.Repo.DB.Put(id, b); err != nil {
			s.writeErr(w, err)
			return
		}
		n++
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]int{"stored": n})
}
