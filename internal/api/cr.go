package api

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/VishiATChoudhary/graft/internal/object"
	"github.com/VishiATChoudhary/graft/internal/repo"
	"github.com/VishiATChoudhary/graft/internal/store"
)

// Change requests and the merge queue (the role Antique plays for Scion).
//
// A change request proposes landing a changeset on a target ref. Its record
// (title, revisions, comments, state) is a JSON object in the store; the
// ref meta/cr/<id> points at the newest version and moves by
// compare-and-swap, so concurrent comments from any API server never lose
// each other and every backend (pebble, a fleet, Spanner) works the same.
// Approvals stay per changeset (see review.go): pushing a new revision
// dismisses them. Queuing a change request lands it as soon as its review
// rules are satisfied, through the ordinary coalesced, lock-free land.

const crRefPrefix = "meta/cr/"

//go:embed reviewui.html
var reviewUI []byte

// ChangeRequest is a change request's record.
type ChangeRequest struct {
	ID          string     `json:"id"`
	Title       string     `json:"title"`
	Description string     `json:"description,omitempty"`
	Author      string     `json:"author"`
	Target      string     `json:"target"`
	Head        object.ID  `json:"head"`
	Revisions   []Revision `json:"revisions"`
	Comments    []Comment  `json:"comments,omitempty"`
	// State is open, queued, landed or closed.
	State   string    `json:"state"`
	Queuer  string    `json:"queuer,omitempty"`
	Waiting string    `json:"waiting,omitempty"` // why a queued request has not landed yet
	Landed  object.ID `json:"landed,omitempty"`
	Created int64     `json:"created"`
	Updated int64     `json:"updated"`
}

// Revision is one pushed version of a change request.
type Revision struct {
	Head   object.ID `json:"head"`
	Author string    `json:"author"`
	Time   int64     `json:"time"`
}

// Comment is a review comment, optionally anchored to a line of a file in
// a revision.
type Comment struct {
	N        int       `json:"n"`
	Author   string    `json:"author"`
	Time     int64     `json:"time"`
	Revision object.ID `json:"revision"`
	Path     string    `json:"path,omitempty"`
	Line     int       `json:"line,omitempty"`
	Text     string    `json:"text"`
}

var errNoCR = fmt.Errorf("%w: no such change request", repo.ErrBadRev)

func (s *Server) actor(ctx context.Context) string {
	if p := s.principal(ctx); p != nil && p.Name != "" {
		return p.Name
	}
	return "anonymous"
}

func (s *Server) loadCR(id string) (*ChangeRequest, object.ID, error) {
	ref, err := s.Repo.DB.GetRef(crRefPrefix + id)
	if errors.Is(err, object.ErrNotFound) {
		return nil, object.Zero, errNoCR
	}
	if err != nil {
		return nil, object.Zero, err
	}
	_, p, err := s.Repo.DB.Payload(ref)
	if err != nil {
		return nil, object.Zero, err
	}
	var cr ChangeRequest
	if err := json.Unmarshal(p, &cr); err != nil {
		return nil, object.Zero, err
	}
	return &cr, ref, nil
}

// updateCR applies fn to the newest record and publishes the result with a
// compare-and-swap, retrying if another server updated it meanwhile.
func (s *Server) updateCR(id string, create bool, fn func(cr *ChangeRequest) error) (*ChangeRequest, error) {
	for attempt := 0; attempt < 100; attempt++ {
		cr, old, err := s.loadCR(id)
		if errors.Is(err, errNoCR) && create {
			cr, err = &ChangeRequest{ID: id}, nil
		}
		if err != nil {
			return nil, err
		}
		if err := fn(cr); err != nil {
			return nil, err
		}
		cr.Updated = time.Now().Unix()
		b, _ := json.Marshal(cr)
		nid, err := store.WritePayload(s.Repo.DB, object.KindBlob, b)
		if err != nil {
			return nil, err
		}
		err = s.Repo.DB.CASRef(crRefPrefix+id, old, nid)
		var rm *store.RefMismatch
		if errors.As(err, &rm) {
			if create && old.IsZero() {
				return nil, fmt.Errorf("%w: change request %s exists", errBadRequest, id)
			}
			continue
		}
		return cr, err
	}
	return nil, fmt.Errorf("change request %s: too much contention", id)
}

func (s *Server) crCreate(r *http.Request) (any, error) {
	var req struct{ Head, Target, Title, Description string }
	if err := decodeJSON(r, &req); err != nil {
		return nil, err
	}
	head, err := s.Repo.Resolve(req.Head)
	if err != nil {
		return nil, err
	}
	if req.Target == "" {
		req.Target = "main"
	}
	if req.Title == "" {
		cs, err := s.Repo.Changeset(head)
		if err != nil {
			return nil, err
		}
		req.Title, _, _ = strings.Cut(cs.Message, "\n")
	}
	who := s.actor(r.Context())
	now := time.Now().Unix()
	return s.updateCR(head.String()[:12], true, func(cr *ChangeRequest) error {
		*cr = ChangeRequest{ID: cr.ID, Title: req.Title, Description: req.Description, Author: who, Target: req.Target,
			Head: head, Revisions: []Revision{{head, who, now}}, State: "open", Created: now}
		return nil
	})
}

func (s *Server) crGet(r *http.Request) (any, error) {
	cr, _, err := s.loadCR(r.URL.Query().Get("id"))
	return cr, err
}

func (s *Server) crList(r *http.Request) (any, error) {
	refs, err := s.Repo.Refs()
	if err != nil {
		return nil, err
	}
	state := r.URL.Query().Get("state")
	out := []*ChangeRequest{}
	for name := range refs {
		id, ok := strings.CutPrefix(name, crRefPrefix)
		if !ok {
			continue
		}
		cr, _, err := s.loadCR(id)
		if err != nil {
			continue
		}
		if state == "" || cr.State == state {
			cr.Comments = nil // summaries only
			out = append(out, cr)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Updated > out[j].Updated })
	return out, nil
}

func (s *Server) crRevise(r *http.Request) (any, error) {
	var req struct{ ID, Head string }
	if err := decodeJSON(r, &req); err != nil {
		return nil, err
	}
	head, err := s.Repo.Resolve(req.Head)
	if err != nil {
		return nil, err
	}
	who := s.actor(r.Context())
	return s.updateCR(req.ID, false, func(cr *ChangeRequest) error {
		if cr.State == "landed" || cr.State == "closed" {
			return fmt.Errorf("%w: change request is %s", errBadRequest, cr.State)
		}
		if cr.Head != head {
			cr.Head = head
			cr.Revisions = append(cr.Revisions, Revision{head, who, time.Now().Unix()})
		}
		if cr.State == "queued" { // a new revision needs its own approvals
			cr.State, cr.Waiting = "open", ""
		}
		return nil
	})
}

func (s *Server) crComment(r *http.Request) (any, error) {
	var req struct {
		ID, Path, Text string
		Line           int
	}
	if err := decodeJSON(r, &req); err != nil {
		return nil, err
	}
	if strings.TrimSpace(req.Text) == "" {
		return nil, fmt.Errorf("%w: empty comment", errBadRequest)
	}
	if req.Path != "" {
		if err := s.requireRead(r.Context(), req.Path); err != nil {
			return nil, err
		}
	}
	who := s.actor(r.Context())
	return s.updateCR(req.ID, false, func(cr *ChangeRequest) error {
		cr.Comments = append(cr.Comments, Comment{N: len(cr.Comments) + 1, Author: who, Time: time.Now().Unix(),
			Revision: cr.Head, Path: req.Path, Line: req.Line, Text: req.Text})
		return nil
	})
}

func (s *Server) crClose(r *http.Request) (any, error) {
	var req struct{ ID string }
	if err := decodeJSON(r, &req); err != nil {
		return nil, err
	}
	return s.updateCR(req.ID, false, func(cr *ChangeRequest) error {
		if cr.State == "landed" {
			return fmt.Errorf("%w: already landed", errBadRequest)
		}
		cr.State, cr.Waiting = "closed", ""
		return nil
	})
}

// crQueue puts a change request in the merge queue. The caller's write
// permission is checked now, against the change request's own changes;
// the land itself happens as soon as review rules are satisfied.
func (s *Server) crQueue(r *http.Request) (any, error) {
	var req struct{ ID string }
	if err := decodeJSON(r, &req); err != nil {
		return nil, err
	}
	cr, _, err := s.loadCR(req.ID)
	if err != nil {
		return nil, err
	}
	tip, _ := s.Repo.Ref(cr.Target)
	base := object.Zero
	if !tip.IsZero() {
		if base, err = s.Repo.MergeBase(cr.Head, tip); err != nil {
			return nil, err
		}
	}
	if err := s.checkRefMove(r.Context())(base, cr.Head); err != nil {
		return nil, err
	}
	who := s.actor(r.Context())
	if _, err := s.updateCR(req.ID, false, func(cr *ChangeRequest) error {
		if cr.State != "open" && cr.State != "queued" {
			return fmt.Errorf("%w: change request is %s", errBadRequest, cr.State)
		}
		cr.State, cr.Queuer = "queued", who
		return nil
	}); err != nil {
		return nil, err
	}
	s.processQueue()
	cr, _, err = s.loadCR(req.ID)
	return cr, err
}

var queueMu sync.Mutex

// processQueue tries to land every queued change request whose review
// rules are satisfied. It runs after queuing and approvals, and
// periodically (StartMergeQueue) so approvals given on other servers are
// picked up. Landing is the ordinary land: lock-free and coalesced, so
// queued requests do not wait for each other.
func (s *Server) processQueue() {
	queueMu.Lock()
	defer queueMu.Unlock()
	refs, err := s.Repo.Refs()
	if err != nil {
		return
	}
	var wg sync.WaitGroup
	for name := range refs {
		id, ok := strings.CutPrefix(name, crRefPrefix)
		if !ok {
			continue
		}
		cr, _, err := s.loadCR(id)
		if err != nil || cr.State != "queued" {
			continue
		}
		wg.Add(1)
		go func(cr *ChangeRequest) {
			defer wg.Done()
			s.tryLand(cr)
		}(cr)
	}
	wg.Wait()
}

func (s *Server) tryLand(cr *ChangeRequest) {
	tip, _ := s.Repo.Ref(cr.Target)
	var landed object.ID
	err := s.requireReviews(cr.Head, tip)
	if err == nil {
		var res *repo.LandResult
		if res, err = s.Repo.Land(cr.Target, cr.Head, cr.Queuer, time.Now().Unix(), 0); err == nil {
			landed = res.New
		}
	}
	var ce *repo.ConflictError
	conflict := errors.As(err, &ce)
	s.updateCR(cr.ID, false, func(c *ChangeRequest) error {
		if c.State != "queued" || c.Head != cr.Head {
			return nil // changed meanwhile; the next pass sees the new state
		}
		switch {
		case err == nil:
			c.State, c.Landed, c.Waiting = "landed", landed, ""
		case conflict:
			c.State, c.Waiting = "open", err.Error() // needs a new revision
		default:
			c.Waiting = err.Error() // e.g. approvals still missing
		}
		return nil
	})
}

// StartMergeQueue retries queued change requests every interval until ctx
// is done.
func (s *Server) StartMergeQueue(ctx context.Context, interval time.Duration) {
	go func() {
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				s.processQueue()
			}
		}
	}()
}
