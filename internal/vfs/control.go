package vfs

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/VishiATChoudhary/graft/internal/api"
	"github.com/VishiATChoudhary/graft/internal/object"
	"github.com/VishiATChoudhary/graft/internal/store"
)

// MountMeta is published at .gft/mount.json inside a writable mount so that
// gft commands run there can find the mount's control socket.
type MountMeta struct {
	Control string `json:"control"` // unix socket path
	Server  string `json:"server"`
}

// MountHead is the revision a mount is based on.
type MountHead struct {
	Changeset object.ID `json:"changeset"`
	Root      object.ID `json:"root"`
	Time      int64     `json:"time"`
	Branch    string    `json:"branch,omitempty"`
	Server    string    `json:"server"`
}

// FileState is one change reported by a mount.
type FileState struct {
	Path  string `json:"path"`
	State string `json:"state"` // A, M, D
}

// Control serves gft commands for a writable mount. Commit uploads only
// the overlay; checkout and land swap the base revision in place.
type Control struct {
	Overlay *Overlay
	API     *api.Client
	State   string
	Author  string

	mu   sync.Mutex
	head MountHead
}

func (c *Control) headPath() string { return filepath.Join(c.State, "head.json") }

// LoadHead restores the mount's revision, if saved.
func (c *Control) LoadHead() (MountHead, bool) {
	b, err := os.ReadFile(c.headPath())
	if err != nil {
		return MountHead{}, false
	}
	var h MountHead
	return h, json.Unmarshal(b, &h) == nil
}

func (c *Control) setHead(h MountHead) error {
	c.head = h
	b, _ := json.MarshalIndent(h, "", "  ")
	tmp := c.headPath() + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, c.headPath())
}

// Init sets the starting revision.
func (c *Control) Init(h MountHead) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.setHead(h)
}

func (c *Control) status() ([]FileState, error) {
	ch, err := c.Overlay.Changes()
	if err != nil {
		return nil, err
	}
	out := make([]FileState, 0, len(ch))
	for _, x := range ch {
		st := "M"
		if x.Deleted {
			st = "D"
		} else if _, ok := c.Overlay.baseEntry(x.Path); !ok {
			st = "A"
		}
		out = append(out, FileState{x.Path, st})
	}
	return out, nil
}

func (c *Control) commit(ctx context.Context, message string) (object.ID, int, error) {
	ch, err := c.Overlay.Changes()
	if err != nil {
		return object.Zero, 0, err
	}
	if len(ch) == 0 {
		return object.Zero, 0, errors.New("nothing to commit")
	}
	mem := store.NewMemLRU(1 << 40)
	req := &api.CreateRequest{Author: c.Author, Time: time.Now().Unix(), Message: message}
	if !c.head.Changeset.IsZero() {
		req.Parents = []object.ID{c.head.Changeset}
	}
	var ids []object.ID
	for _, x := range ch {
		if x.Deleted {
			req.Changes = append(req.Changes, object.Change{Path: x.Path})
			continue
		}
		id, stored := object.Encode(object.KindBlob, x.Content)
		mem.Put(id, stored)
		ids = append(ids, id)
		req.Changes = append(req.Changes, object.Change{Path: x.Path, Mode: x.Mode, Blob: id})
	}
	missing, err := c.API.Missing(ctx, ids)
	if err != nil {
		return object.Zero, 0, err
	}
	if err := c.API.PutObjects(ctx, mem, missing); err != nil {
		return object.Zero, 0, err
	}
	resp, err := c.API.Create(ctx, req)
	if err != nil {
		return object.Zero, 0, err
	}
	if err := c.Overlay.Rebase(resp.Root, time.Now()); err != nil {
		return object.Zero, 0, err
	}
	h := c.head
	h.Changeset, h.Root, h.Time = resp.ID, resp.Root, req.Time
	return resp.ID, len(ch), c.setHead(h)
}

// moveTo swaps the base revision; the overlay must be clean.
func (c *Control) moveTo(ctx context.Context, rev, branch string) (*api.ChangesetInfo, error) {
	if !c.Overlay.Clean() {
		return nil, errors.New("the mount has uncommitted changes; commit them first")
	}
	ci, err := c.API.Resolve(ctx, rev)
	if err != nil {
		return nil, err
	}
	if err := c.Overlay.Rebase(ci.Root, time.Unix(ci.Time, 0)); err != nil {
		return nil, err
	}
	h := c.head
	h.Changeset, h.Root, h.Time = ci.ID, ci.Root, ci.Time
	if branch != "" {
		h.Branch = branch
	}
	return ci, c.setHead(h)
}

func writeJSON(w http.ResponseWriter, v any, err error) {
	w.Header().Set("Content-Type", "application/json")
	if err != nil {
		w.WriteHeader(http.StatusConflict)
		var ae *api.APIError
		body := map[string]any{"error": err.Error()}
		if errors.As(err, &ae) && len(ae.Conflicts) > 0 {
			body["conflicts"] = ae.Conflicts
		}
		json.NewEncoder(w).Encode(body)
		return
	}
	json.NewEncoder(w).Encode(v)
}

// Handler serves the control API.
func (c *Control) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/head", func(w http.ResponseWriter, r *http.Request) {
		c.mu.Lock()
		h := c.head
		c.mu.Unlock()
		writeJSON(w, h, nil)
	})
	mux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		c.mu.Lock()
		defer c.mu.Unlock()
		st, err := c.status()
		writeJSON(w, st, err)
	})
	mux.HandleFunc("/commit", func(w http.ResponseWriter, r *http.Request) {
		c.mu.Lock()
		defer c.mu.Unlock()
		var req struct{ Message string }
		json.NewDecoder(r.Body).Decode(&req)
		id, n, err := c.commit(r.Context(), req.Message)
		writeJSON(w, map[string]any{"id": id, "paths": n}, err)
	})
	mux.HandleFunc("/checkout", func(w http.ResponseWriter, r *http.Request) {
		c.mu.Lock()
		defer c.mu.Unlock()
		var req struct{ Rev, Branch string }
		json.NewDecoder(r.Body).Decode(&req)
		ci, err := c.moveTo(r.Context(), req.Rev, req.Branch)
		writeJSON(w, ci, err)
	})
	mux.HandleFunc("/land", func(w http.ResponseWriter, r *http.Request) {
		c.mu.Lock()
		defer c.mu.Unlock()
		var req struct{ Ref string }
		json.NewDecoder(r.Body).Decode(&req)
		if req.Ref == "" {
			req.Ref = c.head.Branch
		}
		if req.Ref == "" {
			req.Ref = "main"
		}
		res, err := c.API.Land(r.Context(), req.Ref, c.head.Changeset.String(), c.Author)
		if err == nil && res.New != c.head.Changeset {
			_, err = c.moveTo(r.Context(), res.New.String(), req.Ref)
		}
		writeJSON(w, res, err)
	})
	return mux
}

// Serve listens on the control socket until ctx is done.
func (c *Control) Serve(ctx context.Context, sock string) error {
	os.Remove(sock)
	ln, err := net.Listen("unix", sock)
	if err != nil {
		return err
	}
	srv := &http.Server{Handler: c.Handler()}
	go func() {
		<-ctx.Done()
		srv.Close()
		os.Remove(sock)
	}()
	if err := srv.Serve(ln); !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

// MountClient talks to a mount's control socket.
type MountClient struct {
	Meta MountMeta
	http *http.Client
}

// FindMount returns a client if dir is inside a writable graft mount.
func FindMount(dir string) (*MountClient, string) {
	dir, _ = filepath.Abs(dir)
	for {
		b, err := os.ReadFile(filepath.Join(dir, ".gft", "mount.json"))
		if err == nil {
			var m MountMeta
			if json.Unmarshal(b, &m) != nil {
				return nil, ""
			}
			tr := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				var d net.Dialer
				return d.DialContext(ctx, "unix", m.Control)
			}}
			return &MountClient{Meta: m, http: &http.Client{Transport: tr, Timeout: 10 * time.Minute}}, dir
		}
		if _, err := os.Stat(filepath.Join(dir, ".gft")); err == nil {
			return nil, ""
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return nil, ""
		}
		dir = parent
	}
}

// Call performs a control request and decodes the JSON result into out.
func (m *MountClient) Call(path string, body any, out any) error {
	var resp *http.Response
	var err error
	if body == nil {
		resp, err = m.http.Get("http://mount" + path)
	} else {
		b, _ := json.Marshal(body)
		resp, err = m.http.Post("http://mount"+path, "application/json", bytesReader(b))
	}
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		var e struct {
			Error     string
			Conflicts []string
		}
		json.NewDecoder(resp.Body).Decode(&e)
		if len(e.Conflicts) > 0 {
			return fmt.Errorf("%s: %v", e.Error, e.Conflicts)
		}
		return errors.New(e.Error)
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

func bytesReader(b []byte) *bytes.Reader { return bytes.NewReader(b) }
