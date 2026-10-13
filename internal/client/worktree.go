// Package client implements graft's working copy.
//
// The client is deliberately thin. It keeps a bounded local object cache,
// lays out (sparse) working-copy files, detects local modifications, and
// talks to the server for everything else: history, blame, diffs, merges,
// rebases, and even creating commits. Committing uploads only new blobs and
// a list of changed paths; the server builds trees and the changeset.
package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/VishiATChoudhary/graft/internal/api"
	"github.com/VishiATChoudhary/graft/internal/fetch"
	"github.com/VishiATChoudhary/graft/internal/object"
	"github.com/VishiATChoudhary/graft/internal/store"
)

const metaDir = ".gft"

// Config is persisted in .gft/config.json.
type Config struct {
	// Server is the authoritative API endpoint.
	Server string `json:"server"`
	// Objects is where object fetches go: a node daemon socket
	// ("unix:///run/gft.sock"), a regional cache URL, or empty for Server.
	Objects string `json:"objects,omitempty"`
	// Sparse lists directory prefixes to materialize. Empty means all.
	Sparse []string `json:"sparse,omitempty"`
	// CacheBytes bounds the local object cache (used when no daemon).
	CacheBytes int64 `json:"cache_bytes,omitempty"`
	// Author identity for new changesets.
	Author string `json:"author,omitempty"`
	// Token authenticates to the server and tiers ($GFT_TOKEN overrides).
	Token string `json:"token,omitempty"`
}

// Head is persisted in .gft/HEAD.json.
type Head struct {
	Changeset object.ID `json:"changeset"`
	Root      object.ID `json:"root"`
	// Branch is the ref this working copy tracks, if any.
	Branch string `json:"branch,omitempty"`
}

// Worktree is an open working copy.
type Worktree struct {
	Root   string
	Config Config
	Head   Head
	API    *api.Client
	src    fetch.Source
	cache  *store.DiskCache
}

func meta(root string, name string) string { return filepath.Join(root, metaDir, name) }

// Find locates the worktree containing dir.
func Find(dir string) (string, error) {
	dir, err := filepath.Abs(dir)
	if err != nil {
		return "", err
	}
	for {
		if st, err := os.Stat(filepath.Join(dir, metaDir)); err == nil && st.IsDir() {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", errors.New("not a graft working copy (no .gft directory found)")
		}
		dir = parent
	}
}

// Init creates a new worktree directory configured for a server.
func Init(root string, cfg Config) (*Worktree, error) {
	if err := os.MkdirAll(filepath.Join(root, metaDir), 0o755); err != nil {
		return nil, err
	}
	if _, err := os.Stat(meta(root, "config.json")); err == nil {
		return nil, fmt.Errorf("%s is already a graft working copy", root)
	}
	if cfg.CacheBytes == 0 {
		cfg.CacheBytes = 4 << 30
	}
	if err := writeJSON(meta(root, "config.json"), cfg); err != nil {
		return nil, err
	}
	if err := writeJSON(meta(root, "HEAD.json"), Head{}); err != nil {
		return nil, err
	}
	return Open(root)
}

// Open opens an existing worktree rooted at root.
func Open(root string) (*Worktree, error) {
	w := &Worktree{Root: root}
	if err := readJSON(meta(root, "config.json"), &w.Config); err != nil {
		return nil, err
	}
	if err := readJSON(meta(root, "HEAD.json"), &w.Head); err != nil {
		return nil, err
	}
	token := w.Config.Token
	if t := os.Getenv("GFT_TOKEN"); t != "" {
		token = t
	}
	w.API = api.NewClientToken(w.Config.Server, token)
	switch {
	case strings.HasPrefix(w.Config.Objects, "unix://"):
		// The node daemon holds the shared cache; keep nothing locally.
		r := fetch.NewRemote(w.Config.Objects)
		r.Token = token
		w.src = r
	default:
		up := w.Config.Objects
		if up == "" {
			up = w.Config.Server
		}
		c, err := store.OpenDiskCache(meta(root, "objects"), w.Config.CacheBytes)
		if err != nil {
			return nil, err
		}
		w.cache = c
		r := fetch.NewRemote(up)
		r.Token = token
		w.src = fetch.NewTier(store.KindFilter{Objects: c, Kinds: []object.Kind{object.KindTree}}, r)
	}
	return w, nil
}

func (w *Worktree) SaveHead() error   { return writeJSON(meta(w.Root, "HEAD.json"), w.Head) }
func (w *Worktree) SaveConfig() error { return writeJSON(meta(w.Root, "config.json"), w.Config) }

// Source exposes the object source (for tests and tools).
func (w *Worktree) Source() fetch.Source { return w.src }

func writeJSON(path string, v any) error {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, append(b, '\n'), 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func readJSON(path string, v any) error {
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, v)
}

// inSparse reports whether a repo path is within the sparse spec.
func (w *Worktree) inSparse(p string) bool {
	if len(w.Config.Sparse) == 0 {
		return true
	}
	for _, s := range w.Config.Sparse {
		s = strings.Trim(s, "/")
		if s == "" || p == s || strings.HasPrefix(p, s+"/") {
			return true
		}
	}
	return false
}

// towardSparse reports whether a directory may contain sparse paths.
func (w *Worktree) towardSparse(dir string) bool {
	if dir == "" || w.inSparse(dir) {
		return true
	}
	for _, s := range w.Config.Sparse {
		if strings.HasPrefix(strings.Trim(s, "/"), dir+"/") {
			return true
		}
	}
	return false
}

func (w *Worktree) abs(p string) string { return filepath.Join(w.Root, filepath.FromSlash(p)) }

// CacheStats reports local cache counters (zero when using a daemon).
func (w *Worktree) CacheStats() (hits, misses uint64, bytes int64, count int) {
	if w.cache == nil {
		return
	}
	return w.cache.Stats()
}

// ctx is a convenience for CLI calls.
func ctx() context.Context { return context.Background() }
