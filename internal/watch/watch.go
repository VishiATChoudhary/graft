// Package watch monitors a working copy for filesystem changes so status
// can check only what changed instead of stat-ing every file (the role
// git's fsmonitor plays).
//
// A watcher process keeps a journal of changed paths, each stamped with a
// sequence number, under a random epoch. Clients remember the token
// (epoch:seq) current when they last wrote the index and ask for paths
// changed since. If the watcher restarted (epoch differs) or overflowed,
// the answer is "full" and the client falls back to a full scan.
//
// Queries are synchronized with cookie files, like git's fsmonitor: the
// watcher creates a file under .gft/cookies and answers only once it has
// seen that file's own event, which proves every earlier change has been
// delivered.
package watch

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Journal records changed paths (relative, slash-separated). A path ending
// in "/" means the whole directory must be rescanned.
type Journal struct {
	mu      sync.Mutex
	epoch   string
	seq     uint64
	changed map[string]uint64
	full    uint64 // seq at which the journal last overflowed (0: never)
	cookies map[string]chan struct{}
}

func NewJournal() *Journal {
	var b [8]byte
	rand.Read(b[:])
	return &Journal{epoch: hex.EncodeToString(b[:]), changed: map[string]uint64{}, cookies: map[string]chan struct{}{}}
}

const maxJournal = 1 << 20

// Record notes changes to paths (dir: rescan the whole subtree).
func (j *Journal) Record(rel string, dir bool) {
	rel = filepath.ToSlash(rel)
	if strings.HasPrefix(rel, ".gft/cookies/") {
		j.mu.Lock()
		if ch, ok := j.cookies[strings.TrimPrefix(rel, ".gft/cookies/")]; ok {
			close(ch)
			delete(j.cookies, strings.TrimPrefix(rel, ".gft/cookies/"))
		}
		j.mu.Unlock()
		return
	}
	if rel == ".gft" || strings.HasPrefix(rel, ".gft/") || rel == ".git" || strings.HasPrefix(rel, ".git/") {
		return
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	j.seq++
	if dir && rel != "" {
		rel += "/"
	}
	if rel == "" || rel == "/" {
		j.full = j.seq
		return
	}
	j.changed[rel] = j.seq
	if len(j.changed) > maxJournal {
		j.changed = map[string]uint64{}
		j.full = j.seq
	}
}

// Overflow forces the next query to report a full rescan.
func (j *Journal) Overflow() {
	j.mu.Lock()
	j.seq++
	j.full = j.seq
	j.mu.Unlock()
}

// Token returns the current journal position.
func (j *Journal) Token() string {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.epoch + ":" + strconv.FormatUint(j.seq, 10)
}

// Changes is a journal query result.
type Changes struct {
	Token string   `json:"token"`
	Full  bool     `json:"full"`
	Paths []string `json:"paths,omitempty"`
}

// Since returns paths changed after token.
func (j *Journal) Since(token string) Changes {
	j.mu.Lock()
	defer j.mu.Unlock()
	out := Changes{Token: j.epoch + ":" + strconv.FormatUint(j.seq, 10)}
	epoch, s, ok := strings.Cut(token, ":")
	seq, err := strconv.ParseUint(s, 10, 64)
	if !ok || err != nil || epoch != j.epoch || j.full > seq {
		out.Full = true
		return out
	}
	for p, at := range j.changed {
		if at > seq {
			out.Paths = append(out.Paths, p)
		}
	}
	sort.Strings(out.Paths)
	return out
}

// Server serves journal queries for one working copy.
type Server struct {
	Root    string
	Journal *Journal
	mu      sync.Mutex
	n       int
}

// sync creates a cookie and waits until the watcher has seen it.
func (s *Server) sync(ctx context.Context) error {
	s.mu.Lock()
	s.n++
	name := fmt.Sprintf("%d-%d", os.Getpid(), s.n)
	s.mu.Unlock()
	ch := make(chan struct{})
	s.Journal.mu.Lock()
	s.Journal.cookies[name] = ch
	s.Journal.mu.Unlock()
	dir := filepath.Join(s.Root, ".gft", "cookies")
	os.MkdirAll(dir, 0o755)
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, nil, 0o644); err != nil {
		return err
	}
	defer os.Remove(p)
	select {
	case <-ch:
		return nil
	case <-time.After(2 * time.Second):
		s.Journal.mu.Lock()
		delete(s.Journal.cookies, name)
		s.Journal.mu.Unlock()
		return errors.New("watcher did not observe its cookie")
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/changes", func(w http.ResponseWriter, r *http.Request) {
		c := Changes{Full: true}
		if err := s.sync(r.Context()); err == nil {
			c = s.Journal.Since(r.URL.Query().Get("since"))
		} else {
			c.Token = s.Journal.Token()
		}
		json.NewEncoder(w).Encode(c)
	})
	mux.HandleFunc("/v1/token", func(w http.ResponseWriter, r *http.Request) {
		if err := s.sync(r.Context()); err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		fmt.Fprint(w, s.Journal.Token())
	})
	return mux
}

// SocketPath is where a working copy's watcher listens: .gft/watch.sock,
// or, when that exceeds the OS limit on socket paths (104 bytes on macOS),
// a short path under /tmp derived from the resolved root.
func SocketPath(root string) string {
	if r, err := filepath.EvalSymlinks(root); err == nil {
		root = r
	}
	p := filepath.Join(root, ".gft", "watch.sock")
	if len(p) < 100 {
		return p
	}
	h := sha256.Sum256([]byte(root))
	dir := "/tmp"
	if runtime.GOOS == "windows" {
		dir = os.TempDir()
	}
	return filepath.Join(dir, fmt.Sprintf("gft-%d-%s.sock", os.Getuid(), hex.EncodeToString(h[:6])))
}

// Run watches root until ctx is done, serving queries on its socket.
func Run(ctx context.Context, root string) error {
	root, err := filepath.Abs(root)
	if err != nil {
		return err
	}
	if root, err = filepath.EvalSymlinks(root); err != nil {
		return err
	}
	j := NewJournal()
	// The cookie directory must exist before watching starts (inotify only
	// sees files in directories it already watches).
	if err := os.MkdirAll(filepath.Join(root, ".gft", "cookies"), 0o755); err != nil {
		return err
	}
	stop, err := start(ctx, root, j)
	if err != nil {
		return err
	}
	defer stop()
	sock := SocketPath(root)
	os.Remove(sock)
	ln, err := net.Listen("unix", sock)
	if err != nil {
		return err
	}
	defer os.Remove(sock)
	srv := &http.Server{Handler: (&Server{Root: root, Journal: j}).Handler()}
	go func() {
		<-ctx.Done()
		srv.Close()
	}()
	if err := srv.Serve(ln); !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

// Client queries a working copy's watcher, if one is running.
type Client struct{ http *http.Client }

// Dial returns a client if a watcher socket exists for root.
func Dial(root string) *Client {
	sock := SocketPath(root)
	if _, err := os.Stat(sock); err != nil {
		return nil
	}
	tr := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		var d net.Dialer
		return d.DialContext(ctx, "unix", sock)
	}}
	return &Client{http: &http.Client{Transport: tr, Timeout: 5 * time.Second}}
}

func (c *Client) Token() (string, error) {
	resp, err := c.http.Get("http://watch/v1/token")
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("watcher: %s", resp.Status)
	}
	b, err := io.ReadAll(resp.Body)
	return strings.TrimSpace(string(b)), err
}

func (c *Client) Since(token string) (*Changes, error) {
	resp, err := c.http.Get("http://watch/v1/changes?since=" + token)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var out Changes
	return &out, json.NewDecoder(resp.Body).Decode(&out)
}
