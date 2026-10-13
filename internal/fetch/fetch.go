// Package fetch implements graft's object transfer protocol and its tiers.
//
// Unlike git's negotiation, the client never tells the server what it
// already has. It only asks for specific objects it wants, by ID. Each
// request is for immutable data, so any tier can answer it from cache, and a
// transfer is trivially resumable: a retry asks only for what is still
// missing. There is no packfile and no delta computation on the server.
//
// Every tier (scion-daemon / scion-cache / scion-server in the Scion paper;
// here `gft serve --tier daemon|cache|server`) speaks the identical RPC:
//
//	POST /v1/objects   body: N x 32-byte IDs
//	response: stream of frames  [32-byte id][u32 length][stored bytes]
//	          length 0xFFFFFFFF marks a missing object.
//
// Stored bytes are the on-disk format, so tiers stream them straight from
// memory or disk without recompressing.
package fetch

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/VishiATChoudhary/graft/internal/object"
	"github.com/VishiATChoudhary/graft/internal/store"
)

const missingLen = 0xFFFFFFFF

// MaxBatch is the largest number of IDs accepted per request.
const MaxBatch = 1 << 16

// Source yields objects by ID. fn may be called in any order. Objects that
// cannot be found produce an error after all found objects are delivered.
type Source interface {
	Fetch(ctx context.Context, ids []object.ID, fn func(id object.ID, stored []byte) error) error
}

// MissingError lists objects no tier could provide.
type MissingError struct{ IDs []object.ID }

func (e *MissingError) Error() string {
	return fmt.Sprintf("%d object(s) not found (first %s)", len(e.IDs), e.IDs[0].Short())
}

func (e *MissingError) Unwrap() error { return object.ErrNotFound }

// StoreSource serves directly from a store with no upstream (the
// authoritative server tier).
type StoreSource struct{ Store store.Objects }

func (s StoreSource) Fetch(_ context.Context, ids []object.ID, fn func(object.ID, []byte) error) error {
	var missing []object.ID
	for _, id := range ids {
		b, err := s.Store.Get(id)
		if errors.Is(err, object.ErrNotFound) {
			missing = append(missing, id)
			continue
		}
		if err != nil {
			return err
		}
		if err := fn(id, b); err != nil {
			return err
		}
	}
	if len(missing) > 0 {
		return &MissingError{missing}
	}
	return nil
}

// Tier is a caching tier: a local store in front of exactly one upstream.
// Concurrent requests for the same missing object are coalesced into one
// upstream fetch, so a burst of jobs on a node (or nodes in a region)
// produces a single upstream read per object.
type Tier struct {
	Local    store.Objects
	Upstream Source

	mu       sync.Mutex
	inflight map[object.ID]*call
}

type call struct {
	done chan struct{}
	data []byte
	err  error
}

func NewTier(local store.Objects, upstream Source) *Tier {
	return &Tier{Local: local, Upstream: upstream, inflight: map[object.ID]*call{}}
}

func (t *Tier) Fetch(ctx context.Context, ids []object.ID, fn func(object.ID, []byte) error) error {
	var misses []object.ID
	for _, id := range ids {
		b, err := t.Local.Get(id)
		if err == nil {
			if err := fn(id, b); err != nil {
				return err
			}
			continue
		}
		if !errors.Is(err, object.ErrNotFound) {
			return err
		}
		misses = append(misses, id)
	}
	if len(misses) == 0 {
		return nil
	}
	waits := make(map[object.ID]*call, len(misses))
	var mine []object.ID
	t.mu.Lock()
	for _, id := range misses {
		if _, dup := waits[id]; dup {
			continue
		}
		c, ok := t.inflight[id]
		if !ok {
			c = &call{done: make(chan struct{})}
			t.inflight[id] = c
			mine = append(mine, id)
		}
		waits[id] = c
	}
	t.mu.Unlock()

	if len(mine) > 0 {
		t.fetchUpstream(ctx, mine, waits)
	}
	var missing []object.ID
	for _, id := range misses {
		c := waits[id]
		if c == nil {
			continue
		}
		select {
		case <-c.done:
		case <-ctx.Done():
			return ctx.Err()
		}
		delete(waits, id)
		if c.err != nil {
			if errors.Is(c.err, object.ErrNotFound) {
				missing = append(missing, id)
				continue
			}
			return c.err
		}
		if err := fn(id, c.data); err != nil {
			return err
		}
	}
	if len(missing) > 0 {
		return &MissingError{missing}
	}
	return nil
}

func (t *Tier) fetchUpstream(ctx context.Context, ids []object.ID, calls map[object.ID]*call) {
	finish := func(id object.ID, data []byte, err error) {
		t.mu.Lock()
		c := t.inflight[id]
		delete(t.inflight, id)
		t.mu.Unlock()
		if c != nil {
			c.data, c.err = data, err
			close(c.done)
		}
	}
	got := make(map[object.ID]bool, len(ids))
	err := t.Upstream.Fetch(ctx, ids, func(id object.ID, b []byte) error {
		if got[id] {
			return nil
		}
		got[id] = true
		_ = t.Local.Put(id, b)
		finish(id, b, nil)
		return nil
	})
	var me *MissingError
	for _, id := range ids {
		if got[id] {
			continue
		}
		e := err
		if e == nil || errors.As(err, &me) {
			e = object.ErrNotFound
		}
		finish(id, nil, e)
	}
}

// Handler serves the objects RPC from a Source.
func Handler(src Source) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodPost {
			http.Error(w, "POST only", http.StatusMethodNotAllowed)
			return
		}
		body, err := io.ReadAll(io.LimitReader(req.Body, MaxBatch*32+1))
		if err != nil || len(body)%32 != 0 || len(body) > MaxBatch*32 {
			http.Error(w, "body must be at most 65536 32-byte ids", http.StatusBadRequest)
			return
		}
		ids := make([]object.ID, len(body)/32)
		for i := range ids {
			copy(ids[i][:], body[i*32:])
		}
		w.Header().Set("Content-Type", "application/x-graft-objects")
		bw := bufio.NewWriterSize(w, 256<<10)
		var hdr [36]byte
		err = src.Fetch(req.Context(), ids, func(id object.ID, b []byte) error {
			copy(hdr[:32], id[:])
			binary.BigEndian.PutUint32(hdr[32:], uint32(len(b)))
			bw.Write(hdr[:])
			_, err := bw.Write(b)
			return err
		})
		var me *MissingError
		if errors.As(err, &me) {
			for _, id := range me.IDs {
				copy(hdr[:32], id[:])
				binary.BigEndian.PutUint32(hdr[32:], missingLen)
				bw.Write(hdr[:])
			}
			err = nil
		}
		if err != nil {
			// Mid-stream failure: abort the connection so the client sees a
			// truncated stream and retries what it still lacks.
			bw.Flush()
			panic(http.ErrAbortHandler)
		}
		bw.Flush()
	}
}

// Remote is a Source backed by another tier over HTTP or a unix socket.
type Remote struct {
	base   string
	client *http.Client
	// Retries bounds attempts per batch; each retry requests only the
	// objects not yet received.
	Retries int
}

// NewRemote dials an endpoint: "http(s)://host:port" or "unix:///path.sock".
func NewRemote(endpoint string) *Remote {
	tr := &http.Transport{
		MaxIdleConns:        256,
		MaxIdleConnsPerHost: 64,
		IdleConnTimeout:     90 * time.Second,
		DisableCompression:  true, // objects are already compressed
	}
	base := strings.TrimRight(endpoint, "/")
	if sock, ok := strings.CutPrefix(endpoint, "unix://"); ok {
		tr.DialContext = func(ctx context.Context, _, _ string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "unix", sock)
		}
		base = "http://unix"
	}
	return &Remote{base: base, client: &http.Client{Transport: tr}, Retries: 3}
}

// HTTPClient exposes the underlying client for other API calls.
func (r *Remote) HTTPClient() *http.Client { return r.client }

// Base is the URL prefix for requests.
func (r *Remote) Base() string { return r.base }

func (r *Remote) Fetch(ctx context.Context, ids []object.ID, fn func(object.ID, []byte) error) error {
	for start := 0; start < len(ids); start += MaxBatch {
		end := min(start+MaxBatch, len(ids))
		if err := r.fetchBatch(ctx, ids[start:end], fn); err != nil {
			return err
		}
	}
	return nil
}

func (r *Remote) fetchBatch(ctx context.Context, ids []object.ID, fn func(object.ID, []byte) error) error {
	want := ids
	var lastErr error
	for attempt := 0; attempt <= r.Retries && len(want) > 0; attempt++ {
		got := make(map[object.ID]bool, len(want))
		var missing []object.ID
		lastErr = r.once(ctx, want, func(id object.ID, b []byte) error {
			got[id] = true
			if b == nil {
				missing = append(missing, id)
				return nil
			}
			return fn(id, b)
		})
		if lastErr != nil && ctx.Err() != nil {
			return lastErr
		}
		var rest []object.ID
		for _, id := range want {
			if !got[id] {
				rest = append(rest, id)
			}
		}
		want = rest
		if len(missing) > 0 {
			return &MissingError{missing}
		}
		if lastErr == nil && len(want) == 0 {
			return nil
		}
		if attempt < r.Retries {
			time.Sleep(time.Duration(50<<attempt) * time.Millisecond)
		}
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("upstream returned incomplete response")
	}
	return fmt.Errorf("fetch %d objects: %w", len(want), lastErr)
}

func (r *Remote) once(ctx context.Context, ids []object.ID, fn func(object.ID, []byte) error) error {
	body := make([]byte, 0, len(ids)*32)
	for _, id := range ids {
		body = append(body, id[:]...)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, r.base+"/v1/objects", bytes.NewReader(body))
	if err != nil {
		return err
	}
	resp, err := r.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("objects: %s: %s", resp.Status, strings.TrimSpace(string(msg)))
	}
	br := bufio.NewReaderSize(resp.Body, 256<<10)
	var hdr [36]byte
	for {
		if _, err := io.ReadFull(br, hdr[:]); err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		var id object.ID
		copy(id[:], hdr[:32])
		n := binary.BigEndian.Uint32(hdr[32:])
		if n == missingLen {
			if err := fn(id, nil); err != nil {
				return err
			}
			continue
		}
		if n > 1<<31 {
			return fmt.Errorf("object frame too large")
		}
		b := make([]byte, n)
		if _, err := io.ReadFull(br, b); err != nil {
			return err
		}
		if err := fn(id, b); err != nil {
			return err
		}
	}
}
