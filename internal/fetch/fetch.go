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

const (
	missingLen   = 0xFFFFFFFF
	forbiddenLen = 0xFFFFFFFE
	// privateBit marks an object the server considers access-controlled:
	// shared tiers pass it through but never cache it, so every request
	// for it reaches the server with the caller's credentials.
	privateBit = 0x80000000
)

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

// ForbiddenError lists objects the caller may not read.
type ForbiddenError struct{ IDs []object.ID }

func (e *ForbiddenError) Error() string {
	return fmt.Sprintf("%d object(s) forbidden (first %s)", len(e.IDs), e.IDs[0].Short())
}

// FlagSource is implemented by sources that report which objects are
// private (access-controlled) so caching tiers can avoid caching them.
type FlagSource interface {
	FetchFlags(ctx context.Context, ids []object.ID, fn func(id object.ID, stored []byte, private bool) error) error
}

type authKey struct{}

// WithAuth attaches the caller's Authorization header value to ctx; tiers
// forward it upstream.
func WithAuth(ctx context.Context, auth string) context.Context {
	if auth == "" {
		return ctx
	}
	return context.WithValue(ctx, authKey{}, auth)
}

// AuthFrom returns the Authorization value carried by ctx.
func AuthFrom(ctx context.Context) string {
	s, _ := ctx.Value(authKey{}).(string)
	return s
}

func fetchFlags(src Source, ctx context.Context, ids []object.ID, fn func(object.ID, []byte, bool) error) error {
	if fs, ok := src.(FlagSource); ok {
		return fs.FetchFlags(ctx, ids, fn)
	}
	return src.Fetch(ctx, ids, func(id object.ID, b []byte) error { return fn(id, b, false) })
}

// StoreSource serves directly from a store with no upstream (the
// authoritative server tier). Guard, if set, decides per object whether the
// caller may read it and whether it is private.
type StoreSource struct {
	Store store.Objects
	Guard func(ctx context.Context, id object.ID, stored []byte) (allowed, private bool)
}

func (s StoreSource) Fetch(ctx context.Context, ids []object.ID, fn func(object.ID, []byte) error) error {
	return s.FetchFlags(ctx, ids, func(id object.ID, b []byte, _ bool) error { return fn(id, b) })
}

func (s StoreSource) FetchFlags(ctx context.Context, ids []object.ID, fn func(object.ID, []byte, bool) error) error {
	var missing, forbidden []object.ID
	for _, id := range ids {
		b, err := s.Store.Get(id)
		if errors.Is(err, object.ErrNotFound) {
			missing = append(missing, id)
			continue
		}
		if err != nil {
			return err
		}
		private := false
		if s.Guard != nil {
			var ok bool
			if ok, private = s.Guard(ctx, id, b); !ok {
				forbidden = append(forbidden, id)
				continue
			}
		}
		if err := fn(id, b, private); err != nil {
			return err
		}
	}
	if len(forbidden) > 0 {
		return &ForbiddenError{forbidden}
	}
	if len(missing) > 0 {
		return &MissingError{missing}
	}
	return nil
}

// Tier is a caching tier: a local store in front of exactly one upstream.
// Concurrent requests for the same missing object are coalesced into one
// upstream fetch, so a burst of jobs on a node (or nodes in a region)
// produces a single upstream read per object. Private objects are passed
// through without caching, and in-flight fetches are only shared between
// callers presenting the same credentials.
type Tier struct {
	Local    store.Objects
	Upstream Source

	mu       sync.Mutex
	inflight map[flightKey]*call
}

type flightKey struct {
	id   object.ID
	auth string
}

type call struct {
	done    chan struct{}
	data    []byte
	private bool
	err     error
}

func NewTier(local store.Objects, upstream Source) *Tier {
	return &Tier{Local: local, Upstream: upstream, inflight: map[flightKey]*call{}}
}

func (t *Tier) Fetch(ctx context.Context, ids []object.ID, fn func(object.ID, []byte) error) error {
	return t.FetchFlags(ctx, ids, func(id object.ID, b []byte, _ bool) error { return fn(id, b) })
}

func (t *Tier) FetchFlags(ctx context.Context, ids []object.ID, fn func(object.ID, []byte, bool) error) error {
	var misses []object.ID
	for _, id := range ids {
		b, err := t.Local.Get(id)
		if err == nil {
			if err := fn(id, b, false); err != nil {
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
	auth := AuthFrom(ctx)
	waits := make(map[object.ID]*call, len(misses))
	var mine, arrived []object.ID
	t.mu.Lock()
	for _, id := range misses {
		if _, dup := waits[id]; dup {
			continue
		}
		k := flightKey{id, auth}
		c, ok := t.inflight[k]
		if !ok {
			// Another request may have fetched it since our local miss
			// (fetchUpstream stores before leaving the inflight map).
			if has, _ := t.Local.Has(id); has {
				arrived = append(arrived, id)
				continue
			}
			c = &call{done: make(chan struct{})}
			t.inflight[k] = c
			mine = append(mine, id)
		}
		waits[id] = c
	}
	t.mu.Unlock()
	for _, id := range arrived {
		b, err := t.Local.Get(id)
		if err != nil {
			// Evicted in between: fetch it ourselves.
			c := &call{done: make(chan struct{})}
			t.mu.Lock()
			t.inflight[flightKey{id, auth}] = c
			t.mu.Unlock()
			waits[id] = c
			mine = append(mine, id)
			continue
		}
		if err := fn(id, b, false); err != nil {
			return err
		}
	}

	if len(mine) > 0 {
		t.fetchUpstream(ctx, mine, auth)
	}
	var missing, forbidden []object.ID
	for _, id := range misses {
		c := waits[id]
		if c == nil {
			continue // served from the local store above
		}
		select {
		case <-c.done:
		case <-ctx.Done():
			return ctx.Err()
		}
		delete(waits, id)
		if c.err != nil {
			var fe *ForbiddenError
			switch {
			case errors.As(c.err, &fe):
				forbidden = append(forbidden, id)
				continue
			case errors.Is(c.err, object.ErrNotFound):
				missing = append(missing, id)
				continue
			}
			return c.err
		}
		if err := fn(id, c.data, c.private); err != nil {
			return err
		}
	}
	if len(forbidden) > 0 {
		return &ForbiddenError{forbidden}
	}
	if len(missing) > 0 {
		return &MissingError{missing}
	}
	return nil
}

func (t *Tier) fetchUpstream(ctx context.Context, ids []object.ID, auth string) {
	finish := func(id object.ID, data []byte, private bool, err error) {
		t.mu.Lock()
		k := flightKey{id, auth}
		c := t.inflight[k]
		delete(t.inflight, k)
		t.mu.Unlock()
		if c != nil {
			c.data, c.private, c.err = data, private, err
			close(c.done)
		}
	}
	got := make(map[object.ID]bool, len(ids))
	err := fetchFlags(t.Upstream, ctx, ids, func(id object.ID, b []byte, private bool) error {
		if got[id] {
			return nil
		}
		got[id] = true
		if !private {
			_ = t.Local.Put(id, b)
		}
		finish(id, b, private, nil)
		return nil
	})
	var me *MissingError
	var fe *ForbiddenError
	forbidden := map[object.ID]bool{}
	if errors.As(err, &fe) {
		for _, id := range fe.IDs {
			forbidden[id] = true
		}
	}
	for _, id := range ids {
		if got[id] {
			continue
		}
		e := err
		switch {
		case forbidden[id]:
			e = &ForbiddenError{[]object.ID{id}}
		case e == nil || errors.As(err, &me) || errors.As(err, &fe):
			e = object.ErrNotFound
		}
		finish(id, nil, false, e)
	}
}

// Handler serves the objects RPC from a Source. The caller's Authorization
// header is carried to upstream tiers.
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
		ctx := WithAuth(req.Context(), req.Header.Get("Authorization"))
		w.Header().Set("Content-Type", "application/x-graft-objects")
		bw := bufio.NewWriterSize(w, 256<<10)
		var hdr [36]byte
		marker := func(id object.ID, n uint32) {
			copy(hdr[:32], id[:])
			binary.BigEndian.PutUint32(hdr[32:], n)
			bw.Write(hdr[:])
		}
		err = fetchFlags(src, ctx, ids, func(id object.ID, b []byte, private bool) error {
			n := uint32(len(b))
			if private {
				n |= privateBit
			}
			marker(id, n)
			_, err := bw.Write(b)
			return err
		})
		var me *MissingError
		var fe *ForbiddenError
		switch {
		case errors.As(err, &fe):
			for _, id := range fe.IDs {
				marker(id, forbiddenLen)
			}
			err = nil
		case errors.As(err, &me):
			for _, id := range me.IDs {
				marker(id, missingLen)
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
	// Token is sent as a bearer token when the request context carries no
	// forwarded credentials.
	Token string
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
	return r.FetchFlags(ctx, ids, func(id object.ID, b []byte, _ bool) error { return fn(id, b) })
}

func (r *Remote) FetchFlags(ctx context.Context, ids []object.ID, fn func(object.ID, []byte, bool) error) error {
	for start := 0; start < len(ids); start += MaxBatch {
		end := min(start+MaxBatch, len(ids))
		if err := r.fetchBatch(ctx, ids[start:end], fn); err != nil {
			return err
		}
	}
	return nil
}

// Authorization returns the header value to send for ctx.
func (r *Remote) Authorization(ctx context.Context) string {
	if a := AuthFrom(ctx); a != "" {
		return a
	}
	if r.Token != "" {
		return "Bearer " + r.Token
	}
	return ""
}

func (r *Remote) fetchBatch(ctx context.Context, ids []object.ID, fn func(object.ID, []byte, bool) error) error {
	want := ids
	var lastErr error
	for attempt := 0; attempt <= r.Retries && len(want) > 0; attempt++ {
		got := make(map[object.ID]bool, len(want))
		var missing, forbidden []object.ID
		lastErr = r.once(ctx, want, func(id object.ID, b []byte, state uint32) error {
			got[id] = true
			switch state {
			case missingLen:
				missing = append(missing, id)
				return nil
			case forbiddenLen:
				forbidden = append(forbidden, id)
				return nil
			}
			return fn(id, b, state == privateBit)
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
		if len(forbidden) > 0 {
			return &ForbiddenError{forbidden}
		}
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

// once performs one request. fn's state is 0 for a normal object,
// privateBit for a private one, or missingLen / forbiddenLen.
func (r *Remote) once(ctx context.Context, ids []object.ID, fn func(object.ID, []byte, uint32) error) error {
	body := make([]byte, 0, len(ids)*32)
	for _, id := range ids {
		body = append(body, id[:]...)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, r.base+"/v1/objects", bytes.NewReader(body))
	if err != nil {
		return err
	}
	if a := r.Authorization(ctx); a != "" {
		req.Header.Set("Authorization", a)
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
		if n == missingLen || n == forbiddenLen {
			if err := fn(id, nil, n); err != nil {
				return err
			}
			continue
		}
		state := uint32(0)
		if n&privateBit != 0 {
			state, n = privateBit, n&^privateBit
		}
		if n > 1<<30 {
			return fmt.Errorf("object frame too large")
		}
		b := make([]byte, n)
		if _, err := io.ReadFull(br, b); err != nil {
			return err
		}
		if err := fn(id, b, state); err != nil {
			return err
		}
	}
}
