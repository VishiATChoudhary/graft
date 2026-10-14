package store

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/VishiATChoudhary/graft/internal/object"
)

// Sharded is a Backend over a fleet of storage nodes (NodeHandler).
//
// Objects and derived data are placed by ID prefix on shards. Each shard is
// a replica set ("u1|u2|u3"): immutable writes go to every replica and
// succeed once a majority acknowledges; reads go to one replica and fail
// over to the others (repairing a replica that missed a write). The only
// mutable state, refs and the changeset log, lives on shard 0, whose
// replicas run Raft: metadata calls follow the leader, and survive a leader
// failure once a new one is elected.
//
// Everything immutable is cached in memory, so API servers using Sharded
// are, as in Scion, essentially large caches that also compute derived data
// and write it back. Any number of API servers can share one fleet.
type Sharded struct {
	shards [][]string
	client *http.Client
	rr     atomic.Uint32
	leader atomic.Value // string: last known metadata leader

	hot      *MemLRU // stored objects
	payloads *MemLRU // [kind]+payload
	derived  sync.Map
	dmu      sync.Mutex
	dbytes   int64
	seqs     sync.Map // ID -> seq
	ids      sync.Map // seq -> ID

	hints     chan [2]object.ID // asynchronous Deltify hints
	hintsOnce sync.Once

	// Write-behind for objects: PutBase returns once the write is queued;
	// barrier() waits for every queued write before anything that can
	// reference those objects is published (log, derived data, refs).
	queued        sync.Map     // ID -> stored bytes, until written
	byID          sync.Map     // ID -> *pendingWrite
	queuedDerived sync.Map     // derivedKey -> value, until written
	derivedN      atomic.Int64 // queued derived writes (Flush waits)
	waiting       map[uint64]chan struct{}
	ticket        uint64
	pendMu        sync.Mutex
	pendErr       error
	inflight      chan struct{}
}

// ShardOf returns the shard index for an ID (by its storage prefix).
func ShardOf(id object.ID, n int) int {
	return int(binary.BigEndian.Uint16(id[:2])) % n
}

// ParseFleet parses "a|b|c,d|e|f" (already split on commas) into shards of
// replica URLs.
func ParseFleet(spec []string) [][]string {
	var out [][]string
	for _, s := range spec {
		var reps []string
		for _, u := range strings.Split(s, "|") {
			if u = strings.TrimRight(strings.TrimSpace(u), "/"); u != "" {
				reps = append(reps, u)
			}
		}
		if len(reps) > 0 {
			out = append(out, reps)
		}
	}
	return out
}

// NewSharded connects to storage nodes, in shard order. Each entry may list
// replicas separated by "|".
func NewSharded(nodes []string, cacheBytes int64) *Sharded {
	tr := &http.Transport{
		MaxIdleConns:        1024,
		MaxIdleConnsPerHost: 256,
		IdleConnTimeout:     90 * time.Second,
		DisableCompression:  true,
		DialContext:         (&net.Dialer{Timeout: 2 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
	}
	if cacheBytes <= 0 {
		cacheBytes = 1 << 30
	}
	s := &Sharded{shards: ParseFleet(nodes), client: &http.Client{Transport: tr, Timeout: 5 * time.Minute},
		hot: NewMemLRU(cacheBytes / 2), payloads: NewMemLRU(cacheBytes / 2), inflight: make(chan struct{}, 64), waiting: map[uint64]chan struct{}{}}
	s.leader.Store("")
	return s
}

func (s *Sharded) replicas(id object.ID) []string { return s.shards[ShardOf(id, len(s.shards))] }

type httpStatusError struct {
	code int
	msg  string
}

func (e *httpStatusError) Error() string { return fmt.Sprintf("storage node: %d %s", e.code, e.msg) }

// call performs one request. 404 maps to ErrNotFound; 421 to NotLeaderError.
func (s *Sharded) call(method, url string, body []byte) ([]byte, int, error) {
	var rd io.Reader
	if body != nil {
		rd = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(context.Background(), method, url, rd)
	if err != nil {
		return nil, 0, err
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, 0, err
	}
	switch resp.StatusCode {
	case http.StatusOK, http.StatusConflict:
		return b, resp.StatusCode, nil
	case http.StatusNotFound:
		return nil, resp.StatusCode, object.ErrNotFound
	case http.StatusMisdirectedRequest:
		return nil, resp.StatusCode, &NotLeaderError{Leader: resp.Header.Get("X-Graft-Leader")}
	}
	return nil, resp.StatusCode, &httpStatusError{resp.StatusCode, strings.TrimSpace(string(b))}
}

// readAny asks replicas in turn (starting at a rotating one) until one
// returns an answer; found reports whether the body holds the data (as
// opposed to "this replica lacks it"). Replicas that lacked found data are
// passed to repair.
func (s *Sharded) readAny(reps []string, path string, body []byte, found func([]byte) bool, repair func(missed []string, b []byte)) ([]byte, error) {
	start := int(s.rr.Add(1)) % len(reps)
	var missed []string
	var lastErr error
	for i := range reps {
		u := reps[(start+i)%len(reps)]
		b, _, err := s.call("POST", u+path, body)
		if err != nil {
			if errors.Is(err, object.ErrNotFound) {
				missed = append(missed, u)
			} else {
				lastErr = err
			}
			continue
		}
		if found(b) {
			if len(missed) > 0 && repair != nil {
				go repair(missed, b)
			}
			return b, nil
		}
		missed = append(missed, u)
	}
	// Absent only if a majority of replicas answered that it is absent
	// (writes reach a majority, so any majority intersects them).
	if len(missed) >= len(reps)/2+1 {
		return nil, object.ErrNotFound
	}
	if lastErr == nil {
		lastErr = object.ErrNotFound
	}
	return nil, lastErr
}

// writeAll sends an idempotent write to every replica and succeeds once a
// majority acknowledges it.
func (s *Sharded) writeAll(reps []string, path string, body []byte) error {
	ch := make(chan error, len(reps))
	for _, u := range reps {
		go func(u string) {
			_, _, err := s.call("POST", u+path, body)
			ch <- err
		}(u)
	}
	ok, need := 0, len(reps)/2+1
	var firstErr error
	for range reps {
		if err := <-ch; err == nil {
			ok++
			if ok >= need {
				return nil // stragglers finish in the background
			}
		} else if firstErr == nil {
			firstErr = err
		}
	}
	return fmt.Errorf("write reached %d of %d replicas (need %d): %w", ok, len(reps), need, firstErr)
}

// meta performs a metadata call against shard 0's leader, following
// redirects and retrying through elections.
func (s *Sharded) meta(method, path string, body []byte) ([]byte, int, error) {
	reps := s.shards[0]
	deadline := time.Now().Add(30 * time.Second)
	next := 0
	for attempt := 0; ; attempt++ {
		target := s.leader.Load().(string)
		if target == "" {
			target = reps[next%len(reps)]
			next++
		}
		b, code, err := s.call(method, target+path, body)
		var nl *NotLeaderError
		switch {
		case err == nil, errors.Is(err, object.ErrNotFound):
			if target != s.leader.Load().(string) {
				s.leader.Store(target)
			}
			return b, code, err
		case errors.As(err, &nl) && nl.Leader != "" && nl.Leader != target:
			s.leader.Store(nl.Leader)
			continue
		default:
			s.leader.Store("") // try the next replica
		}
		if time.Now().After(deadline) {
			return nil, code, err
		}
		time.Sleep(time.Duration(min(50*(attempt+1), 500)) * time.Millisecond)
	}
}

// frames parses [id][u32 len][data] frames; a nil slice marks missing.
func frames(b []byte, fn func(id object.ID, data []byte)) error {
	for len(b) > 0 {
		if len(b) < 36 {
			return object.ErrCorrupt
		}
		var id object.ID
		copy(id[:], b[:32])
		n := binary.BigEndian.Uint32(b[32:36])
		b = b[36:]
		if n == 0xFFFFFFFF {
			fn(id, nil)
			continue
		}
		if uint32(len(b)) < n {
			return object.ErrCorrupt
		}
		fn(id, b[:n:n])
		b = b[n:]
	}
	return nil
}

func oneFrame(b []byte) []byte {
	var out []byte
	frames(b, func(_ object.ID, d []byte) { out = d })
	return out
}

func putFrame(id, base object.ID, stored []byte) []byte {
	buf := make([]byte, 68, 68+len(stored))
	copy(buf, id[:])
	copy(buf[32:], base[:])
	binary.BigEndian.PutUint32(buf[64:], uint32(len(stored)))
	return append(buf, stored...)
}

func derivedFrame(id object.ID, v []byte) []byte {
	buf := make([]byte, 36, 36+len(v))
	copy(buf, id[:])
	binary.BigEndian.PutUint32(buf[32:], uint32(len(v)))
	return append(buf, v...)
}

func (s *Sharded) repairObject(id object.ID) func([]string, []byte) {
	return func(missed []string, b []byte) {
		if stored := oneFrame(b); stored != nil {
			for _, u := range missed {
				s.call("POST", u+"/kv/put", putFrame(id, object.Zero, stored))
			}
		}
	}
}

func (s *Sharded) Get(id object.ID) ([]byte, error) {
	if b, err := s.hot.Get(id); err == nil {
		return b, nil
	}
	if v, ok := s.queued.Load(id); ok {
		return v.([]byte), nil
	}
	body, err := s.readAny(s.replicas(id), "/kv/get", id[:], func(b []byte) bool { return oneFrame(b) != nil }, s.repairObject(id))
	if err != nil {
		return nil, err
	}
	out := oneFrame(body)
	s.hot.Put(id, out)
	return out, nil
}

func (s *Sharded) Payload(id object.ID) (object.Kind, []byte, error) {
	if v, err := s.payloads.Get(id); err == nil {
		return object.Kind(v[0]), v[1:], nil
	}
	// Objects this server holds (written recently or cached) unpack locally.
	if b, err := s.hot.Get(id); err == nil {
		if k, p, err := object.Unpack(b); err == nil {
			return k, p, nil
		}
	}
	if v, ok := s.queued.Load(id); ok {
		if k, p, err := object.Unpack(v.([]byte)); err == nil {
			return k, p, nil
		}
	}
	body, err := s.readAny(s.replicas(id), "/kv/payload", id[:], func(b []byte) bool { return len(oneFrame(b)) > 0 }, nil)
	if err != nil {
		return 0, nil, err
	}
	out := oneFrame(body)
	s.payloads.Put(id, out)
	return object.Kind(out[0]), out[1:], nil
}

func (s *Sharded) Has(id object.ID) (bool, error) {
	if ok, _ := s.hot.Has(id); ok {
		return true, nil
	}
	if _, ok := s.queued.Load(id); ok {
		return true, nil
	}
	if ok, _ := s.payloads.Has(id); ok {
		return true, nil
	}
	_, err := s.readAny(s.replicas(id), "/kv/has", id[:], func(b []byte) bool { return len(b) == 1 && b[0] == 1 }, nil)
	if errors.Is(err, object.ErrNotFound) {
		return false, nil
	}
	return err == nil, err
}

func (s *Sharded) PayloadSize(id object.ID) (uint64, error) {
	if b, err := s.hot.Get(id); err == nil {
		return object.PayloadSize(b)
	}
	body, err := s.readAny(s.replicas(id), "/kv/size", id[:], func(b []byte) bool { return len(b) == 8 }, nil)
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint64(body), nil
}

func (s *Sharded) Put(id object.ID, stored []byte) error { return s.PutBase(id, stored, object.Zero) }

func (s *Sharded) PutBase(id object.ID, stored []byte, base object.ID) error {
	s.hot.Put(id, stored) // readable by this server immediately
	s.queued.Store(id, stored)
	done := make(chan struct{})
	pw := &pendingWrite{done: done}
	s.byID.Store(id, pw)
	s.pendMu.Lock()
	s.ticket++
	t := s.ticket
	s.waiting[t] = done
	s.pendMu.Unlock()
	s.inflight <- struct{}{}
	go func() {
		err := s.writeAll(s.replicas(id), "/kv/put", putFrame(id, base, stored))
		pw.err = err
		if err == nil { // failures stay visible to waitFor until rewritten
			s.byID.CompareAndDelete(id, pw)
		}
		s.queued.Delete(id)
		<-s.inflight
		s.pendMu.Lock()
		if err != nil && s.pendErr == nil {
			s.pendErr = err
		}
		delete(s.waiting, t)
		s.pendMu.Unlock()
		close(done)
	}()
	return nil
}

type pendingWrite struct {
	done chan struct{}
	err  error // set before done is closed
}

// waitFor waits for queued writes of the given objects only.
func (s *Sharded) waitFor(ids ...object.ID) error {
	for _, id := range ids {
		if v, ok := s.byID.Load(id); ok {
			pw := v.(*pendingWrite)
			<-pw.done
			if pw.err != nil {
				return pw.err
			}
		}
	}
	return nil
}

// barrier waits for every object write queued before it (by any
// goroutine) and reports the first failure.
func (s *Sharded) barrier() error {
	s.pendMu.Lock()
	chans := make([]chan struct{}, 0, len(s.waiting))
	for _, c := range s.waiting {
		chans = append(chans, c)
	}
	s.pendMu.Unlock()
	for _, c := range chans {
		<-c
	}
	s.pendMu.Lock()
	defer s.pendMu.Unlock()
	err := s.pendErr
	s.pendErr = nil
	return err
}

// Deltify is a storage hint: it is queued and sent to every replica in the
// background (dropped if the queue is full), keeping it off the write path.
func (s *Sharded) Deltify(id, base object.ID) error {
	s.hintsOnce.Do(func() {
		s.hints = make(chan [2]object.ID, 4096)
		for i := 0; i < 4; i++ {
			go func() {
				for h := range s.hints {
					for _, u := range s.replicas(h[0]) {
						s.call("POST", u+"/kv/deltify", append(h[0][:len(h[0]):len(h[0])], h[1][:]...))
					}
				}
			}()
		}
	})
	select {
	case s.hints <- [2]object.ID{id, base}:
	default:
	}
	return nil
}

type derivedKey struct {
	kind string
	id   object.ID
}

func (s *Sharded) GetDerived(kind string, id object.ID) ([]byte, error) {
	if v, ok := s.derived.Load(derivedKey{kind, id}); ok {
		return v.([]byte), nil
	}
	if v, ok := s.queuedDerived.Load(derivedKey{kind, id}); ok {
		return v.([]byte), nil
	}
	body, err := s.readAny(s.replicas(id), "/kv/derived/get?kind="+kind, id[:], func(b []byte) bool { return oneFrame(b) != nil },
		func(missed []string, b []byte) {
			if v := oneFrame(b); v != nil {
				for _, u := range missed {
					s.call("POST", u+"/kv/derived/put?kind="+kind, derivedFrame(id, v))
				}
			}
		})
	if err != nil {
		return nil, err
	}
	out := oneFrame(body)
	s.cacheDerived(kind, id, out)
	return out, nil
}

func (s *Sharded) cacheDerived(kind string, id object.ID, v []byte) {
	s.dmu.Lock()
	defer s.dmu.Unlock()
	if s.dbytes > 256<<20 {
		s.derived.Range(func(k, _ any) bool { s.derived.Delete(k); return true })
		s.dbytes = 0
	}
	s.derived.Store(derivedKey{kind, id}, v)
	s.dbytes += int64(len(v)) + 64
}

// PutDerived is write-behind: the value is readable here at once and is
// written after every object queued before it (so derived data never
// refers to objects not yet durable). Derived data is a cache, so
// publishing (log appends, ref updates) does not wait for it; Flush does.
func (s *Sharded) PutDerived(kind string, id object.ID, v []byte) error {
	s.cacheDerived(kind, id, v)
	k := derivedKey{kind, id}
	s.queuedDerived.Store(k, v)
	s.pendMu.Lock()
	prior := make([]chan struct{}, 0, len(s.waiting))
	for _, c := range s.waiting {
		prior = append(prior, c)
	}
	s.pendMu.Unlock()
	s.derivedN.Add(1)
	go func() {
		defer s.derivedN.Add(-1)
		for _, c := range prior {
			<-c
		}
		s.inflight <- struct{}{}
		s.writeAll(s.replicas(id), "/kv/derived/put?kind="+kind, derivedFrame(id, v))
		<-s.inflight
		s.queuedDerived.Delete(k)
	}()
	return nil
}

func (s *Sharded) GetRef(name string) (object.ID, error) {
	body, _, err := s.meta("GET", "/kv/ref?name="+urlEscape(name), nil)
	if err != nil {
		return object.Zero, err
	}
	var id object.ID
	copy(id[:], body)
	return id, nil
}

// CASRef needs no write barrier: a ref names a changeset, and changesets
// are only logged (which the repository checks before any ref update)
// after their contents are durable.
func (s *Sharded) CASRef(name string, old, new object.ID) error {
	b, _ := json.Marshal(struct {
		Name     string
		Old, New object.ID
	}{name, old, new})
	body, code, err := s.meta("POST", "/kv/ref/cas", b)
	if err != nil {
		return err
	}
	if code == http.StatusConflict {
		var cur object.ID
		copy(cur[:], body)
		return &RefMismatch{Name: name, Current: cur, Expected: old}
	}
	return nil
}

func (s *Sharded) ListRefs() (map[string]object.ID, error) {
	body, _, err := s.meta("GET", "/kv/refs", nil)
	if err != nil {
		return nil, err
	}
	out := map[string]object.ID{}
	return out, json.Unmarshal(body, &out)
}

// AppendLog publishes a changeset in the log once the changeset and its
// blobs are durable (trees are derived data; parents are already logged).
// It waits only for those writes, not for unrelated concurrent requests.
func (s *Sharded) AppendLog(id object.ID) (uint64, error) {
	seqs, err := s.AppendLogs([]object.ID{id})
	if err != nil {
		return 0, err
	}
	return seqs[0], nil
}

// AppendLogs appends several changesets, in order, in one request.
func (s *Sharded) AppendLogs(list []object.ID) ([]uint64, error) {
	var deps []object.ID
	body := make([]byte, 0, len(list)*len(object.Zero))
	for _, id := range list {
		body = append(body, id[:]...)
		deps = append(deps, id)
		if v, ok := s.queued.Load(id); ok {
			if _, p, err := object.Unpack(v.([]byte)); err == nil {
				if cs, err := object.DecodeChangeset(p); err == nil {
					for _, ch := range cs.Changes {
						if !ch.Blob.IsZero() {
							deps = append(deps, ch.Blob)
						}
					}
				}
			}
		}
	}
	if err := s.waitFor(deps...); err != nil {
		return nil, err
	}
	resp, _, err := s.meta("POST", "/kv/log/append", body)
	if err != nil {
		return nil, err
	}
	if len(resp) != 8*len(list) {
		return nil, object.ErrCorrupt
	}
	seqs := make([]uint64, len(list))
	for i, id := range list {
		seqs[i] = binary.BigEndian.Uint64(resp[8*i:])
		s.seqs.Store(id, seqs[i])
		s.ids.Store(seqs[i], id)
	}
	return seqs, nil
}

func (s *Sharded) LogSeq(id object.ID) (uint64, error) {
	if v, ok := s.seqs.Load(id); ok {
		return v.(uint64), nil
	}
	body, _, err := s.meta("POST", "/kv/log/seq", id[:])
	if err != nil {
		return 0, err
	}
	if len(body) != 8 {
		return 0, object.ErrCorrupt
	}
	seq := binary.BigEndian.Uint64(body)
	if seq == 0 {
		return 0, object.ErrNotFound
	}
	s.seqs.Store(id, seq)
	return seq, nil
}

func (s *Sharded) LogID(seq uint64) (object.ID, error) {
	if v, ok := s.ids.Load(seq); ok {
		return v.(object.ID), nil
	}
	body, _, err := s.meta("GET", "/kv/log/id?seq="+strconv.FormatUint(seq, 10), nil)
	if err != nil {
		return object.Zero, err
	}
	var id object.ID
	copy(id[:], body)
	s.ids.Store(seq, id)
	return id, nil
}

func (s *Sharded) LastLogSeq() (uint64, error) {
	body, _, err := s.meta("GET", "/kv/log/last", nil)
	if err != nil {
		return 0, err
	}
	if len(body) != 8 {
		return 0, object.ErrCorrupt
	}
	return binary.BigEndian.Uint64(body), nil
}

func (s *Sharded) ScanLogIdxPrefix(prefix []byte, fn func(object.ID) bool) error {
	body, _, err := s.meta("GET", "/kv/log/prefix?p="+hex.EncodeToString(prefix), nil)
	if err != nil {
		return err
	}
	for i := 0; i+32 <= len(body); i += 32 {
		var id object.ID
		copy(id[:], body[i:])
		if !fn(id) {
			break
		}
	}
	return nil
}

func (s *Sharded) Stats() map[string]any {
	out := map[string]any{"shards": len(s.shards)}
	for i, reps := range s.shards {
		var rs []any
		for _, u := range reps {
			body, _, err := s.call("GET", u+"/kv/stats", nil)
			if err != nil {
				rs = append(rs, map[string]any{"url": u, "error": err.Error()})
				continue
			}
			var m map[string]any
			json.Unmarshal(body, &m)
			if m == nil {
				m = map[string]any{}
			}
			m["url"] = u
			rs = append(rs, m)
		}
		out["shard"+strconv.Itoa(i)] = rs
	}
	h, m, b := s.hot.Stats()
	out["api_cache"] = map[string]any{"hits": h, "misses": m, "bytes": b}
	out["metadata_leader"] = s.leader.Load()
	return out
}

func (s *Sharded) Flush() error {
	if err := s.barrier(); err != nil {
		return err
	}
	for s.derivedN.Load() > 0 {
		time.Sleep(time.Millisecond)
	}
	for _, reps := range s.shards {
		for _, u := range reps {
			s.call("POST", u+"/kv/flush", nil)
		}
	}
	return nil
}

func (s *Sharded) Close() error { return nil }

func urlEscape(s string) string {
	var b strings.Builder
	for _, c := range []byte(s) {
		if c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || strings.IndexByte("-._~/", c) >= 0 {
			b.WriteByte(c)
		} else {
			fmt.Fprintf(&b, "%%%02X", c)
		}
	}
	return b.String()
}

// PeerResolver returns a BaseResolver for a node of shard self, fetching
// delta bases held by other shards (from any of their replicas).
func PeerResolver(fleet [][]string, self int, db *DB) BaseResolver {
	client := &http.Client{Timeout: time.Minute, Transport: &http.Transport{
		MaxIdleConns: 256, MaxIdleConnsPerHost: 64, IdleConnTimeout: 90 * time.Second, DisableCompression: true}}
	return func(prefix object.ID, withChain bool) (*BaseInfo, error) {
		i := ShardOf(prefix, len(fleet))
		if i == self {
			return db.localBase(prefix, withChain)
		}
		chain := "0"
		if withChain {
			chain = "1"
		}
		var lastErr error = object.ErrNotFound
		for _, u := range fleet[i] {
			resp, err := client.Get(u + "/kv/base?chain=" + chain + "&p=" + hex.EncodeToString(prefix[:keyIDLen]))
			if err != nil {
				lastErr = err
				continue
			}
			// Always read to EOF so the connection is reused.
			b, err := io.ReadAll(bufio.NewReader(resp.Body))
			resp.Body.Close()
			if err != nil {
				lastErr = err
				continue
			}
			if resp.StatusCode == http.StatusOK {
				return decodeBaseInfo(b)
			}
			if resp.StatusCode != http.StatusNotFound {
				lastErr = fmt.Errorf("peer %s: %s", u, resp.Status)
			}
		}
		return nil, lastErr
	}
}

var _ Backend = (*Sharded)(nil)
