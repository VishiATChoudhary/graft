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
	"time"

	"github.com/VishiATChoudhary/graft/internal/object"
)

// Sharded is a Backend over a fleet of storage nodes (NodeHandler). Objects
// and derived data are placed by ID prefix; refs and the changeset log live
// on shard 0. Everything immutable is cached in memory, so API servers using
// Sharded are, as in Scion, essentially large caches that also compute
// derived data and write it back. Any number of API servers can share one
// fleet: the only mutable state (refs, log sequence) is updated atomically
// on its node.
type Sharded struct {
	nodes  []string
	client *http.Client

	hot      *MemLRU // stored objects
	payloads *MemLRU // [kind]+payload
	derived  sync.Map
	dmu      sync.Mutex
	dbytes   int64
	seqs     sync.Map // ID -> seq
	ids      sync.Map // seq -> ID
}

// ShardOf returns the shard index for an ID (by its storage prefix).
func ShardOf(id object.ID, n int) int {
	return int(binary.BigEndian.Uint16(id[:2])) % n
}

// NewSharded connects to storage nodes, in shard order.
func NewSharded(nodes []string, cacheBytes int64) *Sharded {
	tr := &http.Transport{
		MaxIdleConns:        1024,
		MaxIdleConnsPerHost: 256,
		IdleConnTimeout:     90 * time.Second,
		DisableCompression:  true,
		DialContext:         (&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
	}
	for i := range nodes {
		nodes[i] = strings.TrimRight(nodes[i], "/")
	}
	if cacheBytes <= 0 {
		cacheBytes = 1 << 30
	}
	return &Sharded{nodes: nodes, client: &http.Client{Transport: tr, Timeout: 5 * time.Minute},
		hot: NewMemLRU(cacheBytes / 2), payloads: NewMemLRU(cacheBytes / 2)}
}

func (s *Sharded) node(id object.ID) string { return s.nodes[ShardOf(id, len(s.nodes))] }
func (s *Sharded) meta() string             { return s.nodes[0] }

type httpStatusError struct {
	code int
	msg  string
}

func (e *httpStatusError) Error() string { return fmt.Sprintf("storage node: %d %s", e.code, e.msg) }

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
	}
	return nil, resp.StatusCode, &httpStatusError{resp.StatusCode, strings.TrimSpace(string(b))}
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

func (s *Sharded) Get(id object.ID) ([]byte, error) {
	if b, err := s.hot.Get(id); err == nil {
		return b, nil
	}
	body, _, err := s.call("POST", s.node(id)+"/kv/get", id[:])
	if err != nil {
		return nil, err
	}
	var out []byte
	if err := frames(body, func(_ object.ID, d []byte) { out = d }); err != nil {
		return nil, err
	}
	if out == nil {
		return nil, object.ErrNotFound
	}
	s.hot.Put(id, out)
	return out, nil
}

func (s *Sharded) Payload(id object.ID) (object.Kind, []byte, error) {
	if v, err := s.payloads.Get(id); err == nil {
		return object.Kind(v[0]), v[1:], nil
	}
	body, _, err := s.call("POST", s.node(id)+"/kv/payload", id[:])
	if err != nil {
		return 0, nil, err
	}
	var out []byte
	if err := frames(body, func(_ object.ID, d []byte) { out = d }); err != nil {
		return 0, nil, err
	}
	if len(out) == 0 {
		return 0, nil, object.ErrNotFound
	}
	s.payloads.Put(id, out)
	return object.Kind(out[0]), out[1:], nil
}

func (s *Sharded) Has(id object.ID) (bool, error) {
	if ok, _ := s.hot.Has(id); ok {
		return true, nil
	}
	if ok, _ := s.payloads.Has(id); ok {
		return true, nil
	}
	body, _, err := s.call("POST", s.node(id)+"/kv/has", id[:])
	if err != nil {
		return false, err
	}
	return len(body) == 1 && body[0] == 1, nil
}

func (s *Sharded) PayloadSize(id object.ID) (uint64, error) {
	if b, err := s.hot.Get(id); err == nil {
		return object.PayloadSize(b)
	}
	body, _, err := s.call("POST", s.node(id)+"/kv/size", id[:])
	if err != nil {
		return 0, err
	}
	if len(body) != 8 {
		return 0, object.ErrCorrupt
	}
	return binary.BigEndian.Uint64(body), nil
}

func (s *Sharded) Put(id object.ID, stored []byte) error { return s.PutBase(id, stored, object.Zero) }

func (s *Sharded) PutBase(id object.ID, stored []byte, base object.ID) error {
	buf := make([]byte, 68, 68+len(stored))
	copy(buf, id[:])
	copy(buf[32:], base[:])
	binary.BigEndian.PutUint32(buf[64:], uint32(len(stored)))
	buf = append(buf, stored...)
	if _, _, err := s.call("POST", s.node(id)+"/kv/put", buf); err != nil {
		return err
	}
	s.hot.Put(id, stored)
	return nil
}

func (s *Sharded) Deltify(id, base object.ID) error {
	_, _, err := s.call("POST", s.node(id)+"/kv/deltify", append(id[:len(id):len(id)], base[:]...))
	if errors.Is(err, object.ErrNotFound) {
		return nil
	}
	return err
}

type derivedKey struct {
	kind string
	id   object.ID
}

func (s *Sharded) GetDerived(kind string, id object.ID) ([]byte, error) {
	if v, ok := s.derived.Load(derivedKey{kind, id}); ok {
		return v.([]byte), nil
	}
	body, _, err := s.call("POST", s.node(id)+"/kv/derived/get?kind="+kind, id[:])
	if err != nil {
		return nil, err
	}
	var out []byte
	if err := frames(body, func(_ object.ID, d []byte) { out = d }); err != nil {
		return nil, err
	}
	if out == nil {
		return nil, object.ErrNotFound
	}
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

func (s *Sharded) PutDerived(kind string, id object.ID, v []byte) error {
	buf := make([]byte, 36, 36+len(v))
	copy(buf, id[:])
	binary.BigEndian.PutUint32(buf[32:], uint32(len(v)))
	buf = append(buf, v...)
	if _, _, err := s.call("POST", s.node(id)+"/kv/derived/put?kind="+kind, buf); err != nil {
		return err
	}
	s.cacheDerived(kind, id, v)
	return nil
}

func (s *Sharded) GetRef(name string) (object.ID, error) {
	body, _, err := s.call("GET", s.meta()+"/kv/ref?name="+urlEscape(name), nil)
	if err != nil {
		return object.Zero, err
	}
	var id object.ID
	copy(id[:], body)
	return id, nil
}

func (s *Sharded) CASRef(name string, old, new object.ID) error {
	b, _ := json.Marshal(struct {
		Name     string
		Old, New object.ID
	}{name, old, new})
	body, code, err := s.call("POST", s.meta()+"/kv/ref/cas", b)
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
	body, _, err := s.call("GET", s.meta()+"/kv/refs", nil)
	if err != nil {
		return nil, err
	}
	out := map[string]object.ID{}
	return out, json.Unmarshal(body, &out)
}

func (s *Sharded) AppendLog(id object.ID) (uint64, error) {
	body, _, err := s.call("POST", s.meta()+"/kv/log/append", id[:])
	if err != nil {
		return 0, err
	}
	if len(body) != 8 {
		return 0, object.ErrCorrupt
	}
	seq := binary.BigEndian.Uint64(body)
	s.seqs.Store(id, seq)
	s.ids.Store(seq, id)
	return seq, nil
}

func (s *Sharded) LogSeq(id object.ID) (uint64, error) {
	if v, ok := s.seqs.Load(id); ok {
		return v.(uint64), nil
	}
	body, _, err := s.call("POST", s.meta()+"/kv/log/seq", id[:])
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
	body, _, err := s.call("GET", s.meta()+"/kv/log/id?seq="+strconv.FormatUint(seq, 10), nil)
	if err != nil {
		return object.Zero, err
	}
	var id object.ID
	copy(id[:], body)
	s.ids.Store(seq, id)
	return id, nil
}

func (s *Sharded) LastLogSeq() (uint64, error) {
	body, _, err := s.call("GET", s.meta()+"/kv/log/last", nil)
	if err != nil {
		return 0, err
	}
	if len(body) != 8 {
		return 0, object.ErrCorrupt
	}
	return binary.BigEndian.Uint64(body), nil
}

func (s *Sharded) ScanLogIdxPrefix(prefix []byte, fn func(object.ID) bool) error {
	body, _, err := s.call("GET", s.meta()+"/kv/log/prefix?p="+hex.EncodeToString(prefix), nil)
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
	out := map[string]any{"shards": len(s.nodes)}
	for i, n := range s.nodes {
		body, _, err := s.call("GET", n+"/kv/stats", nil)
		if err == nil {
			var m map[string]any
			json.Unmarshal(body, &m)
			out["shard"+strconv.Itoa(i)] = m
		}
	}
	h, m, b := s.hot.Stats()
	out["api_cache"] = map[string]any{"hits": h, "misses": m, "bytes": b}
	return out
}

func (s *Sharded) Flush() error {
	for _, n := range s.nodes {
		if _, _, err := s.call("POST", n+"/kv/flush", nil); err != nil {
			return err
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

// PeerResolver returns a BaseResolver for shard self of a fleet, fetching
// delta bases held by other shards.
func PeerResolver(nodes []string, self int, db *DB) BaseResolver {
	client := &http.Client{Timeout: time.Minute, Transport: &http.Transport{
		MaxIdleConns: 256, MaxIdleConnsPerHost: 64, IdleConnTimeout: 90 * time.Second, DisableCompression: true}}
	return func(prefix object.ID, withChain bool) (*BaseInfo, error) {
		i := ShardOf(prefix, len(nodes))
		if i == self {
			return db.localBase(prefix, withChain)
		}
		chain := "0"
		if withChain {
			chain = "1"
		}
		resp, err := client.Get(strings.TrimRight(nodes[i], "/") + "/kv/base?chain=" + chain + "&p=" + hex.EncodeToString(prefix[:keyIDLen]))
		if err != nil {
			return nil, err
		}
		defer resp.Body.Close()
		// Always read to EOF so the connection is reused.
		b, err := io.ReadAll(bufio.NewReader(resp.Body))
		if err != nil {
			return nil, err
		}
		if resp.StatusCode == http.StatusNotFound {
			return nil, object.ErrNotFound
		}
		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("peer %d: %s", i, resp.Status)
		}
		return decodeBaseInfo(b)
	}
}

var _ Backend = (*Sharded)(nil)
