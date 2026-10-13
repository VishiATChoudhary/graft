package store

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"

	"github.com/VishiATChoudhary/graft/internal/object"
)

// NodeHandler exposes a DB as one storage shard. A fleet of shards plus any
// number of stateless API servers (using Sharded) replaces a single-process
// server: objects and derived data are spread across shards by ID prefix;
// refs and the changeset log live on shard 0, where ref compare-and-swap is
// performed atomically on one row.
//
// Wire formats are compact binary: IDs are 32 raw bytes, integers are
// big-endian, and object payloads are length-prefixed frames.
func NodeHandler(db *DB) http.Handler {
	mux := http.NewServeMux()
	ids := func(w http.ResponseWriter, r *http.Request) ([]object.ID, bool) {
		b, err := io.ReadAll(io.LimitReader(r.Body, 1<<26))
		if err != nil || len(b)%32 != 0 {
			http.Error(w, "body must be 32-byte ids", http.StatusBadRequest)
			return nil, false
		}
		out := make([]object.ID, len(b)/32)
		for i := range out {
			copy(out[i][:], b[i*32:])
		}
		return out, true
	}
	fail := func(w http.ResponseWriter, err error) {
		if errors.Is(err, object.ErrNotFound) {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
	frame := func(bw *bufio.Writer, id object.ID, b []byte, missing bool) {
		var hdr [36]byte
		copy(hdr[:], id[:])
		n := uint32(len(b))
		if missing {
			n = 0xFFFFFFFF
		}
		binary.BigEndian.PutUint32(hdr[32:], n)
		bw.Write(hdr[:])
		if !missing {
			bw.Write(b)
		}
	}
	mux.HandleFunc("/kv/get", func(w http.ResponseWriter, r *http.Request) {
		list, ok := ids(w, r)
		if !ok {
			return
		}
		bw := bufio.NewWriterSize(w, 64<<10)
		for _, id := range list {
			b, err := db.Get(id)
			if err != nil && !errors.Is(err, object.ErrNotFound) {
				fail(w, err)
				return
			}
			frame(bw, id, b, err != nil)
		}
		bw.Flush()
	})
	mux.HandleFunc("/kv/payload", func(w http.ResponseWriter, r *http.Request) {
		list, ok := ids(w, r)
		if !ok {
			return
		}
		bw := bufio.NewWriterSize(w, 64<<10)
		for _, id := range list {
			k, p, err := db.Payload(id)
			if err != nil && !errors.Is(err, object.ErrNotFound) {
				fail(w, err)
				return
			}
			frame(bw, id, append([]byte{byte(k)}, p...), err != nil)
		}
		bw.Flush()
	})
	mux.HandleFunc("/kv/has", func(w http.ResponseWriter, r *http.Request) {
		list, ok := ids(w, r)
		if !ok {
			return
		}
		out := make([]byte, len(list))
		for i, id := range list {
			has, err := db.Has(id)
			if err != nil {
				fail(w, err)
				return
			}
			if has {
				out[i] = 1
			}
		}
		w.Write(out)
	})
	mux.HandleFunc("/kv/size", func(w http.ResponseWriter, r *http.Request) {
		list, ok := ids(w, r)
		if !ok {
			return
		}
		out := make([]byte, 0, 8*len(list))
		for _, id := range list {
			n, err := db.PayloadSize(id)
			if err != nil {
				fail(w, err)
				return
			}
			out = binary.BigEndian.AppendUint64(out, n)
		}
		w.Write(out)
	})
	// put: frames of [id][base][u32 len][stored].
	mux.HandleFunc("/kv/put", func(w http.ResponseWriter, r *http.Request) {
		br := bufio.NewReaderSize(r.Body, 64<<10)
		var hdr [68]byte
		for {
			if _, err := io.ReadFull(br, hdr[:]); err != nil {
				if err == io.EOF {
					break
				}
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			var id, base object.ID
			copy(id[:], hdr[:32])
			copy(base[:], hdr[32:64])
			b := make([]byte, binary.BigEndian.Uint32(hdr[64:]))
			if _, err := io.ReadFull(br, b); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			if err := db.PutBase(id, b, base); err != nil {
				fail(w, err)
				return
			}
		}
	})
	mux.HandleFunc("/kv/deltify", func(w http.ResponseWriter, r *http.Request) {
		list, ok := ids(w, r)
		if !ok || len(list)%2 != 0 {
			return
		}
		for i := 0; i < len(list); i += 2 {
			if err := db.Deltify(list[i], list[i+1]); err != nil && !errors.Is(err, object.ErrNotFound) {
				fail(w, err)
				return
			}
		}
	})
	// base: delta-base resolution for peer shards, by 16-byte prefix.
	mux.HandleFunc("/kv/base", func(w http.ResponseWriter, r *http.Request) {
		p, err := hex.DecodeString(r.URL.Query().Get("p"))
		if err != nil || len(p) != keyIDLen {
			http.Error(w, "bad prefix", http.StatusBadRequest)
			return
		}
		var id object.ID
		copy(id[:], p)
		info, err := db.localBase(id, r.URL.Query().Get("chain") == "1")
		if err != nil {
			fail(w, err)
			return
		}
		w.Write(info.encode())
	})
	mux.HandleFunc("/kv/derived/get", func(w http.ResponseWriter, r *http.Request) {
		kind := r.URL.Query().Get("kind")
		list, ok := ids(w, r)
		if !ok {
			return
		}
		bw := bufio.NewWriterSize(w, 64<<10)
		for _, id := range list {
			v, err := db.GetDerived(kind, id)
			if err != nil && !errors.Is(err, object.ErrNotFound) {
				fail(w, err)
				return
			}
			frame(bw, id, v, err != nil)
		}
		bw.Flush()
	})
	mux.HandleFunc("/kv/derived/put", func(w http.ResponseWriter, r *http.Request) {
		kind := r.URL.Query().Get("kind")
		br := bufio.NewReader(r.Body)
		var hdr [36]byte
		for {
			if _, err := io.ReadFull(br, hdr[:]); err != nil {
				if err == io.EOF {
					break
				}
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			var id object.ID
			copy(id[:], hdr[:32])
			v := make([]byte, binary.BigEndian.Uint32(hdr[32:]))
			if _, err := io.ReadFull(br, v); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			if err := db.PutDerived(kind, id, v); err != nil {
				fail(w, err)
				return
			}
		}
	})
	mux.HandleFunc("/kv/ref", func(w http.ResponseWriter, r *http.Request) {
		id, err := db.GetRef(r.URL.Query().Get("name"))
		if err != nil {
			fail(w, err)
			return
		}
		w.Write(id[:])
	})
	mux.HandleFunc("/kv/ref/cas", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Name     string
			Old, New object.ID
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		err := db.CASRef(req.Name, req.Old, req.New)
		var rm *RefMismatch
		if errors.As(err, &rm) {
			w.WriteHeader(http.StatusConflict)
			w.Write(rm.Current[:])
			return
		}
		if err != nil {
			fail(w, err)
		}
	})
	mux.HandleFunc("/kv/refs", func(w http.ResponseWriter, r *http.Request) {
		refs, err := db.ListRefs()
		if err != nil {
			fail(w, err)
			return
		}
		json.NewEncoder(w).Encode(refs)
	})
	mux.HandleFunc("/kv/log/append", func(w http.ResponseWriter, r *http.Request) {
		list, ok := ids(w, r)
		if !ok || len(list) != 1 {
			return
		}
		seq, err := db.AppendLog(list[0])
		if err != nil {
			fail(w, err)
			return
		}
		w.Write(be64(seq))
	})
	mux.HandleFunc("/kv/log/seq", func(w http.ResponseWriter, r *http.Request) {
		list, ok := ids(w, r)
		if !ok {
			return
		}
		out := make([]byte, 0, 8*len(list))
		for _, id := range list {
			s, err := db.LogSeq(id)
			if errors.Is(err, object.ErrNotFound) {
				s = 0
			} else if err != nil {
				fail(w, err)
				return
			}
			out = binary.BigEndian.AppendUint64(out, s)
		}
		w.Write(out)
	})
	mux.HandleFunc("/kv/log/id", func(w http.ResponseWriter, r *http.Request) {
		seq, _ := strconv.ParseUint(r.URL.Query().Get("seq"), 10, 64)
		id, err := db.LogID(seq)
		if err != nil {
			fail(w, err)
			return
		}
		w.Write(id[:])
	})
	mux.HandleFunc("/kv/log/last", func(w http.ResponseWriter, r *http.Request) {
		s, _ := db.LastLogSeq()
		w.Write(be64(s))
	})
	mux.HandleFunc("/kv/log/prefix", func(w http.ResponseWriter, r *http.Request) {
		p, err := hex.DecodeString(r.URL.Query().Get("p"))
		if err != nil {
			http.Error(w, "bad prefix", http.StatusBadRequest)
			return
		}
		var out bytes.Buffer
		db.ScanLogIdxPrefix(p, func(id object.ID) bool {
			out.Write(id[:])
			return out.Len() < 32*16
		})
		w.Write(out.Bytes())
	})
	mux.HandleFunc("/kv/stats", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(db.Stats())
	})
	mux.HandleFunc("/kv/flush", func(w http.ResponseWriter, r *http.Request) {
		if err := db.Flush(); err != nil {
			fail(w, err)
		}
	})
	return mux
}
