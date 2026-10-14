package store

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"time"

	"cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	databasepb "cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"

	"github.com/VishiATChoudhary/graft/internal/object"
)

// Spanner is a Backend on Google Cloud Spanner, the store Scion's servers
// read from. Every API server talks to the database directly: objects and
// derived data are rows keyed by ID, refs move with read-write transactions
// (compare-and-swap on a single row), and log sequence numbers are assigned
// transactionally. Spanner provides replication, failover and horizontal
// scaling; API servers stay stateless caches in front of it.
type Spanner struct {
	c        *spanner.Client
	hot      *MemLRU
	payloads *MemLRU
	derived  *MemLRU
	seqs     atomic.Pointer[map[object.ID]uint64]
}

// SpannerSchema is the DDL graft needs.
var SpannerSchema = []string{
	`CREATE TABLE Objects (Id BYTES(16) NOT NULL, Data BYTES(MAX) NOT NULL) PRIMARY KEY (Id)`,
	`CREATE TABLE Derived (Kind STRING(64) NOT NULL, Id BYTES(16) NOT NULL, Value BYTES(MAX) NOT NULL) PRIMARY KEY (Kind, Id)`,
	`CREATE TABLE Refs (Name STRING(1024) NOT NULL, Id BYTES(32) NOT NULL) PRIMARY KEY (Name)`,
	// Log rows are keyed by the bit-reversed sequence so appends spread
	// across splits instead of hot-spotting the end of the key space.
	`CREATE TABLE Log (Key INT64 NOT NULL, Seq INT64 NOT NULL, Id BYTES(32) NOT NULL) PRIMARY KEY (Key)`,
	`CREATE TABLE LogIdx (Id BYTES(16) NOT NULL, Seq INT64 NOT NULL) PRIMARY KEY (Id)`,
	`CREATE TABLE Counters (Name STRING(64) NOT NULL, Value INT64 NOT NULL) PRIMARY KEY (Name)`,
}

func logKey(seq uint64) int64 {
	var r uint64
	for i := 0; i < 63; i++ {
		r = r<<1 | (seq>>i)&1
	}
	return int64(r)
}

// CreateSpannerSchema creates graft's tables in an existing database.
func CreateSpannerSchema(ctx context.Context, db string) error {
	admin, err := database.NewDatabaseAdminClient(ctx)
	if err != nil {
		return err
	}
	defer admin.Close()
	op, err := admin.UpdateDatabaseDdl(ctx, &databasepb.UpdateDatabaseDdlRequest{Database: db, Statements: SpannerSchema})
	if err != nil {
		return err
	}
	return op.Wait(ctx)
}

// OpenSpanner connects to a database ("projects/P/instances/I/databases/D").
func OpenSpanner(ctx context.Context, db string, cacheBytes int64) (*Spanner, error) {
	c, err := spanner.NewClient(ctx, db)
	if err != nil {
		return nil, err
	}
	if cacheBytes <= 0 {
		cacheBytes = 1 << 30
	}
	return &Spanner{c: c, hot: NewMemLRU(cacheBytes / 2), payloads: NewMemLRU(cacheBytes / 4), derived: NewMemLRU(cacheBytes / 4)}, nil
}

func notFound(err error) bool { return spanner.ErrCode(err) == codes.NotFound }

func (s *Spanner) ctx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), time.Minute)
}

func (s *Spanner) Get(id object.ID) ([]byte, error) {
	if b, err := s.hot.Get(id); err == nil {
		return b, nil
	}
	ctx, cancel := s.ctx()
	defer cancel()
	row, err := s.c.Single().ReadRow(ctx, "Objects", spanner.Key{id[:keyIDLen]}, []string{"Data"})
	if notFound(err) {
		return nil, object.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	var b []byte
	if err := row.Columns(&b); err != nil {
		return nil, err
	}
	s.hot.Put(id, b)
	return b, nil
}

func (s *Spanner) Has(id object.ID) (bool, error) {
	if ok, _ := s.hot.Has(id); ok {
		return true, nil
	}
	_, err := s.Get(id)
	if errors.Is(err, object.ErrNotFound) {
		return false, nil
	}
	return err == nil, err
}

func (s *Spanner) Put(id object.ID, stored []byte) error {
	ctx, cancel := s.ctx()
	defer cancel()
	_, err := s.c.Apply(ctx, []*spanner.Mutation{spanner.InsertOrUpdate("Objects", []string{"Id", "Data"}, []any{id[:keyIDLen], stored})})
	if err == nil {
		s.hot.Put(id, stored)
	}
	return err
}

// PutBase stores the standard form; Spanner storage is not delta-encoded.
func (s *Spanner) PutBase(id object.ID, stored []byte, _ object.ID) error { return s.Put(id, stored) }

func (s *Spanner) Deltify(object.ID, object.ID) error { return nil }

func (s *Spanner) Payload(id object.ID) (object.Kind, []byte, error) {
	if v, err := s.payloads.Get(id); err == nil {
		return object.Kind(v[0]), v[1:], nil
	}
	b, err := s.Get(id)
	if err != nil {
		return 0, nil, err
	}
	k, p, err := object.Unpack(b)
	if err == nil {
		s.payloads.Put(id, append([]byte{byte(k)}, p...))
	}
	return k, p, err
}

func (s *Spanner) PayloadSize(id object.ID) (uint64, error) {
	b, err := s.Get(id)
	if err != nil {
		return 0, err
	}
	return object.PayloadSize(b)
}

func derivedCacheKey(kind string, id object.ID) object.ID {
	return versionKeyBytes([]byte(kind), id[:])
}

func versionKeyBytes(a, b []byte) object.ID { return object.Hash(object.KindBlob, append(append([]byte{}, a...), b...)) }

func (s *Spanner) GetDerived(kind string, id object.ID) ([]byte, error) {
	ck := derivedCacheKey(kind, id)
	if v, err := s.derived.Get(ck); err == nil {
		return v, nil
	}
	ctx, cancel := s.ctx()
	defer cancel()
	row, err := s.c.Single().ReadRow(ctx, "Derived", spanner.Key{kind, id[:keyIDLen]}, []string{"Value"})
	if notFound(err) {
		return nil, object.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	var v []byte
	if err := row.Columns(&v); err != nil {
		return nil, err
	}
	s.derived.Put(ck, v)
	return v, nil
}

func (s *Spanner) PutDerived(kind string, id object.ID, v []byte) error {
	ctx, cancel := s.ctx()
	defer cancel()
	_, err := s.c.Apply(ctx, []*spanner.Mutation{spanner.InsertOrUpdate("Derived", []string{"Kind", "Id", "Value"}, []any{kind, id[:keyIDLen], v})})
	if err == nil {
		s.derived.Put(derivedCacheKey(kind, id), v)
	}
	return err
}

func (s *Spanner) GetRef(name string) (object.ID, error) {
	ctx, cancel := s.ctx()
	defer cancel()
	row, err := s.c.Single().ReadRow(ctx, "Refs", spanner.Key{name}, []string{"Id"})
	if notFound(err) {
		return object.Zero, object.ErrNotFound
	}
	if err != nil {
		return object.Zero, err
	}
	var b []byte
	if err := row.Columns(&b); err != nil {
		return object.Zero, err
	}
	var id object.ID
	copy(id[:], b)
	return id, nil
}

// CASRef is a compare-and-swap on one row: a blind insert when the ref
// must be absent (fails if it exists), otherwise a conditional DML update
// or delete matching the expected value. Each is a single atomic statement.
func (s *Spanner) CASRef(name string, old, new object.ID) error {
	ctx, cancel := s.ctx()
	defer cancel()
	mismatch := func() error {
		cur, err := s.GetRef(name)
		if err != nil && !errors.Is(err, object.ErrNotFound) {
			return err
		}
		return &RefMismatch{Name: name, Current: cur, Expected: old}
	}
	switch {
	case old.IsZero() && new.IsZero():
		if _, err := s.GetRef(name); errors.Is(err, object.ErrNotFound) {
			return nil
		}
		return mismatch()
	case old.IsZero():
		_, err := s.c.Apply(ctx, []*spanner.Mutation{spanner.Insert("Refs", []string{"Name", "Id"}, []any{name, new[:]})})
		if spanner.ErrCode(err) == codes.AlreadyExists {
			return mismatch()
		}
		return err
	}
	stmt := spanner.Statement{SQL: "UPDATE Refs SET Id = @new WHERE Name = @name AND Id = @old",
		Params: map[string]any{"name": name, "old": old[:], "new": new[:]}}
	if new.IsZero() {
		stmt = spanner.Statement{SQL: "DELETE FROM Refs WHERE Name = @name AND Id = @old",
			Params: map[string]any{"name": name, "old": old[:]}}
	}
	var n int64
	_, err := s.c.ReadWriteTransaction(ctx, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
		var err error
		n, err = tx.Update(ctx, stmt)
		return err
	})
	if err != nil {
		return err
	}
	if n == 0 {
		return mismatch()
	}
	return nil
}

func (s *Spanner) ListRefs() (map[string]object.ID, error) {
	ctx, cancel := s.ctx()
	defer cancel()
	out := map[string]object.ID{}
	err := s.c.Single().Read(ctx, "Refs", spanner.AllKeys(), []string{"Name", "Id"}).Do(func(r *spanner.Row) error {
		var name string
		var b []byte
		if err := r.Columns(&name, &b); err != nil {
			return err
		}
		var id object.ID
		copy(id[:], b)
		out[name] = id
		return nil
	})
	return out, err
}

// AppendLog assigns the next sequence number transactionally (idempotent).
func (s *Spanner) AppendLog(id object.ID) (uint64, error) {
	for {
		seq, err := s.appendLog(id)
		// A concurrent append that took the same sequence surfaces as a
		// conflict on insert; Spanner normally aborts and retries the
		// transaction itself, but retry here too.
		if spanner.ErrCode(err) == codes.AlreadyExists {
			continue
		}
		return seq, err
	}
}

func (s *Spanner) appendLog(id object.ID) (uint64, error) {
	ctx, cancel := s.ctx()
	defer cancel()
	var seq int64
	_, err := s.c.ReadWriteTransaction(ctx, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
		if row, err := tx.ReadRow(ctx, "LogIdx", spanner.Key{id[:keyIDLen]}, []string{"Seq"}); err == nil {
			return row.Columns(&seq)
		} else if !notFound(err) {
			return err
		}
		var last int64
		if row, err := tx.ReadRow(ctx, "Counters", spanner.Key{"log"}, []string{"Value"}); err == nil {
			if err := row.Columns(&last); err != nil {
				return err
			}
		} else if !notFound(err) {
			return err
		}
		seq = last + 1
		return tx.BufferWrite([]*spanner.Mutation{
			spanner.InsertOrUpdate("Counters", []string{"Name", "Value"}, []any{"log", seq}),
			spanner.Insert("Log", []string{"Key", "Seq", "Id"}, []any{logKey(uint64(seq)), seq, id[:]}),
			spanner.Insert("LogIdx", []string{"Id", "Seq"}, []any{id[:keyIDLen], seq}),
		})
	})
	return uint64(seq), err
}

func (s *Spanner) LogSeq(id object.ID) (uint64, error) {
	ctx, cancel := s.ctx()
	defer cancel()
	row, err := s.c.Single().ReadRow(ctx, "LogIdx", spanner.Key{id[:keyIDLen]}, []string{"Seq"})
	if notFound(err) {
		return 0, object.ErrNotFound
	}
	if err != nil {
		return 0, err
	}
	var seq int64
	return uint64(seq), row.Columns(&seq)
}

func (s *Spanner) LogID(seq uint64) (object.ID, error) {
	ctx, cancel := s.ctx()
	defer cancel()
	row, err := s.c.Single().ReadRow(ctx, "Log", spanner.Key{logKey(seq)}, []string{"Id"})
	if notFound(err) {
		return object.Zero, object.ErrNotFound
	}
	if err != nil {
		return object.Zero, err
	}
	var b []byte
	if err := row.Columns(&b); err != nil {
		return object.Zero, err
	}
	var id object.ID
	copy(id[:], b)
	return id, nil
}

func (s *Spanner) LastLogSeq() (uint64, error) {
	ctx, cancel := s.ctx()
	defer cancel()
	row, err := s.c.Single().ReadRow(ctx, "Counters", spanner.Key{"log"}, []string{"Value"})
	if notFound(err) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	var v int64
	return uint64(v), row.Columns(&v)
}

func (s *Spanner) ScanLogIdxPrefix(prefix []byte, fn func(object.ID) bool) error {
	if len(prefix) > keyIDLen {
		prefix = prefix[:keyIDLen]
	}
	end := append(append([]byte{}, prefix...), []byte(strings.Repeat("\xff", keyIDLen-len(prefix)))...)
	ctx, cancel := s.ctx()
	defer cancel()
	iter := s.c.Single().Read(ctx, "LogIdx", spanner.KeyRange{Start: spanner.Key{prefix}, End: spanner.Key{end}, Kind: spanner.ClosedClosed}, []string{"Seq"})
	defer iter.Stop()
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			return nil
		}
		if err != nil {
			return err
		}
		var seq int64
		if err := row.Columns(&seq); err != nil {
			return err
		}
		id, err := s.LogID(uint64(seq))
		if err != nil {
			return err
		}
		if !fn(id) {
			return nil
		}
	}
}

func (s *Spanner) Stats() map[string]any {
	h, m, b := s.hot.Stats()
	return map[string]any{"backend": "spanner", "api_cache": map[string]any{"hits": h, "misses": m, "bytes": b}}
}

func (s *Spanner) Flush() error { return nil }
func (s *Spanner) Close() error { s.c.Close(); return nil }

var _ Backend = (*Spanner)(nil)

