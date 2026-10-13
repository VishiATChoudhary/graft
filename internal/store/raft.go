package store

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"

	"github.com/VishiATChoudhary/graft/internal/object"
)

// MetaGroup replicates the only mutable state (refs and the changeset log)
// across the replicas of shard 0 with Raft, the role Spanner's consensus
// plays for Scion. Ref compare-and-swap and log appends are Raft commands:
// they commit on a majority and are applied, in the same order, to every
// replica's store. Reads that must be current go through the leader.
type MetaGroup struct {
	r      *raft.Raft
	db     *DB
	byAddr map[raft.ServerAddress]string // raft address -> HTTP URL
}

// NotLeaderError redirects a metadata request to the current leader.
type NotLeaderError struct{ Leader string }

func (e *NotLeaderError) Error() string { return "not the metadata leader (leader: " + e.Leader + ")" }

const (
	cmdCAS    = 1
	cmdAppend = 2
)

type casResult struct {
	current object.ID
	err     error
}

type appendResult struct {
	seq uint64
	err error
}

type metaFSM struct{ db *DB }

func (f *metaFSM) Apply(l *raft.Log) interface{} {
	d := object.Decoder{B: l.Data}
	switch d.Byte() {
	case cmdCAS:
		name := d.String()
		old, new := d.ID(), d.ID()
		err := f.db.CASRef(name, old, new)
		var rm *RefMismatch
		if errors.As(err, &rm) {
			return casResult{rm.Current, err}
		}
		return casResult{new, err}
	case cmdAppend:
		seq, err := f.db.AppendLog(d.ID())
		return appendResult{seq, err}
	}
	return fmt.Errorf("unknown metadata command")
}

// Snapshot captures refs and the log; followers far behind restore it.
func (f *metaFSM) Snapshot() (raft.FSMSnapshot, error) {
	refs, err := f.db.ListRefs()
	if err != nil {
		return nil, err
	}
	n, _ := f.db.LastLogSeq()
	log := make([]object.ID, 0, n)
	for s := uint64(1); s <= n; s++ {
		id, err := f.db.LogID(s)
		if err != nil {
			return nil, err
		}
		log = append(log, id)
	}
	return &metaSnapshot{refs, log}, nil
}

func (f *metaFSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()
	b, err := io.ReadAll(bufio.NewReader(rc))
	if err != nil {
		return err
	}
	d := object.Decoder{B: b}
	nrefs := d.Uvarint()
	refs := map[string]object.ID{}
	for i := uint64(0); i < nrefs && d.Err == nil; i++ {
		name := d.String()
		refs[name] = d.ID()
	}
	nlog := d.Uvarint()
	var log []object.ID
	for i := uint64(0); i < nlog && d.Err == nil; i++ {
		log = append(log, d.ID())
	}
	if d.Err != nil {
		return d.Err
	}
	return f.db.restoreMeta(refs, log)
}

type metaSnapshot struct {
	refs map[string]object.ID
	log  []object.ID
}

func (s *metaSnapshot) Persist(sink raft.SnapshotSink) error {
	var e object.Encoder
	e.Uvarint(uint64(len(s.refs)))
	for name, id := range s.refs {
		e.String(name)
		e.ID(id)
	}
	e.Uvarint(uint64(len(s.log)))
	for _, id := range s.log {
		e.ID(id)
	}
	if _, err := sink.Write(e.B); err != nil {
		sink.Cancel()
		return err
	}
	return sink.Close()
}

func (s *metaSnapshot) Release() {}

// restoreMeta replaces refs and the log with a snapshot's.
func (d *DB) restoreMeta(refs map[string]object.ID, log []object.ID) error {
	cur, err := d.ListRefs()
	if err != nil {
		return err
	}
	b := d.db.NewBatch()
	for name := range cur {
		if _, ok := refs[name]; !ok {
			b.Delete(key(pfxRef, []byte(name)), nil)
		}
	}
	for name, id := range refs {
		b.Set(key(pfxRef, []byte(name)), id[:], nil)
	}
	for i, id := range log {
		seq := uint64(i + 1)
		b.Set(key(pfxLog, be64(seq)), id[:], nil)
		b.Set(key(pfxLogIdx, id[:keyIDLen]), be64(seq), nil)
	}
	if err := b.Commit(pebble.Sync); err != nil {
		return err
	}
	d.seqMu.Lock()
	if uint64(len(log)) > d.seq {
		d.seq = uint64(len(log))
	}
	d.seqMu.Unlock()
	return nil
}

// MetaConfig describes the shard-0 replica group.
type MetaConfig struct {
	Dir      string   // raft state directory
	Self     int      // index of this replica
	RaftAddr []string // raft addresses of all replicas, in replica order
	HTTP     []string // HTTP URLs of all replicas, same order
	// Fast settings for tests and single-machine runs.
	Fast bool
}

// OpenMetaGroup starts (or rejoins) the Raft group for shard 0's metadata.
func OpenMetaGroup(db *DB, cfg MetaConfig) (*MetaGroup, error) {
	if err := os.MkdirAll(cfg.Dir, 0o755); err != nil {
		return nil, err
	}
	conf := raft.DefaultConfig()
	conf.LocalID = raft.ServerID(cfg.RaftAddr[cfg.Self])
	conf.Logger = hclog.NewNullLogger()
	if cfg.Fast {
		conf.HeartbeatTimeout = 300 * time.Millisecond
		conf.ElectionTimeout = 300 * time.Millisecond
		conf.LeaderLeaseTimeout = 150 * time.Millisecond
		conf.CommitTimeout = 5 * time.Millisecond
	}
	logs, err := raftboltdb.New(raftboltdb.Options{Path: filepath.Join(cfg.Dir, "raft.db")})
	if err != nil {
		return nil, err
	}
	snaps, err := raft.NewFileSnapshotStore(cfg.Dir, 2, io.Discard)
	if err != nil {
		return nil, err
	}
	trans, err := raft.NewTCPTransport(cfg.RaftAddr[cfg.Self], nil, 4, 5*time.Second, io.Discard)
	if err != nil {
		return nil, err
	}
	r, err := raft.NewRaft(conf, &metaFSM{db}, logs, logs, snaps, trans)
	if err != nil {
		return nil, err
	}
	if ok, err := raft.HasExistingState(logs, logs, snaps); err == nil && !ok {
		var servers []raft.Server
		for _, a := range cfg.RaftAddr {
			servers = append(servers, raft.Server{ID: raft.ServerID(a), Address: raft.ServerAddress(a)})
		}
		// Identical bootstrap on every replica is safe.
		if err := r.BootstrapCluster(raft.Configuration{Servers: servers}).Error(); err != nil && !errors.Is(err, raft.ErrCantBootstrap) {
			return nil, err
		}
	}
	m := &MetaGroup{r: r, db: db, byAddr: map[raft.ServerAddress]string{}}
	for i, a := range cfg.RaftAddr {
		m.byAddr[raft.ServerAddress(a)] = cfg.HTTP[i]
	}
	return m, nil
}

// Close stops the replica.
func (m *MetaGroup) Close() error { return m.r.Shutdown().Error() }

func (m *MetaGroup) notLeader() error {
	addr, _ := m.r.LeaderWithID()
	return &NotLeaderError{Leader: m.byAddr[addr]}
}

func (m *MetaGroup) apply(cmd []byte) (interface{}, error) {
	if m.r.State() != raft.Leader {
		return nil, m.notLeader()
	}
	f := m.r.Apply(cmd, 10*time.Second)
	if err := f.Error(); err != nil {
		if errors.Is(err, raft.ErrNotLeader) || errors.Is(err, raft.ErrLeadershipLost) {
			return nil, m.notLeader()
		}
		return nil, err
	}
	return f.Response(), nil
}

// CASRef performs a replicated compare-and-swap.
func (m *MetaGroup) CASRef(name string, old, new object.ID) error {
	var e object.Encoder
	e.Byte(cmdCAS)
	e.String(name)
	e.ID(old)
	e.ID(new)
	v, err := m.apply(e.B)
	if err != nil {
		return err
	}
	return v.(casResult).err
}

// AppendLog assigns a replicated log sequence.
func (m *MetaGroup) AppendLog(id object.ID) (uint64, error) {
	var e object.Encoder
	e.Byte(cmdAppend)
	e.ID(id)
	v, err := m.apply(e.B)
	if err != nil {
		return 0, err
	}
	res := v.(appendResult)
	return res.seq, res.err
}

// ReadBarrier makes a linearizable read possible: only the leader, after
// confirming its leadership, may serve current refs.
func (m *MetaGroup) ReadBarrier() error {
	if m.r.State() != raft.Leader {
		return m.notLeader()
	}
	if err := m.r.VerifyLeader().Error(); err != nil {
		return m.notLeader()
	}
	return m.r.Barrier(5 * time.Second).Error()
}

// IsLeader reports whether this replica currently leads.
func (m *MetaGroup) IsLeader() bool { return m.r.State() == raft.Leader }
