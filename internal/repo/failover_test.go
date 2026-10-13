package repo

import (
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/VishiATChoudhary/graft/internal/object"
	"github.com/VishiATChoudhary/graft/internal/store"
)

type replica struct {
	srv  *httptest.Server
	db   *store.DB
	meta *store.MetaGroup
	dead bool
}

func (r *replica) kill() {
	r.dead = true
	r.srv.CloseClientConnections()
	r.srv.Close()
	if r.meta != nil {
		r.meta.Close()
	}
	r.db.Close()
}

func freeAddr(t *testing.T) string {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().String()
}

// replicatedFleet starts shards x replicas storage nodes; shard 0's
// replicas run Raft for refs and the log.
func replicatedFleet(t *testing.T, shards, replicas int) ([][]*replica, []string) {
	t.Helper()
	nodes := make([][]*replica, shards)
	urls := make([][]string, shards)
	for s := range nodes {
		for r := 0; r < replicas; r++ {
			srv := httptest.NewUnstartedServer(nil)
			nodes[s] = append(nodes[s], &replica{srv: srv})
			urls[s] = append(urls[s], "http://"+srv.Listener.Addr().String())
		}
	}
	var spec []string
	for _, u := range urls {
		spec = append(spec, strings.Join(u, "|"))
	}
	var raftAddrs []string
	for r := 0; r < replicas; r++ {
		raftAddrs = append(raftAddrs, freeAddr(t))
	}
	for s := range nodes {
		for r, n := range nodes[s] {
			db, err := store.OpenDB(t.TempDir(), 16<<20)
			if err != nil {
				t.Fatal(err)
			}
			n.db = db
			if s == 0 {
				if n.meta, err = store.OpenMetaGroup(db, store.MetaConfig{Dir: t.TempDir(), Self: r, RaftAddr: raftAddrs, HTTP: urls[0], Fast: true}); err != nil {
					t.Fatal(err)
				}
			}
			db.SetPeers(store.PeerResolver(urls, s, db))
			n.srv.Config.Handler = store.NodeHandler(db, n.meta)
			n.srv.Start()
		}
	}
	t.Cleanup(func() {
		for _, sh := range nodes {
			for _, n := range sh {
				if !n.dead {
					n.kill()
				}
			}
		}
	})
	return nodes, spec
}

func TestReplicatedFailover(t *testing.T) {
	nodes, spec := replicatedFleet(t, 2, 3)
	r, err := Open(store.NewSharded(spec, 16<<20))
	if err != nil {
		t.Fatal(err)
	}
	base := commit(t, r, nil, "base", map[string]string{"README": "hi\n"})
	if err := r.UpdateRef("main", object.Zero, base); err != nil {
		t.Fatal(err)
	}
	land := func(from, n int) {
		var wg sync.WaitGroup
		errs := make(chan error, n)
		for i := from; i < from+n; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				id := commit(t, r, []object.ID{base}, fmt.Sprint("c", i), map[string]string{fmt.Sprintf("f/%d.txt", i): fmt.Sprint(i, "\n")})
				if _, err := r.Land("main", id, "t", 1, 0); err != nil {
					errs <- err
				}
			}(i)
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			t.Fatal(err)
		}
	}
	land(0, 12)

	// Kill the metadata leader and one replica of the data shard.
	var killed bool
	deadline := time.Now().Add(5 * time.Second)
	for !killed && time.Now().Before(deadline) {
		for _, n := range nodes[0] {
			if n.meta.IsLeader() {
				n.kill()
				killed = true
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !killed {
		t.Fatal("no metadata leader")
	}
	nodes[1][2].kill()

	land(12, 12)
	tip, err := r.Ref("main")
	if err != nil {
		t.Fatal(err)
	}
	// A brand-new API server (no caches) sees everything.
	fresh, _ := Open(store.NewSharded(spec, 16<<20))
	if got, _ := fresh.Ref("main"); got != tip {
		t.Fatal("API servers disagree after failover")
	}
	for i := 0; i < 24; i++ {
		if got := read(t, fresh, tip, fmt.Sprintf("f/%d.txt", i)); got != fmt.Sprint(i, "\n") {
			t.Fatalf("change %d lost: %q", i, got)
		}
	}
	g, _ := fresh.Graph(tip)
	if g.Depth != 24 {
		t.Fatalf("depth %d want 24 (linear, nothing lost)", g.Depth)
	}
	if pl, err := fresh.PathLog(tip, "f", 0, false); err != nil || len(pl) != 24 {
		t.Fatalf("history after failover: %d %v", len(pl), err)
	}
	var _ = http.StatusOK
}
