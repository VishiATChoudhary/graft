package main

import (
	"bufio"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/VishiATChoudhary/graft/internal/api"
	"github.com/VishiATChoudhary/graft/internal/fetch"
	"github.com/VishiATChoudhary/graft/internal/metrics"
	"github.com/VishiATChoudhary/graft/internal/object"
	"github.com/VishiATChoudhary/graft/internal/repo"
	"github.com/VishiATChoudhary/graft/internal/store"
)

func cmdServe(args []string) error {
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	tier := fs.String("tier", "server", "server | cache | daemon | store")
	storeNodes := fs.String("store", "", "server: comma-separated storage nodes (stateless API server); omit to use --data")
	shard := fs.Int("shard", 0, "store: this node's index in --nodes")
	nodes := fs.String("nodes", "", "store: comma-separated URLs of every storage node, in shard order")
	listen := fs.String("listen", "127.0.0.1:7070", "listen address (host:port, or unix:///path.sock)")
	data := fs.String("data", "", "server: data directory")
	upstream := fs.String("upstream", "", "cache/daemon: the single upstream tier (strict tiering)")
	mem := fs.String("mem", "1G", "cache: in-memory budget; server: hot cache")
	disk := fs.String("disk", "20G", "daemon: on-disk cache budget")
	dir := fs.String("dir", "", "daemon: cache directory (default ~/.cache/gft/daemon)")
	seed := fs.String("seed", "", "daemon: snapshot file to preload")
	parse(fs, args)

	mux := http.NewServeMux()
	var closeFn func()
	switch *tier {
	case "server":
		var backend store.Backend
		switch {
		case *storeNodes != "":
			// Stateless API server over a storage fleet; run as many as needed.
			backend = store.NewSharded(strings.Split(*storeNodes, ","), mustSize(*mem))
		case *data != "":
			db, err := store.OpenDB(*data, mustSize(*mem))
			if err != nil {
				return err
			}
			closeFn = func() { db.Close() }
			backend = db
		default:
			return errors.New("the server tier needs --data (single node) or --store (storage fleet)")
		}
		r, err := repo.Open(backend)
		if err != nil {
			return err
		}
		(&api.Server{Repo: r, Log: log.Default()}).Routes(mux)
	case "store":
		if *data == "" || *nodes == "" {
			return errors.New("the store tier needs --data and --nodes (and --shard)")
		}
		urls := strings.Split(*nodes, ",")
		if *shard < 0 || *shard >= len(urls) {
			return fmt.Errorf("--shard %d out of range for %d nodes", *shard, len(urls))
		}
		db, err := store.OpenDB(*data, mustSize(*mem))
		if err != nil {
			return err
		}
		closeFn = func() { db.Close() }
		db.SetPeers(store.PeerResolver(urls, *shard, db))
		mux.Handle("/kv/", store.NodeHandler(db))
		mux.HandleFunc("/v1/info", func(w http.ResponseWriter, _ *http.Request) {
			json.NewEncoder(w).Encode(map[string]any{"tier": "store", "shard": *shard, "shards": len(urls),
				"cpu_seconds": metrics.CPUSeconds(), "max_rss": metrics.MaxRSS(), "store": db.Stats()})
		})
	case "cache":
		if *upstream == "" {
			return errors.New("--upstream is required for the cache tier")
		}
		lru := store.NewMemLRU(mustSize(*mem))
		t := fetch.NewTier(lru, fetch.NewRemote(*upstream))
		mux.HandleFunc("/v1/objects", fetch.Handler(t))
		mux.HandleFunc("/v1/info", func(w http.ResponseWriter, _ *http.Request) {
			h, m, b := lru.Stats()
			json.NewEncoder(w).Encode(map[string]any{"tier": "cache", "hits": h, "misses": m, "bytes": b, "hit_rate": rate(h, m), "cpu_seconds": metrics.CPUSeconds(), "max_rss": metrics.MaxRSS()})
		})
	case "daemon":
		if *upstream == "" {
			return errors.New("--upstream is required for the daemon tier")
		}
		d := *dir
		if d == "" {
			home, _ := os.UserHomeDir()
			d = filepath.Join(home, ".cache", "gft", "daemon")
		}
		dc, err := store.OpenDiskCache(d, mustSize(*disk))
		if err != nil {
			return err
		}
		if *seed != "" {
			n, err := loadSnapshot(*seed, dc)
			if err != nil {
				return fmt.Errorf("seed: %w", err)
			}
			log.Printf("daemon: seeded %d objects from %s", n, *seed)
		}
		t := fetch.NewTier(dc, fetch.NewRemote(*upstream))
		mux.HandleFunc("/v1/objects", fetch.Handler(t))
		mux.HandleFunc("/v1/info", func(w http.ResponseWriter, _ *http.Request) {
			h, m, b, n := dc.Stats()
			json.NewEncoder(w).Encode(map[string]any{"tier": "daemon", "hits": h, "misses": m, "bytes": b, "objects": n, "hit_rate": rate(h, m), "cpu_seconds": metrics.CPUSeconds(), "max_rss": metrics.MaxRSS()})
		})
	default:
		return fmt.Errorf("unknown tier %q", *tier)
	}

	ln, err := listenOn(*listen)
	if err != nil {
		return err
	}
	srv := &http.Server{Handler: mux, ReadHeaderTimeout: 30 * time.Second}
	log.Printf("gft %s tier listening on %s", *tier, *listen)
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-stop
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		srv.Shutdown(ctx)
	}()
	err = srv.Serve(ln)
	if closeFn != nil {
		closeFn()
	}
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

func rate(h, m uint64) float64 {
	if h+m == 0 {
		return 0
	}
	return float64(h) / float64(h+m)
}

func listenOn(addr string) (net.Listener, error) {
	if sock, ok := cutPrefix(addr, "unix://"); ok {
		os.Remove(sock)
		ln, err := net.Listen("unix", sock)
		if err != nil {
			return nil, err
		}
		os.Chmod(sock, 0o666)
		return ln, nil
	}
	return net.Listen("tcp", addr)
}

// Snapshots: a flat file of object frames ([id][u32 len][stored]) used to
// seed node daemons, refreshed periodically from a recent revision.

func cmdSnapshot(args []string) error {
	fs := flag.NewFlagSet("snapshot", flag.ExitOnError)
	server := fs.String("server", envOr("GFT_SERVER", "http://127.0.0.1:7070"), "server URL")
	rev := fs.String("rev", "main", "revision to snapshot")
	out := fs.String("out", "snapshot.gfts", "output file")
	parse(fs, args)
	c := api.NewClient(*server)
	ctx := context.Background()
	ci, err := c.Resolve(ctx, *rev)
	if err != nil {
		return err
	}
	f, err := os.Create(*out)
	if err != nil {
		return err
	}
	defer f.Close()
	bw := bufio.NewWriterSize(f, 4<<20)
	var hdr [36]byte
	n := 0
	emit := func(id object.ID, b []byte) error {
		copy(hdr[:32], id[:])
		binary.BigEndian.PutUint32(hdr[32:], uint32(len(b)))
		bw.Write(hdr[:])
		_, err := bw.Write(b)
		n++
		return err
	}
	level := []object.ID{}
	if !ci.Root.IsZero() {
		level = append(level, ci.Root)
	}
	seen := map[object.ID]bool{}
	for len(level) > 0 {
		var next, blobs []object.ID
		err := c.Fetch(ctx, level, func(id object.ID, b []byte) error {
			_, p, err := object.Verify(id, b)
			if err != nil {
				return err
			}
			t, err := object.DecodeTree(p)
			if err != nil {
				return err
			}
			for _, e := range t.Entries {
				if seen[e.ID] {
					continue
				}
				seen[e.ID] = true
				if e.Mode == object.ModeDir {
					next = append(next, e.ID)
				} else {
					blobs = append(blobs, e.ID)
				}
			}
			return emit(id, b)
		})
		if err != nil {
			return err
		}
		if err := c.Fetch(ctx, blobs, emit); err != nil {
			return err
		}
		level = next
	}
	if err := bw.Flush(); err != nil {
		return err
	}
	fmt.Printf("snapshot of %s (%s): %d objects -> %s\n", *rev, ci.ID.Short(), n, *out)
	return nil
}

func loadSnapshot(path string, dst store.Objects) (int, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	br := bufio.NewReaderSize(f, 4<<20)
	var hdr [36]byte
	n := 0
	for {
		if _, err := io.ReadFull(br, hdr[:]); err != nil {
			if err == io.EOF {
				return n, nil
			}
			return n, err
		}
		var id object.ID
		copy(id[:], hdr[:32])
		b := make([]byte, binary.BigEndian.Uint32(hdr[32:]))
		if _, err := io.ReadFull(br, b); err != nil {
			return n, err
		}
		if err := dst.Put(id, b); err != nil {
			return n, err
		}
		n++
	}
}
