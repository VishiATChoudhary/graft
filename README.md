# graft

**Source control for agent-scale monorepos.** An open-source implementation of
the design in Anthropic's
[Redesigning source control at Anthropic](https://www.anthropic.com/engineering/redesigning-source-control).

> graft is an independent project. It is not Anthropic's Scion, contains no
> Anthropic code, and is not affiliated with or endorsed by Anthropic. It
> implements the publicly described design. (A scion is the cutting you graft
> onto rootstock.)

Git assumes every clone is a full, offline-capable repository. When nearly
every client is an always-online agent or CI job, that assumption costs
packfiles, mirrors, negotiation CPU and merge-queue contention. graft drops it:

* **The server does the work.** History, blame, diffs, merge bases, search,
  commits, rebases and merges all happen server-side, and each is an HTTP
  API. Many jobs never need a local repository.
* **Commits are changesets** in a write-ahead log: the files changed relative
  to the first parent, pointing at content-addressed blobs. Creating one costs
  O(size of change), never O(size of repo).
* **Landing is lock-free.** `gft land` rebases server-side and moves the ref
  with a compare-and-swap on one row. No global lock, no merge queue.
* **Derived data** (root trees, a skip-list commit graph, per-path history,
  rename detection, blame, access-control indexes) comes from one generic,
  recursive mechanism, eager or lazy, invisible to clients.
* **Want-only fetch** through identical tiers (node daemon, regional cache,
  server). Objects are immutable, so every tier answers from cache. No
  packfiles, no negotiation, no repository maintenance anywhere.
* **Horizontally scalable.** Storage is sharded across nodes; any number of
  stateless API servers share it.
* **Working copies are optional and sparse.** `gft mount` gives a writable
  virtual checkout that fetches files on first read, commits only what you
  changed, and switches revisions instantly.
* **Repository and directory-level access control**, plus **review gates**
  that tell comment-only, whitespace and docs changes from logic changes.

[docs/DESIGN.md](docs/DESIGN.md) maps each part of the post to code.

## Results vs git

Synthetic agent-scale monorepo: 100,000 files, 20,001 commits of small agent
changes (`bench/gen_monorepo.py`). Same machine for both (Apple silicon, 10
cores, loopback). git serves via `upload-pack` over `file://` with filters
and a commit-graph. graft serves through server, regional cache and node
daemon. CPU counts client **and** server. Median of 5 runs; full table in
[bench/RESULTS.md](bench/RESULTS.md).

| workload | git | graft | |
|---|---:|---:|---|
| Sparse checkout, 1 package of 100k files | 308 ms (`--depth 1 --filter=blob:none --sparse`, best case) | **25 ms** | 12x |
| Virtual checkout + read 1 package | (needs a clone) | **86 ms** | |
| Full checkout, 100k files | 9.0 s (`--depth 1`) | **4.7 s**, less CPU | 1.9x |
| Move a working copy forward 100 commits | 148 ms, 603 ms CPU | **52 ms, 78 ms CPU** | 2.8x, 8x less CPU |
| 16 agents x 8 concurrent lands | 3.6 lands/s, 442 push retries, 224 s CPU | **196 lands/s**, 2.3 s CPU | **54x throughput, 95x less CPU** |
| Search all 100k files (fixed string) | 2.1 s `git grep` (in a full clone) | **64 ms** (server-side, no clone) | 32x |
| Log of one file | 54 ms (in a full clone) | **9 ms** (no clone) | 6x |
| Blame of a hot file | 91 ms (in a full clone) | **8 ms** memoized, 108 ms first time (no clone) | |
| `status`, clean 100k-file working copy | 147 ms | **99 ms** | |
| `status` with a filesystem watcher | 26 ms (git's fsmonitor daemon) | **25 ms** (`gft watch`) | tie |
| Is `main~19000` an ancestor of `main`? | 10 ms (in a full clone) | **5 ms** (no clone) | |

**Storage** (on disk after import; git includes its commit-graph with
changed-path filters). graft stores objects with write-time deltas and a
trained dictionary and needs no repacking. git's numbers are after an
explicit repack (`git repack -adf`) or `gc --aggressive`. graft uses SHA-256
IDs, so the SHA-256 git columns compare like with like:

| repository | graft | git SHA-1 repack | git SHA-1 aggressive | git SHA-256 repack | git SHA-256 aggressive |
|---|---:|---:|---:|---:|---:|
| synthetic monorepo, 20k commits | **96.0 MB** | 113.7 MB | 106.2 MB | 122.6 MB | 115.1 MB |
| redis/redis, 13k commits | **83.8 MB** | 84.1 MB | 80.5 MB | 91.3 MB | 80.9 MB |
| spf13/cobra, 1.1k commits | **1.47 MB** | 1.60 MB | 1.48 MB | 1.61 MB | 1.49 MB |

That table also holds graft's history indexes: per-path history, the commit
graph, and rename sources. graft is smaller than git's repack on all three
repositories (SHA-1 or SHA-256). It is also smaller than `gc --aggressive`
on cobra and the monorepo. On redis, `gc --aggressive` (an offline,
minutes-long repack) is 4% smaller.

**Correctness checks** that run with the benchmarks and tests:
* every concurrent land is verified present and history linear;
* imported trees match git for every commit;
* path logs match `git log -- <path>` on every file of a real repository;
* unified diffs are byte-identical to `git diff` on 3000 random inputs;
* blame matches `git blame` on 99.6% of lines of spf13/cobra (the rest are
  copy-detection edge cases).

Reproduce: `make bench` (generates the monorepo, then runs everything) and
`bench/storage.sh <repo> <name>`.

## Quick start

```sh
go build -o bin/gft ./cmd/gft          # Go 1.26+

# Import a git repository (server stopped; HEAD becomes `main`).
gft import --data ./srv --git ~/src/myrepo

# Single-node server, a regional cache and a node daemon (each tier has one upstream).
gft serve --tier server --data ./srv --listen 127.0.0.1:7070
gft serve --tier cache  --upstream http://127.0.0.1:7070 --listen 127.0.0.1:7071 --mem 8G
gft serve --tier daemon --upstream http://127.0.0.1:7071 --listen unix:///tmp/gft.sock --disk 20G

# Work in a sparse checkout.
gft clone http://127.0.0.1:7070 wc --objects unix:///tmp/gft.sock --sparse services/api
cd wc && gft watch --background          # status now only looks at what changed
$EDITOR services/api/handler.go
gft status && gft diff
gft commit -m "api: handle retries"      # created on the server; nothing to push
gft land                                 # server-side rebase if main moved, then CAS
```

Or skip the checkout entirely and mount a revision:

```sh
gft mount http://127.0.0.1:7070 /tmp/repo --objects unix:///tmp/gft.sock
cd /tmp/repo                     # files are fetched on first read
$EDITOR README.md                # writes go to a local overlay
gft status && gft commit -m "docs" && gft land   # only the overlay is uploaded
gft checkout main~100            # instant: the mount switches revision in place
```

`gft mount` exports a loopback NFSv3 server. On macOS this needs no kernel
extension or root. On Linux, mounting NFS needs root (`--no-mount` prints the
command).

Ask questions without any checkout:

```sh
export GFT_SERVER=http://127.0.0.1:7070
gft log -n 20 services/api                 # commits touching a path
gft log --follow services/api/new_name.go  # across renames
gft blame services/api/handler.go
gft grep -F "TODO(agent)" --in services    # server-side code search
gft diff --stat --from main~500 --to main services/
gft merge-base main some-branch
gft find handler --in services/
gft write --parent main --land main -m "bump" VERSION=@./VERSION   # commit without a checkout
```

## Scaling out

Run storage as shards and put any number of stateless API servers in front:

```sh
NODES=http://s0:7600,http://s1:7600,http://s2:7600
gft serve --tier store  --data /data/s0 --nodes $NODES --shard 0 --listen :7600   # on s0 (and s1, s2)
gft serve --tier server --store $NODES --listen :7070                            # on as many hosts as needed
gft import --store $NODES --git ~/src/myrepo
```

Objects and derived data are placed by ID. Refs and the changeset log live
on shard 0, where ref compare-and-swap is atomic. API servers are caches that
compute derived data and write it back, so they hold no state of their own.
Delta bases are resolved across shards, so storage stays dense. On a single
machine, three API servers over three shards land 139 lands/s versus 200 for
one server: the extra hops cost latency, and the point of the fleet is
spreading load across machines.

## Access control and review

```sh
gft serve --tier server --data ./srv --acl policy.json
gft clone http://host:7070 wc --token "$TOKEN"     # or GFT_TOKEN
```

Policies give tokens principals and groups, repository roles, and
per-directory read and write rules:
* Reads are checked on every path-based query.
* Writes are checked per path, and every ref move is checked against its
  whole diff, so merges cannot smuggle changes in.
* Trees of protected directories are refused on fetch. Shared cache tiers
  never cache them and always forward the caller's credentials.
* Checkouts skip directories the caller cannot read.

Review rules require approvals for changes under a path, but can exempt
comment-only, whitespace-only and docs changes. Those are recognised per
language, so reviewers only see changes that can alter behaviour:

```sh
gft review            # what the current change needs
gft approve <rev>     # approvals are per changeset; authors cannot self-approve
```

See [docs/ACCESS.md](docs/ACCESS.md).

## HTTP API

Everything the CLI does is an API call to the server (`internal/api`):

| endpoint | |
|---|---|
| `POST /v1/objects` | want-only object fetch (same on every tier) |
| `GET /v1/resolve?rev=` | revision to changeset + root tree (`main`, `main~N`, ID prefix) |
| `GET /v1/log?rev=&path=&limit=&follow=&first_parent=` | history |
| `GET /v1/blame?rev=&path=` | blame with authors and line text |
| `GET /v1/diff?from=&to=&mergebase=&path=&patch=` | file changes or a unified patch |
| `GET /v1/grep?rev=&q=&path=&fixed=&i=` | content search |
| `GET /v1/mergebase?a=&b=`, `/v1/isancestor` | ancestry |
| `GET /v1/ls`, `/v1/cat`, `/v1/search` | trees, file content, path search |
| `POST /v1/changesets`, `/v1/write` | create a changeset (from blob IDs or inline content) |
| `POST /v1/rebase`, `/v1/merge`, `/v1/land`, `/v1/refs/update` | server-side rewrites and ref CAS |
| `POST /v1/reviews/approve`, `GET /v1/reviews/status` | review gates |
| `POST /v1/objects/missing`, `/v1/objects/put` | blob upload (verified against IDs) |

## Status

Version 0.1, tested with `go test -race ./...`, the repository suite against
a 3-shard fleet, and `bench/smoke.sh`, which imports a real repository and
cross-checks it against git. Code review here means approval gates; there is
no review UI. Windows is not supported.

## License

Apache 2.0. See [LICENSE](LICENSE).
