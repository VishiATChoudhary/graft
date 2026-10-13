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

* **The server does the work.** History, blame, diffs, merge bases, commits,
  rebases and merges all happen server-side, and every one of them is an
  HTTP API. Many jobs never need a local repository.
* **Commits are changesets** in a write-ahead log: the files changed relative
  to the first parent, pointing at content-addressed blobs. Creating one costs
  O(size of change), never O(size of repo).
* **Landing is lock-free.** `gft land` rebases server-side and moves the ref
  with a compare-and-swap on one row. No global lock, no merge queue.
* **Derived data** (root trees, a skip-list commit graph, per-path history,
  rename detection, blame) is computed by one generic, recursive mechanism,
  eagerly or lazily, and is invisible to clients.
* **Want-only fetch.** Clients ask only for objects they want, by ID. Every
  object is immutable, so a node daemon, a regional cache and the server all
  answer the same RPC from cache. No packfiles, no delta computation, no repo
  maintenance anywhere.
* **Working copies are optional and sparse**, and `gft mount` gives a
  virtualized checkout where files are fetched on first read.

See [docs/DESIGN.md](docs/DESIGN.md) for how each part of the post maps to
code, and where graft differs.

## Results vs git

Synthetic agent-scale monorepo: 100,000 files, 20,001 commits of small agent
changes (`bench/gen_monorepo.py`). Same machine for both systems (Apple
silicon, 10 cores, loopback): git serves via `upload-pack` over `file://`
with filters enabled and a commit-graph; graft serves through
server → regional cache → node daemon. CPU counts client **and** server.
Median of 5 runs. Full table: [bench/RESULTS.md](bench/RESULTS.md).

| workload | git | graft | |
|---|---:|---:|---|
| Sparse checkout of 1 package (50 of 100k files) | 716 ms (`--depth 1 --filter=blob:none --sparse`, best case) | **54 ms** | 13x faster |
| Full checkout, 100k files | 9.3 s (`--depth 1`) / 10.2 s | **4.5 s** | 2x faster, less CPU |
| Move working copy forward 100 commits | 190 ms, 567 ms CPU | **68 ms, 95 ms CPU** | 2.8x, 6x less CPU |
| Concurrent landing: 16 agents x 8 changes | 3.1 lands/s, 439 push retries, 226 s CPU | **170 lands/s**, 2.3 s CPU | **55x throughput, 100x less CPU** |
| Blame of a hot file | 124 ms (needs full clone) | **20 ms** (no clone) | 6x |
| Log of one file | 77 ms (needs full clone) | **12 ms** (no clone) | 6x |
| Is `main~19000` an ancestor of `main`? | 11 ms (needs full clone) | **6 ms** (no clone) | |
| Files changed in a dir across 5000 commits | 11 ms (needs full clone) | **7 ms** (no clone) | |
| `status`, clean 100k-file working copy | 249 ms | **120 ms** wall (more CPU: parallel stat) | |

Correctness checks run alongside: every landed change is verified present
and history linear; imported trees match git byte-for-byte for every commit;
path logs match `git log -- <path>` on every file of a real repository; blame
matches `git blame` on 95% of lines (the rest are equally valid alignments of
repeated lines like `}`).

**Where git wins:** storage. graft compresses objects individually instead of
delta-compressing them into packs (the design trades disk for zero
maintenance and cache-friendly transfer), so its server store is about 4.5x
an aggressively packed git repository (479 MB vs 106 MB here). Each `gft`
query is also a separate process plus an HTTP round trip, so tiny queries use
a few ms more CPU than git's in-process equivalents (but need no clone).

Reproduce: `make bench` (generates the repo in about a minute, then runs
everything).

## Quick start

```sh
go build -o bin/gft ./cmd/gft          # Go 1.26+

# Import an existing git repository (server stopped; HEAD becomes `main`).
gft import --data ./srv --git ~/src/myrepo

# Run the tiers. Each has exactly one upstream.
gft serve --tier server --data ./srv --listen 127.0.0.1:7070
gft serve --tier cache  --upstream http://127.0.0.1:7070 --listen 127.0.0.1:7071 --mem 8G
gft serve --tier daemon --upstream http://127.0.0.1:7071 --listen unix:///tmp/gft.sock --disk 20G

# Work.
gft clone http://127.0.0.1:7070 wc --objects unix:///tmp/gft.sock --sparse services/api
cd wc
$EDITOR services/api/handler.go
gft status
gft diff
gft commit -m "api: handle retries"      # created on the server; nothing to push
gft land                                  # server-side rebase if main moved, then CAS
```

Ask the server questions without any checkout:

```sh
export GFT_SERVER=http://127.0.0.1:7070
gft log -n 20 services/api                # commits touching a path
gft log --follow services/api/new_name.go # across renames
gft blame services/api/handler.go
gft diff --stat --from main~500 --to main services/
gft merge-base main some-branch
gft is-ancestor 1a2b3c4d main
gft find handler --in services/
gft cat --rev main~3 services/api/handler.go
```

Commit without a working copy (agents, bots, codemods):

```sh
gft write --parent main --land main -m "bump version" VERSION=@./VERSION old.txt=-delete
```

Virtual checkout (files fetched on first read; macOS needs no kernel
extension, Linux needs root to mount NFS):

```sh
gft mount http://127.0.0.1:7070 /tmp/repo --rev main --objects unix:///tmp/gft.sock
```

Seed node daemons from a snapshot so they start warm:

```sh
gft snapshot --server http://127.0.0.1:7070 --rev main --out main.gfts
gft serve --tier daemon --upstream http://cache:7071 --seed main.gfts
```

## HTTP API

Everything the CLI does is an API call to the server (`internal/api`):

| endpoint | |
|---|---|
| `POST /v1/objects` | want-only object fetch (same on every tier) |
| `GET /v1/resolve?rev=` | revision to changeset + root tree (`main`, `main~N`, ID prefix) |
| `GET /v1/log?rev=&path=&limit=&follow=&first_parent=` | history |
| `GET /v1/blame?rev=&path=` | blame with authors and line text |
| `GET /v1/diff?from=&to=&mergebase=&path=&patch=` | file changes or a unified patch |
| `GET /v1/mergebase?a=&b=`, `/v1/isancestor` | ancestry |
| `GET /v1/ls`, `/v1/cat`, `/v1/search` | trees, file content, path search |
| `POST /v1/changesets`, `/v1/write` | create a changeset (from blob IDs or inline content) |
| `POST /v1/rebase`, `/v1/merge`, `/v1/land`, `/v1/refs/update` | server-side rewrites and ref CAS |
| `POST /v1/objects/missing`, `/v1/objects/put` | blob upload (verified against IDs) |

## Status

Version 0.1. The core design is implemented and tested (`go test -race ./...`,
plus `bench/smoke.sh` which imports a real repository and cross-checks it
against git). Not yet built: a distributed server backend (the server is one
process on pebble; reads scale out through caches), directory-level ACLs, code
review, and a writable virtual checkout.

## License

Apache 2.0. See [LICENSE](LICENSE).
