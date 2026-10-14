# graft design

graft is an open-source implementation of the source-control design described
in Anthropic's engineering post
[Redesigning source control at Anthropic](https://www.anthropic.com/engineering/redesigning-source-control)
(October 2026). That post describes Scion, Anthropic's internal system. graft
is not Scion and contains no Anthropic code. It is an independent build of the
published design, written to be small enough to read in an afternoon.

This document maps each principle and mechanism from the post to where graft
implements it, and records where graft deliberately differs.

## Principles

| Post | graft |
|---|---|
| **Clients are always online.** Clients fetch data when they need it from an always-reachable server. | The client (`internal/client`) keeps no history. It resolves revisions, asks for diffs, and creates commits through the server API (`internal/api`). |
| **Client-server asymmetry.** The server answers history and blame questions, creates commits, merges and rebases. The client keeps a bounded object cache, lays out files, watches for changes and gives an ergonomic interface. | Server: `internal/repo` (log, path log, blame, merge-base, diff, rebase, merge, land). Client: bounded segment cache (`store.DiskCache`), sparse materialization, stat-based status, `gft` CLI. |
| **Source control as a service.** Every query the client uses is also an API, so many jobs need no local repository. | `internal/api` exposes log, blame, diff (with `mergebase=`), merge-base, is-ancestor, ls, cat, search, write, rebase, merge, land as HTTP/JSON. `gft write` commits without a checkout. |
| **Everything horizontally scalable.** No fully materialized repo anywhere, no packfiles, no repository maintenance. | Objects are content-addressed and immutable; every tier is a cache that needs no coordination. Objects are compressed individually (zstd); there is no packing, repacking or gc anywhere. |
| **Commits are cheap, incremental and lock-free.** Every commit, rebase and merge happens on the server; cost scales with the change, not the repo. Refs are discouraged and moved by compare-and-swap. | `repo.Create` touches only the changed paths (`applyTree`, `applyUnode` are O(changes x depth)). Rebase examines only the paths a changeset touched. `repo.Land` rebases server-side and moves the ref with a CAS on a single row (a per-ref mutex around one key; no global lock). |
| **The working copy is optional and sparse by design.** | `gft clone --sparse`, `gft sparse add/set`, and `gft mount` (virtualized, writable checkout that fetches on first read). CI questions use the API and skip the checkout entirely. |

## Data structures

### Changesets (`internal/object`)

The unit of work is a changeset, an entry in a write-ahead log. Like the post's
changesets, it stores parents, author, time, message, and **the list of files
changed relative to its first parent**, each with a content-addressed pointer
to a compressed blob. It does not point at a root tree.

```
Changeset { Parents []ID; Author; Time; TZ; Message; Origin; Changes []{Path, Mode, Blob} }
```

Changing relative to the first parent follows the post's footnote: in a
trunk-based monorepo, history and blame matter along the main line. Changesets
are appended to the log (`store.DB.AppendLog`) with a sequence number; there
are no refs involved in creating or fetching them.

IDs are SHA-256 over a kind byte plus the payload (hardware-accelerated on
arm64 and amd64). The stored form is `[kind][codec][payload]`, and it is the
same on disk, in every cache and on the wire.

### Derived data (`internal/derive`, `internal/repo`)

Everything that is not part of a changeset is derived data, computed through
one generic mechanism. Each type is defined recursively: its value for a node
is computed from the node plus its parents' values. Each type is either eager
(computed when a changeset is created) or lazy (computed on first query). The
engine derives iteratively, so a lazy type over a 100k-deep history does not
recurse; concurrent requests for the same value are coalesced.

| Type | Keyed by | Eager | Purpose |
|---|---|---|---|
| `cs` | changeset | yes | One compact record per changeset: root tree, the link of its root history version, generation number, first-parent depth, parents (as log sequence numbers) and a deterministic **skip list** (a changeset at depth d keeps pointers 2^k back for each 2^k dividing d: about two per changeset, O(log n) jumps). Gives `rev~N`, trunk ancestry checks and land-stack detection in O(log n). |
| history (`ub`) | changeset | yes | Path-history manifest (after Mononoke's unodes): one version per changed directory, whose entries record the log sequence of the changeset that last modified each file or subdirectory. All directory versions written by a changeset are one record; each is whole or a delta against the previous version of the same directory. `log <path>` walks only commits that touched the path; previous versions are found by looking the path up at the link changeset's parents, which reproduces git's path-limited history including TREESAME merge simplification. Deletions leave tombstones so history survives delete/re-add. |
| rename index (`copy`) | path version | yes | Rename detection like git's `-M50%` (exact, then line similarity), recorded once. Blame follows it; `log --follow` follows it. |
| `blame` | path version | no | Blame runs per file version, computed from the previous version's blame plus a line diff (git's algorithm, see below). |
| `acl` | tree | yes (with a policy) | Trees of access-controlled directories, for checking object fetches. |

Because clients never see these structures, they can change without changing
the client. Adding a new index is a new `derive.Type`.

### Trees

Directory listings sorted by name, with file sizes in each entry so a virtual
checkout can answer `stat` without fetching the blob. Trees are built
server-side; clients only upload blobs.

## Fetch protocol and tiers (`internal/fetch`)

The client never tells the server what it already has. It asks only for the
objects it wants:

```
POST /v1/objects      body: N x 32-byte IDs
response              stream of [id][u32 len][stored bytes]; len=0xFFFFFFFF means missing
```

Checkout starts from the root tree, requests the trees it lacks level by level
(skipping anything outside the sparse spec), then requests blobs in parallel
batches. Every request is for an immutable object, so:

* any tier can answer from cache;
* a failed transfer is resumed by asking only for what is still missing;
* there is no multi-gigabyte packfile response and no delta computation;
* sparse and virtual checkouts need no special support (the client simply
  doesn't ask).

Tiers speak identical RPCs, so the client doesn't know the topology:

```
gft (client) --unix socket--> daemon (per node, disk cache, optional snapshot seed)
                                 --> cache (per region, in-memory LRU, streams from memory)
                                        --> server (authoritative, pebble)
```

Each tier has exactly one upstream (`--upstream`), which enforces strict
tiering and isolates failure domains. Concurrent misses for the same object are
coalesced into a single upstream request (`fetch.Tier`). Daemons can start from
a periodically refreshed snapshot (`gft snapshot`, `--seed`).

## Landing without a merge queue

`repo.Land(ref, head)`:

1. Read the ref tip.
2. If the tip is an ancestor of `head`, fast-forward.
3. Otherwise find `head`'s stack (its first-parent changesets not on the
   trunk) using skip-list jumps, so the cost does not depend on how far the
   trunk has moved. Rebase the stack server-side onto the tip (three-way merge
   per touched path; conflicts are reported with paths).
4. Compare-and-swap the ref. If another land won the race, go to 1.

There is no global lock. This matches the post's move away from a global
merge-queue lock toward post-merge testing.

**Coalescing.** Lands for one ref that reach a server while another is in
flight queue up; one goroutine takes the queue, rebases each request onto
the previous result, and publishes the whole chain with a single CAS. Each
request is still checked (permissions, reviews) against exactly its own
change and fails alone (a conflict or a veto drops that request, not the
batch). Inside a batch, rebased changesets are stored with their root trees
computed directly from the previous root and appended to the log together
in one request (one Raft command), then their derived data is computed.
Inputs every request will need are prefetched in parallel first.

**Ref affinity.** With `--peers`, an API server forwards a land to the peer
that owns the ref (hash of its name), so a ref's lands coalesce on one
server rather than racing across servers. It is only a routing hint: any
server can land any ref, and the CAS keeps it correct if the owner is down.

**Write-and-land.** A changeset created only to be landed is stored and
logged without computing its derived data, since the land rebases it at
once; stack walks take its depth from its parent.

## Storage (`internal/store`)

* **Server**: pebble (an LSM key-value store) holds objects, derived data,
  the changeset log and refs under disjoint key prefixes, standing in for
  Spanner plus blob storage. Refs are written with sync; everything else
  becomes durable with the next synced write.
* **Storage codecs** (server-internal; clients always receive the standard
  stored form, so caches and the wire are unaffected). Each is decided once,
  at write time; nothing is ever repacked:
  * *reverse blob deltas*: when a file changes, the new version stays whole
    and the previous one is re-encoded as a delta against it (as in git, the
    newest content is cheapest to read; code mostly grows, so these deltas
    are small). Keyframes are kept pseudo-randomly with probability
    size/64 MB, bounding the bytes rebuilt to read any version;
  * forward deltas for trees (chains capped at 255);
  * zstd **dictionaries** trained on the repository's own blobs and
    changesets, and stronger zstd for blobs too large for a dictionary;
  * delta bases also come from renames, merges' other parents, deleted
    files' surviving siblings or same-named files added in the same
    changeset, and similar new siblings (same extension, closest size);
  * 16-byte storage keys and base references (2^64 objects to a likely
    collision).
* **Regional cache**: sharded in-memory LRU, byte-bounded.
* **Node daemon and client**: bounded on-disk cache of append-only segment
  files with an in-memory index; eviction drops the oldest segment, and
  objects read from old segments are re-appended (promotion). No
  file-per-object. A client without a daemon caches trees only, since blobs
  are already in its working copy.

## Horizontal scaling and replication (`store.Sharded`, `store.NodeHandler`, `store.MetaGroup`)

`gft serve --tier store` runs one replica of one storage shard. Objects and
derived data are placed on shards by ID prefix, and each shard is a replica
set (`--nodes a|b|c,d|e|f`):

* immutable writes (objects, derived data) go to every replica of the shard
  and succeed once a majority acknowledges them (they are idempotent);
* reads go to one replica and fail over to the others; a replica that missed
  a write is repaired when another replica serves the data; data is reported
  absent only when a majority says so (any majority intersects every write
  majority);
* the only mutable state, refs and the changeset log, lives on shard 0,
  whose replicas form a Raft group (hashicorp/raft). Ref compare-and-swap
  and log appends are Raft commands, committed on a majority and applied in
  the same order on every replica; reads of current refs go through the
  leader after confirming leadership; followers answer with a redirect to
  the leader. Clients follow redirects and retry through elections.

`gft serve --tier server --store ...` runs a stateless API server over the
fleet. It caches everything immutable and computes derived data, writing the
results back, so any number can run.

Writes from an API server are write-behind with explicit ordering:

* object writes are group-committed per shard (what queues up while a
  request is in flight goes in the next request) and readable on that
  server at once;
* a log append waits only for the changeset and its blobs to be durable
  (trees are derived data; parents are already logged); a ref CAS needs no
  wait, since a ref can only name a logged changeset;
* derived writes go through one ordered writer per server, each batch after
  the object writes queued before it, so a record never becomes visible
  before records it refers to. Derived data is a cache: publishing never
  waits for it, and a lost history batch is regenerated from its changeset. A shard resolves delta bases held by
other shards from any of their replicas. Tests run the repository suite
against a 3-shard fleet, land concurrently through three API servers, and
kill the Raft leader and a data replica mid-workload (two shards x three
replicas) checking that landing continues and nothing is lost.

## Code search (`internal/repo/grep.go`, `trigram.go`)

Each directory version (tree ID) has a Bloom filter of the trigrams of the
files directly in it, lowercased so it also serves case-insensitive
searches (16 bits per distinct trigram, three hashes: about 0.1% false
positives per trigram). It is derived data keyed by the tree ID, computed
the first time a search reaches the directory; trees are content-addressed,
so a new revision only needs filters for directories it changed. A search
extracts the literal text every match must contain (the string itself for
fixed searches; literal runs that a regular expression requires, not
through alternations or optional parts), skips directories whose filter
lacks any of its trigrams, and scans the rest in parallel. Filters have no
false negatives and directories with files too large to index are never
skipped, so results equal a full scan (tested). On the 100k-file monorepo
selective searches take 11 to 24 ms against about 400 ms for a full scan;
the filters for a whole revision take about 4% of the repository's size.

Path search (`FindPaths`, `pathindex.go`) has its own index: each tree's
exact set of trigrams of every entry name in its subtree, built bottom-up
from its children's sets. A query without "/" lies inside one path
component, so a subtree lacking one of its trigrams is skipped unless an
enclosing directory's name already matches. Tested against matching every
path; selective path queries on the monorepo go from about 7 ms to 0 to 2 ms.

## Change requests and the merge queue (`internal/api/cr.go`)

A change request's record (title, author, target ref, revisions, comments,
state) is a JSON object in the store. The ref `meta/cr/<id>` names its
newest version and moves by compare-and-swap; an update reads the newest
version, applies its change and retries on a lost race, so comments made
concurrently through different API servers are all kept, on every backend.
`meta/` refs are hidden from ref listings and clones.

Approvals remain per changeset (refs `reviews/<changeset>/<who>`), so a
new revision needs its own. Queuing records the queuer after checking
their write permission against the request's whole diff. Each server's
queue pass (after queuing, after every approval, and every two seconds for
approvals made elsewhere) lands every queued request whose rules are
satisfied with the ordinary land; a conflict sends the request back to
`open` with the conflicting paths, and missing approvals are shown as what
the request is waiting for.

## Diff, merge and blame (`internal/diff`)

graft's line diff ports git's xdiff: end trimming, discarding of lines that
cannot match or match too often, git's divide-and-conquer Myers with its
heuristics and tie-breaking, and change compaction with the indent
heuristic. Unified diffs, including hunk headers with function context, are
byte-identical to `git diff` on 3000 random code-shaped inputs (tested
against git). Blame follows git's rules: a parent with identical content
takes the whole blame, otherwise lines pass to parents through the diff, and
renames are followed. Server-side merges use diff3.

## Working copies (`internal/client`, `internal/watch`, `internal/vfs`)

* **Sparse checkouts** materialize only the requested directories; updates
  move only changed files (from a server-computed diff) and check only the
  paths they touch for local edits.
* **Watcher**: `gft watch` keeps a journal of changed paths using FSEvents
  on macOS and inotify on Linux, synchronized with cookie files like git's
  fsmonitor. Status then examines only what changed. The index token only
  advances when the index provably reflects every earlier change.
* **Virtual checkout**: `gft mount` serves a revision through NFSv3 (macOS
  default, no kernel extension), FUSE (Linux default; fusermount is setuid
  so no root is needed) or WebDAV (Windows default, mounted with `net use`
  as a normal user).
  Unmodified files are fetched on first read; writes go to a copy-on-write
  overlay with whiteouts for deletions. Inside the mount, `gft status`,
  `commit`, `land` and `checkout` talk to the mount's control socket: commit
  uploads only the overlay, and checkout and land swap the base revision in
  place without writing files.

## Search, access control, review

* `gft grep` / `/v1/grep` searches a revision's contents server-side,
  scanning blobs in parallel from the server's caches (no index to store).
* Access control and review gates are described in
  [ACCESS.md](ACCESS.md).

## Where graft differs from the post

* **Storage backend.** `gft serve --spanner projects/P/instances/I/databases/D`
  runs stateless API servers directly on Cloud Spanner, as Scion does,
  with file contents in cloud storage when `--blobs` names a bucket
  (`gs://`, `s3://`, `file://`): trees, changesets and derived data are
  rows, ref CAS is a single conditional
  statement, log sequences are assigned in read-write transactions, and
  Spanner provides replication, failover and scaling. Log appends from
  concurrent requests are group-committed into one read-write transaction.
  CI runs the full repository suite against the in-memory `spannertest`
  server; on 2026-10-14 it was also run against a real Cloud Spanner
  instance (regional-us-west1, 100 processing units, a fresh database per
  test: `GRAFT_TEST_SPANNER=1 GRAFT_SPANNER_INSTANCE=projects/P/instances/I
  go test -tags cloud ./internal/repo/`). Passed there: trees and history,
  rebase/merge/land, concurrent landing, coalesced landing (failure
  isolation; conflicts and stacks), grep, indexed grep and path search,
  plus an end-to-end `gft serve --spanner --blobs` smoke test (write,
  land, change request through the merge queue, log, grep, clone). Not
  run there: the 1,500-changeset skip-list test (hours at that network's
  0.4 to 1.7 s per operation; it exercises no backend operation the
  others do not) and tests that need a local store. Spanner rows hold each object's
  standard compressed form, without graft's delta encodings, so the dense
  storage numbers apply to the pebble backends. Self-hosted alternative:
  pebble shards replicated by graft itself (majority writes, Raft for refs
  and the log), without automatic re-replication to new machines.
* **Code review** is graft's own: change requests with revisions, line
  comments, semantic approval rules and a merge queue that lands approved
  requests, with a web UI; Antique's details beyond what the post says are
  not public. **Code search** is graft's own design (the post gives none):
  per-directory trigram filters as derived data, see below.
* **Storage efficiency** is decided at write time; nothing ever needs
  repacking. `gft compact` is an optional pass (like `git gc --aggressive`)
  that re-deltas blobs against similar ones anywhere in history.

## Code map

```
cmd/gft/            CLI: client commands, serve (server|cache|daemon), mount, import, snapshot
internal/object/    IDs, blobs, trees, changesets, codec
internal/store/     pebble server store, memory LRU, segment disk cache
internal/derive/    generic derived-data engine
internal/repo/      changeset log, derived types, history, blame, renames, rebase/merge/land
internal/api/       HTTP API server and client
internal/fetch/     want-only object protocol, tiers, coalescing, private objects
internal/client/    working copy: checkout, sparse, status, commit, land
internal/watch/     filesystem watcher (FSEvents, inotify) and change journal
internal/vfs/       virtual checkout: lazy filesystem, copy-on-write overlay, mount control
internal/acl/       access control policy
internal/review/    change classification for review gates
internal/gitimport/ git fast-export ingestion
internal/diff/      git-compatible diff (xdiff port), diff3 merge, unified output
bench/              synthetic monorepo generator, benchmark harness, git cross-checks
```
