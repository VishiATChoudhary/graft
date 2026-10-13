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
| **The working copy is optional and sparse by design.** | `gft clone --sparse`, `gft sparse add/set`, and `gft mount` (virtualized, read-only checkout that fetches on first read). CI questions use the API and skip the checkout entirely. |

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
| `root` | changeset | yes | Root tree: parent's root plus this changeset's changes. Needed for checkout and diff. |
| `graph` | changeset | yes | Generation number, first-parent depth, parents, and a **skip list** (`skips[k]` = first-parent ancestor 2^k back). Gives O(log n) `rev~N`, trunk ancestry checks, and stack detection for landing. |
| `unode` | changeset | yes | Path-history manifest (after Mononoke's unodes): each file/dir node records the changeset that last modified it and its previous versions. `log <path>` walks only the commits that touched the path. Deletions leave tombstones so history survives delete/re-add. |
| rename index | (inside `unode`) | yes | Rename detection like git's `-M50%` (exact match, then line-similarity), recorded once as `CopyFrom`. Blame follows it; `log --follow` follows it. |
| `blame` | file unode | no | Per-file-version blame runs, computed from the previous version's blame plus a line diff. |

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

There is no global lock. Landing many changes concurrently costs one CAS each
plus a rebase of the loser, which is proportional to the loser's change. This
matches the post's move away from a global merge-queue lock toward post-merge
testing.

## Storage (`internal/store`)

* **Server**: pebble (an LSM key-value store) holds objects, derived data, the
  changeset log, and refs under disjoint key prefixes. It stands in for Spanner
  plus blob storage. Refs are written with sync; everything else is written
  without fsync and becomes durable with the next synced write.
* **Regional cache**: sharded in-memory LRU, byte-bounded.
* **Node daemon and client**: bounded on-disk cache of append-only segment
  files with an in-memory index. Eviction drops the oldest segment; objects
  read from old segments are re-appended (promotion), approximating LRU. No
  file-per-object, so caching 100k objects is 100k appends.

## Where graft differs from the post

* **Single-node server.** The server tier is one process over a local pebble
  store. Reads scale out through cache tiers (and any number of server
  processes could share a distributed KV store behind the same `store`
  interface), but graft does not ship a distributed backend.
* **No code review, code search, or IAM.** The post's Antique code-review tool,
  code search and directory-level IAM are out of scope.
* **Virtual checkout is read-only** and exported over loopback NFSv3 (the same
  approach EdenFS uses on macOS), so it needs no kernel extension. Writable
  working copies use sparse materialization.
* **File watching.** Status uses parallel stat with an index (size, mtime), not
  an OS file watcher.
* **Storage efficiency.** Objects are compressed individually rather than
  delta-compressed in packs, as the post prescribes; the server store is
  several times larger than an aggressively packed git repository.

## Code map

```
cmd/gft/            CLI: client commands, serve (server|cache|daemon), mount, import, snapshot
internal/object/    IDs, blobs, trees, changesets, codec
internal/store/     pebble server store, memory LRU, segment disk cache
internal/derive/    generic derived-data engine
internal/repo/      changeset log, derived types, history, blame, renames, rebase/merge/land
internal/api/       HTTP API server and client
internal/fetch/     want-only object protocol, tiers, coalescing
internal/client/    working copy: checkout, sparse, status, commit, land
internal/vfs/       virtual checkout filesystem (exported over NFS by `gft mount`)
internal/gitimport/ git fast-export ingestion
internal/diff/      Myers diff, diff3 merge, unified output
bench/              synthetic monorepo generator, benchmark harness, git cross-checks
```
