graft, an open-source implementation of the source control design in
Anthropic's "Redesigning source control" post.

Since v0.1.1:

* Tested against real Cloud Spanner: the whole repository suite passes.
  New versions are stored there as compressed deltas.
* Fleet benchmark across machines (README): reads 1.9x and lands over many
  refs 1.4x a single server.
* `gft loadgen --read` and `--refs N`.

Since v0.1.0:

* Change requests, a merge queue that lands approved requests, and a review
  web UI at `<server>/review` (`gft cr`).
* Indexed code search (per-directory trigram filters) and path search.
* Cloud Spanner backend tested against a real instance; concurrent log
  appends are group-committed. File contents can live in a bucket
  (`--blobs gs://...`).
* Faster cold blame; the everyday `gft` binary leaves the cloud SDKs out
  (26 MB, about 5 ms to start).

Each archive holds `gft` and `gft-cloud` (adds the Cloud Spanner and
cloud-storage backends). Verify downloads with `checksums.txt`.
