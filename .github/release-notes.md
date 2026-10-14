First release of graft, an open-source implementation of the source control
design in Anthropic's "Redesigning source control" post.

Each archive holds two binaries:

* `gft`: client, server, cache, daemon and storage tiers, mount, watch.
* `gft-cloud`: the same plus the Cloud Spanner and cloud-storage backends
  (`gft serve --spanner ... --blobs gs://...`).

Install from source instead with `go install github.com/VishiATChoudhary/graft/cmd/gft@latest`.

See the README for benchmarks against git and docs/DESIGN.md for how it
works and where it differs from the post. Verify downloads with
`checksums.txt`.
